"""Relay's bridge to SkyPilot. It runs in SkyPilot's own Python, never in relay's.

Relay starts it as ``<skypilot python> bridge.py <op>`` with a JSON request on
stdin. It imports only the standard library and ``sky``, so it works in a venv
that has SkyPilot and nothing of relay. SkyPilot prints progress on stdout, so
the bridge moves SkyPilot's stdout to stderr and writes one JSON answer, on a
line that starts with ``CLIO_RELAY_SKY``, to the original stdout.

Operations: ``check``, ``price``, ``launch``, ``queue``, ``cancel``, ``logs``.
Written against SkyPilot 0.14.3 (``sky.jobs.launch``, ``sky.jobs.queue_v2``,
``sky.jobs.cancel``, ``sky.jobs.download_logs``, ``sky.Resources.get_cost``).
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import traceback
from collections.abc import Sequence
from typing import Any, cast

MARKER = "CLIO_RELAY_SKY "


def _value(item: Any, name: str) -> Any:
    if isinstance(item, dict):
        return cast("dict[str, Any]", item).get(name)
    return getattr(item, name, None)


def _plain(value: Any) -> Any:
    value = getattr(value, "value", value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _first(value: Any) -> Any:
    """Return the first item of a list or tuple, or the value itself."""
    if isinstance(value, (list, tuple)):
        items = cast("Sequence[Any]", value)
        return items[0] if items else None
    return value


def _resources(sky: Any, request: dict[str, Any], instance_type: str | None = None) -> Any:
    fields = {
        "infra": request.get("infra"),
        "instance_type": instance_type or request.get("instance_type"),
        "cpus": request.get("cpus"),
        "memory": request.get("memory"),
        "accelerators": request.get("accelerators"),
        "use_spot": request.get("use_spot"),
        "disk_size": request.get("disk_size"),
        "region": request.get("region"),
        "zone": request.get("zone"),
    }
    return sky.Resources(**{key: value for key, value in fields.items() if value is not None})


def _instance_type(resources: Any, request: dict[str, Any]) -> str:
    if resources.instance_type:
        return resources.instance_type
    cloud = resources.cloud
    if cloud is None:
        raise ValueError("pricing needs a cloud; set infra, such as aws")
    if request.get("accelerators"):
        feasible = cloud._get_feasible_launchable_resources(resources)  # noqa: SLF001
        if not feasible.resources_list:
            hint = ", ".join(feasible.fuzzy_candidate_list or [])
            raise ValueError(
                f"{cloud} has no instance with {request['accelerators']}"
                + (f"; it offers {hint}" if hint else "")
            )
        return feasible.resources_list[0].instance_type
    found = cloud.get_default_instance_type(
        cpus=request.get("cpus"),
        memory=request.get("memory"),
        region=request.get("region"),
        zone=request.get("zone"),
        use_spot=bool(request.get("use_spot")),
    )
    if not found:
        raise ValueError(f"{cloud} has no instance that fits; set instance_type")
    return found


def op_check(sky: Any, request: dict[str, Any]) -> dict[str, Any]:
    return {"version": getattr(sky, "__version__", "unknown")}


def op_price(sky: Any, request: dict[str, Any]) -> dict[str, Any]:
    resources = _resources(sky, request)
    instance_type = _instance_type(resources, request)
    priced = _resources(sky, request, instance_type)
    seconds = float(request["seconds"])
    nodes = int(request.get("nodes") or 1)
    return {
        "amount": priced.get_cost(seconds) * nodes,
        "per_hour": priced.get_cost(3600),
        "instance_type": instance_type,
        "cloud": _plain(priced.cloud),
        "region": _plain(getattr(priced, "region", None)),
        "use_spot": bool(request.get("use_spot")),
        "unit": "USD",
    }


def op_launch(sky: Any, request: dict[str, Any]) -> dict[str, Any]:
    task = sky.Task(
        name=request["name"],
        run=request["run"],
        setup=request.get("setup"),
        envs=request.get("envs") or None,
        workdir=request.get("workdir"),
        num_nodes=request.get("num_nodes"),
        file_mounts=request.get("file_mounts") or None,
    )
    task.set_resources(_resources(sky, request))
    result: Any = sky.stream_and_get(sky.jobs.launch(task, name=request["name"]))
    job_ids = _first(_first(result))
    if job_ids is None:
        raise RuntimeError("SkyPilot returned no managed job id")
    return {"job_id": int(job_ids)}


def op_queue(sky: Any, request: dict[str, Any]) -> dict[str, Any]:
    ids = [int(item) for item in request["job_ids"]]
    result: Any = sky.get(sky.jobs.queue_v2(refresh=False, job_ids=ids, all_users=False))
    records = _first(result) if isinstance(result, tuple) else result
    jobs: list[dict[str, Any]] = []
    for record in cast("Sequence[Any]", records or []):
        jobs.append(
            {
                "job_id": _plain(_value(record, "job_id")),
                "status": _plain(_value(record, "status")),
                "failure_reason": _plain(_value(record, "failure_reason")),
                "cloud": _plain(_value(record, "cloud")),
                "region": _plain(_value(record, "region")),
                "end_at": _plain(_value(record, "end_at")),
            }
        )
    return {"jobs": jobs}


def op_cancel(sky: Any, request: dict[str, Any]) -> dict[str, Any]:
    sky.get(sky.jobs.cancel(job_ids=[int(item) for item in request["job_ids"]]))
    return {}


def op_logs(sky: Any, request: dict[str, Any]) -> dict[str, Any]:
    found: Any = sky.jobs.download_logs(
        name=None,
        job_id=int(request["job_id"]),
        refresh=False,
        controller=False,
        local_dir=request["local_dir"],
    )
    paths = cast("dict[Any, Any]", found or {})
    return {"paths": {str(key): str(value) for key, value in paths.items()}}


OPS: dict[str, Any] = {
    "check": op_check,
    "price": op_price,
    "launch": op_launch,
    "queue": op_queue,
    "cancel": op_cancel,
    "logs": op_logs,
}


def main() -> None:
    answer_fd = os.dup(1)
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    op = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        request = json.loads(sys.stdin.read() or "{}")
        if op not in OPS:
            raise ValueError(f"unknown operation {op!r}")
        try:
            sky = importlib.import_module("sky")
            importlib.import_module("sky.jobs")
        except ImportError as exc:
            answer = {"ok": False, "kind": "import", "error": f"cannot import sky: {exc}"}
        else:
            answer = {"ok": True, "result": OPS[op](sky, request)}
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        answer = {"ok": False, "kind": type(exc).__name__, "error": str(exc) or repr(exc)}
    with os.fdopen(answer_fd, "w", encoding="utf-8") as out:
        out.write(MARKER + json.dumps(answer) + "\n")


if __name__ == "__main__":
    main()
