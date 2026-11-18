"""How the ``globus`` adapter talks to Globus Compute.

:class:`ComputeService` is the narrow interface the adapter needs. The real one,
:class:`SdkComputeService`, wraps the Globus Compute SDK, imported only when it
is used. It submits through ``create_batch`` and ``batch_run`` and polls with
``get_task``. It never opens the SDK's ``Executor``, which holds a long-lived
connection.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

from clio_relay.adapters.globus.task import relay_task
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason

INSTALL_HINT = "install the globus extra: pip install 'clio-relay[globus]'"


@dataclass(frozen=True)
class TaskInfo:
    """What the Compute service reports about one task."""

    pending: bool
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None


@dataclass(frozen=True)
class EndpointInfo:
    """What the Compute service reports about an endpoint."""

    status: str
    facts: dict[str, Any] = field(default_factory=dict[str, Any])


class ComputeService(Protocol):
    """The calls the adapter makes on Globus Compute."""

    async def submit(self, payload: dict[str, Any], user_config: dict[str, Any]) -> str:
        """Submit one :func:`relay_task` call and return the task id."""
        ...

    async def task(self, task_id: str) -> TaskInfo:
        """Report one task."""
        ...

    async def endpoint(self) -> EndpointInfo:
        """Report the endpoint."""
        ...


def function_digest() -> str:
    """Return the digest of the function relay registers, so a change re-registers it."""
    return hashlib.sha256(inspect.getsource(relay_task).encode()).hexdigest()


def _http_status(exc: BaseException) -> int | None:
    value = getattr(exc, "http_status", None)
    return value if isinstance(value, int) else None


def _refusal(exc: BaseException, reason: RefusalReason, doing: str) -> Refusal:
    status = _http_status(exc)
    if status in (401, 403):
        return Refusal(
            RefusalReason.NEEDS_SIGN_IN,
            f"Globus refused the sign-in while {doing}; sign in to Globus again",
            provider="globus",
            http_status=status,
        )
    return Refusal(reason, f"Globus Compute failed while {doing}: {exc}", http_status=status)


class SdkComputeService:
    """Globus Compute through its SDK, with an access token from relay's identity."""

    def __init__(self, endpoint_id: str, token: str, state_dir: Path) -> None:
        self.endpoint_id = endpoint_id
        self.token = token
        self.state_dir = state_dir
        self._client: Any = None

    def _sdk_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import globus_sdk
            from globus_compute_sdk import Client
            from globus_compute_sdk.serialize import PureSourceTextInspect
        except ImportError as exc:
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                f"the globus adapter needs the Globus Compute SDK; {INSTALL_HINT}",
            ) from exc
        authorizer = globus_sdk.AccessTokenAuthorizer(self.token)
        # Ship the function as source text, so the endpoint needs no relay install.
        self._client = Client(
            authorizer=authorizer,
            do_version_check=False,
            code_serialization_strategy=PureSourceTextInspect(),
        )
        return self._client

    def _function_id(self, client: Any, *, fresh: bool = False) -> str:
        path = self.state_dir / "function.json"
        digest = function_digest()
        try:
            known: Any = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            known = {}
        if isinstance(known, dict) and not fresh:
            cached = cast(dict[str, Any], known).get(digest)
            if isinstance(cached, str):
                return cached
        function_id = str(client.register_function(relay_task, description="clio-relay job"))
        write_json_atomic(path, {digest: function_id})
        return function_id

    def _batch_run(self, client: Any, function_id: str, payload: dict[str, Any], uc: Any) -> Any:
        batch = client.create_batch(user_endpoint_config=uc or None)
        batch.add(function_id, args=(payload,))
        return client.batch_run(self.endpoint_id, batch)

    def _submit(self, payload: dict[str, Any], user_config: dict[str, Any]) -> str:
        client = self._sdk_client()
        try:
            function_id = self._function_id(client)
            try:
                response: Any = self._batch_run(client, function_id, payload, user_config)
            except Exception as exc:
                # A function registered under another Globus identity is not ours to run.
                if _http_status(exc) not in (403, 404):
                    raise
                function_id = self._function_id(client, fresh=True)
                response = self._batch_run(client, function_id, payload, user_config)
        except Refusal:
            raise
        except Exception as exc:
            raise _refusal(exc, RefusalReason.SUBMIT_FAILED, "submitting the job") from exc
        tasks: Any = (
            cast(dict[str, Any], response).get("tasks") if isinstance(response, dict) else None
        )
        ids: Any = cast(dict[str, Any], tasks).get(function_id) if isinstance(tasks, dict) else None
        if not isinstance(ids, list) or not ids:
            raise Refusal(
                RefusalReason.SUBMIT_FAILED,
                "Globus Compute accepted the batch but returned no task id",
            )
        return str(cast(list[Any], ids)[0])

    def _task(self, task_id: str) -> TaskInfo:
        client = self._sdk_client()
        try:
            from globus_compute_sdk.errors import TaskExecutionFailed
        except ImportError as exc:  # pragma: no cover - the client import already passed
            raise Refusal(RefusalReason.ADAPTER_UNAVAILABLE, INSTALL_HINT) from exc
        try:
            raw: Any = client.get_task(task_id)
        except TaskExecutionFailed as exc:
            return TaskInfo(pending=False, status="failed", error=str(exc))
        except Exception as exc:
            raise _refusal(exc, RefusalReason.UNREACHABLE, "reading the task") from exc
        info = cast(dict[str, Any], raw) if isinstance(raw, dict) else {}
        status = str(info.get("status", "unknown"))
        if info.get("pending", True):
            return TaskInfo(pending=True, status=status)
        result: Any = info.get("result")
        if isinstance(result, dict):
            return TaskInfo(pending=False, status=status, result=cast(dict[str, Any], result))
        reason = info.get("reason") or f"the task ended {status} with no result"
        return TaskInfo(pending=False, status=status, error=str(reason))

    def _endpoint(self) -> EndpointInfo:
        client = self._sdk_client()
        try:
            raw: Any = client.get_endpoint_status(self.endpoint_id)
        except Exception as exc:
            raise _refusal(exc, RefusalReason.UNREACHABLE, "reading the endpoint") from exc
        info = cast(dict[str, Any], raw) if isinstance(raw, dict) else {}
        facts: dict[str, Any] = {}
        try:
            meta: Any = client.get_endpoint_metadata(self.endpoint_id)
        except Exception as exc:
            facts["metadata_error"] = str(exc)
        else:
            if isinstance(meta, dict):
                meta_dict = cast(dict[str, Any], meta)
                schema: Any = meta_dict.get("user_config_schema") or {}
                properties: Any = (
                    cast(dict[str, Any], schema).get("properties")
                    if isinstance(schema, dict)
                    else None
                )
                if isinstance(properties, dict):
                    facts["user_config_keys"] = sorted(cast(dict[str, Any], properties))
                for key in ("endpoint_version", "multi_user", "hostname", "name"):
                    if key in meta_dict:
                        facts[key] = meta_dict[key]
        return EndpointInfo(status=str(info.get("status", "unknown")), facts=facts)

    async def submit(self, payload: dict[str, Any], user_config: dict[str, Any]) -> str:
        return await asyncio.to_thread(self._submit, payload, user_config)

    async def task(self, task_id: str) -> TaskInfo:
        return await asyncio.to_thread(self._task, task_id)

    async def endpoint(self) -> EndpointInfo:
        return await asyncio.to_thread(self._endpoint)
