"""Transport-mode acceptance: frp-relay, frp-direct, SSH-forward, and worker
deployment verification.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of proving
one configured transport mode actually carries a real HTTP API round trip
(job submit, wait, monitor, artifacts, progress) plus the persistent-worker
deployment check that reads the remote systemd unit's lingering/enabled/
active state. All three transport-probe entry points converge on the same
``_verify_transport_http_api``/``_http_json`` HTTP client, which is why they
share one module instead of three.
"""

from __future__ import annotations

import hashlib
import json
import shlex
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, cast
from uuid import uuid4

from clio_relay import __version__
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.installation import (
    verify_remote_clio_kit_native_execution_component,
    verify_remote_native_jarvis_component,
    verify_remote_worker_info,
)
from clio_relay.job_identity import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
)
from clio_relay.live_acceptance_models import CommandRunner, LiveAcceptanceOptions
from clio_relay.live_acceptance_progress import (
    _assert_progress_adapter,
    _expected_progress_adapter,
    _expected_progress_package,
    _runtime_metadata_facts,
)
from clio_relay.live_acceptance_remote_io import _remote_shell
from clio_relay.transport_probe import (
    run_frp_direct_http_probe,
    run_frp_http_probe,
    run_ssh_forward_http_probe,
)
from clio_relay.validation_report import detect_software_identity


def _verify_transport(
    options: LiveAcceptanceOptions,
    *,
    token: str,
    secret_key: str,
    pipeline_yaml: str,
    expected_progress_adapter: str | None,
    expected_progress_package: str | None,
) -> list[str]:
    run_suffix = uuid4().hex[:12]
    return run_frp_http_probe(
        cluster=options.cluster,
        definition=options.definition,
        frpc_bin=options.transport_frpc_bin,
        token=token,
        secret_key=secret_key,
        local_bind_port=(
            options.definition.live_test.transport_local_bind_port
            if options.transport_local_bind_port is None
            else options.transport_local_bind_port
        ),
        remote_api_port=(
            options.definition.live_test.transport_remote_api_port
            if options.transport_remote_api_port is None
            else options.transport_remote_api_port
        )
        or _unique_transport_port(run_suffix),
        proxy_name=(
            options.transport_proxy_name
            or options.definition.live_test.transport_proxy_name
            or f"relay-http-live-test-{run_suffix}"
        ),
        api_token=options.api_token,
        timeout_seconds=options.timeout_seconds,
        http_check=lambda local_url: _verify_transport_http_api(
            local_url,
            cluster=options.cluster,
            pipeline_yaml=pipeline_yaml,
            api_token=options.api_token,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
        ),
    )


def _verify_cluster_deployment(
    definition: ClusterDefinition,
    *,
    runner: CommandRunner,
    expected_artifact_sha256: str | None,
    expected_install_source: str | None,
) -> list[str]:
    service = f"clio-relay-worker-{definition.name}.service"
    script = (
        'export PATH="$HOME/.local/bin:$PATH"\n'
        'relay_user="${USER:-$(id -un)}"\n'
        'linger="$(loginctl show-user "$relay_user" -p Linger --value 2>/dev/null || true)"\n'
        'test "$linger" = yes || { '
        'echo "persistent worker requires systemd user lingering (Linger=yes)" >&2; exit 78; }\n'
        f'test "$(systemctl --user is-enabled {shlex.quote(service)})" = enabled || {{ '
        f'echo "persistent worker service is not enabled: {shlex.quote(service)}" >&2; '
        "exit 1; }\n"
        f'test "$(systemctl --user is-active {shlex.quote(service)})" = active || {{ '
        f'echo "persistent worker service is not active: {shlex.quote(service)}" >&2; '
        "exit 1; }\n"
        f"clio-relay endpoint worker-info --cluster {shlex.quote(definition.name)}\n"
    )
    raw_info = _remote_shell(definition.ssh_host, script, runner=runner)
    try:
        loaded = json.loads(raw_info)
    except json.JSONDecodeError as exc:
        raise RelayError(f"remote installation info was not valid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise RelayError("remote installation info was not an object")
    info = cast(dict[str, Any], loaded)
    try:
        receipt = verify_remote_worker_info(
            info,
            expected_cluster=definition.name,
            expected_version=__version__,
            expected_software=detect_software_identity(),
            expected_artifact_sha256=expected_artifact_sha256,
            expected_source=expected_install_source,
            require_target_identity=False,
        )
    except ConfigurationError as exc:
        raise RelayError(str(exc)) from exc
    try:
        clio_kit_runtime = verify_remote_clio_kit_native_execution_component(info, receipt)
        native_jarvis_runtime = verify_remote_native_jarvis_component(info, receipt)
    except ConfigurationError as exc:
        raise RelayError(str(exc)) from exc
    software = receipt.software
    return [
        "worker.running=passed",
        "worker.service-enabled=verified",
        "worker.service-persistence=verified",
        f"worker.artifact-version={receipt.distribution_version}",
        f"worker.artifact-sha256={receipt.artifact_sha256 or 'none'}",
        "worker.source-identity="
        f"{software.commit or 'none'}:{software.tag or 'none'}:{software.dirty}",
        f"worker.scheduler-provider={info.get('scheduler_provider')}",
        "worker.components=" + json.dumps(receipt.components, sort_keys=True),
        "worker.component-artifacts="
        + json.dumps(
            {
                name: identity.model_dump(mode="json")
                for name, identity in receipt.component_artifacts.items()
            },
            sort_keys=True,
        ),
        "worker.component-runtime="
        + json.dumps(
            {
                "clio-kit": clio_kit_runtime,
                "jarvis-cd": native_jarvis_runtime,
            },
            sort_keys=True,
        ),
        "worker.component-clio-kit-native-jarvis-contract=passed",
        "worker.component-jarvis-native-execution=passed",
    ]


def _verify_direct_transport(
    options: LiveAcceptanceOptions,
    *,
    token: str,
    secret_key: str,
    allow_stcp_fallback: bool,
    pipeline_yaml: str,
    expected_progress_adapter: str | None,
    expected_progress_package: str | None,
) -> list[str]:
    run_suffix = uuid4().hex[:12]
    return run_frp_direct_http_probe(
        cluster=options.cluster,
        definition=options.definition,
        frpc_bin=options.transport_frpc_bin,
        token=token,
        secret_key=secret_key,
        local_bind_port=(
            options.definition.live_test.transport_local_bind_port
            if options.transport_local_bind_port is None
            else options.transport_local_bind_port
        ),
        remote_api_port=(
            options.definition.live_test.transport_remote_api_port
            if options.transport_remote_api_port is None
            else options.transport_remote_api_port
        )
        or _unique_transport_port(run_suffix),
        proxy_name=(
            options.transport_proxy_name
            or options.definition.live_test.transport_proxy_name
            or f"relay-http-direct-live-test-{run_suffix}"
        ),
        api_token=options.api_token,
        timeout_seconds=options.timeout_seconds,
        allow_stcp_fallback=allow_stcp_fallback,
        http_check=lambda local_url: _verify_transport_http_api(
            local_url,
            cluster=options.cluster,
            pipeline_yaml=pipeline_yaml,
            api_token=options.api_token,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
        ),
    )


def _assert_direct_xtcp_acceptance(lines: list[str]) -> None:
    required = {
        "direct_transport.result=xtcp",
        "transport.proxy_type=xtcp",
        "transport.healthz=ok",
        "transport.http_wait=succeeded",
    }
    missing = required - set(lines)
    if missing:
        raise RelayError(f"direct transport acceptance did not prove XTCP: {sorted(missing)}")


def _verify_ssh_transport(
    options: LiveAcceptanceOptions,
    *,
    pipeline_yaml: str,
) -> list[str]:
    run_suffix = uuid4().hex[:12]
    return run_ssh_forward_http_probe(
        cluster=options.cluster,
        definition=options.definition,
        local_bind_port=options.ssh_transport_local_bind_port or _unique_transport_port(run_suffix),
        remote_api_port=options.ssh_transport_remote_api_port
        or _unique_transport_port(run_suffix[::-1]),
        session_id=options.ssh_transport_session_id or f"relay-ssh-live-test-{run_suffix}",
        api_token=options.api_token,
        timeout_seconds=options.timeout_seconds,
        http_check=lambda local_url, session_id, generation_id: _verify_transport_http_api(
            local_url,
            cluster=options.cluster,
            pipeline_yaml=pipeline_yaml,
            api_token=options.api_token,
            owner_session_id=session_id,
            session_generation_id=generation_id,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            expected_progress_adapter=_expected_progress_adapter(pipeline_yaml),
            expected_progress_package=_expected_progress_package(pipeline_yaml),
        ),
    )


def _unique_transport_port(run_suffix: str) -> int:
    return 20000 + (int(run_suffix[:6], 16) % 20000)


def _verify_transport_http_api(
    local_url: str,
    *,
    cluster: str,
    pipeline_yaml: str,
    api_token: str | None,
    owner_session_id: str | None = None,
    session_generation_id: str | None = None,
    timeout_seconds: float,
    poll_seconds: float,
    expected_progress_adapter: str | None,
    expected_progress_package: str | None,
) -> list[str]:
    run_digest = hashlib.sha256(pipeline_yaml.encode("utf-8")).hexdigest()[:16]
    idempotency_key = f"live-test:http-transport:{cluster}:{run_digest}:{uuid4().hex}"
    submitted = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "POST",
            "/jobs/jarvis",
            api_token=api_token,
            body={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml,
                "idempotency_key": idempotency_key,
            },
            owner_session_id=owner_session_id,
            session_generation_id=session_generation_id,
            timeout_seconds=10,
        ),
    )
    job_id = str(submitted["job_id"])
    _wait_for_transport_http_success(
        local_url,
        job_id,
        api_token=api_token,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    monitor = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/monitor",
            api_token=api_token,
            query={"cursor": "1", "limit": "250"},
            timeout_seconds=10,
        ),
    )
    event_types = {event["event_type"] for event in cast(list[dict[str, Any]], monitor["events"])}
    required_events = {"job.queued", "job.running", "jarvis.started", "job.succeeded"}
    missing_events = required_events - event_types
    if missing_events:
        raise RelayError(f"transport HTTP job missing events: {sorted(missing_events)}")
    stdout = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/logs/stdout",
            api_token=api_token,
            query={"offset": "0", "limit": "65536"},
            timeout_seconds=10,
        ),
    )
    if int(stdout["next_offset"]) <= 0:
        raise RelayError("transport HTTP stdout log was empty")
    artifacts = cast(
        list[dict[str, Any]],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/artifacts",
            api_token=api_token,
            timeout_seconds=10,
        ),
    )
    artifact_kinds = {artifact["kind"] for artifact in artifacts}
    if not {"jarvis_pipeline", "stdout", "stderr", "provenance"}.issubset(artifact_kinds):
        raise RelayError(
            f"transport HTTP artifacts missing required kinds: {sorted(artifact_kinds)}"
        )
    provenance_id = next(
        str(artifact["artifact_id"]) for artifact in artifacts if artifact["kind"] == "provenance"
    )
    provenance = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/artifacts/{provenance_id}/content",
            api_token=api_token,
            timeout_seconds=10,
        ),
    )
    if provenance["artifact"]["artifact_id"] != provenance_id:
        raise RelayError("transport HTTP provenance artifact id mismatch")
    if provenance["encoding"] != "base64" or str(provenance["data"]) == "":
        raise RelayError("transport HTTP provenance artifact was empty")
    runtime_facts: list[str] = []
    runtime_artifact = next(
        (artifact for artifact in artifacts if artifact.get("kind") == "runtime_metadata"),
        None,
    )
    if runtime_artifact is not None:
        runtime_artifact_id = runtime_artifact.get("artifact_id")
        if not isinstance(runtime_artifact_id, str) or not runtime_artifact_id:
            raise RelayError("transport HTTP runtime metadata artifact has no artifact id")
        runtime_payload = cast(
            dict[str, Any],
            _http_json(
                local_url,
                "GET",
                f"/artifacts/{runtime_artifact_id}/content",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        runtime_facts = _runtime_metadata_facts(
            runtime_payload,
            artifact_id=runtime_artifact_id,
            line_prefix="transport.http",
        )
    lines = [
        f"transport.http_job_id={job_id}",
        "transport.http_wait=succeeded",
        "transport.http_events=ok",
        f"transport.http_stdout_bytes={stdout['next_offset']}",
        "transport.http_artifacts=ok",
        "transport.http_provenance=ok",
    ]
    lines.extend(runtime_facts)
    if expected_progress_adapter is not None:
        progress = cast(
            list[dict[str, Any]],
            _http_json(
                local_url,
                "GET",
                f"/jobs/{job_id}/progress",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        _assert_progress_adapter(
            progress,
            expected_progress_adapter,
            job_id=job_id,
            package_name=expected_progress_package,
        )
        lines.append(f"transport.http_progress_adapter={expected_progress_adapter}")
    return lines


def _http_json(
    base_url: str,
    method: str,
    path: str,
    *,
    api_token: str | None,
    owner_session_id: str | None = None,
    session_generation_id: str | None = None,
    body: dict[str, object] | None = None,
    query: dict[str, str] | None = None,
    timeout_seconds: float,
) -> dict[str, Any] | list[dict[str, Any]]:
    if (owner_session_id is None) != (session_generation_id is None):
        raise ValueError("owner session and generation HTTP bindings must be provided together")
    encoded_query = "" if not query else "?" + urllib.parse.urlencode(query)
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        base_url + path + encoded_query,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    if api_token is not None:
        request.add_header("Authorization", f"Bearer {api_token}")
    if owner_session_id is not None and session_generation_id is not None:
        request.add_header(OWNER_SESSION_ID_HEADER, owner_session_id)
        request.add_header(SESSION_GENERATION_ID_HEADER, session_generation_id)
    attempts = 3
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read().decode("utf-8")
            return cast(dict[str, Any] | list[dict[str, Any]], json.loads(payload))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RelayError(f"transport HTTP request failed: {method} {path}: {detail}") from exc
        except (OSError, urllib.error.URLError) as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(min(2.0, 0.5 * attempt))
    assert last_error is not None
    raise RelayError(
        f"transport HTTP request failed: {method} {path}: {last_error}"
    ) from last_error


def _wait_for_transport_http_success(
    local_url: str,
    job_id: str,
    *,
    api_token: str | None,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        job = cast(
            dict[str, Any],
            _http_json(
                local_url,
                "GET",
                f"/jobs/{job_id}",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        if job["state"] == "succeeded":
            return job
        if job["state"] in {"failed", "canceled"}:
            raise RelayError(f"transport HTTP job did not succeed: {job['state']}")
        if time.monotonic() >= deadline:
            raise RelayError(f"transport HTTP job did not reach terminal state: {job_id}")
        time.sleep(poll_seconds)


def _require_transport_secrets(
    *,
    token: str | None,
    secret_key: str | None,
) -> tuple[str, str]:
    if token is None:
        raise ConfigurationError("live transport acceptance requires a frp token")
    if secret_key is None:
        raise ConfigurationError("live transport acceptance requires an stcp secret")
    return token, secret_key
