"""A refused JARVIS run terminalizes, and a registered Spack path reaches it.

The MCP result documents below reproduce the shapes captured on the isolated
``p5run2`` deployment for durable job ``job_63d173b2bf8a47d2b811860ac26af569``:
``relay-spool/job_63d1.../mcp-result.json`` recorded ``returncode`` 1,
``protocol_error`` ``"tools/call returned isError=true"``, ``timed_out`` false,
``protocol_result.isError`` true and a ``jarvis.error.v1`` structured payload
carrying ``jarvis_run_failed`` with the message
``"Run failed: Spack executable was not found in PATH, SPACK_ROOT/bin,
~/.local/spack, or /opt/spack"``. The durable job stayed ``running`` with
``updated_at`` frozen two seconds after creation.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import endpoint as endpoint_module
from clio_relay import jarvis_run_environment as jarvis_run_environment_module
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_dispatch_failure import jarvis_dispatch_refusal
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.jarvis_run_environment import (
    RELAY_JARVIS_SPACK_COMMAND_ENV,
    jarvis_run_environment_values,
    registered_site_spack_command,
)
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    deterministic_jarvis_execution_id,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact

CLUSTER = "ares-p5run2"
PIPELINE_ID = "copper-elastic-v1"
SPACK_LOAD_SPEC = "/p5gjmq4rseitqanua7mdd2zdnag4v3u2"
SPECIMEN_ERROR_CODE = "jarvis_run_failed"
SPECIMEN_ERROR_MESSAGE = (
    "Run failed: Spack executable was not found in PATH, SPACK_ROOT/bin, "
    "~/.local/spack, or /opt/spack"
)
SITE_SPACK_COMMAND = "/home/operator/.local/share/clio-relay/site-profiles/ares-spack-v1/bin/spack"


def _specimen_error_result_document(
    *,
    command: list[str],
    digest: str,
    server_artifact: dict[str, Any],
    arguments: dict[str, object],
    expected_registered_contract: str | None,
    execution_id: str,
) -> dict[str, object]:
    """Return the errored dispatch document captured from the live specimen."""
    structured: dict[str, object] = {
        "schema_version": "jarvis.error.v1",
        "error": {
            "code": SPECIMEN_ERROR_CODE,
            "execution_id": execution_id,
            "message": SPECIMEN_ERROR_MESSAGE,
            "pipeline_id": PIPELINE_ID,
        },
    }
    return {
        "server": command[0],
        "server_args": command[1:],
        "expected_server_artifact_digest": digest,
        "expected_registered_contract": expected_registered_contract,
        "expected_jarvis_cd_lock_binding": (
            None
            if expected_registered_contract is not None
            else endpoint_module.jarvis_cd_lock_binding_expectation()
        ),
        "observed_server_artifact_digest": digest,
        "server_artifact": server_artifact,
        "operation": "tools/call",
        "tool": "jarvis_run",
        "arguments": arguments,
        "env_from": {},
        "protocol_result": {
            "content": [{"type": "text", "text": json.dumps(structured, sort_keys=True)}],
            "isError": True,
        },
        "structured_result": structured,
        "protocol_error": "tools/call returned isError=true",
        "protocol_version": "2024-11-05",
        "result_validation": None,
        "returncode": 1,
        "timed_out": False,
        "duration_seconds": 5.603851318359375,
    }


def _successful_result_document(
    *,
    command: list[str],
    digest: str,
    server_artifact: dict[str, Any],
    arguments: dict[str, object],
    expected_registered_contract: str | None,
    execution_id: str,
) -> dict[str, object]:
    """Return the handle-first success document a completed run persists."""
    handle: dict[str, object] = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": execution_id,
        "pipeline_id": PIPELINE_ID,
        "mode": "direct",
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "cluster": None,
    }
    record: dict[str, object] = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": execution_id,
        "pipeline_id": PIPELINE_ID,
        "pipeline_name": PIPELINE_ID,
        "mode": "direct",
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "cluster": None,
        "state": "completed",
        "submitted": False,
        "terminal": True,
        "created_at": "2026-08-07T06:11:44Z",
        "updated_at": "2026-08-07T06:11:49Z",
        "return_code": 0,
        "error": None,
        "metadata": {},
    }
    progress: dict[str, object] = {
        "schema_version": "jarvis.execution.progress.v1",
        "execution_id": execution_id,
        "pipeline_id": PIPELINE_ID,
        "execution_state": "completed",
        "terminal": True,
        "packages": [],
    }
    structured: dict[str, object] = {
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
        "runtime_metadata": {
            "schema_version": "jarvis.runtime.v1",
            "source": "jarvis_mcp",
            "execution_id": execution_id,
            "pipeline_id": PIPELINE_ID,
            "mode": "direct",
            "scheduler_provider": None,
            "scheduler_native_id": None,
            "cluster": None,
            "scheduler_type": None,
            "scheduler_job_id": None,
            "scheduler_phase": None,
            "script_path": None,
            "hostfile_path": None,
            "output_path": f"/runs/{PIPELINE_ID}/stdout.log",
            "error_path": f"/runs/{PIPELINE_ID}/stderr.log",
            "package_provenance": [
                {
                    "pkg_id": "copper-elastic-step1",
                    "pkg_type": "builtin.lammps",
                    "global_id": "builtin.lammps.copper-elastic-step1",
                    "config_path": f"/runs/{PIPELINE_ID}/copper-elastic-step1.yaml",
                }
            ],
            "terminal": {
                "state": "completed",
                "terminal": True,
                "returncode": 0,
                "reason": None,
                "started_at": "2026-08-07T06:11:44Z",
                "finished_at": "2026-08-07T06:11:49Z",
            },
            "details": {
                "execution_owner": "jarvis_cd.execution_record",
                "submit": None,
                "wait": True,
                "environment": {
                    "schema_version": "jarvis.environment.v1",
                    "spack_specs": [SPACK_LOAD_SPEC],
                },
                "execution_handle": handle,
                "execution_record": record,
                "scheduler_submission": None,
            },
        },
    }
    return {
        "server": command[0],
        "server_args": command[1:],
        "expected_server_artifact_digest": digest,
        "expected_registered_contract": expected_registered_contract,
        "expected_jarvis_cd_lock_binding": (
            None
            if expected_registered_contract is not None
            else endpoint_module.jarvis_cd_lock_binding_expectation()
        ),
        "observed_server_artifact_digest": digest,
        "server_artifact": server_artifact,
        "operation": "tools/call",
        "tool": "jarvis_run",
        "arguments": arguments,
        "env_from": {},
        "protocol_result": {"structuredContent": structured},
        "structured_result": structured,
        "protocol_error": None,
        "protocol_version": "2024-11-05",
        "result_validation": None,
        "returncode": 0,
        "timed_out": False,
    }


class _DispatchProvider(JarvisCdProvider):
    """Run the endpoint MCP transport and persist one prepared result document."""

    def __init__(
        self,
        *,
        document: dict[str, object] | None,
        returncode: int,
    ) -> None:
        super().__init__(jarvis_bin="jarvis")
        self._document = document
        self._returncode = returncode
        self.environments: list[dict[str, str]] = []

    def run_command_streaming(
        self,
        command: list[str],
        *,
        process_label: str = "JARVIS-CD",
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        credential_payload: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del command, credential_payload, on_stderr, should_cancel, on_poll
        del timeout_seconds, on_timeout
        assert process_label == "endpoint MCP operation"
        assert cwd is not None
        if env is not None:
            self.environments.append(dict(env))
        if on_start is not None:
            on_start(4242)
        if on_stdout is not None:
            on_stdout("relay mcp transport\n")
        if self._document is not None:
            (cwd / "mcp-result.json").write_text(
                json.dumps(self._document, sort_keys=True),
                encoding="utf-8",
            )
        return subprocess.CompletedProcess(["endpoint-mcp-runner"], self._returncode, "", "")


def _submit_registered_run(
    queue: ClioCoreQueue,
    *,
    command: list[str],
    digest: str,
    idempotency_key: str,
) -> RelayJob:
    """Submit the registered handle-first ``jarvis_run`` the specimen recorded."""
    job_id = "job_63d173b2bf8a47d2b811860ac26af569"
    execution_id = deterministic_jarvis_execution_id(
        cluster=CLUSTER,
        idempotency_key=idempotency_key,
        job_id=job_id,
    )
    return queue.submit_job(
        RelayJob(
            job_id=job_id,
            cluster=CLUSTER,
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=command[0],
                server_args=command[1:],
                expected_server_artifact_digest=digest,
                expected_registered_contract="clio-kit-jarvis-user-v3.6",
                tool="jarvis_run",
                arguments={
                    "pipeline_id": PIPELINE_ID,
                    "spack_specs": [SPACK_LOAD_SPEC],
                    "execution_id": execution_id,
                },
                timeout_seconds=14_400,
            ),
            idempotency_key=idempotency_key,
        )
    )


def _worker(
    settings: RelaySettings,
    queue: ClioCoreQueue,
    provider: _DispatchProvider,
) -> EndpointWorker:
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster=CLUSTER,
        queue=queue,
        provider=provider,
    )
    worker.register()
    return worker


def test_errored_jarvis_run_terminalizes_with_its_typed_reason(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The specimen's errored dispatch fails the durable job with its reason."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_artifact = {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio-kit.whl",
    }
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = _submit_registered_run(
        queue,
        command=command,
        digest=digest,
        idempotency_key="copper-elastic-v1-run-001",
    )
    spec = cast(McpCallSpec, job.spec)
    provider = _DispatchProvider(
        document=_specimen_error_result_document(
            command=command,
            digest=digest,
            server_artifact=server_artifact,
            arguments=spec.arguments,
            expected_registered_contract=spec.expected_registered_contract,
            execution_id=cast(str, spec.arguments["execution_id"]),
        ),
        returncode=1,
    )

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    durable_job = queue.get_job(job.job_id)
    assert durable_job.state is JobState.FAILED
    assert durable_job.last_error == f"{SPECIMEN_ERROR_CODE}: {SPECIMEN_ERROR_MESSAGE}"
    assert durable_job.updated_at > durable_job.created_at
    task = queue.list_tasks(job.job_id)[0]
    assert task.state is JobState.FAILED
    refusal = task.metadata["jarvis_dispatch_refusal"]
    assert isinstance(refusal, dict)
    typed = cast(dict[str, object], refusal)
    assert typed["schema_version"] == "clio-relay.jarvis-dispatch-refusal.v1"
    assert typed["code"] == SPECIMEN_ERROR_CODE
    assert typed["message"] == SPECIMEN_ERROR_MESSAGE
    assert typed["pipeline_id"] == PIPELINE_ID
    assert typed["execution_id"] == spec.arguments["execution_id"]
    recovery = task.metadata["jarvis_execution_recovery"]
    assert isinstance(recovery, dict)
    intent = cast(dict[str, object], recovery)
    assert intent["state"] == "resolved"
    assert intent["resolution"] == "dispatch_refusal"
    assert intent["attempts"] == 0
    events, _cursor = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    event_types = [event.event_type for event in events]
    assert "jarvis.dispatch_refused" in event_types
    assert "jarvis.execution_recovery_started" not in event_types
    assert "jarvis.execution_reconciliation_deferred" not in event_types
    refused = next(event for event in events if event.event_type == "jarvis.dispatch_refused")
    assert refused.payload["code"] == SPECIMEN_ERROR_CODE
    assert refused.payload["message"] == SPECIMEN_ERROR_MESSAGE


def test_refusal_outranks_a_clean_transport_exit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The run's own answer decides the outcome, not the transport's exit code."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_artifact = {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio-kit.whl",
    }
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = _submit_registered_run(
        queue,
        command=command,
        digest=digest,
        idempotency_key="copper-elastic-v1-run-006",
    )
    spec = cast(McpCallSpec, job.spec)
    provider = _DispatchProvider(
        document=_specimen_error_result_document(
            command=command,
            digest=digest,
            server_artifact=server_artifact,
            arguments=spec.arguments,
            expected_registered_contract=spec.expected_registered_contract,
            execution_id=cast(str, spec.arguments["execution_id"]),
        ),
        returncode=0,
    )

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert result.state is JobState.FAILED
    assert result.last_error == f"{SPECIMEN_ERROR_CODE}: {SPECIMEN_ERROR_MESSAGE}"


def test_restart_cleanup_settles_a_run_left_stuck_by_an_earlier_build(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A job already stuck behind a failed recovery query settles on the next pass.

    This reproduces the live specimen's durable state: the errored dispatch is in
    the spool, the recovery intent is still pending, and the job never left
    ``running``. Restart cleanup must read the recorded answer instead of
    querying JARVIS again for an execution that was never created.
    """
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_artifact = {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio-kit.whl",
    }
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = _submit_registered_run(
        queue,
        command=command,
        digest=digest,
        idempotency_key="copper-elastic-v1-run-007",
    )
    spec = cast(McpCallSpec, job.spec)
    provider = _DispatchProvider(
        document=_specimen_error_result_document(
            command=command,
            digest=digest,
            server_artifact=server_artifact,
            arguments=spec.arguments,
            expected_registered_contract=spec.expected_registered_contract,
            execution_id=cast(str, spec.arguments["execution_id"]),
        ),
        returncode=1,
    )
    worker = _worker(settings, queue, provider)

    def _blind_to_refusals(_document: object) -> None:
        return None

    def _failed_recovery_query(*_args: object, **_kwargs: object) -> bool:
        raise endpoint_module.SchedulerSubmissionUnresolvedError(
            "artifact-pinned JARVIS execution recovery result was not trusted"
        )

    monkeypatch.setattr(endpoint_module, "jarvis_dispatch_refusal", _blind_to_refusals)
    monkeypatch.setattr(
        endpoint_module.EndpointWorker,
        "_recover_jarvis_execution",
        _failed_recovery_query,
    )

    stuck = worker.run_once()

    assert stuck is not None
    assert stuck.state is JobState.RUNNING
    stuck_task = queue.list_tasks(job.job_id)[0]
    stuck_intent = cast(dict[str, object], stuck_task.metadata["jarvis_execution_recovery"])
    assert stuck_intent["state"] == "pending"

    monkeypatch.undo()
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)

    assert worker.run_once() is None

    settled = queue.get_job(job.job_id)
    assert settled.state is JobState.FAILED
    assert settled.last_error == f"{SPECIMEN_ERROR_CODE}: {SPECIMEN_ERROR_MESSAGE}"
    settled_task = queue.get_task(stuck_task.task_id)
    assert settled_task.state is JobState.FAILED
    settled_intent = cast(dict[str, object], settled_task.metadata["jarvis_execution_recovery"])
    assert settled_intent["state"] == "resolved"
    assert settled_intent["resolution"] == "dispatch_refusal"
    events, _cursor = queue.drain_events(Cursor(job_id=job.job_id), limit=300)
    assert "jarvis.dispatch_refused" in {event.event_type for event in events}


def test_successful_jarvis_run_still_terminalizes_as_succeeded(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A dispatch that returned its execution handle keeps succeeding."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_artifact = {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio-kit.whl",
    }
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = _submit_registered_run(
        queue,
        command=command,
        digest=digest,
        idempotency_key="copper-elastic-v1-run-002",
    )
    spec = cast(McpCallSpec, job.spec)
    provider = _DispatchProvider(
        document=_successful_result_document(
            command=command,
            digest=digest,
            server_artifact=server_artifact,
            arguments=spec.arguments,
            expected_registered_contract=spec.expected_registered_contract,
            execution_id=cast(str, spec.arguments["execution_id"]),
        ),
        returncode=0,
    )

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    task = queue.list_tasks(job.job_id)[0]
    assert "jarvis_dispatch_refusal" not in task.metadata
    recovery = cast(dict[str, object], task.metadata["jarvis_execution_recovery"])
    assert recovery["resolution"] == "dispatch_result"


def test_run_without_any_result_stays_nonterminal_for_recovery(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A lost run response still defers rather than inventing a terminal answer."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_artifact = {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio-kit.whl",
    }
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = _submit_registered_run(
        queue,
        command=command,
        digest=digest,
        idempotency_key="copper-elastic-v1-run-003",
    )
    provider = _DispatchProvider(document=None, returncode=1)

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert result.state is JobState.RUNNING
    task = queue.list_tasks(job.job_id)[0]
    recovery = cast(dict[str, object], task.metadata["jarvis_execution_recovery"])
    assert recovery["state"] == "pending"
    assert "jarvis_dispatch_refusal" not in task.metadata
    events, _cursor = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    event_types = {event.event_type for event in events}
    assert "jarvis.dispatch_refused" not in event_types
    assert "jarvis.execution_reconciliation_deferred" in event_types


def test_timed_out_dispatch_is_never_read_as_an_answered_refusal() -> None:
    """A lost response carries no answer even when the transport recorded an error."""
    document: dict[str, object] = {
        "returncode": 1,
        "timed_out": True,
        "protocol_error": "tools/call timed out",
        "protocol_result": {"isError": True},
        "structured_result": None,
    }

    assert jarvis_dispatch_refusal(document) is None


def test_untyped_tool_error_still_reports_a_typed_refusal() -> None:
    """An isError answer without a JARVIS payload still reaches the caller typed."""
    document: dict[str, object] = {
        "returncode": 1,
        "timed_out": False,
        "protocol_error": "tools/call returned isError=true",
        "protocol_result": {"isError": True},
        "structured_result": {"schema_version": "jarvis.unexpected.v9"},
    }

    refusal = jarvis_dispatch_refusal(document)

    assert refusal is not None
    assert refusal.code == "jarvis_tool_error"
    assert refusal.message == "tools/call returned isError=true"
    assert refusal.payload_schema_version is None


def test_successful_protocol_result_is_not_a_refusal() -> None:
    """A dispatch that answered without an error is never recorded as refused."""
    document: dict[str, object] = {
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "protocol_result": {"isError": False},
    }

    assert jarvis_dispatch_refusal(document) is None


def _registry_with(tmp_path: Path, *, spack_executable: str | None) -> Path:
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(
        clusters={
            CLUSTER: ClusterDefinition(
                name=CLUSTER,
                ssh_host="localhost",
                spack_executable=spack_executable,
            )
        }
    ).save(registry_path)
    return registry_path


def test_registered_spack_executable_reaches_the_run_environment(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A declared cluster Spack executable is composed for the JARVIS child."""
    registry_path = _registry_with(tmp_path, spack_executable=SITE_SPACK_COMMAND)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))

    resolved = registered_site_spack_command(CLUSTER)

    assert resolved == SITE_SPACK_COMMAND
    assert jarvis_run_environment_values(resolved) == {
        RELAY_JARVIS_SPACK_COMMAND_ENV: SITE_SPACK_COMMAND
    }


def test_home_anchored_spack_executable_expands_on_the_executing_host(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A ``$HOME``-anchored registration resolves against the worker's account."""
    registry_path = _registry_with(
        tmp_path,
        spack_executable="$HOME/.local/share/clio-relay/site-profiles/ares-spack-v1/bin/spack",
    )
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))

    def _fake_expanduser(_value: str) -> str:
        return "/home/operator"

    monkeypatch.setattr(jarvis_run_environment_module.os.path, "expanduser", _fake_expanduser)

    assert registered_site_spack_command(CLUSTER) == SITE_SPACK_COMMAND


def test_cluster_without_a_declaration_composes_nothing(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """An undeclared Spack executable never invents a default."""
    registry_path = _registry_with(tmp_path, spack_executable=None)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))

    assert registered_site_spack_command(CLUSTER) is None
    assert jarvis_run_environment_values(None) == {}
    assert registered_site_spack_command("another-cluster") is None


def test_unreadable_registry_refuses_instead_of_downgrading(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A configured registry that cannot be read is a refusal, not a quiet skip."""
    registry_path = tmp_path / "clusters.json"
    registry_path.write_text("{not json", encoding="utf-8")
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))

    with pytest.raises(ConfigurationError, match="cluster registry could not be read"):
        registered_site_spack_command(CLUSTER)


def test_worker_publishes_the_registered_spack_command_to_the_mcp_runner(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The dispatched runner environment carries the cluster's Spack identity."""
    registry_path = _registry_with(tmp_path, spack_executable=SITE_SPACK_COMMAND)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_artifact = {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio-kit.whl",
    }
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = _submit_registered_run(
        queue,
        command=command,
        digest=digest,
        idempotency_key="copper-elastic-v1-run-004",
    )
    spec = cast(McpCallSpec, job.spec)
    provider = _DispatchProvider(
        document=_successful_result_document(
            command=command,
            digest=digest,
            server_artifact=server_artifact,
            arguments=spec.arguments,
            expected_registered_contract=spec.expected_registered_contract,
            execution_id=cast(str, spec.arguments["execution_id"]),
        ),
        returncode=0,
    )

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert result.state is JobState.SUCCEEDED
    assert provider.environments
    assert provider.environments[0][RELAY_JARVIS_SPACK_COMMAND_ENV] == SITE_SPACK_COMMAND
    events, _cursor = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    composed = [event for event in events if event.event_type == "jarvis.run_environment_composed"]
    assert len(composed) == 1
    assert composed[0].payload["spack_command"] == SITE_SPACK_COMMAND


def test_worker_without_a_declaration_leaves_the_run_environment_unchanged(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """No declaration keeps the previously composed runner environment exactly."""
    registry_path = _registry_with(tmp_path, spack_executable=None)
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    command = ["clio-kit", "mcp-server", "jarvis"]
    server_artifact = {
        **verified_jarvis_server_artifact(),
        "install_spec": "/releases/clio-kit.whl",
    }
    digest = remote_mcp_server_artifact_digest(server_artifact)
    monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", lambda: command)
    job = _submit_registered_run(
        queue,
        command=command,
        digest=digest,
        idempotency_key="copper-elastic-v1-run-005",
    )
    spec = cast(McpCallSpec, job.spec)
    provider = _DispatchProvider(
        document=_successful_result_document(
            command=command,
            digest=digest,
            server_artifact=server_artifact,
            arguments=spec.arguments,
            expected_registered_contract=spec.expected_registered_contract,
            execution_id=cast(str, spec.arguments["execution_id"]),
        ),
        returncode=0,
    )

    result = _worker(settings, queue, provider).run_once()

    assert result is not None
    assert provider.environments
    assert RELAY_JARVIS_SPACK_COMMAND_ENV not in provider.environments[0]
    events, _cursor = queue.drain_events(Cursor(job_id=job.job_id), limit=200)
    assert not [event for event in events if event.event_type == "jarvis.run_environment_composed"]
