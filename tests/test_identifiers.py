"""Portable durable identifier and filesystem-key contract tests."""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from clio_relay import core_queue as core_queue_module
from clio_relay import mcp_server as mcp_server_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue, LegacyQueueStateError
from clio_relay.errors import QueueConflictError
from clio_relay.identifiers import (
    DURABLE_RECORD_ID_MAX_BYTES,
    DURABLE_RECORD_ID_PATTERN,
    FILESYSTEM_KEY_PREFIX,
    DurableRecordId,
    durable_record_id_json_schema,
    filesystem_key,
    validate_durable_record_id,
)
from clio_relay.mcp_server import handle_request
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    JarvisRunSpec,
    JobKind,
    Lease,
    MonitorRule,
    ProgressRecord,
    RelayJob,
    RelayTask,
    SchedulerCancelPending,
    TaskTimelineEvent,
)
from clio_relay.session_lifecycle import RemoteSessionStateEvidence, SessionLifecycleReport
from clio_relay.validation_report import CleanupEvidence


@pytest.mark.parametrize(
    "value",
    [
        "a",
        "job_0123456789abcdef",
        "worker-slot-2",
        "a" * DURABLE_RECORD_ID_MAX_BYTES,
    ],
)
def test_durable_record_id_accepts_portable_lowercase_ascii(value: str) -> None:
    """Portable identifiers remain unchanged."""
    assert validate_durable_record_id(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        ".",
        "..",
        "Job_1",
        "job.1",
        "job:1",
        "../job",
        r"..\job",
        "/tmp/job",
        r"C:\temp\job",
        "jöb",
        "job\x00id",
        "a" * (DURABLE_RECORD_ID_MAX_BYTES + 1),
        "con",
        "prn",
        "aux",
        "nul",
        "com1",
        "com9",
        "lpt1",
        "lpt9",
    ],
)
def test_durable_record_id_rejects_nonportable_or_reserved_values(value: str) -> None:
    """Hostile paths and platform-specific device names fail on every OS."""
    with pytest.raises(ValueError):
        validate_durable_record_id(value)


@pytest.mark.parametrize("value", [None, 1, True, object()])
def test_durable_record_id_rejects_non_string_runtime_values(value: object) -> None:
    """Direct helper misuse raises the explicit type contract, never AttributeError."""
    with pytest.raises(TypeError, match="must be a string"):
        validate_durable_record_id(value)


def test_durable_record_id_type_publishes_machine_readable_constraints() -> None:
    """Pydantic and hand-authored APIs expose the same ID contract."""
    generated = TypeAdapter(DurableRecordId).json_schema()
    fragment = durable_record_id_json_schema()

    assert generated["type"] == fragment["type"] == "string"
    assert generated["minLength"] == fragment["minLength"] == 1
    assert generated["maxLength"] == fragment["maxLength"] == 128
    assert generated["pattern"] == fragment["pattern"] == DURABLE_RECORD_ID_PATTERN


def test_filesystem_key_preserves_safe_labels_and_reserves_encoded_namespace() -> None:
    """Readable safe labels remain readable while k2-like labels are encoded."""
    assert filesystem_key("cluster-alpha", domain="cluster") == "cluster-alpha"

    reserved = filesystem_key("k2-operator-label", domain="cluster")
    assert reserved.startswith(FILESYSTEM_KEY_PREFIX)
    assert reserved != "k2-operator-label"
    assert re.fullmatch(r"k2-[0-9a-f]{64}", reserved) is not None


def test_filesystem_key_eliminates_sanitizer_case_and_device_collisions() -> None:
    """Distinct arbitrary labels cannot collapse to a shared path component."""
    values = ["a:b", "a/b", "a_b", "A_B", ".", "..", "con", "CON"]
    keys = [filesystem_key(value, domain="cluster") for value in values]

    assert len(keys) == len(set(keys))
    assert all("/" not in key and "\\" not in key for key in keys)
    assert all(key not in {".", "..", "con"} for key in keys)


def test_filesystem_key_is_deterministic_and_domain_separated() -> None:
    """The same logical label is stable within a domain and distinct across domains."""
    first = filesystem_key("Target:GPU", domain="cluster")
    second = filesystem_key("Target:GPU", domain="cluster")
    owner = filesystem_key("Target:GPU", domain="owner-session")

    assert first == second
    assert first != owner
    assert re.fullmatch(r"k2-[0-9a-f]{64}", first) is not None


@pytest.mark.parametrize("domain", ["", "Cluster", "owner/session", "a" * 65])
def test_filesystem_key_rejects_ambiguous_domains(domain: str) -> None:
    """Domain separators are fixed portable internal labels."""
    with pytest.raises(ValueError):
        filesystem_key("value", domain=domain)


@pytest.mark.parametrize("value", [None, 1, True, object()])
def test_filesystem_key_rejects_non_string_runtime_values(value: object) -> None:
    """Public filesystem-key misuse raises its explicit type contract."""
    with pytest.raises(TypeError, match="must be a string"):
        filesystem_key(value, domain="cluster")


@pytest.mark.parametrize(
    ("model", "identity_field"),
    [
        (ArtifactRef, "artifact_id"),
        (EndpointRegistration, "endpoint_id"),
        (GatewaySession, "session_id"),
        (Lease, "lease_id"),
        (MonitorRule, "rule_id"),
        (ProgressRecord, "progress_id"),
        (RelayJob, "job_id"),
        (RelayTask, "task_id"),
        (SchedulerCancelPending, "job_id"),
    ],
)
def test_every_canonical_scan_model_has_a_filename_identity_contract(
    model: type[BaseModel],
    identity_field: str,
) -> None:
    """Canonical bulk readers cannot silently omit filename/content identity checks."""
    assert (
        core_queue_module._record_identity_field(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            model
        )
        == identity_field
    )


def test_canonical_scan_layouts_bind_filename_to_record_identity(tmp_path: Path) -> None:
    """Operational primary and derived scans use compatible exact filename identities."""
    queue = ClioCoreQueue(tmp_path / "core")
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="endpoint_scan",
            role=EndpointRole.WORKER,
            cluster="Target:GPU",
            hostname="worker",
            pid=1,
        )
    )
    job = queue.submit_job(
        RelayJob(
            job_id="job_scan",
            cluster="Target:GPU",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="scan-layouts",
        )
    )

    fresh, fresh_truncated = queue.scan_fresh_endpoints(
        limit=10,
        fresh_seconds=60,
        cluster="Target:GPU",
    )
    active, active_truncated = queue.scan_active_jobs(limit=10)
    lease = queue.acquire_next_job(endpoint.endpoint_id, cluster="Target:GPU")
    assert lease is not None
    job_leases, leases_truncated = queue.scan_job_leases(job.job_id, limit=10)
    pending = queue.ensure_scheduler_cancel_pending(job.job_id, reason="identity-regression")
    due, due_truncated = queue.scan_due_scheduler_cancellations(
        cluster="Target:GPU",
        limit=10,
    )

    assert fresh_truncated is active_truncated is leases_truncated is due_truncated is False
    assert [item.endpoint_id for item in fresh] == [endpoint.endpoint_id]
    assert [item.job_id for item in active] == [job.job_id]
    assert [item.lease_id for item in job_leases] == [lease.lease_id]
    assert [item.job_id for item in due] == [pending.job_id]
    cluster_token = core_queue_module._stable_ref_token(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "Target:GPU"
    )
    assert (
        queue.root / "scheduler_cancel_pending" / cluster_token / f"{job.job_id}.json"
    ).is_file()


def test_durable_models_reject_unsafe_primary_and_reference_ids() -> None:
    """Durable model construction rejects unsafe IDs before queue I/O."""
    job = RelayJob(
        cluster="operator target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="identifier-contract",
    )
    endpoint = EndpointRegistration(
        role=EndpointRole.WORKER,
        cluster="operator target",
        hostname="worker",
        pid=1,
    )

    constructors = (
        lambda: RelayJob(
            job_id="../../clusters",
            cluster="operator target",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="unsafe",
        ),
        lambda: RelayTask(task_id="task.1", job_id=job.job_id, name="task"),
        lambda: RelayTask(job_id="../job", name="task"),
        lambda: ArtifactRef(
            artifact_id="C:/artifact", job_id=job.job_id, uri="file:///x", kind="x"
        ),
        lambda: ProgressRecord(progress_id="Progress", job_id=job.job_id),
        lambda: MonitorRule(rule_id="con", job_id=job.job_id, pattern="done"),
        lambda: GatewaySession(session_id="gateway/escape", cluster="x", name="gateway"),
        lambda: Cursor(job_id=r"C:\temp\sentinel"),
        lambda: Lease.new(job.job_id, "../endpoint", ttl_seconds=30),
        lambda: TaskTimelineEvent(
            task_id="task_1",
            event_type="artifact",
            label="artifact",
            summary="artifact",
            artifact_refs=["../artifact"],
        ),
    )

    for constructor in constructors:
        with pytest.raises(ValidationError):
            constructor()

    lease = Lease.new(job.job_id, endpoint.endpoint_id, ttl_seconds=30)
    assert lease.job_id == job.job_id
    assert lease.endpoint_id == endpoint.endpoint_id


def test_mcp_schemas_publish_durable_identifier_contract() -> None:
    """Every static MCP durable-ID input advertises the enforced constraints."""
    tools = {
        str(tool["name"]): tool
        for tool in mcp_server_module._all_tool_definitions(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            clusters=[]
        )
    }
    fields = {
        "relay_status": "job_id",
        "relay_cancel": "job_id",
        "relay_observe": "job_id",
        "relay_wait": "job_id",
        "relay_get_job": "job_id",
        "relay_monitor_job": "job_id",
        "relay_record_task_event": "task_id",
        "relay_watch_task_events": "task_id",
        "relay_read_artifact": "artifact_id",
        "relay_create_monitor_rule": "job_id",
        "relay_get_gateway_session": "session_id",
        "relay_update_gateway_session": "session_id",
        "relay_close_gateway_session": "session_id",
    }

    for tool_name, field_name in fields.items():
        schema = tools[tool_name]["inputSchema"]["properties"][field_name]
        assert schema["pattern"] == DURABLE_RECORD_ID_PATTERN
        assert schema["maxLength"] == DURABLE_RECORD_ID_MAX_BYTES
        assert schema["minLength"] == 1
    artifact_items = tools["relay_record_task_event"]["inputSchema"]["properties"]["artifact_refs"][
        "items"
    ]
    assert artifact_items["pattern"] == DURABLE_RECORD_ID_PATTERN


@pytest.mark.parametrize(
    "hostile_job_id",
    [
        "../../sentinel",
        r"..\..\sentinel",
        "/tmp/clio-relay-sentinel",
        r"C:\temp\clio-relay-sentinel",
    ],
)
def test_relay_observe_rejects_hostile_job_ids_before_filesystem_io(
    tmp_path: Path,
    hostile_job_id: str,
) -> None:
    """The user MCP observation path cannot create cursors or touch sentinels."""
    core_dir = tmp_path / "core"
    sentinel = tmp_path / "sentinel.json"
    sentinel.write_text("operator-owned", encoding="utf-8")
    settings = RelaySettings(core_dir=core_dir, spool_dir=tmp_path / "spool")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "relay_observe",
                "arguments": {"job_id": hostile_job_id},
            },
        },
        queue=ClioCoreQueue(core_dir),
        settings=settings,
        profile="user",
    )

    assert response is not None
    assert "durable record ID" in response["error"]["message"]
    assert sentinel.read_text(encoding="utf-8") == "operator-owned"
    assert not core_dir.exists()


def test_generation_and_operation_models_use_durable_identifier_contract(tmp_path: Path) -> None:
    """Lifecycle generations and cleanup operations reject path-like identities."""
    constructors = (
        lambda: RelaySettings(
            core_dir=tmp_path / "core",
            spool_dir=tmp_path / "spool",
            owner_session_id="operator label remains logical",
            owner_session_generation_id="../generation",
        ),
        lambda: RemoteSessionStateEvidence(
            session_generation_id="Generation-1",
            running=True,
            ownership_verified=True,
            observed_at=datetime.now(UTC),
        ),
        lambda: SessionLifecycleReport(
            session_id="operator label remains logical",
            session_generation_id=r"C:\generation",
            mode="detach",
        ),
        lambda: CleanupEvidence(requested=True, operation_id="cleanup.operation"),
    )

    for constructor in constructors:
        with pytest.raises(ValidationError):
            constructor()

    report = SessionLifecycleReport(
        session_id="Operator Label / cluster:alpha",
        session_generation_id="generation-1",
        cleanup_operation_id="cleanup-1",
        mode="teardown",
    )
    assert report.session_id == "Operator Label / cluster:alpha"


_PUBLIC_QUEUE_ID_OPERATIONS: tuple[Callable[[ClioCoreQueue], object], ...] = (
    lambda queue: queue.get_job("../sentinel"),
    lambda queue: queue.get_endpoint(r"..\sentinel"),
    lambda queue: queue.get_task("/absolute/task"),
    lambda queue: queue.get_artifact(r"C:\absolute\artifact"),
    lambda queue: queue.get_gateway_session("Gateway"),
    lambda queue: queue.read_event_page("job.with.dot"),
    lambda queue: queue.latest_job_event("con"),
    lambda queue: queue.list_tasks("../job"),
    lambda queue: queue.list_artifacts("/job"),
    lambda queue: queue.list_progress("Job"),
    lambda queue: queue.list_monitor_rules("job:1"),
)


@pytest.mark.parametrize("operation", _PUBLIC_QUEUE_ID_OPERATIONS)
def test_public_queue_id_boundaries_reject_before_any_io(
    tmp_path: Path,
    operation: Callable[[ClioCoreQueue], object],
) -> None:
    """Every public raw-ID read rejects traversal before initialization or file access."""
    root = tmp_path / "core"
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("operator-owned", encoding="utf-8")
    queue = ClioCoreQueue(root)

    with pytest.raises(ValueError, match="durable record ID"):
        operation(queue)

    assert not root.exists()
    assert sentinel.read_text(encoding="utf-8") == "operator-owned"


def test_model_construct_cannot_bypass_queue_write_id_boundary(tmp_path: Path) -> None:
    """Queue writes defend against callers that bypass Pydantic construction."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    writes = (
        lambda: queue.register_endpoint(
            EndpointRegistration.model_construct(endpoint_id="../endpoint")
        ),
        lambda: queue.submit_job(RelayJob.model_construct(job_id="../job", metadata={})),
        lambda: queue.append_task(RelayTask.model_construct(task_id="../task", job_id="job_1")),
        lambda: queue.append_task_event(
            TaskTimelineEvent.model_construct(task_id="../task", artifact_refs=[])
        ),
        lambda: queue.append_artifact(
            ArtifactRef.model_construct(artifact_id="../artifact", job_id="job_1")
        ),
        lambda: queue.append_progress(
            ProgressRecord.model_construct(progress_id="../progress", job_id="job_1")
        ),
        lambda: queue.create_gateway_session(
            GatewaySession.model_construct(session_id="../gateway")
        ),
        lambda: queue.append_monitor_rule(
            MonitorRule.model_construct(rule_id="../rule", job_id="job_1")
        ),
        lambda: queue.drain_events(Cursor.model_construct(job_id="../job")),
    )

    for write in writes:
        with pytest.raises(ValueError, match="durable record ID"):
            write()
        assert not root.exists()


def test_safe_legacy_state_initializes_without_rewriting_retired_cursor(tmp_path: Path) -> None:
    """Portable v0.9 canonical records remain readable while cursor persistence is retired."""
    root = tmp_path / "core"
    jobs = root / "jobs"
    cursors = root / "cursors"
    tasks = root / "tasks"
    artifacts = root / "artifacts"
    progress_records = root / "progress"
    jobs.mkdir(parents=True)
    cursors.mkdir()
    tasks.mkdir()
    artifacts.mkdir()
    progress_records.mkdir()
    job = RelayJob(
        job_id="job_safe_legacy",
        cluster="Operator Target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="safe-legacy",
    )
    (jobs / f"{job.job_id}.json").write_text(job.model_dump_json(), encoding="utf-8")
    task = RelayTask(task_id="task_safe_legacy", job_id=job.job_id, name="legacy")
    artifact = ArtifactRef(
        artifact_id="artifact_safe_legacy",
        job_id=job.job_id,
        uri="file:///legacy",
        kind="legacy",
    )
    progress = ProgressRecord(
        progress_id="progress_safe_legacy",
        job_id=job.job_id,
        label="legacy",
    )
    (tasks / f"{task.task_id}.json").write_text(task.model_dump_json(), encoding="utf-8")
    (artifacts / f"{artifact.artifact_id}.json").write_text(
        artifact.model_dump_json(), encoding="utf-8"
    )
    (progress_records / f"{progress.progress_id}.json").write_text(
        progress.model_dump_json(), encoding="utf-8"
    )
    cursor_path = cursors / f"{job.job_id}.json"
    cursor_payload = Cursor(job_id=job.job_id, next_seq=7).model_dump_json()
    cursor_path.write_text(cursor_payload, encoding="utf-8")

    queue = ClioCoreQueue(root)
    queue.initialize()
    assert queue.get_job(job.job_id) == job
    assert queue.list_jobs() == [job]
    assert queue.list_tasks(job.job_id) == [task]
    assert queue.list_artifacts(job.job_id) == [artifact]
    assert queue.list_progress(job.job_id) == [progress]
    queue.drain_events(Cursor(job_id=job.job_id, next_seq=1))

    assert cursor_path.read_text(encoding="utf-8") == cursor_payload


@pytest.mark.parametrize("mutation", ["unsafe_filename", "identity_mismatch"])
def test_unsafe_legacy_state_fails_closed_before_initialization_writes(
    tmp_path: Path,
    mutation: str,
) -> None:
    """Unsafe or mismatched v0.9 canonical records produce a machine-readable refusal."""
    root = tmp_path / "core"
    jobs = root / "jobs"
    jobs.mkdir(parents=True)
    job = RelayJob(
        job_id="job_actual",
        cluster="target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="unsafe-legacy",
    )
    filename = "Job_Actual.json" if mutation == "unsafe_filename" else "job_other.json"
    (jobs / filename).write_text(job.model_dump_json(), encoding="utf-8")

    with pytest.raises(LegacyQueueStateError) as raised:
        ClioCoreQueue(root).initialize()

    assert raised.value.report["schema_version"] == "clio-relay.legacy-state-audit.v1"
    assert raised.value.report["family"] == "jobs"
    assert raised.value.report["reason"]
    assert not (root / "migrations").exists()
    assert not (root / "endpoints").exists()


def test_runtime_canonical_read_rejects_filename_content_identity_mismatch(
    tmp_path: Path,
) -> None:
    """A canonical file replaced after initialization cannot impersonate another ID."""
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    first = RelayJob(
        job_id="job_first",
        cluster="target",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="first",
    )
    second = first.model_copy(update={"job_id": "job_second", "idempotency_key": "second"})
    (queue.root / "jobs" / "job_first.json").write_text(
        second.model_dump_json(),
        encoding="utf-8",
    )

    with pytest.raises(QueueConflictError, match="canonical job identity mismatch"):
        queue.get_job("job_first")


def test_legacy_canonical_reparse_family_fails_before_writes(tmp_path: Path) -> None:
    """A linked v0.9 canonical family is refused before migration state is created."""
    root = tmp_path / "core"
    outside = tmp_path / "outside-jobs"
    root.mkdir()
    outside.mkdir()
    marker = outside / "sentinel"
    marker.write_text("operator-owned", encoding="utf-8")
    jobs = root / "jobs"
    _make_directory_link(outside, jobs)
    try:
        with pytest.raises(LegacyQueueStateError) as raised:
            ClioCoreQueue(root).initialize()
        assert raised.value.report["family"] == "jobs"
        assert "owned directory" in raised.value.report["reason"]
        assert marker.read_text(encoding="utf-8") == "operator-owned"
        assert not (root / "migrations").exists()
    finally:
        if jobs.is_symlink():
            jobs.unlink()
        else:
            jobs.rmdir()


def test_runtime_canonical_scan_rejects_reparse_directory(tmp_path: Path) -> None:
    """Runtime family scans reject a symlink or junction swapped in after initialization."""
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = outside / "sentinel"
    marker.write_text("operator-owned", encoding="utf-8")
    jobs = queue.root / "jobs"
    backup = queue.root / "jobs-owned"
    jobs.replace(backup)
    _make_directory_link(outside, jobs)
    try:
        with pytest.raises(QueueConflictError, match="not an owned directory"):
            queue.list_jobs()
        assert marker.read_text(encoding="utf-8") == "operator-owned"
    finally:
        if jobs.is_symlink():
            jobs.unlink()
        else:
            jobs.rmdir()
        backup.replace(jobs)


def _make_directory_link(target: Path, link: Path) -> None:
    """Create a real directory symlink or Windows junction for reparse-point tests."""
    try:
        os.symlink(target, link, target_is_directory=True)
        return
    except OSError:
        if os.name != "nt":
            raise
    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f"could not create test junction: {completed.stderr}")
