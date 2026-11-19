"""Portable durable identifier and filesystem-key contract tests."""

from __future__ import annotations

import re

import pytest
from pydantic import TypeAdapter, ValidationError

from clio_relay.identifiers import (
    DURABLE_RECORD_ID_MAX_BYTES,
    DURABLE_RECORD_ID_PATTERN,
    FILESYSTEM_KEY_PREFIX,
    DurableRecordId,
    durable_record_id_json_schema,
    filesystem_key,
    validate_durable_record_id,
)
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
    TaskTimelineEvent,
)


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
