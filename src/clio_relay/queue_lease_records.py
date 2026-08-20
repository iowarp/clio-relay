"""Pure lease index and capacity record codecs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

from clio_relay import queue_layout as layout
from clio_relay.errors import QueueConflictError
from clio_relay.models import JobKind


def _is_sha256_digest(value: object) -> bool:
    hex_digits = "0123456789abcdef"
    return isinstance(value, str) and len(value) == 64 and all(c in hex_digits for c in value)


@dataclass(frozen=True, slots=True)
class LeaseIndexIdentity:
    """Exact immutable fields used by every operational lease reference."""

    lease_id: str
    job_id: str
    endpoint_id: str
    cluster: str
    job_kind: JobKind
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class LeaseCapacityAggregate:
    """Validated O(1) lease admission counts for one durable epoch."""

    epoch_id: str
    generation: int
    checkpoint_id: str
    global_live_leases: int
    cluster_kind_counts: dict[str, dict[JobKind, int]]
    document_sha256: str


@dataclass(frozen=True, slots=True)
class LeaseCapacityCheckpoint:
    """Independent anchor for one exact lease-capacity aggregate generation."""

    epoch_id: str
    generation: int
    checkpoint_id: str
    aggregate_document_sha256: str
    document_sha256: str


@dataclass(frozen=True, slots=True)
class LeaseCapacityPair:
    """One mutually bound aggregate/checkpoint pair."""

    aggregate: LeaseCapacityAggregate
    checkpoint: LeaseCapacityCheckpoint


def _stable_ref_token(*values: str) -> str:
    encoded = "\x00".join(values).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:32]


def lease_scope_ref_name(identity: LeaseIndexIdentity, *scope: str) -> str:
    """Encode one scope-bound lease reference filename."""
    return lease_scope_ref_name_from_tokens(*lease_reference(identity), *scope)


def lease_scope_ref_name_from_tokens(
    lease_token: str,
    identity_token: str,
    *scope: str,
) -> str:
    """Encode one scope-bound lease reference from precomputed tokens."""
    scope_token = _stable_ref_token(
        "lease-scope",
        lease_token,
        identity_token,
        *scope,
    )[:16]
    return f"{lease_token}.{identity_token}.{scope_token}.ref"


def lease_index_document(identity: LeaseIndexIdentity) -> dict[str, object]:
    """Encode one immutable operational lease-index record."""
    return {
        "schema_version": layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
        "lease_id": identity.lease_id,
        "job_id": identity.job_id,
        "endpoint_id": identity.endpoint_id,
        "cluster": identity.cluster,
        "job_kind": identity.job_kind.value,
        "expires_at": identity.expires_at.isoformat(),
    }


def lease_capacity_aggregate_document(
    aggregate: LeaseCapacityAggregate,
) -> dict[str, object]:
    """Serialize one validated lease-capacity aggregate."""
    return {
        **lease_capacity_aggregate_digest_payload(
            epoch_id=aggregate.epoch_id,
            generation=aggregate.generation,
            checkpoint_id=aggregate.checkpoint_id,
            global_live_leases=aggregate.global_live_leases,
            cluster_kind_counts=aggregate.cluster_kind_counts,
        ),
        "document_sha256": aggregate.document_sha256,
    }


def lease_capacity_aggregate_digest_payload(
    *,
    epoch_id: str,
    generation: int,
    checkpoint_id: str,
    global_live_leases: int,
    cluster_kind_counts: dict[str, dict[JobKind, int]],
) -> dict[str, object]:
    """Encode the digest-bearing fields of a lease-capacity aggregate."""
    serialized_counts = serialized_lease_capacity_counts(cluster_kind_counts)
    return {
        "schema_version": layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
        "epoch_id": epoch_id,
        "generation": generation,
        "checkpoint_id": checkpoint_id,
        "global_live_leases": global_live_leases,
        "cluster_kind_counts": serialized_counts,
    }


def serialized_lease_capacity_counts(
    cluster_kind_counts: dict[str, dict[JobKind, int]],
) -> dict[str, dict[str, int]]:
    """Serialize lease-capacity counts in canonical key order."""
    return {
        cluster_token: {
            kind.value: kind_counts[kind]
            for kind in sorted(kind_counts, key=lambda item: item.value)
        }
        for cluster_token, kind_counts in sorted(cluster_kind_counts.items())
    }


def lease_capacity_checkpoint_document(
    checkpoint: LeaseCapacityCheckpoint,
) -> dict[str, object]:
    """Serialize one validated lease-capacity checkpoint."""
    return {
        **lease_capacity_checkpoint_digest_payload(
            epoch_id=checkpoint.epoch_id,
            generation=checkpoint.generation,
            checkpoint_id=checkpoint.checkpoint_id,
            aggregate_document_sha256=checkpoint.aggregate_document_sha256,
        ),
        "document_sha256": checkpoint.document_sha256,
    }


def lease_capacity_checkpoint_digest_payload(
    *,
    epoch_id: str,
    generation: int,
    checkpoint_id: str,
    aggregate_document_sha256: str,
) -> dict[str, object]:
    """Encode the digest-bearing fields of a lease-capacity checkpoint."""
    return {
        "schema_version": layout.LEASE_CAPACITY_CHECKPOINT_SCHEMA,
        "epoch_id": epoch_id,
        "generation": generation,
        "checkpoint_id": checkpoint_id,
        "aggregate_document_sha256": aggregate_document_sha256,
    }


def new_lease_capacity_pair(
    counts: dict[str, dict[JobKind, int]],
    *,
    epoch_id: str | None = None,
    generation: int = 0,
    checkpoint_id: str | None = None,
) -> LeaseCapacityPair:
    """Construct one mutually bound aggregate/checkpoint pair."""
    normalized = normalize_lease_capacity_counts(counts)
    selected_epoch = epoch_id or uuid4().hex
    selected_checkpoint = checkpoint_id or uuid4().hex
    global_total = sum(sum(by_kind.values()) for by_kind in normalized.values())
    payload = lease_capacity_aggregate_digest_payload(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
    )
    aggregate_digest = canonical_document_sha256(payload)
    aggregate = LeaseCapacityAggregate(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
        document_sha256=aggregate_digest,
    )
    checkpoint_payload = lease_capacity_checkpoint_digest_payload(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        aggregate_document_sha256=aggregate_digest,
    )
    checkpoint = LeaseCapacityCheckpoint(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        aggregate_document_sha256=aggregate_digest,
        document_sha256=canonical_document_sha256(checkpoint_payload),
    )
    return LeaseCapacityPair(aggregate=aggregate, checkpoint=checkpoint)


def normalize_lease_capacity_counts(
    counts: dict[str, dict[JobKind, int]],
) -> dict[str, dict[JobKind, int]]:
    """Validate and normalize sparse lease-capacity counts."""
    normalized: dict[str, dict[JobKind, int]] = {}
    scopes = 0
    total = 0
    for cluster_token, kind_counts in counts.items():
        if not is_short_ref_token(cluster_token):
            raise QueueConflictError("lease capacity aggregate has an invalid cluster scope")
        selected: dict[JobKind, int] = {}
        for kind, count in kind_counts.items():
            if isinstance(count, bool) or count <= 0:
                raise QueueConflictError(
                    "lease capacity aggregate counts must be positive integers"
                )
            if kind in selected:
                raise QueueConflictError("lease capacity aggregate repeats a job kind")
            selected[kind] = count
            scopes += 1
            total += count
        if not selected:
            raise QueueConflictError("lease capacity aggregate contains an empty cluster scope")
        normalized[cluster_token] = selected
    if scopes > layout.MAX_LEASE_CAPACITY_SCOPES:
        limit = layout.MAX_LEASE_CAPACITY_SCOPES
        reason = f"lease capacity aggregate exceeds its nonzero scope bound of {limit}"
        raise QueueConflictError(reason)
    if total > layout.MAX_LIVE_LEASE_RECORDS:
        limit = layout.MAX_LIVE_LEASE_RECORDS
        reason = f"lease capacity aggregate exceeds its live lease bound of {limit}"
        raise QueueConflictError(reason)
    return normalized


def lease_index_identity_from_document(value: object, *, label: str) -> LeaseIndexIdentity:
    """Decode and validate one operational lease-index record."""
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not an object")
    document = cast(dict[str, object], value)
    expires_at_value = document.get("expires_at")
    job_kind_value = document.get("job_kind")
    try:
        if not isinstance(expires_at_value, str) or not isinstance(job_kind_value, str):
            raise ValueError("lease index temporal or kind identity is invalid")
        expires_at = datetime.fromisoformat(expires_at_value)
        job_kind = JobKind(job_kind_value)
    except ValueError as exc:
        raise QueueConflictError(f"{label} has invalid fields") from exc
    lease_id = document.get("lease_id")
    job_id = document.get("job_id")
    endpoint_id = document.get("endpoint_id")
    cluster = document.get("cluster")
    if (
        document.get("schema_version") != layout.LEASE_OPERATIONAL_INDEX_SCHEMA
        or not layout.safe_global_record_id(lease_id)
        or not layout.safe_global_record_id(job_id)
        or not layout.safe_global_record_id(endpoint_id)
        or not isinstance(cluster, str)
        or not cluster
        or expires_at.tzinfo is None
    ):
        raise QueueConflictError(f"{label} identity mismatch")
    return LeaseIndexIdentity(
        lease_id=cast(str, lease_id),
        job_id=cast(str, job_id),
        endpoint_id=cast(str, endpoint_id),
        cluster=cluster,
        job_kind=job_kind,
        expires_at=expires_at,
    )


def lease_capacity_aggregate_from_document(
    value: object,
    *,
    label: str,
) -> LeaseCapacityAggregate:
    """Decode and validate one lease-capacity aggregate document."""
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not an object")
    document = cast(dict[str, object], value)
    expected_fields = {
        "schema_version",
        "epoch_id",
        "generation",
        "checkpoint_id",
        "global_live_leases",
        "cluster_kind_counts",
        "document_sha256",
    }
    if set(document) != expected_fields or document.get("schema_version") != (
        layout.LEASE_CAPACITY_AGGREGATE_SCHEMA
    ):
        raise QueueConflictError(f"{label} has an unsupported schema or fields")
    epoch_id = document.get("epoch_id")
    generation = document.get("generation")
    checkpoint_id = document.get("checkpoint_id")
    global_total = document.get("global_live_leases")
    raw_counts = document.get("cluster_kind_counts")
    digest = document.get("document_sha256")
    if (
        not is_capacity_identity(epoch_id)
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
        or not is_capacity_identity(checkpoint_id)
        or isinstance(global_total, bool)
        or not isinstance(global_total, int)
        or not 0 <= global_total <= layout.MAX_LIVE_LEASE_RECORDS
        or not isinstance(raw_counts, dict)
        or not _is_sha256_digest(digest)
    ):
        raise QueueConflictError(f"{label} has invalid identity or count fields")
    counts: dict[str, dict[JobKind, int]] = {}
    for cluster_token, raw_kind_counts in cast(dict[object, object], raw_counts).items():
        if not isinstance(cluster_token, str) or not isinstance(raw_kind_counts, dict):
            raise QueueConflictError(f"{label} has an invalid cluster scope")
        parsed: dict[JobKind, int] = {}
        for raw_kind, raw_count in cast(dict[object, object], raw_kind_counts).items():
            if not isinstance(raw_kind, str):
                raise QueueConflictError(f"{label} has an invalid job kind")
            try:
                kind = JobKind(raw_kind)
            except ValueError as exc:
                raise QueueConflictError(f"{label} has an invalid job kind") from exc
            if kind.value != raw_kind:
                raise QueueConflictError(f"{label} has a noncanonical job kind")
            if isinstance(raw_count, bool) or not isinstance(raw_count, int):
                raise QueueConflictError(f"{label} has an invalid lease count")
            parsed[kind] = raw_count
        counts[cluster_token] = parsed
    normalized = normalize_lease_capacity_counts(counts)
    observed_total = sum(sum(by_kind.values()) for by_kind in normalized.values())
    if observed_total != global_total:
        raise QueueConflictError(f"{label} global and scoped counts disagree")
    payload = lease_capacity_aggregate_digest_payload(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
    )
    if canonical_document_sha256(payload) != digest:
        raise QueueConflictError(f"{label} checksum mismatch")
    return LeaseCapacityAggregate(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
        document_sha256=cast(str, digest),
    )


def lease_capacity_checkpoint_from_document(
    value: object,
    *,
    label: str,
) -> LeaseCapacityCheckpoint:
    """Decode and validate one lease-capacity checkpoint document."""
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not an object")
    document = cast(dict[str, object], value)
    expected_fields = {
        "schema_version",
        "epoch_id",
        "generation",
        "checkpoint_id",
        "aggregate_document_sha256",
        "document_sha256",
    }
    if set(document) != expected_fields or document.get("schema_version") != (
        layout.LEASE_CAPACITY_CHECKPOINT_SCHEMA
    ):
        raise QueueConflictError(f"{label} has an unsupported schema or fields")
    epoch_id = document.get("epoch_id")
    generation = document.get("generation")
    checkpoint_id = document.get("checkpoint_id")
    aggregate_digest = document.get("aggregate_document_sha256")
    digest = document.get("document_sha256")
    if (
        not is_capacity_identity(epoch_id)
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
        or not is_capacity_identity(checkpoint_id)
        or not _is_sha256_digest(aggregate_digest)
        or not _is_sha256_digest(digest)
    ):
        raise QueueConflictError(f"{label} has invalid identity fields")
    payload = lease_capacity_checkpoint_digest_payload(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        aggregate_document_sha256=cast(str, aggregate_digest),
    )
    if canonical_document_sha256(payload) != digest:
        raise QueueConflictError(f"{label} checksum mismatch")
    return LeaseCapacityCheckpoint(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        aggregate_document_sha256=cast(str, aggregate_digest),
        document_sha256=cast(str, digest),
    )


def validate_lease_capacity_pair(pair: LeaseCapacityPair, *, label: str) -> None:
    """Validate the mutual identity binding of one capacity pair."""
    aggregate = pair.aggregate
    checkpoint = pair.checkpoint
    if (
        checkpoint.epoch_id != aggregate.epoch_id
        or checkpoint.generation != aggregate.generation
        or checkpoint.checkpoint_id != aggregate.checkpoint_id
        or checkpoint.aggregate_document_sha256 != aggregate.document_sha256
    ):
        raise QueueConflictError(f"{label} aggregate and checkpoint disagree")


def lease_capacity_pair_payload(pair: LeaseCapacityPair) -> dict[str, object]:
    """Encode one complete lease-capacity pair."""
    return {
        "aggregate": lease_capacity_aggregate_document(pair.aggregate),
        "checkpoint": lease_capacity_checkpoint_document(pair.checkpoint),
    }


def lease_capacity_pair_from_payload(value: object, *, label: str) -> LeaseCapacityPair:
    """Decode and validate one complete lease-capacity pair."""
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not a lease capacity pair")
    payload = cast(dict[str, object], value)
    if set(payload) != {"aggregate", "checkpoint"}:
        raise QueueConflictError(f"{label} is not a lease capacity pair")
    pair = LeaseCapacityPair(
        aggregate=lease_capacity_aggregate_from_document(
            payload.get("aggregate"),
            label=f"{label} aggregate",
        ),
        checkpoint=lease_capacity_checkpoint_from_document(
            payload.get("checkpoint"),
            label=f"{label} checkpoint",
        ),
    )
    validate_lease_capacity_pair(pair, label=label)
    return pair


def canonical_document_sha256(document: dict[str, object]) -> str:
    """Hash one canonically encoded JSON document."""
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def is_capacity_identity(value: object) -> bool:
    """Return whether a value is a canonical capacity identity."""
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(character in "0123456789abcdef" for character in value)
    )


def is_short_ref_token(value: str) -> bool:
    """Return whether a value is a canonical shortened reference token."""
    return len(value) == 16 and all(character in "0123456789abcdef" for character in value)


def lease_reference_from_scope_ref(name: str, *scope: str) -> tuple[str, str] | None:
    """Decode and validate a scope-bound lease reference filename."""
    parts = name.split(".")
    if len(parts) != 4 or parts[3] != "ref":
        return None
    lease_token, identity_token, _scope_token, _suffix = parts
    if not all(is_short_ref_token(token) for token in parts[:3]):
        return None
    expected = lease_scope_ref_name_from_tokens(
        lease_token,
        identity_token,
        *scope,
    )
    if name != expected:
        return None
    return lease_token, identity_token


def lease_reference(identity: LeaseIndexIdentity) -> tuple[str, str]:
    """Return the lease and identity tokens for one index identity."""
    return lease_index_token(identity.lease_id), lease_identity_token(identity)


def parse_lease_reference_key(value: str) -> tuple[str, str] | None:
    """Decode one canonical lease/identity reference key."""
    parts = value.split(".")
    if len(parts) != 2 or not all(is_short_ref_token(token) for token in parts):
        return None
    return parts[0], parts[1]


def parse_lease_identity_ref_name(name: str) -> tuple[str, str] | None:
    """Decode one canonical lease identity-reference filename."""
    if not name.endswith(".ref"):
        return None
    return parse_lease_reference_key(name[: -len(".ref")])


def lease_index_token(lease_id: str) -> str:
    """Encode the stable token for one lease identifier."""
    return _stable_ref_token("lease", lease_id)[:16]


def lease_job_token(job_id: str) -> str:
    """Encode the stable token for one leased job identifier."""
    return _stable_ref_token("job", job_id)[:16]


def lease_endpoint_token(endpoint_id: str) -> str:
    """Encode the stable token for one lease endpoint identifier."""
    return _stable_ref_token("endpoint", endpoint_id)[:16]


def lease_cluster_token(cluster: str) -> str:
    """Encode the stable token for one lease cluster."""
    return _stable_ref_token("cluster", cluster)[:16]


def lease_expiry_key(value: datetime) -> int:
    """Encode a datetime as a sortable microsecond expiry key."""
    observed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    delta = observed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def lease_expiry_ref_name(identity: LeaseIndexIdentity) -> str:
    """Encode one canonical lease-expiry reference filename."""
    expires_key = lease_expiry_key(identity.expires_at)
    cluster_token = lease_cluster_token(identity.cluster)
    endpoint_token = lease_endpoint_token(identity.endpoint_id)
    job_token = lease_job_token(identity.job_id)
    kind_code = lease_kind_code(identity.job_kind)
    lease_token, identity_token = lease_reference(identity)
    return (
        f"{expires_key:020d}.{cluster_token}.{kind_code}."
        f"{endpoint_token}.{job_token}.{lease_token}.{identity_token}.ref"
    )


def lease_kind_code(job_kind: JobKind) -> str:
    """Encode a job kind for a compact lease reference."""
    return {
        JobKind.JARVIS: "j",
        JobKind.REMOTE_AGENT: "r",
        JobKind.MCP_CALL: "m",
        JobKind.INPUT_INGEST: "i",
    }[job_kind]


def lease_identity_token(identity: LeaseIndexIdentity) -> str:
    """Encode the stable token binding all lease identity fields."""
    return lease_identity_token_from_parts(
        lease_expiry_key(identity.expires_at),
        lease_cluster_token(identity.cluster),
        lease_kind_code(identity.job_kind),
        lease_endpoint_token(identity.endpoint_id),
        lease_job_token(identity.job_id),
        lease_index_token(identity.lease_id),
    )


def lease_identity_token_from_parts(
    expires_key: int,
    cluster_token: str,
    kind_code: str,
    endpoint_token: str,
    job_token: str,
    lease_token: str,
) -> str:
    """Encode a lease identity token from precomputed record parts."""
    return _stable_ref_token(
        "lease-identity-v2",
        f"{expires_key:020d}",
        cluster_token,
        kind_code,
        endpoint_token,
        job_token,
        lease_token,
    )[:16]


def parse_lease_expiry_ref_name(
    name: str,
) -> tuple[int, str, JobKind, str, str, str, str] | None:
    """Decode and authenticate one canonical lease-expiry reference filename."""
    parts = name.split(".")
    if len(parts) != 8 or parts[7] != "ref":
        return None
    (
        expires_raw,
        cluster_token,
        kind_code,
        endpoint_token,
        job_token,
        lease_token,
        identity_token,
        _suffix,
    ) = parts
    try:
        job_kind = {
            "j": JobKind.JARVIS,
            "r": JobKind.REMOTE_AGENT,
            "m": JobKind.MCP_CALL,
            "i": JobKind.INPUT_INGEST,
        }[kind_code]
        expires_key = int(expires_raw)
    except (KeyError, ValueError):
        return None
    if (
        len(expires_raw) != 20
        or not expires_raw.isdigit()
        or expires_key < 0
        or not all(
            is_short_ref_token(token)
            for token in (
                cluster_token,
                endpoint_token,
                job_token,
                lease_token,
                identity_token,
            )
        )
        or identity_token
        != lease_identity_token_from_parts(
            expires_key,
            cluster_token,
            kind_code,
            endpoint_token,
            job_token,
            lease_token,
        )
    ):
        return None
    return (
        expires_key,
        cluster_token,
        job_kind,
        endpoint_token,
        job_token,
        lease_token,
        identity_token,
    )
