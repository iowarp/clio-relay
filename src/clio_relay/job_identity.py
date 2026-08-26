"""Owner-session attribution for submitted relay jobs."""

from __future__ import annotations

from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clio_relay.dev_mode import dev_mode_enabled
from clio_relay.errors import PublicMessageError, RelayError
from clio_relay.identifiers import DurableRecordId
from clio_relay.models import JobKind, RelayJob

OWNER_SESSION_ID_HEADER: Final = "X-Clio-Relay-Owner-Session-Id"
SESSION_GENERATION_ID_HEADER: Final = "X-Clio-Relay-Session-Generation-Id"
OWNER_SESSION_IDENTITY_ERROR_SCHEMA: Final = "clio-relay.owner-session-identity-error.v1"
IDENTITY_REQUIRED_JOB_KINDS: Final = frozenset(
    {
        JobKind.JARVIS,
        JobKind.REMOTE_AGENT,
    }
)

OwnerSessionIdentityErrorCode = Literal[
    "owner_session_identity_required",
    "owner_session_identity_incomplete",
    "owner_session_identity_invalid",
]


class JobOwnerSessionIdentity(BaseModel):
    """A complete header-derived identity used only for job attribution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    owner_session_id: str = Field(min_length=1, max_length=256)
    owner_session_generation_id: DurableRecordId


class OwnerSessionIdentityError(PublicMessageError, RelayError):
    """A typed owner-session attribution refusal."""

    def __init__(
        self,
        *,
        code: OwnerSessionIdentityErrorCode,
        job_kind: JobKind | None,
        message: str,
    ) -> None:
        super().__init__(message)
        self.detail: dict[str, object] = {
            "schema": OWNER_SESSION_IDENTITY_ERROR_SCHEMA,
            "code": code,
            "job_kind": None if job_kind is None else job_kind.value,
            "required_headers": [
                OWNER_SESSION_ID_HEADER,
                SESSION_GENERATION_ID_HEADER,
            ],
            "message": message,
        }


def parse_job_owner_session_identity(
    owner_session_id: str | None,
    session_generation_id: str | None,
) -> JobOwnerSessionIdentity | None:
    """Parse a complete optional header pair without treating it as authentication."""
    if owner_session_id is None and session_generation_id is None:
        return None
    if owner_session_id is None or session_generation_id is None:
        raise OwnerSessionIdentityError(
            code="owner_session_identity_incomplete",
            job_kind=None,
            message="owner-session identity headers must be supplied together",
        )
    try:
        return JobOwnerSessionIdentity(
            owner_session_id=owner_session_id,
            owner_session_generation_id=session_generation_id,
        )
    except ValidationError as exc:
        raise OwnerSessionIdentityError(
            code="owner_session_identity_invalid",
            job_kind=None,
            message="owner-session identity headers are invalid",
        ) from exc


def require_job_owner_session_identity(
    kind: JobKind,
    identity: JobOwnerSessionIdentity | None,
) -> JobOwnerSessionIdentity | None:
    """Refuse schedulable lanes without attribution and preserve optional identity."""
    if kind in IDENTITY_REQUIRED_JOB_KINDS and identity is None:
        raise OwnerSessionIdentityError(
            code="owner_session_identity_required",
            job_kind=kind,
            message=f"{kind.value} submissions require owner-session identity headers",
        )
    return identity


def job_owned_by_session(
    job: RelayJob,
    *,
    owner_session_id: str | None,
    owner_session_generation_id: str | None,
) -> bool:
    """Return whether an owner session may see one durable job record.

    The one read-time ownership predicate every caller-facing surface that
    resolves a job by SCANNING a shared local core (rather than a direct,
    already-authorized-by-construction lookup) must apply -- promoted out
    of ``RelayApiContext.owns_job`` (the door's own owned-resource
    boundary, ``http_api_context.py``, now a thin wrapper over this
    function) so clio-relay#278's ``resolve_jarvis_run_owner_by_
    execution_id`` can filter scan candidates by the SAME rule before its
    exactly-one-owner check, at the MCP tool and CLI surfaces too, not just
    the door. Without this, a second owner session's job that happens to
    match the same bare execution_id (``jarvis_execution_artifacts.
    _is_jarvis_run`` matches any admitted ``jarvis_run`` spec, trusted or
    legacy) silently 404s the FIRST session's own, legitimately-owned
    artifacts the moment the match count stops being exactly one --
    ownership must gate the candidate set, never run only after resolution
    already succeeded or failed on an unfiltered scan.

    ``owner_session_id is None`` (no owned-session API configured) or dev
    mode being enabled both mean every locally known job is visible, same
    as the door's own long-standing ``owns_job`` semantics.
    """
    if dev_mode_enabled():
        return True
    return owner_session_id is None or (
        job.metadata.get("owner") == "clio-relay"
        and job.metadata.get("owner_session_id") == owner_session_id
        and job.metadata.get("owner_session_generation_id") == owner_session_generation_id
    )
