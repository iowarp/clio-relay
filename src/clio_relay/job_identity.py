"""Owner-session attribution for submitted relay jobs."""

from __future__ import annotations

from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clio_relay.errors import RelayError
from clio_relay.identifiers import DurableRecordId
from clio_relay.models import JobKind

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


class OwnerSessionIdentityError(RelayError):
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
