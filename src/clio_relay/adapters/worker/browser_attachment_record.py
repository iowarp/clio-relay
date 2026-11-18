"""The browser attachment record a gateway session stores in the worker queue.

It moved here from the removed browser gateway so the queue no longer imports it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

BROWSER_ATTACHMENT_RECORD_SCHEMA = "clio-relay.browser-attachment-record.v1"


def parse_utc_timestamp(value: str, field: str) -> datetime:
    """Parse an explicitly UTC ISO-8601 timestamp."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must be UTC")
    return parsed


class BrowserAttachmentRecord(BaseModel):
    """Safe, revocable browser attachment metadata stored in a gateway record."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.browser-attachment-record.v1"] = (
        BROWSER_ATTACHMENT_RECORD_SCHEMA
    )
    attachment_id: str = Field(min_length=1, max_length=256)
    state: Literal["starting", "active", "revoking", "revoked", "failed"]
    issued_at: str
    expires_at: str
    revoked_at: str | None = None
    token_sha256: str
    bind_addr: Literal["127.0.0.1"] = "127.0.0.1"
    bind_port: int = Field(gt=0, le=65_535)
    proxy_process_id: int | None = Field(default=None, gt=0)
    revocation_path: str

    @field_validator("token_sha256")
    @classmethod
    def validate_token_digest(cls, value: str) -> str:
        """Require a canonical persisted capability digest."""
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("token_sha256 must be a canonical SHA-256")
        return value

    @model_validator(mode="after")
    def validate_lifecycle(self) -> BrowserAttachmentRecord:
        """Keep revocation timestamps coherent with the attachment state."""
        parse_utc_timestamp(self.issued_at, "issued_at")
        parse_utc_timestamp(self.expires_at, "expires_at")
        if self.state == "revoked":
            if self.revoked_at is None:
                raise ValueError("revoked browser attachments require revoked_at")
            parse_utc_timestamp(self.revoked_at, "revoked_at")
        elif self.revoked_at is not None:
            raise ValueError("only revoked browser attachments may contain revoked_at")
        return self
