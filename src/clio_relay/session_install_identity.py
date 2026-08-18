"""Installation-identity policy for owned-session API startup."""

from __future__ import annotations

from clio_relay.dev_mode import dev_mode_enabled
from clio_relay.errors import ConfigurationError
from clio_relay.installation import InstallReceipt
from clio_relay.session_wire_models import SessionApiReleaseIdentity
from clio_relay.validation_report import SoftwareIdentity


def release_identity_from_receipt(receipt: InstallReceipt) -> SessionApiReleaseIdentity:
    """Project a verified install receipt into the session API identity."""
    artifact_sha256 = receipt.artifact_sha256
    if artifact_sha256 is None:
        if not dev_mode_enabled():  # pragma: no cover - receipt verification rejects this
            raise ConfigurationError("session API installation identity is incomplete")
        artifact_sha256 = "0" * 64
    return SessionApiReleaseIdentity(
        distribution_version=receipt.distribution_version,
        artifact_sha256=artifact_sha256,
        software=SoftwareIdentity.model_validate(receipt.model_dump(mode="python")["software"]),
    )


def release_identity_is_accepted(
    current: SessionApiReleaseIdentity,
    expected: SessionApiReleaseIdentity,
) -> bool:
    """Return whether startup may accept the observed API release identity."""
    return current == expected or dev_mode_enabled()
