"""The worker's session identity document.

The worker proves which placed session it is by signing a fresh nonce with the
session owner token. Relay minted that token at placement and keeps it, so
relay checks the signature itself (:mod:`clio_relay.adapters.worker.link`).
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Final

SESSION_IDENTITY_SCHEMA: Final = "clio-relay.session-identity.v1"
OWNER_SESSION_ID_HEADER: Final = "X-Clio-Relay-Owner-Session-Id"
SESSION_GENERATION_ID_HEADER: Final = "X-Clio-Relay-Session-Generation-Id"


def session_identity_document(
    *,
    owner_token: str,
    cluster: str,
    session_id: str,
    generation_id: str,
    nonce: str,
) -> dict[str, str]:
    """Return the domain-separated HMAC identity for one session challenge."""
    if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
        raise ValueError("session identity nonce must be a lowercase 256-bit hexadecimal value")
    message = "\n".join((SESSION_IDENTITY_SCHEMA, cluster, session_id, generation_id, nonce))
    return {
        "schema_version": SESSION_IDENTITY_SCHEMA,
        "cluster": cluster,
        "session_id": session_id,
        "session_generation_id": generation_id,
        "nonce": nonce,
        "hmac_sha256": hmac.new(
            owner_token.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
        ).hexdigest(),
    }
