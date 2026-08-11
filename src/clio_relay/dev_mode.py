"""First-class DEV MODE: downgrades install/identity/receipt verification to warnings.

Owner directive (clio-relay#211): during development, no sha/identity check
should block iteration -- push a git sha, install it, restart, retest. This
module is the ONE place dev mode is detected and the ONE shape a downgraded
finding takes; every verification call site that participates imports from
here rather than re-deriving the env/cluster-flag logic itself.

``CLIO_RELAY_DEV_MODE=1`` (environment) and/or ``dev_mode=True`` on a
cluster registry entry both enable the downgrade; either is sufficient.

Checks protecting live state or other tenants -- writer proof, the
worker-lifetime lock, storage admission, teardown scoping -- never import
from this module and stay hard regardless of dev mode. Physical target
identity (operator-pinned hostname/ssh-host-key/site-marker) also stays
hard: dev mode relaxes release-integrity ceremony for a trusted git sha
already being iterated on, not which physical machine a session trusts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from clio_relay.errors import ConfigurationError

DEV_MODE_ENV = "CLIO_RELAY_DEV_MODE"

#: Rendered into every status/info surface that ran a downgraded check, so
#: the mode can never be left on silently (clio-relay#211's explicit
#: requirement). Keep this exact text stable -- callers and tests match it.
DEV_MODE_BANNER = (
    "DEV MODE — verification advisory: install/identity/receipt/sha/"
    "contract-digest checks below were downgraded to warnings and did not "
    "block. Never use in production."
)

_TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _env_dev_mode() -> bool:
    value = os.environ.get(DEV_MODE_ENV)
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY_VALUES


def dev_mode_enabled(*, cluster_dev_mode: bool = False) -> bool:
    """Return whether the verification downgrade applies.

    True when ``CLIO_RELAY_DEV_MODE`` is truthy in the environment, OR the
    caller's cluster registry entry has ``dev_mode=True``. Callers that hold
    cluster context thread ``cluster_dev_mode`` in explicitly -- this never
    reaches into the registry itself.
    """
    return bool(cluster_dev_mode) or _env_dev_mode()


@dataclass
class VerificationFindings:
    """Collects would-have-failed checks when dev mode downgrades them.

    In production mode (the default), a verification failure raises
    immediately and nothing is ever recorded here. In dev mode, each check
    that would have raised instead appends its exact production error
    message and verification proceeds; the caller renders ``.warnings``
    into its status/info payload behind :data:`DEV_MODE_BANNER` so the
    downgrade is always visible, never silent.
    """

    warnings: list[str] = field(default_factory=list[str])

    def record(self, message: str) -> None:
        """Record one would-have-failed check instead of letting it raise."""
        self.warnings.append(message)

    def payload(self) -> dict[str, object] | None:
        """Return the status/info fragment to merge in, or None when clean.

        ``None`` when nothing was downgraded (including when dev mode is
        off, since nothing is ever recorded then) -- callers merge this
        conditionally so an unaffected surface stays exactly as it always
        has been, byte for byte.
        """
        if not self.warnings:
            return None
        return {"dev_mode_banner": DEV_MODE_BANNER, "dev_mode_warnings": list(self.warnings)}


def enforce(
    findings: VerificationFindings,
    *,
    dev_mode: bool,
    condition: bool,
    message: str,
) -> None:
    """Require ``condition``; downgrade to a recorded warning only in dev mode.

    The single choke point every downgradable check in the verification
    chain routes through: production behavior (raise) is unchanged, and dev
    mode substitutes "record and keep going" for "raise and stop" -- never
    the reverse, and never silent either way.
    """
    if condition:
        return
    if dev_mode:
        findings.record(message)
        return
    raise ConfigurationError(message)
