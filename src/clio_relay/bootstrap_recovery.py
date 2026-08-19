"""State-aware bootstrap transaction forward-recovery (clio-relay#247).

Owner module for the recovery logic that completes a transaction which
crashed past the irreversible boundary, without re-deriving a staged
generation identity that a given transaction mode may never have recorded.

Background: forward recovery historically demanded
``phase_identities.prepared_manifest`` unconditionally (see
``bootstrap_recover_previous_transaction`` in ``bootstrap.py``). Only the
``relay-only``/``component-upgrade`` reconcile path ever wrote that phase,
before activating -- the ``full`` fresh-bootstrap path (which drives its own
journal through ``bootstrap_journal.py``, a dependency-free sibling of this
module) never did, so any full-mode journal that crossed the boundary
without reaching ``committed`` was permanently wedged.

The fix: once a transaction has reached ``migration_started`` or later,
activation is durably complete for *every* mode -- the ACTIVE generation's
own install receipt is ground truth, and recovery no longer needs a staged
manifest at all. Only ``activating``/``activated`` (relay-only/
component-upgrade only; ``full`` mode's own boundary excludes these two
states, see ``BootstrapTransactionJournal.advance``) still need to redo
activation from the recorded staged identity.
"""

from __future__ import annotations

from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
)
from clio_relay.errors import ConfigurationError
from clio_relay.installation import installation_info

# States reachable only once activation itself is durably complete (the
# `current` pointer and install-receipt already swapped in) -- for every
# transaction mode. Recovery from any of these verifies the ACTIVE
# generation instead of re-deriving a staged one.
_POST_ACTIVATION_STATES = frozenset(
    {
        BootstrapTransactionState.MIGRATION_STARTED,
        BootstrapTransactionState.MIGRATED,
        BootstrapTransactionState.STARTING,
        BootstrapTransactionState.SERVICE_VERIFIED,
    }
)

_RETIRE_PROCEDURE = (
    "back up the journal file to '<path>.stale-backup-<UTC timestamp>' next to "
    "it, remove the original, first confirming with `clio-relay installation-info` "
    "that the active generation is healthy and untouched, then re-run "
    "`cluster bootstrap`"
)


def recovery_needs_staged_identity(journal: BootstrapTransactionJournal) -> bool:
    """Return whether forward recovery must re-derive a staged manifest identity.

    False once the generation is durably active (state past ``activated``):
    activation itself is fully complete by then for every mode, so recovery
    reasons from the ACTIVE generation's own receipt instead.
    """
    return journal.recovery_mode == "forward" and journal.state not in _POST_ACTIVATION_STATES


def active_generation_recovery_evidence(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
) -> dict[str, object]:
    """Read the ACTIVE generation's own receipt identity for state-aware recovery.

    Raises ConfigurationError if the active generation pointer, or the
    receipt it names, does not durably prove the desired deployment --
    recovery never completes over an unproven or mismatched deployment.
    """
    lexical_home = Path(str(home or Path.home())).expanduser().absolute()
    current = lexical_home / ".local/share/clio-relay/current"
    if not current.is_symlink():
        raise ConfigurationError(
            "active generation recovery found no managed current generation pointer"
        )
    resolved = current.resolve(strict=True)
    receipt_path = current / "install-receipt.json"
    info = installation_info(receipt_path)
    receipt = info.get("receipt")
    if not isinstance(receipt, dict):
        raise ConfigurationError("active generation omitted its install receipt")
    generation_fingerprint = receipt.get("generation")
    deployment_fingerprint = receipt.get("deployment_fingerprint")
    if (
        info.get("receipt_matches_install") is not True
        or not isinstance(generation_fingerprint, str)
        or generation_fingerprint != desired.fingerprint
        or deployment_fingerprint != desired.fingerprint
    ):
        raise ConfigurationError(
            "active generation receipt does not match this bootstrap transaction"
        )
    return {
        "schema_version": "clio-relay.bootstrap-active-recovery.v1",
        "generation": str(resolved),
        "fingerprint": generation_fingerprint,
        "install_receipt_sha256": info.get("install_receipt_sha256"),
    }


def complete_active_generation_recovery(
    journal: BootstrapTransactionJournal,
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
) -> dict[str, object]:
    """Forward-recover a post-activation journal from the ACTIVE generation.

    Never re-derives a staged manifest identity, and never silently accepts
    a deployment whose active generation does not durably match this
    transaction's own recorded ``prepared_generation``.
    """
    if recovery_needs_staged_identity(journal):
        raise ConfigurationError(
            "active-generation recovery is unavailable before activation completes"
        )
    if not journal.prepared_generation:
        raise ConfigurationError("bootstrap transaction omitted its prepared generation")
    evidence = active_generation_recovery_evidence(desired, home=home)
    if evidence["fingerprint"] != journal.prepared_generation:
        raise ConfigurationError(
            "active generation does not match this bootstrap transaction's prepared generation"
        )
    return evidence


def require_phase_identity(
    journal: BootstrapTransactionJournal,
    phase: str,
    *,
    journal_path: Path,
) -> str:
    """Return one recorded phase identity, or an observation-shaped refusal.

    Names the exact absent key, the journal path, and the documented retire
    procedure -- never a bare lookup failure -- so a journal that predates
    the writer that records ``phase`` (a "prior-build journal", #247) fails
    actionably instead of wedging the deployment.
    """
    identity = journal.phase_identities.get(phase)
    if isinstance(identity, str):
        return identity
    raise ConfigurationError(
        f"bootstrap transaction journal is missing phase_identities.{phase}, "
        f"required to forward-recover a '{journal.mode}' transaction at "
        f"'{journal.state.value}'. Journal: {journal_path}. This journal predates "
        f"{phase} being recorded and cannot self-recover; {_RETIRE_PROCEDURE}."
    )
