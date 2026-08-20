"""Failing-first tests for clio_relay.bootstrap_recovery (clio-relay#247).

The exact live defect: a `full`-mode bootstrap transaction journal at
`service_verified` (active generation healthy, commit record never written)
could not forward-recover, because `bootstrap.py`'s forward recovery
unconditionally demanded `phase_identities.prepared_manifest` -- a key ONLY
relay-only/component-upgrade reconcile ever wrote (before activating).
Recovery is now state-aware: past activation, the ACTIVE generation's own
receipt is proof enough, and a journal genuinely missing a required phase
identity for an earlier state gets a typed, observation-shaped refusal.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import clio_relay.bootstrap_recovery as bootstrap_recovery
from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
)
from clio_relay.bootstrap_recovery import (
    complete_active_generation_recovery,
    recovery_needs_staged_identity,
    require_phase_identity,
)
from clio_relay.errors import ConfigurationError


def _desired() -> BootstrapDesiredState:
    return BootstrapDesiredState(
        cluster="ares-p5run2",
        core_dir="~/.local/share/clio-relay/core",
        spool_dir="~/.local/share/clio-relay/spool",
        worker_service="clio-relay-endpoint-ares-p5run2.service",
        relay_install_spec="clio-relay==1.6.6",
        relay_artifact_sha256="a" * 64,
        relay_source_identity=f"wheel:sha256:{'a' * 64}",
        frp_version="0.69.1",
        frpc_sha256="b" * 64,
        frps_sha256="c" * 64,
        uv_version="0.11.28",
        uv_sha256="d" * 64,
        jarvis_util_commit="commit",
        jarvis_cd_version="1.4.4",
        jarvis_cd_wheel_url="https://example.test/jarvis.whl",
        jarvis_cd_wheel_sha256="e" * 64,
        clio_kit_install_spec="https://example.test/clio-kit.whl",
        clio_kit_version="2.3.1",
        clio_kit_artifact_sha256="f" * 64,
        agent_adapter="exec",
    )


def _service_verified_journal(desired: BootstrapDesiredState) -> BootstrapTransactionJournal:
    """A full-mode journal shaped like the live ares incident: reached
    `service_verified` with ONLY the `locked` phase identity a prior-build
    journal ever recorded -- `full` mode never wrote `prepared_manifest`.
    """
    journal = BootstrapTransactionJournal(
        invocation_id="bootstrap-1",
        desired_fingerprint=desired.fingerprint,
        mode="full",
        phase_identities={"locked": desired.fingerprint},
    )
    journal.prepared_generation = desired.fingerprint
    for state in (
        BootstrapTransactionState.INSPECTED,
        BootstrapTransactionState.PREPARING,
        BootstrapTransactionState.PREPARED,
        BootstrapTransactionState.FENCING,
        BootstrapTransactionState.FENCED,
        BootstrapTransactionState.ACTIVATING,
        BootstrapTransactionState.ACTIVATED,
        BootstrapTransactionState.MIGRATION_STARTED,
        BootstrapTransactionState.MIGRATED,
        BootstrapTransactionState.SERVICE_VERIFIED,
    ):
        journal.advance(state)
    return journal


def _write_active_generation(
    home: Path,
    desired: BootstrapDesiredState,
    *,
    monkeypatch: pytest.MonkeyPatch,
    generation_fingerprint: str | None = None,
) -> None:
    """Create the active generation and expose `current` as a symlink to it.

    Simulates the symlink via monkeypatch (matching the suite's
    `_simulate_file_symlink` idiom) rather than `Path.symlink_to`, which
    requires an elevated privilege this box does not grant.
    """
    fingerprint = generation_fingerprint or desired.fingerprint
    generation = home / ".local/share/clio-relay/generations" / fingerprint
    generation.mkdir(parents=True)
    current = home / ".local/share/clio-relay/current"
    receipt = generation / "install-receipt.json"
    receipt.write_text(
        json.dumps({"generation": fingerprint, "deployment_fingerprint": fingerprint}),
        encoding="utf-8",
    )

    original_is_symlink = Path.is_symlink
    original_resolve = Path.resolve

    def simulated_is_symlink(path: Path) -> bool:
        if path == current:
            return True
        return original_is_symlink(path)

    def simulated_resolve(path: Path, strict: bool = False) -> Path:
        if path == current:
            return original_resolve(generation, strict=strict)
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)
    monkeypatch.setattr(Path, "resolve", simulated_resolve)


def _mock_installation_info(monkeypatch: pytest.MonkeyPatch, receipt: dict[str, Any]) -> None:
    """Stand in for the real `installation_info`, which verifies live component
    runtimes this test never installs -- only the receipt-matching CONTRACT
    `active_generation_recovery_evidence` reads is under test here."""

    def fake_installation_info(_path: Path) -> dict[str, Any]:
        return {
            "receipt": receipt,
            "receipt_matches_install": True,
            "install_receipt_sha256": "0" * 64,
        }

    monkeypatch.setattr(bootstrap_recovery, "installation_info", fake_installation_info)


def test_service_verified_journal_only_locked_phase_needs_no_staged_identity() -> None:
    """RED for the historical bug: the exact journal shape observed live on
    ares (only `locked` ever recorded) must NOT need `prepared_manifest`."""
    desired = _desired()
    journal = _service_verified_journal(desired)
    assert journal.phase_identities == {"locked": desired.fingerprint}
    assert journal.irreversible_boundary is True
    assert journal.recovery_mode == "forward"
    assert recovery_needs_staged_identity(journal) is False


def test_service_verified_journal_forward_recovers_to_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GREEN: state-aware recovery completes from the ACTIVE generation's own
    receipt -- no `phase_identities.prepared_manifest` lookup, no ValueError,
    the exact wedge `bootstrap.py:4654` hit live on ares (#247)."""
    desired = _desired()
    journal = _service_verified_journal(desired)
    _write_active_generation(tmp_path, desired, monkeypatch=monkeypatch)
    _mock_installation_info(
        monkeypatch,
        {"generation": desired.fingerprint, "deployment_fingerprint": desired.fingerprint},
    )

    evidence = complete_active_generation_recovery(journal, desired, home=tmp_path)
    assert evidence["fingerprint"] == desired.fingerprint
    assert evidence["generation"] == str(
        (tmp_path / ".local/share/clio-relay/generations" / desired.fingerprint).resolve()
    )

    journal.complete_recovery()
    assert journal.state == BootstrapTransactionState.RECOVERED


def test_active_generation_mismatch_never_silently_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A journal whose prepared_generation does not match the active receipt
    must refuse -- never silently mark a mismatched deployment recovered."""
    desired = _desired()
    journal = _service_verified_journal(desired)
    _write_active_generation(
        tmp_path, desired, monkeypatch=monkeypatch, generation_fingerprint="b" * 64
    )
    _mock_installation_info(
        monkeypatch, {"generation": "b" * 64, "deployment_fingerprint": "b" * 64}
    )

    with pytest.raises(ConfigurationError, match="does not match"):
        complete_active_generation_recovery(journal, desired, home=tmp_path)


def test_missing_prepared_manifest_gets_actionable_refusal(tmp_path: Path) -> None:
    """Defect #247 part (c): a prior-build journal missing a required phase
    identity gets a typed, observation-shaped refusal naming the key, the
    journal path, and the retire procedure -- never a bare lookup failure."""
    desired = _desired()
    journal = BootstrapTransactionJournal(
        invocation_id="bootstrap-1",
        desired_fingerprint=desired.fingerprint,
        mode="relay-only",
        phase_identities={"locked": desired.fingerprint},
    )
    journal.prepared_generation = desired.fingerprint
    for state in (
        BootstrapTransactionState.INSPECTED,
        BootstrapTransactionState.PREPARING,
        BootstrapTransactionState.PREPARED,
        BootstrapTransactionState.FENCING,
        BootstrapTransactionState.FENCED,
        BootstrapTransactionState.ACTIVATING,
    ):
        journal.advance(state)
    assert journal.recovery_mode == "forward"
    assert recovery_needs_staged_identity(journal) is True

    journal_path = tmp_path / "bootstrap-transaction.json"
    with pytest.raises(ConfigurationError) as excinfo:
        require_phase_identity(journal, "prepared_manifest", journal_path=journal_path)
    message = str(excinfo.value)
    assert "phase_identities.prepared_manifest" in message
    assert str(journal_path) in message
    assert "stale-backup" in message


def test_require_phase_identity_returns_recorded_value() -> None:
    """The non-refusal path: an existing phase identity is returned as-is."""
    desired = _desired()
    journal = BootstrapTransactionJournal(
        invocation_id="bootstrap-1",
        desired_fingerprint=desired.fingerprint,
        mode="relay-only",
        phase_identities={"locked": desired.fingerprint, "prepared_manifest": "e" * 64},
    )
    assert (
        require_phase_identity(journal, "prepared_manifest", journal_path=Path("unused"))
        == "e" * 64
    )
