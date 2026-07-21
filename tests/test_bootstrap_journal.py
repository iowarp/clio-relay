"""Filesystem failure-injection tests for fresh bootstrap crash recovery."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import pytest

import clio_relay.bootstrap_journal as journal_module
from clio_relay.bootstrap_journal import (
    BootstrapJournalError,
    advance_journal,
    copy_owned_file,
    create_journal,
    create_owned_directory,
    create_owned_symlink,
    discard_full_transaction,
    load_journal,
    record_owned_path,
    record_phase,
    recovery_plan,
)
from clio_relay.bootstrap_reconcile import BootstrapTransactionJournal

_REVERSIBLE_BOUNDARIES = (
    "downloads",
    "execution_environment",
    "jarvis_initialization",
    "resource_graph",
    "managed_repository",
    "generation_preparation",
    "generation_activation",
)


def _identity(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _owned_paths(home: Path) -> dict[str, dict[str, str]]:
    private = home / ".local/share/clio-relay"
    return {
        name: {"path": str(private / "fresh-owned" / name), "kind": "directory"}
        for name in _REVERSIBLE_BOUNDARIES
    }


@pytest.mark.parametrize("failed_after", _REVERSIBLE_BOUNDARIES)
@pytest.mark.skipif(os.name == "nt", reason="full bootstrap recovery is POSIX-only")
def test_fresh_failure_injection_discards_owned_paths_and_reruns(
    tmp_path: Path,
    failed_after: str,
) -> None:
    """Every pre-queue crash boundary removes only absent-before transaction paths."""
    journal_path = tmp_path / ".local/share/clio-relay/bootstrap-transaction.json"
    owned = _owned_paths(tmp_path)
    operator_path = tmp_path / ".local/share/clio-relay/operator-retained"
    operator_path.mkdir(parents=True)
    journal_path.parent.chmod(0o700)
    (operator_path / "evidence.txt").write_text("operator\n", encoding="utf-8")

    create_journal(
        journal_path,
        invocation_id="failed-run",
        desired_fingerprint="a" * 64,
        mode="full",
        owned_paths=owned,
        service_name=None,
        service_was_active=None,
        service_was_enabled=None,
    )
    advance_journal(journal_path, "inspected")
    advance_journal(journal_path, "preparing")

    boundary_index = _REVERSIBLE_BOUNDARIES.index(failed_after)
    for phase in _REVERSIBLE_BOUNDARIES[: boundary_index + 1]:
        path = Path(owned[phase]["path"])
        path.mkdir(parents=True)
        (path / "partial-state").write_text(phase, encoding="utf-8")
        record_owned_path(journal_path, phase)
        record_phase(journal_path, phase, _identity(phase))
    if boundary_index >= _REVERSIBLE_BOUNDARIES.index("generation_preparation"):
        advance_journal(journal_path, "prepared", prepared_generation="a" * 64)
        advance_journal(journal_path, "fencing")
        advance_journal(journal_path, "fenced")
    if boundary_index >= _REVERSIBLE_BOUNDARIES.index("generation_activation"):
        advance_journal(journal_path, "activating")
        advance_journal(journal_path, "activated")

    assert recovery_plan(journal_path)["recovery_mode"] == "discard"
    recovered = discard_full_transaction(journal_path, home=tmp_path)

    assert recovered["state"] == "recovered"
    assert recovered["recovered_from"] in {"preparing", "fenced", "activated"}
    assert all(not Path(item["path"]).exists() for item in owned.values())
    assert (operator_path / "evidence.txt").read_text(encoding="utf-8") == "operator\n"

    rerun = create_journal(
        journal_path,
        invocation_id="rerun",
        desired_fingerprint="a" * 64,
        mode="full",
        owned_paths=owned,
        service_name=None,
        service_was_active=None,
        service_was_enabled=None,
    )
    assert rerun["state"] == "locked"
    assert rerun["invocation_id"] == "rerun"


def test_fresh_queue_boundary_retains_generation_for_forward_recovery(tmp_path: Path) -> None:
    """Queue migration is irreversible, so recovery cannot discard activated paths."""
    journal_path = tmp_path / ".local/share/clio-relay/bootstrap-transaction.json"
    owned = _owned_paths(tmp_path)
    create_journal(
        journal_path,
        invocation_id="queue-run",
        desired_fingerprint="b" * 64,
        mode="full",
        owned_paths=owned,
        service_name="clio-relay-endpoint.service",
        service_was_active=False,
        service_was_enabled=False,
    )
    advance_journal(journal_path, "inspected")
    advance_journal(journal_path, "preparing")
    for phase, item in owned.items():
        Path(item["path"]).mkdir(parents=True)
        record_phase(journal_path, phase, _identity(phase))
    for state in ("prepared", "fencing", "fenced", "activating", "activated"):
        advance_journal(
            journal_path,
            state,
            prepared_generation="b" * 64 if state == "prepared" else None,
        )
    advance_journal(journal_path, "migration_started")

    assert recovery_plan(journal_path)["recovery_mode"] == "forward"
    with pytest.raises(BootstrapJournalError, match="not safely discardable"):
        discard_full_transaction(journal_path, home=tmp_path)
    assert all(Path(item["path"]).is_dir() for item in owned.values())


def test_fresh_journal_refuses_to_adopt_preexisting_operator_path(tmp_path: Path) -> None:
    """An ownership manifest can contain only paths proven absent before mutation."""
    operator_path = tmp_path / ".local/share/clio-relay/operator-path"
    operator_path.mkdir(parents=True)
    owned = {"operator": {"path": str(operator_path), "kind": "directory"}}

    with pytest.raises(BootstrapJournalError, match="already exists"):
        create_journal(
            tmp_path / ".local/share/clio-relay/bootstrap-transaction.json",
            invocation_id="unsafe-run",
            desired_fingerprint="c" * 64,
            mode="full",
            owned_paths=owned,
            service_name=None,
            service_was_active=None,
            service_was_enabled=None,
        )

    assert operator_path.is_dir()


def test_dependency_free_journal_loads_through_reconcile_model(tmp_path: Path) -> None:
    """The embedded helper and installed Pydantic model share one exact schema."""
    journal_path = tmp_path / ".local/share/clio-relay/bootstrap-transaction.json"
    created = create_journal(
        journal_path,
        invocation_id="compatible-run",
        desired_fingerprint="d" * 64,
        mode="full",
        owned_paths={},
        service_name=None,
        service_was_active=None,
        service_was_enabled=None,
    )

    loaded = BootstrapTransactionJournal.load(journal_path)

    assert loaded.invocation_id == created["invocation_id"]
    assert loaded.desired_fingerprint == created["desired_fingerprint"]
    assert loaded.state.value == created["state"]
    assert loaded.owned_paths == {}


def test_hardlinked_journal_is_rejected(tmp_path: Path) -> None:
    """A second journal link invalidates the owner-private recovery authority."""
    journal_path = tmp_path / ".local/share/clio-relay/bootstrap-transaction.json"
    create_journal(
        journal_path,
        invocation_id="linked-run",
        desired_fingerprint="e" * 64,
        mode="full",
        owned_paths={},
        service_name=None,
        service_was_active=None,
        service_was_enabled=None,
    )
    os.link(journal_path, journal_path.with_name("journal-alias.json"))

    with pytest.raises(BootstrapJournalError, match="owner-private"):
        load_journal(journal_path)


@pytest.mark.skipif(os.name == "nt", reason="descriptor topology is POSIX-only")
def test_preexisting_private_real_journal_parent_is_supported(tmp_path: Path) -> None:
    """A legitimate existing private directory topology remains supported."""
    parent = tmp_path / ".local/share/clio-relay"
    parent.mkdir(parents=True, mode=0o700)
    parent.chmod(0o700)
    journal_path = parent / "bootstrap-transaction.json"

    created = create_journal(
        journal_path,
        invocation_id="existing-parent",
        desired_fingerprint="f" * 64,
        mode="full",
        owned_paths={},
        service_name=None,
        service_was_active=None,
        service_was_enabled=None,
    )

    assert created["state"] == "locked"


@pytest.mark.skipif(os.name == "nt", reason="descriptor topology is POSIX-only")
def test_journal_parent_swap_during_create_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Publication through a pinned parent cannot bless a replacement topology."""
    parent = tmp_path / ".local/share/clio-relay"
    parent.mkdir(parents=True, mode=0o700)
    parent.chmod(0o700)
    relocated = parent.with_name("clio-relay-relocated")
    outside = tmp_path / "outside"
    outside.mkdir()
    original = journal_module._atomic_json_at

    def swapped_atomic(
        parent_descriptor: int | None,
        pinned_parent: Path,
        name: str,
        value: dict[str, Any],
        *,
        expected_identity: tuple[int, int] | None,
    ) -> None:
        parent.rename(relocated)
        parent.symlink_to(outside, target_is_directory=True)
        original(
            parent_descriptor,
            pinned_parent,
            name,
            value,
            expected_identity=expected_identity,
        )

    monkeypatch.setattr(journal_module, "_atomic_json_at", swapped_atomic)

    with pytest.raises(BootstrapJournalError, match="parent identity changed"):
        create_journal(
            parent / "bootstrap-transaction.json",
            invocation_id="parent-swap",
            desired_fingerprint="1" * 64,
            mode="full",
            owned_paths={},
            service_name=None,
            service_was_active=None,
            service_was_enabled=None,
        )

    assert not (outside / "bootstrap-transaction.json").exists()
    assert (relocated / "bootstrap-transaction.json").is_file()


def _reversible_owned_journal(
    home: Path,
    target: Path,
    *,
    kind: str,
) -> Path:
    journal_path = home / ".local/share/clio-relay/bootstrap-transaction.json"
    create_journal(
        journal_path,
        invocation_id="owned-run",
        desired_fingerprint="2" * 64,
        mode="full",
        owned_paths={"target": {"path": str(target), "kind": kind}},
        service_name=None,
        service_was_active=None,
        service_was_enabled=None,
    )
    advance_journal(journal_path, "inspected")
    advance_journal(journal_path, "preparing")
    return journal_path


@pytest.mark.skipif(os.name == "nt", reason="full bootstrap recovery is POSIX-only")
def test_parent_swap_after_journal_open_never_reaches_replacement_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pinned journal must remain bound to the same pinned-home topology."""
    target = tmp_path / ".local/share/clio-relay/owned"
    journal_path = _reversible_owned_journal(tmp_path, target, kind="directory")
    target.mkdir()
    record_owned_path(journal_path, "target")
    original_local = tmp_path / ".local"
    relocated_local = tmp_path / ".local-relocated"
    outside = tmp_path / "outside"
    replacement_target = outside / ".local/share/clio-relay/owned"
    replacement_target.mkdir(parents=True)
    sentinel = replacement_target / "operator.txt"
    sentinel.write_text("operator\n", encoding="utf-8")
    original = journal_module._require_current_owner
    swapped = False

    def swap_after_home_open(descriptor: int, description: str) -> None:
        nonlocal swapped
        original(descriptor, description)
        if not swapped:
            original_local.rename(relocated_local)
            original_local.symlink_to(outside / ".local", target_is_directory=True)
            swapped = True

    monkeypatch.setattr(journal_module, "_require_current_owner", swap_after_home_open)

    with pytest.raises(BootstrapJournalError, match="journal parent"):
        discard_full_transaction(journal_path, home=tmp_path)

    assert sentinel.read_text(encoding="utf-8") == "operator\n"
    assert (relocated_local / "share/clio-relay/owned").is_dir()


@pytest.mark.skipif(os.name == "nt", reason="full bootstrap recovery is POSIX-only")
def test_target_swap_to_symlink_is_rejected_without_following(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A directory replaced by a symlink is retained and never traversed."""
    target = tmp_path / ".local/share/clio-relay/owned"
    journal_path = _reversible_owned_journal(tmp_path, target, kind="directory")
    target.mkdir()
    record_owned_path(journal_path, "target")
    displaced = target.with_name("owned-relay-displaced")
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "operator.txt"
    sentinel.write_text("operator\n", encoding="utf-8")
    original = journal_module._discard_owned_entry_at

    def swap_target(
        parent_descriptor: int,
        name: str,
        *,
        kind: str,
        expected_identity: dict[str, Any] | None,
        display: Path,
    ) -> None:
        target.rename(displaced)
        target.symlink_to(outside, target_is_directory=True)
        original(
            parent_descriptor,
            name,
            kind=kind,
            expected_identity=expected_identity,
            display=display,
        )

    monkeypatch.setattr(journal_module, "_discard_owned_entry_at", swap_target)

    with pytest.raises(BootstrapJournalError, match="identity changed"):
        discard_full_transaction(journal_path, home=tmp_path)

    assert target.is_symlink()
    assert sentinel.read_text(encoding="utf-8") == "operator\n"


@pytest.mark.skipif(os.name == "nt", reason="full bootstrap recovery is POSIX-only")
@pytest.mark.parametrize("kind", ("directory", "file", "symlink"))
def test_operator_replacement_at_owned_name_is_retained(
    tmp_path: Path,
    kind: str,
) -> None:
    """Recovery refuses a same-kind object that is not the relay-created identity."""
    target = tmp_path / ".local/share/clio-relay/owned"
    journal_path = _reversible_owned_journal(tmp_path, target, kind=kind)
    target.parent.mkdir(parents=True, exist_ok=True)
    if kind == "directory":
        target.mkdir()
    elif kind == "file":
        target.write_text("relay\n", encoding="utf-8")
    else:
        target.symlink_to(tmp_path / "relay-target")
    record_owned_path(journal_path, "target")
    if kind == "directory":
        target.rename(target.with_name("relay-owned-displaced"))
        target.mkdir()
        (target / "operator.txt").write_text("operator\n", encoding="utf-8")
    else:
        target.unlink()
        if kind == "file":
            target.write_text("operator\n", encoding="utf-8")
        else:
            target.symlink_to(tmp_path / "operator-target")

    with pytest.raises(BootstrapJournalError, match="identity changed"):
        discard_full_transaction(journal_path, home=tmp_path)

    assert target.exists() or target.is_symlink()


@pytest.mark.skipif(os.name == "nt", reason="full bootstrap recovery is POSIX-only")
def test_nested_symlink_and_hardlink_cleanup_preserves_external_objects(tmp_path: Path) -> None:
    """Recursive cleanup unlinks entries but never follows links outside its root."""
    target = tmp_path / ".local/share/clio-relay/owned"
    journal_path = _reversible_owned_journal(tmp_path, target, kind="directory")
    target.mkdir()
    record_owned_path(journal_path, "target")
    outside_directory = tmp_path / "outside-directory"
    outside_directory.mkdir()
    directory_sentinel = outside_directory / "sentinel.txt"
    directory_sentinel.write_text("directory\n", encoding="utf-8")
    outside_file = tmp_path / "outside-file.txt"
    outside_file.write_text("hardlink\n", encoding="utf-8")
    (target / "directory-link").symlink_to(outside_directory, target_is_directory=True)
    os.link(outside_file, target / "file-hardlink")

    recovered = discard_full_transaction(journal_path, home=tmp_path)

    assert recovered["state"] == "recovered"
    assert directory_sentinel.read_text(encoding="utf-8") == "directory\n"
    assert outside_file.read_text(encoding="utf-8") == "hardlink\n"


@pytest.mark.skipif(os.name == "nt", reason="full bootstrap recovery is POSIX-only")
def test_interrupted_discard_is_idempotently_resumed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash after one unlink leaves a nonterminal journal and a safe rerun."""
    target = tmp_path / ".local/share/clio-relay/owned"
    journal_path = _reversible_owned_journal(tmp_path, target, kind="directory")
    target.mkdir()
    (target / "one").write_text("one", encoding="utf-8")
    (target / "two").write_text("two", encoding="utf-8")
    record_owned_path(journal_path, "target")
    original = journal_module._unlink_entry_at
    interrupted = False

    def interrupt_after_unlink(
        parent_descriptor: int,
        name: str,
        expected: os.stat_result,
        *,
        display: Path,
    ) -> None:
        nonlocal interrupted
        original(parent_descriptor, name, expected, display=display)
        if not interrupted:
            interrupted = True
            raise RuntimeError("injected interruption")

    monkeypatch.setattr(journal_module, "_unlink_entry_at", interrupt_after_unlink)
    with pytest.raises(RuntimeError, match="injected interruption"):
        discard_full_transaction(journal_path, home=tmp_path)
    assert load_journal(journal_path)["state"] == "preparing"

    monkeypatch.setattr(journal_module, "_unlink_entry_at", original)
    recovered = discard_full_transaction(journal_path, home=tmp_path)

    assert recovered["state"] == "recovered"
    assert not target.exists()


@pytest.mark.skipif(os.name == "nt", reason="owned path publication is POSIX-only")
@pytest.mark.parametrize("kind", ("directory", "file", "symlink"))
def test_owned_publication_never_claims_a_racing_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """Exclusive creation detects a swap before its identity reaches the journal."""
    target = tmp_path / ".local/share/clio-relay/owned"
    journal_path = _reversible_owned_journal(tmp_path, target, kind=kind)
    source = tmp_path / "source"
    source.write_text("relay\n", encoding="utf-8")
    displaced = target.with_name("relay-created-displaced")
    original = journal_module._owned_identity_at
    swapped = False

    def swap_before_claim(
        parent_descriptor: int,
        name: str,
        details: os.stat_result,
        *,
        display: Path,
    ) -> dict[str, Any]:
        nonlocal swapped
        if display == target and not swapped:
            target.rename(displaced)
            if kind == "directory":
                target.mkdir()
                (target / "operator.txt").write_text("operator\n", encoding="utf-8")
            elif kind == "file":
                target.write_text("operator\n", encoding="utf-8")
            else:
                target.symlink_to(tmp_path / "operator-target")
            swapped = True
        return original(parent_descriptor, name, details, display=display)

    monkeypatch.setattr(journal_module, "_owned_identity_at", swap_before_claim)

    with pytest.raises(BootstrapJournalError, match="changed"):
        if kind == "directory":
            create_owned_directory(journal_path, "target")
        elif kind == "file":
            copy_owned_file(journal_path, "target", source, mode=0o600)
        else:
            create_owned_symlink(journal_path, "target", str(tmp_path / "relay-target"))

    entry = load_journal(journal_path)["owned_paths"]["target"]
    assert entry["identity"] is None
    assert target.exists() or target.is_symlink()
