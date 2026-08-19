"""Failing-first tests for clio_relay.bootstrap_jarvis_staging (clio-relay#254).

The live defect: a full-mode `cluster bootstrap` reconcile over an
already-bootstrapped host (`~/.local/share/clio-relay/jarvis-venv` exists,
built by an earlier successful full bootstrap) hard-refused with "full
component reconcile requires a staged generation" -- a precondition
`cluster bootstrap` exposed no way to satisfy. The relay could never
redeploy itself. The fix stages the replacement environment at a path the
transaction owns and promotes it with one atomic pathname exchange, so the
live environment is never absent or half-cleared at any observable point,
including under a sabotage-shaped interrupt between stage-complete and the
exchange.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.bootstrap_jarvis_staging import (
    jarvis_venv_staging_plan,
    promote_staged_jarvis_venv,
    retired_jarvis_venv_path,
    staged_jarvis_venv_path,
)
from clio_relay.errors import ConfigurationError


def _relay_dir(home: Path) -> Path:
    directory = home / ".local/share/clio-relay"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def test_virgin_host_has_no_staging_plan(tmp_path: Path) -> None:
    """No managed jarvis-venv exists yet: the virgin path is unchanged (None)."""
    assert jarvis_venv_staging_plan(home=tmp_path, invocation_id="inv-1") is None


def test_existing_jarvis_venv_produces_a_staged_plan_instead_of_a_refusal(
    tmp_path: Path,
) -> None:
    """The defect's exact acceptance bar: a full-mode reconcile over an
    existing managed jarvis-venv produces the STAGED plan, not a refusal."""
    live = _relay_dir(tmp_path) / "jarvis-venv"
    live.mkdir()
    (live / "bin").mkdir()
    (live / "bin" / "python").write_bytes(b"")

    plan = jarvis_venv_staging_plan(home=tmp_path, invocation_id="inv-1")

    assert plan is not None
    assert plan["schema_version"] == "clio-relay.jarvis-venv-staging-plan.v1"
    assert plan["live"] == str(live)
    assert plan["staged"] == str(staged_jarvis_venv_path(home=tmp_path, invocation_id="inv-1"))
    # The plan never proposes clearing the live environment directly.
    assert live.is_dir()
    assert list(live.iterdir())


def test_existing_jarvis_venv_symlink_refuses_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A `jarvis-venv` that is not one owned directory is a typed refusal, not
    a silent adopt-or-clear. Simulated via monkeypatch (matching the suite's
    `_simulate_file_symlink` idiom) rather than a real symlink, which
    requires an elevated privilege this box does not grant."""
    relay_dir = _relay_dir(tmp_path)
    target = relay_dir / "elsewhere"
    target.mkdir()
    live = relay_dir / "jarvis-venv"

    original_exists = Path.exists
    original_is_symlink = Path.is_symlink

    def simulated_exists(path: Path) -> bool:
        if path == live:
            return True
        return original_exists(path)

    def simulated_is_symlink(path: Path) -> bool:
        if path == live:
            return True
        return original_is_symlink(path)

    monkeypatch.setattr(Path, "exists", simulated_exists)
    monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)

    with pytest.raises(ConfigurationError, match="not one owned directory"):
        jarvis_venv_staging_plan(home=tmp_path, invocation_id="inv-1")


def _build_environment(path: Path, *, marker: str) -> None:
    path.mkdir(parents=True)
    (path / "bin").mkdir()
    (path / "bin" / "python").write_text(marker, encoding="utf-8")


def test_promote_swaps_live_and_staged_then_retires_the_old_copy(tmp_path: Path) -> None:
    """The core mechanism: one atomic exchange promotes staged to live, then
    the vacated pathname (holding the OLD content) is renamed to retired --
    never deleted."""
    invocation_id = "inv-1"
    live = _relay_dir(tmp_path) / "jarvis-venv"
    staged = staged_jarvis_venv_path(home=tmp_path, invocation_id=invocation_id)
    _build_environment(live, marker="old")
    _build_environment(staged, marker="new")
    staged_identity = (staged.lstat().st_dev, staged.lstat().st_ino)

    result = promote_staged_jarvis_venv(
        home=tmp_path,
        invocation_id=invocation_id,
        retired_at="20260819T060000Z",
        staged_identity=staged_identity,
    )

    assert live.is_dir()
    assert (live / "bin" / "python").read_text(encoding="utf-8") == "new"
    assert not staged.exists()
    retired = retired_jarvis_venv_path(home=tmp_path, retired_at="20260819T060000Z")
    assert retired.is_dir()
    assert (retired / "bin" / "python").read_text(encoding="utf-8") == "old"
    assert result["live"] == str(live)
    assert result["retired"] == str(retired)


def test_sabotage_interrupt_before_exchange_leaves_live_untouched_and_recovers(
    tmp_path: Path,
) -> None:
    """Sabotage-shaped acceptance (#254): interrupt BETWEEN stage-complete and
    the exchange -- i.e. crash before `promote_staged_jarvis_venv` ever runs.
    The live environment must be untouched, and a later recovery call must
    still converge (complete the promotion forward)."""
    invocation_id = "inv-1"
    live = _relay_dir(tmp_path) / "jarvis-venv"
    staged = staged_jarvis_venv_path(home=tmp_path, invocation_id=invocation_id)
    _build_environment(live, marker="old")
    _build_environment(staged, marker="new")
    staged_identity = (staged.lstat().st_dev, staged.lstat().st_ino)

    # The "interrupt": nothing has run yet. Assert the live env is exactly
    # what it was before staging began.
    assert (live / "bin" / "python").read_text(encoding="utf-8") == "old"

    # Recovery converges: completes the promotion forward.
    promote_staged_jarvis_venv(
        home=tmp_path,
        invocation_id=invocation_id,
        retired_at="20260819T060000Z",
        staged_identity=staged_identity,
    )
    assert (live / "bin" / "python").read_text(encoding="utf-8") == "new"


def test_sabotage_interrupt_after_exchange_before_retire_recovers_idempotently(
    tmp_path: Path,
) -> None:
    """Sabotage-shaped acceptance (#254): interrupt AFTER the atomic exchange
    but BEFORE the retire rename. A second, independent recovery call (its
    own fresh retired_at, exactly how `bootstrap_recover_previous_transaction`
    invokes it) must not re-exchange (which would swap live back to the
    retired copy) -- it proves the exchange already ran via `staged_identity`
    and is a safe no-op."""
    invocation_id = "inv-1"
    live = _relay_dir(tmp_path) / "jarvis-venv"
    staged = staged_jarvis_venv_path(home=tmp_path, invocation_id=invocation_id)
    _build_environment(live, marker="old")
    _build_environment(staged, marker="new")
    staged_identity = (staged.lstat().st_dev, staged.lstat().st_ino)

    # First call completes fully (exchange + retire).
    promote_staged_jarvis_venv(
        home=tmp_path,
        invocation_id=invocation_id,
        retired_at="20260819T060000Z",
        staged_identity=staged_identity,
    )
    assert (live / "bin" / "python").read_text(encoding="utf-8") == "new"

    # Simulate the "interrupted after exchange" recovery re-run: the exchange
    # already happened, `staged` no longer exists, so this call must be a
    # pure no-op -- never re-exchanging live back to the retired copy.
    second = promote_staged_jarvis_venv(
        home=tmp_path,
        invocation_id=invocation_id,
        retired_at="20260819T070000Z",
        staged_identity=staged_identity,
    )
    assert second["live"] == str(live)
    # Never swapped back: live still holds the NEW content, exactly once.
    assert (live / "bin" / "python").read_text(encoding="utf-8") == "new"
    retired = retired_jarvis_venv_path(home=tmp_path, retired_at="20260819T060000Z")
    assert (retired / "bin" / "python").read_text(encoding="utf-8") == "old"
    # The second call's own retired target was never created -- nothing was
    # left to retire a second time.
    assert not retired_jarvis_venv_path(home=tmp_path, retired_at="20260819T070000Z").exists()


def test_promote_refuses_when_staged_content_changed(tmp_path: Path) -> None:
    """Promotion refuses -- typed, not silent -- if the staged directory's
    identity no longer matches what was recorded when it was created."""
    invocation_id = "inv-1"
    live = _relay_dir(tmp_path) / "jarvis-venv"
    _build_environment(live, marker="old")
    with pytest.raises(ConfigurationError, match="unavailable for promotion"):
        promote_staged_jarvis_venv(
            home=tmp_path,
            invocation_id=invocation_id,
            retired_at="20260819T060000Z",
            staged_identity=(0, 0),
        )
    assert (live / "bin" / "python").read_text(encoding="utf-8") == "old"
