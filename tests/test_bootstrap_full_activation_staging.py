"""Failing-first tests for clio_relay.bootstrap_full_activation_staging (clio-relay#257).

The live defect: a full-mode `cluster bootstrap` reconcile chosen over a
host whose `current` generation already exists (not just the #254
jarvis-venv guard) still hit the fresh-bootstrap ownership proof's
unconditional `absent()` assertions for current/install_receipt/
managed_repo/etc -- #254 flagged this as an explicit, out-of-scope
deviation when it fixed the jarvis-venv-only case. The fix generalizes
narrowly: `current` and `relay_source` (`~/.local/src/clio-relay`) are the
only two stable activation paths whose target actually changes between
generations, so they are captured and promoted here instead of being
required absent -- the same "stage beside the live installation, swap
atomically at the fenced boundary, never observe it half-cleared" discipline
clio_relay.bootstrap_jarvis_staging already established for jarvis-venv,
including under a sabotage-shaped interrupt between stage-complete and swap.

Unlike jarvis-venv's owned staged directory, `current`/`relay_source` are
never claimed as owned paths at all: they are single symlinks, and
`bootstrap_reconcile._reconcile_activation_symlink` (the same primitive
relay-only/component-upgrade reconcile already fences its own activation
with) manages its own atomic-exchange staging internally. The persistence
vehicle is therefore the generation manifest (`full_activation_paths`),
not the bootstrap-transaction journal's owned-path identities.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import clio_relay.bootstrap_full_activation_staging as bootstrap_full_activation_staging_module
import clio_relay.bootstrap_reconcile as bootstrap_reconcile_module
from clio_relay.bootstrap_full_activation_staging import (
    FULL_MODE_ACTIVATION_NAMES,
    capture_full_mode_activation_paths,
    capture_full_mode_activation_paths_json,
    promote_full_mode_activation_links,
    promote_full_mode_activation_links_from_manifest,
)
from clio_relay.errors import ConfigurationError


def _simulate_symlinks_on_windows(monkeypatch: pytest.MonkeyPatch) -> dict[Path, Path]:
    """Simulate real symlink semantics on an unprivileged Windows runner.

    Windows symlink creation normally requires an elevated privilege CI does
    not grant, so ``Path.symlink_to``/``is_symlink``/``resolve``/``lstat``/
    ``unlink`` and ``os.readlink``/``os.replace`` are patched to track a
    lightweight link-table instead of real filesystem symlinks. POSIX is
    unaffected (real symlinks, no patching).
    """
    simulated_links: dict[Path, Path] = {}
    if os.name != "nt":
        return simulated_links
    original_is_symlink = Path.is_symlink
    original_resolve = Path.resolve
    original_readlink = os.readlink
    original_replace = os.replace
    original_lstat = Path.lstat
    original_unlink = Path.unlink

    def simulated_target(path: Path) -> Path:
        candidate = path
        for _ in range(10):
            matches: list[tuple[int, Path, Path]] = []
            for link, target in simulated_links.items():
                try:
                    relative = candidate.relative_to(link)
                except ValueError:
                    continue
                matches.append((len(link.parts), target, relative))
            if not matches:
                return candidate
            _length, target, relative = max(matches, key=lambda item: item[0])
            candidate = target / relative
        raise AssertionError("simulated symlink chain did not terminate")

    def simulated_symlink_to(
        path: Path,
        target: Path | str,
        target_is_directory: bool = False,
    ) -> None:
        del target_is_directory
        if path.exists() or path in simulated_links:
            raise FileExistsError(path)
        path.write_bytes(b"simulated-symlink")
        simulated_links[path] = Path(target)

    def simulated_is_symlink(path: Path) -> bool:
        return path in simulated_links or original_is_symlink(path)

    def simulated_resolve(path: Path, strict: bool = False) -> Path:
        return original_resolve(simulated_target(path), strict=strict)

    def simulated_lstat(path: Path) -> os.stat_result:
        if path in simulated_links:
            real = original_lstat(path)
            return cast(
                os.stat_result,
                SimpleNamespace(
                    st_dev=real.st_dev,
                    st_ino=real.st_ino,
                    st_mode=stat.S_IFLNK | 0o777,
                    st_size=len(str(simulated_links[path])),
                    st_mtime_ns=real.st_mtime_ns,
                    st_ctime_ns=real.st_ctime_ns,
                    st_nlink=real.st_nlink,
                ),
            )
        return original_lstat(path)

    def simulated_readlink(path: Path | str) -> str:
        candidate = Path(path)
        if candidate in simulated_links:
            return str(simulated_links[candidate])
        return original_readlink(path)

    def simulated_replace(source: Path | str, destination: Path | str) -> None:
        source_path = Path(source)
        destination_path = Path(destination)
        original_replace(source_path, destination_path)
        if source_path in simulated_links:
            simulated_links[destination_path] = simulated_links.pop(source_path)
        elif destination_path in simulated_links:
            del simulated_links[destination_path]

    def simulated_unlink(path: Path, missing_ok: bool = False) -> None:
        simulated_links.pop(path, None)
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "symlink_to", simulated_symlink_to)
    monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)
    monkeypatch.setattr(Path, "resolve", simulated_resolve)
    monkeypatch.setattr(Path, "lstat", simulated_lstat)
    monkeypatch.setattr(Path, "unlink", simulated_unlink)
    monkeypatch.setattr(bootstrap_reconcile_module.os, "readlink", simulated_readlink)
    monkeypatch.setattr(bootstrap_reconcile_module.os, "replace", simulated_replace)
    return simulated_links


def _build_populated_host(tmp_path: Path) -> dict[str, Path]:
    """Build a host with a full prior successful installation on disk."""
    share = tmp_path / ".local/share/clio-relay"
    src_dir = tmp_path / ".local/src"
    old_generation = share / "generations" / ("0" * 64)
    (old_generation / "bin").mkdir(parents=True)
    (old_generation / "source").mkdir(parents=True)
    src_dir.mkdir(parents=True)
    current = share / "current"
    current.symlink_to(old_generation, target_is_directory=True)
    relay_source = src_dir / "clio-relay"
    relay_source.symlink_to(old_generation / "source", target_is_directory=True)
    return {
        "share": share,
        "old_generation": old_generation,
        "current": current,
        "relay_source": relay_source,
    }


def test_virgin_host_has_no_activation_paths(tmp_path: Path) -> None:
    """No installation exists yet: the virgin path is unchanged (empty)."""
    assert capture_full_mode_activation_paths(tmp_path) == {}
    assert capture_full_mode_activation_paths_json(tmp_path) == {}


def test_existing_installation_produces_captured_paths_instead_of_a_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The defect's exact acceptance bar: a full-mode reconcile over a
    populated host produces the CAPTURED current/relay_source snapshot, not
    the empty/refused state the unfixed ownership proof would have hit."""
    _simulate_symlinks_on_windows(monkeypatch)
    host = _build_populated_host(tmp_path)

    activation_paths = capture_full_mode_activation_paths(tmp_path)

    assert set(activation_paths) == set(FULL_MODE_ACTIVATION_NAMES)
    current_before = activation_paths["current"].before
    relay_source_before = activation_paths["relay_source"].before
    assert current_before is not None
    assert current_before.symlink_target == str(host["old_generation"])
    assert relay_source_before is not None
    assert relay_source_before.symlink_target == str(host["old_generation"] / "source")
    # The capture never proposes clearing the live installation directly.
    assert host["current"].is_symlink()
    assert host["relay_source"].is_symlink()

    as_json = capture_full_mode_activation_paths_json(tmp_path)
    assert set(as_json) == set(FULL_MODE_ACTIVATION_NAMES)
    assert json.loads(json.dumps(as_json)) == as_json  # JSON-round-trips cleanly


def test_existing_current_non_symlink_never_silently_proceeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A `current` that is not one owned symlink never produces a captured
    plan claiming otherwise -- capture returns empty (not a stale/partial
    snapshot), and promotion (below) then refuses typed on the empty set."""
    share = tmp_path / ".local/share/clio-relay"
    share.mkdir(parents=True)
    (share / "current").mkdir()  # a real directory, not a symlink

    assert capture_full_mode_activation_paths(tmp_path) == {}


def test_promote_swaps_current_and_relay_source_then_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The core mechanism: one call promotes BOTH stable paths whose target
    changes across generations, then a second call is a pure no-op
    ("reused") -- the exact idempotency bar #254 set for jarvis-venv."""
    _simulate_symlinks_on_windows(monkeypatch)
    host = _build_populated_host(tmp_path)
    activation_paths = capture_full_mode_activation_paths(tmp_path)
    fingerprint = "1" * 64
    new_generation = host["share"] / "generations" / fingerprint
    (new_generation / "source").mkdir(parents=True)

    first = promote_full_mode_activation_links(
        activation_paths,
        generation=new_generation,
        desired_fingerprint=fingerprint,
        home=tmp_path,
    )
    second = promote_full_mode_activation_links(
        activation_paths,
        generation=new_generation,
        desired_fingerprint=fingerprint,
        home=tmp_path,
    )

    assert first == {"current": "retargeted", "relay_source": "retargeted"}
    assert set(second.values()) == {"reused"}
    assert os.readlink(host["current"]) == str(new_generation)
    assert os.readlink(host["relay_source"]) == str(new_generation / "source")
    # The superseded generation is never deleted -- only unpointed-to.
    assert host["old_generation"].is_dir()


def test_promote_rejects_a_path_set_missing_relay_source(tmp_path: Path) -> None:
    """Promotion requires EXACTLY {current, relay_source} -- a partial or
    wrongly-shaped snapshot is a typed refusal, never silently promoted."""
    generation = tmp_path / ".local/share/clio-relay/generations" / ("2" * 64)
    generation.mkdir(parents=True)
    only_current = {
        "current": bootstrap_reconcile_module.BootstrapActivationPath(
            path=str(tmp_path / "current"), kind="symlink"
        )
    }

    with pytest.raises(ConfigurationError, match="omitted its path identities"):
        promote_full_mode_activation_links(
            only_current,
            generation=generation,
            desired_fingerprint="2" * 64,
            home=tmp_path,
        )


def test_sabotage_interrupt_before_any_exchange_leaves_live_untouched_and_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sabotage-shaped acceptance (clio-relay#257, generalizing #254): a
    crash before the exchange for `current` even starts must leave BOTH
    stable paths exactly as they were; a later call -- recovery re-running
    the SAME persisted snapshot, never a freshly re-derived one -- must
    still converge and promote both."""
    _simulate_symlinks_on_windows(monkeypatch)
    host = _build_populated_host(tmp_path)
    activation_paths = capture_full_mode_activation_paths(tmp_path)
    fingerprint = "3" * 64
    new_generation = host["share"] / "generations" / fingerprint
    (new_generation / "source").mkdir(parents=True)

    # clio-relay#257's module imports `_reconcile_activation_symlink` via
    # `from ... import`, binding its OWN name -- patch that binding, not
    # bootstrap_reconcile's (a separate namespace after import time).
    original = bootstrap_full_activation_staging_module._reconcile_activation_symlink  # noqa: SLF001

    def sabotage_before_any_exchange(*_args: object, **_kwargs: object) -> str:
        raise ConfigurationError("simulated crash before any exchange")

    monkeypatch.setattr(
        bootstrap_full_activation_staging_module,
        "_reconcile_activation_symlink",
        sabotage_before_any_exchange,
    )
    with pytest.raises(ConfigurationError, match="simulated crash before any exchange"):
        promote_full_mode_activation_links(
            activation_paths,
            generation=new_generation,
            desired_fingerprint=fingerprint,
            home=tmp_path,
        )

    assert os.readlink(host["current"]) == str(host["old_generation"])
    assert os.readlink(host["relay_source"]) == str(host["old_generation"] / "source")

    # Restore the real exchange primitive -- NOT the symlink simulation
    # (undoing that would drop its link-table and desync it from the
    # simulated-symlink placeholder files already written to disk above).
    monkeypatch.setattr(
        bootstrap_full_activation_staging_module, "_reconcile_activation_symlink", original
    )
    result = promote_full_mode_activation_links(
        activation_paths,
        generation=new_generation,
        desired_fingerprint=fingerprint,
        home=tmp_path,
    )

    assert result == {"current": "retargeted", "relay_source": "retargeted"}
    assert os.readlink(host["current"]) == str(new_generation)
    assert os.readlink(host["relay_source"]) == str(new_generation / "source")


def test_sabotage_interrupt_between_current_and_relay_source_recovers_idempotently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sabotage-shaped acceptance twin: interrupt AFTER `current` promotes
    but BEFORE `relay_source` does. A second, independent call (its own
    fresh invocation, exactly how a recovery re-run would call this) must
    not re-exchange `current` -- it proves the exchange already ran via the
    "reused" fast-path and only completes `relay_source`."""
    _simulate_symlinks_on_windows(monkeypatch)
    host = _build_populated_host(tmp_path)
    activation_paths = capture_full_mode_activation_paths(tmp_path)
    fingerprint = "4" * 64
    new_generation = host["share"] / "generations" / fingerprint
    (new_generation / "source").mkdir(parents=True)
    original = bootstrap_full_activation_staging_module._reconcile_activation_symlink  # noqa: SLF001

    def sabotage_after_current(snapshot: object, **kwargs: object) -> str:
        if getattr(snapshot, "path", None) == str(host["current"]):
            return cast(str, original(snapshot, **kwargs))
        raise ConfigurationError("simulated crash after current, before relay_source")

    monkeypatch.setattr(
        bootstrap_full_activation_staging_module,
        "_reconcile_activation_symlink",
        sabotage_after_current,
    )
    with pytest.raises(ConfigurationError, match="simulated crash after current"):
        promote_full_mode_activation_links(
            activation_paths,
            generation=new_generation,
            desired_fingerprint=fingerprint,
            home=tmp_path,
        )

    # Interrupt window: current already promoted, relay_source still old.
    assert os.readlink(host["current"]) == str(new_generation)
    assert os.readlink(host["relay_source"]) == str(host["old_generation"] / "source")

    monkeypatch.setattr(
        bootstrap_full_activation_staging_module, "_reconcile_activation_symlink", original
    )
    second = promote_full_mode_activation_links(
        activation_paths,
        generation=new_generation,
        desired_fingerprint=fingerprint,
        home=tmp_path,
    )

    assert second == {"current": "reused", "relay_source": "retargeted"}
    # Never re-exchanged: current still names the new generation, exactly once.
    assert os.readlink(host["current"]) == str(new_generation)
    assert os.readlink(host["relay_source"]) == str(new_generation / "source")


def test_promote_from_manifest_round_trips_a_real_generation_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The candidate-action dispatcher's own entry point: reads the durably
    recorded snapshot from manifest.json (never re-derives it) and promotes
    it, exactly like the rendered script's `full-activation-reconcile`
    action does."""
    _simulate_symlinks_on_windows(monkeypatch)
    host = _build_populated_host(tmp_path)
    fingerprint = "5" * 64
    new_generation = host["share"] / "generations" / fingerprint
    (new_generation / "source").mkdir(parents=True)
    manifest = {
        "schema_version": "clio-relay.bootstrap-generation.v1",
        "fingerprint": fingerprint,
        "full_activation_paths": capture_full_mode_activation_paths_json(tmp_path),
    }
    manifest_path = new_generation / "manifest.json"
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    manifest_path.write_bytes(manifest_bytes)  # binary: avoids Windows \n -> \r\n translation
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

    result = promote_full_mode_activation_links_from_manifest(
        new_generation,
        expected_manifest_sha256=manifest_sha256,
        desired_fingerprint=fingerprint,
        home=tmp_path,
    )

    assert result == {"current": "retargeted", "relay_source": "retargeted"}
    assert os.readlink(host["current"]) == str(new_generation)


def test_promote_from_manifest_rejects_a_tampered_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery uses the JOURNALED manifest digest, never trusts disk state
    that changed since it was recorded."""
    _simulate_symlinks_on_windows(monkeypatch)
    _build_populated_host(tmp_path)
    generation = tmp_path / ".local/share/clio-relay/generations" / ("6" * 64)
    generation.mkdir(parents=True)
    (generation / "manifest.json").write_bytes(b'{"tampered":true}\n')

    def unexpected_mutation(*_args: object, **_kwargs: object) -> str:
        raise AssertionError("tampered manifest reached promotion")

    monkeypatch.setattr(
        "clio_relay.bootstrap_full_activation_staging.promote_full_mode_activation_links",
        unexpected_mutation,
    )

    with pytest.raises(ConfigurationError, match="manifest changed before activation"):
        promote_full_mode_activation_links_from_manifest(
            generation,
            expected_manifest_sha256="f" * 64,
            desired_fingerprint="6" * 64,
        )
