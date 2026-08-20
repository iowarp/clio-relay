"""Full-mode current/relay_source activation staging (clio-relay#257).

Generalizes clio-relay#254's jarvis-venv staging discipline to the rest of a
full-mode installation. The historical defect: a full-mode reconcile chosen
over a host whose ``current`` generation already exists (not just the #254
jarvis-venv guard) still hit the fresh-bootstrap ownership proof's
unconditional ``absent()`` assertions for ``current``/``install_receipt``/
``managed_repo``/etc -- #254 flagged this as an explicit, out-of-scope
deviation when it fixed the jarvis-venv-only case.

The fix generalizes narrowly. ``current`` and ``~/.local/src/clio-relay``
(``relay_source``) are the only two stable activation paths whose TARGET
actually changes between generations -- ``install_receipt``/
``relay_launcher``/``jarvis_launcher`` and ``managed_repo`` all point at
CONSTANT text through ``current`` itself (e.g. ``current/bin/jarvis``), so
once created they are correct forever (this is exactly what the rendered
script's own ``bootstrap_verify_stable_activation_links`` proves) and are
adopted directly, never routed through here. ``current``/``relay_source``
are captured before any change, carried through the generation manifest
durably (mirroring how relay-only/component-upgrade reconcile already
carries its own five-name snapshot in
``bootstrap_reconcile._capture_reconcile_activation_paths``), and promoted
atomically at the fenced boundary by reusing
``bootstrap_reconcile._reconcile_activation_symlink`` -- the SAME per-
symlink atomic-exchange-and-crash-recovery primitive relay-only/component-
upgrade reconcile already fences its own activation with, not a
freshly-invented parallel mechanism.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapActivationPath,
    _capture_activation_path,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _read_regular_bounded,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _reconcile_activation_symlink,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.errors import ConfigurationError

FULL_MODE_ACTIVATION_NAMES = ("current", "relay_source")


def _relay_state_dir(home: Path) -> Path:
    return Path(os.path.abspath(home.expanduser())) / ".local/share/clio-relay"


def capture_full_mode_activation_paths(home: Path) -> dict[str, BootstrapActivationPath]:
    """Best-effort capture of current/relay_source for a full-mode plan.

    Empty means either the virgin path (``current`` genuinely absent --
    nothing to stage a swap for) or a populated host whose snapshot did not
    capture cleanly (a corrupt or partial prior install); either way, this
    never silently proceeds with a stale snapshot --
    ``promote_full_mode_activation_links`` raises its own typed refusal
    instead of promoting from wrong evidence.
    """
    share = _relay_state_dir(home)
    try:
        current = _capture_activation_path(
            share / "current",
            kind="symlink",
            maximum=4096,
            allow_absent=True,
        )
        if current.before is None:
            return {}
        relay_source = _capture_activation_path(
            Path(os.path.abspath(home.expanduser())) / ".local/src/clio-relay",
            kind="symlink",
            maximum=4096,
            allow_absent=True,
        )
    except (ConfigurationError, OSError, RuntimeError, ValueError):
        return {}
    return {"current": current, "relay_source": relay_source}


def capture_full_mode_activation_paths_json(home: Path) -> dict[str, object]:
    """JSON-ready form of :func:`capture_full_mode_activation_paths`.

    For embedding directly into a generation manifest -- see
    ``promote_full_mode_activation_links_from_manifest``, its reader.
    """
    return {
        name: value.model_dump(mode="json")
        for name, value in capture_full_mode_activation_paths(home).items()
    }


def promote_full_mode_activation_links(
    activation_paths: dict[str, BootstrapActivationPath],
    *,
    generation: Path,
    desired_fingerprint: str,
    home: Path | None = None,
) -> dict[str, str]:
    """Atomically promote current/relay_source to the prepared generation.

    Idempotent for crash recovery: reruns of this same call (the SAME
    persisted ``activation_paths`` snapshot, never a freshly re-derived
    one) converge without re-exchanging a link that already promoted --
    ``_reconcile_activation_symlink``'s own "reused" fast-path proves it.
    """
    if set(activation_paths) != set(FULL_MODE_ACTIVATION_NAMES):
        raise ConfigurationError("staged activation plan omitted its path identities")
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    share = lexical_home / ".local/share/clio-relay"
    expected_generation = share / "generations" / desired_fingerprint
    if generation != expected_generation or generation.is_symlink() or not generation.is_dir():
        raise ConfigurationError("staged activation generation path is invalid")
    targets = {"current": generation, "relay_source": generation / "source"}
    live_paths = {
        "current": share / "current",
        "relay_source": lexical_home / ".local/src/clio-relay",
    }
    for name, target_path in live_paths.items():
        if Path(activation_paths[name].path) != target_path:
            raise ConfigurationError(f"staged activation path destination changed: {name}")
    actions: dict[str, str] = {}
    for name in FULL_MODE_ACTIVATION_NAMES:
        actions[name] = _reconcile_activation_symlink(
            activation_paths[name],
            expected_target=targets[name],
            label=f"bootstrap stable activation path {name}",
            exchange_identity=desired_fingerprint,
        )
    return actions


def promote_full_mode_activation_links_from_manifest(
    generation: Path,
    *,
    expected_manifest_sha256: str,
    desired_fingerprint: str,
    home: Path | None = None,
) -> dict[str, str]:
    """Read the durably-recorded snapshot from manifest.json and promote it.

    The bootstrap-transaction candidate-action dispatcher's own entry point
    for the ``full-activation-reconcile`` action -- keeps the manifest
    read/verify/reconstruct plumbing out of the rendered script. ``home``
    defaults to :func:`Path.home` (production always promotes against the
    real host); tests inject a ``tmp_path`` instead.
    """
    raw_manifest = _read_regular_bounded(generation / "manifest.json", maximum=4 * 1024 * 1024)
    if hashlib.sha256(raw_manifest).hexdigest() != expected_manifest_sha256:
        raise ConfigurationError("prepared generation manifest changed before activation")
    raw_activation_paths = json.loads(raw_manifest).get("full_activation_paths") or {}
    activation_paths = {
        name: BootstrapActivationPath.model_validate(value)
        for name, value in raw_activation_paths.items()
    }
    return promote_full_mode_activation_links(
        activation_paths,
        generation=generation,
        desired_fingerprint=desired_fingerprint,
        home=home,
    )


def ownership_proof_populated_host_adoption_python() -> str:
    """Dependency-free adoption logic for the fresh-bootstrap ownership proof.

    That heredoc runs via the SYSTEM ``python3`` before any relay-managed
    environment exists (a virgin host has none yet), so it cannot import
    this module or any other clio_relay code -- the text below is
    stdlib-only Python, embedded verbatim (never re-interpolated) right
    after ``jarvis_venv_entry`` is defined in the rendered ownership-proof
    heredoc. It assumes ``classify``, ``owned``, ``absent``, ``home``,
    ``fingerprint``, ``invocation_id``, ``jarvis_venv_entry``,
    ``activation_staging_mode``, ``stat``, ``os`` are already in scope
    there.
    """
    return """
# clio-relay#257: unlike jarvis-venv's owned staged directory, current/
# relay_source are never claimed here at all in staging mode -- single
# symlinks promoted later by bootstrap_full_activation_staging, which owns
# its own atomic-exchange staging internally.
non_activation_entries = (
    jarvis_venv_entry,
    (
        "transaction_root",
        home / ".local/share/clio-relay/transactions" / invocation_id,
        "directory",
    ),
    ("generation", home / ".local/share/clio-relay/generations" / fingerprint, "directory"),
)
if not activation_staging_mode:
    non_activation_entries = (
        *non_activation_entries,
        ("current", home / ".local/share/clio-relay/current", "symlink"),
        ("relay_source", home / ".local/src/clio-relay", "symlink"),
    )
for name, path, kind in non_activation_entries:
    absent(name, path, kind)

# clio-relay#257: these are fixed, shared cache/state directories reused
# across every bootstrap (component-wheel caches, uv's own tool/python
# storage, JARVIS's own config/private/shared data) -- on a populated host
# they already hold content from an earlier successful bootstrap. Adopting
# them (verifying only that each is one real directory, never a symlink)
# instead of requiring absence is what generalizes; a virgin host still
# claims and creates them exactly as before, since `classify(path)` is None
# there too.
def adopt_shared_directory(name: str, path: Path) -> None:
    details = classify(path)
    if details is None:
        owned[name] = {"path": str(path), "kind": "directory"}
    elif not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
        raise SystemExit(f"bootstrap cannot adopt an existing non-directory: {path}")

for name, path in (
    ("clio_kit_wheels", home / ".local/share/clio-relay/component-wheels/clio-kit"),
    ("jarvis_cd_wheels", home / ".local/share/clio-relay/component-wheels/jarvis-cd"),
    ("uv_tools", home / ".local/share/clio-relay/uv-tools"),
    ("uv_bin", home / ".local/share/clio-relay/uv-bin"),
    ("uv_python", home / ".local/share/clio-relay/uv-python"),
    ("jarvis_state", home / ".ppi-jarvis"),
    ("jarvis_config", home / ".local/share/clio-relay/jarvis-config"),
    ("jarvis_private", home / ".local/share/clio-relay/jarvis-private"),
    ("jarvis_shared", home / ".local/share/clio-relay/jarvis-shared"),
):
    adopt_shared_directory(name, path)

# clio-relay#257: install_receipt/relay_launcher/jarvis_launcher/managed_repo
# all point at CONSTANT text through `current` itself (e.g.
# `current/bin/jarvis`) -- once created they are correct forever, exactly
# what the rendered script's own bootstrap_verify_stable_activation_links
# proves. A populated host's existing link is adopted (never re-created,
# never claimed) if -- and only if -- it already names that exact text; a
# link naming anything else is a typed refusal, never a silent repoint.
def adopt_stable_symlink(name: str, path: Path, expected_target: Path) -> None:
    details = classify(path)
    if details is None:
        owned[name] = {"path": str(path), "kind": "symlink"}
    elif not stat.S_ISLNK(details.st_mode) or os.readlink(path) != str(expected_target):
        raise SystemExit(f"bootstrap cannot adopt an existing activation link: {path}")

for name, path, target in (
    (
        "install_receipt",
        home / ".local/share/clio-relay/install-receipt.json",
        home / ".local/share/clio-relay/current/install-receipt.json",
    ),
    (
        "relay_launcher",
        home / ".local/bin/clio-relay",
        home / ".local/share/clio-relay/current/bin/clio-relay",
    ),
    (
        "jarvis_launcher",
        home / ".local/bin/jarvis",
        home / ".local/share/clio-relay/current/bin/jarvis",
    ),
    (
        "managed_repo",
        home / ".local/share/clio-relay/clio_relay",
        home / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay",
    ),
):
    adopt_stable_symlink(name, path, target)
""".strip("\n")


def shared_directory_mkdir_owned_helper_shell() -> str:
    """A ``bootstrap_journal_action mkdir-owned`` wrapper skipped when adopted.

    clio-relay#257: the ownership proof already skipped claiming these fixed
    shared directories when they exist (``adopt_shared_directory``, above);
    this is the matching downstream skip for their ``mkdir-owned`` call --
    a virgin host still creates every one, since ``[ ! -d "$dir" ]`` is
    true there too.
    """
    return """bootstrap_mkdir_owned_if_absent() {
  if [ ! -d "$1" ]; then
    bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" "$2"
  fi
}"""


def stable_activation_link_adoption_shell() -> str:
    """Shell loop adopting install_receipt/relay_launcher/jarvis_launcher.

    clio-relay#257: their target text never changes across generations (see
    ``ownership_proof_populated_host_adoption_python``'s docstring), so a
    populated host's already-correct link is adopted here exactly like
    ``managed_repo`` -- never re-created. Also covers the rare inconsistent
    host where ``current`` already existed but one of these three did not:
    it still gets created fresh, in either mode, same as it always has.
    Runs unconditionally (not staging-mode-gated): on a virgin host none of
    these three exist yet, so every iteration falls through to its `else`
    branch and creates them exactly as the unfixed script always did.
    """
    return """for bootstrap_stable_link_name in install_receipt relay_launcher jarvis_launcher; do
  case "$bootstrap_stable_link_name" in
    install_receipt)
      bootstrap_stable_link_path="$HOME/.local/share/clio-relay/install-receipt.json"
      bootstrap_stable_link_target="$HOME/.local/share/clio-relay/current/install-receipt.json"
      ;;
    relay_launcher)
      bootstrap_stable_link_path="$HOME/.local/bin/clio-relay"
      bootstrap_stable_link_target="$HOME/.local/share/clio-relay/current/bin/clio-relay"
      ;;
    jarvis_launcher)
      bootstrap_stable_link_path="$HOME/.local/bin/jarvis"
      bootstrap_stable_link_target="$HOME/.local/share/clio-relay/current/bin/jarvis"
      ;;
  esac
  if [ -L "$bootstrap_stable_link_path" ]; then
    if [ "$(readlink "$bootstrap_stable_link_path")" != "$bootstrap_stable_link_target" ]; then
      echo "bootstrap stable activation link points to an unexpected target:" \\
        "$bootstrap_stable_link_path" >&2
      exit 1
    fi
  elif [ -e "$bootstrap_stable_link_path" ]; then
    echo "bootstrap stable activation link is not a symbolic link:" \\
      "$bootstrap_stable_link_path" >&2
    exit 1
  else
    bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \\
      "$bootstrap_stable_link_name" "$bootstrap_stable_link_target"
  fi
done"""
