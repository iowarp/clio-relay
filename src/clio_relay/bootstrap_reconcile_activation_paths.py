"""Pre-fence path capture and atomic stable-symlink activation.

Owns the entire activation-path lifecycle: capturing one exact path before
fencing (``_capture_activation_path``/``_capture_reconcile_activation_paths``),
verifying a stable symlink names desired state (``_verify_stable_symlink``),
and idempotently publishing it through the crash-safe exchange primitive
(``_reconcile_activation_symlink``/``reconcile_staged_activation_links``).
This is the widest-shared owner in the split -- generation inspection,
JARVIS wrapper binding, repository reconciliation, and reconcile planning
all verify through ``_verify_stable_symlink`` here -- which is why it sits
above the 150-500 sweet spot (iowarp/clio-relay#255).
"""

from __future__ import annotations

import hashlib
import os
import stat
from contextlib import suppress
from pathlib import Path
from typing import Literal

from clio_relay.bootstrap_reconcile_constants import MANAGED_JARVIS_REPO_PATH
from clio_relay.bootstrap_reconcile_models import (
    BootstrapActivationPath,
    BootstrapActivationPathIdentity,
    BootstrapReconcilePlan,
)
from clio_relay.bootstrap_reconcile_primitives import (
    _atomic_exchange_paths,
    _expand_home,
    _fsync_directory,
    _read_regular_bounded_with_identity,
    _require_sha256,
    _stat_identity,
)
from clio_relay.errors import ConfigurationError


def _is_generation_repository_target(path: Path, *, home: Path) -> bool:
    """Return whether a proven path has the exact relay generation repository shape."""
    generations = home / ".local/share/clio-relay/generations"
    try:
        relative = path.relative_to(generations)
    except ValueError:
        return False
    fingerprint = relative.parts[0] if relative.parts else ""
    return bool(
        len(relative.parts) == 4
        and len(fingerprint) == 64
        and all(character in "0123456789abcdef" for character in fingerprint)
        and relative.parts[1:] == ("source", "jarvis-packages", "clio_relay")
    )


def _verify_stable_symlink(path: Path, *, expected: Path, label: str) -> Path:
    """Resolve one exact lexical symlink and reject replacement races."""
    before = path.lstat()
    if not path.is_symlink():
        raise ConfigurationError(f"{label} is not a symbolic link")
    raw_target = os.readlink(path)
    target = Path(raw_target)
    if not target.is_absolute():
        target = path.parent / target
    resolved = target.resolve(strict=True)
    if resolved != expected.resolve(strict=True):
        raise ConfigurationError(f"{label} does not name desired state")
    if _stat_identity(path.lstat()) != _stat_identity(before):
        raise ConfigurationError(f"{label} changed during inspection")
    return resolved


def _capture_activation_path(
    path: Path,
    *,
    kind: Literal["file", "file_or_symlink", "symlink"],
    maximum: int,
    allow_absent: bool,
) -> BootstrapActivationPath:
    """Capture one exact pre-fence path without following its final link."""
    lexical = Path(os.path.abspath(path.expanduser()))
    try:
        before = lexical.lstat()
    except FileNotFoundError:
        if not allow_absent:
            raise ConfigurationError(
                f"required activation path is unavailable: {lexical}"
            ) from None
        return BootstrapActivationPath(path=str(lexical), kind=kind)
    except OSError as exc:
        raise ConfigurationError(f"activation path could not be classified: {lexical}") from exc
    file_type: Literal["file", "symlink"]
    digest: str | None = None
    link_target: str | None = None
    if stat.S_ISLNK(before.st_mode):
        if kind == "file":
            raise ConfigurationError(f"activation path must be a regular file: {lexical}")
        try:
            link_target = os.readlink(lexical)
            after = lexical.lstat()
        except OSError as exc:
            raise ConfigurationError(f"activation symlink could not be read: {lexical}") from exc
        if (
            not link_target
            or any(character in link_target for character in "\x00\r\n")
            or _stat_identity(before) != _stat_identity(after)
        ):
            raise ConfigurationError(f"activation symlink changed while inspected: {lexical}")
        file_type = "symlink"
    elif stat.S_ISREG(before.st_mode):
        if kind == "symlink":
            raise ConfigurationError(f"activation path must be a symbolic link: {lexical}")
        raw, _identity = _read_regular_bounded_with_identity(lexical, maximum=maximum)
        try:
            after = lexical.lstat()
        except OSError as exc:
            raise ConfigurationError(f"activation file changed while inspected: {lexical}") from exc
        if _stat_identity(before) != _stat_identity(after):
            raise ConfigurationError(f"activation file changed while inspected: {lexical}")
        digest = hashlib.sha256(raw).hexdigest()
        file_type = "file"
    else:
        raise ConfigurationError(f"activation path has an unsafe type: {lexical}")
    return BootstrapActivationPath(
        path=str(lexical),
        kind=kind,
        before=BootstrapActivationPathIdentity(
            device=before.st_dev,
            inode=before.st_ino,
            mode=before.st_mode,
            size=before.st_size,
            modified_ns=before.st_mtime_ns,
            changed_ns=before.st_ctime_ns,
            file_type=file_type,
            sha256=digest,
            symlink_target=link_target,
        ),
    )


def _activation_path_identity(path: BootstrapActivationPath) -> BootstrapActivationPathIdentity:
    """Re-capture an existing activation path using its original contract."""
    captured = _capture_activation_path(
        Path(path.path),
        kind=path.kind,
        maximum=4 * 1024 * 1024,
        allow_absent=False,
    )
    if captured.before is None:  # pragma: no cover - excluded by allow_absent
        raise ConfigurationError(f"activation path disappeared: {path.path}")
    return captured.before


def _capture_activation_object(
    path: Path,
    *,
    kind: Literal["file", "file_or_symlink", "symlink"],
    maximum: int,
) -> BootstrapActivationPathIdentity:
    """Capture a file or link, including the Windows symlink test representation."""
    lexical = Path(os.path.abspath(path.expanduser()))
    try:
        before = lexical.lstat()
        raw_target = os.readlink(lexical)
        after = lexical.lstat()
    except OSError:
        captured = _capture_activation_path(
            lexical,
            kind=kind,
            maximum=maximum,
            allow_absent=False,
        )
        if captured.before is None:  # pragma: no cover - excluded by allow_absent
            raise ConfigurationError(f"activation path disappeared: {lexical}") from None
        return captured.before
    if (
        kind == "file"
        or not raw_target
        or any(character in raw_target for character in "\x00\r\n")
        or _stat_identity(before) != _stat_identity(after)
    ):
        raise ConfigurationError(f"activation symlink changed while inspected: {lexical}")
    return BootstrapActivationPathIdentity(
        device=before.st_dev,
        inode=before.st_ino,
        mode=before.st_mode,
        size=before.st_size,
        modified_ns=before.st_mtime_ns,
        changed_ns=before.st_ctime_ns,
        file_type="symlink",
        symlink_target=raw_target,
    )


def _capture_reconcile_activation_paths(
    *,
    home: Path,
) -> dict[str, BootstrapActivationPath]:
    """Capture the exact legacy/stable paths a staged activation may replace."""
    lexical_home = Path(os.path.abspath(home.expanduser()))
    share = lexical_home / ".local/share/clio-relay"
    current = _capture_activation_path(
        share / "current",
        kind="symlink",
        maximum=4096,
        allow_absent=True,
    )
    if current.before is not None:
        current_target = _activation_symlink_lexical_target(current)
        try:
            relative = current_target.resolve(strict=True).relative_to(
                (share / "generations").resolve(strict=True)
            )
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(
                "active generation pointer does not name one managed generation"
            ) from exc
        if (
            len(relative.parts) != 1
            or len(relative.name) != 64
            or any(character not in "0123456789abcdef" for character in relative.name)
        ):
            raise ConfigurationError(
                "active generation pointer does not name one managed generation"
            )
    managed = _capture_activation_path(
        _expand_home(MANAGED_JARVIS_REPO_PATH, lexical_home),
        kind="symlink",
        maximum=4096,
        allow_absent=True,
    )
    if managed.before is not None:
        managed_target = _activation_symlink_lexical_target(managed)
        allowed_targets = {
            lexical_home / ".local/src/clio-relay/jarvis-packages/clio_relay",
            share / "current/source/jarvis-packages/clio_relay",
        }
        try:
            target_matches_allowed_alias = any(
                managed_target.resolve(strict=True) == target.resolve(strict=True)
                for target in allowed_targets
            )
        except (OSError, RuntimeError, ValueError):
            target_matches_allowed_alias = False
        if (
            managed_target not in allowed_targets
            and not target_matches_allowed_alias
            and not _is_generation_repository_target(
                managed_target.resolve(strict=True),
                home=lexical_home.resolve(strict=True),
            )
        ):
            raise ConfigurationError(
                "relay-managed repository link is not one proven legacy binding"
            )
    paths = {
        "current": current,
        "install_receipt": _capture_activation_path(
            share / "install-receipt.json",
            kind="file_or_symlink",
            maximum=4 * 1024 * 1024,
            allow_absent=False,
        ),
        "relay_launcher": _capture_activation_path(
            lexical_home / ".local/bin/clio-relay",
            kind="file_or_symlink",
            maximum=1024 * 1024,
            allow_absent=False,
        ),
        "jarvis_launcher": _capture_activation_path(
            lexical_home / ".local/bin/jarvis",
            kind="file_or_symlink",
            maximum=1024 * 1024,
            allow_absent=False,
        ),
        "managed_repo": managed,
    }
    for name in ("relay_launcher", "jarvis_launcher"):
        launcher = Path(paths[name].path)
        try:
            target = launcher.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(f"legacy activation launcher is unavailable: {name}") from exc
        if not target.is_file() or not os.access(target, os.X_OK):
            raise ConfigurationError(f"legacy activation launcher is not executable: {name}")
    return paths


def _activation_symlink_lexical_target(path: BootstrapActivationPath) -> Path:
    """Return one captured symlink target without resolving its final object."""
    if path.before is None or path.before.file_type != "symlink":
        raise ConfigurationError(f"activation path is not a captured symlink: {path.path}")
    raw_target = path.before.symlink_target
    if raw_target is None:  # pragma: no cover - enforced by the model
        raise ConfigurationError(f"activation path omitted its symlink target: {path.path}")
    candidate = Path(raw_target)
    if not candidate.is_absolute():
        candidate = Path(path.path).parent / candidate
    return Path(os.path.abspath(candidate))


def _reconcile_activation_symlink(
    snapshot: BootstrapActivationPath,
    *,
    expected_target: Path,
    label: str,
    exchange_identity: str | None = None,
) -> str:
    """Atomically publish one stable link from either its snapshot or desired state."""
    path = Path(snapshot.path)
    target = Path(os.path.abspath(expected_target.expanduser()))
    token = exchange_identity or hashlib.sha256(f"{snapshot.path}\0{target}".encode()).hexdigest()
    try:
        _require_sha256(token, field="activation_exchange_identity")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    temporary = path.with_name(f".{path.name}.{token}.exchange")
    action = "created" if snapshot.before is None else "retargeted"
    if temporary.exists() or temporary.is_symlink():
        try:
            active = _capture_activation_object(
                path,
                kind=snapshot.kind,
                maximum=4 * 1024 * 1024,
            )
            displaced = _capture_activation_object(
                temporary,
                kind=snapshot.kind,
                maximum=4 * 1024 * 1024,
            )
        except ConfigurationError as exc:
            raise ConfigurationError(
                f"{label} exchange recovery found an invalid path state"
            ) from exc
        active_is_desired = bool(
            active.file_type == "symlink" and active.symlink_target == str(target)
        )
        displaced_is_desired = bool(
            displaced.file_type == "symlink" and displaced.symlink_target == str(target)
        )
        active_is_before = bool(
            snapshot.before is not None
            and _activation_identity_matches_after_rename(snapshot.before, active)
        )
        displaced_is_before = bool(
            snapshot.before is not None
            and _activation_identity_matches_after_rename(snapshot.before, displaced)
        )
        if active_is_desired and displaced_is_before:
            _verify_stable_symlink(path, expected=target, label=label)
            temporary.unlink()
            _fsync_directory(path.parent)
            return action
        if active_is_before and displaced_is_desired:
            temporary.unlink()
            _fsync_directory(path.parent)
        else:
            raise ConfigurationError(
                f"{label} exchange recovery found unproven path states: {path}, {temporary}"
            )
    try:
        before = path.lstat()
        raw_target = os.readlink(path)
        if raw_target != str(target):
            raise ConfigurationError(f"{label} does not use its canonical target")
        _verify_stable_symlink(path, expected=target, label=label)
        if _stat_identity(path.lstat()) != _stat_identity(before):
            raise ConfigurationError(f"{label} changed during inspection")
    except (ConfigurationError, OSError, RuntimeError, ValueError):
        pass
    else:
        return "reused"
    try:
        observed = _activation_path_identity(snapshot)
    except ConfigurationError:
        if snapshot.before is not None:
            raise ConfigurationError(f"{label} changed after bootstrap inspection") from None
        try:
            path.lstat()
        except FileNotFoundError:
            observed = None
        except OSError as exc:
            raise ConfigurationError(f"{label} could not be classified") from exc
        else:
            raise ConfigurationError(f"{label} appeared after bootstrap inspection") from None
    else:
        if snapshot.before is None or observed != snapshot.before:
            raise ConfigurationError(f"{label} changed after bootstrap inspection")
    if snapshot.before is None:
        try:
            path.symlink_to(target, target_is_directory=target.is_dir())
            _fsync_directory(path.parent)
        except FileExistsError as exc:
            raise ConfigurationError(f"{label} appeared before activation") from exc
        _verify_stable_symlink(path, expected=target, label=label)
        if os.readlink(path) != str(target):  # pragma: no cover - written above
            raise ConfigurationError(f"{label} did not use its canonical target")
        return action
    exchanged = False
    try:
        temporary.symlink_to(target, target_is_directory=target.is_dir())
        desired = _capture_activation_object(
            temporary,
            kind="symlink",
            maximum=4096,
        )
        if _activation_path_identity(snapshot) != snapshot.before:
            raise ConfigurationError(f"{label} changed before activation")
        exchanged = True
        _atomic_exchange_paths(temporary, path)
        try:
            displaced = _capture_activation_object(
                temporary,
                kind=snapshot.kind,
                maximum=4 * 1024 * 1024,
            )
        except ConfigurationError:
            displaced = None
        if displaced is None or not _activation_identity_matches_after_rename(
            snapshot.before, displaced
        ):
            try:
                active = _capture_activation_object(
                    path,
                    kind="symlink",
                    maximum=4096,
                )
            except ConfigurationError as exc:
                raise ConfigurationError(
                    f"{label} changed during atomic activation; "
                    f"displaced state retained at {temporary}"
                ) from exc
            if not _activation_identity_matches_after_rename(desired, active):
                raise ConfigurationError(
                    f"{label} changed during atomic activation; "
                    f"displaced state retained at {temporary}"
                )
            _atomic_exchange_paths(temporary, path)
            exchanged = False
            _fsync_directory(path.parent)
            raise ConfigurationError(f"{label} changed before atomic activation")
        try:
            active = _capture_activation_object(
                path,
                kind="symlink",
                maximum=4096,
            )
        except ConfigurationError as exc:
            temporary.unlink()
            exchanged = False
            _fsync_directory(path.parent)
            raise ConfigurationError(f"{label} changed after atomic activation") from exc
        if not _activation_identity_matches_after_rename(desired, active):
            temporary.unlink()
            exchanged = False
            _fsync_directory(path.parent)
            raise ConfigurationError(f"{label} changed after atomic activation")
        temporary.unlink()
        exchanged = False
        _fsync_directory(path.parent)
    except BaseException:
        if not exchanged:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise
    _verify_stable_symlink(path, expected=target, label=label)
    if os.readlink(path) != str(target):  # pragma: no cover - written above
        raise ConfigurationError(f"{label} did not use its canonical target")
    return action


def reconcile_staged_activation_links(
    plan: BootstrapReconcilePlan,
    *,
    generation: Path,
    home: Path | None = None,
) -> dict[str, object]:
    """Idempotently finish one fenced generation activation after any crash boundary."""
    if plan.mode not in {"relay-only", "component-upgrade"}:
        raise ConfigurationError("staged activation requires a replacement reconcile plan")
    expected_names = {
        "current",
        "install_receipt",
        "relay_launcher",
        "jarvis_launcher",
        "managed_repo",
    }
    if set(plan.activation_paths) != expected_names:
        raise ConfigurationError("staged activation plan omitted its path identities")
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    expected_generation = (
        lexical_home / ".local/share/clio-relay/generations" / plan.desired_fingerprint
    )
    if generation != expected_generation or generation.is_symlink() or not generation.is_dir():
        raise ConfigurationError("staged activation generation path is invalid")
    targets = {
        "current": generation,
        "install_receipt": lexical_home / ".local/share/clio-relay/current/install-receipt.json",
        "relay_launcher": lexical_home / ".local/share/clio-relay/current/bin/clio-relay",
        "jarvis_launcher": lexical_home / ".local/share/clio-relay/current/bin/jarvis",
        "managed_repo": lexical_home
        / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay",
    }
    for name, target_path in {
        "current": lexical_home / ".local/share/clio-relay/current",
        "install_receipt": lexical_home / ".local/share/clio-relay/install-receipt.json",
        "relay_launcher": lexical_home / ".local/bin/clio-relay",
        "jarvis_launcher": lexical_home / ".local/bin/jarvis",
        "managed_repo": _expand_home(MANAGED_JARVIS_REPO_PATH, lexical_home),
    }.items():
        if Path(plan.activation_paths[name].path) != target_path:
            raise ConfigurationError(f"staged activation path destination changed: {name}")
    actions: dict[str, str] = {}
    for name in (
        "current",
        "install_receipt",
        "relay_launcher",
        "jarvis_launcher",
        "managed_repo",
    ):
        actions[name] = _reconcile_activation_symlink(
            plan.activation_paths[name],
            expected_target=targets[name],
            label=f"bootstrap stable activation path {name}",
            exchange_identity=plan.desired_fingerprint,
        )
    return {
        "schema_version": "clio-relay.bootstrap-activation.v1",
        "generation": plan.desired_fingerprint,
        "actions": actions,
    }


def _activation_identity_matches_after_rename(
    before: BootstrapActivationPathIdentity,
    after: BootstrapActivationPathIdentity,
) -> bool:
    """Compare a captured activation object after an atomic pathname exchange."""
    return bool(
        before.device == after.device
        and before.inode == after.inode
        and before.mode == after.mode
        and before.size == after.size
        and before.modified_ns == after.modified_ns
        and before.file_type == after.file_type
        and before.sha256 == after.sha256
        and before.symlink_target == after.symlink_target
    )
