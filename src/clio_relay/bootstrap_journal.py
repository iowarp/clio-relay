"""Dependency-free crash journal operations for first-install bootstrap.

This module deliberately uses only the Python standard library.  A virgin
cluster has no relay-managed Python environment yet, so the bootstrap shell
must be able to create and recover its journal with the system interpreter
before downloading or installing anything.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, cast

BOOTSTRAP_TRANSACTION_SCHEMA = "clio-relay.bootstrap-transaction.v1"
MAX_JOURNAL_BYTES = 1024 * 1024
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,160}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_KINDS = {"directory", "file", "file_or_symlink", "symlink"}
_TERMINAL_STATES = {"committed", "recovered"}
_GETUID = cast(Callable[[], int] | None, getattr(os, "getuid", None))
_FCHMOD = cast(Callable[[int, int], None] | None, getattr(os, "fchmod", None))
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_TRANSITIONS: dict[str, frozenset[str]] = {
    "locked": frozenset({"recovering", "inspected"}),
    "recovering": frozenset(
        {
            "inspected",
            "migration_started",
            "migrated",
            "starting",
            "service_verified",
            "committed",
        }
    ),
    "inspected": frozenset({"noop_verified", "preparing", "fencing"}),
    "noop_verified": frozenset({"committed"}),
    "preparing": frozenset({"prepared"}),
    "prepared": frozenset({"fencing", "activating"}),
    "fencing": frozenset({"fenced"}),
    "fenced": frozenset({"activating", "preparing"}),
    "activating": frozenset({"activated"}),
    "activated": frozenset({"migration_started"}),
    "migration_started": frozenset({"migrated"}),
    "migrated": frozenset({"starting", "service_verified"}),
    "starting": frozenset({"service_verified"}),
    "service_verified": frozenset({"committed"}),
    "committed": frozenset(),
    "recovered": frozenset(),
}


class BootstrapJournalError(RuntimeError):
    """Raised when a bootstrap journal or recovery boundary is unsafe."""


def create_journal(
    path: Path,
    *,
    invocation_id: str,
    desired_fingerprint: str,
    mode: str,
    owned_paths: dict[str, dict[str, str]],
    service_name: str | None,
    service_was_active: bool | None,
    service_was_enabled: bool | None,
) -> dict[str, Any]:
    """Create one fsync-backed journal after proving every owned path absent."""
    if not _IDENTIFIER.fullmatch(invocation_id):
        raise BootstrapJournalError("bootstrap invocation identity is invalid")
    if not _SHA256.fullmatch(desired_fingerprint):
        raise BootstrapJournalError("bootstrap desired fingerprint is invalid")
    if mode not in {"full", "relay-only", "component-upgrade", "repair"}:
        raise BootstrapJournalError("bootstrap transaction mode is invalid")
    normalized_owned_paths = _coerce_owned_paths(owned_paths)
    for item in normalized_owned_paths.values():
        target = Path(item["path"])
        if _entry_exists_without_following(target):
            raise BootstrapJournalError(f"bootstrap-owned path already exists: {target}")
    now = datetime.now(UTC).isoformat()
    value: dict[str, Any] = {
        "schema_version": BOOTSTRAP_TRANSACTION_SCHEMA,
        "invocation_id": invocation_id,
        "desired_fingerprint": desired_fingerprint,
        "mode": mode,
        "state": "locked",
        "started_at": now,
        "updated_at": now,
        "previous_generation": None,
        "prepared_generation": None,
        "service_name": service_name,
        "service_was_active": service_was_active,
        "service_was_enabled": service_was_enabled,
        "recovered_from": None,
        "irreversible_boundary": False,
        "owned_paths": normalized_owned_paths,
        "phase_identities": {"locked": desired_fingerprint},
    }
    _validate_journal(value)
    with _open_journal_parent(path, create=True) as (parent_descriptor, parent, name):
        previous_identity = _journal_identity_if_present(parent_descriptor, parent, name)
        _atomic_json_at(
            parent_descriptor,
            parent,
            name,
            value,
            expected_identity=previous_identity,
        )
    return value


def load_journal(path: Path) -> dict[str, Any]:
    """Load one bounded, owner-controlled regular journal."""
    with _open_journal_parent(path) as (parent_descriptor, parent, name):
        value, _ = _load_journal_at(parent_descriptor, parent, name)
        return value


def advance_journal(
    path: Path,
    state: str,
    *,
    prepared_generation: str | None = None,
) -> dict[str, Any]:
    """Advance a journal through one allowed durable state transition."""
    with _open_journal_parent(path) as (parent_descriptor, parent, name):
        value, identity = _load_journal_at(parent_descriptor, parent, name)
        current = _string(value, "state")
        if state not in _TRANSITIONS[current]:
            raise BootstrapJournalError(
                f"invalid bootstrap transaction transition: {current} -> {state}"
            )
        if state == "prepared":
            if prepared_generation is None or not _SHA256.fullmatch(prepared_generation):
                raise BootstrapJournalError("prepared generation identity is invalid")
            value["prepared_generation"] = prepared_generation
        elif prepared_generation is not None:
            raise BootstrapJournalError("prepared generation is only valid for prepared state")
        value["state"] = state
        if state == "migration_started":
            value["irreversible_boundary"] = True
        value["updated_at"] = datetime.now(UTC).isoformat()
        _validate_journal(value)
        _atomic_json_at(
            parent_descriptor,
            parent,
            name,
            value,
            expected_identity=identity,
        )
        return value


def record_phase(path: Path, phase: str, identity: str) -> dict[str, Any]:
    """Bind one completed phase to an exact SHA-256 identity."""
    if not _IDENTIFIER.fullmatch(phase):
        raise BootstrapJournalError("bootstrap phase name is invalid")
    if not _SHA256.fullmatch(identity):
        raise BootstrapJournalError("bootstrap phase identity is invalid")
    with _open_journal_parent(path) as (parent_descriptor, parent, name):
        value, journal_identity = _load_journal_at(parent_descriptor, parent, name)
        if _string(value, "state") in _TERMINAL_STATES:
            raise BootstrapJournalError("terminal bootstrap transaction cannot record a phase")
        phases = _coerce_phase_identities(value["phase_identities"])
        previous = phases.get(phase)
        if previous is not None and previous != identity:
            raise BootstrapJournalError(f"bootstrap phase identity changed: {phase}")
        phases[phase] = identity
        value["phase_identities"] = phases
        value["updated_at"] = datetime.now(UTC).isoformat()
        _validate_journal(value)
        _atomic_json_at(
            parent_descriptor,
            parent,
            name,
            value,
            expected_identity=journal_identity,
        )
        return value


def record_owned_path(path: Path, owned_name: str) -> dict[str, Any]:
    """Persist the exact identity of one transaction-created path."""
    if not _IDENTIFIER.fullmatch(owned_name):
        raise BootstrapJournalError("bootstrap owned path name is invalid")
    with _open_journal_parent(path) as (parent_descriptor, parent, name):
        value, journal_identity = _load_journal_at(parent_descriptor, parent, name)
        if _string(value, "state") in _TERMINAL_STATES:
            raise BootstrapJournalError("terminal bootstrap transaction cannot claim a path")
        owned = _coerce_owned_paths(value["owned_paths"])
        item = owned.get(owned_name)
        if item is None:
            raise BootstrapJournalError(f"bootstrap owned path is unknown: {owned_name}")
        target = Path(item["path"])
        if os.name == "nt":
            observed = _owned_identity(target, kind=item["kind"])
        else:
            home = _home_for_journal_parent(parent)
            with _open_absolute_directory(home) as home_descriptor:
                _require_current_owner(home_descriptor, "bootstrap ownership home")
                _verify_journal_parent_from_home(
                    home_descriptor,
                    home,
                    parent_descriptor,
                    parent,
                )
                observed = _owned_identity_from_home(
                    home_descriptor,
                    home,
                    target,
                    kind=item["kind"],
                )
                _verify_journal_parent_from_home(
                    home_descriptor,
                    home,
                    parent_descriptor,
                    parent,
                )
        previous = item["identity"]
        if previous is not None and previous != observed:
            raise BootstrapJournalError(f"bootstrap owned path identity changed: {owned_name}")
        item["identity"] = observed
        value["owned_paths"] = owned
        value["updated_at"] = datetime.now(UTC).isoformat()
        _validate_journal(value)
        _atomic_json_at(
            parent_descriptor,
            parent,
            name,
            value,
            expected_identity=journal_identity,
        )
        return value


def create_owned_directory(path: Path, owned_name: str) -> dict[str, Any]:
    """Exclusively create and durably bind one empty owned directory."""

    def create(parent_descriptor: int, target_name: str) -> None:
        os.mkdir(target_name, mode=0o700, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)

    return _create_and_record_owned_path(path, owned_name, create=create)


def create_owned_symlink(path: Path, owned_name: str, target: str) -> dict[str, Any]:
    """Exclusively create and durably bind one owned symbolic link."""
    if not target or any(character in target for character in "\x00\r\n"):
        raise BootstrapJournalError("bootstrap owned symlink target is invalid")

    def create(parent_descriptor: int, target_name: str) -> None:
        os.symlink(target, target_name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)

    return _create_and_record_owned_path(path, owned_name, create=create)


def copy_owned_file(
    path: Path,
    owned_name: str,
    source: Path,
    *,
    mode: int,
) -> dict[str, Any]:
    """Exclusively copy one regular file and durably bind its identity."""
    if mode not in {0o600, 0o700, 0o755}:
        raise BootstrapJournalError("bootstrap owned file mode is invalid")
    normalized_source = _normalized_absolute(source, "bootstrap owned file source")

    def create(parent_descriptor: int, target_name: str) -> None:
        with _open_absolute_directory(normalized_source.parent) as source_parent:
            source_descriptor = os.open(
                normalized_source.name,
                os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC,
                dir_fd=source_parent,
            )
            target_descriptor: int | None = None
            target_identity: tuple[int, int, int] | None = None
            try:
                source_details = os.fstat(source_descriptor)
                if not stat.S_ISREG(source_details.st_mode):
                    raise BootstrapJournalError("bootstrap owned file source is not regular")
                linked_source = os.stat(
                    normalized_source.name,
                    dir_fd=source_parent,
                    follow_symlinks=False,
                )
                if _stat_identity(source_details) != _stat_identity(linked_source):
                    raise BootstrapJournalError("bootstrap owned file source identity changed")
                target_descriptor = os.open(
                    target_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW | _O_CLOEXEC,
                    mode,
                    dir_fd=parent_descriptor,
                )
                if _FCHMOD is not None:
                    _FCHMOD(target_descriptor, mode)
                target_identity = _stat_identity(os.fstat(target_descriptor))
                while chunk := os.read(source_descriptor, 1024 * 1024):
                    view = memoryview(chunk)
                    while view:
                        written = os.write(target_descriptor, view)
                        view = view[written:]
                os.fsync(target_descriptor)
                source_after = os.stat(
                    normalized_source.name,
                    dir_fd=source_parent,
                    follow_symlinks=False,
                )
                if (
                    _stat_identity(source_details) != _stat_identity(source_after)
                    or source_details.st_size != source_after.st_size
                    or source_details.st_ctime_ns != source_after.st_ctime_ns
                ):
                    raise BootstrapJournalError("bootstrap owned file source identity changed")
            except BaseException:
                if target_descriptor is not None and target_identity is not None:
                    with suppress(OSError):
                        observed = os.stat(
                            target_name,
                            dir_fd=parent_descriptor,
                            follow_symlinks=False,
                        )
                        if _stat_identity(observed) == target_identity:
                            os.unlink(target_name, dir_fd=parent_descriptor)
                            os.fsync(parent_descriptor)
                raise
            finally:
                if target_descriptor is not None:
                    os.close(target_descriptor)
                os.close(source_descriptor)
        os.fsync(parent_descriptor)

    return _create_and_record_owned_path(path, owned_name, create=create)


def _create_and_record_owned_path(
    path: Path,
    owned_name: str,
    *,
    create: Callable[[int, str], None],
) -> dict[str, Any]:
    if os.name == "nt":
        raise BootstrapJournalError("owned path publication requires POSIX")
    if not _IDENTIFIER.fullmatch(owned_name):
        raise BootstrapJournalError("bootstrap owned path name is invalid")
    with _open_journal_parent(path) as (journal_fd, journal_parent, journal_name):
        value, journal_identity = _load_journal_at(journal_fd, journal_parent, journal_name)
        if _string(value, "state") in _TERMINAL_STATES:
            raise BootstrapJournalError("terminal bootstrap transaction cannot create a path")
        owned = _coerce_owned_paths(value["owned_paths"])
        item = owned.get(owned_name)
        if item is None:
            raise BootstrapJournalError(f"bootstrap owned path is unknown: {owned_name}")
        if item["identity"] is not None:
            raise BootstrapJournalError(f"bootstrap owned path was already created: {owned_name}")
        target = Path(item["path"])
        home = _home_for_journal_parent(journal_parent)
        _validate_recovery_target(target, kind=item["kind"], home=home, name=owned_name)
        try:
            relative = target.relative_to(home)
        except ValueError as exc:
            raise BootstrapJournalError("bootstrap owned path escaped its home") from exc
        with _open_absolute_directory(home) as home_descriptor:
            _require_current_owner(home_descriptor, "bootstrap ownership home")
            _verify_journal_parent_from_home(
                home_descriptor,
                home,
                journal_fd,
                journal_parent,
            )
            target_parent = _open_target_parent_at(home_descriptor, relative.parent)
            if target_parent is None:
                raise BootstrapJournalError(f"bootstrap owned path parent is unavailable: {target}")
            try:
                _verify_target_parent_identity(home_descriptor, relative.parent, target_parent)
                try:
                    os.stat(relative.name, dir_fd=target_parent, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise BootstrapJournalError(f"bootstrap-owned path already exists: {target}")
                create(target_parent, relative.name)
                details = os.stat(relative.name, dir_fd=target_parent, follow_symlinks=False)
                observed = _owned_identity_at(
                    target_parent,
                    relative.name,
                    details,
                    display=target,
                )
                if not _kind_accepts_identity(item["kind"], observed):
                    raise BootstrapJournalError(f"bootstrap owned path kind changed: {target}")
                item["identity"] = observed
                value["owned_paths"] = owned
                value["updated_at"] = datetime.now(UTC).isoformat()
                _validate_journal(value)
                _atomic_json_at(
                    journal_fd,
                    journal_parent,
                    journal_name,
                    value,
                    expected_identity=journal_identity,
                )
                current = _owned_identity_at(
                    target_parent,
                    relative.name,
                    os.stat(relative.name, dir_fd=target_parent, follow_symlinks=False),
                    display=target,
                )
                if not _owned_identity_matches(observed, current):
                    raise BootstrapJournalError(
                        f"bootstrap owned path changed during publication: {target}"
                    )
                _verify_target_parent_identity(home_descriptor, relative.parent, target_parent)
            finally:
                os.close(target_parent)
            _verify_journal_parent_from_home(
                home_descriptor,
                home,
                journal_fd,
                journal_parent,
            )
        return value


def recovery_plan(path: Path) -> dict[str, Any]:
    """Return one strict journal plus its only safe recovery direction."""
    value = load_journal(path)
    result = dict(value)
    result["recovery_mode"] = _recovery_mode(value)
    return result


def discard_full_transaction(path: Path, *, home: Path) -> dict[str, Any]:
    """Discard only absent-before paths owned by a reversible full transaction."""
    normalized_home = _normalized_absolute(home, "bootstrap recovery home")
    with _open_journal_parent(path) as (journal_fd, journal_parent, journal_name):
        value, journal_identity = _load_journal_at(journal_fd, journal_parent, journal_name)
        if value["mode"] != "full" or _recovery_mode(value) != "discard":
            raise BootstrapJournalError("bootstrap transaction is not safely discardable")
        owned = _coerce_owned_paths(value["owned_paths"])
        ordered = sorted(
            owned.items(),
            key=lambda item: len(Path(item[1]["path"]).parts),
            reverse=True,
        )
        if os.name == "nt":
            raise BootstrapJournalError("full bootstrap recovery requires POSIX")
        else:
            with _open_absolute_directory(normalized_home) as home_descriptor:
                _require_current_owner(home_descriptor, "bootstrap recovery home")
                _verify_journal_parent_from_home(
                    home_descriptor,
                    normalized_home,
                    journal_fd,
                    journal_parent,
                )
                for owned_name, item in ordered:
                    target = Path(item["path"])
                    _validate_recovery_target(
                        target,
                        kind=item["kind"],
                        home=normalized_home,
                        name=owned_name,
                    )
                    _discard_owned_posix(
                        home_descriptor,
                        normalized_home,
                        target,
                        kind=item["kind"],
                        expected_identity=item["identity"],
                    )
                _verify_journal_parent_from_home(
                    home_descriptor,
                    normalized_home,
                    journal_fd,
                    journal_parent,
                )
        value["recovered_from"] = value["state"]
        value["state"] = "recovered"
        value["updated_at"] = datetime.now(UTC).isoformat()
        _validate_journal(value)
        _atomic_json_at(
            journal_fd,
            journal_parent,
            journal_name,
            value,
            expected_identity=journal_identity,
        )
        return value


def _validate_journal(value: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "invocation_id",
        "desired_fingerprint",
        "mode",
        "state",
        "started_at",
        "updated_at",
        "previous_generation",
        "prepared_generation",
        "service_name",
        "service_was_active",
        "service_was_enabled",
        "recovered_from",
        "irreversible_boundary",
        "owned_paths",
        "phase_identities",
    }
    if set(value) != required:
        raise BootstrapJournalError("bootstrap transaction journal fields are invalid")
    if value["schema_version"] != BOOTSTRAP_TRANSACTION_SCHEMA:
        raise BootstrapJournalError("bootstrap transaction journal schema is invalid")
    if not _IDENTIFIER.fullmatch(_string(value, "invocation_id")):
        raise BootstrapJournalError("bootstrap invocation identity is invalid")
    if not _SHA256.fullmatch(_string(value, "desired_fingerprint")):
        raise BootstrapJournalError("bootstrap desired fingerprint is invalid")
    if value["mode"] not in {"full", "relay-only", "component-upgrade", "repair"}:
        raise BootstrapJournalError("bootstrap transaction mode is invalid")
    state = _string(value, "state")
    if state not in _TRANSITIONS:
        raise BootstrapJournalError("bootstrap transaction state is invalid")
    if value["recovered_from"] is not None and value["recovered_from"] not in _TRANSITIONS:
        raise BootstrapJournalError("bootstrap recovered state is invalid")
    for field in ("service_was_active", "service_was_enabled"):
        if value[field] is not None and not isinstance(value[field], bool):
            raise BootstrapJournalError(f"bootstrap {field} is invalid")
    if not isinstance(value["irreversible_boundary"], bool):
        raise BootstrapJournalError("bootstrap irreversible boundary is invalid")
    prepared = value["prepared_generation"]
    if prepared is not None and (not isinstance(prepared, str) or not _SHA256.fullmatch(prepared)):
        raise BootstrapJournalError("prepared generation identity is invalid")
    if (
        state
        in {
            "prepared",
            "activating",
            "activated",
            "migration_started",
            "migrated",
            "starting",
            "service_verified",
            "committed",
        }
        and prepared is None
    ):
        raise BootstrapJournalError("prepared generation is required after preparation")
    if (
        state
        in {
            "migration_started",
            "migrated",
            "starting",
            "service_verified",
            "committed",
        }
        and value["irreversible_boundary"] is not True
    ):
        raise BootstrapJournalError("queue migration state omitted its irreversible boundary")
    _coerce_owned_paths(value["owned_paths"])
    _coerce_phase_identities(value["phase_identities"])


def _coerce_owned_paths(value: object) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        raise BootstrapJournalError("bootstrap owned paths are invalid")
    raw_value = cast(dict[object, object], value)
    if len(raw_value) > 128:
        raise BootstrapJournalError("bootstrap owned path count exceeds its bound")
    observed: set[str] = set()
    normalized_value: dict[str, dict[str, Any]] = {}
    for raw_name, raw_item in raw_value.items():
        name = raw_name if isinstance(raw_name, str) else ""
        item = cast(dict[object, object], raw_item) if isinstance(raw_item, dict) else {}
        if not _IDENTIFIER.fullmatch(name):
            raise BootstrapJournalError("bootstrap owned path name is invalid")
        if set(item) not in ({"path", "kind"}, {"path", "kind", "identity"}):
            raise BootstrapJournalError("bootstrap owned path record is invalid")
        path = item.get("path")
        kind = item.get("kind")
        if not isinstance(path, str) or any(character in path for character in "\x00\r\n"):
            raise BootstrapJournalError("bootstrap owned path is invalid")
        candidate = Path(path)
        if not candidate.is_absolute() or ".." in candidate.parts:
            raise BootstrapJournalError("bootstrap owned path must be absolute and normalized")
        normalized = os.path.normpath(path)
        if normalized != path or normalized in observed:
            raise BootstrapJournalError("bootstrap owned path identity is ambiguous")
        if kind not in _KINDS:
            raise BootstrapJournalError("bootstrap owned path kind is invalid")
        identity = _coerce_owned_identity(item.get("identity"))
        if identity is not None and not _kind_accepts_identity(cast(str, kind), identity):
            raise BootstrapJournalError("bootstrap owned path kind and identity disagree")
        observed.add(normalized)
        normalized_value[name] = {
            "path": path,
            "kind": cast(str, kind),
            "identity": identity,
        }
    return normalized_value


def _coerce_owned_identity(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise BootstrapJournalError("bootstrap owned path identity is invalid")
    raw = cast(dict[object, object], value)
    required = {
        "device",
        "inode",
        "file_type",
        "changed_ns",
        "size",
        "sha256",
        "symlink_target",
    }
    if set(raw) != required:
        raise BootstrapJournalError("bootstrap owned path identity is invalid")
    for field in ("device", "inode", "changed_ns", "size"):
        observed = raw[field]
        if not isinstance(observed, int) or isinstance(observed, bool) or observed < 0:
            raise BootstrapJournalError("bootstrap owned path identity is invalid")
    file_type = raw["file_type"]
    if file_type not in {"directory", "file", "symlink"}:
        raise BootstrapJournalError("bootstrap owned path identity is invalid")
    digest = raw["sha256"]
    link_target = raw["symlink_target"]
    if file_type == "file":
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest) or link_target is not None:
            raise BootstrapJournalError("bootstrap owned file identity is invalid")
    elif file_type == "symlink":
        if digest is not None or not isinstance(link_target, str) or "\x00" in link_target:
            raise BootstrapJournalError("bootstrap owned symlink identity is invalid")
    elif digest is not None or link_target is not None:
        raise BootstrapJournalError("bootstrap owned directory identity is invalid")
    return {
        "device": raw["device"],
        "inode": raw["inode"],
        "file_type": file_type,
        "changed_ns": raw["changed_ns"],
        "size": raw["size"],
        "sha256": digest,
        "symlink_target": link_target,
    }


def _kind_accepts_identity(kind: str, identity: dict[str, Any]) -> bool:
    file_type = identity["file_type"]
    if kind == "directory":
        return file_type == "directory"
    if kind == "file":
        return file_type == "file"
    if kind == "symlink":
        return file_type == "symlink"
    return file_type in {"file", "symlink"}


def _coerce_phase_identities(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        raise BootstrapJournalError("bootstrap phase identities are invalid")
    phases: dict[str, str] = {}
    for raw_phase, raw_identity in cast(dict[object, object], value).items():
        if (
            not isinstance(raw_phase, str)
            or not _IDENTIFIER.fullmatch(raw_phase)
            or not isinstance(raw_identity, str)
            or not _SHA256.fullmatch(raw_identity)
        ):
            raise BootstrapJournalError("bootstrap phase identity is invalid")
        phases[raw_phase] = raw_identity
    return phases


def _validate_recovery_target(target: Path, *, kind: str, home: Path, name: str) -> None:
    resolved_home = _normalized_absolute(home, "bootstrap recovery home")
    lexical = _normalized_absolute(target, f"bootstrap owned path {name}")
    private_root = resolved_home / ".local/share/clio-relay"
    exact_roots = {
        resolved_home / ".local/src/clio-relay",
        resolved_home / ".local/src/jarvis-util",
        resolved_home / ".ppi-jarvis",
    }
    exact_bin = {
        resolved_home / ".local/bin/frpc",
        resolved_home / ".local/bin/frps",
        resolved_home / ".local/bin/uv",
        resolved_home / ".local/bin/uvx",
        resolved_home / ".local/bin/clio-relay",
        resolved_home / ".local/bin/jarvis",
    }
    under_private = lexical != private_root and private_root in lexical.parents
    if lexical not in exact_roots and lexical not in exact_bin and not under_private:
        raise BootstrapJournalError(f"owned recovery path is outside relay boundaries: {name}")
    if lexical in exact_bin and kind not in {"file", "symlink"}:
        raise BootstrapJournalError(f"owned binary path has an invalid kind: {name}")


def _recovery_mode(value: dict[str, Any]) -> str:
    state = _string(value, "state")
    if state in _TERMINAL_STATES:
        return "none"
    if value["irreversible_boundary"] is True:
        return "forward"
    if value["mode"] == "full":
        return "discard"
    if state in {"activating", "activated"}:
        return "rollback"
    return "discard"


def _normalized_absolute(path: Path, description: str) -> Path:
    raw = str(path)
    if not path.is_absolute() or os.path.normpath(raw) != raw or ".." in path.parts:
        raise BootstrapJournalError(f"{description} must be absolute and normalized")
    return path


@contextmanager
def _open_absolute_directory(path: Path, *, create: bool = False) -> Generator[int]:
    """Open an absolute directory chain without following any symbolic link."""
    normalized = _normalized_absolute(path, "bootstrap directory")
    if os.name == "nt":
        raise BootstrapJournalError("descriptor-pinned traversal requires POSIX")
    flags = os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC
    descriptor = os.open(os.sep, flags)
    try:
        for component in normalized.parts[1:]:
            try:
                next_descriptor = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                    os.fsync(descriptor)
                except FileExistsError:
                    pass
                next_descriptor = os.open(component, flags, dir_fd=descriptor)
            except OSError as exc:
                raise BootstrapJournalError(
                    f"bootstrap directory topology is unsafe: {normalized}"
                ) from exc
            os.close(descriptor)
            descriptor = next_descriptor
        yield descriptor
    finally:
        os.close(descriptor)


@contextmanager
def _open_journal_parent(
    path: Path,
    *,
    create: bool = False,
) -> Generator[tuple[int | None, Path, str]]:
    """Pin and verify the private directory containing a bootstrap journal."""
    normalized = _normalized_absolute(path, "bootstrap transaction journal")
    if not normalized.name or normalized.name in {".", ".."}:
        raise BootstrapJournalError("bootstrap transaction journal name is invalid")
    parent = normalized.parent
    if os.name == "nt":
        if create:
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            details = parent.lstat()
        except OSError as exc:
            raise BootstrapJournalError("bootstrap journal parent is invalid") from exc
        if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
            raise BootstrapJournalError("bootstrap journal parent must be a real directory")
        identity = details.st_dev, details.st_ino
        try:
            yield None, parent, normalized.name
        finally:
            observed = parent.lstat()
            if (observed.st_dev, observed.st_ino) != identity:
                raise BootstrapJournalError("bootstrap journal parent identity changed")
        return
    try:
        with _open_absolute_directory(parent, create=create) as descriptor:
            details = os.fstat(descriptor)
            wrong_owner = _GETUID is not None and details.st_uid != _GETUID()
            if (
                not stat.S_ISDIR(details.st_mode)
                or wrong_owner
                or stat.S_IMODE(details.st_mode) & 0o077
            ):
                raise BootstrapJournalError("bootstrap journal parent must be owner-private")
            identity = details.st_dev, details.st_ino
            try:
                yield descriptor, parent, normalized.name
            finally:
                try:
                    with _open_absolute_directory(parent) as observed_descriptor:
                        observed = os.fstat(observed_descriptor)
                except (FileNotFoundError, BootstrapJournalError, OSError) as exc:
                    raise BootstrapJournalError(
                        "bootstrap journal parent identity changed"
                    ) from exc
                if (observed.st_dev, observed.st_ino) != identity:
                    raise BootstrapJournalError("bootstrap journal parent identity changed")
    except FileNotFoundError as exc:
        raise BootstrapJournalError("bootstrap journal parent does not exist") from exc


def _journal_identity_if_present(
    parent_descriptor: int | None,
    parent: Path,
    name: str,
) -> tuple[int, int] | None:
    try:
        details = (
            (parent / name).lstat()
            if parent_descriptor is None
            else os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        )
    except FileNotFoundError:
        return None
    _require_private_regular(details)
    return details.st_dev, details.st_ino


def _require_private_regular(details: os.stat_result) -> None:
    wrong_owner = _GETUID is not None and details.st_uid != _GETUID()
    unsafe_mode = os.name != "nt" and bool(stat.S_IMODE(details.st_mode) & 0o077)
    if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1 or wrong_owner or unsafe_mode:
        raise BootstrapJournalError("bootstrap transaction journal is not owner-private")


def _read_regular_at(
    parent_descriptor: int | None,
    parent: Path,
    name: str,
) -> tuple[str, tuple[int, int]]:
    descriptor: int | None = None
    try:
        if parent_descriptor is None:
            descriptor = os.open(parent / name, os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC)
        else:
            descriptor = os.open(
                name,
                os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC,
                dir_fd=parent_descriptor,
            )
        details = os.fstat(descriptor)
        _require_private_regular(details)
        if details.st_size < 1 or details.st_size > MAX_JOURNAL_BYTES:
            raise BootstrapJournalError("bootstrap transaction journal size is invalid")
        chunks: list[bytes] = []
        observed_size = 0
        while chunk := os.read(descriptor, min(64 * 1024, MAX_JOURNAL_BYTES + 1 - observed_size)):
            chunks.append(chunk)
            observed_size += len(chunk)
            if observed_size > MAX_JOURNAL_BYTES:
                raise BootstrapJournalError("bootstrap transaction journal exceeds its bound")
        linked = (
            (parent / name).lstat()
            if parent_descriptor is None
            else os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        )
        identity = (details.st_dev, details.st_ino)
        if observed_size != details.st_size or identity != (linked.st_dev, linked.st_ino):
            raise BootstrapJournalError("bootstrap transaction journal changed while reading")
        return b"".join(chunks).decode("utf-8"), identity
    except (OSError, UnicodeError) as exc:
        raise BootstrapJournalError("bootstrap transaction journal is invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _load_journal_at(
    parent_descriptor: int | None,
    parent: Path,
    name: str,
) -> tuple[dict[str, Any], tuple[int, int]]:
    raw, identity = _read_regular_at(parent_descriptor, parent, name)
    try:
        raw_value = cast(object, json.loads(raw))
    except json.JSONDecodeError as exc:
        raise BootstrapJournalError("bootstrap transaction journal is invalid") from exc
    if not isinstance(raw_value, dict):
        raise BootstrapJournalError("bootstrap transaction journal must contain one object")
    value = cast(dict[str, Any], raw_value)
    _validate_journal(value)
    return value, identity


def _atomic_json_at(
    parent_descriptor: int | None,
    parent: Path,
    name: str,
    value: dict[str, Any],
    *,
    expected_identity: tuple[int, int] | None,
) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary_name = f".{name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor: int | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW | _O_CLOEXEC
        if parent_descriptor is None:
            descriptor = os.open(parent / temporary_name, flags, 0o600)
        else:
            descriptor = os.open(temporary_name, flags, 0o600, dir_fd=parent_descriptor)
        if _FCHMOD is not None:
            _FCHMOD(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        _require_private_regular(os.fstat(descriptor))
        os.close(descriptor)
        descriptor = None
        observed_identity = _journal_identity_if_present(parent_descriptor, parent, name)
        if observed_identity != expected_identity:
            raise BootstrapJournalError("bootstrap transaction journal identity changed")
        if parent_descriptor is None:
            os.replace(parent / temporary_name, parent / name)
            _fsync_directory(parent)
        else:
            os.replace(
                temporary_name,
                name,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            os.fsync(parent_descriptor)
        published = _journal_identity_if_present(parent_descriptor, parent, name)
        if published is None:
            raise BootstrapJournalError("bootstrap transaction journal publication failed")
    finally:
        if descriptor is not None:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            if parent_descriptor is None:
                (parent / temporary_name).unlink()
            else:
                os.unlink(temporary_name, dir_fd=parent_descriptor)


def _entry_exists_without_following(path: Path) -> bool:
    normalized = _normalized_absolute(path, "bootstrap owned path")
    if os.name == "nt":
        try:
            normalized.lstat()
        except FileNotFoundError:
            return False
        return True
    try:
        with _open_absolute_directory(normalized.parent) as parent_descriptor:
            try:
                os.stat(normalized.name, dir_fd=parent_descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return False
            return True
    except FileNotFoundError:
        return False


def _owned_identity(path: Path, *, kind: str) -> dict[str, Any]:
    normalized = _normalized_absolute(path, "bootstrap owned path")
    if os.name == "nt":
        try:
            details = normalized.lstat()
        except OSError as exc:
            raise BootstrapJournalError(f"bootstrap owned path is unavailable: {path}") from exc
        if stat.S_ISREG(details.st_mode):
            digest = hashlib.sha256(normalized.read_bytes()).hexdigest()
            file_type = "file"
            link_target: str | None = None
        elif stat.S_ISLNK(details.st_mode):
            digest = None
            file_type = "symlink"
            link_target = os.readlink(normalized)
        elif stat.S_ISDIR(details.st_mode):
            digest = None
            file_type = "directory"
            link_target = None
        else:
            raise BootstrapJournalError(f"bootstrap owned path type is unsafe: {path}")
        identity = _identity_record(details, file_type, digest=digest, link_target=link_target)
    else:
        try:
            with _open_absolute_directory(normalized.parent) as parent_descriptor:
                details = os.stat(
                    normalized.name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
                identity = _owned_identity_at(
                    parent_descriptor,
                    normalized.name,
                    details,
                    display=normalized,
                )
        except (FileNotFoundError, OSError) as exc:
            raise BootstrapJournalError(f"bootstrap owned path is unavailable: {path}") from exc
    if not _kind_accepts_identity(kind, identity):
        raise BootstrapJournalError(f"bootstrap owned path kind changed: {path}")
    return identity


def _owned_identity_from_home(
    home_descriptor: int,
    home: Path,
    target: Path,
    *,
    kind: str,
) -> dict[str, Any]:
    normalized = _normalized_absolute(target, "bootstrap owned path")
    try:
        relative = normalized.relative_to(home)
    except ValueError as exc:
        raise BootstrapJournalError("bootstrap owned path escaped its home") from exc
    parent_descriptor = _open_target_parent_at(home_descriptor, relative.parent)
    if parent_descriptor is None:
        raise BootstrapJournalError(f"bootstrap owned path is unavailable: {target}")
    try:
        _verify_target_parent_identity(home_descriptor, relative.parent, parent_descriptor)
        details = os.stat(relative.name, dir_fd=parent_descriptor, follow_symlinks=False)
        identity = _owned_identity_at(
            parent_descriptor,
            relative.name,
            details,
            display=normalized,
        )
        _verify_target_parent_identity(home_descriptor, relative.parent, parent_descriptor)
    except (FileNotFoundError, OSError) as exc:
        raise BootstrapJournalError(f"bootstrap owned path is unavailable: {target}") from exc
    finally:
        os.close(parent_descriptor)
    if not _kind_accepts_identity(kind, identity):
        raise BootstrapJournalError(f"bootstrap owned path kind changed: {target}")
    return identity


def _home_for_journal_parent(parent: Path) -> Path:
    try:
        home = parent.parents[2]
    except IndexError as exc:
        raise BootstrapJournalError("bootstrap journal parent is outside the user home") from exc
    if parent != home / ".local/share/clio-relay":
        raise BootstrapJournalError("bootstrap journal parent is outside the relay namespace")
    return home


def _owned_identity_at(
    parent_descriptor: int,
    name: str,
    details: os.stat_result,
    *,
    display: Path,
) -> dict[str, Any]:
    if stat.S_ISREG(details.st_mode):
        descriptor = os.open(
            name,
            os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC,
            dir_fd=parent_descriptor,
        )
        try:
            opened = os.fstat(descriptor)
            if _stat_identity(opened) != _stat_identity(details):
                raise BootstrapJournalError(f"bootstrap owned file changed: {display}")
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            observed = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
            if _stat_identity(opened) != _stat_identity(observed) or (
                opened.st_size,
                opened.st_ctime_ns,
            ) != (observed.st_size, observed.st_ctime_ns):
                raise BootstrapJournalError(f"bootstrap owned file changed: {display}")
            return _identity_record(
                observed,
                "file",
                digest=digest.hexdigest(),
                link_target=None,
            )
        finally:
            os.close(descriptor)
    if stat.S_ISLNK(details.st_mode):
        target = os.readlink(name, dir_fd=parent_descriptor)
        observed = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if _stat_identity(observed) != _stat_identity(details):
            raise BootstrapJournalError(f"bootstrap owned symlink changed: {display}")
        return _identity_record(
            observed,
            "symlink",
            digest=None,
            link_target=target,
        )
    if stat.S_ISDIR(details.st_mode):
        descriptor = os.open(
            name,
            os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC,
            dir_fd=parent_descriptor,
        )
        try:
            opened = os.fstat(descriptor)
            if _stat_identity(opened) != _stat_identity(details):
                raise BootstrapJournalError(f"bootstrap owned directory changed: {display}")
            return _identity_record(
                opened,
                "directory",
                digest=None,
                link_target=None,
            )
        finally:
            os.close(descriptor)
    raise BootstrapJournalError(f"bootstrap owned path type is unsafe: {display}")


def _identity_record(
    details: os.stat_result,
    file_type: str,
    *,
    digest: str | None,
    link_target: str | None,
) -> dict[str, Any]:
    return {
        "device": details.st_dev,
        "inode": details.st_ino,
        "file_type": file_type,
        "changed_ns": details.st_ctime_ns,
        "size": details.st_size,
        "sha256": digest,
        "symlink_target": link_target,
    }


def _owned_identity_matches(expected: dict[str, Any], observed: dict[str, Any]) -> bool:
    stable_fields = {"device", "inode", "file_type"}
    if any(expected[field] != observed[field] for field in stable_fields):
        return False
    if expected["file_type"] == "directory":
        return True
    return expected == observed


def _require_current_owner(descriptor: int, description: str) -> None:
    details = os.fstat(descriptor)
    if not stat.S_ISDIR(details.st_mode) or (_GETUID is not None and details.st_uid != _GETUID()):
        raise BootstrapJournalError(f"{description} is not owned by the current user")


def _open_target_parent_at(home_descriptor: int, relative_parent: Path) -> int | None:
    descriptor = os.dup(home_descriptor)
    flags = os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC
    try:
        for component in relative_parent.parts:
            if component in {"", "."}:
                continue
            try:
                next_descriptor = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                os.close(descriptor)
                return None
            except OSError as exc:
                raise BootstrapJournalError("bootstrap owned path parent topology changed") from exc
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        with suppress(OSError):
            os.close(descriptor)
        raise


def _discard_owned_posix(
    home_descriptor: int,
    home: Path,
    target: Path,
    *,
    kind: str,
    expected_identity: dict[str, Any] | None,
) -> None:
    try:
        relative = target.relative_to(home)
    except ValueError as exc:
        raise BootstrapJournalError("bootstrap owned path escaped its home") from exc
    parent_descriptor = _open_target_parent_at(home_descriptor, relative.parent)
    if parent_descriptor is None:
        return
    try:
        _verify_target_parent_identity(
            home_descriptor,
            relative.parent,
            parent_descriptor,
        )
        _discard_owned_entry_at(
            parent_descriptor,
            relative.name,
            kind=kind,
            expected_identity=expected_identity,
            display=target,
        )
        _verify_target_parent_identity(
            home_descriptor,
            relative.parent,
            parent_descriptor,
        )
    finally:
        os.close(parent_descriptor)


def _verify_target_parent_identity(
    home_descriptor: int,
    relative_parent: Path,
    expected_descriptor: int,
) -> None:
    observed_descriptor = _open_target_parent_at(home_descriptor, relative_parent)
    if observed_descriptor is None:
        raise BootstrapJournalError("bootstrap owned path parent identity changed")
    try:
        expected = os.fstat(expected_descriptor)
        observed = os.fstat(observed_descriptor)
        if _stat_identity(expected) != _stat_identity(observed):
            raise BootstrapJournalError("bootstrap owned path parent identity changed")
    finally:
        os.close(observed_descriptor)


def _verify_journal_parent_from_home(
    home_descriptor: int,
    home: Path,
    journal_descriptor: int | None,
    journal_parent: Path,
) -> None:
    if journal_descriptor is None:
        raise BootstrapJournalError("descriptor-pinned recovery requires POSIX")
    try:
        relative_parent = journal_parent.relative_to(home)
    except ValueError as exc:
        raise BootstrapJournalError("bootstrap journal is outside the recovery home") from exc
    observed_descriptor = _open_target_parent_at(home_descriptor, relative_parent)
    if observed_descriptor is None:
        raise BootstrapJournalError("bootstrap journal parent is no longer reachable")
    try:
        if _stat_identity(os.fstat(observed_descriptor)) != _stat_identity(
            os.fstat(journal_descriptor)
        ):
            raise BootstrapJournalError("bootstrap journal parent identity changed")
    finally:
        os.close(observed_descriptor)


def _discard_owned_entry_at(
    parent_descriptor: int,
    name: str,
    *,
    kind: str,
    expected_identity: dict[str, Any] | None,
    display: Path,
) -> None:
    try:
        details = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return
    if expected_identity is None:
        raise BootstrapJournalError(
            f"bootstrap owned path identity was not durably recorded: {display}"
        )
    observed_identity = _owned_identity_at(
        parent_descriptor,
        name,
        details,
        display=display,
    )
    if not _owned_identity_matches(expected_identity, observed_identity):
        raise BootstrapJournalError(f"bootstrap owned path identity changed: {display}")
    if kind == "directory":
        if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
            raise BootstrapJournalError(f"owned directory identity changed: {display}")
        _remove_directory_at(parent_descriptor, name, details, display=display)
        return
    if kind == "file":
        accepted = stat.S_ISREG(details.st_mode)
        error = "owned file identity changed"
    elif kind == "file_or_symlink":
        accepted = stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode)
        error = "owned path identity changed"
    else:
        accepted = stat.S_ISLNK(details.st_mode)
        error = "owned symlink identity changed"
    if not accepted:
        raise BootstrapJournalError(f"{error}: {display}")
    _unlink_entry_at(parent_descriptor, name, details, display=display)


def _unlink_entry_at(
    parent_descriptor: int,
    name: str,
    expected: os.stat_result,
    *,
    display: Path,
) -> None:
    try:
        observed = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return
    if _stat_identity(observed) != _stat_identity(expected):
        raise BootstrapJournalError(f"owned path changed during recovery: {display}")
    os.unlink(name, dir_fd=parent_descriptor)
    os.fsync(parent_descriptor)


def _remove_directory_at(
    parent_descriptor: int,
    name: str,
    expected: os.stat_result,
    *,
    display: Path,
) -> None:
    flags = os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW | _O_CLOEXEC
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as exc:
        raise BootstrapJournalError(f"owned directory changed during recovery: {display}") from exc
    try:
        if _stat_identity(os.fstat(descriptor)) != _stat_identity(expected):
            raise BootstrapJournalError(f"owned directory changed during recovery: {display}")
        _remove_directory_contents(descriptor, display=display)
        observed = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if _stat_identity(observed) != _stat_identity(expected):
            raise BootstrapJournalError(f"owned directory changed during recovery: {display}")
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=parent_descriptor)
    os.fsync(parent_descriptor)


def _remove_directory_contents(descriptor: int, *, display: Path) -> None:
    with os.scandir(descriptor) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        child_display = display / name
        try:
            details = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if stat.S_ISDIR(details.st_mode) and not stat.S_ISLNK(details.st_mode):
            _remove_directory_at(descriptor, name, details, display=child_display)
        elif stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode):
            _unlink_entry_at(descriptor, name, details, display=child_display)
        else:
            raise BootstrapJournalError(
                f"owned directory contains an unsupported entry: {child_display}"
            )


def _stat_identity(details: os.stat_result) -> tuple[int, int, int]:
    return details.st_dev, details.st_ino, stat.S_IFMT(details.st_mode)


def _discard_owned_windows(target: Path, *, kind: str) -> None:
    try:
        details = target.lstat()
    except FileNotFoundError:
        return
    if kind == "directory":
        if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
            raise BootstrapJournalError(f"owned directory identity changed: {target}")
        for child in target.iterdir():
            child_details = child.lstat()
            if stat.S_ISDIR(child_details.st_mode) and not stat.S_ISLNK(child_details.st_mode):
                _discard_owned_windows(child, kind="directory")
            elif stat.S_ISREG(child_details.st_mode) or stat.S_ISLNK(child_details.st_mode):
                child.unlink()
            else:
                raise BootstrapJournalError(
                    f"owned directory contains an unsupported entry: {child}"
                )
        target.rmdir()
    elif (
        (kind == "file" and stat.S_ISREG(details.st_mode))
        or (
            kind == "file_or_symlink"
            and (stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode))
        )
        or (kind == "symlink" and stat.S_ISLNK(details.st_mode))
    ):
        target.unlink()
    else:
        raise BootstrapJournalError(f"owned path identity changed: {target}")
    _fsync_directory(target.parent)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _string(value: dict[str, Any], field: str) -> str:
    observed = value.get(field)
    if not isinstance(observed, str):
        raise BootstrapJournalError(f"bootstrap {field} is invalid")
    return observed


def _parse_optional_bool(value: str) -> bool | None:
    if value == "true":
        return True
    if value == "false":
        return False
    if value == "unknown":
        return None
    raise BootstrapJournalError("bootstrap optional boolean is invalid")


def _fail(message: str) -> NoReturn:
    raise SystemExit(message)


def main(arguments: list[str] | None = None) -> int:
    """Run the small command surface embedded into the bootstrap shell."""
    argv = list(sys.argv[1:] if arguments is None else arguments)
    try:
        action = argv.pop(0)
        if action == "create":
            if len(argv) != 8:
                _fail("bootstrap journal create arguments are invalid")
            path, invocation, fingerprint, mode, service, active, enabled, owned_json = argv
            owned = _coerce_owned_paths(cast(object, json.loads(owned_json)))
            create_journal(
                Path(path),
                invocation_id=invocation,
                desired_fingerprint=fingerprint,
                mode=mode,
                owned_paths=owned,
                service_name=service or None,
                service_was_active=_parse_optional_bool(active),
                service_was_enabled=_parse_optional_bool(enabled),
            )
        elif action == "advance":
            if len(argv) not in {2, 3}:
                _fail("bootstrap journal advance arguments are invalid")
            advance_journal(
                Path(argv[0]),
                argv[1],
                prepared_generation=argv[2] if len(argv) == 3 else None,
            )
        elif action == "phase":
            if len(argv) != 3:
                _fail("bootstrap journal phase arguments are invalid")
            record_phase(Path(argv[0]), argv[1], argv[2])
        elif action == "own":
            if len(argv) != 2:
                _fail("bootstrap journal ownership arguments are invalid")
            record_owned_path(Path(argv[0]), argv[1])
        elif action == "mkdir-owned":
            if len(argv) != 2:
                _fail("bootstrap owned directory arguments are invalid")
            create_owned_directory(Path(argv[0]), argv[1])
        elif action == "symlink-owned":
            if len(argv) != 3:
                _fail("bootstrap owned symlink arguments are invalid")
            create_owned_symlink(Path(argv[0]), argv[1], argv[2])
        elif action == "copy-owned":
            if len(argv) != 4 or not re.fullmatch(r"0?[0-7]{3}", argv[3]):
                _fail("bootstrap owned file arguments are invalid")
            copy_owned_file(Path(argv[0]), argv[1], Path(argv[2]), mode=int(argv[3], 8))
        elif action == "recovery-plan":
            if len(argv) != 1:
                _fail("bootstrap journal recovery arguments are invalid")
            print(json.dumps(recovery_plan(Path(argv[0])), sort_keys=True, separators=(",", ":")))
        elif action == "discard-full":
            if len(argv) != 2:
                _fail("bootstrap journal discard arguments are invalid")
            discard_full_transaction(Path(argv[0]), home=Path(argv[1]))
        else:
            _fail("unknown bootstrap journal action")
    except (BootstrapJournalError, json.JSONDecodeError, OSError) as exc:
        _fail(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
