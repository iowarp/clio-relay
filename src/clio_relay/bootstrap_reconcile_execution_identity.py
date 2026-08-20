"""Execution-environment and JARVIS-state identity for bootstrap reconciliation.

Read-only identity capture: a venv's pinned python/jarvis executables
(``execution_environment_identity``), the deterministic relay-owned JARVIS
launcher payload (``jarvis_wrapper_payload``/``write_jarvis_wrapper``), and
operator-owned JARVIS config/repos/resource-graph state
(``inspect_jarvis_state``) (iowarp/clio-relay#255).
"""

from __future__ import annotations

import hashlib
import os
import shlex
import stat
from contextlib import suppress
from pathlib import Path
from typing import cast

from clio_relay.bootstrap_reconcile_constants import (
    _FCHMOD,
    _O_NOFOLLOW,
    MAX_JARVIS_CONFIG_BYTES,
    MAX_JARVIS_GRAPH_BYTES,
    MAX_JARVIS_REPOS_BYTES,
)
from clio_relay.bootstrap_reconcile_models import BootstrapDesiredState, JarvisStateEvidence
from clio_relay.bootstrap_reconcile_primitives import (
    _canonical_path_preserving_final,
    _expand_home,
    _fsync_directory,
    _read_regular_bounded,
    _stat_identity,
    _yaml_mapping,
)
from clio_relay.errors import ConfigurationError
from clio_relay.validation_report import sha256_file


def execution_environment_identity(
    root: Path,
    *,
    executables: dict[str, Path],
) -> dict[str, object]:
    """Identify a reused execution boundary without scanning or copying its tree."""
    lexical_root = Path(os.path.abspath(root.expanduser()))
    try:
        root_details = lexical_root.lstat()
    except OSError as exc:
        raise ConfigurationError("execution environment is unavailable") from exc
    if lexical_root.is_symlink() or not lexical_root.is_dir():
        raise ConfigurationError("execution environment is not one owned directory")
    identities: dict[str, object] = {}
    resolved_root = lexical_root.resolve(strict=True)
    for name, executable in sorted(executables.items()):
        try:
            lexical = Path(os.path.abspath(executable.expanduser()))
            before = lexical.lstat()
            located = lexical.parent.resolve(strict=True) / lexical.name
            resolved = lexical.resolve(strict=True)
            if (
                located == resolved_root
                or not located.is_relative_to(resolved_root)
                or not resolved.is_file()
                or not os.access(resolved, os.X_OK)
            ):
                raise ConfigurationError(f"execution boundary executable is invalid: {name}")
            digest = sha256_file(resolved)
            if _stat_identity(lexical.lstat()) != _stat_identity(before):
                raise ConfigurationError(f"execution boundary executable changed: {name}")
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(f"execution boundary executable is invalid: {name}") from exc
        identities[name] = {
            "lexical_path": str(lexical),
            "resolved_path": str(resolved),
            "sha256": digest,
            "size_bytes": resolved.stat().st_size,
        }
    config_path = lexical_root / "pyvenv.cfg"
    config_sha256 = sha256_file(config_path) if config_path.is_file() else None
    if _stat_identity(lexical_root.lstat()) != _stat_identity(root_details):
        raise ConfigurationError("execution environment changed during inspection")
    return {
        "schema_version": "clio-relay.execution-boundary.v1",
        "root": str(lexical_root),
        "root_identity": {
            "device": root_details.st_dev,
            "inode": root_details.st_ino,
            "mode": root_details.st_mode,
            "modified_ns": root_details.st_mtime_ns,
            "changed_ns": root_details.st_ctime_ns,
        },
        "pyvenv_cfg_sha256": config_sha256,
        "executables": identities,
        "tree_scanned": False,
        "tree_copied": False,
    }


def jarvis_wrapper_payload(execution_python: Path) -> bytes:
    """Return the deterministic relay-owned JARVIS launcher payload."""
    lexical_python = Path(os.path.abspath(execution_python.expanduser()))
    try:
        before = lexical_python.lstat()
        resolved_python = lexical_python.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("JARVIS execution interpreter is unavailable") from exc
    if (
        any(character in str(lexical_python) for character in "\x00\r\n")
        or not resolved_python.is_file()
        or not os.access(lexical_python, os.X_OK)
        or _stat_identity(lexical_python.lstat()) != _stat_identity(before)
    ):
        raise ConfigurationError("JARVIS execution interpreter is not executable")
    invocation = "from jarvis_cd.core.cli import main; raise SystemExit(main())"
    return (
        f'#!/bin/sh\nexec {shlex.quote(str(lexical_python))} -c {shlex.quote(invocation)} "$@"\n'
    ).encode()


def write_jarvis_wrapper(path: Path, execution_python: Path) -> dict[str, object]:
    """Create and fsync one exclusive relay-owned JARVIS launcher."""
    payload = jarvis_wrapper_payload(execution_python)
    try:
        parent_details = path.parent.lstat()
        parent_identity = (
            parent_details.st_dev,
            parent_details.st_ino,
            parent_details.st_mode,
        )
        if path.parent.is_symlink() or not path.parent.is_dir():
            raise ConfigurationError("JARVIS wrapper parent is not an owned directory")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW
        descriptor = os.open(path, flags, 0o755)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if _FCHMOD is not None:
                _FCHMOD(descriptor, 0o755)
            else:
                os.chmod(path, 0o755)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        parent_after = path.parent.lstat()
        if (parent_after.st_dev, parent_after.st_ino, parent_after.st_mode) != parent_identity:
            raise ConfigurationError("JARVIS wrapper parent changed during creation")
        _fsync_directory(path.parent)
    except BaseException:
        with suppress(OSError):
            path.unlink(missing_ok=True)
        raise
    return {
        "path": str(path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "execution_python": str(execution_python.resolve(strict=True)),
    }


def inspect_jarvis_state(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
) -> JarvisStateEvidence:
    """Validate initialized JARVIS roots and hash operator-owned state read-only."""
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    jarvis_root = _expand_home(desired.jarvis_root, lexical_home)
    config_file = jarvis_root / "jarvis_config.yaml"
    repos_file = jarvis_root / "repos.yaml"
    resource_graph_file = jarvis_root / "resource_graph.yaml"
    state_files = (config_file, repos_file, resource_graph_file)
    metadata: list[os.stat_result | None] = []
    for path in state_files:
        try:
            details = path.lstat()
        except FileNotFoundError:
            details = None
        except OSError as exc:
            raise ConfigurationError(f"could not classify JARVIS state: {path}") from exc
        metadata.append(details)
    existing = [details is not None for details in metadata]
    if not any(existing):
        return JarvisStateEvidence(initialized=False, root=str(jarvis_root))
    if not all(existing):
        raise ConfigurationError(
            "JARVIS state is partially initialized; refusing bootstrap mutation"
        )
    typed_metadata = [cast(os.stat_result, details) for details in metadata]
    if any(
        not stat.S_ISREG(details.st_mode) or details.st_size < 1 or details.st_size > maximum
        for details, maximum in zip(
            typed_metadata,
            (MAX_JARVIS_CONFIG_BYTES, MAX_JARVIS_REPOS_BYTES, MAX_JARVIS_GRAPH_BYTES),
            strict=True,
        )
    ):
        raise ConfigurationError("JARVIS state must contain three bounded regular files")
    file_ids = [(details.st_dev, details.st_ino) for details in typed_metadata]
    if len(set(file_ids)) != len(file_ids):
        raise ConfigurationError("JARVIS state files must not share one file identity")

    raw_config = _read_regular_bounded(config_file, maximum=MAX_JARVIS_CONFIG_BYTES)
    raw_repos = _read_regular_bounded(repos_file, maximum=MAX_JARVIS_REPOS_BYTES)
    raw_graph = _read_regular_bounded(resource_graph_file, maximum=MAX_JARVIS_GRAPH_BYTES)
    config = _yaml_mapping(raw_config, label="JARVIS configuration")
    repos = _yaml_mapping(raw_repos, label="JARVIS repositories")
    observed_roots: dict[str, str] = {}
    for field in ("config_dir", "private_dir", "shared_dir"):
        observed = config.get(field)
        if not isinstance(observed, str) or not observed:
            raise ConfigurationError(f"JARVIS configuration omitted {field}")
        try:
            observed_path = Path(observed).expanduser()
            if not observed_path.is_absolute():
                raise ConfigurationError(f"JARVIS {field} is not absolute")
            normalized_path = observed_path.resolve(strict=True)
            if not normalized_path.is_dir():
                raise ConfigurationError(f"JARVIS {field} is not a directory")
            normalized = str(normalized_path)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(f"JARVIS {field} is invalid") from exc
        observed_roots[field] = normalized
    raw_repo_values = repos.get("repos")
    typed_repo_values = (
        cast(list[object], raw_repo_values) if isinstance(raw_repo_values, list) else []
    )
    if not isinstance(raw_repo_values, list) or any(
        not isinstance(value, str) or not value for value in typed_repo_values
    ):
        raise ConfigurationError("JARVIS repositories must contain a string list")
    managed_repo_path = _expand_home(desired.managed_jarvis_repo, lexical_home)
    lexical_managed_repo = str(Path(os.path.abspath(managed_repo_path.expanduser())))
    canonical_managed_repo = str(_canonical_path_preserving_final(managed_repo_path))
    managed_aliases = {lexical_managed_repo, canonical_managed_repo}
    managed_builtin_path = jarvis_root / "builtin"
    lexical_managed_builtin = str(Path(os.path.abspath(managed_builtin_path.expanduser())))
    canonical_managed_builtin = str(_canonical_path_preserving_final(managed_builtin_path))
    managed_builtin_aliases = {
        lexical_managed_builtin,
        canonical_managed_builtin,
    }
    repo_values = cast(list[str], raw_repo_values)
    managed_matches = [value for value in repo_values if value in managed_aliases]
    if len(managed_matches) > 1:
        raise ConfigurationError(
            "relay-managed JARVIS repository is registered through multiple path aliases"
        )
    managed_builtin_matches = [value for value in repo_values if value in managed_builtin_aliases]
    if len(managed_builtin_matches) > 1:
        raise ConfigurationError(
            "JARVIS-managed builtin repository is registered through multiple path aliases"
        )
    return JarvisStateEvidence(
        initialized=True,
        root=str(jarvis_root),
        roots=observed_roots,
        config_sha256=hashlib.sha256(raw_config).hexdigest(),
        repos_sha256=hashlib.sha256(raw_repos).hexdigest(),
        resource_graph_sha256=hashlib.sha256(raw_graph).hexdigest(),
        managed_repo_registered=managed_matches == [lexical_managed_repo],
        managed_builtin_repo_registered=managed_builtin_matches == [lexical_managed_builtin],
    )
