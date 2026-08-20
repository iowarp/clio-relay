"""Proof that a JARVIS ``builtin`` repository slot came from a relay-owned venv.

Old JARVIS releases registered their wheel-installed ``builtin`` package
directly in ``repos.yaml``. This module constrains migration to the fixed
legacy venv and content-addressed generation venvs, then requires wheel
``METADATA``/``RECORD`` evidence proving ``jarvis-cd`` installed the
repository (iowarp/clio-relay#255).
"""

from __future__ import annotations

import csv
import io
import os
import stat
from pathlib import Path

from clio_relay.bootstrap_reconcile_constants import (
    MAX_JARVIS_DISTRIBUTION_METADATA_BYTES,
    MAX_JARVIS_DISTRIBUTION_RECORD_BYTES,
)
from clio_relay.bootstrap_reconcile_primitives import (
    _is_sha256,
    _path_is_directory_alias,
    _read_regular_bounded,
    _stat_identity,
)
from clio_relay.errors import ConfigurationError


def _relay_owned_jarvis_builtin_repositories(
    *,
    home: Path,
    execution_environments: tuple[Path, ...] = (),
) -> tuple[Path, ...]:
    """Return builtin repositories proven to belong to relay-managed JARVIS venvs.

    Old JARVIS releases registered their wheel-installed ``builtin`` package
    directly in ``repos.yaml``.  JARVIS now owns a stable repository slot, but
    that cannot identify the historical path when it lives in relay's legacy
    virtual environment.  Constrain migration to the fixed legacy venv and to
    content-addressed generation venvs, then require wheel ``METADATA`` and
    ``RECORD`` evidence proving that ``jarvis-cd`` installed the repository.
    """
    lexical_home = Path(os.path.abspath(home.expanduser()))
    resolved_home = lexical_home.resolve(strict=True)
    lexical_relay_root = lexical_home / ".local/share/clio-relay"
    resolved_relay_root = resolved_home / ".local/share/clio-relay"
    fixed_legacy = lexical_relay_root / "jarvis-venv"
    candidates = (fixed_legacy, *execution_environments)
    repositories: dict[str, Path] = {}
    seen_environments: set[str] = set()
    for candidate in candidates:
        lexical_environment = Path(os.path.abspath(candidate.expanduser()))
        lexical_identity: tuple[int, int, int, int, int, int]
        try:
            before = lexical_environment.lstat()
            if (
                not stat.S_ISDIR(before.st_mode)
                or _path_is_directory_alias(lexical_environment)
                or not lexical_environment.is_absolute()
                or ".." in lexical_environment.parts
            ):
                continue
            resolved_environment = lexical_environment.resolve(strict=True)
            fixed_environment = resolved_relay_root / "jarvis-venv"
            owned_layout = resolved_environment == fixed_environment
            if not owned_layout:
                relative = resolved_environment.relative_to(
                    (resolved_relay_root / "generations").resolve(strict=True)
                )
                owned_layout = bool(
                    len(relative.parts) == 2
                    and _is_sha256(relative.parts[0])
                    and relative.parts[1] == "jarvis-venv"
                )
            if not owned_layout:
                continue
            lexical_identity = _stat_identity(before)
            if _stat_identity(lexical_environment.lstat()) != lexical_identity:
                continue
            getuid = getattr(os, "getuid", None)
            if callable(getuid) and before.st_uid != getuid():
                continue
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            continue
        environment_key = str(resolved_environment)
        if environment_key in seen_environments:
            continue
        seen_environments.add(environment_key)
        for site_packages in _jarvis_site_package_directories(lexical_environment):
            repository = _jarvis_cd_builtin_repository(site_packages)
            if repository is not None:
                repositories[str(repository)] = repository
        if _stat_identity(lexical_environment.lstat()) != lexical_identity:
            raise ConfigurationError(
                "relay-owned JARVIS environment changed during repository reconciliation"
            )
    return tuple(repositories[key] for key in sorted(repositories))


def _jarvis_site_package_directories(environment: Path) -> tuple[Path, ...]:
    """Enumerate bounded, real site-package directories inside one proven venv."""
    candidates = [environment / "Lib/site-packages"]
    for library_name in ("lib", "lib64"):
        library = environment / library_name
        try:
            python_directories = sorted(library.glob("python*"), key=lambda path: path.name)
        except OSError:
            continue
        if len(python_directories) > 16:
            raise ConfigurationError("relay-owned JARVIS environment has too many Python roots")
        candidates.extend(path / "site-packages" for path in python_directories)
    directories: dict[str, Path] = {}
    for candidate in candidates:
        try:
            before = candidate.lstat()
            if not stat.S_ISDIR(before.st_mode) or _path_is_directory_alias(candidate):
                continue
            resolved = candidate.resolve(strict=True)
            relative = resolved.relative_to(environment.resolve(strict=True))
            parts = relative.parts
            posix_shape = bool(
                len(parts) == 3
                and parts[0] in {"lib", "lib64"}
                and _is_python_library_directory(parts[1])
                and parts[2] == "site-packages"
            )
            windows_shape = parts == ("Lib", "site-packages")
            if not posix_shape and not windows_shape:
                continue
            if _stat_identity(candidate.lstat()) != _stat_identity(before):
                continue
            directories[str(candidate)] = candidate
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            continue
    return tuple(directories[key] for key in sorted(directories))


def _is_python_library_directory(value: str) -> bool:
    """Return whether a venv library name is exactly ``python<major>.<minor>``."""
    if not value.startswith("python"):
        return False
    version = value.removeprefix("python")
    major, separator, minor = version.partition(".")
    return bool(separator and major.isdigit() and minor.isdigit())


def _jarvis_cd_builtin_repository(site_packages: Path) -> Path | None:
    """Prove that one site-packages ``builtin`` directory came from jarvis-cd."""
    builtin = site_packages / "builtin"
    try:
        builtin_before = builtin.lstat()
        if not stat.S_ISDIR(builtin_before.st_mode) or _path_is_directory_alias(builtin):
            return None
        distributions = sorted(
            site_packages.glob("jarvis_cd-*.dist-info"),
            key=lambda path: path.name,
        )
        if len(distributions) > 8:
            raise ConfigurationError("relay-owned JARVIS environment has too many distributions")
        for distribution in distributions:
            distribution_before = distribution.lstat()
            if not stat.S_ISDIR(distribution_before.st_mode) or _path_is_directory_alias(
                distribution
            ):
                continue
            metadata_payload = _read_regular_bounded(
                distribution / "METADATA",
                maximum=MAX_JARVIS_DISTRIBUTION_METADATA_BYTES,
            )
            record_payload = _read_regular_bounded(
                distribution / "RECORD",
                maximum=MAX_JARVIS_DISTRIBUTION_RECORD_BYTES,
            )
            if not _jarvis_cd_metadata(metadata_payload) or not _record_installs_jarvis_builtin(
                record_payload
            ):
                continue
            if _stat_identity(distribution.lstat()) != _stat_identity(
                distribution_before
            ) or _stat_identity(builtin.lstat()) != _stat_identity(builtin_before):
                raise ConfigurationError(
                    "relay-owned JARVIS distribution changed during repository reconciliation"
                )
            getuid = getattr(os, "getuid", None)
            if callable(getuid) and (
                distribution_before.st_uid != getuid() or builtin_before.st_uid != getuid()
            ):
                continue
            return builtin
    except (FileNotFoundError, OSError, RuntimeError, UnicodeError, ValueError):
        return None
    return None


def _jarvis_cd_metadata(payload: bytes) -> bool:
    """Return whether wheel metadata names the jarvis-cd distribution exactly."""
    for line in payload.decode("utf-8").splitlines():
        field, separator, value = line.partition(":")
        if separator and field.casefold() == "name":
            return value.strip().casefold().replace("_", "-") == "jarvis-cd"
    return False


def _record_installs_jarvis_builtin(payload: bytes) -> bool:
    """Require both repository package markers in a jarvis-cd wheel RECORD."""
    paths: set[str] = set()
    reader = csv.reader(io.StringIO(payload.decode("utf-8"), newline=""))
    for row in reader:
        if row:
            paths.add(row[0].replace("\\", "/"))
    return {"builtin/__init__.py", "builtin/builtin/__init__.py"} <= paths
