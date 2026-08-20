"""Exact-path JARVIS repository registration and stable-binding repair.

``reconcile_managed_jarvis_repository`` performs a compare-before-replace
update of relay's exact repository paths in ``repos.yaml`` without JARVIS's
basename-matching ``repo add --force``; ``repair_managed_jarvis_binding``
repairs relay's stable package link plus that exact registration
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import hashlib
import os
from contextlib import suppress
from pathlib import Path
from typing import cast

import yaml

from clio_relay.bootstrap_reconcile_activation_paths import (
    _activation_symlink_lexical_target,
    _capture_activation_path,
    _is_generation_repository_target,
    _reconcile_activation_symlink,
    _verify_stable_symlink,
)
from clio_relay.bootstrap_reconcile_builtin_repos import _relay_owned_jarvis_builtin_repositories
from clio_relay.bootstrap_reconcile_constants import (
    LEGACY_MANAGED_JARVIS_REPO_PATH,
    MAX_JARVIS_REPOS_BYTES,
)
from clio_relay.bootstrap_reconcile_models import BootstrapDesiredState
from clio_relay.bootstrap_reconcile_primitives import (
    _atomic_exchange_paths,
    _canonical_path_preserving_final,
    _expand_home,
    _fsync_directory,
    _identity_matches_after_rename,
    _read_regular_bounded_with_identity,
    _require_sha256,
    _stat_identity,
    _yaml_mapping,
)
from clio_relay.errors import ConfigurationError


def _managed_repository_payload(
    raw: bytes,
    *,
    managed: str,
    managed_aliases: set[str],
    managed_builtin: str | None,
    managed_builtin_aliases: set[str],
    previous_aliases: dict[str, str],
    previous_builtin_aliases: set[str],
) -> tuple[bytes, list[str], list[str]]:
    """Return the exact converged repository bytes and mutation evidence."""
    document = _yaml_mapping(raw, label="JARVIS repositories")
    raw_repos = document.get("repos")
    typed_repos = cast(list[object], raw_repos) if isinstance(raw_repos, list) else []
    if not isinstance(raw_repos, list) or any(
        not isinstance(value, str) or not value for value in typed_repos
    ):
        raise ConfigurationError("JARVIS repositories must contain a string list")
    repos = list(cast(list[str], raw_repos))
    managed_matches = [value for value in repos if value in managed_aliases]
    if len(managed_matches) > 1:
        raise ConfigurationError(
            "relay-managed JARVIS repository is registered through multiple path aliases"
        )
    managed_builtin_matches = [value for value in repos if value in managed_builtin_aliases]
    if len(managed_builtin_matches) > 1:
        raise ConfigurationError(
            "JARVIS-managed builtin repository is registered through multiple path aliases"
        )
    previous_matches: dict[str, list[str]] = {}
    for value in repos:
        normalized = previous_aliases.get(value)
        if normalized is not None:
            previous_matches.setdefault(normalized, []).append(value)
    if any(len(values) > 1 for values in previous_matches.values()):
        raise ConfigurationError(
            "a proven previous relay-managed JARVIS repository is registered through "
            "multiple path aliases"
        )
    removed_previous = sorted(previous_matches)
    if managed_builtin is None:
        if managed_matches == [managed] and not removed_previous:
            return raw, [], []
        updated = [
            value
            for value in repos
            if value not in managed_aliases and value not in previous_aliases
        ]
        updated.insert(0, managed)
        added_managed = [managed] if managed_matches != [managed] else []
        document["repos"] = updated
        return (
            yaml.safe_dump(document, sort_keys=False).encode("utf-8"),
            added_managed,
            removed_previous,
        )
    if (
        managed_matches == [managed]
        and managed_builtin_matches == [managed_builtin]
        and not removed_previous
    ):
        return raw, [], []
    builtin_anchor: int | None = None
    if managed_builtin_matches:
        builtin_anchor = repos.index(managed_builtin_matches[0])
    else:
        builtin_anchor = next(
            (index for index, value in enumerate(repos) if value in previous_builtin_aliases),
            None,
        )
    operator_repositories: list[str] = []
    builtin_position: int | None = None
    for index, value in enumerate(repos):
        if index == builtin_anchor:
            builtin_position = len(operator_repositories)
        if (
            value in managed_aliases
            or value in managed_builtin_aliases
            or value in previous_aliases
        ):
            continue
        operator_repositories.append(value)
    if builtin_position is None:
        builtin_position = len(operator_repositories)
    operator_repositories.insert(builtin_position, managed_builtin)
    updated = [managed, *operator_repositories]
    if repos == updated and not removed_previous:
        return raw, [], []
    added_managed: list[str] = []
    if not repos or repos[0] != managed:
        added_managed.append(managed)
    if managed_builtin_matches != [managed_builtin]:
        added_managed.append(managed_builtin)
    document["repos"] = updated
    return (
        yaml.safe_dump(document, sort_keys=False).encode("utf-8"),
        added_managed,
        removed_previous,
    )


def reconcile_managed_jarvis_repository(
    repos_file: Path,
    managed_repo: Path,
    *,
    managed_builtin_repo: Path | None = None,
    previous_managed_repos: tuple[Path, ...] = (),
    exchange_identity: str | None = None,
) -> dict[str, object]:
    """Register only the exact relay-owned repository without basename matching.

    JARVIS's public ``repo add --force`` replaces every repository with the
    same basename. Relay instead performs a compare-before-replace update of
    its exact paths and leaves operator repositories, including same-name
    repositories, untouched. ``managed_builtin_repo`` may name only JARVIS's
    exact ``<JARVIS_ROOT>/builtin`` slot. JARVIS 1.6+ rebinds that stable slot
    in memory to the builtin repository shipped by the running distribution,
    so the independently installed execution and MCP runtimes each see their
    release-pinned package contract. ``previous_managed_repos`` is a caller-supplied
    provenance boundary: each path must come from an earlier relay receipt or
    an exact relay-owned generation path. This operation is serialized by the
    bootstrap lock; the final byte-and-file-identity comparison also detects
    non-cooperating writers before the atomic replacement.
    """
    if managed_repo.name != "clio_relay":
        raise ConfigurationError(
            "relay-managed JARVIS repository basename must match its clio_relay namespace"
        )
    managed = str(Path(os.path.abspath(managed_repo.expanduser())))
    canonical_managed = str(_canonical_path_preserving_final(managed_repo))
    managed_aliases = {managed, canonical_managed}
    managed_builtin: str | None = None
    managed_builtin_aliases: set[str] = set()
    if managed_builtin_repo is not None:
        lexical_builtin_path = Path(os.path.abspath(managed_builtin_repo.expanduser()))
        expected_builtin_path = Path(os.path.abspath(repos_file.parent.expanduser())) / "builtin"
        canonical_builtin_path = _canonical_path_preserving_final(managed_builtin_repo)
        canonical_expected_builtin = _canonical_path_preserving_final(expected_builtin_path)
        if (
            lexical_builtin_path != expected_builtin_path
            or canonical_builtin_path != canonical_expected_builtin
        ):
            raise ConfigurationError(
                "JARVIS-managed builtin repository must be the exact active root slot"
            )
        managed_builtin = str(lexical_builtin_path)
        managed_builtin_aliases = {managed_builtin, str(canonical_builtin_path)}
    managed_alias_union = managed_aliases | managed_builtin_aliases
    previous_aliases: dict[str, str] = {}
    previous_builtin_aliases: set[str] = set()
    for path in previous_managed_repos:
        lexical_previous = str(Path(os.path.abspath(path.expanduser())))
        try:
            canonical_previous = str(_canonical_path_preserving_final(path))
        except ConfigurationError:
            canonical_previous = lexical_previous
        if canonical_previous in managed_alias_union:
            continue
        previous_aliases[lexical_previous] = canonical_previous
        previous_aliases[canonical_previous] = canonical_previous
        if Path(canonical_previous).name == "builtin":
            previous_builtin_aliases.update({lexical_previous, canonical_previous})
    token_source = managed if managed_builtin is None else f"{managed}\0{managed_builtin}"
    token = exchange_identity or hashlib.sha256(token_source.encode("utf-8")).hexdigest()
    try:
        _require_sha256(token, field="repository_exchange_identity")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    temporary = repos_file.with_name(f".{repos_file.name}.{token}.exchange")
    raw, before_identity = _read_regular_bounded_with_identity(
        repos_file,
        maximum=MAX_JARVIS_REPOS_BYTES,
    )
    payload, added_managed, removed_previous = _managed_repository_payload(
        raw,
        managed=managed,
        managed_aliases=managed_aliases,
        managed_builtin=managed_builtin,
        managed_builtin_aliases=managed_builtin_aliases,
        previous_aliases=previous_aliases,
        previous_builtin_aliases=previous_builtin_aliases,
    )
    if temporary.exists() or temporary.is_symlink():
        displaced, _displaced_identity = _read_regular_bounded_with_identity(
            temporary,
            maximum=MAX_JARVIS_REPOS_BYTES,
        )
        displaced_payload, displaced_added, displaced_removed = _managed_repository_payload(
            displaced,
            managed=managed,
            managed_aliases=managed_aliases,
            managed_builtin=managed_builtin,
            managed_builtin_aliases=managed_builtin_aliases,
            previous_aliases=previous_aliases,
            previous_builtin_aliases=previous_builtin_aliases,
        )
        if displaced != displaced_payload and displaced_payload == raw:
            temporary.unlink()
            _fsync_directory(repos_file.parent)
            return {
                "action": "updated",
                "managed_repo": managed,
                "added_managed_repos": displaced_added,
                "removed_previous_managed_repos": displaced_removed,
                "before_sha256": hashlib.sha256(displaced).hexdigest(),
                "after_sha256": hashlib.sha256(raw).hexdigest(),
            }
        if raw != payload and payload == displaced:
            temporary.unlink()
            _fsync_directory(repos_file.parent)
        else:
            raise ConfigurationError(
                "JARVIS repository exchange recovery found unproven path states: "
                f"{repos_file}, {temporary}"
            )
    if payload == raw:
        return {
            "action": "reused",
            "managed_repo": managed,
            "added_managed_repos": [],
            "removed_previous_managed_repos": [],
            "before_sha256": hashlib.sha256(raw).hexdigest(),
            "after_sha256": hashlib.sha256(raw).hexdigest(),
        }
    exchanged = False
    try:
        with temporary.open("xb") as stream:
            os.chmod(temporary, 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        desired_identity = _stat_identity(temporary.lstat())
        exchanged = True
        _atomic_exchange_paths(temporary, repos_file)
        try:
            displaced, displaced_identity = _read_regular_bounded_with_identity(
                temporary,
                maximum=MAX_JARVIS_REPOS_BYTES,
            )
        except ConfigurationError:
            displaced = b""
            displaced_identity = (-1, -1, -1, -1, -1, -1)
        if displaced != raw or not _identity_matches_after_rename(
            before_identity, displaced_identity
        ):
            try:
                active, active_identity = _read_regular_bounded_with_identity(
                    repos_file,
                    maximum=MAX_JARVIS_REPOS_BYTES,
                )
            except ConfigurationError as exc:
                raise ConfigurationError(
                    "JARVIS repositories changed during atomic reconciliation; "
                    f"displaced state retained at {temporary}"
                ) from exc
            if active != payload or not _identity_matches_after_rename(
                desired_identity, active_identity
            ):
                raise ConfigurationError(
                    "JARVIS repositories changed during atomic reconciliation; "
                    f"displaced state retained at {temporary}"
                )
            _atomic_exchange_paths(temporary, repos_file)
            exchanged = False
            _fsync_directory(repos_file.parent)
            raise ConfigurationError("JARVIS repositories changed during reconciliation")
        try:
            active, active_identity = _read_regular_bounded_with_identity(
                repos_file,
                maximum=MAX_JARVIS_REPOS_BYTES,
            )
        except ConfigurationError as exc:
            temporary.unlink()
            exchanged = False
            _fsync_directory(repos_file.parent)
            raise ConfigurationError(
                "JARVIS repositories changed after atomic reconciliation"
            ) from exc
        if active != payload or not _identity_matches_after_rename(
            desired_identity, active_identity
        ):
            temporary.unlink()
            exchanged = False
            _fsync_directory(repos_file.parent)
            raise ConfigurationError("JARVIS repositories changed after atomic reconciliation")
        temporary.unlink()
        exchanged = False
        _fsync_directory(repos_file.parent)
    except BaseException:
        if not exchanged:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise
    return {
        "action": "updated",
        "managed_repo": managed,
        "added_managed_repos": added_managed,
        "removed_previous_managed_repos": removed_previous,
        "before_sha256": hashlib.sha256(raw).hexdigest(),
        "after_sha256": hashlib.sha256(payload).hexdigest(),
    }


def repair_managed_jarvis_binding(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
    previous_managed_repos: tuple[Path, ...] = (),
) -> dict[str, object]:
    """Repair only relay's stable package link and exact repository registration."""
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    resolved_home = lexical_home.resolve(strict=True)
    generation_path = lexical_home / ".local/share/clio-relay/generations" / desired.fingerprint
    generation = generation_path.resolve(strict=True)
    generations = (lexical_home / ".local/share/clio-relay/generations").resolve(strict=True)
    if (
        generation_path.is_symlink()
        or generation.parent != generations
        or generation.name != desired.fingerprint
    ):
        raise ConfigurationError("desired generation path is not one owned directory")
    current = lexical_home / ".local/share/clio-relay/current"
    _verify_stable_symlink(current, expected=generation, label="active generation")
    expected_target = current / "source/jarvis-packages/clio_relay"
    if not expected_target.resolve(strict=True).is_dir():
        raise ConfigurationError("desired generation has no relay JARVIS package repository")
    managed = _expand_home(desired.managed_jarvis_repo, lexical_home)
    managed.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    snapshot = _capture_activation_path(
        managed,
        kind="symlink",
        maximum=4096,
        allow_absent=True,
    )
    if snapshot.before is not None:
        lexical_target = _activation_symlink_lexical_target(snapshot)
        try:
            target_is_current = lexical_target.resolve(strict=True) == expected_target.resolve(
                strict=True
            )
        except (OSError, RuntimeError, ValueError):
            target_is_current = False
        if lexical_target != expected_target and not target_is_current:
            proven_targets = {
                Path(os.path.abspath(path.expanduser())) for path in previous_managed_repos
            }
            fixed_legacy_target = lexical_home / ".local/src/clio-relay/jarvis-packages/clio_relay"
            try:
                target_is_fixed_legacy = (
                    lexical_target == fixed_legacy_target
                    or lexical_target.resolve(strict=True)
                    == fixed_legacy_target.resolve(strict=True)
                )
            except (OSError, RuntimeError, ValueError):
                target_is_fixed_legacy = lexical_target == fixed_legacy_target
            target_is_proven_generation = bool(
                lexical_target in proven_targets
                and _is_generation_repository_target(
                    lexical_target.resolve(strict=True),
                    home=resolved_home,
                )
            )
            if not target_is_fixed_legacy and not target_is_proven_generation:
                raise ConfigurationError(
                    "relay-managed repository link target is not proven by an earlier receipt"
                )
    link_action = _reconcile_activation_symlink(
        snapshot,
        expected_target=expected_target,
        label="relay-managed repository",
        exchange_identity=desired.fingerprint,
    )
    repos_file = _expand_home(desired.jarvis_root, lexical_home) / "repos.yaml"
    legacy_managed_repo = _expand_home(LEGACY_MANAGED_JARVIS_REPO_PATH, lexical_home)
    relay_owned_builtin_repos = _relay_owned_jarvis_builtin_repositories(
        home=lexical_home,
        execution_environments=(generation / "jarvis-venv",),
    )
    repo_evidence = reconcile_managed_jarvis_repository(
        repos_file,
        managed,
        managed_builtin_repo=_expand_home(desired.jarvis_root, lexical_home) / "builtin",
        previous_managed_repos=(
            *previous_managed_repos,
            legacy_managed_repo,
            *relay_owned_builtin_repos,
        ),
        exchange_identity=desired.fingerprint,
    )
    return {
        "link_action": link_action,
        "link": str(_expand_home(desired.managed_jarvis_repo, lexical_home)),
        "target": str(
            lexical_home / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        ),
        "repositories": repo_evidence,
    }
