"""The packages relay can run: its catalog of staged package archives (RL24, RL42).

Staging a package puts its archive in relay's content store and records it in
the catalog, one entry per archive at ``<state>/packages/<sha256>.json``. An
agent stages one with the ``packages`` tool (:mod:`clio_relay.surface.staging`),
from a git repository at one commit or from a directory on the relay host;
``python -m clio_relay.packages`` does the same from a shell. The
entry says what the archive holds, read from the archive itself and never by
running it: the parameters of ``_configure_menu`` in ``pkg.py``, and the
summary, version, outputs, progress and resources of a ``contract.yaml``
beside it when the package has one.

``describe(site=...)`` lists the catalog, so an agent finds a package by name.
``run`` takes ``package: {name, parameters}``, and :meth:`PackageCatalog.resolve`
fills the version and SHA-256 before the guard sees the job, so the review is
still pinned to one archive. The contract's resources, read with the run's
parameters, fill the walltime, queue and process layout the job leaves unset.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, cast

import yaml
from pydantic import BaseModel, Field

from clio_relay.jarvis_site import (
    CLIO_JARVIS_COMMIT,
    CLIO_JARVIS_URL,
    archive_package,
    git_source,
    inspect_archive,
    split_name,
)
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import RecordStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, PackageRef

STAGE_COMMAND = (
    'the packages tool: packages(op="stage", package="<repository>.<package>"), from a git '
    "repository at a commit (git, commit; by default relay's pinned clio-jarvis) or from a "
    "directory on the relay host (dir)"
)
"""How an agent stages a package. Refusals that find none name it."""
GIT_TIMEOUT_S = 300.0
_GIT_URL = re.compile(r"(?:https?|ssh|git|file)://[^\s]+|[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[^\s]+")
_COMMIT = re.compile(r"[0-9a-fA-F]{7,40}")
_MENU_TYPES = {"int": "integer", "float": "number", "str": "string", "bool": "boolean"}
_OUTPUT_KEYS = ("name", "path", "path_kind", "format", "reader")


class PackageEntry(BaseModel):
    """One staged package archive."""

    name: str
    version: str
    sha256: str
    kind: str | None = None
    summary: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict[str, Any])
    outputs: list[dict[str, Any]] = Field(default_factory=list[dict[str, Any]])
    progress: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    source: dict[str, Any] | None = None
    staged_at: datetime = Field(default_factory=utc_now)


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _menu(source: str) -> list[dict[str, Any]] | None:
    """Return the literal list ``_configure_menu`` returns, without running pkg.py."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name != "_configure_menu":
            continue
        returns = [item for item in ast.walk(node) if isinstance(item, ast.Return)]
        if len(returns) != 1 or not isinstance(returns[0].value, ast.List):
            return None
        entries: list[dict[str, Any]] = []
        for element in returns[0].value.elts:
            if not isinstance(element, ast.Dict):
                return None
            entry: dict[str, Any] = {}
            for key_node, value_node in zip(element.keys, element.values, strict=True):
                if key_node is None:
                    return None
                try:
                    key = ast.literal_eval(key_node)
                    entry[str(key)] = (
                        getattr(value_node, "id", None)
                        if key == "type"
                        else ast.literal_eval(value_node)
                    )
                except ValueError:
                    return None
            if isinstance(entry.get("name"), str):
                entries.append(entry)
        return entries
    return None


def _parameters(menu: list[dict[str, Any]] | None, contract: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON Schema of the parameters: the menu, refined by the contract."""
    refinement = cast(dict[str, Any], contract.get("parameters") or {})
    properties: dict[str, Any] = {}
    for entry in menu or []:
        prop: dict[str, Any] = {}
        if entry.get("msg"):
            prop["description"] = str(entry["msg"])
        if _MENU_TYPES.get(str(entry.get("type"))):
            prop["type"] = _MENU_TYPES[str(entry.get("type"))]
        if entry.get("default") is not None:
            prop["default"] = _json(entry["default"])
        if entry.get("choices"):
            prop["enum"] = _json(list(entry["choices"]))
        properties[str(entry["name"])] = prop
    for name, extra in cast(dict[str, Any], refinement.get("properties") or {}).items():
        if isinstance(extra, dict):
            properties[name] = {**properties.get(name, {}), **_json(extra)}
    if not properties:
        return {}
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if refinement.get("required"):
        schema["required"] = [str(item) for item in refinement["required"]]
    return schema


def _members(data: bytes, wanted: set[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar.getmembers():
            if member.name in wanted and member.isfile():
                extracted = tar.extractfile(member)
                if extracted is not None:
                    found[member.name] = extracted.read().decode("utf-8", errors="replace")
    return found


def describe_archive(data: bytes, name: str, *, version: str | None = None) -> PackageEntry:
    """Return the catalog entry of the archive ``data`` of package ``name``.

    The version is ``version`` when given, else the contract's, else the git
    tag the archive was built at. With none of these it refuses.
    """
    info = inspect_archive(data, name)
    repo, package = split_name(name)
    base = f"{repo}/{repo}/{package}"
    texts = _members(data, {f"{base}/pkg.py", f"{base}/contract.yaml"})
    contract: dict[str, Any] = {}
    if f"{base}/contract.yaml" in texts:
        try:
            loaded = yaml.safe_load(texts[f"{base}/contract.yaml"])
        except yaml.YAMLError:
            loaded = None
        contract = cast(dict[str, Any], loaded) if isinstance(loaded, dict) else {}
    source = info.source or {}
    chosen = version or contract.get("version") or source.get("tag")
    if not chosen:
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"{name} has no contract.yaml version and no git tag; give one with --version",
            package=name,
        )
    outputs = [
        {key: _json(item[key]) for key in _OUTPUT_KEYS if key in item}
        for item in cast(list[Any], contract.get("outputs") or [])
        if isinstance(item, dict)
    ]
    progress = contract.get("progress")
    resources = contract.get("resources")
    return PackageEntry(
        name=name,
        version=str(chosen),
        sha256=info.sha256,
        kind=str(contract["kind"]) if contract.get("kind") else None,
        summary=" ".join(str(contract["summary"]).split()) if contract.get("summary") else None,
        parameters=_parameters(_menu(texts.get(f"{base}/pkg.py", "")), contract),
        outputs=outputs,
        progress={
            key: _json(value)
            for key, value in cast(dict[str, Any], progress).items()
            if key in ("unit", "output")
        }
        if isinstance(progress, dict)
        else None,
        resources=_json(resources) if isinstance(resources, dict) else None,
        source=info.source,
    )


# A contract's resources (RL15): PSI/J names whose values may read the parameters

_LAYOUT = ("node_count", "process_count", "processes_per_node")
_COUNTS = (*_LAYOUT, "cpu_cores_per_process", "gpu_cores_per_process", "memory")


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def contract_value(value: Any, parameters: dict[str, Any]) -> Any:
    """Evaluate a contract value: a literal, ``{param: name}``, or integer arithmetic.

    The arithmetic is ``{div|mul|add|ceil_div: [a, b]}`` over numbers and
    parameter names, as clio-jarvis evaluates it. A missing operand gives ``None``.
    """
    if not isinstance(value, dict):
        return value
    rule = cast(dict[str, Any], value)
    if "param" in rule:
        return parameters.get(str(rule["param"]))
    if len(rule) != 1:
        return None
    ((operator, operands),) = rule.items()
    if not isinstance(operands, list) or len(cast(list[Any], operands)) != 2:
        return None
    a, b = (
        _number(parameters.get(item) if isinstance(item, str) else item)
        for item in cast(list[Any], operands)
    )
    if a is None or b is None:
        return None
    if operator == "mul":
        return a * b
    if operator == "add":
        return a + b
    if operator not in ("div", "ceil_div") or int(b) == 0:
        return None
    return int(a) // int(b) if operator == "div" else -(-int(a) // int(b))


def contract_resources(entry: PackageEntry, parameters: dict[str, Any]) -> dict[str, Any]:
    """Return the PSI/J resources ``entry``'s contract asks for with ``parameters``.

    A parameter the run leaves out takes its default. A value that resolves to
    nothing, an empty queue or a count that is not a positive whole number is
    left out, so the site's defaults still apply to it.
    """
    properties = cast(dict[str, Any], entry.parameters.get("properties") or {})
    values: dict[str, Any] = {}
    for name, prop in properties.items():
        if isinstance(prop, dict) and "default" in prop:
            values[name] = cast(dict[str, Any], prop)["default"]
    values.update(parameters)
    found: dict[str, Any] = {}
    for key, rule in (entry.resources or {}).items():
        value = contract_value(rule, values)
        number = _number(value)
        if key == "queue_name" and isinstance(value, str) and value.strip():
            found[key] = value.strip()
        elif key == "exclusive_node_use" and isinstance(value, bool):
            found[key] = value
        elif key == "duration" and number is not None and number > 0:
            found[key] = timedelta(seconds=number)
        elif key in _COUNTS and number is not None and number >= 1 and number.is_integer():
            found[key] = int(number)
    return found


def with_contract_resources(spec: JobSpec, entries: Sequence[PackageEntry | None]) -> JobSpec:
    """Fill what the steps' contracts set for ``spec`` where ``spec`` leaves it unset.

    ``entries`` are the catalog entries of ``spec.steps()``, in order, with None
    for a step relay has no entry for. The walltime and queue are filled one by
    one: a pipeline takes the longest walltime and the first queue its steps
    set. A one-step job takes its contract's process layout (nodes, processes,
    processes per node) when the job sets none of it. A pipeline's steps share
    one allocation, so their layouts do not add up; a pipeline that sets no
    node count gets the last node its steps' ``host_slice`` parameters name.
    """
    steps = spec.steps()
    if not steps:
        return spec
    found: dict[str, Any] = {}
    for step, entry in zip(steps, entries, strict=True):
        if entry is None:
            continue
        own = contract_resources(entry, step.parameters)
        if len(steps) == 1:
            found = own
        if "duration" in own and own["duration"] > found.get("duration", timedelta(0)):
            found["duration"] = own["duration"]
        if "queue_name" in own:
            found.setdefault("queue_name", own["queue_name"])
    attributes = {
        key: found[key]
        for key in ("duration", "queue_name")
        if key in found and getattr(spec.attributes, key) is None
    }
    own_layout = any(getattr(spec.resources, key) is not None for key in _LAYOUT)
    resources = {
        key: value
        for key, value in found.items()
        if (key in _COUNTS and getattr(spec.resources, key) is None)
        or (key == "exclusive_node_use" and key not in spec.resources.model_fields_set)
        if not (own_layout and key in _LAYOUT)
    }
    last = slice_end(steps)
    if len(steps) > 1 and not own_layout and last is not None:
        resources["node_count"] = last
    if not attributes and not resources:
        return spec
    return spec.model_copy(
        update={
            "attributes": spec.attributes.model_copy(update=attributes),
            "resources": spec.resources.model_copy(update=resources),
        }
    )


CORES_PARAMETER = "cores_per_node"
"""The package parameter JARVIS reads a node's cores from: an empty ``ppn`` takes it."""


def with_site_cores(
    spec: JobSpec, entries: Sequence[PackageEntry | None], cores: int | None
) -> JobSpec:
    """Set ``cores_per_node`` to the partition's physical cores where a step leaves it unset.

    Only a step whose package declares the parameter gets it, so a package that
    has none is configured as before. A value the run sets, other than empty,
    wins. ``cores`` is the physical cores per node of the partition the job runs
    in, from the site's ``describe``; with none known nothing changes.
    """
    steps = spec.steps()
    if not steps or not cores:
        return spec
    changed = False
    filled: list[PackageRef] = []
    for step, entry in zip(steps, entries, strict=True):
        properties = cast(
            dict[str, Any], (entry.parameters if entry else {}).get("properties") or {}
        )
        current = step.parameters.get(CORES_PARAMETER)
        if CORES_PARAMETER in properties and current in (None, "", 0):
            step = step.model_copy(
                update={"parameters": {**step.parameters, CORES_PARAMETER: cores}}
            )
            changed = True
        filled.append(step)
    return spec.with_steps(filled) if changed else spec


def node_range(step: PackageRef) -> tuple[int, int] | None:
    """Return the ``[start, end)`` nodes a step's ``host_slice`` names.

    JARVIS reads ``host_slice`` as a Python slice of the allocation's hosts. A
    slice with no end, or a negative one, depends on the allocation's size, so
    it gives None here.
    """
    value = step.nodes()
    match = re.fullmatch(r"(\d*):(\d+)", value.strip()) if value is not None else None
    if match is None:
        return None
    first = int(match.group(1) or 0)
    last = int(match.group(2))
    return (first, last) if first < last else None


def slice_end(steps: Sequence[PackageRef]) -> int | None:
    """Return how many nodes the steps' host slices reach, or None when none sets one."""
    ends = [found[1] for found in map(node_range, steps) if found is not None]
    return max(ends) if ends else None


def pipeline_conflict(entries: Sequence[PackageEntry | None]) -> Refusal | None:
    """Return why the steps cannot share one pipeline, or None when they can.

    JARVIS loads every step's archive into one tree per repository, so two steps
    from one repository must come from the same commit.
    """
    commits: dict[str, tuple[str, str]] = {}
    for entry in entries:
        commit = (entry.source or {}).get("commit") if entry is not None else None
        if entry is None or not isinstance(commit, str) or not commit:
            continue
        repo, _package = split_name(entry.name)
        seen = commits.setdefault(repo, (commit, entry.name))
        if seen[0] != commit:
            return Refusal(
                RefusalReason.INVALID_SPEC,
                f"{seen[1]} and {entry.name} come from different commits of the {repo} "
                f"repository ({seen[0][:12]} and {commit[:12]}); a pipeline's steps from one "
                "repository must be staged from the same commit",
                repository=repo,
            )
    return None


class PackageCatalog:
    """The package archives staged on this relay host."""

    def __init__(self, root: Path) -> None:
        self.records: RecordStore[PackageEntry] = RecordStore(root, PackageEntry)

    def put(self, entry: PackageEntry) -> None:
        """Record ``entry``, replacing an earlier entry of the same archive."""
        self.records.put(entry.sha256, entry)

    def remove(
        self, name: str, *, version: str | None = None, sha256: str | None = None
    ) -> list[PackageEntry]:
        """Remove every entry of ``name`` matching ``version`` and ``sha256``; return them."""
        removed: list[PackageEntry] = []
        for entry in self.all():
            if entry.name != name or version not in (None, entry.version):
                continue
            if sha256 is not None and entry.sha256 != sha256.lower():
                continue
            if self.records.delete(entry.sha256):
                removed.append(entry)
        return removed

    def all(self) -> list[PackageEntry]:
        """Return every entry, newest version of each name last."""
        return sorted(self.records.all(), key=lambda entry: (entry.name, entry.staged_at))

    def newest(self) -> list[PackageEntry]:
        """Return the newest entry of each package name, by when it was staged."""
        latest: dict[str, PackageEntry] = {}
        for entry in self.all():
            latest[entry.name] = entry
        return list(latest.values())

    def resolve(self, package: PackageRef) -> PackageRef:
        """Fill ``package``'s version and SHA-256 from the catalog.

        A package that gives both is returned as it is. Otherwise the one
        staged archive of that name matching what it gives is pinned. None
        refuses with ``invalid_spec``, and so do several: relay never picks one
        for the person. The refusal lists the candidates, so the caller asks
        which one and pins its ``version`` or ``sha256``.
        """
        if package.version is not None and package.sha256 is not None:
            return package
        found = [
            entry
            for entry in self.all()
            if entry.name == package.name
            and package.version in (None, entry.version)
            and package.sha256 in (None, entry.sha256)
        ]
        wanted = " ".join(part for part in (package.name, package.version, package.sha256) if part)
        if not found:
            known = sorted({entry.name for entry in self.all()})
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"relay has no package {wanted}. It has: {', '.join(known) or 'none'}. "
                f"Stage one with {STAGE_COMMAND}",
                package=package.name,
                known=known,
            )
        if len(found) > 1:
            candidates = [
                {
                    "version": entry.version,
                    "sha256": entry.sha256,
                    "staged_at": entry.staged_at.isoformat(),
                }
                for entry in reversed(found)
            ]
            listed = "; ".join(
                f"{item['version'] or 'no version'} sha256 {item['sha256'][:12]} "
                f"staged {item['staged_at']}"
                for item in candidates
            )
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"relay has {len(found)} archives of {wanted}: {listed}. Ask the person "
                "which one, then pin its version or sha256",
                field="package",
                package=package.name,
                candidates=candidates,
                next_step="pin_package",
            )
        chosen = found[0]
        return package.model_copy(update={"version": chosen.version, "sha256": chosen.sha256})

    def entry(self, package: PackageRef, content: ContentStore) -> PackageEntry | None:
        """Return the entry of the pinned ``package``, or ``None`` if it was not staged here.

        An entry staged before relay read the contract's resources is read again
        from its archive in ``content``, and saved.
        """
        found = None if package.sha256 is None else self.records.get(package.sha256)
        if found is None or "resources" in found.model_fields_set or not content.has(found.sha256):
            return found
        try:
            fresh = describe_archive(content.get(found.sha256), found.name, version=found.version)
        except Refusal:
            return found
        updated = found.model_copy(update={"resources": fresh.resources})
        self.put(updated)
        return updated


def stage(state_dir: Path, data: bytes, name: str, *, version: str | None = None) -> PackageEntry:
    """Put an archive in relay's content store and record it in the catalog."""
    entry = describe_archive(data, name, version=version)
    ContentStore(state_dir / "content").put(data)
    PackageCatalog(state_dir / "packages").put(entry)
    return entry


def repository_dir(root: Path, name: str) -> Path:
    """Return the JARVIS repository of package ``name`` at ``root`` or just below it.

    That is a directory named like the repository, holding a Python package of
    the same name with ``<package>/pkg.py``: ``root`` itself, or ``root/<repo>``
    as in clio-jarvis, whose checkout holds ``clio_jarvis/clio_jarvis/...``.
    """
    repo, package = split_name(name)
    for candidate in (root, root / repo):
        if candidate.name == repo and (candidate / repo / package / "pkg.py").is_file():
            return candidate
    raise Refusal(
        RefusalReason.INVALID_ARGUMENTS,
        f"{root} holds no JARVIS repository with {name}: relay looks for a directory "
        f"named {repo} holding {repo}/{package}/pkg.py, at {root} or {root / repo}",
        package=name,
    )


def _archive_and_stage(
    state_dir: Path, repo_dir: Path, name: str, version: str | None
) -> tuple[PackageEntry, int]:
    data = archive_package(repo_dir, name, source=git_source(repo_dir))
    return stage(state_dir, data, name, version=version), len(data)


def stage_dir(
    state_dir: Path, directory: Path, name: str, *, version: str | None = None
) -> tuple[PackageEntry, int]:
    """Stage package ``name`` from a directory on the relay host; return it and its size.

    ``directory`` is the JARVIS repository or the checkout that holds it.
    """
    root = directory.expanduser()
    if not root.is_absolute():
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"{directory} is relative; give the directory's absolute path on the relay host",
            path=str(directory),
        )
    if not root.is_dir():
        raise Refusal(
            RefusalReason.PATH_NOT_FOUND,
            f"{root} is not a directory on the relay host",
            path=str(root),
        )
    return _archive_and_stage(state_dir, repository_dir(root.resolve(), name), name, version)


def _git(args: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "never"}
    env.setdefault("GIT_SSH_COMMAND", "ssh -o BatchMode=yes")
    try:
        return subprocess.run(
            ["git", "-c", "protocol.ext.allow=never", *args],
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_S,
            check=False,
        )
    except FileNotFoundError as exc:
        raise Refusal(RefusalReason.INVALID_CONFIG, "git is not on the relay host's PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise Refusal(
            RefusalReason.TIMEOUT,
            f"git {args[0]} took longer than {GIT_TIMEOUT_S:.0f} s",
        ) from exc


def _tail(done: subprocess.CompletedProcess[str]) -> str:
    lines = (done.stderr or done.stdout or "").strip().splitlines()
    return " ".join(lines[-3:]) or f"exit {done.returncode}"


def fetch_commit(url: str, commit: str, checkout: Path) -> str:
    """Check out ``commit`` of the git repository ``url`` in ``checkout``; return it in full."""
    checkout.mkdir(parents=True)
    for step in (["init", "-q"], ["remote", "add", "origin", url]):
        done = _git(step, checkout)
        if done.returncode != 0:
            raise Refusal(RefusalReason.INPUT_MISSING, f"git {step[0]} failed: {_tail(done)}")
    ref = "FETCH_HEAD"
    fetched = _git(["fetch", "-q", "--depth", "1", "origin", commit], checkout)
    if fetched.returncode != 0:
        # A short commit, or a server that serves no commit by hash: fetch all of it.
        ref = commit
        fetched = _git(["fetch", "-q", "--tags", "origin"], checkout)
        if fetched.returncode != 0:
            raise Refusal(
                RefusalReason.INPUT_MISSING,
                f"relay could not fetch {url}: {_tail(fetched)}",
                url=url,
            )
    done = _git(["checkout", "-q", "--detach", ref], checkout)
    head = _git(["rev-parse", "HEAD"], checkout).stdout.strip()
    if done.returncode != 0 or not head.lower().startswith(commit.lower()):
        raise Refusal(
            RefusalReason.INPUT_MISSING,
            f"{url} has no commit {commit}",
            url=url,
            commit=commit,
        )
    return head


def stage_git(
    state_dir: Path,
    name: str,
    *,
    url: str | None = None,
    commit: str | None = None,
    path: str | None = None,
    version: str | None = None,
) -> tuple[PackageEntry, int]:
    """Stage package ``name`` from a git repository at one commit; return it and its size.

    With neither ``url`` nor ``commit`` it is relay's pinned clio-jarvis. ``path``
    is where the JARVIS repository sits in the checkout, when it is neither the
    top nor ``<repo>`` there.
    """
    if url is None and commit is None:
        url, commit = CLIO_JARVIS_URL, CLIO_JARVIS_COMMIT
    if url is None or commit is None:
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            "give git and commit together, or neither for relay's pinned clio-jarvis",
        )
    if not _GIT_URL.fullmatch(url):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"{url!r} is not a git URL relay fetches: use https://, ssh://, git://, file:// "
            "or user@host:path",
            url=url,
        )
    if not _COMMIT.fullmatch(commit):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"{commit!r} is not a commit: relay stages one fixed commit, 7 to 40 hex digits, "
            "not a branch or a tag",
            commit=commit,
        )
    relative = PurePosixPath(path) if path else None
    if relative is not None and (relative.is_absolute() or ".." in relative.parts):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"path {path!r} must stay inside the checkout",
            path=path,
        )
    repo, _package = split_name(name)
    with tempfile.TemporaryDirectory(
        prefix="clio-relay-stage-", ignore_cleanup_errors=True
    ) as scratch:
        checkout = Path(scratch) / repo
        fetch_commit(url, commit, checkout)
        root = checkout.joinpath(*relative.parts) if relative is not None else checkout
        return _archive_and_stage(state_dir, repository_dir(root, name), name, version)


# Command line


def _state_dir() -> Path:
    from clio_relay.surface.settings import RelayPaths

    return RelayPaths.from_env().state_dir


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m clio_relay.packages``: stage and list package archives."""
    parser = argparse.ArgumentParser(
        prog="python -m clio_relay.packages",
        description="Stage JARVIS package archives relay can run, and list them.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    put = sub.add_parser("stage", help="archive one package of a JARVIS repository and stage it")
    put.add_argument("repo_dir", type=Path, help="the directory `jarvis repo add` takes")
    put.add_argument("package", help="<repository>.<package>")
    put.add_argument("--version", help="the version, when the package has no contract or tag")
    put.add_argument("--output", type=Path, help="also write the archive here")
    put.add_argument("--dry-run", action="store_true", help="build and describe it, stage nothing")
    sub.add_parser("list", help="list the staged packages")
    args = parser.parse_args(argv)
    if args.command == "list":
        entries = PackageCatalog(_state_dir() / "packages").all()
        fields = {"name", "version", "sha256", "kind", "staged_at"}
        print(
            json.dumps(
                [entry.model_dump(mode="json", include=fields) for entry in entries], indent=2
            )
        )
        return 0
    try:
        data = archive_package(args.repo_dir, args.package, source=git_source(args.repo_dir))
        if args.dry_run:
            entry = describe_archive(data, args.package, version=args.version)
        else:
            entry = stage(_state_dir(), data, args.package, version=args.version)
    except Refusal as refusal:
        print(refusal.message, file=sys.stderr)
        return 2
    if args.output is not None:
        args.output.write_bytes(data)
    print(
        json.dumps(
            {
                **entry.model_dump(mode="json", include={"name", "version", "sha256", "source"}),
                "size": len(data),
                "staged": not args.dry_run,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
