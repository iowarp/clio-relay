"""The packages relay can run: its catalog of staged package archives (RL24, RL42).

Staging a package puts its archive in relay's content store and records it in
the catalog, one entry per archive at ``<state>/packages/<sha256>.json``. The
entry says what the archive holds, read from the archive itself and never by
running it: the parameters of ``_configure_menu`` in ``pkg.py``, and the
summary, version, outputs and progress of a ``contract.yaml`` beside it when
the package has one.

``describe(site=...)`` lists the catalog, so an agent finds a package by name.
``run`` takes ``package: {name, parameters}``, and :meth:`PackageCatalog.resolve`
fills the version and SHA-256 before the guard sees the job, so the review is
still pinned to one archive.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import sys
import tarfile
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import BaseModel, Field

from clio_relay.jarvis_site import archive_package, git_source, inspect_archive, split_name
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import RecordStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import PackageRef

STAGE_COMMAND = "python -m clio_relay.packages stage <repository dir> <repo.package>"
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
        source=info.source,
    )


class PackageCatalog:
    """The package archives staged on this relay host."""

    def __init__(self, root: Path) -> None:
        self.records: RecordStore[PackageEntry] = RecordStore(root, PackageEntry)

    def put(self, entry: PackageEntry) -> None:
        """Record ``entry``, replacing an earlier entry of the same archive."""
        self.records.put(entry.sha256, entry)

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

        A package that gives both is returned as it is. Otherwise the newest
        staged archive of that name matching what it gives is used, and none
        refuses with ``invalid_spec``.
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
        if not found:
            known = sorted({entry.name for entry in self.all()})
            wanted = " ".join(
                part for part in (package.name, package.version, package.sha256) if part
            )
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"relay has no package {wanted}. It has: {', '.join(known) or 'none'}. "
                f"Stage one with `{STAGE_COMMAND}`",
                package=package.name,
                known=known,
            )
        chosen = found[-1]
        return package.model_copy(update={"version": chosen.version, "sha256": chosen.sha256})


def stage(state_dir: Path, data: bytes, name: str, *, version: str | None = None) -> PackageEntry:
    """Put an archive in relay's content store and record it in the catalog."""
    entry = describe_archive(data, name, version=version)
    ContentStore(state_dir / "content").put(data)
    PackageCatalog(state_dir / "packages").put(entry)
    return entry


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
