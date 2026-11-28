"""The packages relay can run: each site's JARVIS root, as the site lists it (RL24, RL42).

Every package relay runs on a site is in that site's JARVIS root
(:data:`clio_relay.jarvis_site.SITE_ROOT_DIR`): clio-jarvis, which the site's
setup clones, and any repository the person's agent copied to the site and
registered with ``packages(op="add")``. :data:`~clio_relay.jarvis_site.PACKAGES_SCRIPT`
lists them through the adapter's exec: JARVIS's ``jarvis repo list +json``
(each package's ``_configure_menu`` and ``contract.yaml``, which JARVIS reads
without running the package) and each package's review digest and files.
Relay turns those into the parameters, and the title, summary, version,
outputs, progress and resources of the contract. The catalog keeps one file
per site at ``<state>/packages/<site>.json``.

``describe(site=...)`` lists the catalog, so an agent finds a package by name.
``run`` takes ``package: {name, parameters}``, and :meth:`PackageCatalog.resolve`
fills the version and digest before the guard sees the job, so the review is
pinned to the package's content on the site. The contract's resources, read
with the run's parameters, fill the walltime, queue and process layout the job
leaves unset.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, Field

from clio_relay.jarvis_site import parse_packages, refuse_builtin
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import COUPLING_KINDS, Coupling, JobSpec, PackageRef, step_ids

if TYPE_CHECKING:
    from clio_relay.adapters.base import BaseAdapter

ADD_COMMAND = (
    "the files tool, to copy a repository laid out <name>/<name>/<package>/pkg.py to the "
    'site, then packages(op="add", site=..., path=<its path on the site>)'
)
"""How an agent adds a package of its own. Refusals that find none name it."""
_MENU_TYPES = {"int": "integer", "float": "number", "str": "string", "bool": "boolean"}
_OUTPUT_KEYS = ("name", "path", "path_kind", "format", "reader")


class PackageEntry(BaseModel):
    """One package of a site's JARVIS root."""

    name: str
    version: str
    digest: str
    """The review digest of the package's content in the root
    (:data:`clio_relay.jarvis_site.DIGEST_FUNCTIONS`)."""
    kind: str | None = None
    title: str | None = None
    """The contract's ``title``: the software's name as a person knows it (LAMMPS)."""
    summary: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict[str, Any])
    outputs: list[dict[str, Any]] = Field(default_factory=list[dict[str, Any]])
    progress: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    data_flow: dict[str, Any] | None = None
    """A service's ``service.data_flow``: ``direction`` (``in``, the other steps' outputs
    feed it; ``out``, it feeds them), ``kind`` and ``label``."""
    repo_dir: str = ""
    source: dict[str, Any] | None = None
    """``{repository, commit}`` when the repository is a git clone."""
    files: list[dict[str, Any]] = Field(default_factory=list[dict[str, Any]])
    """The reviewed files: ``path`` (from the repository's package directory),
    ``sha256``, ``size`` and ``executable``."""


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


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


def describe_package(listed: dict[str, Any]) -> PackageEntry:
    """Return the catalog entry of one package :func:`~clio_relay.jarvis_site.parse_packages` read.

    The version is the contract's, else the git commit's first 12 characters,
    else the digest's.
    """
    loaded = listed.get("contract")
    contract = cast(dict[str, Any], loaded) if isinstance(loaded, dict) else {}
    git = cast(dict[str, str], listed.get("git") or {})
    source = {"repository": git.get("url", ""), "commit": git["commit"]} if git else None
    digest = str(listed["digest"])
    chosen = contract.get("version") or (git.get("commit") or digest)[:12]
    outputs = [
        {key: _json(item[key]) for key in _OUTPUT_KEYS if key in item}
        for item in cast(list[Any], contract.get("outputs") or [])
        if isinstance(item, dict)
    ]
    progress = contract.get("progress")
    resources = contract.get("resources")
    service = contract.get("service")
    flow = cast(dict[str, Any], service).get("data_flow") if isinstance(service, dict) else None
    return PackageEntry(
        name=str(listed["name"]),
        version=str(chosen),
        digest=digest,
        kind=str(contract["kind"]) if contract.get("kind") else None,
        title=" ".join(str(contract["title"]).split()) if contract.get("title") else None,
        summary=" ".join(str(contract["summary"]).split()) if contract.get("summary") else None,
        parameters=_parameters(cast(list[dict[str, Any]] | None, listed.get("menu")), contract),
        outputs=outputs,
        progress={
            key: _json(value)
            for key, value in cast(dict[str, Any], progress).items()
            if key in ("unit", "output")
        }
        if isinstance(progress, dict)
        else None,
        resources=_json(resources) if isinstance(resources, dict) else None,
        data_flow=contract_data_flow(flow),
        repo_dir=str(listed.get("repo_dir") or ""),
        source=source,
        files=cast(list[dict[str, Any]], listed.get("files") or []),
    )


def contract_data_flow(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    flow = cast(dict[str, Any], value)
    if flow.get("direction") not in ("in", "out") or flow.get("kind") not in COUPLING_KINDS:
        return None
    found = {"direction": flow["direction"], "kind": flow["kind"]}
    if isinstance(flow.get("label"), str) and flow["label"].strip():
        found["label"] = " ".join(flow["label"].split())
    return found


def contract_couplings(
    steps: Sequence[PackageRef], entries: Sequence[PackageEntry | None]
) -> list[Coupling]:
    """Return the couplings the steps' contracts declare, producer to consumer.

    A service whose ``data_flow`` is ``in`` reads the outputs of every other step
    whose contract declares outputs; one whose ``data_flow`` is ``out`` feeds every
    other step. Each end is the step's id (:func:`~clio_relay.jobs.spec.step_ids`).
    """
    if len(steps) < 2:
        return []
    ids = step_ids(list(steps))
    found: list[Coupling] = []
    for index, entry in enumerate(entries):
        flow = entry.data_flow if entry is not None else None
        if flow is None:
            continue
        for other, peer in enumerate(entries):
            if other == index:
                continue
            if flow["direction"] == "in" and not (peer is not None and peer.outputs):
                continue
            ends = (
                (ids[other], ids[index]) if flow["direction"] == "in" else (ids[index], ids[other])
            )
            edge = Coupling.model_validate(
                {"from": ends[0], "to": ends[1], "kind": flow["kind"], "label": flow.get("label")}
            )
            if all((e.producer, e.consumer) != ends for e in found):
                found.append(edge)
    return found


def with_contract_couplings(spec: JobSpec, entries: Sequence[PackageEntry | None]) -> JobSpec:
    """Fill the pipeline's couplings from its steps' contracts when the job names none."""
    if spec.couplings:
        return spec
    found = contract_couplings(spec.steps(), entries)
    return spec.model_copy(update={"couplings": found}) if found else spec


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

    The arithmetic is ``{div|mul|add|ceil_div: [a, b]}`` over numbers, parameter
    names and nested arithmetic, as clio-jarvis evaluates it. A missing operand
    gives ``None``.
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
        _number(parameters.get(item) if isinstance(item, str) else contract_value(item, parameters))
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


def person_name(package: str, titles: Mapping[str, str] | None = None) -> str:
    """The name a person knows a package by: its contract's title, else its short name."""
    return (titles or {}).get(package) or package.partition(".")[2] or package


class PackageCatalog:
    """The packages each site's JARVIS root holds, as the site last listed them."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, site: str) -> Path:
        return self.root / f"{site}.json"

    def all(self, site: str) -> list[PackageEntry]:
        """Return the site's packages, by name; none before relay listed them."""
        try:
            raw = json.loads(self._path(site).read_text(encoding="utf-8"))
            return [PackageEntry.model_validate(item) for item in cast(list[Any], raw)]
        except (OSError, ValueError):
            return []

    def titles(self, site: str) -> dict[str, str]:
        """Each package's contract title, by package name."""
        return {entry.name: entry.title for entry in self.all(site) if entry.title}

    def store(self, site: str, listed: list[dict[str, Any]]) -> list[PackageEntry]:
        """Record what the site listed. A package whose digest relay read before is kept."""
        known = {(entry.name, entry.digest): entry for entry in self.all(site)}
        entries: dict[str, PackageEntry] = {}
        for item in listed:
            key = (str(item["name"]), str(item["digest"]))
            entries.setdefault(key[0], known.get(key) or describe_package(item))
        found = sorted(entries.values(), key=lambda entry: entry.name)
        self.root.mkdir(parents=True, exist_ok=True)
        partial = self._path(site).with_suffix(".part")
        partial.write_text(json.dumps([e.model_dump(mode="json") for e in found]), "utf-8")
        partial.replace(self._path(site))
        return found

    async def refresh(
        self, site: str, adapter: BaseAdapter[Any], *, add: str = "", force: bool = False
    ) -> list[PackageEntry] | None:
        """List the site's root again through the adapter; ``None`` when it is not set up."""
        listed = parse_packages(await adapter.site_packages(add, force=force))
        return None if listed is None else self.store(site, listed)

    def resolve(self, site: str, package: PackageRef) -> PackageRef:
        """Fill ``package``'s version and digest from the site's catalog.

        The package is pinned to what the site's root holds now. A digest the
        root no longer holds refuses with ``invalid_spec``, naming the current
        one, so the caller asks the person again. A pinned package the catalog
        has no entry of passes as it is, for the site script to check.
        jarvis-cd's builtin packages are refused (:func:`refuse_builtin`).
        """
        refuse_builtin(package.name)
        found = self.entry(site, package.model_copy(update={"sha256": None}))
        if found is None and package.version is not None and package.sha256 is not None:
            return package
        if found is None:
            known = sorted(entry.name for entry in self.all(site))
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"{site} has no package {package.name}. It has: {', '.join(known) or 'none'}. "
                f"Add one with {ADD_COMMAND}",
                package=package.name,
                known=known,
            )
        if package.sha256 not in (None, found.digest) or package.version not in (
            None,
            found.version,
        ):
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"{package.name} on {site} is now version {found.version} digest "
                f"{found.digest[:12]}, not the one the job pins; leave the pin out or use it",
                field="package",
                package=package.name,
                candidates=[{"version": found.version, "sha256": found.digest}],
                next_step="pin_package",
            )
        return package.model_copy(update={"version": found.version, "sha256": found.digest})

    def entry(self, site: str, package: PackageRef) -> PackageEntry | None:
        """Return the site's entry of ``package``, or ``None`` when the root has none."""
        for entry in self.all(site):
            if entry.name == package.name and package.sha256 in (None, entry.digest):
                return entry
        return None
