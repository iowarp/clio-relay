"""The release-identity + contract pin registry (iowarp/clio-relay#198, #231 R7).

Every place in the tree where a release-identity value -- the relay's own
version, the clio-kit distribution pin, or the JARVIS MCP user contract
revision -- is *pinned* is registered here exactly once, as a
:class:`PinSite`. Nothing else in the tree independently decides "is this
site still correct" or "what else moves with it" -- that is precisely the
failure mode #198 describes (the 13-copy v3.7 bump, four dead protected
tags each dying on a missed site; design doc
``docs/design/relay-architecture-2026-08.md`` §7).

Three independent version axes are tracked:

* ``relay_version`` -- clio-relay's own release version.
* ``kit_version`` -- the pinned clio-kit distribution (version text and the
  wheel's SHA-256, both mirrored across CI and docs).
* ``jarvis_contract`` -- the JARVIS MCP user contract revision (its id
  literal, the contract file whose *path* embeds the revision, and the
  content/wire/artifact digests that certify it).

A :class:`PinSite`'s ``kind`` documents *how* it is recognized (doc §7's
selector taxonomy: line, key, filename, placeholder, regex, or
derived-digest); its ``value_group`` says which other sites must currently
agree with it -- exactly the sites one version bump moves together. The
:data:`PINSITES` table itself lives in the companion module
:mod:`clio_relay.release_pin_sites` (~70 rows -- too large for this file's
own 800-line cap alongside the logic below, the same reasoning that split
``frp_link.py`` from ``frp_transport.py`` in R4/R5); every name a caller
needs is re-exported from here.

``scripts/bump_release_version.py`` rewrites every mutable site through this
registry. ``scripts/check_release_identity.py`` is the fast preflight: every
value-group agrees, and no unregistered site pins the same family
(:func:`sweep_incompleteness`). Both are thin -- the logic lives here, the
single owner (ground rule 1, §2).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from clio_relay.ci_validation import compute_release_acceptance_matrix_sha256
from clio_relay.release_pin_sites import (
    PINSITES,
    PinFamily,
    PinSite,
    PinSiteError,
)
from clio_relay.release_pin_sites import (
    PinSiteDrifted as PinSiteDrifted,
)
from clio_relay.release_pin_sites import (
    SelectorKind as SelectorKind,
)

__all__ = [
    "PINSITES",
    "BumpTargets",
    "FamilyAgreement",
    "PinChange",
    "PinFamily",
    "PinSite",
    "PinSiteDrifted",
    "PinSiteError",
    "PreflightResult",
    "SelectorKind",
    "SiteReading",
    "UnregisteredHit",
    "apply_bump",
    "check_all_agreement",
    "plan_bump",
    "read_family_agreement",
    "read_site_value",
    "render_preflight",
    "run_preflight",
    "sites_in_group",
    "sweep_incompleteness",
    "sweep_jarvis_contract_v37_completeness",
    "value_groups",
    "write_site_value",
]


# --------------------------- Reading a site's current value ---------------


def _read_line_like(root: Path, site: PinSite) -> str:
    assert site.line is not None and site.pattern is not None
    file_path = root / site.path
    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise PinSiteDrifted(site, f"could not read file: {exc}") from exc
    if not (1 <= site.line <= len(lines)):
        raise PinSiteDrifted(site, f"file has {len(lines)} lines, no line {site.line}")
    text = lines[site.line - 1]
    matches = list(site.pattern.finditer(text))
    if not matches:
        raise PinSiteDrifted(site, f"pattern not found on line {site.line}: {text!r}")
    values = {match.group(1) for match in matches}
    if len(values) != 1:
        raise PinSiteDrifted(site, f"line {site.line} holds inconsistent values: {sorted(values)}")
    return values.pop()


def _read_json_key(root: Path, site: PinSite) -> str:
    assert site.key_path is not None
    file_path = root / site.path
    try:
        document = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PinSiteDrifted(site, f"could not read/parse JSON: {exc}") from exc
    cursor: object = document
    for part in site.key_path:
        if not isinstance(cursor, dict):
            raise PinSiteDrifted(site, f"key path {site.key_path} not found")
        mapping = cast("dict[str, object]", cursor)
        if part not in mapping:
            raise PinSiteDrifted(site, f"key path {site.key_path} not found")
        cursor = mapping[part]
    return str(cursor)


def _read_filename(root: Path, site: PinSite) -> str:
    assert site.filename_template is not None
    file_path = root / site.path
    prefix, _, suffix = site.filename_template.partition("{value}")
    name = file_path.name
    if not (name.startswith(prefix) and name.endswith(suffix)) or not file_path.is_file():
        raise PinSiteDrifted(
            site, f"filename {name!r} does not match template {site.filename_template!r}"
        )
    return name[len(prefix) : len(name) - len(suffix)]


def read_site_value(root: Path, site: PinSite) -> str:
    """Read a :class:`PinSite`'s current value from the tree at ``root``.

    Raises:
        PinSiteDrifted: The site no longer matches its recorded shape.
    """
    if site.kind is SelectorKind.PLACEHOLDER:
        assert site.line is not None and site.placeholder is not None
        lines = (root / site.path).read_text(encoding="utf-8").splitlines()
        if not (1 <= site.line <= len(lines)) or site.placeholder not in lines[site.line - 1]:
            raise PinSiteDrifted(site, f"placeholder {site.placeholder!r} not found")
        return f"<placeholder:{site.placeholder}>"
    if site.filename_template is not None and site.line is None:
        return _read_filename(root, site)
    if site.key_path is not None and site.path.endswith(".json"):
        return _read_json_key(root, site)
    return _read_line_like(root, site)


# --------------------------- Writing a new value (bump's rewrite primitive)


def _write_line_like(root: Path, site: PinSite, new_value: str) -> None:
    assert site.line is not None and site.pattern is not None
    file_path = root / site.path
    lines = file_path.read_text(encoding="utf-8").splitlines(keepends=True)
    text = lines[site.line - 1]
    pieces: list[str] = []
    cursor = 0
    for match in site.pattern.finditer(text):
        pieces.append(text[cursor : match.start(1)])
        pieces.append(new_value)
        cursor = match.end(1)
    pieces.append(text[cursor:])
    lines[site.line - 1] = "".join(pieces)
    file_path.write_text("".join(lines), encoding="utf-8")


def _write_json_key(root: Path, site: PinSite, new_value: str) -> None:
    assert site.key_path is not None
    file_path = root / site.path
    document = json.loads(file_path.read_text(encoding="utf-8"))
    cursor = document
    for part in site.key_path[:-1]:
        cursor = cursor[part]
    cursor[site.key_path[-1]] = new_value
    file_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def _write_filename(root: Path, site: PinSite, new_value: str) -> None:
    assert site.filename_template is not None
    file_path = root / site.path
    file_path.rename(file_path.with_name(site.filename_template.format(value=new_value)))


def write_site_value(root: Path, site: PinSite, new_value: str) -> None:
    """Rewrite a mutable :class:`PinSite` to ``new_value``.

    Raises:
        PinSiteError: ``site.mutable`` is ``False`` -- a frozen/placeholder
            site must never be rewritten.
    """
    if not site.mutable:
        raise PinSiteError(f"{site.id} is not mutable -- refusing to rewrite it")
    if site.filename_template is not None and site.line is None:
        _write_filename(root, site, new_value)
        return
    if site.key_path is not None and site.path.endswith(".json"):
        _write_json_key(root, site, new_value)
        return
    _write_line_like(root, site, new_value)


# --------------------------- Agreement: does one value_group agree? -------


@dataclass(frozen=True)
class SiteReading:
    """One site's current value, or the drift reason it could not be read."""

    site: PinSite
    value: str | None
    error: str | None = None


@dataclass(frozen=True)
class FamilyAgreement:
    """Whether every mutable site in a value_group currently agrees."""

    value_group: str
    readings: tuple[SiteReading, ...]

    @property
    def agrees(self) -> bool:
        values = {reading.value for reading in self.readings if reading.error is None}
        return not any(reading.error for reading in self.readings) and len(values) <= 1

    @property
    def consensus_value(self) -> str | None:
        values = {reading.value for reading in self.readings if reading.error is None}
        return next(iter(values)) if len(values) == 1 else None


def value_groups() -> tuple[str, ...]:
    """Every distinct ``value_group`` name across mutable, agreement-checked sites."""
    groups = {site.value_group for site in PINSITES if site.mutable and site.value_group}
    return tuple(sorted(groups))


def sites_in_group(value_group: str) -> tuple[PinSite, ...]:
    """Every mutable site registered under ``value_group``, in registry order."""
    return tuple(site for site in PINSITES if site.value_group == value_group)


def read_family_agreement(root: Path, value_group: str) -> FamilyAgreement:
    """Read every site in ``value_group`` and report whether they agree."""
    readings: list[SiteReading] = []
    for site in sites_in_group(value_group):
        try:
            readings.append(SiteReading(site, read_site_value(root, site)))
        except PinSiteDrifted as exc:
            readings.append(SiteReading(site, None, str(exc)))
    return FamilyAgreement(value_group, tuple(readings))


def check_all_agreement(root: Path) -> tuple[FamilyAgreement, ...]:
    """Read every value_group and report each one's agreement."""
    return tuple(read_family_agreement(root, group) for group in value_groups())


# --------------------------- Completeness: any unregistered pin site? -----

#: Sweep name -> (path, whole-file regex). Every registered ``REGEX``-kind
#: site names the sweep it participates in; :func:`sweep_incompleteness`
#: scans the whole file for the same shape and diffs the two line sets, so
#: a newly added block can never be silently missed (doc §7's
#: "repeated-structural-block coverage" requirement).
_SWEEPS: dict[str, tuple[str, re.Pattern[str]]] = {
    "release_gate_kit_text": (
        "docs/release-gate-1.0.yaml",
        re.compile(r'\bclio-kit: "[0-9][0-9.]*"|clio_kit-[0-9][0-9.]*-py3-none-any\.whl'),
    ),
    "release_gate_kit_digest": (
        "docs/release-gate-1.0.yaml",
        re.compile(r"\b\w*artifact_sha256: [0-9a-f]{64}"),
    ),
    "release_gate_contract_id": (
        "docs/release-gate-1.0.yaml",
        re.compile(r"contract_id: clio-kit-jarvis-user-v[0-9.]+"),
    ),
    "release_gate_contract_sha256": (
        "docs/release-gate-1.0.yaml",
        re.compile(r"^\s{16}contract_sha256: [0-9a-f]{64}", re.MULTILINE),
    ),
}
#: A digest that structurally matches ``release_gate_kit_digest``'s pattern
#: but pins the *unrelated* jarvis-cd artifact, not clio-kit -- named and
#: excluded rather than silently miscounted (jarvis-cd is not a #198 axis).
_JARVIS_CD_ARTIFACT_SHA256 = "2c2e2042d0256bd3d9c117d75aaf00d26d9e814fcbcca9a904abf06399fc1067"


@dataclass(frozen=True)
class UnregisteredHit:
    """A sweep found a pin-shaped value at a line not in the registry."""

    sweep: str
    path: str
    line: int
    text: str


def sweep_incompleteness(root: Path) -> tuple[UnregisteredHit, ...]:
    """Find every pin-shaped value in a swept file that is NOT registered.

    The opposite direction -- a registered site whose exact line no longer
    matches -- surfaces as a :class:`PinSiteDrifted` from
    :func:`read_family_agreement` instead, so both directions of drift are
    covered.
    """
    hits: list[UnregisteredHit] = []
    for sweep_name, (path, pattern) in _SWEEPS.items():
        registered = {
            site.line for site in PINSITES if site.sweep == sweep_name and site.line is not None
        }
        text = (root / path).read_text(encoding="utf-8")
        for line_number, line_text in enumerate(text.splitlines(), start=1):
            if pattern.search(line_text) is None:
                continue
            if sweep_name == "release_gate_kit_digest" and _JARVIS_CD_ARTIFACT_SHA256 in line_text:
                continue
            if line_number not in registered:
                hits.append(UnregisteredHit(sweep_name, path, line_number, line_text.strip()))
    hits.extend(sweep_jarvis_contract_v37_completeness(root))
    return tuple(hits)


#: The CURRENT canonical contract revision only. Sites pinning an OLDER
#: revision (v3.1-v3.6) are a deliberate, permanent multi-version
#: compatibility surface -- not #198's concern, and not swept here.
_CURRENT_CONTRACT = re.compile(r"(?:clio-kit-)?jarvis-user-v3\.7\b")
#: Files naming a v3.7 path or literal as registry metadata or test-fixture
#: sample data, not an unowned duplicate pin: this module and its
#: companion data table (their own docstrings/row data), and this sweep's
#: own test file (sabotage fixtures that legitimately embed sample text).
_SWEEP_EXCLUDED_FILES = frozenset(
    {
        "src/clio_relay/release_pins.py",
        "src/clio_relay/release_pin_sites.py",
        "tests/test_release_pins.py",
    }
)


def sweep_jarvis_contract_v37_completeness(root: Path) -> tuple[UnregisteredHit, ...]:
    """Every ``v3.7`` JARVIS contract literal must be a registered site.

    Scans ``src/clio_relay``, the vendored ``jarvis-packages/clio_relay``
    mirror, and ``tests/`` -- the same three trees #198's "13-copy" story
    was scattered across -- and names any file:line the registry misses.
    Restricted to the current revision (v3.7): legacy v3.1-v3.6 references
    are deliberate backward-compatibility surface, not a #198 pin.
    """
    registered = {
        (site.path, site.line)
        for site in PINSITES
        if site.family is PinFamily.JARVIS_CONTRACT and site.line is not None
    }
    hits: list[UnregisteredHit] = []
    for base in ("src/clio_relay", "jarvis-packages/clio_relay", "tests"):
        base_dir = root / base
        if not base_dir.is_dir():
            continue
        for file_path in sorted(base_dir.rglob("*")):
            if not file_path.is_file() or file_path.suffix not in {".py", ".json"}:
                continue
            rel = file_path.relative_to(root).as_posix()
            if rel in _SWEEP_EXCLUDED_FILES:
                continue
            text = file_path.read_text(encoding="utf-8", errors="strict")
            for line_number, line_text in enumerate(text.splitlines(), start=1):
                if _CURRENT_CONTRACT.search(line_text) is None:
                    continue
                if (rel, line_number) not in registered:
                    hits.append(
                        UnregisteredHit("jarvis_contract_v37", rel, line_number, line_text.strip())
                    )
    return tuple(hits)


# --------------------------- Preflight: the release-gate local check ------


@dataclass(frozen=True)
class PreflightResult:
    """The outcome of the release-identity preflight."""

    agreements: tuple[FamilyAgreement, ...]
    unregistered: tuple[UnregisteredHit, ...]

    @property
    def passed(self) -> bool:
        return all(agreement.agrees for agreement in self.agreements) and not self.unregistered


def run_preflight(root: Path) -> PreflightResult:
    """Run the full release-identity preflight against the tree at ``root``."""
    return PreflightResult(check_all_agreement(root), sweep_incompleteness(root))


def render_preflight(result: PreflightResult) -> list[str]:
    """Render a :class:`PreflightResult` as human-readable report lines."""
    lines: list[str] = []
    for agreement in result.agreements:
        if agreement.agrees:
            lines.append(f"OK: {agreement.value_group} ({len(agreement.readings)} sites agree)")
            continue
        lines.append(f"FAIL: {agreement.value_group} disagrees:")
        for reading in agreement.readings:
            if reading.error is not None:
                lines.append(f"  {reading.site.id}: {reading.error}")
            else:
                lines.append(
                    f"  {reading.site.id} ({reading.site.path}:{reading.site.line}): "
                    f"{reading.value}"
                )
    for hit in result.unregistered:
        lines.append(f"FAIL: unregistered {hit.sweep} pin at {hit.path}:{hit.line}: {hit.text}")
    if result.passed:
        checked = sum(len(agreement.readings) for agreement in result.agreements)
        lines.append(
            f"OK: release-identity preflight passed ({checked} sites, {len(PINSITES)} registered)"
        )
    return lines


# --------------------------- Bump: rewrite every mutable site for an axis -


@dataclass(frozen=True)
class BumpTargets:
    """New values for one or more release-identity axes.

    Each axis is independent -- pass only the ones you are bumping. A
    ``kit_version``/``contract_version`` bump that also needs to move a
    digest mirror requires the matching ``*_sha256`` argument; when it is
    omitted, that value_group is left unchanged (never silently dropped --
    ground rule 2, no silent fallback: :func:`plan_bump` still reports it,
    unset).
    """

    relay_version: str | None = None
    kit_version: str | None = None
    kit_wheel_sha256: str | None = None
    contract_version: str | None = None
    contract_sha256: str | None = None
    contract_wire_sha256: str | None = None
    contract_artifact_sha256: str | None = None


@dataclass(frozen=True)
class PinChange:
    """One site's planned or applied change."""

    site: PinSite
    old_value: str | None
    new_value: str | None
    applied: bool
    skipped_reason: str | None = None


def _target_for_group(value_group: str, targets: BumpTargets) -> str | None:
    return {
        "relay_version": targets.relay_version,
        "kit_version_text": targets.kit_version,
        "kit_wheel_sha256": targets.kit_wheel_sha256,
        "jarvis_contract_id": targets.contract_version,
        "jarvis_contract_sha256": targets.contract_sha256,
        "jarvis_contract_wire_sha256": targets.contract_wire_sha256,
        "jarvis_contract_artifact_sha256": targets.contract_artifact_sha256,
    }.get(value_group)


def plan_bump(root: Path, targets: BumpTargets) -> tuple[PinChange, ...]:
    """Compute the per-site diff a bump would apply, without writing anything."""
    changes: list[PinChange] = []
    for group in value_groups():
        new_value = _target_for_group(group, targets)
        for site in sites_in_group(group):
            try:
                old_value = read_site_value(root, site)
            except PinSiteDrifted as exc:
                changes.append(PinChange(site, None, new_value, False, str(exc)))
                continue
            if new_value is not None and old_value != new_value:
                changes.append(PinChange(site, old_value, new_value, False))
    if targets.relay_version is not None:
        digest_change = _matrix_digest_change(root, targets.relay_version)
        if digest_change is not None:
            changes.append(digest_change)
    return tuple(changes)


def _matrix_digest_change(root: Path, new_relay_version: str) -> PinChange | None:
    matrix_site = next(site for site in PINSITES if site.id == "matrix.canonical_digest")
    document = json.loads((root / matrix_site.path).read_text(encoding="utf-8"))
    old_digest = str(document.get("matrix_sha256", ""))
    document["release_version"] = new_relay_version
    canonical = {key: value for key, value in document.items() if key != "matrix_sha256"}
    new_digest = compute_release_acceptance_matrix_sha256(canonical)
    if old_digest == new_digest:
        return None
    return PinChange(matrix_site, old_digest, new_digest, False)


def apply_bump(root: Path, targets: BumpTargets) -> tuple[PinChange, ...]:
    """Rewrite every registered mutable site named by ``targets``.

    The two ``matrix_digest`` sites are recomputed and written strictly
    after every other site is rewritten (doc §7's ordering rule for derived
    digests) via
    :func:`clio_relay.ci_validation.compute_release_acceptance_matrix_sha256`
    over the already-bumped acceptance matrix, never independently.
    """
    applied: list[PinChange] = []
    for group in value_groups():
        new_value = _target_for_group(group, targets)
        if new_value is None:
            continue
        for site in sites_in_group(group):
            try:
                old_value = read_site_value(root, site)
            except PinSiteDrifted as exc:
                applied.append(PinChange(site, None, new_value, False, str(exc)))
                continue
            if old_value != new_value:
                write_site_value(root, site, new_value)
                applied.append(PinChange(site, old_value, new_value, True))
    if targets.relay_version is not None:
        digest_change = _apply_matrix_digest(root)
        if digest_change is not None:
            applied.append(digest_change)
    return tuple(applied)


def _apply_matrix_digest(root: Path) -> PinChange | None:
    matrix_site = next(site for site in PINSITES if site.id == "matrix.canonical_digest")
    mirror_site = next(site for site in PINSITES if site.id == "matrix.release_gate_mirror")
    document = json.loads((root / matrix_site.path).read_text(encoding="utf-8"))
    old_digest = str(document.get("matrix_sha256", ""))
    canonical = {key: value for key, value in document.items() if key != "matrix_sha256"}
    new_digest = compute_release_acceptance_matrix_sha256(canonical)
    if old_digest == new_digest:
        return None
    _write_json_key(root, matrix_site, new_digest)
    _write_line_like(root, mirror_site, new_digest)
    return PinChange(matrix_site, old_digest, new_digest, True)
