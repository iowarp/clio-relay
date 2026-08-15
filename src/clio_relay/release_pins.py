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
from clio_relay.release_pin_validation import (
    PinSiteShapeError as PinSiteShapeError,
)
from clio_relay.release_pin_validation import (
    UnregisteredHit as UnregisteredHit,
)
from clio_relay.release_pin_validation import (
    sweep_structural_fixture_blocks,
    validate_registry,
)
from clio_relay.release_pin_validation import (
    validate_registry_sites as validate_registry_sites,
)

validate_registry()

__all__ = [
    "PINSITES",
    "BumpTargets",
    "CrossAxisNote",
    "FamilyAgreement",
    "PinChange",
    "PinFamily",
    "PinSite",
    "PinSiteDrifted",
    "PinSiteError",
    "PinSiteShapeError",
    "PreflightResult",
    "SelectorKind",
    "SiteReading",
    "UnregisteredHit",
    "apply_bump",
    "check_all_agreement",
    "frozen_sites",
    "plan_bump",
    "read_family_agreement",
    "read_frozen_sites",
    "read_site_value",
    "render_preflight",
    "resolve_site_path",
    "run_preflight",
    "sites_in_group",
    "sweep_incompleteness",
    "sweep_jarvis_contract_v37_completeness",
    "validate_registry",
    "validate_registry_sites",
    "value_groups",
    "write_site_value",
]


# --------------------------- Reading a site's current value ---------------


def _is_rename_site(site: PinSite) -> bool:
    """A ``FILENAME`` site whose own path (not a referencing line) embeds the value."""
    return site.filename_template is not None and site.line is None


def _anchor_value(root: Path, site: PinSite) -> str:
    """Resolve ``site``'s dynamic path via a reliable, never-renamed sibling.

    A ``dynamic_path`` site's own ``path`` is a ``{value}`` template, not a
    literal -- "the version IS the path" (doc §7). The anchor is the first
    non-``dynamic_path`` site in ``site.path_group`` (or ``site.value_group``
    when ``path_group`` is unset -- the common case, where a site's own
    value IS the path-determining one): by construction the anchor is a
    stable source line (e.g. ``jc.jarvis_mcp_contract_id``), never itself
    renamed, so it is always safe to read regardless of where the
    dynamic-path site's own file currently sits. A digest embedded in the
    same file needs the *id* group's anchor, not its own digest group's --
    that is exactly what ``path_group`` overrides.
    """
    group = site.path_group or site.value_group
    assert group is not None
    anchor = next(candidate for candidate in sites_in_group(group) if not candidate.dynamic_path)
    return read_site_value(root, anchor)


def resolve_site_path(root: Path, site: PinSite, *, at_value: str | None = None) -> Path:
    """Resolve the real file ``site`` targets.

    ``at_value`` pins the dynamic-path resolution to a specific value (the
    value already captured during a bump's read phase) rather than
    re-reading the anchor -- required at write time, since sibling sites in
    the same group may already have been rewritten by the time a
    dynamic-path site is written (:func:`_group_changes`'s ordering).
    """
    if not site.dynamic_path:
        return root / site.path
    resolved = at_value if at_value is not None else _anchor_value(root, site)
    return root / site.path.format(value=resolved)


def _read_line_like(root: Path, site: PinSite) -> str:
    assert site.pattern is not None
    file_path = resolve_site_path(root, site)
    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise PinSiteDrifted(site, f"could not read file: {exc}") from exc
    if site.line is not None:
        if not (1 <= site.line <= len(lines)):
            raise PinSiteDrifted(site, f"file has {len(lines)} lines, no line {site.line}")
        candidates = [lines[site.line - 1]]
        locator = f"line {site.line}"
    else:
        # No fixed line: the file is wholesale-regenerated on a real bump
        # (the vendored contract JSON's own embedded digests), so search
        # the whole file for the pattern instead of trusting a line number
        # that shifts with the file's content.
        candidates = lines
        locator = "the file"
    values: set[str] = set()
    for text in candidates:
        values.update(match.group(1) for match in site.pattern.finditer(text))
    if not values:
        raise PinSiteDrifted(site, f"pattern not found in {locator}")
    if len(values) != 1:
        raise PinSiteDrifted(site, f"{locator} holds inconsistent values: {sorted(values)}")
    return values.pop()


def _read_json_key(root: Path, site: PinSite) -> str:
    assert site.key_path is not None
    file_path = resolve_site_path(root, site)
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
    anchor_value = _anchor_value(root, site) if site.dynamic_path else None
    file_path = resolve_site_path(root, site, at_value=anchor_value)
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
    if _is_rename_site(site):
        return _read_filename(root, site)
    if site.key_path is not None and site.path.endswith(".json"):
        return _read_json_key(root, site)
    return _read_line_like(root, site)


# --------------------------- Writing a new value (bump's rewrite primitive)


def _own_value_is_path_value(site: PinSite) -> bool:
    """Does this site's own value (not another site's) determine its path?

    True for the rename/id-content sites (``path_group`` unset -- their own
    value literally IS the contract revision the path embeds). False for a
    digest embedded in the same file (``path_group="jarvis_contract_id"``,
    ``value_group="jarvis_contract_sha256"``/etc.): its own ``old_value`` is
    a digest, useless for path resolution, so path resolution must always
    re-read the id group's anchor fresh rather than trust a passed-in value.
    """
    return site.dynamic_path and site.path_group is None


def _write_line_like(root: Path, site: PinSite, new_value: str, *, old_value: str) -> None:
    assert site.pattern is not None
    at_value = old_value if _own_value_is_path_value(site) else None
    file_path = resolve_site_path(root, site, at_value=at_value)
    lines = file_path.read_text(encoding="utf-8").splitlines(keepends=True)
    line_numbers = (
        [site.line] if site.line is not None else [index + 1 for index in range(len(lines))]
    )
    for line_number in line_numbers:
        text = lines[line_number - 1]
        pieces: list[str] = []
        cursor = 0
        changed = False
        for match in site.pattern.finditer(text):
            pieces.append(text[cursor : match.start(1)])
            pieces.append(new_value)
            cursor = match.end(1)
            changed = True
        if not changed:
            continue
        pieces.append(text[cursor:])
        lines[line_number - 1] = "".join(pieces)
    file_path.write_text("".join(lines), encoding="utf-8")


def _write_json_key(root: Path, site: PinSite, new_value: str) -> None:
    assert site.key_path is not None
    file_path = resolve_site_path(root, site)
    document = json.loads(file_path.read_text(encoding="utf-8"))
    cursor = document
    for part in site.key_path[:-1]:
        cursor = cursor[part]
    cursor[site.key_path[-1]] = new_value
    file_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def _write_filename(root: Path, site: PinSite, new_value: str, *, old_value: str) -> None:
    assert site.filename_template is not None
    at_value = old_value if _own_value_is_path_value(site) else None
    file_path = resolve_site_path(root, site, at_value=at_value)
    file_path.rename(file_path.with_name(site.filename_template.format(value=new_value)))


def write_site_value(root: Path, site: PinSite, new_value: str, *, old_value: str) -> None:
    """Rewrite a mutable :class:`PinSite` to ``new_value``.

    ``old_value`` is the value already read for this site during the bump's
    read-all-first phase (:func:`_group_changes`) -- required (not
    re-derived) for a ``dynamic_path`` site, since a sibling anchor in the
    same group may already have been rewritten by the time this call
    happens, which would resolve the wrong (already-new) path.

    Raises:
        PinSiteError: ``site.mutable`` is ``False`` -- a frozen/placeholder
            site must never be rewritten.
    """
    if not site.mutable:
        raise PinSiteError(f"{site.id} is not mutable -- refusing to rewrite it")
    if _is_rename_site(site):
        _write_filename(root, site, new_value, old_value=old_value)
        return
    if site.key_path is not None and site.path.endswith(".json"):
        _write_json_key(root, site, new_value)
        return
    _write_line_like(root, site, new_value, old_value=old_value)


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


def frozen_sites() -> tuple[PinSite, ...]:
    """Every registered site that is tracked but never rewritten or agreement-checked."""
    return tuple(site for site in PINSITES if not site.mutable)


def read_frozen_sites(root: Path) -> tuple[SiteReading, ...]:
    """Read every frozen site's current value now -- reported, never failing.

    "Registered" must never be decorative (doc §7.9, B5): a frozen site is
    excluded from agreement-checking on purpose (a stable historical label
    or a placeholder, never rewritten), but it is still read every
    preflight run, so a site whose own pattern stops matching at all (a
    genuinely broken reference, not merely "says an old version on
    purpose") is at least visible instead of silently unverified forever.
    """
    readings: list[SiteReading] = []
    for site in frozen_sites():
        try:
            readings.append(SiteReading(site, read_site_value(root, site)))
        except PinSiteDrifted as exc:
            readings.append(SiteReading(site, None, str(exc)))
    return tuple(readings)


# --------------------------- Completeness: any unregistered pin site? -----


def sweep_incompleteness(root: Path) -> tuple[UnregisteredHit, ...]:
    """Every pin-shaped value in the tree that is NOT a registered site.

    Combines the two completeness sweeps: the structural release-gate
    fixture blocks (``release_pin_validation.sweep_structural_fixture_
    blocks``, no dynamic-path dependency) and the v3.7 contract-literal
    sweep below (needs :func:`resolve_site_path`, which is why it lives
    here rather than alongside the other one -- a circular import).
    """
    return sweep_structural_fixture_blocks(root) + sweep_jarvis_contract_v37_completeness(root)


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


#: (directory, recursive) pairs scanned for the v3.7-literal sweep.
#: ``docs``/``docs/ai`` are shallow (not recursive): that deliberately
#: excludes docs/design/relay-architecture-2026-08.md, which discusses the
#: v3.7 pin extensively as prose *about* the registry (this section's own
#: audit language, quoted example values) -- not a duplicate pin, and
#: sweeping it recursively would manufacture false positives against the
#: document that specifies this very sweep (doc §7.9, B3).
_SWEEP_BASES: tuple[tuple[str, bool], ...] = (
    ("src/clio_relay", True),
    ("jarvis-packages/clio_relay", True),
    ("tests", True),
    ("docs", False),
    ("docs/ai", False),
)
_SWEEP_SUFFIXES = frozenset({".py", ".json", ".md"})


def sweep_jarvis_contract_v37_completeness(root: Path) -> tuple[UnregisteredHit, ...]:
    """Every ``v3.7`` JARVIS contract literal must be a registered site.

    Scans ``src/clio_relay``, the vendored ``jarvis-packages/clio_relay``
    mirror, ``tests/`` -- the same three trees #198's "13-copy" story was
    scattered across -- and the top-level Markdown under ``docs/`` and
    ``docs/ai/`` (doc §7.9, B3: prose describing the staging gate drifted
    stale independently of the code sites), naming any file:line the
    registry misses. Restricted to the current revision (v3.7): legacy
    v3.1-v3.6 references are deliberate backward-compatibility surface,
    not a #198 pin.
    """
    # A dynamic_path site's own `.path` is a `{value}` template, not the
    # real relative path any file scan will ever report -- resolve it
    # against the real tree first, or every dynamic_path site's line
    # permanently misses (never matches a real scanned `rel`). Resolution
    # itself reads an anchor site elsewhere in the tree (e.g.
    # cluster_config.py); on a partial/synthetic tree (a test fixture that
    # only sets up the one file it cares about) that anchor may not exist --
    # skip the entry rather than crashing the whole sweep for every other,
    # perfectly resolvable site.
    registered: set[tuple[str, int]] = set()
    for site in PINSITES:
        if site.family is not PinFamily.JARVIS_CONTRACT or site.line is None:
            continue
        if not site.dynamic_path:
            registered.add((site.path, site.line))
            continue
        try:
            resolved = resolve_site_path(root, site).relative_to(root).as_posix()
        except PinSiteDrifted:
            continue
        registered.add((resolved, site.line))
    hits: list[UnregisteredHit] = []
    for base, recursive in _SWEEP_BASES:
        base_dir = root / base
        if not base_dir.is_dir():
            continue
        candidates = base_dir.rglob("*") if recursive else base_dir.glob("*")
        for file_path in sorted(candidates):
            if not file_path.is_file() or file_path.suffix not in _SWEEP_SUFFIXES:
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

#: Prefixes marking two value_groups as the SAME underlying concern
#: pinned on two deliberately independent axes (clio-relay #190/#199,
#: 41b912c/eef50b5): a bootstrap default and an ares acceptance-policy
#: fixture that records what a past live run actually had installed.
#: Diverging is allowed by design -- reported as :class:`CrossAxisNote`,
#: never a preflight failure.
_CROSS_AXIS_PREFIXES: tuple[str, ...] = ("bootstrap_", "acceptance_")


@dataclass(frozen=True)
class CrossAxisNote:
    """Two independent value_groups for the same concern currently differ.

    Informational only -- see :data:`_CROSS_AXIS_PREFIXES`.
    """

    group_a: str
    value_a: str | None
    group_b: str
    value_b: str | None


def _cross_axis_notes(agreements: tuple[FamilyAgreement, ...]) -> tuple[CrossAxisNote, ...]:
    consensus = {agreement.value_group: agreement.consensus_value for agreement in agreements}
    notes: list[CrossAxisNote] = []
    seen: set[frozenset[str]] = set()
    for prefix in _CROSS_AXIS_PREFIXES:
        for group in consensus:
            if not group.startswith(prefix):
                continue
            suffix = group[len(prefix) :]
            for other_prefix in _CROSS_AXIS_PREFIXES:
                other_group = f"{other_prefix}{suffix}"
                if other_prefix == prefix or other_group not in consensus:
                    continue
                pair = frozenset({group, other_group})
                if pair in seen:
                    continue
                seen.add(pair)
                value, other_value = consensus[group], consensus[other_group]
                if value is not None and other_value is not None and value != other_value:
                    notes.append(CrossAxisNote(group, value, other_group, other_value))
    return tuple(notes)


@dataclass(frozen=True)
class PreflightResult:
    """The outcome of the release-identity preflight."""

    agreements: tuple[FamilyAgreement, ...]
    unregistered: tuple[UnregisteredHit, ...]
    cross_axis: tuple[CrossAxisNote, ...] = ()
    frozen: tuple[SiteReading, ...] = ()

    @property
    def passed(self) -> bool:
        return all(agreement.agrees for agreement in self.agreements) and not self.unregistered


def run_preflight(root: Path) -> PreflightResult:
    """Run the full release-identity preflight against the tree at ``root``."""
    agreements = check_all_agreement(root)
    return PreflightResult(
        agreements,
        sweep_incompleteness(root),
        _cross_axis_notes(agreements),
        read_frozen_sites(root),
    )


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
    for note in result.cross_axis:
        lines.append(
            f"INFO: {note.group_a}={note.value_a!r} differs from "
            f"{note.group_b}={note.value_b!r} (independent axes by design, not a failure)"
        )
    for reading in result.frozen:
        if reading.error is not None:
            lines.append(
                f"INFO: frozen site {reading.site.id} did not read cleanly: {reading.error}"
            )
        else:
            lines.append(f"INFO: frozen site {reading.site.id} currently reads {reading.value!r}")
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
    ``*_kit_version``/``contract_version`` bump that also needs to move a
    digest mirror requires the matching ``*_sha256`` argument; when it is
    omitted, that value_group is left unchanged (never silently dropped --
    ground rule 2, no silent fallback: :func:`plan_bump` still reports it,
    unset).

    ``bootstrap_kit_version``/``bootstrap_kit_wheel_sha256`` and
    ``acceptance_kit_version``/``acceptance_kit_wheel_sha256`` are
    deliberately separate axes, not two names for one value (clio-relay
    #190/#199, 41b912c/eef50b5): the acceptance-policy pin
    (``docs/release-gate-1.0.yaml``) records what a past live ares run
    actually had installed, independent of whatever the current bootstrap
    default is. Bumping one must never silently move the other.
    """

    relay_version: str | None = None
    bootstrap_kit_version: str | None = None
    bootstrap_kit_wheel_sha256: str | None = None
    acceptance_kit_version: str | None = None
    acceptance_kit_wheel_sha256: str | None = None
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
        "bootstrap_kit_version_text": targets.bootstrap_kit_version,
        "bootstrap_kit_wheel_sha256": targets.bootstrap_kit_wheel_sha256,
        "acceptance_kit_version_text": targets.acceptance_kit_version,
        "acceptance_kit_wheel_sha256": targets.acceptance_kit_wheel_sha256,
        "jarvis_contract_id": targets.contract_version,
        "jarvis_contract_sha256": targets.contract_sha256,
        "jarvis_contract_wire_sha256": targets.contract_wire_sha256,
        "jarvis_contract_artifact_sha256": targets.contract_artifact_sha256,
    }.get(value_group)


def _group_changes(root: Path, group: str, new_value: str, *, write: bool) -> list[PinChange]:
    """Compute (and optionally apply) one value_group's changes, atomically.

    Every site in ``group`` is read *before* any of them is written, both so
    a rename earlier in registry order can never make a later sibling's read
    of the pre-rename path fail (the exact bug this ordering prevented
    B1 from reproducing), and so one drifted site blocks the *whole* group
    rather than leaving it partially rewritten -- ``write=True`` only ever
    touches disk once every site in the group has read cleanly.
    """
    sites = sites_in_group(group)
    readings: list[tuple[PinSite, str | None, str | None]] = []
    for site in sites:
        try:
            readings.append((site, read_site_value(root, site), None))
        except PinSiteDrifted as exc:
            readings.append((site, None, str(exc)))
    drifted = [reading for reading in readings if reading[2] is not None]
    if drifted:
        drifted_ids = sorted(reading[0].id for reading in drifted)
        blocked_reason = f"blocked: sibling site(s) drifted in the same value_group: {drifted_ids}"
        return [
            PinChange(
                site, old_value, new_value, False, error if error is not None else blocked_reason
            )
            for site, old_value, error in readings
        ]
    # Every site in the group read cleanly. Renames are applied last so a
    # sibling KEY/LINE site targeting the same (pre-rename) path always
    # finds it, even though every read already happened above.
    ordered = sorted(readings, key=lambda reading: _is_rename_site(reading[0]))
    changes: list[PinChange] = []
    for site, old_value, _ in ordered:
        assert old_value is not None, "no drift was detected above, so every read succeeded"
        if old_value == new_value:
            continue
        if write:
            write_site_value(root, site, new_value, old_value=old_value)
        changes.append(PinChange(site, old_value, new_value, write))
    return changes


def plan_bump(root: Path, targets: BumpTargets) -> tuple[PinChange, ...]:
    """Compute the per-site diff a bump would apply, without writing anything."""
    changes: list[PinChange] = []
    for group in value_groups():
        new_value = _target_for_group(group, targets)
        if new_value is not None:
            changes.extend(_group_changes(root, group, new_value, write=False))
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

    Each value_group is all-or-nothing: every site in it is read before any
    of them is written, so one drifted sibling blocks writes to the whole
    group instead of leaving it partially rewritten (:func:`_group_changes`).
    The two ``matrix_digest`` sites are recomputed and written strictly
    after every other site is rewritten (doc §7's ordering rule for derived
    digests) via
    :func:`clio_relay.ci_validation.compute_release_acceptance_matrix_sha256`
    over the already-bumped acceptance matrix, never independently.
    """
    applied: list[PinChange] = []
    for group in value_groups():
        new_value = _target_for_group(group, targets)
        if new_value is not None:
            applied.extend(_group_changes(root, group, new_value, write=True))
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
    _write_line_like(root, mirror_site, new_digest, old_value=old_digest)
    return PinChange(matrix_site, old_digest, new_digest, True)
