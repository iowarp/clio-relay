"""Registry shape validation and the structural-fixture completeness sweep.

Split from :mod:`clio_relay.release_pins` (ground rule 6, the same
reasoning that split ``release_pin_sites.py``'s data table out): the logic
module was over the 800-line cap once B1's atomicity rewrite, B6's axis
split, B9's shape validator, and B3's completeness-sweep extension all
landed in the same file. Both concerns here are self-contained: neither
needs ``release_pins.py``'s ``resolve_site_path`` (the dynamic-path v3.7
sweep does, so it stays in ``release_pins.py`` -- moving it here would
create a circular import).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from clio_relay.release_pin_sites import PINSITES, PinSite, PinSiteError, SelectorKind


class PinSiteShapeError(PinSiteError):
    """A registered site's ``kind`` does not match its actual read/write shape.

    Raised at import time -- never at read/preflight time, when it would be
    too late -- so a mislabeled site is caught immediately rather than
    silently dispatching through whichever code path its actual field
    combination happens to match (the exact bug this check exists to catch:
    ``jc.contract_file_content`` was labeled ``KEY`` but had no
    ``key_path``, so it silently fell through to the line-pattern path
    the read/write dispatch functions actually use).
    """


def dispatch_shape(site: PinSite) -> str:
    """Which of the four real read/write code paths ``site`` takes.

    Mirrors ``release_pins``'s ``read_site_value``/``write_site_value``
    dispatch order exactly -- this is what "honest" means here: the
    validator checks the SAME shape the dispatch functions actually use,
    not a second, independently-maintained description of it.
    """
    if site.placeholder is not None:
        return "placeholder"
    if site.filename_template is not None and site.line is None:
        return "rename"
    if site.key_path is not None and site.path.endswith(".json"):
        return "json_key"
    if site.pattern is not None:
        return "pattern"
    return "unknown"


#: Which SelectorKind values are honest labels for each dispatch shape.
#: "pattern" covers every kind that CAN be pattern-matched: LINE/REGEX
#: (source code, fixture blocks), FILENAME (a line referencing a filename
#: string), and KEY/DERIVED_DIGEST when the target is YAML (a line-anchored
#: scalar, deliberately not round-tripped through a full parse -- see
#: PinSite.dynamic_path's docstring). A JSON file is never allowed to take
#: the "pattern" path under KEY/DERIVED_DIGEST -- see the explicit check
#: below, the one rule that would have caught the bug above.
_KINDS_FOR_SHAPE: dict[str, frozenset[SelectorKind]] = {
    "placeholder": frozenset({SelectorKind.PLACEHOLDER}),
    "rename": frozenset({SelectorKind.FILENAME}),
    "json_key": frozenset({SelectorKind.KEY, SelectorKind.DERIVED_DIGEST}),
    "pattern": frozenset(
        {
            SelectorKind.LINE,
            SelectorKind.REGEX,
            SelectorKind.FILENAME,
            SelectorKind.KEY,
            SelectorKind.DERIVED_DIGEST,
        }
    ),
}


def _validate_site_shape(site: PinSite) -> None:
    shape = dispatch_shape(site)
    if shape == "unknown":
        raise PinSiteShapeError(
            f"{site.id}: no recognized read shape -- needs a pattern, key_path, "
            "filename_template, or placeholder"
        )
    if site.kind not in _KINDS_FOR_SHAPE[shape]:
        raise PinSiteShapeError(
            f"{site.id}: kind={site.kind.value!r} does not match its actual dispatch shape "
            f"{shape!r}"
        )
    if (
        site.kind in (SelectorKind.KEY, SelectorKind.DERIVED_DIGEST)
        and site.path.endswith(".json")
        and shape != "json_key"
    ):
        raise PinSiteShapeError(
            f"{site.id}: {site.kind.value} kind on a JSON file must use key_path (real "
            "structured parsing), not a line pattern"
        )
    if site.kind is SelectorKind.REGEX and site.sweep is None:
        raise PinSiteShapeError(f"{site.id}: REGEX kind requires a completeness sweep")
    if site.dynamic_path and site.value_group is None:
        raise PinSiteShapeError(
            f"{site.id}: dynamic_path requires a value_group to resolve an anchor from"
        )
    if site.path_group is not None and not site.dynamic_path:
        raise PinSiteShapeError(f"{site.id}: path_group is only meaningful for dynamic_path sites")


def validate_registry_sites(sites: tuple[PinSite, ...]) -> None:
    """Validate an arbitrary set of sites -- the real registry, or a test fixture."""
    for site in sites:
        _validate_site_shape(site)


def validate_registry() -> None:
    """Validate every registered site's ``kind`` against its real shape.

    Called at :mod:`clio_relay.release_pins` import time -- a mislabeled
    site fails loudly the moment that module loads, not silently at some
    later read.
    """
    validate_registry_sites(PINSITES)


# --------------------------- Completeness: structural fixture blocks ------


@dataclass(frozen=True)
class UnregisteredHit:
    """A sweep found a pin-shaped value at a line not in the registry."""

    sweep: str
    path: str
    line: int
    text: str


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


def sweep_structural_fixture_blocks(root: Path) -> tuple[UnregisteredHit, ...]:
    """Find every pin-shaped value in ``docs/release-gate-1.0.yaml`` that is NOT registered.

    Covers only the ``_SWEEPS`` structural-block patterns -- the dynamic-
    path-aware v3.7 contract-literal sweep
    (``release_pins.sweep_jarvis_contract_v37_completeness``) lives
    separately (it needs ``resolve_site_path``, which would create a
    circular import here); ``release_pins.sweep_incompleteness`` combines
    both. The opposite direction -- a registered site whose exact line no
    longer matches -- surfaces as a ``PinSiteDrifted`` from
    ``release_pins.read_family_agreement`` instead, so both directions of
    drift are covered.
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
    return tuple(hits)
