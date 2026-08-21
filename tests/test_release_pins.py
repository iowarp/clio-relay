"""Tests for the release-identity pin registry (iowarp/clio-relay#198, #231 R7)."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from clio_relay import ci_validation
from clio_relay.release_pins import (
    PINSITES,
    BumpTargets,
    PinSite,
    PinSiteDrifted,
    PinSiteError,
    PinSiteShapeError,
    SelectorKind,
    apply_bump,
    frozen_sites,
    plan_bump,
    read_frozen_sites,
    read_site_value,
    render_preflight,
    resolve_site_path,
    run_preflight,
    sites_in_group,
    sweep_incompleteness,
    sweep_jarvis_contract_v37_completeness,
    validate_registry,
    validate_registry_sites,
    value_groups,
    write_site_value,
)

ROOT = Path(__file__).parents[1]

#: Every file PINSITES references, relative to the repo root -- exactly the
#: files a mirrored test tree needs to exercise agreement/read/write logic.
#: A dynamic_path site's own `.path` is a `{value}` template, not the real
#: path (resolve_site_path resolves it against the real tree).
_REGISTERED_FILES: tuple[str, ...] = tuple(
    sorted({resolve_site_path(ROOT, site).relative_to(ROOT).as_posix() for site in PINSITES})
)


def _mirror_registered_tree(destination: Path) -> Path:
    """Copy every file PINSITES references into ``destination``, same layout."""
    for relative in _REGISTERED_FILES:
        source = ROOT / relative
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return destination


@pytest.fixture
def mirrored_root(tmp_path: Path) -> Path:
    """A tmp tree byte-identical, file-by-file, to every registered pin site."""
    return _mirror_registered_tree(tmp_path)


# --------------------------- Registry completeness against the real tree --


def test_every_pin_site_currently_agrees() -> None:
    """Every registered value_group agrees, and no site pins are unregistered."""
    result = run_preflight(ROOT)
    report = "\n".join(render_preflight(result))
    assert result.passed, report


def test_bootstrap_and_acceptance_kit_axes_may_diverge_without_failing(mirrored_root: Path) -> None:
    """clio-relay#190/#199 (41b912c/eef50b5): these are independent axes by design.

    The real tree currently has them diverged (bootstrap 2.7.2, ares
    acceptance-policy fixture still 2.6.6 -- no live re-certification
    evidence exists yet for 2.7.2 on ares). The preflight must still pass,
    and must say so via an informational note, never a failure.
    """
    result = run_preflight(mirrored_root)

    assert result.passed
    bootstrap = next(a for a in result.agreements if a.value_group == "bootstrap_kit_version_text")
    acceptance = next(
        a for a in result.agreements if a.value_group == "acceptance_kit_version_text"
    )
    assert bootstrap.agrees and acceptance.agrees
    assert bootstrap.consensus_value != acceptance.consensus_value
    matching_notes = [
        note
        for note in result.cross_axis
        if {note.group_a, note.group_b}
        == {"bootstrap_kit_version_text", "acceptance_kit_version_text"}
    ]
    assert len(matching_notes) == 1
    assert matching_notes[0].value_a == bootstrap.consensus_value
    assert matching_notes[0].value_b == acceptance.consensus_value


def test_bumping_the_bootstrap_kit_axis_never_touches_the_acceptance_axis(
    mirrored_root: Path,
) -> None:
    """A --kit-version bump must not silently drag the acceptance-policy pin along."""
    acceptance_before = read_site_value(
        mirrored_root, next(site for site in PINSITES if site.id == "kv.release_gate_text_115")
    )

    apply_bump(
        mirrored_root,
        BumpTargets(bootstrap_kit_version="9.9.9", bootstrap_kit_wheel_sha256="c" * 64),
    )

    acceptance_after = read_site_value(
        mirrored_root, next(site for site in PINSITES if site.id == "kv.release_gate_text_115")
    )
    assert acceptance_after == acceptance_before


def test_value_groups_cover_every_release_identity_axis() -> None:
    groups = value_groups()
    assert groups == (
        "acceptance_kit_version_text",
        "acceptance_kit_wheel_sha256",
        "bootstrap_kit_version_text",
        "bootstrap_kit_wheel_sha256",
        "jarvis_contract_artifact_sha256",
        "jarvis_contract_id",
        "jarvis_contract_sha256",
        "jarvis_contract_wire_sha256",
        "matrix_digest",
        "relay_version",
    )


def test_registry_has_no_duplicate_site_ids() -> None:
    ids = [site.id for site in PINSITES]
    duplicates = sorted({site_id for site_id in ids if ids.count(site_id) > 1})
    assert len(ids) == len(set(ids)), f"duplicate PinSite ids: {duplicates}"


def test_the_real_registry_passes_shape_validation() -> None:
    """validate_registry() already ran at import time; re-running proves it's wired in."""
    validate_registry()  # must not raise


def test_jarvis_contract_id_sites_round_trip_a_three_part_patch_version(
    mirrored_root: Path,
) -> None:
    """clio-relay's own contract-versioning doctrine is patch-level (vX.Y.Z for
    a small additive change), but the shared jarvis-contract pin pattern's
    capturing group was authored assuming every revision is vX.Y -- it silently
    TRUNCATES a 3-part value on read instead of raising or round-tripping it.
    A real v3.7 -> v3.7.1 mint would corrupt every pin site's read value to
    'v3.7' the moment it was written, which would then make every site
    FALSELY agree with an actually-unbumped sibling still literally holding
    'v3.7' -- the exact silent-drift failure #198 exists to prevent. Regression
    for teaching the pin regex 3-part versions before any v3.7.1 mint.
    """
    site = next(s for s in PINSITES if s.id == "jc.jarvis_mcp_contract_id")
    original = read_site_value(mirrored_root, site)

    write_site_value(mirrored_root, site, "v3.7.1", old_value=original)
    reread = read_site_value(mirrored_root, site)

    assert reread == "v3.7.1", (
        f"contract-pin regex truncated a 3-part patch version: read back {reread!r} "
        "instead of 'v3.7.1' -- its capturing group only understands vX.Y"
    )


def test_a_key_kind_site_on_a_json_file_without_key_path_is_rejected() -> None:
    """B9: the exact bug this validator exists to catch (jc.contract_file_content

    was once labeled KEY with no key_path, silently dispatching through the
    line-pattern path instead).
    """
    mislabeled = replace(
        next(site for site in PINSITES if site.id == "jc.contract_file_content"),
        id="test.mislabeled",
        kind=SelectorKind.KEY,
    )
    with pytest.raises(PinSiteShapeError, match="key_path"):
        validate_registry_sites((mislabeled,))


def test_a_regex_kind_site_without_a_sweep_is_rejected() -> None:
    mislabeled = replace(
        next(site for site in PINSITES if site.id == "kv.release_gate_text_115"),
        id="test.mislabeled",
        sweep=None,
    )
    with pytest.raises(PinSiteShapeError, match="completeness sweep"):
        validate_registry_sites((mislabeled,))


def test_a_dynamic_path_site_without_a_value_group_is_rejected() -> None:
    mislabeled = replace(
        next(site for site in PINSITES if site.id == "jc.contract_file_rename"),
        id="test.mislabeled",
        value_group=None,
    )
    with pytest.raises(PinSiteShapeError, match="value_group"):
        validate_registry_sites((mislabeled,))


def test_v37_contract_family_has_no_unregistered_site_in_the_real_tree() -> None:
    """Every ``v3.7`` literal in src/clio_relay, jarvis-packages, and tests is registered."""
    hits = [hit for hit in sweep_incompleteness(ROOT) if hit.sweep == "jarvis_contract_v37"]
    assert hits == []


def test_release_gate_fixture_sweeps_find_no_unregistered_site_in_the_real_tree() -> None:
    """Every clio-kit/jarvis-contract pin-shaped value in release-gate-1.0.yaml is registered."""
    hits = [hit for hit in sweep_incompleteness(ROOT) if hit.sweep != "jarvis_contract_v37"]
    assert hits == []


# --------------------------- Per-site sabotage: preflight names the site --

_MUTABLE_SITE_IDS: tuple[str, ...] = tuple(
    site.id for site in PINSITES if site.value_group is not None
)


def _sabotage_candidate(original_value: str) -> str:
    """A same-shaped, different fake value for ``original_value``.

    Different sites enforce different value shapes (a strict 64-hex
    digest, a vX.Y contract revision, an X.Y.Z kit version, an
    unconstrained quoted string) -- ``write_site_value`` does not validate
    the new value's shape, so a fixed sabotage string can corrupt the file
    in a way its OWN pattern no longer recognizes at all (unrecoverable:
    a later ``finditer`` finds nothing to replace, silently). Classifying
    ``original_value``'s own shape picks a candidate guaranteed to
    round-trip through the same pattern that produced it.
    """
    if re.fullmatch(r"[0-9a-f]{64}", original_value):
        return "0" * 63 + "1"
    if re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", original_value):
        return "v9.9.9"
    if re.fullmatch(r"v[0-9]+\.[0-9]+", original_value):
        return "v9.9"
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", original_value):
        return "9.9.9"
    return "SABOTAGED-0000000000000000000000000000000"


def _is_self_referential_rename(site: PinSite) -> bool:
    """A rename site whose own value determines its own path.

    Sabotaging it (renaming the file without also updating the anchor
    that names it) makes its OWN reading un-findable -- structurally a
    drift, never "found holding a different value", since the read
    depends on a different, unchanged sibling (the anchor) to know which
    filename to look for in the first place.
    """
    return (
        site.dynamic_path
        and site.path_group is None
        and site.filename_template is not None
        and site.line is None
    )


def _cascading_groups(site: PinSite) -> set[str]:
    """Which OTHER value_groups legitimately also disagree if ``site`` breaks.

    Two distinct mechanisms, both real consequences of "the version IS the
    path" (doc §7), neither cross-contamination:

    * ``site`` is the anchor -- the first non-dynamic member of some
      group -- that a DIFFERENT dynamic_path site (a different
      value_group) depends on to resolve its own file. Sabotaging an
      anchor's VALUE cascades into every dependent site's path
      resolution (e.g. jc.cluster_config anchors both
      jc.contract_file_content_sha256 and jc.contract_file_wire_sha256).
    * ``site`` is itself a self-referential rename (renames the actual
      file). Every OTHER dynamic_path site sharing the same path_group
      also can no longer find that file -- even though ITS OWN anchor
      value is untouched, the file it names has physically moved.
    """
    site_group = site.path_group or site.value_group
    cascading: set[str] = set()
    for dynamic_site in PINSITES:
        if not dynamic_site.dynamic_path or dynamic_site.value_group is None:
            continue
        if dynamic_site.value_group == site.value_group:
            continue
        group = dynamic_site.path_group or dynamic_site.value_group
        anchor = next(
            candidate for candidate in sites_in_group(group) if not candidate.dynamic_path
        )
        is_anchor = anchor.id == site.id
        is_rename_sibling = _is_self_referential_rename(site) and group == site_group
        if is_anchor or is_rename_sibling:
            cascading.add(dynamic_site.value_group)
    return cascading


def _sabotage(root: Path, site: PinSite, original_value: str) -> str:
    """Write a same-shaped fake value; return it (verified by the caller)."""
    candidate = _sabotage_candidate(original_value)
    assert candidate != original_value
    write_site_value(root, site, candidate, old_value=original_value)
    return candidate


@pytest.mark.parametrize("site_id", _MUTABLE_SITE_IDS)
def test_sabotaging_one_site_fails_agreement_and_names_it(
    mirrored_root: Path, site_id: str
) -> None:
    """Corrupting exactly one site's value fails only its own value_group, by name."""
    site = next(candidate for candidate in PINSITES if candidate.id == site_id)
    original_value = read_site_value(mirrored_root, site)
    sabotage_value = _sabotage(mirrored_root, site, original_value)

    result = run_preflight(mirrored_root)

    assert not result.passed
    sabotaged_group = next(a for a in result.agreements if a.value_group == site.value_group)
    assert not sabotaged_group.agrees
    readings_by_id = {reading.site.id: reading for reading in sabotaged_group.readings}
    own_reading = readings_by_id[site_id]
    if _is_self_referential_rename(site):
        # A renamed file that its own (unchanged) anchor can no longer
        # find is a drift, not "found holding sabotage_value" -- the read
        # depends on a sibling, not on what this site was renamed to.
        assert own_reading.error is not None, f"{site_id} should drift, not silently agree"
    else:
        # The sabotaged site's OWN reading holds the sabotaged value --
        # not just "some site in this group is present in the readings
        # list" (every site in the group is always present; that alone
        # proves nothing, the original B2 bug).
        assert own_reading.value == sabotage_value
    # Every sibling in the same group either still reads its real,
    # un-sabotaged value, or -- only when it is a rename-kind site whose
    # anchor is what we just corrupted -- surfaces its own honest drift.
    # What must never happen is a sibling silently reading some OTHER
    # valid-looking value: that would be real cross-contamination.
    for other_id, reading in readings_by_id.items():
        if other_id != site_id:
            assert reading.value == original_value or reading.error is not None, (
                f"{other_id} silently changed to an unexplained value: {reading.value!r}"
            )
    # Every OTHER value_group is untouched, UNLESS this site is the shared
    # dynamic-path anchor those groups' own sites resolve their file
    # through (see _cascading_groups) -- a real, honest consequence of one
    # anchor serving multiple digest families, not cross-contamination.
    cascading = _cascading_groups(site)
    for agreement in result.agreements:
        if agreement.value_group != site.value_group and agreement.value_group not in cascading:
            assert agreement.agrees, f"unrelated value_group {agreement.value_group} broke too"


def test_sabotaging_a_frozen_site_refuses_to_write() -> None:
    frozen = next(site for site in PINSITES if not site.mutable)
    with pytest.raises(PinSiteError, match="not mutable"):
        write_site_value(ROOT, frozen, "irrelevant", old_value="irrelevant")


def test_frozen_sites_are_actually_read_not_decorative() -> None:
    """B5: 'registered' must never be decorative -- a frozen site is still read."""
    frozen = frozen_sites()
    assert len(frozen) >= 2  # the stable check-id label + the bootstrap placeholder
    assert all(not site.mutable for site in frozen)

    readings = read_frozen_sites(ROOT)

    assert {reading.site.id for reading in readings} == {site.id for site in frozen}
    assert all(reading.error is None for reading in readings)
    check_id_reading = next(
        r for r in readings if r.site.id == "jc.release_gate_secure_runtime_check_id"
    )
    # Legitimately still "v3.6" -- a stable check-id, not a drift bug.
    assert check_id_reading.value == "v3.6"


def test_a_broken_frozen_site_is_reported_but_never_fails_preflight(mirrored_root: Path) -> None:
    site = next(s for s in PINSITES if s.id == "jc.release_gate_secure_runtime_check_id")
    lines = (mirrored_root / site.path).read_text(encoding="utf-8").splitlines(keepends=True)
    assert site.line is not None
    lines[site.line - 1] = "      - secure-runtime.jarvis-query-no-version-here\n"
    (mirrored_root / site.path).write_text("".join(lines), encoding="utf-8")

    result = run_preflight(mirrored_root)

    assert result.passed
    broken = next(r for r in result.frozen if r.site.id == site.id)
    assert broken.error is not None
    report = "\n".join(render_preflight(result))
    assert site.id in report


def test_reading_a_drifted_line_raises_typed_error(mirrored_root: Path) -> None:
    site = next(site for site in PINSITES if site.id == "relay.pyproject")
    (mirrored_root / site.path).write_text("nothing useful here\n", encoding="utf-8")

    with pytest.raises(PinSiteDrifted, match="relay.pyproject"):
        read_site_value(mirrored_root, site)


def test_reading_a_missing_json_key_raises_typed_error(mirrored_root: Path) -> None:
    site = next(site for site in PINSITES if site.id == "relay.matrix_version")
    document = json.loads((mirrored_root / site.path).read_text(encoding="utf-8"))
    del document["release_version"]
    (mirrored_root / site.path).write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(PinSiteDrifted, match="key path"):
        read_site_value(mirrored_root, site)


# --------------------------- v3.7 completeness sweep: sabotage direction --


def test_an_unregistered_v37_literal_is_named_by_the_sweep(tmp_path: Path) -> None:
    package_dir = tmp_path / "src" / "clio_relay"
    package_dir.mkdir(parents=True)
    (package_dir / "new_module.py").write_text(
        'STALE_COPY = "clio-kit-jarvis-user-v3.7.1"\n', encoding="utf-8"
    )

    hits = sweep_jarvis_contract_v37_completeness(tmp_path)

    assert len(hits) == 1
    assert hits[0].path == "src/clio_relay/new_module.py"
    assert hits[0].line == 1


def test_legacy_contract_literals_are_not_swept(tmp_path: Path) -> None:
    """v3.1-v3.7 are deliberate backward-compatibility surface, not #198's concern."""
    package_dir = tmp_path / "src" / "clio_relay"
    package_dir.mkdir(parents=True)
    (package_dir / "legacy_module.py").write_text(
        'OLD = "clio-kit-jarvis-user-v3.6"\nALSO_OLD = "clio-kit-jarvis-user-v3.7"\n',
        encoding="utf-8",
    )

    assert sweep_jarvis_contract_v37_completeness(tmp_path) == ()


def test_an_unregistered_v37_literal_in_docs_markdown_is_named_by_the_sweep(
    tmp_path: Path,
) -> None:
    """The sweep covers docs/*.md and docs/ai/*.md too (doc §7.9, B3), not just code."""
    docs_dir = tmp_path / "docs"
    ai_dir = docs_dir / "ai"
    ai_dir.mkdir(parents=True)
    (docs_dir / "some-guide.md").write_text(
        "Exact `clio-kit-jarvis-user-v3.7.1` routes support staging.\n", encoding="utf-8"
    )
    (ai_dir / "some-context.md").write_text(
        "Registered `clio-kit-jarvis-user-v3.7.1` gates the staging plane.\n", encoding="utf-8"
    )

    hits = sweep_jarvis_contract_v37_completeness(tmp_path)

    hit_paths = {hit.path for hit in hits}
    assert hit_paths == {"docs/some-guide.md", "docs/ai/some-context.md"}


def test_docs_design_prose_about_the_registry_is_not_swept(tmp_path: Path) -> None:
    """docs/design/ is deliberately shallow-excluded: it discusses v3.7.1 as prose

    about the pin registry itself (this section's own audit language), not a
    duplicate pin -- sweeping it recursively would flag the document that
    specifies this sweep.
    """
    design_dir = tmp_path / "docs" / "design"
    design_dir.mkdir(parents=True)
    (design_dir / "relay-architecture-2026-08.md").write_text(
        "The registry tracks `clio-kit-jarvis-user-v3.7.1` sites.\n", encoding="utf-8"
    )

    assert sweep_jarvis_contract_v37_completeness(tmp_path) == ()


# --------------------------- Bump: dry-run golden -------------------------


def test_bump_dry_run_reports_every_kit_version_site_and_writes_nothing(
    mirrored_root: Path,
) -> None:
    before = {relative: (mirrored_root / relative).read_bytes() for relative in _REGISTERED_FILES}
    new_sha256 = "b" * 64

    changes = plan_bump(
        mirrored_root,
        BumpTargets(bootstrap_kit_version="9.9.9", bootstrap_kit_wheel_sha256=new_sha256),
    )

    changed_ids = {change.site.id for change in changes}
    expected_text_ids = {
        site.id for site in PINSITES if site.value_group == "bootstrap_kit_version_text"
    }
    expected_digest_ids = {
        site.id for site in PINSITES if site.value_group == "bootstrap_kit_wheel_sha256"
    }
    assert expected_text_ids <= changed_ids
    assert expected_digest_ids <= changed_ids
    # The independent acceptance-policy axis is untouched by a bootstrap-only
    # bump (clio-relay#190/#199) -- not silently dragged along.
    acceptance_ids = {
        site.id
        for site in PINSITES
        if site.value_group in {"acceptance_kit_version_text", "acceptance_kit_wheel_sha256"}
    }
    assert not (acceptance_ids & changed_ids)
    assert all(change.applied is False for change in changes)
    assert all(change.skipped_reason is None for change in changes)
    # A dry-run plan never writes -- every mirrored file is byte-identical after.
    for relative in _REGISTERED_FILES:
        assert (mirrored_root / relative).read_bytes() == before[relative], relative


def test_bump_dry_run_reports_no_changes_when_target_equals_current(
    mirrored_root: Path,
) -> None:
    changes = plan_bump(mirrored_root, BumpTargets(relay_version="1.6.8"))
    assert changes == ()


# --------------------------- Bump: apply + matrix digest reuse ------------


def test_bump_applies_relay_version_and_recomputes_matrix_digest_via_ci_validation(
    mirrored_root: Path,
) -> None:
    changes = apply_bump(mirrored_root, BumpTargets(relay_version="9.9.9"))

    assert all(change.applied for change in changes)
    changed_ids = {change.site.id for change in changes}
    assert {"relay.pyproject", "relay.package_init", "matrix.canonical_digest"} <= changed_ids

    matrix_path = mirrored_root / "examples/release-gate/report-matrix-1.0.json"
    matrix_document = json.loads(matrix_path.read_text(encoding="utf-8"))
    assert matrix_document["release_version"] == "9.9.9"

    # The exact digest computation the matrix's own validator requires --
    # never independently recomputed by this test either.
    validated = ci_validation.validate_release_acceptance_matrix(matrix_document)
    assert validated["matrix_sha256"] == matrix_document["matrix_sha256"]

    canonical = {key: value for key, value in matrix_document.items() if key != "matrix_sha256"}
    assert matrix_document["matrix_sha256"] == (
        ci_validation.compute_release_acceptance_matrix_sha256(canonical)
    )

    gate_path = mirrored_root / "docs/release-gate-1.0.yaml"
    gate_text = gate_path.read_text(encoding="utf-8")
    assert f"acceptance_matrix_sha256: {matrix_document['matrix_sha256']}" in gate_text
    assert 'release_version: "9.9.9"' in gate_text

    result = run_preflight(mirrored_root)
    assert result.passed, "\n".join(render_preflight(result))


def test_bump_applies_contract_id_sites_without_digest_arguments(mirrored_root: Path) -> None:
    """Omitting the digest arguments leaves digest value_groups unchanged, not silently."""
    rename_site = next(site for site in PINSITES if site.id == "jc.contract_file_rename")
    old_contract_version = read_site_value(mirrored_root, rename_site)

    changes = apply_bump(mirrored_root, BumpTargets(contract_version="v3.8"))

    id_changes = [change for change in changes if change.site.value_group == "jarvis_contract_id"]
    assert len(id_changes) == len(
        [site for site in PINSITES if site.value_group == "jarvis_contract_id"]
    )
    assert all(change.new_value == "v3.8" for change in id_changes)
    # No digest value_group was touched: no --contract-sha256 was given.
    assert not any(change.site.value_group == "jarvis_contract_sha256" for change in changes)

    contract_path = mirrored_root / "src/clio_relay/_contracts/jarvis-user-v3.8.json"
    assert contract_path.is_file()
    assert not (
        mirrored_root / f"src/clio_relay/_contracts/jarvis-user-{old_contract_version}.json"
    ).exists()


def test_apply_bump_reports_drift_instead_of_silently_skipping(mirrored_root: Path) -> None:
    site = next(site for site in PINSITES if site.id == "relay.pyproject")
    (mirrored_root / site.path).write_text("nothing useful here\n", encoding="utf-8")

    changes = apply_bump(mirrored_root, BumpTargets(relay_version="9.9.9"))

    drifted = [change for change in changes if change.skipped_reason is not None]
    assert any(change.site.id == "relay.pyproject" for change in drifted)
