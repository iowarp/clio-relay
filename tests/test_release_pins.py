"""Tests for the release-identity pin registry (iowarp/clio-relay#198, #231 R7)."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from clio_relay import ci_validation
from clio_relay.release_pins import (
    PINSITES,
    BumpTargets,
    PinSiteDrifted,
    PinSiteError,
    apply_bump,
    plan_bump,
    read_site_value,
    render_preflight,
    run_preflight,
    sweep_incompleteness,
    sweep_jarvis_contract_v37_completeness,
    value_groups,
    write_site_value,
)

ROOT = Path(__file__).parents[1]

#: Every file PINSITES references, relative to the repo root -- exactly the
#: files a mirrored test tree needs to exercise agreement/read/write logic.
_REGISTERED_FILES: tuple[str, ...] = tuple(sorted({site.path for site in PINSITES}))


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


def test_value_groups_cover_every_release_identity_axis() -> None:
    groups = value_groups()
    assert groups == (
        "jarvis_contract_artifact_sha256",
        "jarvis_contract_id",
        "jarvis_contract_sha256",
        "jarvis_contract_wire_sha256",
        "kit_version_text",
        "kit_wheel_sha256",
        "matrix_digest",
        "relay_version",
    )


def test_registry_has_no_duplicate_site_ids() -> None:
    ids = [site.id for site in PINSITES]
    duplicates = sorted({site_id for site_id in ids if ids.count(site_id) > 1})
    assert len(ids) == len(set(ids)), f"duplicate PinSite ids: {duplicates}"


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


@pytest.mark.parametrize("site_id", _MUTABLE_SITE_IDS)
def test_sabotaging_one_site_fails_agreement_and_names_it(
    mirrored_root: Path, site_id: str
) -> None:
    """Corrupting exactly one site's value fails only its own value_group, by name."""
    site = next(candidate for candidate in PINSITES if candidate.id == site_id)
    write_site_value(mirrored_root, site, "SABOTAGED-0000000000000000000000000000000")

    result = run_preflight(mirrored_root)

    assert not result.passed
    sabotaged_group = next(a for a in result.agreements if a.value_group == site.value_group)
    assert not sabotaged_group.agrees
    named_ids = {reading.site.id for reading in sabotaged_group.readings}
    assert site_id in named_ids
    # Every OTHER value_group is untouched by sabotaging this one site.
    for agreement in result.agreements:
        if agreement.value_group != site.value_group:
            assert agreement.agrees, f"unrelated value_group {agreement.value_group} broke too"


def test_sabotaging_a_frozen_site_refuses_to_write() -> None:
    frozen = next(site for site in PINSITES if not site.mutable)
    with pytest.raises(PinSiteError, match="not mutable"):
        write_site_value(ROOT, frozen, "irrelevant")


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
        'STALE_COPY = "clio-kit-jarvis-user-v3.7"\n', encoding="utf-8"
    )

    hits = sweep_jarvis_contract_v37_completeness(tmp_path)

    assert len(hits) == 1
    assert hits[0].path == "src/clio_relay/new_module.py"
    assert hits[0].line == 1


def test_legacy_contract_literals_are_not_swept(tmp_path: Path) -> None:
    """v3.1-v3.6 are deliberate backward-compatibility surface, not #198's concern."""
    package_dir = tmp_path / "src" / "clio_relay"
    package_dir.mkdir(parents=True)
    (package_dir / "legacy_module.py").write_text(
        'OLD = "clio-kit-jarvis-user-v3.6"\n', encoding="utf-8"
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
        BumpTargets(kit_version="9.9.9", kit_wheel_sha256=new_sha256),
    )

    changed_ids = {change.site.id for change in changes}
    expected_text_ids = {site.id for site in PINSITES if site.value_group == "kit_version_text"}
    expected_digest_ids = {site.id for site in PINSITES if site.value_group == "kit_wheel_sha256"}
    assert expected_text_ids <= changed_ids
    assert expected_digest_ids <= changed_ids
    assert all(change.applied is False for change in changes)
    assert all(change.skipped_reason is None for change in changes)
    # A dry-run plan never writes -- every mirrored file is byte-identical after.
    for relative in _REGISTERED_FILES:
        assert (mirrored_root / relative).read_bytes() == before[relative], relative


def test_bump_dry_run_reports_no_changes_when_target_equals_current(
    mirrored_root: Path,
) -> None:
    changes = plan_bump(mirrored_root, BumpTargets(relay_version="1.6.6"))
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
    assert not (mirrored_root / "src/clio_relay/_contracts/jarvis-user-v3.7.json").exists()


def test_apply_bump_reports_drift_instead_of_silently_skipping(mirrored_root: Path) -> None:
    site = next(site for site in PINSITES if site.id == "relay.pyproject")
    (mirrored_root / site.path).write_text("nothing useful here\n", encoding="utf-8")

    changes = apply_bump(mirrored_root, BumpTargets(relay_version="9.9.9"))

    drifted = [change for change in changes if change.skipped_reason is not None]
    assert any(change.site.id == "relay.pyproject" for change in drifted)
