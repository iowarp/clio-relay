"""Convergence acceptance for clio-schemas record projections (#143, P2.2).

The inputs are GOLDEN FIXTURES generated from clio-schemas v0.2.0's own
``ArtifactVersion.to_artifact_ref()`` / ``ProvEdge.to_artifact_use()`` output
(``tests/fixtures/clio_schemas_v0_2_0_projections.json``) rather than a live
dependency: relay's published wheel must stay PyPI-resolvable and its
dependency audit requires hashed requirements, while clio-schemas is a
git-tag-only package. The fixtures state their provenance; regeneration happens
only on a deliberate contract rev, and cross-repo drift detection is owned by
the P2.3 schema-equality CI (iowarp/clio-agent#1122).
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from clio_relay.models import ArtifactRef, ArtifactUse, ArtifactUseEvidence

_FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "clio_schemas_v0_2_0_projections.json").read_text(
        encoding="utf-8"
    )
)


def _clio_artifact_ref_output() -> dict[str, Any]:
    """A fresh copy of clio's ``to_artifact_ref()`` output (v0.2.0 golden)."""
    return copy.deepcopy(cast("dict[str, Any]", _FIXTURE["artifact_ref_input"]))


def test_clio_artifact_version_output_validates_as_relay_artifact_ref() -> None:
    artifact = ArtifactRef(**_clio_artifact_ref_output())

    assert artifact.artifact_id == "artifact_" + "a" * 32
    assert artifact.job_id == "job_test0001"
    assert artifact.sequence == 7
    assert artifact.uri == "artifact://workspace/data.csv@v7"
    assert artifact.kind == "dataset"
    assert artifact.size_bytes == 123


def test_clio_prov_edge_output_validates_as_relay_artifact_use() -> None:
    output = copy.deepcopy(cast("dict[str, Any]", _FIXTURE["artifact_use_input"]))

    artifact_use = ArtifactUse(**output)
    assert artifact_use.artifact_id == "artifact_" + "a" * 32
    assert artifact_use.sha256 == "b" * 64


def test_clio_and_relay_artifact_use_evidence_values_are_identical() -> None:
    assert _FIXTURE["edge_evidence_values"] == [item.value for item in ArtifactUseEvidence]


def test_clio_artifact_ref_convergence_keeps_extra_forbid() -> None:
    output = _clio_artifact_ref_output()
    output["genuinely_unknown"] = True

    with pytest.raises(ValidationError) as exc_info:
        ArtifactRef(**output)

    assert any(error["type"] == "extra_forbidden" for error in exc_info.value.errors())


def test_clio_artifact_ref_round_trip_preserves_provenance_fields() -> None:
    output = _clio_artifact_ref_output()
    expected_provenance = cast("dict[str, Any]", output["metadata"])["clio.provenance.v1"]

    serialized = ArtifactRef(**output).model_dump(mode="json")

    assert serialized["metadata"]["clio.provenance.v1"] == expected_provenance
    assert {
        field: serialized[field]
        for field in ("job_id", "sequence", "uri", "size_bytes", "created_at")
    } == expected_provenance
