"""Relay half of cross-repo schema and task-state equality CI (#1121, P2.3)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay.fastmcp_server import RELAY_STATE_MAP
from clio_relay.models import ArtifactUse, JobState
from tests.generate_schema_equality_goldens import (
    MODEL_DISPOSITIONS,
    SCHEMA_MODELS,
    assert_schema_equality_golden_current,
)

_FIXTURES = Path(__file__).parent / "fixtures"


def test_relay_wire_schemas_match_committed_cross_repo_goldens() -> None:
    """Every federated relay wire model must equal its committed expectation."""
    assert_schema_equality_golden_current()


def test_schema_equality_model_dispositions_are_explicit() -> None:
    """Shared projections and relay-owned models must retain distinct provenance."""
    assert {
        name: disposition["disposition"] for name, disposition in MODEL_DISPOSITIONS.items()
    } == {
        "ArtifactUse": "clio-schemas projection reference",
        "ArtifactRef": "clio-schemas projection reference",
        "RelayJob": "relay-owned committed golden",
        "TaskTimelineEvent": "relay-owned committed golden",
    }


def test_ci_checks_committed_schema_goldens_without_syncing() -> None:
    """The primary CI leg must run the generator's read-only check mode."""
    workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )

    assert "uv run --no-sync python -m tests.generate_schema_equality_goldens --check" in workflow


def test_schema_equality_detector_rejects_one_field_model_sabotage() -> None:
    """Adding one wire field must make equality fail before a golden contract rev."""

    class SabotagedArtifactUse(ArtifactUse):
        sabotage_field: str | None = None

    sabotaged_models = {**SCHEMA_MODELS, "ArtifactUse": SabotagedArtifactUse}
    with pytest.raises(AssertionError, match="sabotage_field"):
        assert_schema_equality_golden_current(sabotaged_models)


def test_relay_state_map_matches_documented_mcp_task_projection() -> None:
    """Relay's implementation table must equal the committed cross-repo table."""
    document = cast(
        "dict[str, Any]",
        json.loads((_FIXTURES / "relay_state_map_v1.json").read_text(encoding="utf-8")),
    )
    actual = [
        {
            "relay_observations": list(observations),
            "mcp_task_status": status,
            "isError": is_error,
        }
        for observations, status, is_error in RELAY_STATE_MAP
    ]

    assert actual == document["rows"]


def test_relay_state_map_is_total_and_bijective_by_documented_row() -> None:
    """Each relay observation occurs once and each semantic projection is distinct."""
    observations = [
        observation
        for row_observations, _status, _is_error in RELAY_STATE_MAP
        for observation in row_observations
    ]
    projections = [(status, is_error) for _observations, status, is_error in RELAY_STATE_MAP]

    direct_job_states = set(JobState) - {JobState.FAILED}
    assert direct_job_states <= set(observations)
    assert {"tool_failure", "protocol_error"} <= set(observations)
    assert JobState.FAILED.value not in observations
    assert len(observations) == len(set(observations))
    assert len(projections) == len(set(projections))
