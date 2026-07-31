"""Generate the committed cross-repo relay JSON-Schema expectations.

This generator deliberately imports relay models only. The shared-model source
provenance is the committed clio-schemas v0.2.0 projection fixture, not a live
``clio-schemas`` dependency, because release validation exports and audits
hash-pinned requirements.
"""

from __future__ import annotations

import argparse
import difflib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final, cast

from pydantic import BaseModel

from clio_relay.models import ArtifactRef, ArtifactUse, RelayJob, TaskTimelineEvent

type JSONValue = dict[str, JSONValue] | list[JSONValue] | str | int | float | bool | None

GOLDEN_PATH: Final = Path(__file__).parent / "fixtures" / "relay_schema_equality_v1.json"
SCHEMA_MODELS: Final[dict[str, type[BaseModel]]] = {
    "ArtifactUse": ArtifactUse,
    "ArtifactRef": ArtifactRef,
    "RelayJob": RelayJob,
    "TaskTimelineEvent": TaskTimelineEvent,
}
MODEL_DISPOSITIONS: Final[dict[str, dict[str, str]]] = {
    "ArtifactUse": {
        "disposition": "clio-schemas projection reference",
        "reference": (
            "clio-schemas v0.2.0 ProvEdge.to_artifact_use; "
            "tests/fixtures/clio_schemas_v0_2_0_projections.json"
        ),
    },
    "ArtifactRef": {
        "disposition": "clio-schemas projection reference",
        "reference": (
            "clio-schemas v0.2.0 ArtifactVersion.to_artifact_ref; "
            "tests/fixtures/clio_schemas_v0_2_0_projections.json"
        ),
    },
    "RelayJob": {
        "disposition": "relay-owned committed golden",
        "reference": "clio_relay.models.RelayJob",
    },
    "TaskTimelineEvent": {
        "disposition": "relay-owned committed golden",
        "reference": "clio_relay.models.TaskTimelineEvent",
    },
}
_NON_WIRE_SCHEMA_KEYS: Final = frozenset({"description", "title"})


def canonical_wire_schema(value: JSONValue) -> JSONValue:
    """Remove documentation-only JSON-Schema annotations and sort mappings."""
    if isinstance(value, dict):
        return {
            key: canonical_wire_schema(item)
            for key, item in sorted(value.items())
            if key not in _NON_WIRE_SCHEMA_KEYS
        }
    if isinstance(value, list):
        return [canonical_wire_schema(item) for item in value]
    return value


def build_schema_equality_document(
    models: Mapping[str, type[BaseModel]] = SCHEMA_MODELS,
) -> dict[str, Any]:
    """Build the canonical committed schema-equality document."""
    if set(models) != set(MODEL_DISPOSITIONS):
        raise ValueError("schema equality models must match the committed disposition catalog")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "schema_version": "clio-relay.cross-repo-schema-equality.v1",
        "issue": "iowarp/clio-agent#1121 P2.3",
        "runtime_dependency_policy": (
            "No clio-schemas runtime dependency; release audit requires hashed requirements."
        ),
        "models": {
            name: {
                **MODEL_DISPOSITIONS[name],
                "json_schema": canonical_wire_schema(
                    cast("JSONValue", model.model_json_schema(mode="serialization"))
                ),
            }
            for name, model in sorted(models.items())
        },
    }


def render_schema_equality_document(
    models: Mapping[str, type[BaseModel]] = SCHEMA_MODELS,
) -> str:
    """Render the canonical schema document with stable bytes."""
    return (
        json.dumps(
            build_schema_equality_document(models),
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n"
    )


def assert_schema_equality_golden_current(
    models: Mapping[str, type[BaseModel]] = SCHEMA_MODELS,
) -> None:
    """Raise with a unified diff when runtime wire schemas drift from the golden."""
    expected = GOLDEN_PATH.read_text(encoding="utf-8")
    actual = render_schema_equality_document(models)
    if actual == expected:
        return
    diff = "".join(
        difflib.unified_diff(
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            fromfile=str(GOLDEN_PATH),
            tofile="generated relay wire schemas",
        )
    )
    raise AssertionError(f"committed relay schema equality golden is stale:\n{diff}")


def main(argv: Sequence[str] | None = None) -> int:
    """Write the golden, or verify it without mutation with ``--check``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail when the committed golden differs instead of rewriting it",
    )
    arguments = parser.parse_args(argv)
    if arguments.check:
        assert_schema_equality_golden_current()
        print(f"OK: {GOLDEN_PATH} is canonical")
        return 0
    GOLDEN_PATH.write_text(render_schema_equality_document(), encoding="utf-8", newline="\n")
    print(f"Wrote {GOLDEN_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
