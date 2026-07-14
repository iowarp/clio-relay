"""CI invariants for the exact upstream clio-kit release wheel."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
WHEEL_FILENAME = "clio_kit-2.3.2-py3-none-any.whl"
WHEEL_SHA256 = "6763c500db777428edc57ed2e1157cefdbe54f9504f2374e9fdc8055870b7321"
WHEEL_URL = f"https://github.com/iowarp/clio-kit/releases/download/v2.3.2/{WHEEL_FILENAME}"


def _ci_workflow() -> dict[str, Any]:
    """Load the CI workflow as a mapping."""
    document = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise AssertionError("CI workflow must be a mapping")
    return cast(dict[str, Any], document)


def test_ci_matrix_stages_exact_clio_kit_wheel_before_evidence_gate() -> None:
    """Every supported runner must bind gate evidence to the released wheel bytes."""
    workflow = _ci_workflow()
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    test_job = jobs["test"]
    matrix = cast(dict[str, list[str]], test_job["strategy"]["matrix"])
    steps = cast(list[dict[str, Any]], test_job["steps"])
    by_name = {str(step.get("name")): step for step in steps}
    stage = by_name["stage exact clio-kit release wheel"]
    stage_index = steps.index(stage)
    gate_index = steps.index(by_name["run evidence-producing local release gate"])

    assert matrix["os"] == ["ubuntu-latest", "windows-latest"]
    assert stage_index < gate_index
    assert stage["shell"] == "bash"
    assert stage["env"] == {
        "CLIO_KIT_WHEEL_FILENAME": WHEEL_FILENAME,
        "CLIO_KIT_WHEEL_SHA256": WHEEL_SHA256,
        "CLIO_KIT_WHEEL_URL": WHEEL_URL,
    }


def test_ci_wheel_download_is_bounded_https_only_and_fail_closed() -> None:
    """The staged dependency must never fall back to mutable index resolution."""
    workflow = _ci_workflow()
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    steps = cast(list[dict[str, Any]], jobs["test"]["steps"])
    stage = next(step for step in steps if step.get("name") == "stage exact clio-kit release wheel")
    script = str(stage["run"])

    for required in (
        "set -euo pipefail",
        "--fail",
        "--location",
        "--proto '=https'",
        "--proto-redir '=https'",
        "--tlsv1.2",
        "--retry 3",
        "--retry-all-errors",
        "--retry-max-time 180",
        "--connect-timeout 20",
        "--max-time 180",
        "sha256sum --check --strict",
        'if [ "$RUNNER_OS" = Windows ]',
        "cygpath --windows --absolute",
        "CLIO_RELAY_CLIO_KIT_WHEEL=%s",
        "CLIO_RELAY_CLIO_KIT_WHEEL_SHA256=%s",
        '>> "$GITHUB_ENV"',
    ):
        assert required in script

    assert "pypi.org" not in script
    assert "pip install" not in script
    assert "uvx" not in script
    assert "|| true" not in script


def test_tag_payload_gate_stages_the_same_exact_clio_kit_wheel() -> None:
    """The tag payload's full local gate must receive the release-bound dependency."""
    workflow = cast(dict[str, Any], yaml.safe_load(RELEASE_WORKFLOW.read_text(encoding="utf-8")))
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    steps = cast(list[dict[str, Any]], jobs["build"]["steps"])
    by_name = {str(step.get("name")): step for step in steps}
    stage = by_name["stage exact clio-kit release wheel"]
    gate = by_name["run evidence-producing local release gate"]

    assert steps.index(stage) < steps.index(gate)
    assert stage["env"] == {
        "CLIO_KIT_WHEEL_FILENAME": WHEEL_FILENAME,
        "CLIO_KIT_WHEEL_SHA256": WHEEL_SHA256,
        "CLIO_KIT_WHEEL_URL": WHEEL_URL,
    }
    script = str(stage["run"])
    assert "set -euo pipefail" in script
    assert "sha256sum --check --strict" in script
    assert "--proto '=https'" in script
    assert "CLIO_RELAY_CLIO_KIT_WHEEL=%s" in script
    assert "CLIO_RELAY_CLIO_KIT_WHEEL_SHA256=%s" in script
