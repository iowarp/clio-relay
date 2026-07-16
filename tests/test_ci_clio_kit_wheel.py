"""CI invariants for the exact upstream clio-kit release wheel."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)

ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
WHEEL_FILENAME = "clio_kit-2.4.7-py3-none-any.whl"
WHEEL_SHA256 = "68f9a10b586898781c02f88f005bfffb6f2174a7f6f8b783925d58eb23accd84"
WHEEL_URL = f"https://github.com/iowarp/clio-kit/releases/download/v2.4.7/{WHEEL_FILENAME}"


def test_runtime_and_ci_share_one_exact_clio_kit_release_pin() -> None:
    """Keep bootstrap, JARVIS MCP, and CI on the same exact release wheel bytes."""
    assert CLIO_KIT_JARVIS_MCP_VERSION == "2.4.7"
    assert CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME == WHEEL_FILENAME
    assert CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 == WHEEL_SHA256
    assert CLIO_KIT_JARVIS_MCP_WHEEL_URL == WHEEL_URL


def _ci_workflow() -> dict[str, Any]:
    """Load the CI workflow as a mapping."""
    document = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise AssertionError("CI workflow must be a mapping")
    return cast(dict[str, Any], document)


def test_ci_jobs_stage_exact_clio_kit_wheel_before_evidence_gate() -> None:
    """Every evidence-producing CI job must bind to the released wheel bytes."""
    workflow = _ci_workflow()
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    gate_names = {
        "build": "run the sole artifact-building release gate",
        "validate": "validate without rebuilding the distributions",
    }

    for job_name, gate_name in gate_names.items():
        steps = cast(list[dict[str, Any]], jobs[job_name]["steps"])
        by_name = {str(step.get("name")): step for step in steps}
        stage = by_name["stage exact clio-kit release wheel"]

        assert steps.index(stage) < steps.index(by_name[gate_name])
        assert stage["shell"] == "bash"
        assert stage["env"] == {
            "CLIO_KIT_WHEEL_FILENAME": WHEEL_FILENAME,
            "CLIO_KIT_WHEEL_SHA256": WHEEL_SHA256,
            "CLIO_KIT_WHEEL_URL": WHEEL_URL,
        }

    assert jobs["validate"]["needs"] == "build"


def test_ci_wheel_download_is_bounded_https_only_and_fail_closed() -> None:
    """The staged dependency must never fall back to mutable index resolution."""
    workflow = _ci_workflow()
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    scripts: dict[str, str] = {}
    for job_name in ("build", "validate"):
        steps = cast(list[dict[str, Any]], jobs[job_name]["steps"])
        stage = next(
            step for step in steps if step.get("name") == "stage exact clio-kit release wheel"
        )
        scripts[job_name] = str(stage["run"])

    for script in scripts.values():
        for required in (
            "set -euo pipefail",
            "umask 077",
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
            "CLIO_RELAY_CLIO_KIT_WHEEL=%s",
            "CLIO_RELAY_CLIO_KIT_WHEEL_SHA256=%s",
            '>> "$GITHUB_ENV"',
        ):
            assert required in script

        assert "pypi.org" not in script
        assert "pip install" not in script
        assert "uvx" not in script
        assert "|| true" not in script

    assert 'if [ "$RUNNER_OS" = Windows ]' in scripts["validate"]
    assert "cygpath --windows --absolute" in scripts["validate"]


def test_tag_workflow_does_not_repeat_the_clio_kit_release_gate() -> None:
    """Tag publication must consume prior build output without rerunning the full gate."""
    workflow = cast(dict[str, Any], yaml.safe_load(RELEASE_WORKFLOW.read_text(encoding="utf-8")))
    jobs = cast(dict[str, dict[str, Any]], workflow["jobs"])
    serialized = str(workflow)

    assert set(jobs) == {"bind", "publish-pypi"}
    assert "stage exact clio-kit release wheel" not in serialized
    assert "CLIO_RELAY_CLIO_KIT_WHEEL" not in serialized
    assert "release validate-local" not in serialized
