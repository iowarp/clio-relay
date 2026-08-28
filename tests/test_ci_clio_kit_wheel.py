"""CI invariants for the exact upstream clio-kit release wheel."""

from __future__ import annotations

import hashlib
import urllib.error
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)
from clio_relay.remote_mcp import (
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_WHEEL_VERSION,
    CLIO_KIT_SPACK_USER_WHEEL_VERSION,
)
from tests import clio_kit_test_artifacts

ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"

# The bootstrap default install pin (clio-relay#190): the exact clio-kit
# release a *fresh* deployment installs for its built-in JARVIS MCP server.
# This is the runtime dependency pin, not a certification snapshot: it moves
# independently of CONTRACT_CERTIFICATION_WHEEL_* below. Kept in sync with
# clio_relay.jarvis_mcp.CLIO_KIT_JARVIS_MCP_VERSION by hand (release 1.6.8,
# chore(release) c0ec01f) -- this file intentionally re-derives its own
# copies rather than importing the wheel filename/URL/SHA-256 so a source
# bump can never silently drag the CI-workflow-consistency check along
# unnoticed; this test is the tripwire that forces the two to be reconciled.
JARVIS_MCP_WHEEL_FILENAME = "clio_kit-2.10.5-py3-none-any.whl"
JARVIS_MCP_WHEEL_SHA256 = "954c2f76aef96ad1a69717f693b15f76d6fa0a3cd1d2ae417baf2830c73c3a67"
JARVIS_MCP_WHEEL_URL = (
    f"https://github.com/iowarp/clio-kit/releases/download/v2.10.5/{JARVIS_MCP_WHEEL_FILENAME}"
)

# The exact upstream wheel CI stages before the local release gate, which
# includes the cross-repository contract-certification suite
# (test_clio_kit_mcp_contracts.py). That suite vendors byte-exact copies of
# clio-kit's jarvis-user, spack-user, and scientific-catalog-user contracts
# under src/clio_relay/_contracts/ and re-verifies them against a live wheel.
# clio-kit 2.7.2 shifted the wire/contract bytes for every locked MCP
# surface (jarvis, spack, scientific-catalog, slurm) by adding a "title" key
# to every user-profile tool. #190/#191 moved only the bootstrap install pin
# above to 2.7.2 and deliberately left this certification pin on 2.6.6 as
# tracked follow-up; clio-relay#199 completed that follow-up by
# re-certifying every locked surface against 2.7.2, so this pin now matches
# the bootstrap install pin again. CI stages exactly one wheel per job
# (single "stage exact clio-kit release wheel" step, ci.yml) and
# provision_clio_kit_wheel() consumes that same staged artifact for the
# contract-certification suite, so this pin tracks the bootstrap pin above
# going forward -- re-certified to 2.10.5 by the clio-relay#288 recert
# (the v3.7.2-revision JARVIS contract-recognition work;
# test_clio_kit_mcp_contracts.py passes against this exact wheel).
CONTRACT_CERTIFICATION_WHEEL_FILENAME = "clio_kit-2.10.5-py3-none-any.whl"
CONTRACT_CERTIFICATION_WHEEL_SHA256 = (
    "954c2f76aef96ad1a69717f693b15f76d6fa0a3cd1d2ae417baf2830c73c3a67"
)
CONTRACT_CERTIFICATION_WHEEL_URL = (
    "https://github.com/iowarp/clio-kit/releases/download/v2.10.5/"
    f"{CONTRACT_CERTIFICATION_WHEEL_FILENAME}"
)


def _configure_fixture_wheel_pin(
    monkeypatch: pytest.MonkeyPatch,
    *,
    filename: str,
    payload: bytes,
) -> None:
    """Point the provisioning helper at one small deterministic test wheel."""
    monkeypatch.setattr(
        clio_kit_test_artifacts,
        "CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME",
        filename,
    )
    monkeypatch.setattr(
        clio_kit_test_artifacts,
        "CLIO_KIT_JARVIS_MCP_WHEEL_SHA256",
        hashlib.sha256(payload).hexdigest(),
    )
    monkeypatch.setattr(
        clio_kit_test_artifacts,
        "CLIO_KIT_JARVIS_MCP_WHEEL_URL",
        f"https://downloads.invalid/{filename}",
    )


def test_contract_wheel_provisioning_reuses_a_verified_cross_worktree_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh checkout can reuse exact cached bytes without sibling artifacts."""
    payload = b"small immutable wheel fixture"
    filename = "clio_kit-9.9.9-py3-none-any.whl"
    _configure_fixture_wheel_pin(monkeypatch, filename=filename, payload=payload)
    monkeypatch.delenv("CLIO_RELAY_CLIO_KIT_WHEEL", raising=False)
    monkeypatch.delenv("CLIO_RELAY_CLIO_KIT_WHEEL_SHA256", raising=False)
    monkeypatch.setenv("CLIO_RELAY_CLIO_KIT_WHEEL_CACHE", str(tmp_path))
    cached = tmp_path / filename
    cached.write_bytes(payload)

    assert clio_kit_test_artifacts.provision_clio_kit_wheel() == cached.resolve()


def test_contract_wheel_provisioning_reports_an_actionable_offline_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unavailable immutable artifact is a typed configuration error, never a skip."""
    payload = b"absent wheel fixture"
    filename = "clio_kit-9.9.8-py3-none-any.whl"
    _configure_fixture_wheel_pin(monkeypatch, filename=filename, payload=payload)
    monkeypatch.delenv("CLIO_RELAY_CLIO_KIT_WHEEL", raising=False)
    monkeypatch.delenv("CLIO_RELAY_CLIO_KIT_WHEEL_SHA256", raising=False)
    monkeypatch.setenv("CLIO_RELAY_CLIO_KIT_WHEEL_CACHE", str(tmp_path))

    def unavailable(_request: object, *, timeout: float) -> Any:
        del timeout
        raise urllib.error.URLError("offline")

    monkeypatch.setattr(clio_kit_test_artifacts.urllib.request, "urlopen", unavailable)

    with pytest.raises(ConfigurationError, match="provide the verified release artifact"):
        clio_kit_test_artifacts.provision_clio_kit_wheel()


def test_bootstrap_jarvis_mcp_install_pin_is_self_consistent() -> None:
    """The bootstrap default JARVIS MCP install pin resolves to one exact wheel (#190)."""
    assert CLIO_KIT_JARVIS_MCP_VERSION == "2.10.5"
    assert CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME == JARVIS_MCP_WHEEL_FILENAME
    assert CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 == JARVIS_MCP_WHEEL_SHA256
    assert CLIO_KIT_JARVIS_MCP_WHEEL_URL == JARVIS_MCP_WHEEL_URL


def test_contract_certification_pins_match_the_certified_release() -> None:
    """spack/scientific-catalog are certified against the same released wheel.

    clio-kit 2.7.2 shifted every locked MCP contract's wire/contract bytes
    (jarvis, spack, scientific-catalog, slurm) by adding a "title" key to
    every user-profile tool. #190/#191 moved only the bootstrap install pin
    to 2.7.2 and deliberately left this certification pin on 2.6.6 as
    tracked follow-up (clio-relay#199); that follow-up re-certified every
    locked surface against 2.7.2, so the certification pin below now equals
    the bootstrap install pin above instead of trailing it. clio-relay#288
    (kit 2.10.5 recert) moved this pin again -- the spack v2.1 / scientific-
    catalog v1.1 contract bytes/digests are byte-identical in 2.10.5 (live-
    probe verified), so this is the single-wheel-download version tracking
    the bootstrap pin forward, not a contract re-certification.
    """
    assert CLIO_KIT_SPACK_USER_WHEEL_VERSION == "2.10.5"
    assert CLIO_KIT_SCIENTIFIC_CATALOG_USER_WHEEL_VERSION == "2.10.5"


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
            "CLIO_KIT_WHEEL_FILENAME": CONTRACT_CERTIFICATION_WHEEL_FILENAME,
            "CLIO_KIT_WHEEL_SHA256": CONTRACT_CERTIFICATION_WHEEL_SHA256,
            "CLIO_KIT_WHEEL_URL": CONTRACT_CERTIFICATION_WHEEL_URL,
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
        assert "clio-kit-v2.7.2" in script
        assert "clio-kit-v2.6.6" not in script

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
