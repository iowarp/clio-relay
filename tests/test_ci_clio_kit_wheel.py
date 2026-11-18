"""CI invariants for the exact upstream clio-kit release wheel."""

from __future__ import annotations

import hashlib
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)
from tests import clio_kit_test_artifacts

# The bootstrap default install pin (clio-relay#190): the exact clio-kit
# release a *fresh* deployment installs for its built-in JARVIS MCP server.
# This is the runtime dependency pin, not a certification snapshot: it moves
# independently of CONTRACT_CERTIFICATION_WHEEL_* below. Kept in sync with
# clio_relay.jarvis_mcp.CLIO_KIT_JARVIS_MCP_VERSION by hand (release 1.6.8,
# chore(release) c0ec01f) -- this file intentionally re-derives its own
# copies rather than importing the wheel filename/URL/SHA-256 so a source
# bump can never silently drag the CI-workflow-consistency check along
# unnoticed; this test is the tripwire that forces the two to be reconciled.
# 2.10.5 -> 2.10.6 (v1.7.0 upstream wave): pin-carrier only, no contract
# regeneration -- clio-kit's own generator --check reported the 2.10.6
# MCP contract artifacts byte-identical to 2.10.5.
JARVIS_MCP_WHEEL_FILENAME = "clio_kit-2.10.6-py3-none-any.whl"
JARVIS_MCP_WHEEL_SHA256 = "bfbdba1252de18daf4954399e8b973143a709d834109c294661d859dae30353d"
JARVIS_MCP_WHEEL_URL = (
    f"https://github.com/iowarp/clio-kit/releases/download/v2.10.6/{JARVIS_MCP_WHEEL_FILENAME}"
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
# test_clio_kit_mcp_contracts.py passes against this exact wheel). 2.10.5 ->
# 2.10.6 (v1.7.0 upstream wave) is a single-wheel-download version move
# only -- byte-identical contract artifacts, confirmed by clio-kit's own
# generator --check -- so no EXPECTED_CONTRACTS repair is required.


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
    assert CLIO_KIT_JARVIS_MCP_VERSION == "2.10.6"
    assert CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME == JARVIS_MCP_WHEEL_FILENAME
    assert CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 == JARVIS_MCP_WHEEL_SHA256
    assert CLIO_KIT_JARVIS_MCP_WHEEL_URL == JARVIS_MCP_WHEEL_URL
