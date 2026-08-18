"""Tests for per-surface MCP contract capability gating (iowarp/clio-relay#242).

Owner doctrine under test: bootstrap-time enumeration is INTEGRITY-only and
per-surface -- a surface shipping a known, digest-verified but below-pin
contract must record a typed degradation and let bootstrap succeed, while a
genuinely unrecognized or tampered response must still refuse outright. The
below-pin surface only refuses later, at USE-time, with a typed error naming
surface/have/need.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

import clio_relay.contract_gate as contract_gate_module
from clio_relay.contract_gate import (
    SurfaceContractDegradation,
    SurfaceContractStatus,
    evaluate_degradation,
    mcp_contract_digest,
    probe_surface_contract_identity,
    require_surface_contract,
)
from clio_relay.dev_mode import DEV_MODE_ENV
from clio_relay.errors import ConfigurationError, ContractSurfaceUnavailableError
from clio_relay.installation import CLIO_KIT_MCP_CONTRACT_SCHEMA
from clio_relay.remote_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_ID,
    CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID,
)

# Reuse the relay's own registered ids/schema (single source of truth,
# release_pin_sites-tracked in remote_mcp.py/installation.py) instead of
# duplicating the literal strings here -- this test exercises the
# NEGOTIATION algorithm, not a second copy of the pin.
CONTRACT_SCHEMA = CLIO_KIT_MCP_CONTRACT_SCHEMA
CURRENT_ID = CLIO_KIT_JARVIS_USER_CONTRACT_ID
LEGACY_ID = CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID

_CURRENT_TOOLS: list[dict[str, object]] = [
    {"name": "jarvis_run", "description": "v3.7", "inputSchema": {}, "outputSchema": {}}
]
_LEGACY_TOOLS: list[dict[str, object]] = [
    {"name": "jarvis_run", "description": "v3.6", "inputSchema": {}, "outputSchema": {}}
]
CURRENT_SHA256 = mcp_contract_digest(_CURRENT_TOOLS)
LEGACY_SHA256 = mcp_contract_digest(_LEGACY_TOOLS)
SHA256_BY_ID = {CURRENT_ID: CURRENT_SHA256, LEGACY_ID: LEGACY_SHA256}


def _document(contract_id: str, tools: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema_version": CONTRACT_SCHEMA,
        "contract_id": contract_id,
        "tools": tools,
    }


def _fake_probe(
    responses: dict[str, dict[str, object] | ConfigurationError],
) -> Callable[..., dict[str, object]]:
    """Build a fake ``run_json_probe`` keyed by the last command argument (the id)."""

    def probe(command: list[str], *, label: str) -> dict[str, object]:
        del label
        contract_id = command[-1]
        response = responses[contract_id]
        if isinstance(response, ConfigurationError):
            raise response
        return response

    return probe


def test_probe_surface_contract_identity_accepts_current_pin_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The common, healthy case: the launcher answers the current pin first try."""
    monkeypatch.setattr(
        contract_gate_module,
        "run_json_probe",
        _fake_probe(
            {
                CURRENT_ID: _document(CURRENT_ID, _CURRENT_TOOLS),
                LEGACY_ID: ConfigurationError("should not be asked"),
            }
        ),
    )
    status = probe_surface_contract_identity(
        ["/opt/clio-kit"],
        surface="jarvis",
        candidate_contract_ids=(CURRENT_ID, LEGACY_ID),
        contract_schema_version=CONTRACT_SCHEMA,
        sha256_by_id=SHA256_BY_ID,
    )
    assert status.surface == "jarvis"
    assert status.shipped_contract_id == CURRENT_ID
    assert status.shipped_contract_sha256 == CURRENT_SHA256
    assert status.required_contract_id == CURRENT_ID
    assert status.meets_requirement is True


def test_probe_surface_contract_identity_negotiates_down_to_known_legacy_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clio-kit only ships v3.6: negotiate down instead of failing bootstrap.

    This is the exact failure shape that blocked the live ares bootstrap
    (iowarp/clio-relay#242): the launcher's own CLI refuses to describe the
    id it does not recognize ("unknown MCP user contract: <id>"), and the
    probe must treat that as "try the next, older candidate" rather than a
    hard failure.
    """
    monkeypatch.setattr(
        contract_gate_module,
        "run_json_probe",
        _fake_probe(
            {
                CURRENT_ID: ConfigurationError(
                    "clio-kit native execution contract failed: "
                    f"unknown MCP user contract: {CURRENT_ID}"
                ),
                LEGACY_ID: _document(LEGACY_ID, _LEGACY_TOOLS),
            }
        ),
    )
    status = probe_surface_contract_identity(
        ["/opt/clio-kit"],
        surface="jarvis",
        candidate_contract_ids=(CURRENT_ID, LEGACY_ID),
        contract_schema_version=CONTRACT_SCHEMA,
        sha256_by_id=SHA256_BY_ID,
    )
    assert status.shipped_contract_id == LEGACY_ID
    assert status.shipped_contract_sha256 == LEGACY_SHA256
    assert status.required_contract_id == CURRENT_ID
    assert status.meets_requirement is False


def test_probe_surface_contract_identity_raises_when_no_candidate_recognized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wheel matching NONE of the known ids is a genuine integrity failure."""
    monkeypatch.setattr(
        contract_gate_module,
        "run_json_probe",
        _fake_probe(
            {
                CURRENT_ID: ConfigurationError(
                    "clio-kit native execution contract failed: "
                    f"unknown MCP user contract: {CURRENT_ID}"
                ),
                LEGACY_ID: ConfigurationError(
                    "clio-kit native execution contract failed: "
                    f"unknown MCP user contract: {LEGACY_ID}"
                ),
            }
        ),
    )
    with pytest.raises(ConfigurationError, match="shipped none of the known contracts"):
        probe_surface_contract_identity(
            ["/opt/clio-kit"],
            surface="jarvis",
            candidate_contract_ids=(CURRENT_ID, LEGACY_ID),
            contract_schema_version=CONTRACT_SCHEMA,
            sha256_by_id=SHA256_BY_ID,
        )


def test_probe_surface_contract_identity_rejects_digest_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recognized id whose tool surface does not match its OWN digest never
    gets downgraded to a degradation -- that is tampering/corruption, not a
    below-pin surface, and integrity pinning stays exact."""
    tampered_tools: list[dict[str, object]] = [
        {"name": "jarvis_run", "description": "TAMPERED", "inputSchema": {}, "outputSchema": {}}
    ]
    monkeypatch.setattr(
        contract_gate_module,
        "run_json_probe",
        _fake_probe({CURRENT_ID: _document(CURRENT_ID, tampered_tools)}),
    )
    with pytest.raises(ConfigurationError, match="digest did not match its"):
        probe_surface_contract_identity(
            ["/opt/clio-kit"],
            surface="jarvis",
            candidate_contract_ids=(CURRENT_ID, LEGACY_ID),
            contract_schema_version=CONTRACT_SCHEMA,
            sha256_by_id=SHA256_BY_ID,
        )


def test_probe_surface_contract_identity_propagates_non_unknown_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real transport failure (not "unknown contract") is never swallowed
    or silently retried against an older candidate."""
    monkeypatch.setattr(
        contract_gate_module,
        "run_json_probe",
        _fake_probe({CURRENT_ID: ConfigurationError("jarvis surface contract failed: timed out")}),
    )
    with pytest.raises(ConfigurationError, match="timed out"):
        probe_surface_contract_identity(
            ["/opt/clio-kit"],
            surface="jarvis",
            candidate_contract_ids=(CURRENT_ID, LEGACY_ID),
            contract_schema_version=CONTRACT_SCHEMA,
            sha256_by_id=SHA256_BY_ID,
        )


def test_probe_surface_contract_identity_requires_at_least_one_candidate() -> None:
    with pytest.raises(ConfigurationError, match="no candidate contract ids"):
        probe_surface_contract_identity(
            ["/opt/clio-kit"],
            surface="jarvis",
            candidate_contract_ids=(),
            contract_schema_version=CONTRACT_SCHEMA,
            sha256_by_id=SHA256_BY_ID,
        )


def test_evaluate_degradation_returns_none_when_meets_requirement() -> None:
    status = SurfaceContractStatus(
        surface="spack",
        shipped_contract_id="clio-kit-spack-user-v2.1",
        shipped_contract_sha256="a" * 64,
        required_contract_id="clio-kit-spack-user-v2.1",
        meets_requirement=True,
    )
    assert evaluate_degradation(status, tracking_issue="iowarp/clio-relay#242") is None


def test_evaluate_degradation_builds_typed_record_when_below_pin() -> None:
    status = SurfaceContractStatus(
        surface="jarvis",
        shipped_contract_id=LEGACY_ID,
        shipped_contract_sha256=LEGACY_SHA256,
        required_contract_id=CURRENT_ID,
        meets_requirement=False,
    )
    detected_at = datetime(2026, 8, 17, tzinfo=UTC)
    degradation = evaluate_degradation(
        status,
        tracking_issue="iowarp/clio-relay#242",
        detected_at=detected_at,
    )
    assert degradation is not None
    assert degradation.surface == "jarvis"
    assert degradation.have == LEGACY_ID
    assert degradation.need == CURRENT_ID
    assert degradation.reason == "contract_surface_below_pin"
    assert degradation.tracking_issue == "iowarp/clio-relay#242"
    assert degradation.detected_at == detected_at
    assert isinstance(degradation, SurfaceContractDegradation)


def test_require_surface_contract_is_noop_when_meets_requirement() -> None:
    status = SurfaceContractStatus(
        surface="spack",
        shipped_contract_id="clio-kit-spack-user-v2.1",
        shipped_contract_sha256="a" * 64,
        required_contract_id="clio-kit-spack-user-v2.1",
        meets_requirement=True,
    )
    require_surface_contract(status)  # must not raise


def _below_pin_jarvis_status() -> SurfaceContractStatus:
    return SurfaceContractStatus(
        surface="jarvis",
        shipped_contract_id=LEGACY_ID,
        shipped_contract_sha256=LEGACY_SHA256,
        required_contract_id=CURRENT_ID,
        meets_requirement=False,
    )


def test_require_surface_contract_raises_typed_error_with_have_need(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enforcing mode (dev mode off, the default): unchanged, still refuses."""
    monkeypatch.delenv(DEV_MODE_ENV, raising=False)
    status = _below_pin_jarvis_status()
    with pytest.raises(ContractSurfaceUnavailableError) as excinfo:
        require_surface_contract(status)
    error = excinfo.value
    assert error.surface == "jarvis"
    assert error.have == LEGACY_ID
    assert error.need == CURRENT_ID
    assert error.reason == "contract_surface_unavailable"
    assert "jarvis" in str(error)
    assert LEGACY_ID in str(error)
    assert CURRENT_ID in str(error)


def test_require_surface_contract_explicit_dev_mode_false_overrides_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit dev_mode=False refuses even when the ambient env is on."""
    monkeypatch.setenv(DEV_MODE_ENV, "1")
    with pytest.raises(ContractSurfaceUnavailableError):
        require_surface_contract(_below_pin_jarvis_status(), dev_mode=False)


def test_require_surface_contract_env_dev_mode_defers_and_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """clio-relay#242 owner ruling: dev mode is LOUD AND NON-BLOCKING.

    The ``CLIO_RELAY_DEV_MODE`` env switch (no explicit ``dev_mode=``
    argument) defers the below-pin refusal instead of raising, and logs the
    same typed surface/have/need record at WARNING with
    ``enforcement="deferred_dev_mode"`` stamped on it -- so the surface
    serves and a security-phase retest can still find exactly what was
    deferred.
    """
    monkeypatch.setenv(DEV_MODE_ENV, "1")
    with caplog.at_level(logging.WARNING, logger=contract_gate_module.__name__):
        require_surface_contract(_below_pin_jarvis_status())  # must not raise
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    assert "DEV MODE" in record.message
    assert "jarvis" in record.message
    assert LEGACY_ID in record.message
    assert CURRENT_ID in record.message
    assert "deferred_dev_mode" in record.message


def test_require_surface_contract_explicit_dev_mode_true_defers_without_env(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A caller-threaded ``dev_mode=True`` (e.g. a cluster registry flag)
    defers identically without the environment switch ever being set."""
    monkeypatch.delenv(DEV_MODE_ENV, raising=False)
    with caplog.at_level(logging.WARNING, logger=contract_gate_module.__name__):
        require_surface_contract(_below_pin_jarvis_status(), dev_mode=True)
    assert any("deferred_dev_mode" in record.message for record in caplog.records)


def test_surface_contract_degradation_enforcement_defaults_enforced() -> None:
    """The bootstrap-time record's new field never changes existing behavior
    unless a caller explicitly stamps a deferral onto one (only the
    USE-time gate does, and only when it actually defers)."""
    status = _below_pin_jarvis_status()
    degradation = evaluate_degradation(status, tracking_issue="iowarp/clio-relay#242")
    assert degradation is not None
    assert degradation.enforcement == "enforced"
