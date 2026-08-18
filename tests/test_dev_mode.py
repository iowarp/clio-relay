"""Tests for the DEV MODE verification-downgrade core (clio-relay#211)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from clio_relay.dev_mode import (
    DEV_MODE_BANNER,
    DEV_MODE_ENV,
    VerificationFindings,
    dev_mode_enabled,
    enforce,
)
from clio_relay.errors import ConfigurationError
from clio_relay.installation import InstallReceipt
from clio_relay.session_install_identity import (
    release_identity_from_receipt,
    release_identity_is_accepted,
)
from clio_relay.validation_report import SoftwareIdentity

#: Modules protecting live state/other tenants -- clio-relay#211 requires these
#: to stay hard regardless of dev mode. Zero coupling to clio_relay.dev_mode is
#: proof by construction: these modules cannot consult a flag they never import.
_HARD_CHECK_MODULES = (
    "worker_lifetime_lock.py",  # exclusive worker lock per cluster
    "storage_policy.py",  # storage admission/reservation limits
    "session_lifecycle.py",  # teardown scoping and cleanup policy
)


def test_hard_check_modules_never_import_dev_mode() -> None:
    """clio-relay#211: writer proof, worker-lifetime lock, storage admission,
    and teardown scoping stay hard -- proven here by zero coupling: none of
    these modules import anything from clio_relay.dev_mode, so none of them
    can possibly consult it to downgrade a check, now or by future accident.
    """
    src_root = Path(__file__).resolve().parents[1] / "src" / "clio_relay"
    for module_name in _HARD_CHECK_MODULES:
        source = (src_root / module_name).read_text(encoding="utf-8")
        assert "dev_mode" not in source, (
            f"{module_name} must never reference dev_mode -- it protects live "
            "state/other tenants and clio-relay#211 requires it stay hard "
            "unconditionally"
        )


def test_session_install_identity_policy_stays_outside_hard_lifecycle_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The startup identity owner retains the advisory development exception."""
    receipt = InstallReceipt(
        installed_at=datetime.now(UTC),
        install_spec="clio-relay==1.0.0",
        requested_source="pypi",
        distribution_version="1.0.0",
        software=SoftwareIdentity(version="1.0.0"),
    )
    monkeypatch.delenv(DEV_MODE_ENV, raising=False)
    with pytest.raises(ConfigurationError, match="identity is incomplete"):
        release_identity_from_receipt(receipt)

    monkeypatch.setenv(DEV_MODE_ENV, "1")
    current = release_identity_from_receipt(receipt)
    expected = current.model_copy(update={"artifact_sha256": "f" * 64})
    assert current.artifact_sha256 == "0" * 64

    monkeypatch.delenv(DEV_MODE_ENV)
    assert release_identity_is_accepted(current, expected) is False
    monkeypatch.setenv(DEV_MODE_ENV, "1")
    assert release_identity_is_accepted(current, expected) is True


def test_dev_mode_enabled_honors_env_and_cluster_flag_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DEV_MODE_ENV, raising=False)
    assert dev_mode_enabled() is False
    assert dev_mode_enabled(cluster_dev_mode=True) is True

    monkeypatch.setenv(DEV_MODE_ENV, "1")
    assert dev_mode_enabled() is True
    assert dev_mode_enabled(cluster_dev_mode=False) is True


@pytest.mark.parametrize("value", ["1", "true", "True", "yes", "on", " 1 "])
def test_dev_mode_env_truthy_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv(DEV_MODE_ENV, value)
    assert dev_mode_enabled() is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "", "   "])
def test_dev_mode_env_falsy_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv(DEV_MODE_ENV, value)
    assert dev_mode_enabled() is False


def test_dev_mode_env_absent_is_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(DEV_MODE_ENV, raising=False)
    assert dev_mode_enabled() is False


def test_enforce_raises_in_production_and_records_in_dev_mode() -> None:
    """The single choke point every downgradable check routes through."""
    findings = VerificationFindings()

    with pytest.raises(ConfigurationError, match="would have failed"):
        enforce(findings, dev_mode=False, condition=False, message="would have failed")
    assert findings.warnings == []

    enforce(findings, dev_mode=True, condition=False, message="would have failed")
    assert findings.warnings == ["would have failed"]

    # a satisfied condition never records or raises, in either mode
    enforce(findings, dev_mode=True, condition=True, message="never seen")
    enforce(findings, dev_mode=False, condition=True, message="never seen")
    assert findings.warnings == ["would have failed"]


def test_verification_findings_payload_is_none_when_clean() -> None:
    findings = VerificationFindings()
    assert findings.payload() is None
    findings.record("something downgraded")
    payload = findings.payload()
    assert payload is not None
    assert payload["dev_mode_banner"] == DEV_MODE_BANNER
    assert payload["dev_mode_warnings"] == ["something downgraded"]


def test_verification_findings_payload_marker_is_unmissable() -> None:
    """clio-relay#211: the banner text itself names DEV MODE explicitly."""
    assert "DEV MODE" in DEV_MODE_BANNER
    findings = VerificationFindings()
    findings.record("x")
    payload = findings.payload()
    assert payload is not None
    assert "DEV MODE" in str(payload["dev_mode_banner"])
