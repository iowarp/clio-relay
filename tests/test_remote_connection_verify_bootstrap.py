"""Evidence-class-aware channel bootstrap verification (E3 live-proven defect).

The brokered transports fetch the live API's ``/session-status`` self-report
(``evidence: live_api_self_report``), which honestly omits the cluster-local
``ownership_verified`` audit fact only the ssh-carried status executor can
produce. ``verify_bootstrap`` used to demand that fact unconditionally, so a
brokered attach could NEVER verify -- proven live against the homelab
deployment (frps + stcp visitor up, identity challenge passed, then the
aggregate refusal). These tests pin the fix: the self-report class passes
without the audit fact, every identity fact stays demanded of both classes,
and the refusal names exactly which check(s) failed.
"""

from __future__ import annotations

import pytest

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.control_channel import OwnedSessionChannelBootstrap
from clio_relay.errors import RelayError
from clio_relay.remote_connection_registry import verify_bootstrap

CLUSTER = "homelab"
SESSION_ID = "session-1"
GENERATION_ID = "generation-1"
API_PORT = 8765


def _definition() -> ClusterDefinition:
    return ClusterDefinition(name=CLUSTER, ssh_host=CLUSTER)


def _status(**overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "owner": "clio-relay",
        "cluster": CLUSTER,
        "session_id": SESSION_ID,
        "session_generation_id": GENERATION_ID,
        "remote_api_port": API_PORT,
        "running": True,
    }
    document.update(overrides)
    return document


def _verify(status: dict[str, object]) -> None:
    verify_bootstrap(
        OwnedSessionChannelBootstrap.model_validate({"status": status, "identity": {}}),
        definition=_definition(),
        session_id=SESSION_ID,
        generation_id=GENERATION_ID,
        remote_api_port=API_PORT,
    )


class TestSshAuditEvidence:
    """Documents without the self-report marker keep the full audit demand."""

    def test_audited_document_passes(self) -> None:
        _verify(_status(ownership_verified=True))

    def test_missing_ownership_audit_is_refused_and_named(self) -> None:
        with pytest.raises(RelayError, match=r"failed check\(s\): ownership_verified"):
            _verify(_status())

    def test_false_ownership_audit_is_refused(self) -> None:
        with pytest.raises(RelayError, match="ownership_verified"):
            _verify(_status(ownership_verified=False))

    def test_unknown_evidence_class_keeps_the_audit_demand(self) -> None:
        # Only the exact self-report marker relaxes the audit fact; anything
        # else (a typo, a future class) fails closed.
        with pytest.raises(RelayError, match="ownership_verified"):
            _verify(_status(evidence="some_future_evidence"))


class TestLiveApiSelfReportEvidence:
    """The brokered modes' self-report class: no audit fact demanded."""

    def test_self_report_without_audit_fact_passes(self) -> None:
        _verify(_status(evidence="live_api_self_report"))

    def test_self_report_with_wrong_generation_is_refused(self) -> None:
        with pytest.raises(RelayError, match="session_generation_id"):
            _verify(
                _status(
                    evidence="live_api_self_report",
                    session_generation_id="stale-generation",
                )
            )

    def test_self_report_with_wrong_session_is_refused(self) -> None:
        with pytest.raises(RelayError, match="session_id"):
            _verify(_status(evidence="live_api_self_report", session_id="other-session"))

    def test_self_report_not_running_is_refused(self) -> None:
        with pytest.raises(RelayError, match="running"):
            _verify(_status(evidence="live_api_self_report", running=False))

    def test_self_report_with_bogus_port_is_refused(self) -> None:
        with pytest.raises(RelayError, match="remote_api_port"):
            _verify(_status(evidence="live_api_self_report", remote_api_port=True))


class TestRefusalNamesEveryFailedCheck:
    def test_multiple_failures_are_all_named(self) -> None:
        with pytest.raises(RelayError, match="cluster, session_id"):
            _verify(_status(cluster="elsewhere", session_id="other-session"))
