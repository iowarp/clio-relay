"""Tests for the owned-session wire-model cluster (#231 R8(iii)).

Two concerns:

1. **Extraction seam** -- ``clio_relay.session_wire_models`` is the owner
   module; ``clio_relay.session_lifecycle`` must re-export every moved name
   under an identical binding (proven by identity, not structural equality)
   so existing callers, tests, and ``session_lifecycle.<Symbol>`` monkeypatch
   seams (the R8(i) lesson) keep resolving to the *same* class after the
   move.
2. **Model behavior** -- focused unit coverage for the 17 types' own
   validators and methods. Before this slice, none of this existed anywhere
   in the suite: every one of these types was exercised only indirectly,
   as a data carrier passed through ``session_lifecycle.py``'s state-machine
   functions and ``cli.py``'s command tests. Those tests correctly stay
   where they are -- they test the state machine, not the wire types -- so
   this file adds net-new coverage for the wire contracts themselves rather
   than relocating anything.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

import clio_relay.session_lifecycle as session_lifecycle
import clio_relay.session_wire_models as session_wire_models
from clio_relay.session_wire_models import (
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
    MAX_SESSION_START_ERROR_CHARS,
    CleanupResource,
    OwnedSessionCleanupReportReference,
    OwnedSessionCleanupTarget,
    OwnedSessionIdentityChallengeRequest,
    OwnedSessionInputPolicy,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartPlan,
    OwnedSessionStartReceipt,
    OwnedSessionStartRejection,
    OwnedSessionStartRequest,
    OwnedSessionStartResult,
    OwnedSessionStartRetrySelector,
    OwnedSessionStartStatusSelector,
    OwnedSessionTeardownRequest,
    RemoteSession,
    RemoteSessionStateEvidence,
    SessionApiReleaseIdentity,
)
from clio_relay.validation_report import SoftwareIdentity, ValidationResource

# -- Extraction seam ---------------------------------------------------------

# RemoteSession is not re-exported by session_lifecycle.py: nothing in that
# module (or anywhere else in the tree) references it -- ruff's F401
# confirms it, and re-exporting a name nothing consumes is dead weight, not
# compatibility. It is still defined and tested directly against
# session_wire_models below (test_remote_session_is_a_frozen_dataclass_record).
#
# OwnedSessionCleanupTarget stopped being re-exported in the
# split/session-lifecycle rework (#231): its only session_lifecycle.py
# consumers (_inspect_owned_session_cleanup_receipt,
# _inspect_owned_session_failed_cleaned_receipt) moved to
# session_recovery_cleanup_receipt.py / session_recovery_cleaned_receipt.py,
# which import it directly from session_wire_models -- ruff's F401 confirmed
# it, same reasoning as RemoteSession above.
#
# OwnedSessionStartRejection stopped being re-exported in the same rework:
# its only session_lifecycle.py consumer, _ssh_script's rejection-response
# parsing, moved to session_remote_scripts.py, which imports it directly
# from session_wire_models.
_MOVED_SYMBOLS = [
    "SessionApiReleaseIdentity",
    "OwnedSessionInputPolicy",
    "OwnedSessionStartRequest",
    "OwnedSessionStartStatusSelector",
    "OwnedSessionStartRetrySelector",
    "OwnedSessionTeardownRequest",
    "OwnedSessionIdentityChallengeRequest",
    "CleanupResource",
    "RemoteSessionStateEvidence",
    "OwnedSessionCleanupReportReference",
    "OwnedSessionRecoveryStatus",
    "OwnedSessionStartResult",
    "OwnedSessionStartReceipt",
    "OwnedSessionStartPlan",
]


@pytest.mark.parametrize("name", _MOVED_SYMBOLS)
def test_session_lifecycle_reexports_the_identical_class(name: str) -> None:
    """session_lifecycle.<Symbol> is the *same object* as the owner module's.

    A duplicate redefinition (two distinct classes with the same name) would
    pass a naive `is not None` check but silently diverge -- `isinstance`
    checks and `model_validate` calls against one would reject instances of
    the other. Identity is the only proof that the re-export is real.
    """
    owner_class = getattr(session_wire_models, name)
    reexported = getattr(session_lifecycle, name)
    assert reexported is owner_class


def test_session_lifecycle_reexports_the_bound_constants() -> None:
    """The two Field()-bound constants re-export identically too."""
    assert session_lifecycle.MAX_SESSION_START_ERROR_CHARS is (
        session_wire_models.MAX_SESSION_START_ERROR_CHARS
    )
    assert session_lifecycle.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES is (
        session_wire_models.MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES
    )


def test_session_lifecycle_monkeypatch_seam_still_bites(monkeypatch: MonkeyPatch) -> None:
    """The R8(i) lesson: `session_lifecycle.<Symbol>` must stay patchable.

    session_lifecycle.py's own code refers to these names as bare globals
    (e.g. ``OwnedSessionStartReceipt(...)``), which Python resolves from the
    module's namespace at call time. Patching that namespace slot must still
    redirect callers inside session_lifecycle.py to the fake, exactly as it
    did before the move.
    """

    sentinel = object()

    class FakeReceipt:
        def __init__(self, **_kwargs: object) -> None:
            self.sentinel = sentinel

    monkeypatch.setattr(session_lifecycle, "OwnedSessionStartReceipt", FakeReceipt)

    # session_lifecycle.py's own top-level binding is what a bare
    # `OwnedSessionStartReceipt(...)` call inside that module would resolve
    # -- assert the patched slot, not a fresh import, mirrors what the real
    # call sites see. The patched attribute is statically still typed as the
    # real pydantic model, so the fake's arbitrary-kwargs constructor needs
    # an explicit Any cast here (house style, e.g. tests/test_cli.py:5048).
    patched = cast(Any, session_lifecycle.OwnedSessionStartReceipt)
    constructed = patched(unrelated="ignored")
    assert isinstance(constructed, FakeReceipt)
    assert constructed.sentinel is sentinel


# -- SessionApiReleaseIdentity ------------------------------------------------


def _software_identity() -> SoftwareIdentity:
    return SoftwareIdentity(version="1.2.3", commit="a" * 40, tag="v1.2.3", dirty=False)


def test_release_identity_canonical_json_is_deterministic_and_sorted() -> None:
    identity = SessionApiReleaseIdentity(
        distribution_version="1.2.3",
        artifact_sha256="b" * 64,
        software=_software_identity(),
    )
    first = identity.canonical_json()
    second = identity.canonical_json()
    assert first == second
    # Canonical form is compact (no whitespace after separators) and sorted.
    assert ", " not in first
    assert '"artifact_sha256"' in first
    assert first.index('"artifact_sha256"') < first.index('"distribution_version"')


def test_release_identity_sha256_matches_manual_digest() -> None:
    import hashlib

    identity = SessionApiReleaseIdentity(
        distribution_version="1.2.3",
        artifact_sha256="c" * 64,
        software=_software_identity(),
    )
    expected = hashlib.sha256(identity.canonical_json().encode("utf-8")).hexdigest()
    assert identity.sha256() == expected


def test_release_identity_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        SessionApiReleaseIdentity.model_validate(
            {
                "distribution_version": "1.2.3",
                "artifact_sha256": "d" * 64,
                "software": _software_identity().model_dump(),
                "unexpected": "field",
            }
        )


def test_release_identity_rejects_malformed_sha256() -> None:
    with pytest.raises(ValidationError):
        SessionApiReleaseIdentity(
            distribution_version="1.2.3",
            artifact_sha256="not-hex",
            software=_software_identity(),
        )


# -- OwnedSessionInputPolicy ---------------------------------------------------


def test_input_policy_defaults_are_internally_consistent() -> None:
    policy = OwnedSessionInputPolicy()
    assert policy.total_max_bytes >= policy.file_max_bytes


def test_input_policy_rejects_total_smaller_than_file_max() -> None:
    with pytest.raises(ValidationError, match="total_max_bytes must cover file_max_bytes"):
        OwnedSessionInputPolicy(file_max_bytes=1000, total_max_bytes=999)


def test_input_policy_is_frozen() -> None:
    policy = OwnedSessionInputPolicy()
    with pytest.raises(ValidationError):
        policy.file_max_count = 5  # type: ignore[misc]


def test_input_policy_environment_projects_exact_keys() -> None:
    policy = OwnedSessionInputPolicy(file_max_bytes=10, total_max_bytes=20, file_max_count=3)
    assert policy.environment() == {
        "CLIO_RELAY_INPUT_FILE_MAX_BYTES": "10",
        "CLIO_RELAY_INPUT_TOTAL_MAX_BYTES": "20",
        "CLIO_RELAY_INPUT_FILE_MAX_COUNT": "3",
    }


# -- OwnedSessionCleanupTarget -------------------------------------------------


def test_cleanup_target_absent_is_complete_with_no_identity_fields() -> None:
    target = OwnedSessionCleanupTarget(name="a.txt", present=False)
    assert target.identity_is_complete()


def test_cleanup_target_absent_with_a_stray_identity_field_is_incomplete() -> None:
    target = OwnedSessionCleanupTarget(name="a.txt", present=False, size=0)
    assert not target.identity_is_complete()


def test_cleanup_target_present_inode_mode_requires_no_sha256() -> None:
    target = OwnedSessionCleanupTarget(
        name="a.txt",
        present=True,
        device=1,
        inode=2,
        size=3,
        identity_mode="inode",
    )
    assert target.identity_is_complete()


def test_cleanup_target_present_inode_mode_with_sha256_is_incomplete() -> None:
    target = OwnedSessionCleanupTarget(
        name="a.txt",
        present=True,
        device=1,
        inode=2,
        size=3,
        sha256="e" * 64,
        identity_mode="inode",
    )
    assert not target.identity_is_complete()


def test_cleanup_target_present_content_sha256_mode_requires_sha256() -> None:
    target = OwnedSessionCleanupTarget(
        name="a.txt",
        present=True,
        device=1,
        inode=2,
        size=3,
        sha256="f" * 64,
        identity_mode="content_sha256",
    )
    assert target.identity_is_complete()


def test_cleanup_target_present_missing_stat_fields_is_incomplete() -> None:
    target = OwnedSessionCleanupTarget(name="a.txt", present=True)
    assert not target.identity_is_complete()


# -- CleanupResource -----------------------------------------------------------


def test_cleanup_resource_maps_known_kind_and_carries_metadata() -> None:
    resource = CleanupResource(
        kind="remote_relay_api",
        resource_id="sess-1",
        location="cluster:port",
        action="stop",
        ownership_verified=True,
        outcome="stopped",
        provider="ssh",
        metadata={"extra": 1},
    )
    validation_resource = resource.to_validation_resource(cluster="cluster-a")
    assert isinstance(validation_resource, ValidationResource)
    assert validation_resource.kind == "relay_session"
    assert validation_resource.resource_id == "sess-1"
    assert validation_resource.role == "remote_relay_api:stop"
    assert validation_resource.cluster == "cluster-a"
    assert validation_resource.state == "stopped"
    assert validation_resource.references == ["cluster:port"]
    assert validation_resource.metadata["extra"] == 1
    assert validation_resource.metadata["ownership_verified"] is True


def test_cleanup_resource_unknown_kind_passes_through_unmapped() -> None:
    resource = CleanupResource(
        kind="some_future_kind",
        resource_id="r-1",
        location="loc",
        action="retain",
        ownership_verified=False,
        outcome="retained",
    )
    validation_resource = resource.to_validation_resource(cluster=None)
    assert validation_resource.kind == "some_future_kind"
    assert validation_resource.cluster is None


# -- OwnedSessionRecoveryStatus -------------------------------------------------


def test_recovery_status_requires_matching_report_digest() -> None:
    report_ref = OwnedSessionCleanupReportReference(
        name="coordinator-cleanup-report-" + "0" * 64 + ".json",
        size=10,
        sha256="1" * 64,
    )
    with pytest.raises(ValidationError, match="does not match status"):
        OwnedSessionRecoveryStatus(
            cluster="c",
            session_id="s",
            coordinator_report_ref=report_ref,
            coordinator_report_sha256="2" * 64,
        )


def test_recovery_status_accepts_matching_report_digest() -> None:
    report_ref = OwnedSessionCleanupReportReference(
        name="coordinator-cleanup-report-" + "0" * 64 + ".json",
        size=10,
        sha256="1" * 64,
    )
    status = OwnedSessionRecoveryStatus(
        cluster="c",
        session_id="s",
        coordinator_report_ref=report_ref,
        coordinator_report_sha256="1" * 64,
    )
    assert status.coordinator_report_sha256 == report_ref.sha256


def test_recovery_status_start_error_is_bounded_by_the_shared_constant() -> None:
    with pytest.raises(ValidationError):
        OwnedSessionRecoveryStatus(
            cluster="c",
            session_id="s",
            start_error="x" * (MAX_SESSION_START_ERROR_CHARS + 1),
        )


# -- OwnedSessionStartReceipt ---------------------------------------------------


def _receipt_kwargs(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "cluster": "c",
        "session_id": "s",
        "start_operation_id": "op-1",
        "cluster_route_revision": "rev-1",
        "session_generation_id": "gen-1",
        "remote_api_port": 8080,
        "api_pid": 100,
        "outcome": "started",
        "ready_seconds": 1.5,
    }
    base.update(overrides)
    return base


def test_start_receipt_requires_ready_seconds_when_not_already_running() -> None:
    with pytest.raises(ValidationError, match="ready_seconds is required"):
        OwnedSessionStartReceipt.model_validate(
            _receipt_kwargs(outcome="started", ready_seconds=None)
        )


def test_start_receipt_forbids_ready_seconds_when_already_running() -> None:
    with pytest.raises(ValidationError, match="ready_seconds must be absent"):
        OwnedSessionStartReceipt.model_validate(
            _receipt_kwargs(outcome="already_running", ready_seconds=1.0)
        )


def test_start_receipt_already_running_without_ready_seconds_is_valid() -> None:
    kwargs = _receipt_kwargs(outcome="already_running", ready_seconds=None)
    receipt = OwnedSessionStartReceipt.model_validate(kwargs)
    assert receipt.outcome == "already_running"
    assert receipt.running is True
    assert receipt.ownership_verified is True
    assert receipt.recovery_verified is True


# -- OwnedSessionStartPlan / OwnedSessionStartResult (cross-model identity) ----


def _selector_pair(
    **overrides: object,
) -> tuple[OwnedSessionStartStatusSelector, OwnedSessionStartRetrySelector]:
    base: dict[str, object] = {
        "cluster": "c",
        "session_id": "s",
        "start_operation_id": "op-1",
        "cluster_route_revision": "rev-1",
        "remote_api_port": 8080,
        "replace": False,
        "require_token": True,
    }
    base.update(overrides)
    status = OwnedSessionStartStatusSelector.model_validate(base)
    retry = OwnedSessionStartRetrySelector.model_validate(base)
    return status, retry


def test_start_plan_rejects_selectors_with_a_different_operation_id() -> None:
    status, retry = _selector_pair()
    mismatched_retry = retry.model_copy(update={"start_operation_id": "op-2"})
    with pytest.raises(ValidationError, match="changed identity"):
        OwnedSessionStartPlan(
            cluster="c",
            session_id="s",
            start_operation_id="op-1",
            cluster_route_revision="rev-1",
            remote_api_port=8080,
            status_selector=status,
            retry_selector=mismatched_retry,
        )


def test_start_plan_accepts_matching_selectors() -> None:
    status, retry = _selector_pair()
    plan = OwnedSessionStartPlan(
        cluster="c",
        session_id="s",
        start_operation_id="op-1",
        cluster_route_revision="rev-1",
        remote_api_port=8080,
        status_selector=status,
        retry_selector=retry,
    )
    assert plan.status_selector == status
    assert plan.retry_selector == retry


def _ready_result_kwargs(
    status: OwnedSessionStartStatusSelector,
    retry: OwnedSessionStartRetrySelector,
    **overrides: object,
) -> dict[str, object]:
    base: dict[str, object] = {
        "cluster": "c",
        "session_id": "s",
        "start_operation_id": "op-1",
        "cluster_route_revision": "rev-1",
        "session_generation_id": "gen-1",
        "remote_api_port": 8080,
        "state": "ready",
        "terminal": True,
        "retryable": False,
        "usable": True,
        "transition_accepted": True,
        "transport_deadline_exceeded": False,
        "running": True,
        "ownership_verified": True,
        "recovery_verified": True,
        "status_selector": status,
        "retry_selector": retry,
    }
    base.update(overrides)
    return base


def test_start_result_ready_state_requires_full_completion_evidence() -> None:
    status, retry = _selector_pair()
    with pytest.raises(ValidationError, match="is incomplete"):
        OwnedSessionStartResult.model_validate(
            _ready_result_kwargs(status, retry, ownership_verified=False),
        )


def test_start_result_ready_state_accepts_complete_evidence() -> None:
    status, retry = _selector_pair()
    result = OwnedSessionStartResult.model_validate(_ready_result_kwargs(status, retry))
    assert result.state == "ready"
    assert result.usable is True


def test_start_result_usable_must_match_ready_state() -> None:
    status, retry = _selector_pair()
    with pytest.raises(ValidationError, match="only a ready.*is usable"):
        OwnedSessionStartResult.model_validate(_ready_result_kwargs(status, retry, usable=False))


def test_start_result_rejects_a_selector_that_changed_result_identity() -> None:
    _status, retry = _selector_pair()
    other_status, _retry = _selector_pair(session_id="different")
    with pytest.raises(ValidationError, match="changed result identity"):
        OwnedSessionStartResult.model_validate(_ready_result_kwargs(other_status, retry))


# -- RemoteSession / OwnedSessionIdentityChallengeRequest / RemoteSessionStateEvidence --


def test_remote_session_is_a_frozen_dataclass_record() -> None:
    session = RemoteSession(session_id="s", remote_api_port=8080, api_token="tok")
    assert session.session_id == "s"
    with pytest.raises(Exception):  # noqa: B017 - dataclasses.FrozenInstanceError
        session.session_id = "other"  # type: ignore[misc]


def test_identity_challenge_request_requires_hex_nonce() -> None:
    with pytest.raises(ValidationError):
        OwnedSessionIdentityChallengeRequest(
            cluster="c",
            session_id="s",
            session_generation_id="gen-1",
            nonce="not-hex",
        )
    challenge = OwnedSessionIdentityChallengeRequest(
        cluster="c",
        session_id="s",
        session_generation_id="gen-1",
        nonce="a" * 64,
    )
    assert challenge.nonce == "a" * 64


def test_remote_session_state_evidence_requires_observed_at() -> None:
    with pytest.raises(ValidationError):
        RemoteSessionStateEvidence.model_validate({"running": True, "ownership_verified": True})
    evidence = RemoteSessionStateEvidence(
        running=True,
        ownership_verified=True,
        observed_at=datetime.now(UTC),
    )
    assert evidence.started_at is None


# -- Field bound constants themselves ------------------------------------------


def test_cleanup_report_reference_size_bounded_by_shared_constant() -> None:
    with pytest.raises(ValidationError):
        OwnedSessionCleanupReportReference(
            name="coordinator-cleanup-report-" + "0" * 64 + ".json",
            size=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + 1,
            sha256="1" * 64,
        )
    reference = OwnedSessionCleanupReportReference(
        name="coordinator-cleanup-report-" + "0" * 64 + ".json",
        size=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        sha256="1" * 64,
    )
    assert reference.size == MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES


# -- Stdin-contract request models (structural smoke) ---------------------------


def test_start_request_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        OwnedSessionStartRequest.model_validate(
            {
                "cluster": "c",
                "session_id": "s",
                "start_operation_id": "op-1",
                "remote_api_port": 8080,
                "cluster_registry": {},
                "cluster_registry_sha256": "1" * 64,
                "cluster_route_revision": "rev-1",
                "unexpected": True,
            }
        )


def test_teardown_request_defaults() -> None:
    request = OwnedSessionTeardownRequest(
        cluster="c",
        session_id="s",
        expected_session_generation_id="gen-1",
        expected_cleanup_operation_id="op-1",
    )
    assert request.stop_worker is False
    assert request.cancel_jobs is False
    assert request.cancel_scheduler_jobs is False


def test_start_rejection_error_is_bounded_by_the_shared_constant() -> None:
    with pytest.raises(ValidationError):
        OwnedSessionStartRejection(
            cluster="c",
            session_id="s",
            start_operation_id="op-1",
            cluster_route_revision="rev-1",
            error="x" * (MAX_SESSION_START_ERROR_CHARS + 1),
        )
