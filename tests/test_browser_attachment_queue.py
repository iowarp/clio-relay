from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import clio_relay.service_runtime as service_runtime
from clio_relay.browser_gateway import BrowserAttachmentRecord
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import GatewaySession, GatewaySessionState, utc_now


def test_concurrent_browser_attachment_prepare_reserves_exactly_one_slot(
    tmp_path: Path,
) -> None:
    queue, session = _owned_ready_gateway(tmp_path)
    attachments = (_attachment("browser-a", 28781), _attachment("browser-b", 28782))
    barrier = threading.Barrier(2)

    def prepare(attachment: BrowserAttachmentRecord) -> str:
        barrier.wait()
        prepared = queue.prepare_gateway_browser_attachment(
            session.session_id,
            attachment=attachment,
            browser_proxy_intent=_intent("starting", attachment.attachment_id),
        )
        return prepared.session_id

    outcomes: list[str | Exception] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(prepare, attachment) for attachment in attachments]
        for future in futures:
            try:
                outcomes.append(future.result())
            except Exception as exc:  # deterministic assertion records both contenders
                outcomes.append(exc)

    assert sum(isinstance(outcome, str) for outcome in outcomes) == 1
    conflicts = [outcome for outcome in outcomes if isinstance(outcome, QueueConflictError)]
    assert len(conflicts) == 1
    persisted = queue.get_gateway_session(session.session_id)
    record = BrowserAttachmentRecord.model_validate(persisted.gateway["browser_attachment"])
    assert record.state == "starting"
    assert record.attachment_id in {attachment.attachment_id for attachment in attachments}


def test_browser_attachment_transitions_preserve_latest_gateway_and_teardown(
    tmp_path: Path,
) -> None:
    queue, original = _owned_ready_gateway(tmp_path)
    starting = _attachment("browser-a", 28781)
    prepared = queue.prepare_gateway_browser_attachment(
        original.session_id,
        attachment=starting,
        browser_proxy_intent=_intent("starting", starting.attachment_id),
    )
    queue.update_gateway_session(
        original.session_id,
        gateway={**prepared.gateway, "unrelated_runtime_revision": 7},
        metadata={"concurrent_metadata": "retained"},
    )
    active = starting.model_copy(update={"state": "active", "proxy_process_id": 701})
    completed = queue.complete_gateway_browser_attachment(
        original.session_id,
        attachment=active,
        browser_proxy=_proxy(active.attachment_id, 701),
        browser_proxy_intent=_intent("recorded", active.attachment_id, pid=701),
    )

    assert completed.gateway["unrelated_runtime_revision"] == 7
    assert completed.metadata["concurrent_metadata"] == "retained"
    with_teardown = queue.prepare_gateway_teardown_intent(
        original.session_id,
        cancel_scheduler_job=False,
    )
    teardown_intent = with_teardown.gateway["teardown_intent"]
    revoking = queue.begin_gateway_browser_attachment_revoke(
        original.session_id,
        attachment_id=active.attachment_id,
    )
    assert revoking.gateway["teardown_intent"] == teardown_intent
    revoked = active.model_copy(update={"state": "revoked", "revoked_at": utc_now().isoformat()})
    finished = queue.finish_gateway_browser_attachment_revoke(
        original.session_id,
        attachment=revoked,
        browser_proxy_absent_intent=_intent(
            "absent_verified",
            active.attachment_id,
            pid=701,
        ),
        metadata={"browser_detached_at": revoked.revoked_at},
    )

    assert finished.gateway["teardown_intent"] == teardown_intent
    assert finished.gateway["unrelated_runtime_revision"] == 7
    assert finished.metadata["concurrent_metadata"] == "retained"
    assert finished.metadata["browser_detached_at"] == revoked.revoked_at
    assert "browser_proxy" not in finished.gateway["transport"]
    assert finished.gateway["ownership_intents"]["browser_proxy"]["state"] == ("absent_verified")


def test_teardown_and_exact_attachment_identity_reject_stale_attach_completion(
    tmp_path: Path,
) -> None:
    queue, session = _owned_ready_gateway(tmp_path)
    starting = _attachment("browser-a", 28781)
    queue.prepare_gateway_browser_attachment(
        session.session_id,
        attachment=starting,
        browser_proxy_intent=_intent("starting", starting.attachment_id),
    )
    teardown = queue.prepare_gateway_teardown_intent(
        session.session_id,
        cancel_scheduler_job=False,
    )
    active = starting.model_copy(update={"state": "active", "proxy_process_id": 701})

    with pytest.raises(QueueConflictError, match="committed to teardown"):
        queue.complete_gateway_browser_attachment(
            session.session_id,
            attachment=active,
            browser_proxy=_proxy(active.attachment_id, 701),
            browser_proxy_intent=_intent("recorded", active.attachment_id, pid=701),
        )
    with pytest.raises(QueueConflictError, match="committed to teardown"):
        queue.prepare_gateway_browser_attachment(
            session.session_id,
            attachment=_attachment("browser-b", 28782),
            browser_proxy_intent=_intent("starting", "browser-b"),
        )
    with pytest.raises(QueueConflictError, match="changed before revocation"):
        queue.begin_gateway_browser_attachment_revoke(
            session.session_id,
            attachment_id="browser-b",
        )

    latest = queue.get_gateway_session(session.session_id)
    assert latest.gateway["teardown_intent"] == teardown.gateway["teardown_intent"]
    assert (
        BrowserAttachmentRecord.model_validate(latest.gateway["browser_attachment"]).state
        == "starting"
    )


def test_failed_browser_revoke_keeps_owned_proxy_for_retry(
    tmp_path: Path,
) -> None:
    queue, session = _owned_ready_gateway(tmp_path)
    starting = _attachment("browser-a", 28781)
    queue.prepare_gateway_browser_attachment(
        session.session_id,
        attachment=starting,
        browser_proxy_intent=_intent("starting", starting.attachment_id),
    )
    active = starting.model_copy(update={"state": "active", "proxy_process_id": 701})
    queue.complete_gateway_browser_attachment(
        session.session_id,
        attachment=active,
        browser_proxy=_proxy(active.attachment_id, 701),
        browser_proxy_intent=_intent("recorded", active.attachment_id, pid=701),
    )
    queue.begin_gateway_browser_attachment_revoke(
        session.session_id,
        attachment_id=active.attachment_id,
    )
    failed = active.model_copy(update={"state": "failed"})
    persisted = queue.finish_gateway_browser_attachment_revoke(
        session.session_id,
        attachment=failed,
        metadata={"browser_detach_error": "process still present"},
    )

    assert persisted.gateway["transport"]["browser_proxy"]["pid"] == 701
    assert persisted.gateway["ownership_intents"]["browser_proxy"]["state"] == "recorded"
    assert (
        BrowserAttachmentRecord.model_validate(persisted.gateway["browser_attachment"]).state
        == "failed"
    )
    assert persisted.metadata["browser_detach_error"] == "process still present"


def test_concurrent_revoke_finish_is_idempotent_for_the_exact_attachment(
    tmp_path: Path,
) -> None:
    queue, session = _owned_ready_gateway(tmp_path)
    starting = _attachment("browser-a", 28781)
    queue.prepare_gateway_browser_attachment(
        session.session_id,
        attachment=starting,
        browser_proxy_intent=_intent("starting", starting.attachment_id),
    )
    active = starting.model_copy(update={"state": "active", "proxy_process_id": 701})
    queue.complete_gateway_browser_attachment(
        session.session_id,
        attachment=active,
        browser_proxy=_proxy(active.attachment_id, 701),
        browser_proxy_intent=_intent("recorded", active.attachment_id, pid=701),
    )
    first = queue.begin_gateway_browser_attachment_revoke(
        session.session_id,
        attachment_id=active.attachment_id,
    )
    second = queue.begin_gateway_browser_attachment_revoke(
        session.session_id,
        attachment_id=active.attachment_id,
    )
    assert first.gateway["browser_attachment"] == second.gateway["browser_attachment"]
    first_revoked = active.model_copy(
        update={"state": "revoked", "revoked_at": "2026-07-15T20:10:00+00:00"}
    )
    second_revoked = active.model_copy(
        update={"state": "revoked", "revoked_at": "2026-07-15T20:11:00+00:00"}
    )
    absent = _intent("absent_verified", active.attachment_id, pid=701)

    persisted = queue.finish_gateway_browser_attachment_revoke(
        session.session_id,
        attachment=first_revoked,
        browser_proxy_absent_intent=absent,
    )
    retried = queue.finish_gateway_browser_attachment_revoke(
        session.session_id,
        attachment=second_revoked,
        browser_proxy_absent_intent=absent,
    )

    assert retried == persisted
    assert (
        BrowserAttachmentRecord.model_validate(retried.gateway["browser_attachment"]).revoked_at
        == first_revoked.revoked_at
    )
    with pytest.raises(QueueConflictError, match="cannot be reused"):
        queue.prepare_gateway_browser_attachment(
            session.session_id,
            attachment=starting,
            browser_proxy_intent=_intent("starting", starting.attachment_id),
        )


def test_parallel_revocation_marker_writes_use_unique_staging_files(tmp_path: Path) -> None:
    marker = tmp_path / "browser-a.revoked"
    barrier = threading.Barrier(2)

    def revoke() -> None:
        barrier.wait()
        service_runtime._write_browser_revocation_marker(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            marker,
            "browser-a",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(revoke) for _ in range(2)]
        for future in futures:
            future.result()

    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["attachment_id"] == "browser-a"
    assert list(tmp_path.glob("*.tmp")) == []


def _owned_ready_gateway(tmp_path: Path) -> tuple[ClioCoreQueue, GatewaySession]:
    queue = ClioCoreQueue(tmp_path / "core")
    session = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="paraview",
            state=GatewaySessionState.READY,
            gateway={
                "runtime_spec": {"deployment_driver": "jarvis-bound"},
                "jarvis_runtime_binding": {"schema_version": "binding"},
                "transport": {"desktop_connector": {"pid": 600}},
                "ownership_intents": {
                    "desktop_connector": {
                        "schema_version": "clio-relay.gateway-ownership-intent.v1",
                        "state": "recorded",
                    }
                },
            },
            metadata={"owner": "clio-relay"},
        )
    )
    return queue, session


def _attachment(attachment_id: str, port: int) -> BrowserAttachmentRecord:
    return BrowserAttachmentRecord(
        attachment_id=attachment_id,
        state="starting",
        issued_at="2026-07-15T20:00:00+00:00",
        expires_at="2026-07-15T20:30:00+00:00",
        token_sha256="a" * 64,
        bind_port=port,
        revocation_path=f"C:/runtime/{attachment_id}.revoked",
    )


def _intent(
    state: str,
    attachment_id: str,
    *,
    pid: int | None = None,
) -> dict[str, object]:
    intent: dict[str, object] = {
        "schema_version": "clio-relay.gateway-ownership-intent.v1",
        "state": state,
        "updated_at": "2026-07-15T20:00:00+00:00",
        "attachment_id": attachment_id,
        "owner_token": f"owner-token-{attachment_id}",
        "connector_generation_id": f"generation-{attachment_id}",
        "config_path": f"C:/runtime/{attachment_id}.json",
    }
    if pid is not None:
        intent["pid"] = pid
    return intent


def _proxy(attachment_id: str, pid: int) -> dict[str, object]:
    return {
        "owner": "clio-relay",
        "session_id": "gateway-placeholder",
        "attachment_id": attachment_id,
        "pid": pid,
        "owner_token": f"owner-token-{attachment_id}",
        "connector_generation_id": f"generation-{attachment_id}",
        "config_path": f"C:/runtime/{attachment_id}.json",
    }
