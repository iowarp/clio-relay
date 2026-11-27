"""``clio-relay asks``: list, answer, decline, page and identities from a terminal (RL28, RL30)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from clio_relay import cli_asks
from clio_relay.adapters.hooks import Ask
from clio_relay.asks.pages import PageTokens
from clio_relay.asks.provider import base_file
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site, provider_of
from .test_identity import write_grant

pytestmark = [pytest.mark.release_platform("posix")]

ECHO = JobSpec(executable="echo", arguments=["hello-relay"])
runner = CliRunner()


def _approval(paths: RelayPaths) -> Ask:
    add_site(paths, spend="always")

    async def make() -> Ask:
        service = RelayService(paths)
        try:
            record = await service.start_run("fx", ECHO)
            (ask,) = service.outstanding(record)
            return ask
        finally:
            await service.close()

    return asyncio.run(make())


def _choice(paths: RelayPaths) -> Ask:
    add_site(paths)

    async def make() -> Ask:
        service = RelayService(paths)
        try:
            return await provider_of(service).choose(
                site=service.site("fx"),
                operation_id=None,
                message="Which queue?",
                field_name="queue",
                options=["debug", "batch"],
            )
        finally:
            await service.close()

    return asyncio.run(make())


def _stored(paths: RelayPaths, ask_id: str) -> Ask:
    found = RelayService(paths).asks.get(ask_id)
    assert found is not None
    return found


def _json(text: str) -> Any:
    return json.loads(text)


def test_list_and_show(relay_paths: RelayPaths) -> None:
    ask = _approval(relay_paths)
    listed = runner.invoke(cli_asks.app, ["list"])
    assert listed.exit_code == 0, listed.output
    (row,) = _json(listed.stdout)
    assert row["ask_id"] == ask.ask_id and row["kind"] == "approve" and row["state"] == "pending"
    shown = runner.invoke(cli_asks.app, ["show", ask.ask_id])
    assert shown.exit_code == 0, shown.output
    detail = _json(shown.stdout)
    assert detail["binding"]["spec_digest"] == ask.binding["spec_digest"]
    assert "url" not in detail
    missing = runner.invoke(cli_asks.app, ["show", "ask-nothing"])
    assert missing.exit_code == 2


def test_answer_approves_once(relay_paths: RelayPaths) -> None:
    ask = _approval(relay_paths)
    answered = runner.invoke(cli_asks.app, ["answer", ask.ask_id])
    assert answered.exit_code == 0, answered.output
    result = _json(answered.stdout)
    assert result["operation"] == ask.operation_id
    assert result["state"] != OperationState.CANCELLED.value
    stored = _stored(relay_paths, ask.ask_id)
    assert stored.state == "accepted" and stored.answered_by == "cli"
    assert runner.invoke(cli_asks.app, ["list"]).stdout.strip() == "[]"
    again = runner.invoke(cli_asks.app, ["decline", ask.ask_id])
    assert _stored(relay_paths, ask.ask_id).state == "accepted", again.output


def test_answer_refuses_a_changed_job(relay_paths: RelayPaths) -> None:
    ask = _approval(relay_paths)
    assert ask.operation_id is not None
    service = RelayService(relay_paths)
    with service.operations.locked(ask.operation_id):
        record = service.operation(ask.operation_id)
        record.spec_digest = "0" * 64
        service.operations.put(ask.operation_id, record)
    refused = runner.invoke(cli_asks.app, ["answer", ask.ask_id])
    assert refused.exit_code == 2
    assert "approval_mismatch" in refused.output and "Nothing was approved" in refused.output
    assert _stored(relay_paths, ask.ask_id).state == "pending"


def test_decline_cancels(relay_paths: RelayPaths) -> None:
    ask = _approval(relay_paths)
    declined = runner.invoke(cli_asks.app, ["decline", ask.ask_id])
    assert declined.exit_code == 0, declined.output
    assert _json(declined.stdout)["state"] == OperationState.CANCELLED.value
    assert _stored(relay_paths, ask.ask_id).state == "declined"


def test_choose_from_the_terminal(relay_paths: RelayPaths) -> None:
    ask = _choice(relay_paths)
    wrong = runner.invoke(cli_asks.app, ["answer", ask.ask_id, "--value", "gpu"])
    assert wrong.exit_code == 2 and "debug, batch" in wrong.output
    chosen = runner.invoke(cli_asks.app, ["answer", ask.ask_id, "--value", "batch"])
    assert chosen.exit_code == 0, chosen.output
    stored = _stored(relay_paths, ask.ask_id)
    assert stored.answer == {"action": "accept", "content": {"queue": "batch"}}


def test_page_prints_a_fresh_address(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    ask = _choice(relay_paths)
    base_file(relay_paths.state_dir).parent.mkdir(parents=True, exist_ok=True)
    base_file(relay_paths.state_dir).write_text(
        json.dumps({"base": "http://127.0.0.1:9", "pid": 1}), encoding="utf-8"
    )

    def answers(base: str) -> bool:
        return True

    monkeypatch.setattr(cli_asks, "_page_answers", answers)
    printed = runner.invoke(cli_asks.app, ["page", ask.ask_id])
    assert printed.exit_code == 0, printed.output
    url = printed.stdout.strip().splitlines()[0]
    assert url.startswith(f"http://127.0.0.1:9/relay/asks/{ask.ask_id}?t=")
    token = url.split("t=", 1)[1]
    assert PageTokens(relay_paths.state_dir / "pages").check(ask.ask_id, token) == "ok"
    other = runner.invoke(cli_asks.app, ["page", ask.ask_id]).stdout.strip()
    assert other != url


def test_page_serves_itself_when_the_recorded_page_is_gone(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    ask = _choice(relay_paths)
    base_file(relay_paths.state_dir).parent.mkdir(parents=True, exist_ok=True)
    base_file(relay_paths.state_dir).write_text(
        json.dumps({"base": "http://127.0.0.1:9", "pid": 1}), encoding="utf-8"
    )
    served: list[int] = []

    def answer_from_the_page(_seconds: float) -> None:
        base = json.loads(base_file(relay_paths.state_dir).read_text(encoding="utf-8"))["base"]
        served.append(httpx.get(f"{base}/relay/asks/", timeout=5.0).status_code)
        RelayService(relay_paths).answer(ask.ask_id, "accept", {"queue": "batch"}, by="page")

    monkeypatch.setattr(cli_asks.time, "sleep", answer_from_the_page)
    printed = runner.invoke(cli_asks.app, ["page", ask.ask_id])
    assert printed.exit_code == 0, printed.output
    url = printed.stdout.strip().splitlines()[0]
    assert url.startswith("http://127.0.0.1:") and "127.0.0.1:9/" not in url
    assert served == [404]
    assert _stored(relay_paths, ask.ask_id).state == "accepted"
    assert not base_file(relay_paths.state_dir).exists()


def test_identities_show_where_a_token_comes_from(
    relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def globus() -> dict[str, Any]:
        shown = runner.invoke(cli_asks.app, ["identities"])
        assert shown.exit_code == 0, shown.output
        rows = {row["provider"]: row for row in _json(shown.stdout)}
        assert {"globus", "github"} <= set(rows)
        return rows["globus"]

    assert globus()["state"] == "signed_out"
    grant = tmp_path / "host" / "globus.json"
    write_grant(grant, "globus", "host-token")
    monkeypatch.setenv("CLIO_RELAY_GRANT_FILE_GLOBUS", str(grant))
    row = globus()
    assert row["state"] == "signed_in" and row["source"] == "grant"
    assert "host-token" not in json.dumps(row)
    out = runner.invoke(cli_asks.app, ["sign-out", "globus"])
    assert out.exit_code == 0, out.output
    assert grant.is_file()
    unknown = runner.invoke(cli_asks.app, ["sign-out", "nobody"])
    assert unknown.exit_code == 2
