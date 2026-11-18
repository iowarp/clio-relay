"""``SSH_ASKPASS``: the helper's protocol, and ssh's prompts on relay's page (RL28)."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from clio_relay.adapters.base import SignInNeed
from clio_relay.adapters.hooks import Ask
from clio_relay.asks import askpass
from clio_relay.asks.askpass import AskpassSession, Prompt
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site, eventually, hidden_fields, provider_of, submit

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.usefixtures("no_listeners")]


class Person:
    """Answers prompts for one site the way the page would."""

    def __init__(self, site: str, answer: str | None) -> None:
        self.site = site
        self.answer = answer
        self.seen: list[Prompt] = []

    def __call__(self, session: AskpassSession, prompt: Prompt) -> None:
        if session.site != self.site:
            return
        self.seen.append(prompt)

        def reply() -> None:
            if self.answer is None:
                session.decline(prompt.prompt_id)
            else:
                session.answer(prompt.prompt_id, self.answer)

        threading.Thread(target=reply, daemon=True).start()


@pytest.fixture
def person() -> Iterator[Person]:
    listener = Person("helper-site", "the-answer")
    askpass.broker().add_listener(listener)
    yield listener
    askpass.broker().remove_listener(listener)


def _env(monkeypatch: pytest.MonkeyPatch, site: str, tmp_path: Path) -> dict[str, str]:
    env = askpass.environment(site=site, operation_id="op-1", state_dir=tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("SSH_ASKPASS_PROMPT", raising=False)
    return env


def test_environment_names_the_helper(tmp_path: Path) -> None:
    env = askpass.environment(site="s", operation_id="op", state_dir=tmp_path)
    assert env["SSH_ASKPASS_REQUIRE"] == "force"
    assert env["DISPLAY"]
    assert Path(env["SSH_ASKPASS"]).name.startswith(askpass.PROGRAM)
    assert env[askpass.ENV_ADDR].startswith("127.0.0.1:")
    assert env[askpass.ENV_SITE] == "s" and env[askpass.ENV_OPERATION] == "op"
    again = askpass.environment(site="s", operation_id="op", state_dir=tmp_path)
    assert again[askpass.ENV_TOKEN] == env[askpass.ENV_TOKEN]
    other = askpass.environment(site="s", operation_id="op-2", state_dir=tmp_path)
    assert other[askpass.ENV_TOKEN] != env[askpass.ENV_TOKEN]


def test_helper_prints_the_answer(
    person: Person,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _env(monkeypatch, "helper-site", tmp_path)
    assert askpass.main(["Password: "]) == 0
    assert capsys.readouterr().out == "the-answer\n"
    assert person.seen[0].text == "Password: " and person.seen[0].kind == "secret"


def test_helper_exits_1_on_decline(
    person: Person,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    person.answer = None
    _env(monkeypatch, "helper-site", tmp_path)
    assert askpass.main(["Verification code: "]) == 1
    assert capsys.readouterr().out == ""


def test_helper_confirm_prints_yes(
    person: Person,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _env(monkeypatch, "helper-site", tmp_path)
    monkeypatch.setenv("SSH_ASKPASS_PROMPT", "confirm")
    assert askpass.main(["Allow use of key?"]) == 0
    assert capsys.readouterr().out == "yes\n"
    assert person.seen[0].kind == "confirm"


def test_helper_notice_holds_until_seen(
    person: Person,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _env(monkeypatch, "helper-site", tmp_path)
    monkeypatch.setenv("SSH_ASKPASS_PROMPT", "none")
    assert askpass.main(["Confirm user presence for key"]) == 0
    assert capsys.readouterr().out == ""
    assert person.seen[0].kind == "notice"


def test_helper_without_a_page_declines(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    askpass.broker()._listeners.clear()  # pyright: ignore[reportPrivateUsage]
    _env(monkeypatch, "lonely-site", tmp_path)
    assert askpass.main(["Password: "]) == 1
    assert "no page" in capsys.readouterr().err


def test_helper_with_a_wrong_token_declines(
    person: Person,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _env(monkeypatch, "helper-site", tmp_path)
    monkeypatch.setenv(askpass.ENV_TOKEN, "not-a-token")
    assert askpass.main(["Password: "]) == 1
    assert "does not know" in capsys.readouterr().err
    assert person.seen == []


def test_helper_without_relay_declines(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv(askpass.ENV_ADDR, raising=False)
    assert askpass.main(["Password: "]) == 1
    assert askpass.ENV_ADDR in capsys.readouterr().err


def test_installed_helper(person: Person, tmp_path: Path) -> None:
    env = askpass.environment(site="helper-site", state_dir=tmp_path)
    program = env["SSH_ASKPASS"]
    assert program == askpass.program(tmp_path)
    done = subprocess.run(
        [program, "Password for relay@site: "],
        env={**os.environ, **env},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout == "the-answer\n"


def _helper(env: dict[str, str], prompt: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "clio_relay.asks.askpass", prompt],
        env={**os.environ, **env},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def _prompt_ask(service: RelayService, site: str) -> Ask | None:
    for ask in service.asks.all():
        if ask.site == site and ask.binding.get("mode") == "prompt" and ask.state == "pending":
            return ask
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", ["typed-on-the-page", None])
async def test_a_stray_prompt_becomes_a_page(relay_paths: RelayPaths, answer: str | None) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)
    provider = provider_of(service)
    provider.start_background()
    env = askpass.environment(site="fx", operation_id="op-stray", state_dir=relay_paths.state_dir)
    running = asyncio.create_task(asyncio.to_thread(_helper, env, "Password: "))
    ask = await asyncio.to_thread(eventually, lambda: _prompt_ask(service, "fx"), "the ask")
    assert ask.operation_id == "op-stray" and ask.url is not None
    page = await asyncio.to_thread(httpx.get, ask.url, timeout=30)
    assert "Password:" in page.text and "type='password'" in page.text
    prompt_id = hidden_fields(page.text)["prompt_id"]
    if answer is None:
        posted = await asyncio.to_thread(
            submit, ask.url, "prompt_decline", extra={"prompt_id": prompt_id}
        )
    else:
        posted = await asyncio.to_thread(
            submit, ask.url, "prompt", answer, extra={"prompt_id": prompt_id}
        )
    assert posted.status_code in (200, 303)
    done = await running
    if answer is None:
        assert done.returncode == 1 and done.stdout == ""
    else:
        assert done.returncode == 0 and done.stdout == f"{answer}\n"
        stored = service.asks.get(ask.ask_id)
        assert stored is not None and stored.completed_at is not None
        assert answer not in stored.model_dump_json()
    await service.close()


@pytest.mark.asyncio
async def test_an_askpass_sign_in_runs_from_the_page(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    seen: list[str] = []

    async def sign_in(env: dict[str, str]) -> None:
        done = await asyncio.to_thread(_helper, env, "Verification code: ")
        if done.returncode != 0:
            raise Refusal(RefusalReason.NEEDS_SIGN_IN, "the code was refused")
        seen.append(done.stdout.strip())

    askpass.register_sign_in("fx", sign_in)
    service = RelayService(relay_paths)
    try:
        provider = provider_of(service)
        need = SignInNeed(provider="fx-ssh", message="Sign in to fx")
        ask = await provider.url(site=service.site("fx"), operation_id=None, need=need)
        assert ask.binding["mode"] == "askpass" and ask.url is not None
        url = ask.url
        first = await asyncio.to_thread(httpx.get, url, timeout=30)
        assert "Start sign-in" in first.text
        assert (await asyncio.to_thread(submit, url, "start")).status_code == 303

        def prompt_page() -> str | None:
            text = httpx.get(url, timeout=30).text
            return text if "prompt_id" in text else None

        page = await asyncio.to_thread(eventually, prompt_page, "the prompt")
        assert "Verification code:" in page
        prompt_id = hidden_fields(page)["prompt_id"]
        await asyncio.to_thread(submit, url, "prompt", "123456", extra={"prompt_id": prompt_id})

        def finished() -> Ask | None:
            found = service.asks.get(ask.ask_id)
            return found if found is not None and found.completed_at is not None else None

        await asyncio.to_thread(eventually, finished, "the sign-in")
        assert seen == ["123456"]
    finally:
        askpass.register_sign_in("fx", None)
        await service.close()
