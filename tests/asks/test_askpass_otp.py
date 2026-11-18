"""Real ssh against W2's fx-otp fixture: key first, then a one-time code typed on relay's page.

Collected only where ``scripts/ares/fixtures.sh up`` wrote its env file.
"""

from __future__ import annotations

import asyncio
import os
import subprocess

import httpx
import pytest

from clio_relay.adapters.hooks import Ask
from clio_relay.asks import askpass
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import FIXTURES_ENV, add_site, eventually, hidden_fields, provider_of, submit, totp

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.usefixtures("no_listeners")]


def _fixtures() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in FIXTURES_ENV.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key] = value.strip().strip('"')
    return values


def _ssh(fx: dict[str, str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "ssh",
            "-F",
            fx["FX_SSH_CONFIG"],
            "-o",
            "BatchMode=no",
            "-o",
            "NumberOfPasswordPrompts=1",
            "-o",
            "PreferredAuthentications=publickey,keyboard-interactive",
            "fx-otp",
            "hostname",
        ],
        env={**os.environ, **env},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", ["code", "decline"])
async def test_otp_code_typed_on_the_page(relay_paths: RelayPaths, answer: str) -> None:
    fx = _fixtures()
    secret = open(fx["FX_OTP_SECRET_FILE"], encoding="ascii").read().strip()  # noqa: PTH123, SIM115
    add_site(relay_paths, "fx-otp")
    service = RelayService(relay_paths)
    provider = provider_of(service)
    provider.start_background()
    env = askpass.environment(
        site="fx-otp", operation_id=f"op-otp-{answer}", state_dir=relay_paths.state_dir
    )
    running = asyncio.create_task(asyncio.to_thread(_ssh, fx, env))

    def pending() -> Ask | None:
        for ask in service.asks.all():
            if ask.site == "fx-otp" and ask.state == "pending" and ask.binding.get("mode") == "prompt":
                return ask
        return None

    ask = await asyncio.to_thread(eventually, pending, "the one-time code prompt", 120.0)
    assert ask.url is not None
    page = await asyncio.to_thread(httpx.get, ask.url, timeout=30)
    assert "Verification code" in page.text
    prompt_id = hidden_fields(page.text)["prompt_id"]
    if answer == "code":
        await asyncio.to_thread(
            submit, ask.url, "prompt", totp(secret), extra={"prompt_id": prompt_id}
        )
    else:
        await asyncio.to_thread(submit, ask.url, "prompt_decline", extra={"prompt_id": prompt_id})
    done = await running
    if answer == "code":
        assert done.returncode == 0, done.stderr
        assert done.stdout.strip() == "fx-otp"
    else:
        assert done.returncode == 255
        assert "Permission denied" in done.stderr
    await service.close()
