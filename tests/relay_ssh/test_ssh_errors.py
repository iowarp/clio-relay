"""Unit checks for how the ``ssh`` adapter reads ssh's failures and retries them.

The stderr samples are what OpenSSH 8.9 printed against W2's fixtures.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.adapters.ssh import SshConfig, errors
from clio_relay.adapters.ssh import transport as ssh_transport
from clio_relay.adapters.ssh.transport import Completed, SshTransport
from clio_relay.jobs.refusals import Refusal, RefusalReason

pytestmark = pytest.mark.release_platform("posix")

NO_KEY = "relay@127.0.0.1: Permission denied (publickey).\n"
MISSING_KEY_FILE = (
    "no such identity: /home/relay/.ssh/gone: No such file or directory\n"
    "relay@127.0.0.1: Permission denied (publickey).\n"
)
CODE_SITE = "relay@127.0.0.1: Permission denied (keyboard-interactive).\n"
UNKNOWN_HOST_KEY = (
    "No ED25519 host key is known for [127.0.0.1]:47100 and you have requested strict "
    "checking.\nHost key verification failed.\n"
)
CHANGED_HOST_KEY = (
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
    "@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @\n"
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
    "The fingerprint for the ED25519 key sent by the remote host is\n"
    "SHA256:abcDEF0123456789abcDEF0123456789abcDEF01234.\n"
    '  ssh-keygen -f "/tmp/kh" -R "[127.0.0.1]:47100"\n'
    "Host key for [127.0.0.1]:47100 has changed and you have requested strict checking.\n"
    "Host key verification failed.\n"
)
NO_SUCH_HOST = "ssh: Could not resolve hostname nosuch: Name or service not known\n"
REFUSED = "ssh: connect to host 127.0.0.1 port 47199: Connection refused\n"
DROPPED = "client_loop: send disconnect: Broken pipe\n"


@pytest.mark.parametrize(
    ("stderr", "expected"),
    [
        (NO_KEY, "key_refused"),
        (MISSING_KEY_FILE, "key_refused"),
        ("Received disconnect from 1.2.3.4: Too many authentication failures\n", "key_refused"),
        ("relay@h: Permission denied (publickey,keyboard-interactive).\n", "key_refused"),
        (CODE_SITE, "sign_in"),
        ("relay@h: Permission denied (password,keyboard-interactive).\n", "sign_in"),
        (UNKNOWN_HOST_KEY, "host_key_unknown"),
        (CHANGED_HOST_KEY, "host_key_changed"),
        (NO_SUCH_HOST, "unknown_host"),
        ("ssh: Could not resolve hostname h: Temporary failure in name resolution\n", "network"),
        (REFUSED, "network"),
        (DROPPED, "network"),
        ("Connection closed by 127.0.0.1 port 47100\n", "network"),
        ("ssh: something new\n", "ssh"),
        ("my tool failed\n", None),
    ],
)
def test_each_failure_has_a_cause(stderr: str, expected: str | None) -> None:
    assert errors.cause(stderr, 255) == expected


def test_a_remote_exit_other_than_255_is_never_an_ssh_failure() -> None:
    assert errors.cause(REFUSED, 1) is None
    assert errors.refusal(REFUSED, 7, profile="p", who="p", what="x", sign_in=False) is None


def test_only_network_failures_are_retried() -> None:
    assert errors.retryable(REFUSED, 255)
    assert errors.retryable(DROPPED, 255)
    for stderr in (NO_KEY, CODE_SITE, UNKNOWN_HOST_KEY, CHANGED_HOST_KEY, NO_SUCH_HOST):
        assert not errors.retryable(stderr, 255)


@pytest.mark.parametrize(
    ("stderr", "again"),
    [
        (REFUSED, True),
        ("kex_exchange_identification: Connection closed by remote host\n", True),
        ("Connection closed by 127.0.0.1 port 47100\n", True),
        ("ssh: Could not resolve hostname h: Temporary failure in name resolution\n", True),
        (DROPPED, False),
        ("Connection to 127.0.0.1 closed by remote host.\n", False),
        ("Connection reset by 127.0.0.1 port 47100\n", False),
    ],
)
def test_a_call_that_must_run_once_is_retried_only_before_a_session(
    stderr: str, again: bool
) -> None:
    assert errors.retryable(stderr, 255)
    assert errors.retryable(stderr, 255, once=True) is again


def _refuse(stderr: str, *, sign_in: bool = False) -> Refusal:
    refused = errors.refusal(stderr, 255, profile="fx", who="fx", what="describe", sign_in=sign_in)
    assert refused is not None
    return refused


def test_a_refused_key_says_how_to_offer_one() -> None:
    refused = _refuse(MISSING_KEY_FILE)
    assert refused.reason == RefusalReason.UNREACHABLE
    assert refused.data["cause"] == "key_refused"
    assert "/home/relay/.ssh/gone" in refused.message
    assert "ssh-add" in refused.message
    assert "IdentityFile" in refused.message


def test_a_changed_host_key_names_the_fingerprint_and_the_fix() -> None:
    refused = _refuse(CHANGED_HOST_KEY)
    assert refused.data["cause"] == "host_key_changed"
    assert "SHA256:abcDEF0123456789abcDEF0123456789abcDEF01234" in refused.message
    assert 'ssh-keygen -f "/tmp/kh" -R "[127.0.0.1]:47100"' in refused.message


def test_an_unknown_host_key_asks_for_one_manual_connection() -> None:
    refused = _refuse(UNKNOWN_HOST_KEY)
    assert refused.data["cause"] == "host_key_unknown"
    assert "ssh fx" in refused.message


def test_a_code_site_needs_sign_in_only_when_relay_can_ask() -> None:
    refused = _refuse(CODE_SITE)
    assert refused.reason == RefusalReason.INTERACTIVE_AUTH_REQUIRED
    assert refused.data["adapter"] == "worker"
    assert "interactive_sign_in" in refused.message
    assert _refuse(CODE_SITE, sign_in=True).reason == RefusalReason.NEEDS_SIGN_IN


def _transport(tmp_path: Path, retries: int) -> SshTransport:
    return SshTransport(
        SshConfig(ssh_profile="fx", retries=retries), site_id="s", state_dir=tmp_path
    )


def _completed(stderr: str, code: int) -> Completed:
    return Completed(argv=["ssh"], exit_code=code, stdout=b"", stderr=stderr.encode())


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    waits: list[float] = []

    async def record(delay: float) -> None:
        waits.append(delay)

    monkeypatch.setattr(ssh_transport.asyncio, "sleep", record)
    return waits


@pytest.mark.asyncio
async def test_a_dropped_call_runs_again_until_it_works(
    tmp_path: Path, no_sleep: list[float]
) -> None:
    answers = [_completed(DROPPED, 255), _completed(REFUSED, 255), _completed("", 0)]

    async def call() -> Completed:
        return answers.pop(0)

    result = await _transport(tmp_path, 5).retrying(call, "status")
    assert result.exit_code == 0
    assert no_sleep == [2.0, 4.0]


@pytest.mark.asyncio
async def test_retries_stop_at_the_limit(tmp_path: Path, no_sleep: list[float]) -> None:
    calls = 0

    async def call() -> Completed:
        nonlocal calls
        calls += 1
        return _completed(REFUSED, 255)

    result = await _transport(tmp_path, 5).retrying(call, "status")
    assert result.exit_code == 255
    assert calls == 6
    assert no_sleep == [2.0, 4.0, 8.0, 16.0, 30.0]


@pytest.mark.asyncio
async def test_a_key_failure_is_not_retried(tmp_path: Path, no_sleep: list[float]) -> None:
    calls = 0

    async def call() -> Completed:
        nonlocal calls
        calls += 1
        return _completed(NO_KEY, 255)

    await _transport(tmp_path, 5).retrying(call, "status")
    assert calls == 1
    assert no_sleep == []


@pytest.mark.asyncio
async def test_scp_failures_are_read_as_ssh_failures(tmp_path: Path, no_sleep: list[float]) -> None:
    answers = [_completed("lost connection\n", 1), _completed("", 0)]

    async def call() -> Completed:
        return answers.pop(0)

    result = await _transport(tmp_path, 1).retrying(call, "copying", scp=True)
    assert result.exit_code == 0
    assert no_sleep == [2.0]


def test_a_refusal_names_the_site_by_its_title(tmp_path: Path) -> None:
    transport = SshTransport(
        SshConfig(ssh_profile="fx"), site_id="s", title="Ares", state_dir=tmp_path
    )
    with pytest.raises(Refusal) as refused:
        transport.raise_for_failure(_completed(REFUSED, 255), "describe")
    assert "Ares (ssh fx)" in refused.value.message
    assert "site" not in refused.value.data


def test_the_old_setting_name_is_refused() -> None:
    with pytest.raises(ValueError, match="transfer_retries"):
        SshConfig.model_validate({"ssh_profile": "fx", "transfer_retries": 3})
