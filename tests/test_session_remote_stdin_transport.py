"""The owned-session stdin transport must not send its script as a command-line argument.

clio-relay#246: ``_ssh_stdin_command`` put its script in one ssh argv element
(``bash -lc <script>``) and only the payload on stdin. Some ssh clients silently
truncate a long argument -- the MSYS2 OpenSSH shipped with Git for Windows
delivered an 8117-byte command intact and not a 10117-byte one -- while the
script guard allowed several MiB. The script and the payload now both travel on
stdin, the shape ``_ssh_script`` and the bootstrap preflight (#158) already use.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import cast

import pytest
from pytest import MonkeyPatch

import clio_relay.session_remote_command as session_remote_command
import clio_relay.session_remote_scripts as session_remote_scripts
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError

_DEFINITION = ClusterDefinition(name="ares", ssh_host="ares")


def _large_script(tail: str) -> str:
    """A script well past the ~8 KB where argv delivery started losing bytes."""
    padding = "".join(f"# padding line {index:05d}\n" for index in range(600))
    return f"set -euo pipefail\n{padding}{tail}\n"


def _capture(monkeypatch: MonkeyPatch) -> dict[str, object]:
    observed: dict[str, object] = {}

    def run_bounded(
        command: list[str], **kwargs: object
    ) -> session_remote_command._BoundedCommandResult:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        observed["command"] = command
        observed["input_bytes"] = kwargs["input_bytes"]
        return session_remote_command._BoundedCommandResult(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            returncode=0, stdout=b"ok", stderr=b""
        )

    monkeypatch.setattr(session_remote_command, "_run_bounded_command", run_bounded)
    return observed


def test_no_stdin_command_argument_can_be_truncated_by_a_client_limit(
    monkeypatch: MonkeyPatch,
) -> None:
    """No single argv element may carry script- or payload-sized content."""
    observed = _capture(monkeypatch)
    script = _large_script("cat >/dev/null")
    payload = ('{"marker": "' + "p" * (1024 * 1024) + '"}').encode("utf-8")

    session_remote_scripts._ssh_stdin_command(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _DEFINITION,
        script,
        input_bytes=payload,
        input_limit=2 * 1024 * 1024,
        stdout_limit=1024,
    )

    command = cast(list[str], observed["command"])
    input_bytes = cast(bytes, observed["input_bytes"])
    assert len(script.encode("utf-8")) > 10 * 1024
    assert command == ["ssh", "ares", "bash", "-l", "-s"]
    assert max(len(argument) for argument in command) < 1024
    assert script.encode("utf-8") in input_bytes
    assert payload in input_bytes


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs a local bash")
def test_a_large_script_receives_its_exact_payload_on_stdin(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Run the rendered transport through a real local bash in place of ssh.

    The payload carries every character shell quoting can get wrong, and both it
    and the script are past the size a truncating client would cut.
    """
    received = tmp_path / "received.json"
    script = _large_script(f"cat > {received.as_posix()!r}\nprintf 'done'")
    payload = (
        '{"quote": "it\'s", "double": "\\"x\\"", "dollar": "$HOME $(id)", '
        '"backslash": "C:\\\\Users", "newline": "a\\nb", "unicode": "\u00e9\u4e2d", '
        f'"bulk": "{"q" * 20_000}"}}\n'
    ).encode()
    real_run = session_remote_command._run_bounded_command  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def run_locally(
        command: list[str], **kwargs: object
    ) -> session_remote_command._BoundedCommandResult:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        assert command[:2] == ["ssh", "ares"]
        return real_run(command[2:], **kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(session_remote_command, "_run_bounded_command", run_locally)

    output = session_remote_scripts._ssh_stdin_command(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _DEFINITION,
        script,
        input_bytes=payload,
        input_limit=64 * 1024,
        stdout_limit=64 * 1024,
    )

    assert output.endswith("done")
    assert received.read_bytes() == payload


@pytest.mark.parametrize(
    ("payload", "message"),
    [(b"\xff\xfe", "not UTF-8"), (b'{"a": "\x00"}', "NUL byte")],
)
def test_a_payload_the_shell_cannot_carry_is_refused_before_dialing(
    monkeypatch: MonkeyPatch, payload: bytes, message: str
) -> None:
    observed = _capture(monkeypatch)

    with pytest.raises(RelayError, match=message):
        session_remote_scripts._ssh_stdin_command(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            _DEFINITION,
            "cat",
            input_bytes=payload,
            input_limit=1024,
            stdout_limit=1024,
        )

    assert "command" not in observed
