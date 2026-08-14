from __future__ import annotations

import platform
from pathlib import Path

import pytest

from clio_relay.errors import ConfigurationError
from clio_relay.frp_check import run_frpc_connection_check
from clio_relay.relay_host import FrpcConfig


def test_frpc_connection_succeeds_when_process_stays_connected(tmp_path: Path) -> None:
    fake = _write_fake_frpc(tmp_path)

    lines = run_frpc_connection_check(
        frpc_bin=str(fake),
        config=FrpcConfig(
            server_addr="example.test",
            server_port=443,
            token="secret",
            local_port=8848,
            secret_key="stcp-secret",
        ),
        timeout_seconds=0.5,
    )

    assert lines[0] == "frpc stayed connected until timeout"


def test_frpc_failure_detail_is_bounded_by_bytes_not_line_count(tmp_path: Path) -> None:
    """The failure detail embedded in ``ConfigurationError`` used to be
    ``"\\n".join(stdout.splitlines()[-12:])`` -- a line-count heuristic with
    no byte guarantee at all. Three lines, each far larger than the T1
    budget, would previously all survive intact (well under the 12-line
    cap) and blow the detail out to roughly 9,000 bytes. The byte-bounded
    tail (doc §6.4) keeps the detail near the 2,000-byte T1 budget
    regardless of how many lines that spans.
    """
    payload_path = tmp_path / "frpc-output.txt"
    lines = ["x" * 3_000, "y" * 3_000, "z" * 3_000]
    payload_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    fake = _write_fake_failing_frpc(tmp_path, payload_path)

    with pytest.raises(ConfigurationError) as excinfo:
        run_frpc_connection_check(
            frpc_bin=str(fake),
            config=FrpcConfig(
                server_addr="example.test",
                server_port=443,
                token="secret",
                local_port=8848,
                secret_key="stcp-secret",
            ),
            timeout_seconds=5.0,
        )

    detail = str(excinfo.value)
    assert "[clio-relay: elided" in detail
    assert "x" * 3_000 not in detail  # the earliest line is well outside the tail window
    assert len(detail.encode("utf-8")) < 2_500  # far under the ~9,000-byte unbounded total


def _write_fake_failing_frpc(tmp_path: Path, payload_path: Path) -> Path:
    """A fake frpc that dumps ``payload_path`` to stdout and exits nonzero."""
    if platform.system().lower() == "windows":
        path = tmp_path / "fake-failing-frpc.cmd"
        path.write_text(
            f'@echo off\ntype "{payload_path}"\nexit /b 1\n',
            encoding="utf-8",
        )
        return path
    path = tmp_path / "fake-failing-frpc"
    path.write_text(
        f'#!/usr/bin/env sh\ncat "{payload_path}"\nexit 1\n',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | 0o111)
    return path


def _write_fake_frpc(tmp_path: Path) -> Path:
    if platform.system().lower() == "windows":
        path = tmp_path / "fake-frpc.cmd"
        path.write_text(
            "@echo off\necho login to server success\nping -n 3 127.0.0.1 > nul\n",
            encoding="utf-8",
        )
        return path
    path = tmp_path / "fake-frpc"
    path.write_text(
        "#!/usr/bin/env sh\necho login to server success\nsleep 2\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | 0o111)
    return path
