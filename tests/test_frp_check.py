from __future__ import annotations

import platform
from pathlib import Path

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
