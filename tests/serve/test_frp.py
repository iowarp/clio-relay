"""frpc configuration at both ends, and the commands that hold the desktop port (RL20)."""

from __future__ import annotations

import asyncio
import shlex
import socket
import sys
from pathlib import Path

import pytest

from clio_relay.serve import frp
from clio_relay.serve.forward import Forward, port_bound, ssh_command, visitor_command

from .helpers import frp_plan

pytestmark = pytest.mark.release_platform("posix")


def common(**overrides: object) -> frp.Common:
    values: dict[str, object] = {"address": "relay.example.org", "port": 7000, "protocol": "tcp"}
    values.update(overrides)
    return frp.Common(**values)  # pyright: ignore[reportArgumentType]


def test_site_config_has_stcp_and_xtcp_when_direct() -> None:
    text = frp.render_site(common(), [("web", 8080)], "op_1", direct=True)
    assert 'name = "relay-op_1-web"' in text
    assert 'type = "stcp"' in text
    assert 'name = "relay-op_1-web-p2p"' in text
    assert 'type = "xtcp"' in text
    assert "localPort = 8080" in text
    assert 'auth.token = "{{ .Envs.CLIO_RELAY_FRP_TOKEN }}"' in text
    assert "xtcp" not in frp.render_site(common(), [("web", 8080)], "op_1", direct=False)


def test_visitor_binds_loopback_and_falls_back_to_stcp() -> None:
    text = frp.render_visitor(common(), [("web", 41000)], "op_1", direct=True)
    assert 'bindAddr = "127.0.0.1"' in text
    assert "0.0.0.0" not in text
    assert 'fallbackTo = "relay-op_1-web-visitor"' in text
    assert "bindPort = 41000" in text
    assert "bindPort = -1" in text
    plain = frp.render_visitor(common(), [("web", 41000)], "op_1", direct=False)
    assert "bindPort = 41000" in plain
    assert "xtcp" not in plain


def test_wss_always_verifies_the_server() -> None:
    with pytest.raises(ValueError, match="CA file"):
        common(protocol="wss").render()
    lines = common(protocol="wss", ca_file="/ca.pem", server_name="frps.example.org").render()
    assert 'transport.tls.trustedCaFile = "/ca.pem"' in lines
    assert 'transport.tls.serverName = "frps.example.org"' in lines


def test_visitor_command_keeps_secrets_in_the_environment(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    argv, env = visitor_command(plan, tmp_path / "fwd")
    config = (tmp_path / "fwd" / "visitor.toml").read_text(encoding="utf-8")
    assert argv[1:] == ["-c", str(tmp_path / "fwd" / "visitor.toml")]
    assert env[frp.ENV_TOKEN] == "frp-token-value"
    assert env[frp.ENV_SECRET_KEY] == "shared-key-value"
    assert "frp-token-value" not in config
    assert "shared-key-value" not in config
    assert "frp-token-value" not in " ".join(argv)


def test_ssh_forward_jumps_through_the_login_host(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path).model_copy(
        update={
            "mode": "ssh",
            "ssh_argv": ["ssh", "-F", "/cfg", "-o", "BatchMode=yes", "ares"],
            "host": "ares-comp-03",
            "login_host": "ares",
        }
    )
    port = plan.ports[0]
    argv, env = ssh_command(plan, {"SSH_ASKPASS": "/askpass"})
    assert argv[:5] == ["ssh", "-F", "/cfg", "-o", "BatchMode=yes"]
    assert f"127.0.0.1:{port.local_port}:127.0.0.1:{port.remote_port}" in argv
    assert "ExitOnForwardFailure=yes" in argv
    assert argv[-1] == "ares-comp-03"
    jump = next(item for item in argv if item.startswith("ProxyCommand="))
    assert shlex.split(jump.split("=", 1)[1])[-3:] == ["-W", "%h:%p", "ares"]
    assert env["SSH_ASKPASS"] == "/askpass"
    same = plan.model_copy(update={"host": "ares"})
    assert ssh_command(same)[0][-1] == "ares"


def test_port_bound_does_not_connect() -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        taken = sock.getsockname()[1]
        assert port_bound(taken)
    assert not port_bound(taken)


def test_forward_counts_a_rebind_only_when_the_new_forward_holds_the_port(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A forward that keeps dying is retried but never reported as bound again."""
    plan = frp_plan(tmp_path)
    port = plan.ports[0].local_port
    script = tmp_path / "hold.py"
    script.write_text(
        "import socket, sys, time\n"
        "if sys.argv[1] == 'die':\n"
        "    sys.exit(3)\n"
        "s = socket.socket()\n"
        "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
        f"s.bind(('127.0.0.1', {port}))\n"
        "s.listen()\n"
        "time.sleep(600)\n",
        encoding="utf-8",
    )
    mode = {"value": "hold"}
    monkeypatch.setattr("clio_relay.serve.forward.POLL_S", 0.05)
    monkeypatch.setattr("clio_relay.serve.forward.MAX_BACKOFF_S", 0.05)
    calls: list[int] = []

    def command(_self: Forward) -> tuple[list[str], dict[str, str]]:
        return [sys.executable, str(script), mode["value"]], {}

    monkeypatch.setattr(Forward, "_command", command)

    async def until(check: object) -> None:
        # A ceiling only so a broken forward fails the test instead of hanging it.
        for _ in range(2400):
            if check():  # pyright: ignore[reportCallIssue]
                return
            await asyncio.sleep(0.05)
        raise AssertionError("the forward never reached the expected state")

    async def scenario() -> None:
        forward = Forward(plan, tmp_path / "fwd", lambda count, _pid: calls.append(count))
        await forward.start()
        await until(lambda: forward.held)
        assert calls == [0]
        mode["value"] = "die"
        assert forward.process is not None
        forward.process.kill()
        await until(lambda: forward.restarts >= 3)
        assert forward.rebound == 0
        mode["value"] = "hold"
        await until(lambda: forward.rebound == 1)
        assert forward.bound()
        assert max(calls) == 1
        await forward.close()
        assert not forward.bound()

    asyncio.run(scenario())


def test_forward_first_bind_after_failed_tries_is_not_a_rebind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A node that refuses the forward for a while, then accepts it, was never bound before."""
    plan = frp_plan(tmp_path)
    port = plan.ports[0].local_port
    script = tmp_path / "hold.py"
    script.write_text(
        "import socket, sys, time\n"
        "if sys.argv[1] == 'die':\n"
        "    sys.exit(3)\n"
        "s = socket.socket()\n"
        "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
        f"s.bind(('127.0.0.1', {port}))\n"
        "s.listen()\n"
        "time.sleep(600)\n",
        encoding="utf-8",
    )
    mode = {"value": "die"}
    monkeypatch.setattr("clio_relay.serve.forward.POLL_S", 0.05)
    monkeypatch.setattr("clio_relay.serve.forward.MAX_BACKOFF_S", 0.05)

    def command(_self: Forward) -> tuple[list[str], dict[str, str]]:
        return [sys.executable, str(script), mode["value"]], {}

    monkeypatch.setattr(Forward, "_command", command)

    async def until(check: object) -> None:
        # A ceiling only so a broken forward fails the test instead of hanging it.
        for _ in range(2400):
            if check():  # pyright: ignore[reportCallIssue]
                return
            await asyncio.sleep(0.05)
        raise AssertionError("the forward never reached the expected state")

    async def scenario() -> None:
        forward = Forward(plan, tmp_path / "fwd", lambda _count, _pid: None)
        await forward.start()
        await until(lambda: forward.restarts >= 2)
        mode["value"] = "hold"
        await until(lambda: forward.held)
        assert forward.rebound == 0
        await forward.close()

    asyncio.run(scenario())
