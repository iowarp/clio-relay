"""Smoke tests for the Ares fixture sites (tests/fixtures/sites, scripts/ares).

They run against fixtures that ``scripts/ares/fixtures.sh up`` started, and read the
env file it wrote: ``$CLIO_RELAY_FIXTURES_ENV``, or by default
``~/clio-relay-rework/state/w2-fixtures/fixtures.env``. Without that file the module
is skipped, so an ordinary run elsewhere is unaffected.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import shlex
import socket
import struct
import subprocess
import time
import uuid
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from websockets.exceptions import InvalidStatus
from websockets.sync.client import connect as ws_connect

ENV_FILE = Path(
    os.environ.get(
        "CLIO_RELAY_FIXTURES_ENV",
        str(Path.home() / "clio-relay-rework" / "state" / "w2-fixtures" / "fixtures.env"),
    )
)

if not ENV_FILE.is_file():
    pytest.skip(
        f"fixture env file {ENV_FILE} not found; run scripts/ares/fixtures.sh up",
        allow_module_level=True,
    )

pytestmark = pytest.mark.release_platform("posix")


def _load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key] = value.strip().strip('"')
    return values


FX = _load_env(ENV_FILE)
SITES = set(FX.get("FX_SITES", "").split())


def _needs(site: str) -> None:
    if site not in SITES:
        pytest.fail(f"site {site} is not up; FX_SITES is {sorted(SITES)}")


def _read_secret(key: str) -> str:
    return Path(FX[key]).read_text(encoding="utf-8").strip()


def _ssh(
    site: str, command: str, *, timeout: float = 60, batch: bool = True
) -> subprocess.CompletedProcess[str]:
    options = ["-o", "BatchMode=yes"] if batch else []
    return subprocess.run(
        ["ssh", "-F", FX["FX_SSH_CONFIG"], *options, f"fx-{site}", command],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        stdin=subprocess.DEVNULL,
    )


def _ssh_ok(site: str, command: str, *, timeout: float = 60) -> str:
    result = _ssh(site, command, timeout=timeout)
    assert result.returncode == 0, f"{site}: {command!r} failed: {result.stderr}"
    return result.stdout


def _poll[T](what: str, timeout: float, probe: Callable[[], T | None]) -> T:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = probe()
        if value is not None:
            return value
        time.sleep(1)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# ------------------------------------------------------------------ ssh


def test_ssh_site_runs_commands_and_copies_files(tmp_path: Path) -> None:
    _needs("ssh")
    assert _ssh_ok("ssh", "hostname").strip() == "fx-ssh"
    assert _ssh_ok("ssh", "id -un").strip() == "relay"
    assert "Python 3" in _ssh_ok("ssh", "python3 --version")

    payload = tmp_path / "payload.bin"
    payload.write_bytes(os.urandom(64 * 1024))
    digest = hashlib.sha256(payload.read_bytes()).hexdigest()
    remote = f"/tmp/fx-smoke-{uuid.uuid4().hex}.bin"
    scp = subprocess.run(
        ["scp", "-F", FX["FX_SSH_CONFIG"], "-o", "BatchMode=yes", str(payload), f"fx-ssh:{remote}"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert scp.returncode == 0, scp.stderr
    assert _ssh_ok("ssh", f"sha256sum {remote}").split()[0] == digest

    batch = tmp_path / "sftp.batch"
    batch.write_text(f"get {remote} {tmp_path / 'back.bin'}\nrm {remote}\n", encoding="utf-8")
    sftp = subprocess.run(
        ["sftp", "-F", FX["FX_SSH_CONFIG"], "-o", "BatchMode=yes", "-b", str(batch), "fx-ssh"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert sftp.returncode == 0, sftp.stderr
    assert hashlib.sha256((tmp_path / "back.bin").read_bytes()).hexdigest() == digest


def test_ssh_site_refuses_unknown_keys(tmp_path: Path) -> None:
    _needs("ssh")
    stranger = tmp_path / "stranger"
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(stranger)],
        check=True,
        timeout=30,
    )
    result = subprocess.run(
        [
            "ssh",
            "-F",
            FX["FX_SSH_CONFIG"],
            "-o",
            "BatchMode=yes",
            "-o",
            f"IdentityFile={stranger}",
            "fx-ssh",
            "true",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 255
    assert "Permission denied" in result.stderr


# ------------------------------------------------------------------ schedulers


def test_slurm_site_runs_a_batch_job_with_accounting() -> None:
    _needs("slurm")
    assert _ssh_ok("slurm", "hostname").strip() == "fx-slurm"
    partitions = set(_ssh_ok("slurm", "sinfo -h -o %P").split())
    assert {"compute*", "debug"} <= partitions
    # The query psij's Slurm executor issues.
    _ssh_ok("slurm", "squeue --me -o %i -h -r -t all")

    marker = f"slurm-smoke-{uuid.uuid4().hex}"
    job_id = _ssh_ok(
        "slurm",
        f"sbatch --parsable -o $HOME/{marker}.out --wrap {shlex.quote(f'hostname; echo {marker}')}",
    ).strip()
    assert job_id.isdigit(), job_id

    def finished() -> str | None:
        out = _ssh("slurm", f"sacct -n -X -P -j {job_id} -o State").stdout.strip()
        return out if out in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"} else None

    assert _poll("slurm job", 120, finished) == "COMPLETED"
    output = _ssh_ok("slurm", f"cat $HOME/{marker}.out; rm -f $HOME/{marker}.out")
    assert output.split() == ["fx-slurm", marker]


def test_pbs_site_runs_a_job_visible_to_qstat_json_history() -> None:
    _needs("pbs")
    assert _ssh_ok("pbs", "hostname").strip() == "fx-pbs"
    marker = f"pbs-smoke-{uuid.uuid4().hex}"
    job_id = _ssh_ok(
        "pbs",
        f"qsub -N fxsmoke -j oe -o $HOME/{marker}.out -- /bin/sh -c "
        + shlex.quote(f"hostname; echo {marker}"),
    ).strip()
    assert job_id, "qsub printed no job id"

    def finished() -> dict[str, Any] | None:
        result = _ssh("pbs", f"qstat -f -F json -x {job_id}")
        if result.returncode != 0:
            return None
        jobs = cast(dict[str, dict[str, Any]], json.loads(result.stdout).get("Jobs") or {})
        records = list(jobs.values())
        record = records[0] if records else {}
        return record if record.get("job_state") == "F" else None

    record = _poll("pbs job", 120, finished)
    assert record.get("Exit_status") == 0

    def output() -> str | None:
        result = _ssh("pbs", f"cat $HOME/{marker}.out")
        return result.stdout if result.returncode == 0 else None

    assert _poll("pbs output", 30, output).split() == ["fx-pbs", marker]
    _ssh_ok("pbs", f"rm -f $HOME/{marker}.out")


# ------------------------------------------------------------------ one-time code


def _totp(secret_b32: str, at: float) -> str:
    padded = secret_b32 + "=" * (-len(secret_b32) % 8)
    key = base64.b32decode(padded, casefold=True)
    digest = hmac.new(key, struct.pack(">Q", int(at) // 30), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return f"{code % 1_000_000:06d}"


def test_otp_site_needs_the_key_and_a_one_time_code(tmp_path: Path) -> None:
    _needs("otp")
    refused = _ssh("otp", "true", timeout=30)
    assert refused.returncode == 255
    assert "Permission denied" in refused.stderr

    prompts = tmp_path / "prompts.txt"
    code_file = tmp_path / "code.txt"
    askpass = tmp_path / "askpass.sh"
    askpass.write_text(
        f'#!/bin/sh\nprintf "%s\\n" "$1" >> {shlex.quote(str(prompts))}\n'
        f"cat {shlex.quote(str(code_file))}\n",
        encoding="utf-8",
    )
    askpass.chmod(0o700)
    code_file.write_text(_totp(_read_secret("FX_OTP_SECRET_FILE"), time.time()) + "\n")
    result = subprocess.run(
        [
            "ssh",
            "-F",
            FX["FX_SSH_CONFIG"],
            "-o",
            "BatchMode=no",
            "-o",
            "NumberOfPasswordPrompts=1",
            "-o",
            "PreferredAuthentications=publickey,keyboard-interactive",
            "fx-otp",
            "hostname",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        stdin=subprocess.DEVNULL,
        env={**os.environ, "SSH_ASKPASS": str(askpass), "SSH_ASKPASS_REQUIRE": "force"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "fx-otp"
    assert "Verification code" in prompts.read_text(encoding="utf-8")


# ------------------------------------------------------------------ secret service


def test_secret_service_checks_the_secret_over_http_and_websocket() -> None:
    _needs("secret")
    base = FX["FX_SECRET_URL"]
    secret = _read_secret("FX_SECRET_FILE")
    assert httpx.get(f"{base}/health", timeout=10).text == "ok\n"
    assert httpx.get(f"{base}/", timeout=10).status_code == 401
    wrong = httpx.get(f"{base}/", headers={"Authorization": "Bearer wrong"}, timeout=10)
    assert wrong.status_code == 401
    good = httpx.get(f"{base}/", headers={"Authorization": f"Bearer {secret}"}, timeout=10)
    assert good.status_code == 200
    assert good.json()["service"] == "clio-relay-fixture-secret-service"

    ws_base = base.replace("http://", "ws://", 1)
    with pytest.raises(InvalidStatus) as refused:
        ws_connect(f"{ws_base}/ws", open_timeout=10).close()
    assert refused.value.response.status_code == 401
    with ws_connect(f"{ws_base}/ws?token={secret}", open_timeout=10) as socket_:
        socket_.send("hello fixture")
        assert socket_.recv(timeout=10) == "hello fixture"


def test_secret_service_writes_its_final_artifact_on_a_site() -> None:
    _needs("ssh")
    artifact = f"/tmp/fx-final-{uuid.uuid4().hex}.json"
    script = f"""
set -e
export FIXTURE_SECRET=site-local
clio-fixture-secret-service --port 18080 --final-artifact {artifact} >/tmp/fx-svc.log 2>&1 &
pid=$!
for i in $(seq 50); do curl -fs http://127.0.0.1:18080/health >/dev/null && break; sleep 0.2; done
curl -fs -H 'Authorization: Bearer site-local' http://127.0.0.1:18080/ >/dev/null
kill -TERM $pid
wait $pid
cat {artifact}
rm -f {artifact}
"""
    record = json.loads(_ssh_ok("ssh", script))
    assert record["service"] == "clio-relay-fixture-secret-service"
    assert record["requests"] >= 2


# ------------------------------------------------------------------ frp


@contextmanager
def _frpc(config: str, tmp_path: Path, name: str) -> Generator[subprocess.Popen[str]]:
    path = tmp_path / f"{name}.toml"
    path.write_text(config, encoding="utf-8")
    path.chmod(0o600)
    process = subprocess.Popen(
        [FX["FX_FRPC"], "-c", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        yield process
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def _frpc_config(protocol: str, token: str, extra: str = "") -> str:
    port = FX["FX_FRPS_WSS_PORT"] if protocol == "wss" else FX["FX_FRPS_PORT"]
    return (
        f'serverAddr = "{FX["FX_CONNECT_ADDR"]}"\n'
        f"serverPort = {port}\n"
        "loginFailExit = true\n"
        'auth.method = "token"\n'
        f'auth.token = "{token}"\n'
        f'transport.protocol = "{protocol}"\n'
        'log.to = "console"\n'
        f"{extra}"
    )


@pytest.mark.parametrize("protocol", ["tcp", "websocket", "wss"])
def test_frps_carries_an_stcp_tunnel_to_the_secret_service(protocol: str, tmp_path: Path) -> None:
    _needs("frps")
    _needs("secret")
    tag = uuid.uuid4().hex[:12]
    visitor_port = _free_port()
    secret_port = FX["FX_SECRET_URL"].rsplit(":", 1)[1]
    proxies = f"""
[[proxies]]
name = "fx-secret-{protocol}-{tag}"
type = "stcp"
secretKey = "{tag}"
localIP = "{FX["FX_CONNECT_ADDR"]}"
localPort = {secret_port}

[[visitors]]
name = "fx-visitor-{protocol}-{tag}"
type = "stcp"
serverName = "fx-secret-{protocol}-{tag}"
secretKey = "{tag}"
bindAddr = "127.0.0.1"
bindPort = {visitor_port}
"""
    config = _frpc_config(protocol, _read_secret("FX_FRP_TOKEN_FILE"), proxies)
    secret = _read_secret("FX_SECRET_FILE")
    with _frpc(config, tmp_path, f"frpc-{protocol}") as process:

        def through_tunnel() -> httpx.Response | None:
            assert process.poll() is None, process.stdout.read() if process.stdout else ""
            try:
                return httpx.get(
                    f"http://127.0.0.1:{visitor_port}/",
                    headers={"Authorization": f"Bearer {secret}"},
                    timeout=5,
                )
            except httpx.HTTPError:
                return None

        response = _poll(f"{protocol} tunnel", 30, through_tunnel)
        assert response.status_code == 200
        assert response.json()["service"] == "clio-relay-fixture-secret-service"


def test_frps_refuses_a_wrong_token(tmp_path: Path) -> None:
    _needs("frps")
    config = _frpc_config("tcp", "not-the-token")
    with _frpc(config, tmp_path, "frpc-bad-token") as process:
        returncode = process.wait(timeout=30)
        output = process.stdout.read() if process.stdout else ""
    assert returncode != 0
    assert "token" in output.lower()


# ------------------------------------------------------------------ counter MCP


def _structured(result: Any) -> dict[str, Any]:
    content = cast(dict[str, Any] | None, result.structured_content)
    assert content is not None, result
    return content


def test_counter_mcp_keeps_one_session_and_runs_a_task() -> None:
    _needs("counter")
    from fastmcp import Client
    from fastmcp_tasks import ToolTask, call_tool_task  # pyright: ignore[reportUnknownVariableType]

    # fastmcp_tasks types its client parameter as a bare generic Client.
    start_task = cast(Callable[..., Awaitable[ToolTask]], call_tool_task)

    async def scenario() -> None:
        async with Client(FX["FX_COUNTER_URL"]) as client:
            names = {tool.name for tool in await client.list_tools()}
            assert {"increment", "read", "reset", "count_slowly"} <= names
            start = _structured(await client.call_tool("read"))["count"]
            results = [_structured(await client.call_tool("increment")) for _ in range(3)]
            assert [r["count"] for r in results] == [start + 1, start + 2, start + 3]
            assert len({r["session_id"] for r in results}) == 1

            task = await start_task(client, "count_slowly", {"steps": 3, "delay_seconds": 0.2})
            assert task.task_id
            final = _structured(await task.result())
            assert final["background_task"] is True
            assert final["count"] >= start + 6

    asyncio.run(scenario())


def test_counter_mcp_runs_over_ssh_stdio() -> None:
    _needs("ssh")
    from fastmcp import Client
    from fastmcp.client.transports import StdioTransport

    transport = StdioTransport(
        command="ssh",
        args=[
            "-F",
            FX["FX_SSH_CONFIG"],
            "-o",
            "BatchMode=yes",
            "fx-ssh",
            "clio-fixture-counter-mcp",
        ],
    )

    async def scenario() -> None:
        async with Client(transport) as client:
            results = [_structured(await client.call_tool("increment")) for _ in range(3)]
            assert [r["count"] for r in results] == [1, 2, 3]

    asyncio.run(scenario())
