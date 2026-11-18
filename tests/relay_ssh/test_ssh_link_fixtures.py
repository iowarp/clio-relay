"""Live checks on W2's SSH sites: clear errors, and calls that outlive a dropped link.

The link checks go through a local TCP proxy in front of fx-slurm that can
refuse connections, close one as it opens, or cut one partway through. The
proxy waits on events, not on clocks, so a slow site only makes them longer.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import subprocess
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path

import pytest

from clio_relay.adapters.ssh import SshAdapter
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobSpec
from tests.relay_ssh.test_ssh_fixtures import (
    RUN_DIR,
    build,
    site_shell,
    ssh_children,
    wait_final,
)

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]


def host_block(
    fx: dict[str, str],
    name: str,
    *,
    port: int,
    alias: str | None = None,
    identity: str | None = "",
    known_hosts: str | None = None,
) -> str:
    """Return a ``Host`` block like the fixtures', with one thing changed."""
    lines = [
        f"Host {name}",
        "    HostName 127.0.0.1",
        f"    Port {port}",
        f"    User {fx['FX_USER']}",
        "    IdentitiesOnly yes",
        "    IdentityAgent none",
        f"    UserKnownHostsFile {known_hosts or fx['FX_KNOWN_HOSTS']}",
        "    GlobalKnownHostsFile /dev/null",
        "    StrictHostKeyChecking yes",
        "    ConnectTimeout 10",
    ]
    if identity is not None:
        lines.append(f"    IdentityFile {identity or fx['FX_SSH_KEY']}")
    if alias:
        lines.append(f"    HostKeyAlias {alias}")
    return "\n".join(lines) + "\n"


def config_with(tmp_path: Path, block: str) -> str:
    path = tmp_path / "ssh_config"
    path.write_text(block)
    path.chmod(0o600)
    return str(path)


async def refusal_of(adapter: SshAdapter) -> Refusal:
    with pytest.raises(Refusal) as refused:
        await adapter.describe()
    assert ssh_children() == []
    return refused.value


@pytest.fixture(scope="module", autouse=True)
def _clean_remote(fx: dict[str, str]) -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    yield
    if os.environ.get("RELAY_SSH_KEEP"):
        return
    subprocess.run(
        ["ssh", "-F", fx["FX_SSH_CONFIG"], "-o", "BatchMode=yes", "fx-slurm", f"rm -rf {RUN_DIR}"],
        check=False,
        capture_output=True,
        timeout=120,
    )


# errors


async def test_no_key_says_how_to_offer_one(fx: dict[str, str], tmp_path: Path) -> None:
    port = int(fx["FX_SSH_PORT"])
    adapter = build(
        fx,
        tmp_path,
        "fx-ssh",
        ssh_config=config_with(tmp_path, host_block(fx, "fx-ssh", port=port, identity=None)),
    )
    refused = await refusal_of(adapter)
    assert refused.reason == RefusalReason.UNREACHABLE
    assert refused.data["cause"] == "key_refused"
    assert "ssh-add" in refused.message


async def test_a_missing_key_file_is_named(fx: dict[str, str], tmp_path: Path) -> None:
    gone = tmp_path / "no_such_key"
    block = host_block(fx, "fx-ssh", port=int(fx["FX_SSH_PORT"]), identity=str(gone))
    adapter = build(fx, tmp_path, "fx-ssh", ssh_config=config_with(tmp_path, block))
    refused = await refusal_of(adapter)
    assert refused.data["cause"] == "key_refused"
    assert str(gone) in refused.message


async def test_an_unknown_host_key_is_never_accepted(fx: dict[str, str], tmp_path: Path) -> None:
    empty = tmp_path / "known_hosts"
    empty.write_text("")
    block = host_block(fx, "fx-ssh", port=int(fx["FX_SSH_PORT"]), known_hosts=str(empty))
    adapter = build(fx, tmp_path, "fx-ssh", ssh_config=config_with(tmp_path, block))
    refused = await refusal_of(adapter)
    assert refused.data["cause"] == "host_key_unknown"
    assert "ssh fx-ssh" in refused.message
    assert empty.read_text() == ""


async def test_a_changed_host_key_names_the_new_fingerprint(
    fx: dict[str, str], tmp_path: Path
) -> None:
    other = tmp_path / "other"
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(other)],
        check=True,
        capture_output=True,
    )
    port = int(fx["FX_SSH_PORT"])
    known = tmp_path / "known_hosts"
    known.write_text(f"[127.0.0.1]:{port} {other.with_suffix('.pub').read_text()}")
    block = host_block(fx, "fx-ssh", port=port, known_hosts=str(known))
    adapter = build(fx, tmp_path, "fx-ssh", ssh_config=config_with(tmp_path, block))
    refused = await refusal_of(adapter)
    assert refused.data["cause"] == "host_key_changed"
    assert "SHA256:" in refused.message
    assert "ssh-keygen" in refused.message
    assert "-R" in refused.message


async def test_an_unknown_profile_is_named(fx: dict[str, str], tmp_path: Path) -> None:
    adapter = build(fx, tmp_path, "fx-nosuch.invalid")
    refused = await refusal_of(adapter)
    assert refused.data["cause"] == "unknown_host"


# a dropped link


class Proxy:
    """A TCP proxy to one port that can refuse, close or cut its connections."""

    def __init__(self, target: int) -> None:
        self.target = target
        self.port = 0
        self.opened = 0
        self.close_next = 0
        self.cut_after: int | None = None
        self.cut = asyncio.Event()
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.port)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        """Stop listening, so new connections are refused."""
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    def drop(self) -> None:
        """Close every open connection, as a dropped link would."""
        for writer in list(self._writers):
            writer.transport.abort()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.opened += 1
        if self.close_next > 0:
            self.close_next -= 1
            writer.close()
            return
        try:
            up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.target)
        except OSError:
            writer.close()
            return
        self._writers.update((writer, up_writer))
        sent = 0

        async def pump(source: asyncio.StreamReader, sink: asyncio.StreamWriter, up: bool) -> None:
            nonlocal sent
            with contextlib.suppress(OSError, ConnectionError):
                while data := await source.read(65536):
                    sink.write(data)
                    await sink.drain()
                    if up:
                        sent += len(data)
                        if self.cut_after is not None and sent > self.cut_after:
                            self.cut_after = None
                            self.cut.set()
                            break
            for end in (writer, up_writer):
                end.close()

        await asyncio.gather(pump(reader, up_writer, True), pump(up_reader, writer, False))
        self._writers.difference_update((writer, up_writer))


@contextlib.asynccontextmanager
async def proxied(fx: dict[str, str]) -> AsyncGenerator[Proxy]:
    proxy = Proxy(int(fx["FX_SLURM_PORT"]))
    await proxy.start()
    try:
        yield proxy
    finally:
        await proxy.stop()


def through(fx: dict[str, str], tmp_path: Path, proxy: Proxy, **config: object) -> SshAdapter:
    block = host_block(fx, "fx-slurm", port=proxy.port, alias=f"[127.0.0.1]:{fx['FX_SLURM_PORT']}")
    return build(fx, tmp_path, "fx-slurm", ssh_config=config_with(tmp_path, block), **config)


async def test_a_run_outlives_a_cut_copy_and_closed_connections(
    fx: dict[str, str], tmp_path: Path
) -> None:
    """The copy is cut partway and two connections close as they open; the job runs once."""
    async with proxied(fx) as proxy:
        adapter = through(fx, tmp_path, proxy)
        data = os.urandom(24 * 1024 * 1024)
        digest = adapter.context.content.put(data)
        del data
        spec = JobSpec(
            executable="sha256sum",
            arguments=["big.bin"],
            inputs=[InputRef(name="big.bin", sha256=digest)],
        )
        operation_id = new_id("op")
        proxy.close_next = 1
        proxy.cut_after = 8 * 1024 * 1024
        handle = await adapter.run(spec, operation_id=operation_id)
        assert proxy.cut.is_set()
        proxy.close_next = 1
        final = await wait_final(adapter, handle)
        assert final.state.value == "completed", final
        assert proxy.close_next == 0
        files = await adapter.files()
        stdout = (await files.read(f"{handle.data['job_dir']}/stdout")).decode()
        assert stdout.split()[0] == digest
        again = await adapter.find(operation_id)
        assert again is not None
        assert again.native_id == handle.native_id


async def test_a_status_waits_out_an_outage(
    fx: dict[str, str], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with proxied(fx) as proxy:
        adapter = through(fx, tmp_path, proxy, retries=8)
        handle = await adapter.run(
            JobSpec(executable="sh", arguments=["-c", "echo outage"]), operation_id=new_id("op")
        )
        await proxy.stop()
        caplog.set_level(logging.WARNING, logger="clio_relay.adapters.ssh.transport")
        polling = asyncio.create_task(adapter.status(handle))
        while not any("retry 2 of 8" in record.getMessage() for record in caplog.records):
            assert not polling.done(), polling.result()
            await asyncio.sleep(0.2)
        await proxy.start()
        status = await polling
        assert status.state.value in ("queued", "running", "completed"), status
        assert (await wait_final(adapter, handle)).state.value == "completed"


async def test_exec_is_not_repeated_after_its_session_drops(
    fx: dict[str, str], tmp_path: Path
) -> None:
    marker = f"{RUN_DIR}/exec-{new_id('m')}"
    command = ["sh", "-c", f"mkdir -p {RUN_DIR}; echo x >> {marker}; sleep 60"]
    async with proxied(fx) as proxy:
        adapter = through(fx, tmp_path, proxy)
        running = asyncio.create_task(adapter.exec(command, max_output_bytes=4096))
        while site_shell(fx, "fx-slurm", f"cat {marker} 2>/dev/null || true").strip() != "x":
            assert not running.done(), running.result()
            await asyncio.sleep(0.5)
        proxy.drop()
        with pytest.raises(Refusal) as refused:
            await running
        assert refused.value.data["cause"] == "network", refused.value.message
        assert proxy.opened == 1
    assert site_shell(fx, "fx-slurm", f"cat {marker}").strip() == "x"
