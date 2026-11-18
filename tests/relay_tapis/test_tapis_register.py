"""LQ40: registering a person's own host as a Tapis system, through its ssh alternative.

The host is W2's ``fx-ssh`` fixture. The stub checks the credential the way
Tapis does, by logging in with the private key relay registered, so the test
passes only when the key relay put in ``authorized_keys`` really works. The
key line carries a comment naming relay and the system, so other keys in the
fixture's ``authorized_keys`` are left as they are.
"""

from __future__ import annotations

import contextlib
import os
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import cast

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.tapis import TapisAdapter, TapisConfig
from clio_relay.adapters.tapis.register import (
    SshHost,
    key_comment,
    register_host,
    unregister_host,
)
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import (
    USERNAME,
    sign_in,
    system_id,
    tapis_config,
    use_memory_keyring,
)

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]


def _adapter(stub: TapisStub, site_id: str, tmp_path: Path) -> TapisAdapter:
    context = AdapterContext(
        site_id=site_id,
        state_dir=tmp_path / "relay",
        content=ContentStore(tmp_path / "content"),
    )
    return TapisAdapter(TapisConfig.model_validate(tapis_config(stub)), context)


def _ssh(fx: dict[str, str], profile: str) -> SshHost:
    return SshHost.from_alternative({"ssh_profile": profile, "ssh_config": fx["FX_SSH_CONFIG"]})


def _private_files(root: Path) -> list[Path]:
    return sorted(Path(item) for item in root.glob("clio-relay-tapis-key-*"))


_opened: list[list[str]] = []
_hooked = False


def _record_open(event: str, args: tuple[object, ...]) -> None:
    if event == "open" and _opened and args and isinstance(args[0], str | bytes | os.PathLike):
        _opened[-1].append(os.fsdecode(cast("str | bytes | os.PathLike[str]", args[0])))


@contextlib.contextmanager
def _files_opened() -> Generator[list[str]]:
    """Record every file this process opens inside the block (an audit hook)."""
    global _hooked
    if not _hooked:
        sys.addaudithook(_record_open)
        _hooked = True
    seen: list[str] = []
    _opened.append(seen)
    try:
        yield seen
    finally:
        _opened.remove(seen)


def _key_dirs(fx: dict[str, str]) -> list[Path]:
    """Where the person's own keys live: ~/.ssh and the fixture's ssh directory."""
    return [(Path.home() / ".ssh").resolve(), Path(fx["FX_SSH_CONFIG"]).resolve().parent]


async def test_register_installs_a_working_key_and_removal_leaves_nothing(
    stub: TapisStub,
    fx: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_memory_keyring(monkeypatch)
    (tmp_path / "keys").mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "keys"))
    await sign_in(stub)
    site = "lq40"
    adapter = _adapter(stub, site, tmp_path)
    ssh = _ssh(fx, "fx-ssh")
    found = system_id(site)
    comment = key_comment(found)
    assert not ssh.has_key(comment)
    try:
        await _register_and_remove(stub, fx, tmp_path, adapter, ssh, found)
    finally:
        # A failed run must not leave a line in the shared fixture's authorized_keys.
        ssh.remove_key(comment)


async def _register_and_remove(
    stub: TapisStub,
    fx: dict[str, str],
    tmp_path: Path,
    adapter: TapisAdapter,
    ssh: SshHost,
    found: str,
) -> None:
    comment = key_comment(found)
    with _files_opened() as opened:
        registered = await register_host(adapter, ssh)
    print(f"LQ40 registration {registered}")
    config_file = Path(fx["FX_SSH_CONFIG"]).resolve()
    key_reads = [
        path
        for path in opened
        if Path(path).resolve() != config_file
        and any(Path(path).resolve().is_relative_to(root) for root in _key_dirs(fx))
    ]
    print(f"LQ40 files opened {len(opened)}, under the person's key directories {key_reads}")
    assert key_reads == [], "relay read a file where the person's keys live"
    assert registered.system_id == found
    assert registered.created_system
    assert registered.login_user == fx["FX_USER"]
    assert registered.key_installed
    assert registered.private_key_deleted
    assert _private_files(tmp_path / "keys") == []
    assert ssh.has_key(comment)
    system = stub.state.systems[found]
    assert system["defaultAuthnMethod"] == "PKI_KEYS"
    assert system["host"] == "127.0.0.1" and system["port"] == int(fx["FX_SSH_PORT"])
    assert system["jobWorkingDir"].endswith("/.clio-relay/tapis")
    credential = stub.state.credentials[(found, USERNAME)]
    assert credential["loginUser"] == fx["FX_USER"]
    assert credential["publicKey"].split()[1] in ssh.run(
        "cat ~/.ssh/authorized_keys", "reading authorized_keys"
    )
    described = await adapter.describe()
    assert described.facts["credential"] == "ok"
    assert described.facts["system_id"] == found

    removed = await unregister_host(adapter, ssh)
    print(f"LQ40 removal {removed}")
    assert removed == {
        "system_id": found,
        "credential": True,
        "system": True,
        "key_line": True,
    }
    assert (found, USERNAME) not in stub.state.credentials
    assert found not in stub.state.systems
    assert not ssh.has_key(comment)


async def test_a_refused_credential_takes_the_key_back_out(
    stub: TapisStub,
    fx: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    site = "lq40-refused"
    adapter = _adapter(stub, site, tmp_path)
    ssh = _ssh(fx, "fx-ssh")
    comment = key_comment(system_id(site))
    monkeypatch.setattr(stub.state, "refuse_credential", "Credential check failed: no login")
    try:
        with pytest.raises(Refusal) as refused:
            await register_host(adapter, ssh)
    finally:
        await unregister_host(adapter, ssh)
    print(f"LQ40 refusal {refused.value.to_dict()}")
    assert refused.value.reason == RefusalReason.SUBMIT_FAILED
    assert "no login" in refused.value.message
    assert not ssh.has_key(comment)


async def test_slurm_partitions_become_logical_queues(
    stub: TapisStub,
    fx: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    site = "lq40-slurm"
    adapter = _adapter(stub, site, tmp_path)
    ssh = _ssh(fx, "fx-slurm")
    partitions = ssh.partitions()
    assert partitions, "the Slurm fixture has partitions"
    try:
        registered = await register_host(adapter, ssh)
        print(f"LQ40 slurm registration {registered}")
        system = stub.state.systems[system_id(site)]
        assert system["batchScheduler"] == "SLURM"
        assert [item["name"] for item in system["batchLogicalQueues"]] == partitions
        assert registered.queues == partitions
        assert (await adapter.describe()).facts["job_type"] == "BATCH"
    finally:
        removed = await unregister_host(adapter, ssh)
    assert removed["key_line"] is True
