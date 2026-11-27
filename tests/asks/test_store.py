"""Relay's one secret store: the keyring, or a private file when there is none (RL28)."""

from __future__ import annotations

import json
import stat
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from clio_relay.identity import GrantFiles, Identities, IdentityContext, TokenStore
from clio_relay.identity.store import FILE_NAME
from clio_relay.identity.tacc import TaccIdentity, TaccRecord, TaccSettings
from clio_relay.jobs.refusals import Refusal, RefusalReason

pytestmark = [pytest.mark.release_platform("posix")]


@pytest.fixture
def no_keyring(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Give ``keyring`` its ``fail`` backend, as on a machine with no Secret Service."""
    keyring = pytest.importorskip("keyring")
    fail = pytest.importorskip("keyring.backends.fail")
    monkeypatch.delenv("CLIO_RELAY_KEYRING")
    before = keyring.get_keyring()
    keyring.set_keyring(fail.Keyring())
    try:
        yield
    finally:
        keyring.set_keyring(before)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_with_no_keyring_entries_go_to_a_private_file(no_keyring: None, tmp_path: Path) -> None:
    store = TokenStore(config_dir=tmp_path)
    assert store.kind == "file"
    assert store.location()["path"] == str(tmp_path / FILE_NAME)
    assert "no usable backend" in (store.location()["why"] or "")
    assert store.get("identity:globus") is None
    assert not (tmp_path / FILE_NAME).exists(), "a read creates nothing"
    store.set("identity:globus", {"tokens": [{"access_token": "kept"}]})
    store.set_secret("cloud", "typed")
    assert _mode(tmp_path / FILE_NAME) == 0o600
    again = TokenStore(config_dir=tmp_path)
    assert again.get("identity:globus") == {"tokens": [{"access_token": "kept"}]}
    assert again.secret("cloud") == "typed"
    again.delete("identity:globus")
    again.delete("identity:globus")
    assert store.get("identity:globus") is None and store.secret("cloud") == "typed"
    leftovers = sorted(item.name for item in tmp_path.iterdir() if item.name.endswith(".tmp"))
    assert leftovers == []


def test_a_file_others_can_read_is_tightened(no_keyring: None, tmp_path: Path) -> None:
    store = TokenStore(config_dir=tmp_path)
    store.set("entry", {"a": 1})
    (tmp_path / FILE_NAME).chmod(0o644)
    assert store.get("entry") == {"a": 1}
    assert _mode(tmp_path / FILE_NAME) == 0o600


def test_a_broken_file_reads_as_signed_out_and_refuses_writes(
    no_keyring: None, tmp_path: Path
) -> None:
    (tmp_path / FILE_NAME).write_text("{not json", encoding="utf-8")
    store = TokenStore(config_dir=tmp_path)
    assert store.get("entry") is None
    with pytest.raises(Refusal) as refused:
        store.set("entry", {"a": 1})
    assert refused.value.reason is RefusalReason.ADAPTER_UNAVAILABLE
    assert (tmp_path / FILE_NAME).read_text(encoding="utf-8") == "{not json"


def test_a_keyring_asked_for_by_name_refuses_rather_than_raising(
    no_keyring: None, tmp_path: Path
) -> None:
    store = TokenStore("keyring", config_dir=tmp_path)
    assert store.kind == "keyring"
    assert store.get("entry") is None
    for write in (lambda: store.set("entry", {"a": 1}), lambda: store.delete("entry")):
        with pytest.raises(Refusal) as refused:
            write()
        assert refused.value.reason is RefusalReason.ADAPTER_UNAVAILABLE
    assert not (tmp_path / FILE_NAME).exists()


def test_the_file_can_be_chosen_by_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIO_RELAY_KEYRING", "file")
    monkeypatch.setenv("CLIO_RELAY_CONFIG_DIR", str(tmp_path / "config"))
    store = TokenStore()
    assert store.kind == "file" and store.source == "file"
    store.set("entry", {"a": 1})
    assert json.loads((tmp_path / "config" / FILE_NAME).read_text(encoding="utf-8")) == {
        "clio-relay": {"entry": '{"a": 1}'}
    }
    assert _mode(tmp_path / "config") == 0o700


@pytest.mark.asyncio
async def test_tacc_with_no_keyring_reads_signed_out_then_keeps_its_sign_in(
    no_keyring: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CONFIG_DIR", str(tmp_path))
    settings = TaccSettings(base_url="https://tacc.stub.invalid")
    first = TaccIdentity(settings=settings)
    status = await first.status()
    assert status.state == "signed_out"
    assert (status.stored_in, status.store_path) == ("file", str(tmp_path / FILE_NAME))
    record = TaccRecord(access_token="header.e30.sig", access_expires_at=time.time() + 3600)
    first.store.set(first.key, record.model_dump(mode="json"))

    restarted = TaccIdentity(settings=settings)
    status = await restarted.status()
    assert (status.state, status.source, status.stored_in) == ("signed_in", "file", "file")
    assert await restarted.access_token() == "header.e30.sig"
    assert _mode(tmp_path / FILE_NAME) == 0o600
    restarted.store.delete(restarted.key)
    assert (await TaccIdentity(settings=settings).status()).state == "signed_out"
    assert json.loads((tmp_path / FILE_NAME).read_text(encoding="utf-8")) == {}


@pytest.mark.asyncio
async def test_identity_statuses_say_where_sign_ins_are_kept(
    no_keyring: None, tmp_path: Path
) -> None:
    identities = Identities(
        IdentityContext(store=TokenStore(config_dir=tmp_path), grants=GrantFiles())
    )
    statuses = await identities.statuses()
    assert statuses and all(
        (item.stored_in, item.store_path) == ("file", str(tmp_path / FILE_NAME))
        for item in statuses
    )
