"""Endpoint identity in the site store: one place, reached one way, is one endpoint.

The store compares what a config reaches (the adapter's ``reaches``), not its
settings: an alias and ``user@host`` for the same host and user are the same
endpoint, and changing a setting updates the row instead of making another.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.adapters.ssh import hosts as ssh_hosts
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.settings import RelayPaths


@pytest.fixture
def store(relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SiteStore:
    home = tmp_path / "ssh"
    home.mkdir()
    (home / "config").write_text(
        "Host lab\n  HostName Lab.Example.org\n  User me\n", encoding="utf-8"
    )
    monkeypatch.setattr(ssh_hosts, "ssh_dir", lambda: home)
    return SiteStore(relay_paths.sites_file)


def _ssh(site_id: str, profile: str, **config: object) -> SiteEntry:
    return SiteEntry(id=site_id, adapter="ssh", config={"ssh_profile": profile, **config})


def test_an_alias_and_user_at_host_for_one_place_are_one_endpoint(store: SiteStore) -> None:
    store.add(_ssh("lab", "lab"))
    with pytest.raises(Refusal) as refused:
        store.add(_ssh("lab-2", "me@lab.example.org."))
    assert refused.value.reason is RefusalReason.SITE_EXISTS
    assert refused.value.data["existing"] == "lab"
    assert (refused.value.data["host"], refused.value.data["user"]) == ("lab.example.org", "me")
    assert [site.id for site in store.list()] == ["lab"]


def test_another_user_or_protocol_is_another_endpoint(store: SiteStore) -> None:
    store.add(_ssh("lab", "lab"))
    store.add(_ssh("lab-them", "them@lab.example.org"))
    store.add(SiteEntry(id="lab-worker", adapter="worker", config={"ssh_profile": "lab"}))
    assert [site.id for site in store.list()] == ["lab", "lab-them", "lab-worker"]


def test_changing_a_setting_updates_the_endpoint(store: SiteStore) -> None:
    store.add(_ssh("lab", "lab"))
    updated = store.edit("lab", {"config": {"ssh_profile": "lab", "remote_dir": "~/elsewhere"}})
    assert updated.config["remote_dir"] == "~/elsewhere"
    # Reaching it another way that lands on the same place is still this endpoint.
    store.edit("lab", {"config": {"ssh_profile": "me@lab.example.org"}})
    assert [site.id for site in store.list()] == ["lab"]


def test_an_edit_cannot_turn_one_endpoint_into_another(store: SiteStore) -> None:
    store.add(_ssh("lab", "lab"))
    store.add(_ssh("other", "me@other.example.org"))
    with pytest.raises(Refusal) as refused:
        store.edit("other", {"config": {"ssh_profile": "me@lab.example.org"}})
    assert refused.value.data["existing"] == "lab"
    assert store.get("other").config == {"ssh_profile": "me@other.example.org"}


def test_a_site_relay_cannot_place_is_never_refused_as_a_repeat(store: SiteStore) -> None:
    # Local reaches no remote place, so two local rows stay two names for this computer.
    store.add(SiteEntry(id="local", name="This computer", adapter="local"))
    store.add(SiteEntry(id="mine", name="My laptop", adapter="local"))
    assert [site.id for site in store.list()] == ["local", "mine"]
