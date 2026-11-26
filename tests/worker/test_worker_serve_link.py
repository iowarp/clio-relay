"""A worker site's service meets the desktop through the worker's own link."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.worker.adapter import Rendezvous, WorkerAdapter, WorkerConfig, WorkerLink
from clio_relay.jobs.content import ContentStore
from clio_relay.serve.config import Rendezvous as ServeRendezvous
from clio_relay.serve.config import SiteServe, with_link
from clio_relay.serve.planning import path_of


def _linked(tmp_path: Path, *, stored: bool = True, **config: Any) -> WorkerAdapter:
    context = AdapterContext(
        site_id="site", state_dir=tmp_path / "adapter", content=ContentStore(tmp_path / "content")
    )
    rendezvous = Rendezvous(
        address="frps.example.org", port=443, site_address="10.0.0.9", token_file="~/t"
    )
    adapter = WorkerAdapter(
        WorkerConfig(ssh_profile="site-login", rendezvous=rendezvous, **config), context
    )
    link = WorkerLink(
        cluster="c",
        session_id="s",
        generation_id="g",
        api_token="a",
        stcp_secret="k",
        proxy_name="p",
        port=8765,
        remote_dir="/home/u/.clio-relay-w/",
        link_mode="udp_rendezvous",
    )
    if stored:
        adapter._store_link(link)  # pyright: ignore[reportPrivateUsage]
    return adapter


def test_a_worker_site_serves_through_its_own_link_with_no_serve_settings(
    tmp_path: Path,
) -> None:
    # Before this, a worker site's service took ssh unless a person ran rendezvous set.
    adapter = _linked(tmp_path)
    assert _linked(tmp_path / "unlinked", stored=False).serve_link() is None

    rendezvous, site = with_link(adapter.serve_link(), None, SiteServe())

    assert rendezvous is not None
    assert (rendezvous.address, rendezvous.port, rendezvous.protocol) == (
        "frps.example.org",
        443,
        "wss",
    )
    assert rendezvous.token_file == "~/t"
    assert rendezvous.direct is True
    assert site.frpc == "/home/u/.clio-relay-w/bin/frpc"
    assert site.address == "10.0.0.9"
    (tmp_path / "t").write_text("token", encoding="utf-8")
    rendezvous = rendezvous.model_copy(update={"token_file": str(tmp_path / "t")})
    assert path_of(adapter, site, rendezvous) == "frp"


def test_serve_settings_a_person_set_win_over_the_worker_link(tmp_path: Path) -> None:
    link = _linked(tmp_path, site_frpc="/opt/frpc").serve_link()
    own = ServeRendezvous(address="mine.example.org", port=7000, token_env="MINE")

    rendezvous, site = with_link(link, own, SiteServe(frpc_binary="./frpc"))

    assert rendezvous == own
    assert site.frpc is None
    assert site.address is None
    _, site = with_link(link, own, SiteServe())
    assert site.frpc == "/opt/frpc"
