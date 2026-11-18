from __future__ import annotations

import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from clio_relay.adapters.hooks import set_hook
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.stubs import DefaultGuard

# The Slurm suite needs a directory the compute nodes share; without one it is not collected.
collect_ignore = [] if os.environ.get("RELAY_TEST_SLURM_DIR") else ["test_conformance_slurm.py"]


@pytest.fixture
def relay_paths(tmp_path: Path) -> RelayPaths:
    return RelayPaths(config_dir=tmp_path / "config", state_dir=tmp_path / "state")


def add_site(
    paths: RelayPaths,
    site_id: str = "fx",
    *,
    adapter: str = "fixture",
    config: dict[str, Any] | None = None,
    policy: str = "open",
    spend: str | float = "never",
) -> SiteEntry:
    entry = SiteEntry.model_validate(
        {
            "id": site_id,
            "adapter": adapter,
            "config": config if config is not None else {"queue_s": 0.2, "run_s": 0.3},
            "policy": policy,
            "spend": {"ask": spend},
        }
    )
    return SiteStore(paths.sites_file).add(entry)


def answer_page(
    url: str,
    action: str = "accept",
    value: str | None = None,
    auth: tuple[str, str] | None = None,
) -> int:
    """Open a loopback ask page and submit it the way a person would.

    Over HTTP the page sits behind the operator credential, passed as ``auth``.
    """
    with httpx.Client(timeout=10, auth=auth) as client:
        page = client.get(url)
        page.raise_for_status()
        token = re.search(r"name='t' value='([^']+)'", page.text)
        csrf = re.search(r"name='csrf' value='([^']+)'", page.text)
        assert token is not None and csrf is not None, page.text
        form = {"t": token.group(1), "csrf": csrf.group(1), "action": action}
        if value is not None:
            form["value"] = value
        posted = client.post(url.split("?", 1)[0], data=form)
        return posted.status_code


@pytest.fixture
def stub_guard() -> Iterator[None]:
    """Use relay's guard stub even when a guard package is installed."""
    set_hook("guard", DefaultGuard)
    try:
        yield
    finally:
        set_hook("guard", None)
