"""Helpers the Tapis tests share: a signed-in identity and a seeded system."""

from __future__ import annotations

import io
import tarfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from clio_relay.identity.tacc import MemoryStore, TaccIdentity, TaccSettings, default_store

from . import TapisStub

CLIENT_ID = "clio-relay-test"
CLIENT_KEY = "stub-client-key-not-a-secret"
USERNAME = "alice"


def use_memory_keyring(env: Any) -> MemoryStore:
    """Point relay at this process's memory keyring and an empty TACC record.

    ``env`` is pytest's ``monkeypatch``.
    """
    env.setenv("CLIO_RELAY_KEYRING", "memory")
    env.setenv("CLIO_RELAY_TACC_CLIENT_ID", CLIENT_ID)
    env.setenv("CLIO_RELAY_TACC_CLIENT_KEY", CLIENT_KEY)
    store = default_store()
    assert isinstance(store, MemoryStore)
    store.values.clear()
    return store


def identity(stub: TapisStub, **settings: Any) -> TaccIdentity:
    return TaccIdentity(
        settings=TaccSettings.resolve(None, base_url=stub.base_url, tenant_id="tacc", **settings)
    )


async def sign_in(stub: TapisStub, username: str = USERNAME) -> TaccIdentity:
    """Sign in through the stub's device code endpoints, as the person would."""
    found = identity(stub)
    step = await found.begin()
    assert stub.state.approve(step.display["user_code"], username)
    result = await found.poll(force=True)
    assert result.state == "done", result
    return found


def system_id(site_id: str, username: str = USERNAME) -> str:
    return f"clio-relay.{username}.{site_id}"


def seed_host(
    stub: TapisStub,
    site_id: str,
    work_dir: Path,
    *,
    username: str = USERNAME,
    **fields: Any,
) -> str:
    """Register this machine as the person's system for ``site_id``, with a credential."""
    found = system_id(site_id, username)
    work_dir.mkdir(parents=True, exist_ok=True)
    stub.seed_system(found, jobWorkingDir=str(work_dir), **fields)
    stub.seed_credential(found, username)
    return found


def tapis_config(stub: TapisStub, **extra: Any) -> dict[str, Any]:
    return {"base_url": stub.base_url, "tenant_id": "tacc", "client_id": CLIENT_ID} | extra


def package_archive(root: Path, names: Iterable[str]) -> bytes:
    """A package archive in W13's layout: ``<repo>/<repo>/<package>/...`` at its root.

    It holds each of ``names`` from ``root``. For clio-jarvis, ``root`` is the
    checkout and the names are ``clio_jarvis`` and ``schemas``, which the
    contract loader reads beside the repo directory.
    """
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz", format=tarfile.USTAR_FORMAT) as archive:
        for name in names:
            archive.add(
                root / name,
                arcname=name,
                filter=lambda item: None if "__pycache__" in item.name.split("/") else item,
            )
    return buffer.getvalue()


def demo_checkout(root: Path) -> Path:
    """A checkout holding ``w10demo``, a jarvis repo with the package ``w10demo.hello``."""
    checkout = root / "src"
    package = checkout / "w10demo" / "w10demo" / "hello"
    package.mkdir(parents=True)
    (package.parent / "__init__.py").write_text("", encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "pkg.py").write_text("# the fake jarvis never imports this\n", encoding="utf-8")
    return checkout
