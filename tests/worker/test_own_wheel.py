"""The relay a worker site installs when its settings name none (own_wheel)."""

from __future__ import annotations

import base64
import hashlib
import json
import zipfile
from importlib import metadata
from pathlib import Path

import pytest

from clio_relay.adapters.worker.own_wheel import RelayInstall, relay_install
from clio_relay.jobs.refusals import Refusal, RefusalReason

FILES = {
    "clio_relay/__init__.py": b'__version__ = "9.9.9"\n',
    "clio_relay-9.9.9.dist-info/METADATA": b"Name: clio-relay\nVersion: 9.9.9\n",
}


def _hash(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _installed(tmp_path: Path, direct_url: dict[str, object] | None) -> metadata.Distribution:
    """An installed clio-relay 9.9.9 whose RECORD hashes FILES, and its wheel."""
    wheel = tmp_path / "dist" / "clio_relay-9.9.9-py3-none-any.whl"
    wheel.parent.mkdir()
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, data in FILES.items():
            archive.writestr(name, data)
        archive.writestr("clio_relay-9.9.9.dist-info/RECORD", "")
    info = tmp_path / "site" / "clio_relay-9.9.9.dist-info"
    info.mkdir(parents=True)
    for name, data in FILES.items():
        installed = tmp_path / "site" / name
        installed.parent.mkdir(parents=True, exist_ok=True)
        installed.write_bytes(data)
    record = [f"{name},sha256={_hash(data)},{len(data)}" for name, data in FILES.items()]
    record.append("clio_relay-9.9.9.dist-info/RECORD,,")
    (info / "RECORD").write_text("\n".join(record) + "\n", encoding="utf-8")
    if direct_url is not None:
        (info / "direct_url.json").write_text(json.dumps(direct_url), encoding="utf-8")
    return metadata.PathDistribution(info)


def _wheel_url(tmp_path: Path) -> dict[str, object]:
    wheel = tmp_path / "dist" / "clio_relay-9.9.9-py3-none-any.whl"
    return {"url": wheel.as_uri(), "archive_info": {}}


def test_an_index_install_installs_its_version(tmp_path: Path) -> None:
    dist = _installed(tmp_path, None)

    assert relay_install(tmp_path / "cache", dist) == RelayInstall(requirement="clio-relay==9.9.9")


def test_a_wheel_url_install_sends_that_wheel_once_it_matches(tmp_path: Path) -> None:
    dist = _installed(tmp_path, _wheel_url(tmp_path))

    got = relay_install(tmp_path / "cache", dist)

    assert got.wheel == tmp_path / "dist" / "clio_relay-9.9.9-py3-none-any.whl"


def test_a_wheel_that_differs_from_the_install_is_refused(tmp_path: Path) -> None:
    dist = _installed(tmp_path, _wheel_url(tmp_path))
    wheel = tmp_path / "dist" / "clio_relay-9.9.9-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr("clio_relay/extra.py", b"changed = True\n")

    with pytest.raises(Refusal) as refusal:
        relay_install(tmp_path / "cache", dist)

    assert refusal.value.reason is RefusalReason.INVALID_CONFIG
    assert "clio_relay/extra.py" in refusal.value.message
    assert "relay_wheel" in refusal.value.message


@pytest.mark.parametrize(
    ("direct_url", "said"),
    [
        ({"url": "file:///src/clio-relay", "dir_info": {"editable": True}}, "editable checkout"),
        ({"url": "file:///src/clio-relay", "dir_info": {}}, "local directory"),
        (
            {"url": "https://github.com/iowarp/clio-relay", "vcs_info": {"vcs": "git"}},
            "git source",
        ),
    ],
)
def test_an_install_with_no_wheel_names_the_settings(
    tmp_path: Path, direct_url: dict[str, object], said: str
) -> None:
    dist = _installed(tmp_path, direct_url)

    with pytest.raises(Refusal) as refusal:
        relay_install(tmp_path / "cache", dist)

    assert refusal.value.reason is RefusalReason.INVALID_CONFIG
    assert said in refusal.value.message
    assert "relay_wheel" in refusal.value.message
    assert "relay_requirement" in refusal.value.message
