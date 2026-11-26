"""Which clio-relay a worker site installs when its settings name none.

The site runs the relay this computer runs. Relay reads how it was installed
(the ``direct_url.json`` of PEP 610):

- from an index, with no ``direct_url.json``: the site installs ``clio-relay==<version>``;
- from a wheel URL, such as a release asset: relay fetches that wheel, checks every
  file in it against the hashes in its own installed ``RECORD``, and sends it to the
  site, so the site needs no index that carries the version;
- from anything else (an editable install, a local directory, git or an sdist): there
  is no wheel to send, so relay refuses and names the ``relay_wheel`` or
  ``relay_requirement`` setting.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import zipfile
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any, cast
from urllib.parse import ParseResult, unquote, urlparse

import httpx

from clio_relay.jobs.refusals import Refusal, RefusalReason

DIST = "clio-relay"
_WHEEL_NAME = re.compile(r"[A-Za-z0-9_.+-]+\.whl")
# Written by the installer, so they are in RECORD without a hash or differ from the wheel.
_INSTALLER_FILES = {"RECORD", "INSTALLER", "REQUESTED", "direct_url.json"}
FETCH_TIMEOUT_S = 120.0
_DRIVE = re.compile(r"/[A-Za-z]:")


def _file_path(parsed: ParseResult) -> Path:
    """The local path a file:// URL names, a Windows drive or share included."""
    path = unquote(parsed.path)
    if _DRIVE.match(path):
        path = path[1:]
    if parsed.netloc and parsed.netloc != "localhost":
        path = f"//{parsed.netloc}{path}"
    return Path(path)


@dataclass(frozen=True)
class RelayInstall:
    """What the site installs: a wheel relay sends, or a requirement uv resolves."""

    wheel: Path | None = None
    requirement: str | None = None


def _direct_url(dist: metadata.Distribution) -> dict[str, Any] | None:
    text = dist.read_text("direct_url.json")
    if not text:
        return None
    loaded: object = json.loads(text)
    return cast(dict[str, Any], loaded) if isinstance(loaded, dict) else None


def _unusable(how: str) -> Refusal:
    return Refusal(
        RefusalReason.INVALID_CONFIG,
        f"relay on this computer was installed from {how}, which has no wheel to send to "
        "the site and no index that carries its version. Set the site's relay_wheel to a "
        "clio-relay wheel, or relay_requirement to what uv installs on the site",
    )


def _recorded(dist: metadata.Distribution) -> dict[str, str]:
    """Return each installed file's RECORD sha256, as RECORD writes it."""
    hashes: dict[str, str] = {}
    for file in dist.files or []:
        if file.hash is not None and file.hash.mode == "sha256":
            hashes[file.as_posix()] = file.hash.value
    return hashes


def matches_install(wheel: Path, recorded: dict[str, str]) -> str | None:
    """Return why ``wheel`` is not the installed relay, or None when every file matches."""
    try:
        with zipfile.ZipFile(wheel) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                name = info.filename
                if name.rsplit("/", 1)[-1] in _INSTALLER_FILES and ".dist-info/" in name:
                    continue
                expected = recorded.get(name)
                if expected is None:
                    return f"{name} is in the wheel but not in relay's installed RECORD"
                digest = hashlib.sha256(archive.read(info)).digest()
                got = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
                if got != expected:
                    return f"{name} in the wheel differs from the installed file"
    except (OSError, zipfile.BadZipFile) as exc:
        return f"the wheel cannot be read: {exc}"
    return None


def _fetch(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".part")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=FETCH_TIMEOUT_S) as reply:
            reply.raise_for_status()
            with partial.open("wb") as out:
                for chunk in reply.iter_bytes():
                    out.write(chunk)
    except httpx.HTTPError as exc:
        partial.unlink(missing_ok=True)
        raise Refusal(
            RefusalReason.UNREACHABLE,
            f"relay could not fetch its own wheel {url} to send to the site: {exc}",
        ) from exc
    partial.replace(target)


def relay_install(cache_dir: Path, dist: metadata.Distribution | None = None) -> RelayInstall:
    """Return what the site installs so it runs this computer's relay.

    A fetched wheel is kept in ``cache_dir`` and checked again on every use.
    """
    dist = dist or metadata.distribution(DIST)
    direct = _direct_url(dist)
    if direct is None:
        return RelayInstall(requirement=f"{DIST}=={dist.version}")
    url = str(direct.get("url") or "")
    if "dir_info" in direct:
        editable = bool(cast(dict[str, Any], direct["dir_info"]).get("editable"))
        how = "an editable checkout" if editable else "a local directory"
        raise _unusable(f"{how} ({url})")
    if "vcs_info" in direct:
        raise _unusable(f"a git source ({url})")
    parsed = urlparse(url)
    name = unquote(parsed.path.rsplit("/", 1)[-1])
    if "archive_info" not in direct or not _WHEEL_NAME.fullmatch(name):
        raise _unusable(f"an archive that is not a wheel ({url})")
    if parsed.scheme == "file":
        wheel = _file_path(parsed)
    elif parsed.scheme in ("http", "https"):
        wheel = cache_dir / name
        if not wheel.is_file():
            _fetch(url, wheel)
    else:
        raise _unusable(f"a {parsed.scheme or 'relative'} URL ({url})")
    why = matches_install(wheel, _recorded(dist))
    if why is not None:
        if wheel.parent == cache_dir:
            wheel.unlink(missing_ok=True)
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"relay's own wheel {url} is not the relay installed on this computer ({why}), "
            "so relay does not send it to the site. Set the site's relay_wheel or "
            "relay_requirement",
        )
    return RelayInstall(wheel=wheel)
