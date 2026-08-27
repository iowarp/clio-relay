"""Provision the immutable clio-kit wheel used by cross-repository tests."""

from __future__ import annotations

import hashlib
import os
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)

_CACHE_ENV = "CLIO_RELAY_CLIO_KIT_WHEEL_CACHE"
_WHEEL_ENV = "CLIO_RELAY_CLIO_KIT_WHEEL"
_WHEEL_SHA256_ENV = "CLIO_RELAY_CLIO_KIT_WHEEL_SHA256"
_MAX_WHEEL_BYTES = 64 * 1024 * 1024
_DOWNLOAD_TIMEOUT_SECONDS = 180.0
_LOCK_TIMEOUT_SECONDS = 180.0


def _wheel_sha256(path: Path) -> str:
    """Return the SHA-256 digest of one wheel without buffering it all in memory."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verified_wheel(path: Path, *, source: str, check_name: bool = True) -> Path:
    """Validate one candidate against the immutable runtime wheel pin.

    ``check_name=False`` skips the filename-identity check for a
    not-yet-renamed download candidate. ``provision_clio_kit_wheel`` downloads
    to a deliberately randomized ``*.partial`` temp name (so concurrent
    provisioning attempts against the same shared cache never collide) and
    only renames it to the canonical ``CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME``
    after this check passes -- a candidate at that stage can never carry the
    canonical name yet, so requiring it there made every cold-cache download
    fail unconditionally regardless of content. Only the hash (the candidate's
    actual identity) is meaningful to check before the atomic rename; the name
    check still applies to every other caller (the cached/sibling/env-var
    artifacts, whose name is not under this function's control).
    """
    try:
        resolved = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise ConfigurationError(
            f"{source} does not name a readable clio-kit wheel: {path}"
        ) from exc
    name_mismatch = check_name and resolved.name != CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME
    if not resolved.is_file() or name_mismatch:
        raise ConfigurationError(
            f"{source} must name {CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}, got {resolved.name}"
        )
    observed = _wheel_sha256(resolved)
    if observed != CLIO_KIT_JARVIS_MCP_WHEEL_SHA256:
        raise ConfigurationError(
            f"{source} clio-kit wheel SHA-256 mismatch: expected "
            f"{CLIO_KIT_JARVIS_MCP_WHEEL_SHA256}, observed {observed}"
        )
    return resolved


def _cache_directory() -> Path:
    """Return the cross-worktree immutable test-artifact cache directory."""
    configured = os.getenv(_CACHE_ENV)
    if configured:
        return Path(configured).expanduser()
    return Path(tempfile.gettempdir()) / "clio-relay-test-artifacts"


def _download_pinned_wheel(destination: Path) -> None:
    """Download the exact release wheel to ``destination`` with a strict bound."""
    request = urllib.request.Request(
        CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        headers={"User-Agent": "clio-relay-contract-tests/1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as response:  # noqa: S310
            final_url = str(response.geturl())
            if not final_url.startswith("https://"):
                raise ConfigurationError(
                    f"clio-kit wheel download redirected outside HTTPS: {final_url}"
                )
            declared_length = response.headers.get("Content-Length")
            if declared_length is not None and int(declared_length) > _MAX_WHEEL_BYTES:
                raise ConfigurationError("clio-kit release wheel exceeds the 64 MiB test bound")
            written = 0
            with destination.open("xb") as stream:
                while chunk := response.read(1024 * 1024):
                    written += len(chunk)
                    if written > _MAX_WHEEL_BYTES:
                        raise ConfigurationError(
                            "clio-kit release wheel exceeds the 64 MiB test bound"
                        )
                    stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())
    except (OSError, ValueError, urllib.error.URLError) as exc:
        destination.unlink(missing_ok=True)
        if isinstance(exc, ConfigurationError):
            raise
        raise ConfigurationError(
            "could not provision the pinned clio-kit wheel; provide the verified release "
            f"artifact through {_WHEEL_ENV}, seed {_CACHE_ENV}, or restore HTTPS access to "
            f"{CLIO_KIT_JARVIS_MCP_WHEEL_URL}: {type(exc).__name__}"
        ) from exc


def provision_clio_kit_wheel() -> Path:
    """Return the verified pinned wheel, provisioning a shared cache when needed."""
    configured_sha256 = os.getenv(_WHEEL_SHA256_ENV)
    if configured_sha256 not in {None, CLIO_KIT_JARVIS_MCP_WHEEL_SHA256}:
        raise ConfigurationError(
            f"{_WHEEL_SHA256_ENV} must equal the committed wheel pin "
            f"{CLIO_KIT_JARVIS_MCP_WHEEL_SHA256}"
        )
    configured_wheel = os.getenv(_WHEEL_ENV)
    if configured_wheel:
        return _verified_wheel(Path(configured_wheel), source=_WHEEL_ENV)

    sibling_wheel = (
        Path(__file__).resolve().parents[2]
        / "clio-kit"
        / "dist"
        / CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME
    )
    if sibling_wheel.is_file():
        return _verified_wheel(sibling_wheel, source="sibling clio-kit dist artifact")

    cache_directory = _cache_directory()
    try:
        cache_directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigurationError(
            f"could not create the clio-kit test-artifact cache {cache_directory}: {exc}"
        ) from exc
    cached_wheel = cache_directory / CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME
    lock = FileLock(str(cached_wheel) + ".lock", timeout=_LOCK_TIMEOUT_SECONDS)
    try:
        with lock:
            if cached_wheel.is_file():
                try:
                    return _verified_wheel(cached_wheel, source="cached clio-kit artifact")
                except ConfigurationError:
                    cached_wheel.unlink(missing_ok=True)
            partial = cache_directory / f".{cached_wheel.name}.{uuid4().hex}.partial"
            try:
                _download_pinned_wheel(partial)
                _verified_wheel(partial, source="downloaded clio-kit artifact", check_name=False)
                os.replace(partial, cached_wheel)
            finally:
                partial.unlink(missing_ok=True)
            return _verified_wheel(cached_wheel, source="cached clio-kit artifact")
    except FileLockTimeout as exc:
        raise ConfigurationError(
            f"timed out waiting for the clio-kit test-artifact cache lock: {lock.lock_file}"
        ) from exc
