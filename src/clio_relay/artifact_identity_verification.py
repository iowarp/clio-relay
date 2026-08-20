"""Verify the running process's install artifact against its claimed digest (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). This module owns the
artifact-provenance concern: binding a claimed ``artifact_sha256`` (or, for a
VCS install, an exact-pinned git commit sha) to the exact bytes the running
interpreter loaded. :func:`verify_running_artifact_identity` is the boolean
gate a release-pinned launch checks; :func:`infer_running_artifact_identity`
derives the digest/verified pair a receipt records when no claim was made up
front. Everything else here -- fetching one bounded wheel from a local path
or an official HTTPS release/PyPI channel
(:func:`is_official_release_wheel_url` / :func:`is_github_release_asset_url`
/ :func:`url_host_resolves_publicly` guard which hosts are trusted), hashing
it, and diffing its ``RECORD`` closure against the files actually installed
(:func:`installed_files_match_wheel`) -- exists to make that one binding
trustworthy without ever executing untrusted archive content.

Embedded/checkout build identity (:func:`embedded_build_info` /
:func:`checkout_build_info`) and install-source classification
(:func:`classify_install_source` / :func:`is_official_github_release_wheel`)
live here too since both feed the same provenance picture. The install-source
*detection* orchestration (still in :mod:`clio_relay.validation_report`, not
yet extracted) calls into this module; :func:`_direct_wheel_bytes` reads the
shared regular-file identity primitives from
:mod:`clio_relay.regular_file_identity`.
"""

from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import ipaddress
import json
import os
import re
import socket
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from importlib import metadata
from pathlib import Path, PurePosixPath
from typing import Any, cast

from clio_relay import __version__
from clio_relay.redaction import redact_url
from clio_relay.regular_file_identity import read_open_regular_file, regular_file_identity
from clio_relay.validation_limits import MAX_DISTRIBUTION_WHEEL_BYTES
from clio_relay.validation_schema import InstallSourceKind

_OFFICIAL_RELEASE_WHEEL_PATH = re.compile(
    r"/iowarp/clio-relay/releases/download/v(?P<version>[0-9A-Za-z][0-9A-Za-z.+-]*)/"
    r"clio_relay-(?P=version)-py3-none-any\.whl"
)
_OFFICIAL_PYPI_WHEEL_PATH = re.compile(
    r"/packages/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{16,64}/"
    r"clio_relay-[0-9A-Za-z][0-9A-Za-z.+-]*-py3-none-any\.whl"
)
_FULL_GIT_COMMIT_SHA = re.compile(r"[0-9a-fA-F]{40}")


def embedded_build_info() -> dict[str, Any] | None:
    from importlib import resources

    try:
        content = (
            resources.files("clio_relay").joinpath("_build_info.json").read_text(encoding="utf-8")
        )
    except (FileNotFoundError, ModuleNotFoundError):
        return None
    loaded = cast(object, json.loads(content))
    return cast(dict[str, Any], loaded) if isinstance(loaded, dict) else None


def checkout_build_info() -> dict[str, Any]:
    package_path = Path(__file__).resolve()
    for parent in package_path.parents:
        if not (parent / ".git").exists():
            continue
        commit = _git_output(parent, ["rev-parse", "HEAD"])
        tag = _git_output(parent, ["describe", "--tags", "--exact-match", "HEAD"])
        dirty_output = _git_output(parent, ["status", "--porcelain"])
        return {"commit": commit, "tag": tag, "dirty": bool(dirty_output)}
    return {}


def _git_output(root: Path, args: list[str]) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        check=False,
        text=True,
    )
    value = completed.stdout.strip()
    return value if completed.returncode == 0 and value else None


def distribution_direct_url(distribution: metadata.Distribution) -> dict[str, Any] | None:
    content = distribution.read_text("direct_url.json")
    if content is None:
        return None
    loaded = cast(object, json.loads(content))
    if not isinstance(loaded, dict):
        return None
    value = cast(dict[str, Any], loaded)
    url = value.get("url")
    if isinstance(url, str):
        value = {**value, "url": redact_url(url)}
    return value


def _vcs_commit_identity_verified(direct_url: dict[str, Any] | None) -> str | None:
    """Return the pinned full git commit sha, or None unless it is exact-pinned.

    A VCS install only counts as identity-verified when it is pinned to an
    exact 40-hex commit sha (``git+https://.../clio-relay@<sha>``) -- a
    branch or tag reference is a moving target, not a fixed identity, and is
    rejected even though it still resolves to a ``commit_id``. The returned
    sha plays the same identity-anchor role ``artifact_sha256`` plays for a
    wheel: recorded on the receipt and compared for exact equality.
    """
    if direct_url is None:
        return None
    vcs_info = direct_url.get("vcs_info")
    if not isinstance(vcs_info, dict):
        return None
    typed_vcs_info = cast(dict[str, Any], vcs_info)
    if typed_vcs_info.get("vcs") != "git":
        return None
    requested_revision = typed_vcs_info.get("requested_revision")
    commit_id = typed_vcs_info.get("commit_id")
    if (
        not isinstance(requested_revision, str)
        or _FULL_GIT_COMMIT_SHA.fullmatch(requested_revision) is None
    ):
        return None
    if not isinstance(commit_id, str) or commit_id.lower() != requested_revision.lower():
        return None
    return requested_revision.lower()


def verify_running_artifact_identity(
    distribution: metadata.Distribution,
    *,
    detected_kind: InstallSourceKind,
    direct_url: dict[str, Any] | None,
    artifact_sha256: str | None,
    launcher: str,
) -> bool:
    """Bind a claimed archive digest to the files loaded by this process."""
    if artifact_sha256 is None or not re.fullmatch(r"[0-9a-fA-F]{64}", artifact_sha256):
        return False
    expected = artifact_sha256.lower()
    if launcher not in {"uv-tool", "uvx"}:
        return False
    if detected_kind is InstallSourceKind.WHEEL:
        if _local_wheel_archive_path(direct_url) is not None:
            return _local_wheel_matches_install(distribution, direct_url, expected)
        return _wheel_url_matches_install(distribution, direct_url, expected)
    if detected_kind is InstallSourceKind.PYPI:
        return _pypi_wheel_matches_install(distribution, expected)
    return False


def infer_running_artifact_identity(
    distribution: metadata.Distribution,
    *,
    detected_kind: InstallSourceKind,
    direct_url: dict[str, Any] | None,
    launcher: str,
) -> tuple[str | None, bool]:
    """Inspect one exact direct wheel, or exact-sha VCS pin, for installation identity."""
    if launcher != "uv-tool":
        return None, False
    if detected_kind is InstallSourceKind.VCS:
        commit_sha = _vcs_commit_identity_verified(direct_url)
        return commit_sha, commit_sha is not None
    if detected_kind is not InstallSourceKind.WHEEL:
        return None, False
    wheel_bytes = _direct_wheel_bytes(direct_url)
    if wheel_bytes is None:
        return None, False
    digest = hashlib.sha256(wheel_bytes).hexdigest()
    direct_hashes = _direct_url_sha256_hashes(direct_url)
    if direct_hashes and digest not in direct_hashes:
        return digest, False
    try:
        return digest, _installed_files_match_wheel(distribution, wheel_bytes)
    except (OSError, ValueError, zipfile.BadZipFile):
        return digest, False


def _wheel_url_matches_install(
    distribution: metadata.Distribution,
    direct_url: dict[str, Any] | None,
    expected_sha256: str,
) -> bool:
    """Verify exact local or HTTPS wheel bytes and their installed RECORD closure."""
    wheel_bytes = _direct_wheel_bytes(direct_url)
    if wheel_bytes is None:
        return False
    direct_hashes = _direct_url_sha256_hashes(direct_url)
    if direct_hashes and expected_sha256 not in direct_hashes:
        return False
    if hashlib.sha256(wheel_bytes).hexdigest() != expected_sha256:
        return False
    try:
        return _installed_files_match_wheel(distribution, wheel_bytes)
    except (OSError, ValueError, zipfile.BadZipFile):
        return False


def _direct_wheel_bytes(direct_url: dict[str, Any] | None) -> bytes | None:
    """Read one bounded wheel from its exact local-file or clean HTTPS URL."""
    if direct_url is None:
        return None
    raw_url = direct_url.get("url")
    if not isinstance(raw_url, str):
        return None
    parsed = urllib.parse.urlsplit(raw_url)
    if not parsed.path.casefold().endswith(".whl"):
        return None
    if parsed.scheme.casefold() == "file":
        path = _local_wheel_archive_path(direct_url)
        if path is None:
            return None
        identity = regular_file_identity(path)
        if identity is None or identity[2] > MAX_DISTRIBUTION_WHEEL_BYTES:
            return None
        return read_open_regular_file(
            path,
            identity,
            maximum_bytes=MAX_DISTRIBUTION_WHEEL_BYTES,
        )
    if not is_official_release_wheel_url(raw_url) or not url_host_resolves_publicly(raw_url):
        return None
    try:
        opener = urllib.request.build_opener(_ReleaseWheelRedirectHandler())
        with opener.open(raw_url, timeout=60) as response:  # noqa: S310
            final_url = urllib.parse.urlsplit(str(response.geturl()))
            final_url_text = urllib.parse.urlunsplit(final_url)
            if not (
                is_official_release_wheel_url(final_url_text)
                or is_github_release_asset_url(final_url_text)
            ):
                return None
            content = response.read(MAX_DISTRIBUTION_WHEEL_BYTES + 1)
    except (OSError, ValueError, urllib.error.HTTPError):
        return None
    return content if len(content) <= MAX_DISTRIBUTION_WHEEL_BYTES else None


class _ReleaseWheelRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject a release download redirect before it can reach an unsafe host."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if not is_github_release_asset_url(newurl) or not url_host_resolves_publicly(newurl):
            raise urllib.error.HTTPError(
                newurl,
                403,
                "unsafe wheel download redirect",
                headers,
                fp,
            )
        return super().redirect_request(
            req,
            fp,
            code,
            msg,
            headers,
            newurl,
        )


def is_official_release_wheel_url(value: str) -> bool:
    """Allow only credential-free canonical clio-relay GitHub or PyPI wheel URLs.

    GitHub release assets and the trusted-publishing PyPI upload both carry
    the identical released bytes (the same artifact is attached to a GitHub
    Release and published to PyPI from that release); either canonical
    channel is an official source of the wheel. This function recognizes the
    *channel* only -- the sha256 digest binding performed by
    ``_wheel_url_matches_install`` remains the actual integrity anchor.
    """
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme.casefold() != "https"
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return False
    if parsed.hostname == "github.com":
        return _OFFICIAL_RELEASE_WHEEL_PATH.fullmatch(parsed.path) is not None
    if parsed.hostname == "files.pythonhosted.org":
        return _OFFICIAL_PYPI_WHEEL_PATH.fullmatch(parsed.path) is not None
    return False


def is_github_release_asset_url(value: str) -> bool:
    """Allow only GitHub's credential-free HTTPS release-asset redirect target."""
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme.casefold() == "https"
        and parsed.hostname == "release-assets.githubusercontent.com"
        and port in {None, 443}
        and parsed.username is None
        and parsed.password is None
        and not parsed.fragment
        and parsed.path.startswith("/github-production-release-asset/")
    )


def url_host_resolves_publicly(value: str) -> bool:
    """Fail closed unless every resolved address for one HTTPS URL is globally routable."""
    try:
        parsed = urllib.parse.urlsplit(value)
        hostname = parsed.hostname
        if parsed.scheme.casefold() != "https" or hostname is None:
            return False
        answers = socket.getaddrinfo(hostname, parsed.port or 443, type=socket.SOCK_STREAM)
        addresses = {
            str(answer[4][0]).split("%", maxsplit=1)[0]
            for answer in answers
            if answer[0] in {socket.AF_INET, socket.AF_INET6}
        }
        return bool(addresses) and all(
            ipaddress.ip_address(address).is_global for address in addresses
        )
    except (OSError, ValueError):
        return False


def _local_wheel_matches_install(
    distribution: metadata.Distribution,
    direct_url: dict[str, Any] | None,
    expected_sha256: str,
) -> bool:
    """Verify a local wheel archive and the installed files derived from its RECORD."""
    if _local_wheel_archive_path(direct_url) is None:
        return False
    return _wheel_url_matches_install(distribution, direct_url, expected_sha256)


def _local_wheel_archive_path(direct_url: dict[str, Any] | None) -> Path | None:
    """Resolve only an explicit local-file wheel reference from PEP 610 metadata."""
    if direct_url is None:
        return None
    raw_url = direct_url.get("url")
    if not isinstance(raw_url, str):
        return None
    parsed = urllib.parse.urlsplit(raw_url)
    if parsed.scheme.casefold() != "file" or parsed.query or parsed.fragment:
        return None
    if parsed.netloc not in {"", "localhost"}:
        return None
    decoded = urllib.parse.unquote(parsed.path)
    if os.name == "nt" and re.fullmatch(r"/[A-Za-z]:/.*", decoded):
        decoded = decoded[1:]
    return Path(decoded)


def _direct_url_sha256_hashes(direct_url: dict[str, Any] | None) -> set[str]:
    if direct_url is None:
        return set()
    archive_info = direct_url.get("archive_info")
    if not isinstance(archive_info, dict):
        return set()
    typed = cast(dict[str, Any], archive_info)
    values: list[object] = []
    if "hash" in typed:
        values.append(typed["hash"])
    hashes = typed.get("hashes")
    if isinstance(hashes, dict):
        sha256_value = cast(dict[object, object], hashes).get("sha256")
        values.append(f"sha256={sha256_value}" if isinstance(sha256_value, str) else None)
    verified: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        algorithm, separator, digest = value.partition("=")
        if algorithm.lower() == "sha256" and separator and re.fullmatch(r"[0-9a-fA-F]{64}", digest):
            verified.add(digest.lower())
    return verified


def _pypi_wheel_matches_install(
    distribution: metadata.Distribution,
    expected_sha256: str,
) -> bool:
    """Verify installed files against the exact official PyPI wheel digest."""
    try:
        with urllib.request.urlopen(  # noqa: S310 - fixed HTTPS PyPI endpoint
            f"https://pypi.org/pypi/clio-relay/{urllib.parse.quote(distribution.version)}/json",
            timeout=30,
        ) as response:
            content = response.read(4 * 1024 * 1024 + 1)
        if len(content) > 4 * 1024 * 1024:
            return False
        payload = cast(object, json.loads(content))
        payload_mapping = cast(dict[object, object], payload) if isinstance(payload, dict) else {}
        urls = payload_mapping.get("urls")
        if not isinstance(urls, list):
            return False
        wheel_url: str | None = None
        for item in cast(list[object], urls):
            if not isinstance(item, dict):
                continue
            record = cast(dict[str, Any], item)
            digests = record.get("digests")
            digest = (
                cast(dict[str, Any], digests).get("sha256") if isinstance(digests, dict) else None
            )
            url = record.get("url")
            if (
                record.get("packagetype") == "bdist_wheel"
                and isinstance(digest, str)
                and digest.lower() == expected_sha256
                and isinstance(url, str)
                and url.startswith("https://files.pythonhosted.org/")
            ):
                wheel_url = url
                break
        if wheel_url is None:
            return False
        with urllib.request.urlopen(  # noqa: S310 - URL constrained above
            wheel_url,
            timeout=60,
        ) as response:
            wheel_bytes = response.read(128 * 1024 * 1024 + 1)
        if len(wheel_bytes) > 128 * 1024 * 1024:
            return False
        if hashlib.sha256(wheel_bytes).hexdigest() != expected_sha256:
            return False
        return _installed_files_match_wheel(distribution, wheel_bytes)
    except (OSError, ValueError, json.JSONDecodeError, zipfile.BadZipFile):
        return False


def _installed_files_match_wheel(
    distribution: metadata.Distribution,
    wheel_bytes: bytes,
) -> bool:
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as archive:
        archive_names = [item.filename for item in archive.infolist() if not item.is_dir()]
        if len(archive_names) != len(set(archive_names)):
            return False
        record_names = [name for name in archive_names if name.endswith(".dist-info/RECORD")]
        if len(record_names) != 1:
            return False
        rows = csv.reader(io.StringIO(archive.read(record_names[0]).decode("utf-8")))
        records: dict[str, tuple[str, str]] = {}
        for row in rows:
            if len(row) != 3 or not _safe_wheel_member(row[0]) or row[0] in records:
                return False
            records[row[0]] = (row[1], row[2])
        if set(records) != set(archive_names):
            return False
        checked = 0
        for wheel_path, (hash_field, size_field) in records.items():
            if wheel_path == record_names[0]:
                if hash_field or size_field:
                    return False
                continue
            algorithm, separator, encoded_digest = hash_field.partition("=")
            if algorithm != "sha256" or not separator or not size_field.isdecimal():
                return False
            try:
                wheel_content = archive.read(wheel_path)
                installed_content = Path(str(distribution.locate_file(wheel_path))).read_bytes()
                expected = base64.b64decode(
                    encoded_digest + "=" * (-len(encoded_digest) % 4),
                    altchars=b"-_",
                    validate=True,
                )
            except (KeyError, OSError, ValueError, binascii.Error):
                return False
            if len(expected) != hashlib.sha256().digest_size:
                return False
            expected_size = int(size_field)
            if len(wheel_content) != expected_size or len(installed_content) != expected_size:
                return False
            if hashlib.sha256(wheel_content).digest() != expected:
                return False
            if hashlib.sha256(installed_content).digest() != expected:
                return False
            checked += 1
        return checked > 0


def _safe_wheel_member(value: str) -> bool:
    """Reject absolute, platform-ambiguous, or traversing wheel member names."""
    if not value or "\\" in value:
        return False
    segments = value.split("/")
    if any(part in {"", ".", ".."} for part in segments):
        return False
    path = PurePosixPath(value)
    return not path.is_absolute()


def classify_install_source(
    direct_url: dict[str, Any] | None,
) -> tuple[InstallSourceKind, str | None]:
    if direct_url is None:
        return InstallSourceKind.PYPI, f"clio-relay=={__version__}"
    raw_url = direct_url.get("url")
    url = raw_url if isinstance(raw_url, str) and raw_url else None
    directory_info = direct_url.get("dir_info")
    if (
        isinstance(directory_info, dict)
        and cast(dict[str, Any], directory_info).get("editable") is True
    ):
        return InstallSourceKind.EDITABLE, url
    if isinstance(direct_url.get("vcs_info"), dict):
        return InstallSourceKind.VCS, url
    if url is not None and url.lower().endswith(".whl"):
        return InstallSourceKind.WHEEL, url
    if url is not None and url.startswith("file:"):
        return InstallSourceKind.CHECKOUT, url
    return InstallSourceKind.UNKNOWN, url


def is_official_github_release_wheel(
    direct_url: dict[str, Any] | None,
    distribution_version: str,
) -> bool:
    """Recognize the canonical clio-relay wheel URL for one GitHub release."""
    if direct_url is None:
        return False
    value = direct_url.get("url")
    if not isinstance(value, str):
        return False
    expected = (
        "https://github.com/iowarp/clio-relay/releases/download/"
        f"v{distribution_version}/clio_relay-{distribution_version}-py3-none-any.whl"
    )
    return value == expected
