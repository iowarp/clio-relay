"""Shared JSON/type primitives, exceptions, and release policy constants.

The owner for every bounded-parsing helper (``_mapping``/``_list``/
``_positive_integer``/...), the ``ProvenanceError``/``GitHubNotFound``
exception family, the ``GitHubJsonFetcher`` protocol and its concrete
``_github_fetcher`` implementation, bounded JSON file I/O
(``_load_json``/``_write_json``), the identity validators
(``_validate_repository``/``_validate_commit``/``_validate_git_tree``/
``_validate_tag``/``_canonical_json_sha256``), and the release policy
constants (byte/count limits, required job/environment sets, fixed
payload filename sets, schema literals) every other ``ci_validation``
owner module is built from. Extracted from ``ci_validation.py`` per
clio-relay#231 (docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Protocol, cast
from uuid import uuid4

REQUIRED_MATRIX_JOBS: tuple[str, ...] = (
    "ubuntu-latest / python 3.12",
    "ubuntu-latest / python 3.13",
    "ubuntu-latest / python 3.14",
    "windows-latest / python 3.12",
    "windows-latest / python 3.13",
    "windows-latest / python 3.14",
)
REQUIRED_CI_JOBS: tuple[str, ...] = (
    "GitHub Actions syntax and semantics",
    *REQUIRED_MATRIX_JOBS,
    "seal tested release candidate",
)
GITHUB_ACTIONS_APP_ID = 15368
MAIN_REVIEW_POLICY = "single-maintainer"
REQUIRED_APPROVING_REVIEW_COUNT = 0
REQUIRE_LAST_PUSH_APPROVAL = False
REQUIRED_ENVIRONMENTS: tuple[str, ...] = (
    "live-validation",
    "pypi",
    "release-finalization",
    "released-validation",
)
CI_WORKFLOW_PATH = ".github/workflows/ci.yml"
RELEASE_WORKFLOW_PATH = ".github/workflows/release.yml"
IMMUTABLE_RELEASES_API_VERSION = "2026-03-10"
REQUIRED_MERGE_QUEUE_PARAMETERS: dict[str, object] = {
    "check_response_timeout_minutes": 60,
    "grouping_strategy": "ALLGREEN",
    "max_entries_to_build": 5,
    "max_entries_to_merge": 5,
    "merge_method": "SQUASH",
    "min_entries_to_merge": 1,
    "min_entries_to_merge_wait_minutes": 0,
}
RELEASE_TAG_PATTERN = "refs/tags/v*"
MAX_RELEASE_ASSET_METADATA_RECORDS = 96
MAX_RELEASE_HISTORY_PAGES = 100
MAX_VALIDATION_REPORT_ASSETS = 64
MAX_VALIDATION_REPORT_BYTES = 8 * 1024 * 1024
MAX_VALIDATION_REPORT_AGGREGATE_BYTES = 64 * 1024 * 1024
MAX_JSON_DOCUMENT_BYTES = 16 * 1024 * 1024
MAX_ACTIONS_ARTIFACT_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_DISTRIBUTION_BYTES = 64 * 1024 * 1024
MAX_DISTRIBUTION_MEMBERS = 4096
MAX_DISTRIBUTION_MEMBER_BYTES = 16 * 1024 * 1024
MAX_DISTRIBUTION_UNCOMPRESSED_BYTES = 128 * 1024 * 1024
MAX_DISTRIBUTION_TAR_BYTES = MAX_DISTRIBUTION_UNCOMPRESSED_BYTES + (
    (MAX_DISTRIBUTION_MEMBERS + 4) * 2048
)
MAX_DISTRIBUTION_METADATA_BYTES = 1024 * 1024
MAX_DISTRIBUTION_PATH_LENGTH = 512
MAX_FIXED_JSON_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_RELEASE_ASSET_BYTES = 128 * 1024 * 1024
MAX_RELEASE_ASSET_AGGREGATE_BYTES = 512 * 1024 * 1024
TAG_PAYLOAD_FIXED_FILES = frozenset({"SHA256SUMS", "validation-local.json"})
CANDIDATE_PAYLOAD_FIXED_FILES = frozenset(
    {"SHA256SUMS", "validation-local.json", "CANDIDATE-BUILD.json"}
)
RELEASE_ACCEPTANCE_MATRIX_SCHEMA = "clio-relay.release-acceptance-matrix.v1"
RELEASE_ACCEPTANCE_MATRIX_STAGES = ("candidate", "released")


class ProvenanceError(ValueError):
    """Raised when live GitHub state cannot prove a release prerequisite."""


class GitHubNotFound(ProvenanceError):
    """Raised when an authenticated GitHub resource is conclusively absent."""


class GitHubJsonFetcher(Protocol):
    """Fetch one bounded JSON document from an authenticated GitHub API path."""

    def __call__(self, path: str) -> object:
        """Return the decoded JSON response for a repository API path."""
        ...


def _load_json(path: Path) -> object:
    try:
        details = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(details.st_mode):
            raise ProvenanceError(f"JSON document is not a regular file: {path}")
        if details.st_size > MAX_JSON_DOCUMENT_BYTES:
            raise ProvenanceError(f"JSON document exceeds {MAX_JSON_DOCUMENT_BYTES} bytes: {path}")
        with path.open("rb") as stream:
            content = stream.read(MAX_JSON_DOCUMENT_BYTES + 1)
        if len(content) > MAX_JSON_DOCUMENT_BYTES:
            raise ProvenanceError(f"JSON document exceeds {MAX_JSON_DOCUMENT_BYTES} bytes: {path}")
        return cast(object, json.loads(content.decode("utf-8-sig")))
    except ProvenanceError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProvenanceError(f"could not read JSON document {path}: {exc}") from exc


def _github_fetcher(token: str) -> GitHubJsonFetcher:
    if not token:
        raise ProvenanceError("GH_TOKEN is required to verify live repository governance")

    def fetch(path: str) -> object:
        if not path.startswith("repos/") or "://" in path or ".." in path:
            raise ProvenanceError("GitHub API path is outside the repository allowlist")
        request = urllib.request.Request(  # noqa: S310 - fixed api.github.com origin.
            f"https://api.github.com/{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "clio-relay-release-governance",
                "X-GitHub-Api-Version": "2026-03-10",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                content = response.read(4 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise GitHubNotFound(f"GitHub resource was not found: {path}") from exc
            raise ProvenanceError(
                f"GitHub governance query failed for {path}: HTTP {exc.code}"
            ) from exc
        except (OSError, urllib.error.URLError) as exc:
            raise ProvenanceError(f"GitHub governance query failed for {path}: {exc}") from exc
        if len(content) > 4 * 1024 * 1024:
            raise ProvenanceError(f"GitHub governance response is too large for {path}")
        try:
            return cast(object, json.loads(content))
        except json.JSONDecodeError as exc:
            raise ProvenanceError(f"GitHub governance response is invalid for {path}") from exc

    return fetch


def _write_json(path: Path, document: object) -> None:
    encoded = json.dumps(document, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(encoded, encoding="utf-8", newline="\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ProvenanceError(f"{field} must be a JSON object with string keys")
    typed = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in typed):
        raise ProvenanceError(f"{field} must be a JSON object with string keys")
    return {cast(str, key): item for key, item in typed.items()}


def _list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise ProvenanceError(f"{field} must be a JSON array")
    return list(cast(list[object], value))


def _string_list(value: object, field: str) -> list[str]:
    items = _list(value, field)
    if not all(isinstance(item, str) and item for item in items):
        raise ProvenanceError(f"{field} must contain only non-empty strings")
    return [cast(str, item) for item in items]


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProvenanceError(f"{field} must be an integer")
    return value


def _positive_integer(value: object, field: str) -> int:
    integer = _integer(value, field)
    if integer < 1:
        raise ProvenanceError(f"{field} must be positive")
    return integer


def _nonempty_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProvenanceError(f"{field} must be a non-empty string")
    return value


def _https_url(value: object, field: str) -> str:
    url = _nonempty_string(value, field)
    if not url.startswith("https://"):
        raise ProvenanceError(f"{field} must be an HTTPS URL")
    return url


def _rfc3339_timestamp(value: object, field: str) -> datetime:
    timestamp = _nonempty_string(value, field)
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProvenanceError(f"{field} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProvenanceError(f"{field} must include a timezone")
    return parsed


def _sha256_bounded_file(path: Path, *, maximum_bytes: int) -> str:
    digest = hashlib.sha256()
    observed = 0
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(min(1024 * 1024, maximum_bytes + 1 - observed))
                if not chunk:
                    break
                observed += len(chunk)
                if observed > maximum_bytes:
                    raise ProvenanceError(f"file exceeds {maximum_bytes} bytes: {path}")
                digest.update(chunk)
    except ProvenanceError:
        raise
    except OSError as exc:
        raise ProvenanceError(f"could not hash bounded file {path}: {exc}") from exc
    return digest.hexdigest()


def _validate_repository(repository: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ProvenanceError("repository must be an owner/name slug")


def _validate_commit(commit: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ProvenanceError("source commit must be a lowercase 40-character SHA")


def _validate_git_tree(tree: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", tree) is None:
        raise ProvenanceError("source tree must be a lowercase 40-character Git object id")


def _canonical_json_sha256(document: object) -> str:
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_tag(tag: str) -> None:
    if re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z.-]+)?", tag) is None:
        raise ProvenanceError("release tag is invalid")
