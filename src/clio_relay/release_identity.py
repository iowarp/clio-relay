"""Resolve and verify a live GitHub release's identity, and gate mutations on it.

The owner for "does this GitHub release identify one exact, immutable
source commit" -- resolving a release by tag through GitHub's bounded,
draft-blind listing endpoint, verifying its tag/target/draft/prerelease/
immutable identity, and (built on ``branch_protection``'s governance
lifecycle) revalidating protected main, tag, governance, and release state
before any persistent mutation. Extracted from ``ci_validation.py`` per
clio-relay#231 (docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

import re

from clio_relay.branch_protection import verify_live_repository_governance
from clio_relay.provenance_primitives import (
    MAX_RELEASE_HISTORY_PAGES,
    GitHubJsonFetcher,
    GitHubNotFound,
    ProvenanceError,
    _list,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _validate_commit,
    _validate_repository,
    _validate_tag,
)


def verify_release_identity(
    document: object,
    *,
    tag: str,
    source_commit: str,
    resolved_tag_commit: str,
    resolved_target_commit: str,
    expect_draft: bool | None,
    expect_prerelease: bool,
    expect_immutable: bool | None = None,
) -> None:
    """Require a GitHub release to identify one exact immutable source commit."""
    _validate_tag(tag)
    _validate_commit(source_commit)
    _validate_commit(resolved_tag_commit)
    _validate_commit(resolved_target_commit)
    release = _mapping(document, "GitHub release")
    _positive_integer(release.get("id"), "GitHub release id")
    expected: dict[str, object] = {
        "tag_name": tag,
        "prerelease": expect_prerelease,
    }
    if expect_draft is not None:
        expected["draft"] = expect_draft
    if expect_immutable is not None:
        expected["immutable"] = expect_immutable
    mismatches = [key for key, value in expected.items() if release.get(key) != value]
    if mismatches:
        raise ProvenanceError(f"GitHub release identity mismatch: {sorted(mismatches)}")
    if resolved_tag_commit != source_commit:
        raise ProvenanceError("live release tag does not resolve to the reviewed source commit")
    # GitHub ignores target_commitish when a release is created for an existing
    # tag.  The stored value may therefore be ``main`` even when the workflow
    # supplied the exact SHA.  Its live resolution, not its spelling, is what
    # must remain bound to the reviewed commit.
    if resolved_target_commit != source_commit:
        raise ProvenanceError("release target does not resolve to the reviewed source commit")


def resolve_live_release(
    *,
    repository: str,
    tag: str,
    expect_draft: bool | None,
    fetch_json: GitHubJsonFetcher,
    allow_absent: bool = False,
    expect_immutable: bool | None = None,
) -> dict[str, object] | None:
    """Resolve one exact release by tag through a bounded list and numeric ID.

    GitHub's tag-scoped release endpoint does not expose draft releases. This
    resolver therefore walks bounded 100-record pages to an explicit empty page,
    requires stable numeric identities and one unique tag match, and then reloads
    that match through the numeric release endpoint before returning it.
    """
    _validate_repository(repository)
    _validate_tag(tag)
    release_summaries: list[object] = []
    for page_number in range(1, MAX_RELEASE_HISTORY_PAGES + 1):
        page = _list(
            fetch_json(f"repos/{repository}/releases?per_page=100&page={page_number}"),
            f"GitHub releases page {page_number}",
        )
        if len(page) > 100:
            raise ProvenanceError(
                f"GitHub releases page {page_number} exceeds the requested page size"
            )
        if not page:
            break
        release_summaries.extend(page)
    else:
        raise ProvenanceError("repository release history exceeds the bounded pagination window")
    matches: list[dict[str, object]] = []
    seen_release_ids: set[int] = set()
    for item in release_summaries:
        summary = _mapping(item, "GitHub release summary")
        release_id = _positive_integer(summary.get("id"), "GitHub release id")
        if release_id in seen_release_ids:
            raise ProvenanceError(
                f"GitHub release history changed during pagination: duplicate id {release_id}"
            )
        seen_release_ids.add(release_id)
        if summary.get("tag_name") == tag:
            matches.append(summary)
    if not matches:
        if allow_absent:
            return None
        raise GitHubNotFound(f"GitHub release was not found for tag: {tag}")
    if len(matches) != 1:
        raise ProvenanceError(f"expected one GitHub release for {tag}; found {len(matches)}")
    summary = matches[0]
    release_id = _positive_integer(summary.get("id"), "GitHub release id")
    release = _mapping(
        fetch_json(f"repos/{repository}/releases/{release_id}"),
        "GitHub release",
    )
    compared_fields = ("id", "tag_name", "target_commitish", "draft", "prerelease", "immutable")
    mismatches = [field for field in compared_fields if release.get(field) != summary.get(field)]
    if mismatches:
        raise ProvenanceError(
            f"GitHub release changed during numeric resolution: {sorted(mismatches)}"
        )
    if release.get("tag_name") != tag:
        raise ProvenanceError("numeric GitHub release identity does not match the requested tag")
    if expect_draft is not None and release.get("draft") is not expect_draft:
        raise ProvenanceError("GitHub release draft state does not match the required state")
    if expect_immutable is not None and release.get("immutable") is not expect_immutable:
        raise ProvenanceError("GitHub release immutable state does not match the required state")
    if release.get("draft") is True and release.get("immutable") is not False:
        raise ProvenanceError("a draft GitHub release must remain mutable")
    return release


def verify_live_release_identity(
    *,
    repository: str,
    tag: str,
    source_commit: str,
    expect_draft: bool | None,
    expect_prerelease: bool,
    fetch_json: GitHubJsonFetcher,
    expect_immutable: bool | None = None,
) -> None:
    """Fetch and verify the live release, tag, and target identities."""
    _validate_repository(repository)
    _validate_tag(tag)
    _validate_commit(source_commit)
    release = resolve_live_release(
        repository=repository,
        tag=tag,
        expect_draft=expect_draft,
        fetch_json=fetch_json,
        expect_immutable=expect_immutable,
    )
    if release is None:  # pragma: no cover - allow_absent is false above.
        raise GitHubNotFound(f"GitHub release was not found for tag: {tag}")
    target = _nonempty_string(release.get("target_commitish"), "release target_commitish")
    if re.fullmatch(r"[A-Za-z0-9_./-]+", target) is None or ".." in target:
        raise ProvenanceError("release target_commitish is unsafe")
    tag_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{tag}"),
        "release tag commit",
    )
    target_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{target}"),
        "release target commit",
    )
    verify_release_identity(
        release,
        tag=tag,
        source_commit=source_commit,
        resolved_tag_commit=_nonempty_string(tag_commit.get("sha"), "release tag commit SHA"),
        resolved_target_commit=_nonempty_string(
            target_commit.get("sha"), "release target commit SHA"
        ),
        expect_draft=expect_draft,
        expect_prerelease=expect_prerelease,
        expect_immutable=expect_immutable,
    )


def verify_live_mutation_authority(
    governance_receipt: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
    workflow_ref: str,
    workflow_sha: str,
    release_state: str,
    expect_draft: bool | None,
    fetch_json: GitHubJsonFetcher,
    fetch_admin_json: GitHubJsonFetcher,
) -> None:
    """Revalidate protected main, tag, governance, and release before mutation."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    if workflow_ref != "refs/heads/main" or workflow_sha != source_commit:
        raise ProvenanceError("persistent mutation is not executing from the exact main commit")
    if release_state == "present" and expect_draft is not True:
        raise ProvenanceError("release mutation is permitted only while the release is a draft")
    main_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/main"),
        "live main commit",
    )
    tag_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{tag}"),
        "live release tag commit",
    )
    if main_commit.get("sha") != source_commit or tag_commit.get("sha") != source_commit:
        raise ProvenanceError("live main or release tag moved away from the reviewed commit")
    verify_live_repository_governance(
        governance_receipt,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
        fetch_json=fetch_json,
        fetch_admin_json=fetch_admin_json,
    )
    if release_state not in {"absent", "present"}:
        raise ProvenanceError("release state expectation is invalid")
    if release_state == "present" and expect_draft is None:
        raise ProvenanceError("present release mutation requires an exact draft state")
    release = resolve_live_release(
        repository=repository,
        tag=tag,
        expect_draft=expect_draft if release_state == "present" else None,
        fetch_json=fetch_json,
        allow_absent=release_state == "absent",
    )
    if release is None:
        return
    if release_state == "absent":
        raise ProvenanceError("GitHub release appeared before create mutation")
    target = _nonempty_string(release.get("target_commitish"), "release target_commitish")
    if release.get("immutable") is not False:
        raise ProvenanceError("a mutable draft is required before every release mutation")
    if re.fullmatch(r"[A-Za-z0-9_./-]+", target) is None or ".." in target:
        raise ProvenanceError("release target_commitish is unsafe")
    target_commit = _mapping(
        fetch_json(f"repos/{repository}/commits/{target}"),
        "release target commit",
    )
    verify_release_identity(
        release,
        tag=tag,
        source_commit=source_commit,
        resolved_tag_commit=_nonempty_string(tag_commit.get("sha"), "release tag SHA"),
        resolved_target_commit=_nonempty_string(
            target_commit.get("sha"), "release target commit SHA"
        ),
        expect_draft=expect_draft,
        expect_prerelease=False,
        expect_immutable=False,
    )
