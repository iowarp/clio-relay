"""Repository governance receipts: branch/tag/environment protection.

The owner for translating raw GitHub branch-ruleset/tag-ruleset/
environment/immutable-releases API responses into one deterministic
"repository governance" receipt, verifying that receipt's shape, and
re-querying live GitHub to require the carried receipt still equals
current state. Extracted from ``ci_validation.py`` per clio-relay#231
(docs/design/relay-architecture-2026-08.md).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from clio_relay.provenance_primitives import (
    GITHUB_ACTIONS_APP_ID,
    IMMUTABLE_RELEASES_API_VERSION,
    MAIN_REVIEW_POLICY,
    RELEASE_TAG_PATTERN,
    REQUIRE_LAST_PUSH_APPROVAL,
    REQUIRED_APPROVING_REVIEW_COUNT,
    REQUIRED_CI_JOBS,
    REQUIRED_ENVIRONMENTS,
    REQUIRED_MERGE_QUEUE_PARAMETERS,
    GitHubJsonFetcher,
    ProvenanceError,
    _integer,
    _list,
    _mapping,
    _nonempty_string,
    _positive_integer,
    _string_list,
    _validate_commit,
    _validate_repository,
    _validate_tag,
)


def build_repository_governance(
    main_effective_rules: object,
    protected_branches: object,
    branch_rulesets: object,
    tag_rulesets: object,
    environments: Mapping[str, object],
    immutable_releases: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
) -> dict[str, object]:
    """Build a deterministic receipt for enforced main/tag release governance."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    branch = _main_ruleset_protection_receipt(main_effective_rules, branch_rulesets)
    protected_branch_names = _protected_branch_names(protected_branches)
    tags = _tag_protection_receipts(tag_rulesets)
    environment_receipts = _environment_receipts(environments)
    immutable_receipt = _immutable_releases_receipt(immutable_releases)
    receipt: dict[str, object] = {
        "schema_version": "1.1",
        "repository": repository,
        "source_commit": source_commit,
        "tag": tag,
        "main_branch": "main",
        "protected_branches": protected_branch_names,
        "main_protection": branch,
        "tag_pattern": RELEASE_TAG_PATTERN,
        "tag_protections": tags,
        "environment_reviewers_available": False,
        "environments": environment_receipts,
        "immutable_releases": immutable_receipt,
    }
    verify_repository_governance(
        receipt,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
    )
    return receipt


def verify_repository_governance(
    receipt: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
) -> None:
    """Fail unless a governance receipt proves the required live controls."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    document = _mapping(receipt, "repository governance receipt")
    expected = {
        "schema_version": "1.1",
        "repository": repository,
        "source_commit": source_commit,
        "tag": tag,
        "main_branch": "main",
        "protected_branches": ["main"],
        "tag_pattern": RELEASE_TAG_PATTERN,
        "environment_reviewers_available": False,
    }
    mismatches = [key for key, value in expected.items() if document.get(key) != value]
    if mismatches:
        raise ProvenanceError(
            f"repository governance receipt identity mismatch: {sorted(mismatches)}"
        )
    _verify_main_protection(_mapping(document.get("main_protection"), "main protection"))
    tag_protections = _list(document.get("tag_protections"), "tag protections")
    if not tag_protections:
        raise ProvenanceError("repository governance receipt has no enforced tag protection")
    for raw in tag_protections:
        _verify_tag_protection(_mapping(raw, "tag protection"))
    environment_receipts = _list(document.get("environments"), "environments")
    if [_mapping(item, "environment receipt").get("name") for item in environment_receipts] != list(
        REQUIRED_ENVIRONMENTS
    ):
        raise ProvenanceError("repository governance receipt environment set does not match")
    for raw in environment_receipts:
        environment = _mapping(raw, "environment receipt")
        if environment.get("protection_rules") != ["branch_policy"]:
            raise ProvenanceError(
                "environment receipt does not enforce protected-branch deployment policy"
            )
        if (
            environment.get("required_reviewers") != []
            or environment.get("required_reviewers_available") is not False
        ):
            raise ProvenanceError(
                "environment receipt must not claim unavailable reviewer protection"
            )
        if environment.get("can_admins_bypass") is not False:
            raise ProvenanceError("environment receipt permits administrator bypass")
        if environment.get("deployment_branch_policy") != {
            "protected_branches": True,
            "custom_branch_policies": False,
        }:
            raise ProvenanceError("environment receipt branch policy does not match")
    immutable = _mapping(document.get("immutable_releases"), "immutable releases receipt")
    if immutable != {
        "api_version": IMMUTABLE_RELEASES_API_VERSION,
        "enabled": True,
        "enforced_by_owner": True,
    }:
        raise ProvenanceError("repository governance does not enforce immutable releases")


def fetch_live_repository_governance(
    *,
    repository: str,
    source_commit: str,
    tag: str,
    fetch_json: GitHubJsonFetcher,
    fetch_admin_json: GitHubJsonFetcher,
) -> dict[str, object]:
    """Query and normalize the current GitHub controls for a release identity."""
    _validate_repository(repository)
    _validate_commit(source_commit)
    _validate_tag(tag)
    main_effective_rules = fetch_json(f"repos/{repository}/rules/branches/main?per_page=100")
    protected_branches = fetch_json(f"repos/{repository}/branches?protected=true&per_page=100")
    effective = _list(main_effective_rules, "effective main branch rules")
    if len(effective) >= 100:
        raise ProvenanceError("effective main branch rule query is not provably complete")
    branch_ruleset_ids = sorted(
        {
            _positive_integer(
                _mapping(raw, "effective main branch rule").get("ruleset_id"),
                "effective main branch ruleset id",
            )
            for raw in effective
        }
    )
    branch_rulesets = [
        fetch_json(f"repos/{repository}/rulesets/{ruleset_id}") for ruleset_id in branch_ruleset_ids
    ]
    summaries = _list(
        fetch_json(f"repos/{repository}/rulesets?includes_parents=true&per_page=100"),
        "repository rulesets",
    )
    if len(summaries) >= 100:
        raise ProvenanceError("repository ruleset query is not provably complete")
    tag_rulesets: list[object] = []
    for raw in summaries:
        summary = _mapping(raw, "repository ruleset summary")
        if summary.get("target") != "tag":
            continue
        ruleset_id = _positive_integer(summary.get("id"), "repository ruleset id")
        tag_rulesets.append(fetch_json(f"repos/{repository}/rulesets/{ruleset_id}"))
    environments: dict[str, object] = {}
    for name in REQUIRED_ENVIRONMENTS:
        environment = _mapping(
            fetch_json(f"repos/{repository}/environments/{name}"),
            f"environment {name}",
        )
        environments[name] = environment
    immutable_releases = fetch_admin_json(f"repos/{repository}/immutable-releases")
    return build_repository_governance(
        main_effective_rules,
        protected_branches,
        branch_rulesets,
        tag_rulesets,
        environments,
        immutable_releases,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
    )


def verify_live_repository_governance(
    receipt: object,
    *,
    repository: str,
    source_commit: str,
    tag: str,
    fetch_json: GitHubJsonFetcher,
    fetch_admin_json: GitHubJsonFetcher,
) -> None:
    """Require the carried governance receipt to equal current GitHub state."""
    verify_repository_governance(
        receipt,
        repository=repository,
        source_commit=source_commit,
        tag=tag,
    )
    current = fetch_live_repository_governance(
        repository=repository,
        source_commit=source_commit,
        tag=tag,
        fetch_json=fetch_json,
        fetch_admin_json=fetch_admin_json,
    )
    if current != receipt:
        raise ProvenanceError(
            "carried repository governance receipt differs from current GitHub controls"
        )


def _main_ruleset_protection_receipt(
    effective_rules_document: object,
    rulesets_document: object,
) -> dict[str, object]:
    effective_rules = _list(effective_rules_document, "effective main branch rules")
    if len(effective_rules) >= 100:
        raise ProvenanceError("effective main branch rule query is not provably complete")
    rulesets = _list(rulesets_document, "main branch rulesets")
    normalized_rulesets: dict[int, dict[str, object]] = {}
    for raw in rulesets:
        ruleset = _mapping(raw, "main branch ruleset")
        ruleset_id = _positive_integer(ruleset.get("id"), "main branch ruleset id")
        if ruleset_id in normalized_rulesets:
            raise ProvenanceError(f"duplicate main branch ruleset id: {ruleset_id}")
        if ruleset.get("target") != "branch" or ruleset.get("enforcement") != "active":
            raise ProvenanceError(f"main branch ruleset {ruleset_id} is not active")
        conditions = _mapping(ruleset.get("conditions"), "main branch ruleset conditions")
        ref_name = _mapping(conditions.get("ref_name"), "main branch ruleset ref_name")
        includes = sorted(_string_list(ref_name.get("include"), "main ruleset include"))
        excludes = sorted(_string_list(ref_name.get("exclude", []), "main ruleset exclude"))
        if "refs/heads/main" not in includes and "~DEFAULT_BRANCH" not in includes:
            raise ProvenanceError(f"main branch ruleset {ruleset_id} does not cover main")
        if excludes:
            raise ProvenanceError(f"main branch ruleset {ruleset_id} has exclusions")
        if ruleset.get("current_user_can_bypass") != "never":
            raise ProvenanceError(
                f"current workflow token can bypass main branch ruleset {ruleset_id}"
            )
        bypass_visible = "bypass_actors" in ruleset
        bypass_count: int | None = None
        if bypass_visible:
            bypass_count = len(
                _list(ruleset.get("bypass_actors"), "main branch ruleset bypass actors")
            )
        normalized_rulesets[ruleset_id] = {
            "id": ruleset_id,
            "name": _nonempty_string(ruleset.get("name"), "main branch ruleset name"),
            "enforcement": "active",
            "current_workflow_token_can_bypass": False,
            "global_bypass_actors_visible": bypass_visible,
            "configured_bypass_actor_count": bypass_count,
        }
    if not normalized_rulesets:
        raise ProvenanceError("no active ruleset supplies effective main branch rules")

    by_type: dict[str, list[dict[str, object]]] = {}
    effective_ruleset_ids: set[int] = set()
    for raw in effective_rules:
        rule = _mapping(raw, "effective main branch rule")
        rule_type = _nonempty_string(rule.get("type"), "effective main branch rule type")
        ruleset_id = _positive_integer(rule.get("ruleset_id"), "effective rule ruleset id")
        if ruleset_id not in normalized_rulesets:
            raise ProvenanceError(
                f"effective main rule references unavailable ruleset {ruleset_id}"
            )
        effective_ruleset_ids.add(ruleset_id)
        by_type.setdefault(rule_type, []).append(rule)
    required_types = {
        "deletion",
        "merge_queue",
        "non_fast_forward",
        "pull_request",
        "required_status_checks",
    }
    missing = sorted(required_types - set(by_type))
    if missing:
        raise ProvenanceError(f"effective main branch rules are incomplete: {missing}")
    for rule_type in required_types:
        if len(by_type[rule_type]) != 1:
            raise ProvenanceError(f"effective main branch rule is ambiguous: {rule_type}")

    status = _mapping(
        by_type["required_status_checks"][0].get("parameters"),
        "effective required status checks parameters",
    )
    status_checks: list[dict[str, object]] = []
    for raw in _list(status.get("required_status_checks"), "effective required status checks"):
        check = _mapping(raw, "effective required status check")
        status_checks.append(
            {
                "context": _nonempty_string(check.get("context"), "effective status check context"),
                "app_id": _positive_integer(
                    check.get("integration_id"), "effective status check integration id"
                ),
            }
        )
    status_checks.sort(key=lambda item: cast(str, item["context"]))
    reviews = _mapping(
        by_type["pull_request"][0].get("parameters"),
        "effective pull request rule parameters",
    )
    merge_queue = _mapping(
        by_type["merge_queue"][0].get("parameters"),
        "effective merge queue rule parameters",
    )
    normalized_merge_queue = {key: merge_queue.get(key) for key in REQUIRED_MERGE_QUEUE_PARAMETERS}
    if normalized_merge_queue != REQUIRED_MERGE_QUEUE_PARAMETERS or set(merge_queue) != set(
        REQUIRED_MERGE_QUEUE_PARAMETERS
    ):
        raise ProvenanceError("effective merge queue parameters differ from the release contract")
    receipt: dict[str, object] = {
        "source": "effective_rulesets",
        "ruleset_ids": sorted(effective_ruleset_ids),
        "rulesets": [normalized_rulesets[item] for item in sorted(effective_ruleset_ids)],
        "strict_status_checks": status.get("strict_required_status_checks_policy") is True,
        "required_status_checks": status_checks,
        "review_policy": MAIN_REVIEW_POLICY,
        "required_approving_review_count": _integer(
            reviews.get("required_approving_review_count"), "required approval count"
        ),
        "dismiss_stale_reviews": reviews.get("dismiss_stale_reviews_on_push") is True,
        "require_last_push_approval": reviews.get("require_last_push_approval") is True,
        "required_conversation_resolution": reviews.get("required_review_thread_resolution")
        is True,
        "merge_queue": normalized_merge_queue,
        "prevents_force_pushes": True,
        "prevents_deletions": True,
        "current_workflow_token_can_bypass": False,
        "global_bypass_visibility_complete": all(
            cast(bool, item["global_bypass_actors_visible"])
            for item in normalized_rulesets.values()
        ),
    }
    _verify_main_protection(receipt)
    return receipt


def _protected_branch_names(document: object) -> list[str]:
    """Require main to be the repository's sole protected deployment branch."""
    branches = _list(document, "protected branches")
    if len(branches) >= 100:
        raise ProvenanceError("protected branch query is not provably complete")
    names: list[str] = []
    for raw in branches:
        branch = _mapping(raw, "protected branch")
        name = _nonempty_string(branch.get("name"), "protected branch name")
        if branch.get("protected") is not True:
            raise ProvenanceError(f"protected branch query returned an unprotected branch: {name}")
        names.append(name)
    if sorted(names) != ["main"] or len(set(names)) != len(names):
        raise ProvenanceError(
            "main must be the sole protected branch admitted by release environments"
        )
    return ["main"]


def _verify_main_protection(branch: Mapping[str, object]) -> None:
    checks = branch.get("required_status_checks")
    approval_count = branch.get("required_approving_review_count")
    last_push_approval = branch.get("require_last_push_approval")
    expected_checks = [
        {"context": context, "app_id": GITHUB_ACTIONS_APP_ID}
        for context in sorted(REQUIRED_CI_JOBS)
    ]
    failures = {
        "effective_ruleset_source": branch.get("source") == "effective_rulesets",
        "ruleset_ids": isinstance(branch.get("ruleset_ids"), list)
        and bool(branch.get("ruleset_ids")),
        "strict_status_checks": branch.get("strict_status_checks") is True,
        "required_status_checks": checks == expected_checks,
        "review_policy": branch.get("review_policy") == MAIN_REVIEW_POLICY,
        "required_approving_review_count": type(approval_count) is int
        and approval_count == REQUIRED_APPROVING_REVIEW_COUNT,
        "dismiss_stale_reviews": branch.get("dismiss_stale_reviews") is True,
        "require_last_push_approval": type(last_push_approval) is bool
        and last_push_approval == REQUIRE_LAST_PUSH_APPROVAL,
        "required_conversation_resolution": branch.get("required_conversation_resolution") is True,
        "merge_queue": branch.get("merge_queue") == REQUIRED_MERGE_QUEUE_PARAMETERS,
        "prevents_force_pushes": branch.get("prevents_force_pushes") is True,
        "prevents_deletions": branch.get("prevents_deletions") is True,
        "current_workflow_token_cannot_bypass": branch.get("current_workflow_token_can_bypass")
        is False,
    }
    rulesets = _list(branch.get("rulesets"), "main protection rulesets")
    ruleset_ids = branch.get("ruleset_ids")
    normalized_ids: list[int] = []
    for raw in rulesets:
        ruleset = _mapping(raw, "main protection ruleset")
        normalized_ids.append(_positive_integer(ruleset.get("id"), "main ruleset id"))
        if ruleset.get("enforcement") != "active":
            raise ProvenanceError("main protection receipt contains an inactive ruleset")
        if ruleset.get("current_workflow_token_can_bypass") is not False:
            raise ProvenanceError("main protection receipt permits workflow-token bypass")
        visible = ruleset.get("global_bypass_actors_visible")
        count = ruleset.get("configured_bypass_actor_count")
        if visible is True:
            _integer(count, "visible configured bypass actor count")
        elif visible is False:
            if count is not None:
                raise ProvenanceError(
                    "hidden global bypass actors must not be represented as empty"
                )
        else:
            raise ProvenanceError("main protection bypass visibility is invalid")
    if normalized_ids != ruleset_ids:
        failures["ruleset_receipts"] = False
    visibility = branch.get("global_bypass_visibility_complete")
    if visibility is not all(
        _mapping(raw, "main protection ruleset").get("global_bypass_actors_visible") is True
        for raw in rulesets
    ):
        failures["global_bypass_visibility"] = False
    rejected = sorted(name for name, passed in failures.items() if not passed)
    if rejected:
        raise ProvenanceError(f"main branch protection is incomplete: {rejected}")


def _tag_protection_receipts(document: object) -> list[dict[str, object]]:
    rulesets = _list(document, "tag rulesets")
    matching: list[dict[str, object]] = []
    for raw in rulesets:
        ruleset = _mapping(raw, "tag ruleset")
        if ruleset.get("target") != "tag" or ruleset.get("enforcement") != "active":
            continue
        conditions = _mapping(ruleset.get("conditions"), "tag ruleset conditions")
        ref_name = _mapping(conditions.get("ref_name"), "tag ruleset ref_name")
        includes = sorted(_string_list(ref_name.get("include"), "tag ruleset include"))
        excludes = sorted(_string_list(ref_name.get("exclude", []), "tag ruleset exclude"))
        if RELEASE_TAG_PATTERN not in includes and "~ALL" not in includes:
            continue
        if excludes:
            continue
        bypass_visible = "bypass_actors" in ruleset
        bypass_count: int | None = None
        if bypass_visible:
            bypass_count = len(_list(ruleset.get("bypass_actors"), "tag ruleset bypass actors"))
        rule_types = sorted(
            _nonempty_string(_mapping(item, "tag rule").get("type"), "tag rule type")
            for item in _list(ruleset.get("rules"), "tag rules")
        )
        if not {"deletion", "update"}.issubset(rule_types):
            continue
        receipt: dict[str, object] = {
            "id": _positive_integer(ruleset.get("id"), "tag ruleset id"),
            "name": _nonempty_string(ruleset.get("name"), "tag ruleset name"),
            "enforcement": "active",
            "include": includes,
            "exclude": excludes,
            "global_bypass_actors_visible": bypass_visible,
            "configured_bypass_actor_count": bypass_count,
            "current_workflow_token_can_bypass": ruleset.get("current_user_can_bypass") != "never",
            "rules": rule_types,
        }
        _verify_tag_protection(receipt)
        matching.append(receipt)
    if not matching:
        raise ProvenanceError(
            "no active workflow-token-protected tag ruleset prevents v* deletion and rewrites"
        )
    return sorted(matching, key=lambda item: cast(int, item["id"]))


def _verify_tag_protection(ruleset: Mapping[str, object]) -> None:
    _positive_integer(ruleset.get("id"), "tag protection id")
    _nonempty_string(ruleset.get("name"), "tag protection name")
    includes = ruleset.get("include")
    rules = ruleset.get("rules")
    checks = {
        "active": ruleset.get("enforcement") == "active",
        "covers_release_tags": isinstance(includes, list)
        and (RELEASE_TAG_PATTERN in includes or "~ALL" in includes),
        "no_exclusions": ruleset.get("exclude") == [],
        "current_workflow_token_cannot_bypass": ruleset.get("current_workflow_token_can_bypass")
        is False,
        "prevents_deletion": isinstance(rules, list) and "deletion" in rules,
        "prevents_rewrite": isinstance(rules, list) and "update" in rules,
    }
    visible = ruleset.get("global_bypass_actors_visible")
    count = ruleset.get("configured_bypass_actor_count")
    if visible is True:
        _integer(count, "visible configured tag bypass actor count")
    elif visible is False:
        if count is not None:
            checks["hidden_bypass_not_claimed_empty"] = False
    else:
        checks["bypass_visibility"] = False
    rejected = sorted(name for name, passed in checks.items() if not passed)
    if rejected:
        raise ProvenanceError(f"tag protection is incomplete: {rejected}")


def _environment_receipts(environments: Mapping[str, object]) -> list[dict[str, object]]:
    if set(environments) != set(REQUIRED_ENVIRONMENTS):
        raise ProvenanceError(
            "live environment set differs from the release contract: "
            f"expected={list(REQUIRED_ENVIRONMENTS)}, observed={sorted(environments)}"
        )
    receipts: list[dict[str, object]] = []
    for name in REQUIRED_ENVIRONMENTS:
        environment = _mapping(environments[name], f"environment {name}")
        if environment.get("name") != name:
            raise ProvenanceError(f"environment API identity mismatch for {name}")
        protection_rules = _list(
            environment.get("protection_rules", []), f"environment {name} protection rules"
        )
        protection_types = sorted(
            _nonempty_string(
                _mapping(item, f"environment {name} protection rule").get("type"),
                f"environment {name} protection rule type",
            )
            for item in protection_rules
        )
        if protection_types != ["branch_policy"]:
            raise ProvenanceError(
                f"environment {name} must enforce only protected-branch deployment policy"
            )
        branch_policy = _mapping(
            environment.get("deployment_branch_policy"),
            f"environment {name} deployment branch policy",
        )
        if (
            branch_policy.get("protected_branches") is not True
            or branch_policy.get("custom_branch_policies") is not False
        ):
            raise ProvenanceError(
                f"environment {name} does not require a protected-branch deployment policy"
            )
        if environment.get("can_admins_bypass") is not False:
            raise ProvenanceError(f"environment {name} must disable administrator bypass")
        receipts.append(
            {
                "name": name,
                "protection_rules": ["branch_policy"],
                "required_reviewers": [],
                "required_reviewers_available": False,
                "can_admins_bypass": False,
                "deployment_branch_policy": {
                    "protected_branches": True,
                    "custom_branch_policies": False,
                },
            }
        )
    return receipts


def _immutable_releases_receipt(document: object) -> dict[str, object]:
    """Normalize the administration-read immutable-release policy response."""
    policy = _mapping(document, "immutable releases policy")
    if set(policy) != {"enabled", "enforced_by_owner"}:
        raise ProvenanceError("immutable releases policy contains an unexpected schema")
    if policy.get("enabled") is not True or policy.get("enforced_by_owner") is not True:
        raise ProvenanceError(
            "immutable releases must be enabled and enforced by the repository owner"
        )
    return {
        "api_version": IMMUTABLE_RELEASES_API_VERSION,
        "enabled": True,
        "enforced_by_owner": True,
    }
