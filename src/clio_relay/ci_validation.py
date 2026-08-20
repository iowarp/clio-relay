"""Build and verify release receipts from live GitHub repository state."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

from clio_relay.actions_artifact import (
    build_actions_artifact_manifest,
    verify_actions_artifact_archive,
)
from clio_relay.branch_protection import (
    build_repository_governance,
    verify_live_repository_governance,
    verify_repository_governance,
)
from clio_relay.candidate_provenance import (
    build_candidate_build_receipt,
    build_tag_binding,
)
from clio_relay.ci_run_status import build_ci_status, select_ci_run, verify_ci_status
from clio_relay.distribution_archive import build_distribution_archive_receipt
from clio_relay.payload_policy import (
    write_candidate_checksum_manifest,
)
from clio_relay.provenance_primitives import (
    REQUIRED_ENVIRONMENTS,
    ProvenanceError,
    _github_fetcher,
    _load_json,
    _mapping,
    _write_json,
)
from clio_relay.release_assets import (
    build_exact_release_asset_inventory,
    build_staged_release_asset_plan,
    verify_exact_release_asset_inventory,
)

# Re-exported for external callers outside this split's file budget
# (clio-relay#231 TREE DISCIPLINE): release_pins.py's own test suite
# (module-attribute access, ``ci_validation.<name>``) and
# validation_report.py both still reach these three names through
# ``clio_relay.ci_validation`` rather than their true new owner,
# ``clio_relay.release_assets``. The ``as`` form marks them as an
# intentional public re-export so lint does not flag them as unused.
from clio_relay.release_assets import (
    compute_release_acceptance_matrix_sha256 as compute_release_acceptance_matrix_sha256,
)
from clio_relay.release_assets import (
    load_release_acceptance_matrix as load_release_acceptance_matrix,
)
from clio_relay.release_assets import (
    validate_release_acceptance_matrix as validate_release_acceptance_matrix,
)
from clio_relay.release_identity import (
    resolve_live_release,
    verify_live_mutation_authority,
    verify_live_release_identity,
)
from clio_relay.validation_report_assets import (
    build_validation_report_asset_manifest,
    verify_downloaded_validation_report_assets,
)


def _error(message: str) -> NoReturn:
    raise SystemExit(message)


def main(argv: Sequence[str] | None = None) -> int:
    """Build or verify canonical release prerequisite receipts."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    select = subparsers.add_parser("select-ci-run")
    select.add_argument("--runs", type=Path, required=True)
    select.add_argument("--repository", required=True)
    select.add_argument("--source-commit", required=True)
    select.add_argument("--output", type=Path, required=True)

    build_ci = subparsers.add_parser("build-ci-status")
    build_ci.add_argument("--runs", type=Path, required=True)
    build_ci.add_argument("--jobs", type=Path, required=True)
    build_ci.add_argument("--candidate-build", type=Path, required=True)
    build_ci.add_argument("--candidate-artifact", type=Path, required=True)
    build_ci.add_argument("--tag-binding", type=Path, required=True)
    build_ci.add_argument("--repository", required=True)
    build_ci.add_argument("--source-commit", required=True)
    build_ci.add_argument("--output", type=Path, required=True)

    artifact_manifest = subparsers.add_parser("actions-artifact-manifest")
    artifact_manifest.add_argument("--run", type=Path, required=True)
    artifact_manifest.add_argument("--artifacts", type=Path, required=True)
    artifact_manifest.add_argument("--repository", required=True)
    artifact_manifest.add_argument("--source-commit", required=True)
    artifact_manifest.add_argument("--tag", required=True)
    artifact_manifest.add_argument("--run-id", type=int, required=True)
    artifact_manifest.add_argument("--run-attempt", type=int, required=True)
    artifact_manifest.add_argument("--artifact-name", required=True)
    artifact_manifest.add_argument(
        "--artifact-kind",
        choices=("candidate", "tag-binding", "tag-payload", "promotion"),
        required=True,
    )
    artifact_manifest.add_argument("--source-tree")
    artifact_manifest.add_argument("--output", type=Path, required=True)

    extract_artifact = subparsers.add_parser("extract-actions-artifact")
    extract_artifact.add_argument("--manifest", type=Path, required=True)
    extract_artifact.add_argument("--archive", type=Path, required=True)
    extract_artifact.add_argument("--output-dir", type=Path, required=True)

    candidate_manifest = subparsers.add_parser("candidate-manifest")
    candidate_manifest.add_argument("--candidate-dir", type=Path, required=True)

    staged_assets = subparsers.add_parser("staged-assets")
    staged_assets.add_argument("--release", type=Path, required=True)
    staged_assets.add_argument("--candidate-dir", type=Path, required=True)
    staged_assets.add_argument("--output", type=Path, required=True)

    verify_ci = subparsers.add_parser("verify-ci-status")
    verify_ci.add_argument("--receipt", type=Path, required=True)
    verify_ci.add_argument("--repository", required=True)
    verify_ci.add_argument("--source-commit", required=True)

    build_governance = subparsers.add_parser("build-governance")
    build_governance.add_argument("--main-effective-rules", type=Path, required=True)
    build_governance.add_argument("--protected-branches", type=Path, required=True)
    build_governance.add_argument("--branch-rulesets", type=Path, required=True)
    build_governance.add_argument("--tag-rulesets", type=Path, required=True)
    build_governance.add_argument("--environments-dir", type=Path, required=True)
    build_governance.add_argument("--immutable-releases", type=Path, required=True)
    build_governance.add_argument("--repository", required=True)
    build_governance.add_argument("--source-commit", required=True)
    build_governance.add_argument("--tag", required=True)
    build_governance.add_argument("--output", type=Path, required=True)

    verify_governance = subparsers.add_parser("verify-governance")
    verify_governance.add_argument("--receipt", type=Path, required=True)
    verify_governance.add_argument("--repository", required=True)
    verify_governance.add_argument("--source-commit", required=True)
    verify_governance.add_argument("--tag", required=True)

    verify_live_governance = subparsers.add_parser("verify-live-governance")
    verify_live_governance.add_argument("--receipt", type=Path, required=True)
    verify_live_governance.add_argument("--repository", required=True)
    verify_live_governance.add_argument("--source-commit", required=True)
    verify_live_governance.add_argument("--tag", required=True)

    verify_live_release = subparsers.add_parser("verify-live-release")
    verify_live_release.add_argument("--repository", required=True)
    verify_live_release.add_argument("--tag", required=True)
    verify_live_release.add_argument("--source-commit", required=True)
    verify_live_release.add_argument("--draft", choices=("true", "false", "any"), required=True)
    verify_live_release.add_argument("--prerelease", choices=("true", "false"), required=True)
    verify_live_release.add_argument("--immutable", choices=("true", "false", "any"), default="any")

    resolve_release = subparsers.add_parser("resolve-live-release")
    resolve_release.add_argument("--repository", required=True)
    resolve_release.add_argument("--tag", required=True)
    resolve_release.add_argument("--draft", choices=("true", "false", "any"), required=True)
    resolve_release.add_argument("--allow-absent", action="store_true")
    resolve_release.add_argument("--immutable", choices=("true", "false", "any"), default="any")
    resolve_release.add_argument("--output", type=Path, required=True)

    mutation_authority = subparsers.add_parser("mutation-authority")
    mutation_authority.add_argument("--governance-receipt", type=Path, required=True)
    mutation_authority.add_argument("--repository", required=True)
    mutation_authority.add_argument("--source-commit", required=True)
    mutation_authority.add_argument("--tag", required=True)
    mutation_authority.add_argument("--workflow-ref", required=True)
    mutation_authority.add_argument("--workflow-sha", required=True)
    mutation_authority.add_argument(
        "--release-state",
        choices=("absent", "present"),
        required=True,
    )
    mutation_authority.add_argument("--draft", choices=("true", "false", "any"), required=True)

    report_assets = subparsers.add_parser("report-assets")
    report_assets.add_argument("--release", type=Path, required=True)
    report_assets.add_argument("--kind", choices=("candidate", "released"), required=True)
    report_assets.add_argument("--matrix", type=Path, required=True)
    report_assets.add_argument("--report-dir", type=Path)
    report_assets.add_argument("--output", type=Path, required=True)

    distributions = subparsers.add_parser("distribution-archives")
    distributions.add_argument("--wheel", type=Path, required=True)
    distributions.add_argument("--sdist", type=Path, required=True)
    distributions.add_argument("--project", required=True)
    distributions.add_argument("--version", required=True)
    distributions.add_argument("--output", type=Path, required=True)

    exact_assets = subparsers.add_parser("exact-release-assets")
    exact_assets.add_argument("--release", type=Path, required=True)
    exact_assets.add_argument("--next-assets-page", type=Path, required=True)
    exact_assets.add_argument("--page-size", type=int, required=True)
    exact_assets.add_argument("--asset", type=Path, action="append", required=True)
    exact_destination = exact_assets.add_mutually_exclusive_group(required=True)
    exact_destination.add_argument("--output", type=Path)
    exact_destination.add_argument("--verify-existing", type=Path)

    candidate_build = subparsers.add_parser("candidate-build-receipt")
    candidate_build.add_argument("--candidate-dir", type=Path, required=True)
    candidate_build.add_argument("--reports-dir", type=Path, required=True)
    candidate_build.add_argument("--repository", required=True)
    candidate_build.add_argument("--source-commit", required=True)
    candidate_build.add_argument("--source-tree", required=True)
    candidate_build.add_argument("--event", required=True)
    candidate_build.add_argument("--run-id", type=int, required=True)
    candidate_build.add_argument("--run-attempt", type=int, required=True)
    candidate_build.add_argument("--head-ref", required=True)
    candidate_build.add_argument("--base-ref", required=True)
    candidate_build.add_argument("--output", type=Path, required=True)

    tag_binding = subparsers.add_parser("tag-binding")
    tag_binding.add_argument("--candidate-build", type=Path, required=True)
    tag_binding.add_argument("--candidate-artifact", type=Path, required=True)
    tag_binding.add_argument("--pulls", type=Path, required=True)
    tag_binding.add_argument("--repository", required=True)
    tag_binding.add_argument("--source-commit", required=True)
    tag_binding.add_argument("--source-tree", required=True)
    tag_binding.add_argument("--tag", required=True)
    tag_binding.add_argument("--output", type=Path, required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "select-ci-run":
            selected = select_ci_run(
                _load_json(args.runs),
                repository=args.repository,
                source_commit=args.source_commit,
            )
            _write_json(args.output, selected)
        elif args.command == "build-ci-status":
            receipt = build_ci_status(
                _load_json(args.runs),
                _load_json(args.jobs),
                _load_json(args.candidate_build),
                _load_json(args.candidate_artifact),
                _load_json(args.tag_binding),
                repository=args.repository,
                source_commit=args.source_commit,
            )
            _write_json(args.output, receipt)
        elif args.command == "actions-artifact-manifest":
            manifest = build_actions_artifact_manifest(
                _load_json(args.run),
                _load_json(args.artifacts),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                artifact_name=args.artifact_name,
                artifact_kind=args.artifact_kind,
                source_tree=args.source_tree,
            )
            _write_json(args.output, manifest)
        elif args.command == "extract-actions-artifact":
            verify_actions_artifact_archive(
                _load_json(args.manifest),
                args.archive,
                args.output_dir,
            )
        elif args.command == "candidate-manifest":
            write_candidate_checksum_manifest(args.candidate_dir)
        elif args.command == "staged-assets":
            plan = build_staged_release_asset_plan(
                _load_json(args.release),
                args.candidate_dir,
            )
            _write_json(args.output, plan)
        elif args.command == "verify-ci-status":
            verify_ci_status(
                _load_json(args.receipt),
                repository=args.repository,
                source_commit=args.source_commit,
            )
        elif args.command == "build-governance":
            environment_documents: dict[str, object] = {}
            for name in REQUIRED_ENVIRONMENTS:
                environment = _mapping(
                    _load_json(args.environments_dir / f"{name}.json"),
                    f"environment {name}",
                )
                environment_documents[name] = environment
            receipt = build_repository_governance(
                _load_json(args.main_effective_rules),
                _load_json(args.protected_branches),
                _load_json(args.branch_rulesets),
                _load_json(args.tag_rulesets),
                environment_documents,
                _load_json(args.immutable_releases),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
            )
            _write_json(args.output, receipt)
        elif args.command == "verify-governance":
            verify_repository_governance(
                _load_json(args.receipt),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
            )
        elif args.command == "verify-live-governance":
            verify_live_repository_governance(
                _load_json(args.receipt),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
                fetch_admin_json=_github_fetcher(os.environ.get("GH_ADMIN_READ_TOKEN", "")),
            )
        elif args.command == "verify-live-release":
            verify_live_release_identity(
                repository=args.repository,
                tag=args.tag,
                source_commit=args.source_commit,
                expect_draft=None if args.draft == "any" else args.draft == "true",
                expect_prerelease=args.prerelease == "true",
                expect_immutable=(None if args.immutable == "any" else args.immutable == "true"),
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
            )
        elif args.command == "resolve-live-release":
            release = resolve_live_release(
                repository=args.repository,
                tag=args.tag,
                expect_draft=None if args.draft == "any" else args.draft == "true",
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
                allow_absent=args.allow_absent,
                expect_immutable=(None if args.immutable == "any" else args.immutable == "true"),
            )
            _write_json(args.output, release)
        elif args.command == "mutation-authority":
            verify_live_mutation_authority(
                _load_json(args.governance_receipt),
                repository=args.repository,
                source_commit=args.source_commit,
                tag=args.tag,
                workflow_ref=args.workflow_ref,
                workflow_sha=args.workflow_sha,
                release_state=args.release_state,
                expect_draft=None if args.draft == "any" else args.draft == "true",
                fetch_json=_github_fetcher(os.environ.get("GH_TOKEN", "")),
                fetch_admin_json=_github_fetcher(os.environ.get("GH_ADMIN_READ_TOKEN", "")),
            )
        elif args.command == "report-assets":
            manifest = build_validation_report_asset_manifest(
                _load_json(args.release),
                kind=args.kind,
                acceptance_matrix=_load_json(args.matrix),
            )
            if args.report_dir is not None:
                verify_downloaded_validation_report_assets(manifest, args.report_dir)
            _write_json(args.output, manifest)
        elif args.command == "distribution-archives":
            receipt = build_distribution_archive_receipt(
                args.wheel,
                args.sdist,
                project=args.project,
                version=args.version,
            )
            _write_json(args.output, receipt)
        elif args.command == "exact-release-assets":
            release_document = _load_json(args.release)
            if args.verify_existing is not None:
                verify_exact_release_asset_inventory(
                    _load_json(args.verify_existing),
                    release_document,
                    args.asset,
                    next_page_document=_load_json(args.next_assets_page),
                    page_size=args.page_size,
                )
            else:
                inventory = build_exact_release_asset_inventory(
                    release_document,
                    args.asset,
                    next_page_document=_load_json(args.next_assets_page),
                    page_size=args.page_size,
                )
                _write_json(args.output, inventory)
        elif args.command == "candidate-build-receipt":
            receipt = build_candidate_build_receipt(
                args.candidate_dir,
                args.reports_dir,
                repository=args.repository,
                source_commit=args.source_commit,
                source_tree=args.source_tree,
                event=args.event,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                head_ref=args.head_ref,
                base_ref=args.base_ref,
            )
            _write_json(args.output, receipt)
        elif args.command == "tag-binding":
            binding = build_tag_binding(
                _load_json(args.candidate_build),
                _load_json(args.candidate_artifact),
                _load_json(args.pulls),
                repository=args.repository,
                source_commit=args.source_commit,
                source_tree=args.source_tree,
                tag=args.tag,
            )
            _write_json(args.output, binding)
        else:  # pragma: no cover - argparse owns command validation.
            _error(f"unsupported command: {args.command}")
    except ProvenanceError as exc:
        _error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
