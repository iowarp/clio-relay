"""Local release checks that emit the same evidence contract as live runs."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from clio_relay.command_evidence import command_evidence
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import (
    WINDOWS_LEGACY_PATH_HEADROOM,
    internal_filesystem_path,
    logical_filesystem_path,
    logical_filesystem_text,
)
from clio_relay.identifiers import DurableRecordId
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    new_live_validation_report,
    sha256_file,
)

_CONTAINMENT_HARD_CRASH_TESTS = (
    "tests/test_process_containment.py::test_enforceable_provider_rejects_and_kills_background_escape",
    "tests/test_endpoint.py::test_hard_crashed_worker_is_reconciled_before_cancellation_acknowledgment",
    "tests/test_endpoint.py::test_hard_crash_before_broker_release_never_starts_workload_and_reconciles",
    "tests/test_endpoint.py::test_hard_crashed_non_canceled_attempt_blocks_requeue_until_cleanup_retry",
    "tests/test_endpoint.py::test_retry_exhausted_hard_crash_cleans_sidecars_without_requeue",
    "tests/test_storage_process_guard.py::test_runtime_storage_violation_terminates_owned_child_tree",
    "tests/test_storage_managed_queue.py::test_hard_crash_reservation_is_reconciled_before_canonical_retry",
)
_SIDECAR_RECLAMATION_TESTS = (
    "tests/test_endpoint.py::test_pending_execution_cleanup_processes_truncated_batches_automatically",
    "tests/test_endpoint.py::test_cleanup_batch_reaches_expired_marker_after_live_lease_markers",
    "tests/test_endpoint.py::test_execution_cleanup_marker_is_durable_before_task_metadata_update",
    "tests/test_endpoint.py::test_execution_cleanup_empty_directory_crash_boundaries_fail_closed",
    "tests/test_endpoint.py::test_execution_cleanup_legacy_flat_markers_migrate_in_bounded_batches",
    "tests/test_endpoint.py::test_execution_sidecar_cleanup_removes_only_owned_non_directory_entries",
    "tests/test_endpoint.py::test_windows_sidecar_cleanup_anchors_parent_and_rejects_reparse_points",
)
_RETENTION_STORAGE_PAGINATION_TESTS = (
    "tests/test_core_global_pagination.py::test_global_order_upgrade_is_explicit_bounded_and_handles_more_than_500_jobs",
    "tests/test_core_index_safety.py::test_stale_recovery_uses_exact_scheduler_indexes_without_global_task_scan",
    "tests/test_core_index_safety.py::test_gateway_reverse_indexes_refuse_cardinality_overflow",
    "tests/test_core_retention.py::test_terminal_gc_scales_past_501_owned_and_unrelated_records",
    "tests/test_core_retention.py::test_gc_purge_is_iterative_and_detects_directory_swap_races",
    "tests/test_retention.py::test_outer_retention_fails_closed_on_source_swap_before_anchored_rename",
    "tests/test_retention.py::test_outer_retention_purge_is_bounded_past_501_spool_entries",
    "tests/test_storage_managed_queue.py::test_managed_queue_never_scans_storage_while_core_lock_is_held",
    "tests/test_storage_managed_queue.py::test_managed_queue_recovers_crash_reserved_canonical_id_without_leak",
    "tests/test_surface_pagination.py::test_sparse_job_filters_return_empty_source_page_with_next_cursor",
)
_PYTEST_RELEASE_GATE_ARGUMENTS = ("-p", "clio_relay.pytest_release_gate")


class ReleaseCommandRunner(Protocol):
    """Injectable command runner for local release checks."""

    def __call__(
        self,
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        """Run one command in the release checkout."""
        ...


@dataclass(frozen=True)
class LocalReleaseValidationOptions:
    """Paths and evidence outputs for local release validation."""

    project_root: Path
    report_path: Path
    markdown_report_path: Path | None = None
    artifact_dir: Path | None = None
    report_id: DurableRecordId | None = None


def run_local_release_validation(
    options: LocalReleaseValidationOptions,
    *,
    runner: ReleaseCommandRunner | None = None,
) -> LiveValidationReport:
    """Run every local release check and persist partial evidence on failure."""
    root = logical_filesystem_path(options.project_root).resolve()
    storage_root = internal_filesystem_path(root, force_extended=True)
    if not (storage_root / "pyproject.toml").is_file():
        raise ConfigurationError(f"release checkout has no pyproject.toml: {root}")
    command_runner = runner or _run_command
    report = new_live_validation_report(
        scenario="local-release",
        cluster="local",
        launcher="uv",
        install_source=f"checkout:{root.as_uri()}",
        report_id=options.report_id,
    )
    recorder = ValidationRecorder(report)
    logical_artifact_dir = (
        options.artifact_dir or root / ".clio-relay" / "release-artifacts" / report.report_id
    ).resolve()
    artifact_dir = internal_filesystem_path(logical_artifact_dir, force_extended=True)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    support_dir = artifact_dir / ".validation-support"
    support_dir.mkdir(parents=True, exist_ok=True)
    try:
        _run_check(
            recorder,
            "local.ruff-check",
            "Ruff lint check",
            ["uv", "run", "--no-sync", "ruff", "check"],
            root=root,
            runner=command_runner,
        )
        _run_check(
            recorder,
            "local.ruff-format",
            "Ruff formatting check",
            ["uv", "run", "--no-sync", "ruff", "format", "--check"],
            root=root,
            runner=command_runner,
        )
        _run_check(
            recorder,
            "local.pyright",
            "strict Python type check",
            ["uv", "run", "--no-sync", "pyright"],
            root=root,
            runner=command_runner,
        )
        _run_check(
            recorder,
            "local.pytest",
            "full pytest suite with no failed, skipped, xfailed, xpassed, or deselected tests",
            [
                "uv",
                "run",
                "--no-sync",
                "pytest",
                *_PYTEST_RELEASE_GATE_ARGUMENTS,
                "-ra",
            ],
            root=root,
            runner=command_runner,
        )
        _run_required_tests(
            recorder,
            check_id="local.containment-hard-crash",
            summary="execute kernel containment and hard-crash acceptance tests",
            node_ids=_CONTAINMENT_HARD_CRASH_TESTS,
            root=root,
            runner=command_runner,
        )
        _run_required_tests(
            recorder,
            check_id="local.sidecar-reclamation",
            summary="execute durable sidecar cleanup acceptance tests",
            node_ids=_SIDECAR_RECLAMATION_TESTS,
            root=root,
            runner=command_runner,
        )
        _run_required_tests(
            recorder,
            check_id="local.retention-storage-pagination",
            summary="execute bounded retention, storage, and pagination acceptance tests",
            node_ids=_RETENTION_STORAGE_PAGINATION_TESTS,
            root=root,
            runner=command_runner,
        )
        audit_requirements = support_dir / "dependency-audit-requirements.txt"
        _run_check(
            recorder,
            "local.dependency-lock-export",
            "export the exact dependency lock with hashes",
            [
                "uv",
                "export",
                "--quiet",
                "--locked",
                "--all-groups",
                "--no-emit-project",
                "--output-file",
                str(audit_requirements),
            ],
            root=root,
            runner=command_runner,
        )
        _run_check(
            recorder,
            "local.dependency-audit",
            "audit every locked dependency against known vulnerabilities",
            [
                "uv",
                "run",
                "--no-sync",
                "pip-audit",
                "--strict",
                "--require-hashes",
                "--disable-pip",
                "--progress-spinner",
                "off",
                "--requirement",
                str(audit_requirements),
            ],
            root=root,
            runner=command_runner,
        )
        _run_check(
            recorder,
            "local.build-backend",
            "verify the exact lock-installed build backend identity",
            [
                "uv",
                "run",
                "--no-sync",
                "python",
                "-c",
                (
                    "import importlib.metadata as m; "
                    "version=m.version('hatchling'); print(f'hatchling={version}'); "
                    "assert version == '1.31.0'"
                ),
            ],
            root=root,
            runner=command_runner,
        )
        _run_check(
            recorder,
            "local.build",
            "build wheel and source distribution",
            [
                "uv",
                "build",
                "--no-build-isolation",
                "--python",
                sys.executable,
                "--out-dir",
                str(artifact_dir),
            ],
            root=root,
            runner=command_runner,
        )
        artifacts = _release_artifacts(artifact_dir)
        _record_release_artifacts(recorder, artifacts)
        _run_check(
            recorder,
            "local.artifact-check",
            "validate wheel and source distribution metadata",
            ["uv", "run", "--no-sync", "twine", "check", *[str(path) for path in artifacts]],
            root=root,
            runner=command_runner,
        )
        wheel = next(path for path in artifacts if path.suffix == ".whl")
        sdist = next(path for path in artifacts if path.name.endswith(".tar.gz"))
        runtime_requirements = support_dir / "runtime-requirements.txt"
        _run_check(
            recorder,
            "local.runtime-lock-export",
            "export the exact production dependency lock with hashes",
            [
                "uv",
                "export",
                "--quiet",
                "--locked",
                "--no-dev",
                "--no-emit-project",
                "--output-file",
                str(runtime_requirements),
            ],
            root=root,
            runner=command_runner,
        )
        smoke_environment = support_dir / "wheel-smoke-environment"
        smoke_python = smoke_environment / (
            "Scripts/python.exe" if os.name == "nt" else "bin/python"
        )
        smoke_executable = smoke_environment / (
            "Scripts/clio-relay.exe" if os.name == "nt" else "bin/clio-relay"
        )
        try:
            _run_check_sequence(
                recorder,
                "local.wheel-smoke",
                "install and launch the exact wheel against only hashed locked dependencies",
                [
                    [
                        "uv",
                        "venv",
                        "--clear",
                        "--python",
                        sys.executable,
                        str(smoke_environment),
                    ],
                    [
                        "uv",
                        "pip",
                        "sync",
                        "--python",
                        str(smoke_python),
                        "--require-hashes",
                        str(runtime_requirements),
                    ],
                    [
                        "uv",
                        "pip",
                        "install",
                        "--python",
                        str(smoke_python),
                        "--no-deps",
                        str(wheel),
                    ],
                    ["uv", "pip", "freeze", "--python", str(smoke_python)],
                    [str(smoke_executable), "--help"],
                ],
                root=root,
                runner=command_runner,
            )
        finally:
            shutil.rmtree(smoke_environment, ignore_errors=True)
        _run_sdist_smoke(
            recorder,
            sdist=sdist,
            runtime_requirements=runtime_requirements,
            support_dir=support_dir,
            root=root,
            runner=command_runner,
        )
    except BaseException as exc:
        if not report.checks or report.checks[-1].status.value != "failed":
            recorder.record_failure("local-release.completed", "complete local release gate", exc)
        recorder.finish(exc)
        recorder.write(
            internal_filesystem_path(options.report_path, force_extended=True),
            None
            if options.markdown_report_path is None
            else internal_filesystem_path(options.markdown_report_path, force_extended=True),
        )
        raise
    recorder.finish()
    recorder.write(
        internal_filesystem_path(options.report_path, force_extended=True),
        None
        if options.markdown_report_path is None
        else internal_filesystem_path(options.markdown_report_path, force_extended=True),
    )
    return report


def _run_check(
    recorder: ValidationRecorder,
    check_id: str,
    summary: str,
    command: list[str],
    *,
    root: Path,
    runner: ReleaseCommandRunner,
) -> str:
    with recorder.check(check_id, summary) as evidence:
        completed = runner(command, cwd=root)
        diagnostic = command_evidence(
            _logicalize_windows_text(completed.stdout),
            _logicalize_windows_text(completed.stderr),
            exit_code=completed.returncode,
        )
        evidence.append(
            EvidenceReference(
                kind="command",
                reference=" ".join(_logical_command(command)),
                excerpt=diagnostic.excerpt,
                metadata=diagnostic.metadata,
            )
        )
        if completed.returncode != 0:
            raise RelayError(f"release check failed ({check_id}): {diagnostic.error_detail}")
        return diagnostic.output


def _run_check_sequence(
    recorder: ValidationRecorder,
    check_id: str,
    summary: str,
    commands: list[list[str]],
    *,
    root: Path,
    runner: ReleaseCommandRunner,
) -> None:
    with recorder.check(check_id, summary) as evidence:
        for command in commands:
            completed = runner(command, cwd=root)
            diagnostic = command_evidence(
                _logicalize_windows_text(completed.stdout),
                _logicalize_windows_text(completed.stderr),
                exit_code=completed.returncode,
            )
            evidence.append(
                EvidenceReference(
                    kind="command",
                    reference=" ".join(_logical_command(command)),
                    excerpt=diagnostic.excerpt,
                    metadata=diagnostic.metadata,
                )
            )
            if completed.returncode != 0:
                raise RelayError(f"release check failed ({check_id}): {diagnostic.error_detail}")


def _run_required_tests(
    recorder: ValidationRecorder,
    *,
    check_id: str,
    summary: str,
    node_ids: tuple[str, ...],
    root: Path,
    runner: ReleaseCommandRunner,
) -> None:
    """Execute the exact named production acceptance tests under the strict gate."""
    _run_check(
        recorder,
        check_id,
        summary,
        [
            "uv",
            "run",
            "--no-sync",
            "pytest",
            *_PYTEST_RELEASE_GATE_ARGUMENTS,
            "-q",
            *node_ids,
        ],
        root=root,
        runner=runner,
    )


def _run_sdist_smoke(
    recorder: ValidationRecorder,
    *,
    sdist: Path,
    runtime_requirements: Path,
    support_dir: Path,
    root: Path,
    runner: ReleaseCommandRunner,
) -> None:
    """Build and launch the exact sdist only inside the unprivileged local gate."""
    build_dir = support_dir / "sdist-wheel-build"
    environment = support_dir / "sdist-smoke-environment"
    smoke_python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    smoke_executable = environment / (
        "Scripts/clio-relay.exe" if os.name == "nt" else "bin/clio-relay"
    )
    shutil.rmtree(build_dir, ignore_errors=True)
    shutil.rmtree(environment, ignore_errors=True)
    build_dir.mkdir(parents=True, exist_ok=True)
    with recorder.check(
        "local.sdist-smoke",
        "build, install, and launch the exact sdist in an isolated runtime environment",
    ) as evidence:
        try:
            build_command = [
                "uv",
                "build",
                "--no-build-isolation",
                "--wheel",
                "--python",
                sys.executable,
                "--out-dir",
                str(build_dir),
                str(sdist),
            ]
            _run_evidenced_command(evidence, build_command, root=root, runner=runner)
            wheels = sorted(build_dir.glob("*.whl"))
            if len(wheels) != 1:
                raise RelayError(
                    "sdist smoke must build exactly one wheel; "
                    f"found {[path.name for path in wheels]}"
                )
            commands = [
                ["uv", "venv", "--clear", "--python", sys.executable, str(environment)],
                [
                    "uv",
                    "pip",
                    "sync",
                    "--python",
                    str(smoke_python),
                    "--require-hashes",
                    str(runtime_requirements),
                ],
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(smoke_python),
                    "--no-deps",
                    str(wheels[0]),
                ],
                ["uv", "pip", "freeze", "--python", str(smoke_python)],
                [str(smoke_executable), "--help"],
            ]
            for command in commands:
                _run_evidenced_command(evidence, command, root=root, runner=runner)
        finally:
            shutil.rmtree(environment, ignore_errors=True)
            shutil.rmtree(build_dir, ignore_errors=True)


def _run_evidenced_command(
    evidence: list[EvidenceReference],
    command: list[str],
    *,
    root: Path,
    runner: ReleaseCommandRunner,
) -> None:
    completed = runner(command, cwd=root)
    diagnostic = command_evidence(
        _logicalize_windows_text(completed.stdout),
        _logicalize_windows_text(completed.stderr),
        exit_code=completed.returncode,
    )
    evidence.append(
        EvidenceReference(
            kind="command",
            reference=" ".join(_logical_command(command)),
            excerpt=diagnostic.excerpt,
            metadata=diagnostic.metadata,
        )
    )
    if completed.returncode != 0:
        raise RelayError(f"release check failed (local.sdist-smoke): {diagnostic.error_detail}")


def _release_artifacts(artifact_dir: Path) -> list[Path]:
    wheels = sorted(artifact_dir.glob("*.whl"))
    sdists = sorted(artifact_dir.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise RelayError(
            "release build must produce exactly one wheel and one source distribution; "
            f"found wheels={len(wheels)}, sdists={len(sdists)}"
        )
    return [*wheels, *sdists]


def _record_release_artifacts(
    recorder: ValidationRecorder,
    artifacts: list[Path],
) -> None:
    for path in artifacts:
        kind = "wheel" if path.suffix == ".whl" else "source_distribution"
        digest = sha256_file(path)
        logical_path = logical_filesystem_path(path)
        recorder.add_resource(
            ValidationResource(
                kind=kind,
                resource_id=path.name,
                role="release_artifact",
                cluster="local",
                references=[str(logical_path)],
                metadata={"sha256": digest, "size_bytes": path.stat().st_size},
            )
        )
        recorder.report.artifacts.append(
            EvidenceReference(kind=kind, reference=str(logical_path), sha256=digest)
        )


def _run_command(
    command: list[str],
    *,
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    logical_cwd = logical_filesystem_path(cwd)
    if os.name == "nt":
        absolute_cwd = os.path.abspath(logical_cwd)
        if absolute_cwd.startswith("\\\\"):
            raise ConfigurationError(
                "release subprocess checkout paths on Windows must not use UNC; "
                "run the gate from a local checkout path"
            )
        if len(absolute_cwd) >= WINDOWS_LEGACY_PATH_HEADROOM:
            raise ConfigurationError(
                "release subprocess checkout path exceeds the verified Windows path bound; "
                "run the gate from a shorter checkout path"
            )
    return subprocess.run(
        command,
        cwd=logical_cwd,
        capture_output=True,
        check=False,
        text=True,
    )


def _logical_command(command: list[str]) -> list[str]:
    """Remove internal Windows path prefixes from recorded command evidence."""
    return [_logicalize_windows_text(argument) for argument in command]


def _logicalize_windows_text(value: str) -> str:
    return logical_filesystem_text(value)
