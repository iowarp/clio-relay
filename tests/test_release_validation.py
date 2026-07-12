"""Tests for evidence-producing local release checks."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from clio_relay.errors import RelayError
from clio_relay.release_validation import (
    LocalReleaseValidationOptions,
    run_local_release_validation,
)
from clio_relay.validation_report import ValidationStatus, load_validation_report


def test_local_release_validation_runs_all_checks_and_records_artifacts(
    tmp_path: Path,
) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n", encoding="utf-8")
    report_path = tmp_path / "report.json"
    commands: list[list[str]] = []

    def runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        assert cwd == tmp_path.resolve()
        commands.append(command)
        if command[:2] == ["uv", "build"]:
            output_dir = Path(command[command.index("--out-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "clio_relay-1.0.0-py3-none-any.whl").write_bytes(b"wheel")
            (output_dir / "clio_relay-1.0.0.tar.gz").write_bytes(b"sdist")
        return subprocess.CompletedProcess(command, 0, stdout="250 passed", stderr="")

    result = run_local_release_validation(
        LocalReleaseValidationOptions(project_root=tmp_path, report_path=report_path),
        runner=runner,
    )

    assert result.status is ValidationStatus.PASSED
    assert {check.check_id for check in result.checks} == {
        "local.ruff-check",
        "local.ruff-format",
        "local.pyright",
        "local.pytest",
        "local.containment-hard-crash",
        "local.sidecar-reclamation",
        "local.retention-storage-pagination",
        "local.dependency-lock-export",
        "local.dependency-audit",
        "local.build-backend",
        "local.build",
        "local.artifact-check",
        "local.runtime-lock-export",
        "local.wheel-smoke",
        "local.sdist-smoke",
    }
    assert {resource.kind for resource in result.resources} == {
        "wheel",
        "source_distribution",
    }
    assert any(command[:5] == ["uv", "run", "--no-sync", "twine", "check"] for command in commands)
    assert any(command[:3] == ["uv", "build", "--no-build-isolation"] for command in commands)
    assert any(
        command[:4] == ["uv", "build", "--no-build-isolation", "--wheel"]
        and command[-1].endswith(".tar.gz")
        for command in commands
    )
    assert any(command[:2] == ["uv", "venv"] for command in commands)
    assert any("--require-hashes" in command for command in commands)
    assert commands[-1][-1] == "--help"
    pytest_commands = [
        command for command in commands if command[:4] == ["uv", "run", "--no-sync", "pytest"]
    ]
    assert len(pytest_commands) == 4
    assert all(
        command[4:6] == ["-p", "clio_relay.pytest_release_gate"] for command in pytest_commands
    )
    assert all("--collect-only" not in command for command in pytest_commands)
    assert pytest_commands[1][7:] == [
        "tests/test_process_containment.py::test_enforceable_provider_rejects_and_kills_background_escape",
        "tests/test_endpoint.py::test_hard_crashed_worker_is_reconciled_before_cancellation_acknowledgment",
        "tests/test_endpoint.py::test_hard_crash_before_broker_release_never_starts_workload_and_reconciles",
        "tests/test_endpoint.py::test_hard_crashed_non_canceled_attempt_blocks_requeue_until_cleanup_retry",
        "tests/test_endpoint.py::test_retry_exhausted_hard_crash_cleans_sidecars_without_requeue",
        "tests/test_storage_process_guard.py::test_runtime_storage_violation_terminates_owned_child_tree",
        "tests/test_storage_managed_queue.py::test_hard_crash_reservation_is_reconciled_before_canonical_retry",
    ]
    assert pytest_commands[2][7:] == [
        "tests/test_endpoint.py::test_pending_execution_cleanup_processes_truncated_batches_automatically",
        "tests/test_endpoint.py::test_cleanup_batch_reaches_expired_marker_after_live_lease_markers",
        "tests/test_endpoint.py::test_execution_cleanup_marker_is_durable_before_task_metadata_update",
        "tests/test_endpoint.py::test_execution_cleanup_empty_directory_crash_boundaries_fail_closed",
        "tests/test_endpoint.py::test_execution_cleanup_legacy_flat_markers_migrate_in_bounded_batches",
        "tests/test_endpoint.py::test_execution_sidecar_cleanup_removes_only_owned_non_directory_entries",
        "tests/test_endpoint.py::test_windows_sidecar_cleanup_anchors_parent_and_rejects_reparse_points",
    ]
    assert pytest_commands[3][7:] == [
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
    ]
    assert load_validation_report(report_path).status is ValidationStatus.PASSED


def test_local_release_validation_persists_structured_pytest_gate_failure(
    tmp_path: Path,
) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n", encoding="utf-8")
    report_path = tmp_path / "failed.json"

    def runner(
        command: list[str],
        *,
        cwd: Path,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        if "pytest" in command:
            return subprocess.CompletedProcess(
                command,
                1,
                stdout=(
                    "1 passed, 1 xfailed\n"
                    "release gate rejected outcomes: "
                    "skipped=0, xfailed=1, xpassed=0, deselected=0"
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    with pytest.raises(RelayError, match=r"release check failed \(local\.pytest\)"):
        run_local_release_validation(
            LocalReleaseValidationOptions(project_root=tmp_path, report_path=report_path),
            runner=runner,
        )

    report = load_validation_report(report_path)
    assert report.status is ValidationStatus.FAILED
    failed = next(check for check in report.checks if check.check_id == "local.pytest")
    assert failed.status is ValidationStatus.FAILED


@pytest.mark.parametrize(
    ("source", "extra_arguments", "expected_outcome"),
    [
        (
            "import pytest\n\ndef test_case():\n    pytest.skip('not available')\n",
            [],
            "skipped=1",
        ),
        (
            (
                "import pytest\n\n@pytest.mark.xfail(reason='expected failure')\n"
                "def test_case():\n    assert False\n"
            ),
            [],
            "xfailed=1",
        ),
        (
            (
                "import pytest\n\n@pytest.mark.xfail(reason='expected failure')\n"
                "def test_case():\n    assert True\n"
            ),
            [],
            "xpassed=1",
        ),
        (
            "def test_keep():\n    assert True\n\ndef test_drop():\n    assert True\n",
            ["-k", "keep"],
            "deselected=1",
        ),
    ],
)
def test_pytest_release_gate_rejects_non_clean_structured_outcomes(
    tmp_path: Path,
    source: str,
    extra_arguments: list[str],
    expected_outcome: str,
) -> None:
    (tmp_path / "test_outcome.py").write_text(source, encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "clio_relay.pytest_release_gate",
            "-q",
            *extra_arguments,
            "test_outcome.py",
        ],
        cwd=tmp_path,
        capture_output=True,
        check=False,
        text=True,
    )

    output = completed.stdout + completed.stderr
    assert completed.returncode == int(pytest.ExitCode.TESTS_FAILED), output
    assert "release gate rejected outcomes" in output
    assert expected_outcome in output


def test_pytest_release_gate_accepts_only_clean_passes(tmp_path: Path) -> None:
    (tmp_path / "test_outcome.py").write_text(
        "def test_case():\n    assert True\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "clio_relay.pytest_release_gate",
            "-q",
            "test_outcome.py",
        ],
        cwd=tmp_path,
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == int(pytest.ExitCode.OK), completed.stdout + completed.stderr
