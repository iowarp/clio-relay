"""Tests for the ``queue`` core-management command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ten core-management ``queue_app`` commands' extraction into
``src/clio_relay/cli_queue.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.
``test_cli_queue_management_commands`` also exercises ``queue cleanup-stale``
(the sibling ``cli_queue_maintenance.py``'s only command in this flow) as
one step among five -- it stays here rather than splitting, since the
majority of its steps (list/diagnose/stale/cancel) belong to this group.
``test_remote_owned_job_discovery_never_cancels_unrelated_session`` stays in
``tests/test_cli.py`` -- it fakes ``queue owner-jobs``/``queue cancel``
remote calls as intermediate steps inside a broader session-teardown flow,
not a test of this group's commands themselves.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). It is reproduced here (the env-var half only,
the same precedent ``tests/test_cli_relay_host.py``'s own
``_default_cli_mode`` established) -- the trap
``tests/test_cli_worker.py``'s docstring documents hitting for real.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from clio_relay.cli import app
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import JarvisRunSpec, JobKind, RelayJob


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only."""
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_cli_repairs_lease_operational_indexes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cli-repair-lease-indexes",
        )
    )
    lease = queue.acquire_job(job.job_id, "worker", cluster=job.cluster)
    assert lease is not None
    identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        lease,
        job=queue.get_job(job.job_id),
    )
    endpoint_ref = queue._lease_endpoint_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    endpoint_ref.unlink()
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(app, ["queue", "repair-lease-indexes"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["complete"] is True
    assert payload["record_count"] == 1
    assert endpoint_ref.is_file()


def test_cli_audits_lease_capacity_and_exits_nonzero_on_mismatch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cli-audit-lease-capacity",
        )
    )
    lease = queue.acquire_job(job.job_id, "worker", cluster=job.cluster)
    assert lease is not None
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    valid = CliRunner().invoke(app, ["queue", "audit-lease-capacity"])

    assert valid.exit_code == 0
    report = json.loads(valid.output)
    assert report["schema_version"] == "clio-relay.lease-capacity-audit.v1"
    assert report["valid"] is True

    aggregate_path = core_dir / "lease_capacity" / "aggregate.json"
    aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
    aggregate["document_sha256"] = "0" * 64
    aggregate_path.write_text(json.dumps(aggregate), encoding="utf-8")

    invalid = CliRunner().invoke(app, ["queue", "audit-lease-capacity"])

    assert invalid.exit_code == 1
    invalid_report = json.loads(invalid.output)
    assert invalid_report["valid"] is False
    assert invalid_report["mismatches"][0]["type"] == "audit_error"


def test_cli_queue_management_commands(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="cli-queue-management",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="test-cluster", ttl_seconds=-1)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    listed = runner.invoke(
        app,
        [
            "queue",
            "list",
            "--cluster",
            "test-cluster",
            "--kind",
            "jarvis",
            "--limit",
            "1",
        ],
    )
    diagnosed = runner.invoke(
        app,
        ["queue", "diagnose", job.job_id, "--cluster", "test-cluster"],
    )
    stale = runner.invoke(
        app,
        [
            "queue",
            "stale",
            "--cluster",
            "test-cluster",
            "--older-than",
            "1h",
        ],
    )
    cleanup = runner.invoke(
        app,
        [
            "queue",
            "cleanup-stale",
            "--cluster",
            "test-cluster",
            "--no-dry-run",
        ],
    )
    canceled = runner.invoke(
        app,
        ["queue", "cancel", job.job_id, "--cluster", "test-cluster"],
    )

    assert listed.exit_code == 0
    assert diagnosed.exit_code == 0
    assert stale.exit_code == 0
    assert cleanup.exit_code == 0
    assert canceled.exit_code == 0
    assert json.loads(listed.output)["count"] == 1
    assert json.loads(listed.output)["jobs"][0]["job"]["kind"] == "jarvis"
    assert json.loads(diagnosed.output)["reason"] == "stale_lease"
    assert json.loads(stale.output)["jobs"][0]["job"]["job_id"] == job.job_id
    assert json.loads(cleanup.output)["recovered_count"] == 1
    assert json.loads(canceled.output)["scheduler_policy"] == "relay-only"
