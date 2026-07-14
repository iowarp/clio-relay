from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import RelayError
from clio_relay.models import EndpointRole, RelayTask


def _worker(tmp_path: Path) -> EndpointWorker:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    return EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="configured-target",
        queue=ClioCoreQueue(settings.core_dir),
    )


def test_worker_refuses_truncated_exact_task_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)

    def truncated_tasks(_job_id: str, *, limit: int) -> tuple[list[RelayTask], bool]:
        del limit
        return [], True

    monkeypatch.setattr(worker.queue, "scan_job_tasks", truncated_tasks)

    with pytest.raises(RelayError, match="task index exceeded its safety bound"):
        worker._bounded_job_tasks("job")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_worker_scheduler_reconciliation_never_scans_lifetime_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)

    def fail_lifetime_scan(*, limit: int) -> tuple[list[object], bool]:
        del limit
        raise AssertionError("scheduler reconciliation read lifetime job history")

    monkeypatch.setattr(worker.queue, "scan_jobs", fail_lifetime_scan)

    worker._reconcile_canceled_scheduler_jobs()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
