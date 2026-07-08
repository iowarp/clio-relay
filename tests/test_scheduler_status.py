from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, cast

from pytest import MonkeyPatch

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    RelayTask,
    SchedulerPhase,
)
from clio_relay.relay_ops import job_status
from clio_relay.scheduler_providers import SlurmSchedulerProvider
from clio_relay.scheduler_status import relay_queue_status


def test_relay_queue_status_counts_older_cluster_jobs(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    first = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: first\npkgs: []\n"),
            idempotency_key="first",
        )
    )
    second = queue.submit_job(
        RelayJob(
            cluster="other",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: other\npkgs: []\n"),
            idempotency_key="other",
        )
    )
    third = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: third\npkgs: []\n"),
            idempotency_key="third",
        )
    )

    assert relay_queue_status(queue, first) == {
        "state": "queued",
        "jobs_ahead": 0,
        "position": 1,
    }
    assert relay_queue_status(queue, second)["jobs_ahead"] == 0
    assert relay_queue_status(queue, third) == {
        "state": "queued",
        "jobs_ahead": 1,
        "position": 2,
    }
    queue.update_job_state(first.job_id, JobState.RUNNING)
    assert relay_queue_status(queue, queue.get_job(first.job_id)) == {
        "state": "running",
        "jobs_ahead": None,
        "position": None,
    }


def test_poll_slurm_status_reports_pending_queue_position(monkeypatch: MonkeyPatch) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert text is True
        assert capture_output is True
        assert check is False
        if command[:4] == ["squeue", "-h", "-j", "100"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "100|PENDING|Resources|compute|normal|alice|1|4|4G|2026-07-07T10:00:00|N/A|0:00|1:00:00\n",
                "",
            )
        if command[:4] == ["squeue", "-h", "-t", "PD"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "\n".join(
                    [
                        "99|PENDING|Priority|compute|normal|bob|1|4|4G|2026-07-07T09:00:00|N/A|0:00|1:00:00",
                        "100|PENDING|Resources|compute|normal|alice|1|4|4G|2026-07-07T10:00:00|N/A|0:00|1:00:00",
                        "101|PENDING|Priority|debug|normal|bob|1|4|4G|2026-07-07T08:00:00|N/A|0:00|1:00:00",
                    ]
                ),
                "",
            )
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    status = SlurmSchedulerProvider().poll("100")

    assert status.phase == SchedulerPhase.PENDING
    assert status.reason == "Resources"
    assert status.partition == "compute"
    assert status.jobs_ahead == 1
    assert status.queue_position == 2
    assert status.queue_position_note is not None


def test_poll_slurm_status_uses_sacct_when_squeue_is_empty(monkeypatch: MonkeyPatch) -> None:
    def fake_run(
        command: list[str],
        *,
        text: bool,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del text, capture_output, check
        if command[:4] == ["squeue", "-h", "-j", "100"]:
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:4] == ["sacct", "-n", "-P", "-j"]:
            return subprocess.CompletedProcess(
                command,
                0,
                "100|COMPLETED|compute|normal|2026-07-07T10:00:00|2026-07-07T10:01:00|00:02:00|1|4|4G\n",
                "",
            )
        raise AssertionError(command)

    monkeypatch.setattr("clio_relay.scheduler_providers.subprocess.run", fake_run)

    status = SlurmSchedulerProvider().poll("100")

    assert status.phase == SchedulerPhase.COMPLETED
    assert status.raw_state == "COMPLETED"


def test_job_status_includes_relay_queue_and_scheduler_metadata(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: queued\npkgs: []\n"),
            idempotency_key="status-job",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    queue.update_task_metadata(
        task.task_id,
        {
            "scheduler_status": {
                "scheduler": "slurm",
                "scheduler_job_id": "100",
                "phase": "pending",
            }
        },
    )

    status = job_status(queue, job.job_id)

    assert status["relay_queue"] == {"state": "queued", "jobs_ahead": 0, "position": 1}
    scheduler = cast(list[dict[str, Any]], status["scheduler"])
    assert scheduler[0]["status"]["scheduler_job_id"] == "100"
