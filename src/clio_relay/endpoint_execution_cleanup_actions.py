"""Per-task execution-cleanup actions: terminate, reconcile canceled, quarantine, and
remove sidecars.

Owner module for iowarp/clio-relay#231's endpoint decomposition. The action family
``_reconcile_pending_execution_cleanup`` (``endpoint_execution_lifecycle.py``)
dispatches to per task: terminate an orphaned execution or its recovery-query process
(``_terminate_recorded_execution``/ ``_terminate_recorded_jarvis_recovery_query``),
reconcile one canceled execution (``_reconcile_canceled_execution``), and
quarantine/remove its sidecars once ownership is proven
(``_stage_execution_sidecar_quarantine``/ ``_ensure_recorded_execution_cleanup_plan``/
``_remove_recorded_execution_sidecars``).
"""

from __future__ import annotations

import socket
from pathlib import Path
from typing import cast

from clio_relay import process_containment
from clio_relay.endpoint_execution_sidecar_cleanup import (
    _execution_cleanup_ack_metadata,
    _execution_cleanup_quarantine_paths,
    _execution_sidecar_cleanup_plan,
    _remove_execution_sidecars,
)
from clio_relay.endpoint_jarvis_recovery import (
    _durable_jarvis_execution_recovery,
)
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _runtime_sidecar_anchor_from_metadata,
)
from clio_relay.endpoint_sidecar_types import (
    EXECUTION_CLEANUP_SCHEMA,
    EXECUTION_LAUNCH_PROTOCOL,
    _RuntimeSidecarAnchor,
)
from clio_relay.errors import RelayError
from clio_relay.models import (
    JobState,
    RelayJob,
    RelayTask,
    utc_now,
)


class ExecutionCleanupActionsMixin:
    """Mixin: ExecutionCleanupActions methods split from EndpointWorker (clio-relay#231)."""

    def _terminate_recorded_execution(
        self,
        task: RelayTask,
        *,
        allow_unstarted: bool = False,
    ) -> int | None:
        """Terminate one task's recorded process tree, or prove launch was unreleased."""
        raw_ownership = task.metadata.get("execution_ownership")
        if not isinstance(raw_ownership, dict):
            raw_cleanup = task.metadata.get("execution_cleanup")
            cleanup = cast(dict[str, object], raw_cleanup) if isinstance(raw_cleanup, dict) else {}
            if (
                allow_unstarted
                and cleanup.get("schema_version") == EXECUTION_CLEANUP_SCHEMA
                and cleanup.get("launch_protocol") == EXECUTION_LAUNCH_PROTOCOL
            ):
                return None
            raise RelayError(
                f"cannot prove cleanup for prior task without execution ownership: {task.task_id}"
            )
        ownership = cast(dict[str, object], raw_ownership)
        if ownership.get("schema_version") != "clio-relay.execution-ownership.v1":
            raise RelayError(f"unsupported execution ownership for task {task.task_id}")
        current_hostname = socket.gethostname()
        hostname = ownership.get("hostname")
        if hostname != current_hostname:
            raise RelayError(
                f"cannot reconcile task {task.task_id} from host {hostname!r} "
                f"on replacement host {current_hostname!r}"
            )
        process_id = ownership.get("pid")
        start_identity = ownership.get("process_start_identity")
        process_group_id = ownership.get("process_group_id")
        raw_containment = ownership.get("containment")
        containment = (
            cast(dict[str, object], raw_containment) if isinstance(raw_containment, dict) else {}
        )
        if (
            not isinstance(process_id, int)
            or isinstance(process_id, bool)
            or process_id <= 0
            or not isinstance(start_identity, str)
            or not start_identity
            or (
                process_group_id is not None
                and (
                    not isinstance(process_group_id, int)
                    or isinstance(process_group_id, bool)
                    or process_group_id <= 0
                )
            )
        ):
            raise RelayError(f"invalid execution ownership for task {task.task_id}")
        try:
            process_containment.terminate_recorded_process_tree(
                process_id=process_id,
                expected_start_identity=start_identity,
                process_group_id=process_group_id,
                containment_mode=(
                    cast(str, containment["mode"])
                    if isinstance(containment.get("mode"), str)
                    else None
                ),
                systemd_unit=(
                    cast(str, containment["systemd_unit"])
                    if isinstance(containment.get("systemd_unit"), str)
                    else None
                ),
                cgroup_path=(
                    cast(str, containment["cgroup_path"])
                    if isinstance(containment.get("cgroup_path"), str)
                    else None
                ),
            )
        except RuntimeError as exc:
            raise RelayError(
                f"could not reconcile prior execution for task {task.task_id}: {exc}"
            ) from exc
        return process_id

    def _terminate_recorded_jarvis_recovery_query(
        self,
        job: RelayJob,
        task: RelayTask,
    ) -> int | None:
        """Terminate an interrupted read-only recovery query before replaying it."""
        intent = _durable_jarvis_execution_recovery(job, task)
        if intent is None:
            return None
        raw_ownership = intent.get("query_process")
        if raw_ownership is None:
            return None
        if not isinstance(raw_ownership, dict):
            raise RelayError(f"JARVIS recovery query ownership is invalid for task {task.task_id}")
        synthetic = task.model_copy(update={"metadata": {"execution_ownership": raw_ownership}})
        process_id = self._terminate_recorded_execution(synthetic)
        self.queue.update_task_metadata(
            task.task_id,
            {
                "jarvis_execution_recovery": {
                    **intent,
                    "query_process": None,
                }
            },
        )
        self.queue.append_event(
            job.job_id,
            "jarvis.execution_recovery_process_reconciled",
            "Interrupted JARVIS execution recovery query was proven stopped",
            payload={"task_id": task.task_id, "pid": process_id},
        )
        return process_id

    def _reconcile_canceled_execution(self, job: RelayJob) -> None:
        """Prove prior attempt cleanup before acknowledging a recovered cancellation."""
        active_tasks = [
            task
            for task in self._bounded_job_tasks(job.job_id)
            if task.state not in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
        ]
        for task in active_tasks:
            process_id = self._terminate_recorded_execution(task)
            cleanup_metadata = self._remove_recorded_execution_sidecars(job, task)
            self.queue.update_task_state(
                task.task_id,
                JobState.CANCELED,
                message=f"Recovered task cancellation after worker restart: {task.name}",
                metadata={"restart_cleanup_recovered": True},
            )
            self.queue.acknowledge_execution_cleanup(
                job.job_id,
                task.task_id,
                metadata={
                    **cleanup_metadata,
                    "restart_cleanup_acknowledged": True,
                    "restart_cleanup_at": utc_now().isoformat(),
                },
            )
            self.queue.append_event(
                job.job_id,
                "cancellation.execution_reconciled",
                "Prior worker execution tree was proven stopped",
                payload={
                    "task_id": task.task_id,
                    "pid": process_id,
                    "hostname": socket.gethostname(),
                },
            )

    def _stage_execution_sidecar_quarantine(
        self,
        job_id: str,
        task_ids: list[str],
        source: Path,
        quarantine: Path,
    ) -> None:
        """Durably stage one exact sidecar quarantine before cleanup acknowledgment."""
        matches: list[tuple[str, str]] = []
        for task_id in task_ids:
            task = self.queue.get_task(task_id)
            raw_cleanup = task.metadata.get("execution_cleanup")
            if not isinstance(raw_cleanup, dict):
                continue
            raw_sidecars = cast(dict[str, object], raw_cleanup).get("sidecars")
            if not isinstance(raw_sidecars, dict):
                continue
            for role, raw_state in cast(dict[str, object], raw_sidecars).items():
                if not isinstance(raw_state, dict):
                    continue
                state = cast(dict[str, object], raw_state)
                if state.get("source_name") == source.name:
                    matches.append((task_id, role))
        if len(matches) != 1:
            raise RelayError(
                f"execution sidecar quarantine ownership was not unique for {source.name}: {job_id}"
            )
        task_id, role = matches[0]
        self.queue.stage_execution_cleanup_sidecar(
            job_id,
            task_id,
            role=role,
            source_name=source.name,
            quarantine_name=quarantine.name,
        )

    def _ensure_recorded_execution_cleanup_plan(
        self,
        job: RelayJob,
        task: RelayTask,
        *,
        paths_by_role: dict[str, Path],
        expected_anchors: dict[Path, _RuntimeSidecarAnchor],
    ) -> RelayTask:
        """Atomically migrate an anchored legacy marker before quarantine."""
        raw_cleanup = task.metadata.get("execution_cleanup")
        if not isinstance(raw_cleanup, dict):
            raise RelayError(f"execution cleanup state is missing for task {task.task_id}")
        cleanup = cast(dict[str, object], raw_cleanup)
        if (
            cleanup.get("schema_version") != EXECUTION_CLEANUP_SCHEMA
            or cleanup.get("launch_protocol") != EXECUTION_LAUNCH_PROTOCOL
        ):
            raise RelayError(f"execution cleanup state is unsupported for task {task.task_id}")
        raw_plans = cleanup.get("sidecars")
        if isinstance(raw_plans, dict):
            return task
        if raw_plans is not None:
            raise RelayError(f"execution cleanup plans are invalid for task {task.task_id}")
        plans: dict[str, object] = {}
        for role, path in paths_by_role.items():
            anchor = expected_anchors.get(path)
            if anchor is None:
                raise RelayError(
                    f"legacy {role} sidecar anchor is missing for task {task.task_id}; "
                    "cleanup remains pending"
                )
            plans[role] = _execution_sidecar_cleanup_plan(path, anchor)
        return self.queue.migrate_execution_cleanup_plan(
            job.job_id,
            task.task_id,
            cleanup={
                **cleanup,
                "acknowledgment_stage": "prepared",
                "sidecars": plans,
            },
        )

    def _remove_recorded_execution_sidecars(
        self,
        job: RelayJob,
        task: RelayTask,
    ) -> dict[str, object]:
        raw_sidecars = task.metadata.get("execution_sidecars")
        if not isinstance(raw_sidecars, dict):
            raise RelayError(
                f"cannot prove sidecar cleanup for prior task without ownership: {task.task_id}"
            )
        sidecars = cast(dict[str, object], raw_sidecars)
        if sidecars.get("schema_version") != "clio-relay.execution-sidecars.v1":
            raise RelayError(f"unsupported execution sidecar ownership for task {task.task_id}")
        paths: list[Path] = []
        paths_by_role: dict[str, Path] = {}
        expected_anchors: dict[Path, _RuntimeSidecarAnchor] = {}
        for role, prefix in (("progress", ".progress-"), ("runtime", ".runtime-")):
            name = sidecars.get(role)
            if (
                not isinstance(name, str)
                or Path(name).name != name
                or not name.startswith(prefix)
                or not name.endswith(".jsonl")
            ):
                raise RelayError(f"invalid {role} sidecar ownership for task {task.task_id}")
            path = self.settings.spool_dir / job.job_id / name
            paths.append(path)
            paths_by_role[role] = path
            anchor_key = f"{role}_anchor"
            raw_anchor = sidecars.get(anchor_key)
            if raw_anchor is not None:
                expected_anchors[path] = _runtime_sidecar_anchor_from_metadata(
                    raw_anchor,
                    task_id=task.task_id,
                )
            else:
                raise RelayError(
                    f"legacy {role} sidecar anchor is missing for task {task.task_id}; "
                    "cleanup remains pending"
                )
        task = self._ensure_recorded_execution_cleanup_plan(
            job,
            task,
            paths_by_role=paths_by_role,
            expected_anchors=expected_anchors,
        )
        quarantine_paths = _execution_cleanup_quarantine_paths(
            task,
            paths=paths,
            expected_anchors=expected_anchors,
        )
        quarantined = _remove_execution_sidecars(
            paths,
            spool_path=self.settings.spool_dir / job.job_id,
            expected_anchors=expected_anchors,
            expected_quarantines=quarantine_paths,
            on_quarantined=lambda source, quarantine: self._stage_execution_sidecar_quarantine(
                job.job_id,
                [task.task_id],
                source,
                quarantine,
            ),
        )
        return _execution_cleanup_ack_metadata(self.queue.get_task(task.task_id), quarantined)
