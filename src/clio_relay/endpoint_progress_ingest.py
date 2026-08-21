"""Progress-sidecar ingest, poll-cadence wiring, and package progress log tailing.

Owner module for iowarp/clio-relay#231's endpoint decomposition. Covers reading the
authenticated package-progress sidecar (``_ingest_progress_sidecar``), composing the
#259 console-tail step onto the job's own poll cadence
(``_wrap_poll``/``_tail_console_stream``), the per-poll-tick orchestration
(``_poll_running_job``), the storage safety guard (``_check_runtime_storage``), and
tailing a package's own progress log file with reset/truncation detection
(``_ingest_package_progress_logs``/``_drain_package_progress_logs``/
``_baseline_package_progress_logs``).
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from clio_relay.console_stream import (
    CONSOLE_STDERR_STREAM,
    CONSOLE_STREAM,
    ConsoleLiveTailer,
)
from clio_relay.endpoint_progress_log_io import (
    _open_package_progress_log,
    _progress_log_identity,
    _render_progress_log_identity,
)
from clio_relay.endpoint_progress_trust import (
    _progress_from_sidecar_record,
    _progress_log_checkpoint,
    _progress_log_checkpoint_matches,
    _read_bounded_sidecar_record,
)
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _open_owned_sidecar,
)
from clio_relay.endpoint_scheduler_metadata import (
    _runtime_metadata_is_native,
)
from clio_relay.endpoint_sidecar_types import (
    PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES,
    PACKAGE_PROGRESS_LOG_READ_BYTES,
    PROGRESS_SIDECAR_MAX_RECORD_BYTES,
    PROGRESS_SIDECAR_MAX_RECORDS,
    PROGRESS_SIDECAR_MAX_TOTAL_BYTES,
    _PackageProgressLogState,
    _RuntimeSidecarAnchor,
)
from clio_relay.errors import ConfigurationError
from clio_relay.models import (
    Lease,
    RelayJob,
)
from clio_relay.progress_adapters import (
    PackageProgressProvider,
)
from clio_relay.progress_provenance import (
    PackageProgressSourceAuthority,
)
from clio_relay.runtime_metadata import (
    JarvisRuntimeMetadata,
)
from clio_relay.spool import JobSpool
from clio_relay.storage_runtime import (
    StorageRuntimeViolation,
)

if TYPE_CHECKING:
    from clio_relay.core_queue import ClioCoreQueue
    from clio_relay.storage_runtime import StorageRuntime


class ProgressIngestMixin:
    """Mixin: ProgressIngest methods split from EndpointWorker (clio-relay#231).

    ``queue``/``storage_runtime`` are declared ``TYPE_CHECKING``-only (never
    assigned here) so strict pyright can resolve ``self.queue``/
    ``self.storage_runtime`` across this mixin's own methods -- see
    ``JarvisDispatchMixin``'s identical note in
    ``endpoint_jarvis_dispatch.py`` for why.
    """

    if TYPE_CHECKING:
        queue: ClioCoreQueue
        storage_runtime: StorageRuntime | None

    def _ingest_progress_sidecar(
        self,
        job: RelayJob,
        progress_sidecar: Path,
        *,
        progress_sidecar_offset: list[int],
        progress_sidecar_record_count: list[int],
        progress_sidecar_sequence: list[int],
        progress_sidecar_token: str,
        progress_sidecar_anchor: _RuntimeSidecarAnchor,
        failures: list[str],
        allow_final_record: bool = False,
    ) -> None:
        def fail(message: str) -> None:
            if message not in failures:
                failures.append(message)
            self.queue.append_event(job.job_id, "progress.parse_failed", message)

        try:
            handle = _open_owned_sidecar(
                progress_sidecar,
                label="package progress sidecar",
                expected_anchor=progress_sidecar_anchor,
            )
        except ConfigurationError as exc:
            fail(str(exc))
            return
        if handle is None:
            fail("precreated package progress sidecar disappeared")
            return
        with handle:
            size = os.fstat(handle.fileno()).st_size
            if size > PROGRESS_SIDECAR_MAX_TOTAL_BYTES:
                if progress_sidecar_offset[0] <= PROGRESS_SIDECAR_MAX_TOTAL_BYTES:
                    fail("Package progress sidecar exceeded its total byte limit")
                progress_sidecar_offset[0] = size
                return
            handle.seek(progress_sidecar_offset[0])
            while True:
                if progress_sidecar_record_count[0] >= PROGRESS_SIDECAR_MAX_RECORDS:
                    if progress_sidecar_record_count[0] == PROGRESS_SIDECAR_MAX_RECORDS:
                        fail("Package progress sidecar exceeded its record limit")
                        progress_sidecar_record_count[0] += 1
                    progress_sidecar_offset[0] = os.fstat(handle.fileno()).st_size
                    return
                line, status = _read_bounded_sidecar_record(
                    handle,
                    max_bytes=PROGRESS_SIDECAR_MAX_RECORD_BYTES,
                    allow_final_record=allow_final_record,
                )
                if status in {"eof", "incomplete"}:
                    break
                if handle.tell() > PROGRESS_SIDECAR_MAX_TOTAL_BYTES:
                    fail("Package progress sidecar exceeded its total byte limit")
                    progress_sidecar_offset[0] = os.fstat(handle.fileno()).st_size
                    return
                progress_sidecar_record_count[0] += 1
                if status == "oversized":
                    fail("Package progress sidecar record exceeded its byte limit")
                    continue
                assert line is not None
                try:
                    payload = json.loads(line)
                    progress_payload = _progress_from_sidecar_record(
                        payload,
                        expected_key=progress_sidecar_token,
                        expected_sequence=progress_sidecar_sequence[0] + 1,
                    )
                    self._append_package_progress_records(
                        job,
                        [progress_payload],
                        source_event_seq=None,
                        progress_sidecar_authenticated=True,
                    )
                except (
                    ConfigurationError,
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    ValueError,
                ) as exc:
                    fail(f"Side-channel package progress was invalid: {exc}")
                else:
                    progress_sidecar_sequence[0] += 1
            progress_sidecar_offset[0] = handle.tell()

    def _wrap_poll(
        self,
        job: RelayJob,
        console_tailer: ConsoleLiveTailer | None,
        inner: Callable[[], None],
    ) -> Callable[[], None]:
        """Compose the job's own poll step with the #259 console tail step.

        ``inner`` (lease renewal, progress/runtime sidecar ingest, ...) is
        unchanged; this only adds a best-effort console-tail increment on
        the same cadence, self-throttled inside :class:`ConsoleLiveTailer`.
        """

        def _poll() -> None:
            inner()
            self._tail_console_stream(job, console_tailer)

        return _poll

    def _tail_console_stream(
        self,
        job: RelayJob,
        console_tailer: ConsoleLiveTailer | None,
    ) -> None:
        """Advance one #259 live-tail increment (stdout AND stderr); never
        raises into the job."""
        if console_tailer is None:
            return
        step = console_tailer.poll()
        if step.reason is not None:
            self.queue.append_event(
                job.job_id,
                f"console.{step.reason}",
                step.message or "console live-tail reason",
                payload={"stream": CONSOLE_STREAM, "reason": step.reason},
            )
        stderr_step = console_tailer.poll_stderr()
        if stderr_step.reason is not None:
            self.queue.append_event(
                job.job_id,
                f"console_stderr.{stderr_step.reason}",
                stderr_step.message or "console_stderr live-tail reason",
                payload={"stream": CONSOLE_STDERR_STREAM, "reason": stderr_step.reason},
            )

    def _poll_running_job(
        self,
        lease: Lease,
        last_renewed_at: list[float],
        *,
        job: RelayJob,
        task_id: str,
        progress_sidecar: Path,
        progress_sidecar_offset: list[int],
        progress_sidecar_record_count: list[int],
        progress_sidecar_sequence: list[int],
        progress_sidecar_token: str,
        progress_sidecar_anchor: _RuntimeSidecarAnchor,
        progress_sidecar_failures: list[str],
        scheduler_job_ids: list[str],
        package_progress_adapter: PackageProgressProvider | None = None,
        package_progress_log_offsets: dict[Path, _PackageProgressLogState] | None = None,
        runtime_sidecar: Path | None = None,
        runtime_sidecar_offset: list[int] | None = None,
        runtime_sidecar_record_count: list[int] | None = None,
        runtime_sidecar_sequence: list[int] | None = None,
        runtime_sidecar_key: str | None = None,
        runtime_sidecar_anchor: _RuntimeSidecarAnchor | None = None,
        runtime_sidecar_failures: list[str] | None = None,
        runtime_metadata_state: list[JarvisRuntimeMetadata | None] | None = None,
        runtime_metadata_digests: set[str] | None = None,
        spool: JobSpool | None = None,
        ingest_progress_sidecar: bool = True,
        ingest_runtime_sidecar: bool = True,
    ) -> None:
        self._renew_lease_if_needed(lease, last_renewed_at)
        if spool is not None:
            self._check_runtime_storage(job, spool)
        if ingest_progress_sidecar:
            self._ingest_progress_sidecar(
                job,
                progress_sidecar,
                progress_sidecar_offset=progress_sidecar_offset,
                progress_sidecar_record_count=progress_sidecar_record_count,
                progress_sidecar_sequence=progress_sidecar_sequence,
                progress_sidecar_token=progress_sidecar_token,
                progress_sidecar_anchor=progress_sidecar_anchor,
                failures=progress_sidecar_failures,
            )
        if (
            ingest_runtime_sidecar
            and runtime_sidecar is not None
            and runtime_sidecar_offset is not None
            and runtime_sidecar_record_count is not None
            and runtime_sidecar_sequence is not None
            and runtime_sidecar_key is not None
            and runtime_sidecar_anchor is not None
            and runtime_sidecar_failures is not None
            and runtime_metadata_state is not None
            and runtime_metadata_digests is not None
        ):
            self._ingest_runtime_metadata_sidecar(
                job,
                task_id=task_id,
                path=runtime_sidecar,
                offset=runtime_sidecar_offset,
                record_count=runtime_sidecar_record_count,
                sequence=runtime_sidecar_sequence,
                expected_key=runtime_sidecar_key,
                expected_anchor=runtime_sidecar_anchor,
                failures=runtime_sidecar_failures,
                state=runtime_metadata_state,
                digests=runtime_metadata_digests,
                scheduler_job_ids=scheduler_job_ids,
                allow_final_record=False,
            )
        native_runtime_active = (
            runtime_metadata_state is not None
            and runtime_metadata_state[0] is not None
            and _runtime_metadata_is_native(runtime_metadata_state[0])
        )
        if (
            package_progress_adapter is not None
            and package_progress_log_offsets is not None
            and not native_runtime_active
        ):
            self._ingest_package_progress_logs(
                job,
                package_progress_adapter,
                package_progress_log_offsets,
            )
        if scheduler_job_ids:
            self._refresh_scheduler_status(job, scheduler_job_ids, task_id=task_id)

    def _check_runtime_storage(
        self,
        job: RelayJob,
        spool: JobSpool,
        *,
        force_job_scan: bool = False,
    ) -> None:
        """Stop an owned execution which crosses a storage safety boundary."""
        if self.storage_runtime is None:
            return
        decision = self.storage_runtime.check_running_job(
            job.job_id,
            spool_path=spool.path,
            force_job_scan=force_job_scan,
        )
        if decision.allowed:
            return
        self.queue.append_event(
            job.job_id,
            "storage.runtime_guard_failed",
            "Execution crossed a durable storage safety boundary",
            payload=decision.to_dict(),
        )
        raise StorageRuntimeViolation(decision)

    def _ingest_package_progress_logs(
        self,
        job: RelayJob,
        package_progress_adapter: PackageProgressProvider,
        log_offsets: dict[Path, _PackageProgressLogState],
        *,
        max_bytes_per_path: int = PACKAGE_PROGRESS_LOG_READ_BYTES,
    ) -> tuple[int, bool]:
        if max_bytes_per_path < 1:
            raise ConfigurationError("package progress log read limit must be positive")
        bytes_read = 0
        all_at_eof = True
        for state in log_offsets.values():
            handle = _open_package_progress_log(state.path)
            if handle is None:
                continue
            with handle:
                opened_stat = os.fstat(handle.fileno())
                identity = _progress_log_identity(opened_stat)
                reset_reason: str | None = None
                if state.identity is not None and identity != state.identity:
                    reset_reason = "replaced"
                elif opened_stat.st_size < state.offset:
                    reset_reason = "truncated"
                elif not _progress_log_checkpoint_matches(state, handle):
                    reset_reason = "rewritten"
                if reset_reason is not None:
                    package_progress_adapter.reset_stdout()
                    state.offset = 0
                    state.checkpoint_offset = 0
                    state.checkpoint_sha256 = None
                    self.queue.append_event(
                        job.job_id,
                        "progress.provider_log_reset",
                        f"Package progress log source was {reset_reason}",
                        payload={
                            "path": str(state.path),
                            "reason": reset_reason,
                            "provider_source_authority": (
                                PackageProgressSourceAuthority.PACKAGE_LOG.value
                            ),
                        },
                    )
                state.identity = identity
                handle.seek(state.offset)
                payload = handle.read(max_bytes_per_path)
                state.offset = handle.tell()
                final_stat = os.fstat(handle.fileno())
                at_eof = state.offset >= final_stat.st_size
                state.checkpoint_offset, state.checkpoint_sha256 = _progress_log_checkpoint(
                    handle,
                    state.offset,
                    path=state.path,
                )
            bytes_read += len(payload)
            all_at_eof = all_at_eof and at_eof
            text = payload.decode("utf-8", errors="replace")
            if text == "":
                continue
            self._append_package_progress_records(
                job,
                package_progress_adapter.observe_stdout(text),
                source_event_seq=None,
                package_progress_provider=package_progress_adapter,
                source_authority=PackageProgressSourceAuthority.PACKAGE_LOG,
            )
        return bytes_read, all_at_eof

    def _drain_package_progress_logs(
        self,
        job: RelayJob,
        package_progress_adapter: PackageProgressProvider,
        log_offsets: dict[Path, _PackageProgressLogState],
    ) -> None:
        """Drain a completed provider log in bounded chunks before parser EOF."""
        remaining = PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES
        while remaining > 0:
            read_limit = min(PACKAGE_PROGRESS_LOG_READ_BYTES, remaining)
            consumed, at_eof = self._ingest_package_progress_logs(
                job,
                package_progress_adapter,
                log_offsets,
                max_bytes_per_path=read_limit,
            )
            remaining -= consumed
            if at_eof:
                return
            if consumed == 0:
                raise ConfigurationError("package progress log made no bounded-read progress")
        raise ConfigurationError(
            "package progress log exceeded the final bounded-read budget "
            f"of {PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES} bytes"
        )

    def _baseline_package_progress_logs(
        self,
        job: RelayJob,
        paths: list[Path],
    ) -> dict[Path, _PackageProgressLogState]:
        """Checkpoint provider logs before launch so historical bytes are never emitted."""
        states: dict[Path, _PackageProgressLogState] = {}
        for path in paths:
            handle = _open_package_progress_log(path)
            if handle is None:
                state = _PackageProgressLogState(
                    path=path,
                    offset=0,
                    identity=None,
                    checkpoint_offset=0,
                    checkpoint_sha256=None,
                )
            else:
                with handle:
                    opened_stat = os.fstat(handle.fileno())
                    checkpoint_offset, checkpoint_sha256 = _progress_log_checkpoint(
                        handle,
                        opened_stat.st_size,
                        path=path,
                    )
                    state = _PackageProgressLogState(
                        path=path,
                        offset=opened_stat.st_size,
                        identity=_progress_log_identity(opened_stat),
                        checkpoint_offset=checkpoint_offset,
                        checkpoint_sha256=checkpoint_sha256,
                    )
            states[path] = state
            self.queue.append_event(
                job.job_id,
                "progress.provider_log_baselined",
                "Package progress log baselined before launch",
                payload={
                    "path": str(path),
                    "prelaunch_size": state.offset,
                    "prelaunch_identity": _render_progress_log_identity(state.identity),
                    "provider_source_authority": PackageProgressSourceAuthority.PACKAGE_LOG.value,
                },
            )
        return states
