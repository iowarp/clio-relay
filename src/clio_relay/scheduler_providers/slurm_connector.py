"""SLURM connector-step lifecycle: placement, launch, poll, cancel, reconcile.

``_SlurmConnectorMixin`` is mixed into ``SlurmSchedulerProvider``
(``slurm_provider.py``) rather than defined there directly, so the ~285
lines of connector-step machinery stay a separate owner concern from core
poll/cancel/lifecycle-validation. ``connector_placement`` calls
``self._scontrol_one(...)``, which only the concrete provider defines --
the ``TYPE_CHECKING``-only import below lets that resolve under strict
pyright without creating a real runtime import cycle (``slurm_provider.py``
imports this module unconditionally; this module only imports
``slurm_provider`` under ``TYPE_CHECKING``, which never executes).

``launch_connector_step``/``_cleanup_failed_connector_registration`` read
``CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS``,
``CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS``, and
``_register_connector_launcher_for_reaping`` off the package facade
(``clio_relay.scheduler_providers``) at call time rather than importing
them by value -- tests monkeypatch exactly those three names on the facade
(``monkeypatch.setattr("clio_relay.scheduler_providers.CONNECTOR_STEP_...",
...)``), and a plain ``from . import NAME`` would capture an unpatchable
copy at import time. The other constants/helpers used here are never
patched, so they stay ordinary static imports.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
import time
from collections.abc import Sequence
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    SchedulerConnectorStepStatus,
)

from .command import _run_scheduler_command, _scheduler_command_error
from .constants import (
    CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS,
    CONNECTOR_STEP_REGISTRATION_POLL_SECONDS,
)
from .slurm_connector_launcher import (
    _read_connector_launcher_diagnostic,
    _terminate_connector_launcher,
)
from .slurm_status import _split_row

if TYPE_CHECKING:
    from .slurm_provider import SlurmSchedulerProvider

_SLURM_ALLOCATION_JOB_ID = re.compile(r"^[0-9]+(?:_[0-9]+)?(?:\+[0-9]+)?$")
_CONNECTOR_STEP_MARKER = re.compile(r"^clio-relay-connector-[a-f0-9]{32}$")


class _SlurmConnectorMixin:
    """Connector-step lifecycle mixed into ``SlurmSchedulerProvider``."""

    name: str

    def connector_placement(
        self: SlurmSchedulerProvider, scheduler_job_id: str
    ) -> SchedulerConnectorPlacement:
        """Resolve and prove one BatchHost for a single-node SLURM allocation."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        record = self._scontrol_one(scheduler_job_id)
        if record is None:
            raise RelayError(f"SLURM job was not found for connector placement: {scheduler_job_id}")
        raw_node_count = record.get("NumNodes")
        try:
            node_count = int(raw_node_count or "")
        except ValueError as exc:
            raise RelayError("SLURM connector placement has an invalid NumNodes value") from exc
        if node_count != 1:
            raise RelayError(
                "SLURM connector placement requires an unambiguous single-node allocation"
            )
        batch_host = record.get("BatchHost")
        node_list = record.get("NodeList")
        if (
            batch_host is None
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,1023}", batch_host) is None
            or node_list is None
            or not node_list
            or node_list in {"(null)", "None", "N/A"}
        ):
            raise RelayError("SLURM connector placement omitted a valid BatchHost or NodeList")
        hosts_result = _run_scheduler_command(["scontrol", "show", "hostnames", node_list])
        if hosts_result.returncode != 0:
            raise _scheduler_command_error("scontrol show hostnames", hosts_result)
        hosts = [line.strip() for line in hosts_result.stdout.splitlines() if line.strip()]
        if hosts != [batch_host]:
            raise RelayError("SLURM BatchHost did not exactly match the single allocation host")
        return SchedulerConnectorPlacement(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            placement_host=batch_host,
            allocation_node_count=1,
            source="slurm-scontrol-batch-host",
            verified=True,
        )

    def launch_connector_step(
        self,
        scheduler_job_id: str,
        *,
        placement_host: str,
        step_marker: str,
        command: Sequence[str],
        output_path: str,
    ) -> SchedulerConnectorStepIdentity:
        """Launch one detached connector and resolve its exact active SLURM step."""
        from clio_relay import scheduler_providers as _scheduler_providers

        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_placement_host(placement_host)
        _validate_connector_step_marker(step_marker)
        connector_command = _validate_connector_command(command)
        connector_output = _validate_connector_output_path(output_path)
        launch_command = [
            "srun",
            f"--jobid={scheduler_job_id}",
            "--overlap",
            "--exact",
            "--nodes=1",
            "--ntasks=1",
            f"--nodelist={placement_host}",
            f"--job-name={step_marker}",
            "--input=none",
            f"--output={connector_output}",
            f"--error={connector_output}",
            "--open-mode=append",
            "--",
            *connector_command,
        ]
        with tempfile.TemporaryFile(prefix="clio-relay-srun-", mode="w+b") as private_output:
            try:
                launcher = subprocess.Popen(  # noqa: S603 - validated argv, no shell
                    launch_command,
                    stdin=subprocess.DEVNULL,
                    stdout=private_output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )
            except OSError as exc:
                raise RelayError(
                    f"could not start detached SLURM connector launcher: {exc}"
                ) from exc
            try:
                deadline = (
                    time.monotonic()
                    + _scheduler_providers.CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS
                )
                while True:
                    identity = self.find_connector_step(
                        scheduler_job_id,
                        step_marker=step_marker,
                        placement_host=placement_host,
                    )
                    if identity is not None:
                        _scheduler_providers._register_connector_launcher_for_reaping(launcher)
                        return identity.model_copy(update={"source": "slurm-srun-detached-marker"})
                    returncode = launcher.poll()
                    if returncode is not None:
                        diagnostic = _read_connector_launcher_diagnostic(private_output)
                        suffix = f": {diagnostic}" if diagnostic else ""
                        raise RelayError(
                            "detached SLURM connector launcher exited before its exact step "
                            f"was registered (returncode={returncode}){suffix}"
                        )
                    if time.monotonic() >= deadline:
                        raise RelayError(
                            "detached SLURM connector step was not registered within the "
                            "bounded provider timeout"
                        )
                    time.sleep(CONNECTOR_STEP_REGISTRATION_POLL_SECONDS)
            except BaseException as launch_error:
                _terminate_connector_launcher(launcher)
                try:
                    self._cleanup_failed_connector_registration(
                        scheduler_job_id,
                        step_marker=step_marker,
                        placement_host=placement_host,
                    )
                except (ConfigurationError, RelayError) as cleanup_error:
                    raise RelayError(
                        "failed SLURM connector registration could not prove exact-step cleanup: "
                        f"{cleanup_error}"
                    ) from launch_error
                raise

    def _cleanup_failed_connector_registration(
        self,
        scheduler_job_id: str,
        *,
        step_marker: str,
        placement_host: str,
    ) -> None:
        """Reconcile and cancel only a late step after launcher registration failed."""
        from clio_relay import scheduler_providers as _scheduler_providers

        deadline = time.monotonic() + CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS
        absent_observations = 0
        while True:
            identity = self.find_connector_step(
                scheduler_job_id,
                step_marker=step_marker,
                placement_host=placement_host,
            )
            if identity is None:
                absent_observations += 1
                if absent_observations >= (
                    _scheduler_providers.CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS
                ):
                    return
            else:
                canceled = self.cancel_connector_step(
                    scheduler_job_id,
                    scheduler_step_id=identity.scheduler_step_id,
                )
                cancel_error = (
                    _scheduler_command_error("scancel connector step", canceled)
                    if canceled.returncode != 0
                    else None
                )
                while True:
                    status = self.poll_connector_step(
                        scheduler_job_id,
                        scheduler_step_id=identity.scheduler_step_id,
                        placement_host=placement_host,
                    )
                    if status.state == "absent":
                        return
                    if time.monotonic() >= deadline:
                        detail = f": {cancel_error}" if cancel_error is not None else ""
                        raise RelayError(
                            "late SLURM connector step remained active after exact-step "
                            f"cancellation{detail}"
                        )
                    time.sleep(CONNECTOR_STEP_REGISTRATION_POLL_SECONDS)
            if time.monotonic() >= deadline:
                raise RelayError(
                    "failed SLURM connector registration did not reach a stable absent state"
                )
            time.sleep(CONNECTOR_STEP_REGISTRATION_POLL_SECONDS)

    def poll_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
        placement_host: str,
    ) -> SchedulerConnectorStepStatus:
        """Observe exact active-step presence through ``squeue --steps``."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_step_id(scheduler_job_id, scheduler_step_id)
        _validate_connector_placement_host(placement_host)
        result = _run_scheduler_command(
            [
                "squeue",
                "--noheader",
                f"--steps={scheduler_step_id}",
                "--format=%i|%N",
            ]
        )
        if result.returncode != 0:
            raise _scheduler_command_error("squeue --steps", result)
        rows = [
            row
            for line in result.stdout.splitlines()
            if line.strip() and (row := _split_row(line, 2)) is not None
        ]
        if any(row[0] != scheduler_step_id for row in rows) or len(rows) > 1:
            raise RelayError("SLURM returned ambiguous connector step identity")
        if not rows:
            return SchedulerConnectorStepStatus(
                scheduler=self.name,
                scheduler_job_id=scheduler_job_id,
                scheduler_step_id=scheduler_step_id,
                placement_host=placement_host,
                record_found=False,
                state="absent",
                observed_host=None,
                verified=True,
            )
        observed_host = rows[0][1]
        if observed_host != placement_host:
            raise RelayError(
                "SLURM connector step did not run on its provider-verified placement host"
            )
        return SchedulerConnectorStepStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            scheduler_step_id=scheduler_step_id,
            placement_host=placement_host,
            record_found=True,
            state="active",
            observed_host=observed_host,
            verified=True,
        )

    def cancel_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
    ) -> subprocess.CompletedProcess[str]:
        """Cancel only ``job.step`` so the parent allocation remains untouched."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_step_id(scheduler_job_id, scheduler_step_id)
        return _run_scheduler_command(["scancel", scheduler_step_id])

    def find_connector_step(
        self,
        scheduler_job_id: str,
        *,
        step_marker: str,
        placement_host: str,
    ) -> SchedulerConnectorStepIdentity | None:
        """Find at most one active connector step after an interrupted launch."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_step_marker(step_marker)
        _validate_connector_placement_host(placement_host)
        result = _run_scheduler_command(
            [
                "squeue",
                "--noheader",
                "--steps",
                f"--jobs={scheduler_job_id}",
                f"--name={step_marker}",
                "--format=%i|%j|%N",
            ]
        )
        if result.returncode != 0:
            raise _scheduler_command_error("squeue --steps marker lookup", result)
        matches: list[str] = []
        for line in result.stdout.splitlines():
            row = _split_row(line, 3)
            if row is None or row[1] != step_marker:
                continue
            _validate_connector_step_id(scheduler_job_id, row[0])
            if row[2] != placement_host:
                raise RelayError(
                    "SLURM connector marker resolved outside its verified placement host"
                )
            if row[0] not in matches:
                matches.append(row[0])
        if len(matches) > 1:
            raise RelayError("multiple active SLURM steps matched one connector marker")
        if not matches:
            return None
        return SchedulerConnectorStepIdentity(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            scheduler_step_id=matches[0],
            step_marker=step_marker,
            placement_host=placement_host,
            source="slurm-squeue-step-marker",
            verified=True,
        )


def _validate_slurm_allocation_job_id(value: str) -> None:
    if _SLURM_ALLOCATION_JOB_ID.fullmatch(value) is None:
        raise ConfigurationError(f"invalid SLURM allocation job id: {value!r}")


def _validate_connector_step_id(scheduler_job_id: str, scheduler_step_id: str) -> None:
    prefix = f"{scheduler_job_id}."
    if not scheduler_step_id.startswith(prefix) or not scheduler_step_id[len(prefix) :].isdecimal():
        raise ConfigurationError(
            "SLURM connector step id must be an exact numeric step of its allocation"
        )


def _validate_connector_placement_host(value: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,1023}", value) is None:
        raise ConfigurationError("SLURM connector placement host is invalid")


def _validate_connector_step_marker(value: str) -> None:
    if _CONNECTOR_STEP_MARKER.fullmatch(value) is None:
        raise ConfigurationError("SLURM connector step marker is invalid")


def _validate_connector_output_path(value: str) -> str:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ConfigurationError("connector output path contains forbidden characters")
    parsed = PurePosixPath(value)
    if not parsed.is_absolute() or ".." in parsed.parts or str(parsed) != value:
        raise ConfigurationError("connector output path must be normalized and absolute")
    if len(value.encode("utf-8")) > 4_096:
        raise ConfigurationError("connector output path exceeds the provider limit")
    return value


def _validate_connector_command(command: Sequence[str]) -> list[str]:
    rendered = list(command)
    if not rendered or len(rendered) > 128:
        raise ConfigurationError("connector command must contain between 1 and 128 arguments")
    encoded_size = 0
    for argument in rendered:
        if argument == "":
            raise ConfigurationError("connector command arguments must be non-empty strings")
        if "\x00" in argument or "\n" in argument or "\r" in argument:
            raise ConfigurationError("connector command contains forbidden characters")
        encoded_size += len(argument.encode("utf-8"))
    if encoded_size > 32 * 1024:
        raise ConfigurationError("connector command exceeds the provider limit")
    return rendered
