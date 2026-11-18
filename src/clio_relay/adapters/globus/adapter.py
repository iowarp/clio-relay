"""The ``globus`` adapter (RL12): jobs through Globus Compute, files through Globus Transfer.

A Globus Compute endpoint needs only outbound connectivity, so relay places
nothing on the site. Where the facility runs a multi-user endpoint (ALCF Polaris,
NCSA DeltaAI), the site entry names it. Where the person runs their own (ALCF
Aurora), ``start`` names another adapter, such as ``worker`` or ``ssh``, that
runs the endpoint's start command on a login node when the endpoint is offline.

Each relay job is one Compute task running :func:`relay_task`. The job spec's
account, queue, walltime and node count map onto the endpoint's user
configuration through ``user_config_keys``, because each facility's template
names them differently. Credentials come from relay's ``globus`` identity.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext, BaseAdapter, SignInNeed, SiteInfo, Verb
from clio_relay.adapters.globus import tokens
from clio_relay.adapters.globus.compute import (
    ComputeService,
    EndpointInfo,
    SdkComputeService,
    TaskInfo,
)
from clio_relay.adapters.globus.transfer import (
    GlobusFileAccess,
    SdkTransferService,
    TransferService,
)
from clio_relay.jobs.handles import FileAccess, JobHandle, JobState, JobStatus, OutputRef
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

log = logging.getLogger(__name__)
UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
LAUNCHERS = ("mpiexec", "mpirun", "srun", "aprun")
_STATES = {
    "completed": JobState.COMPLETED,
    "failed": JobState.FAILED,
    "cancelled": JobState.CANCELLED,
}


class UserConfigKeys(BaseModel):
    """Which key in the endpoint's user configuration takes each job setting.

    Leave a key empty when the endpoint's template has no such setting.
    """

    model_config = ConfigDict(extra="forbid")

    account: str | None = Field(default="account", description="Key for the allocation")
    queue: str | None = Field(default="queue", description="Key for the queue or partition")
    walltime: str | None = Field(default="walltime", description="Key for the walltime")
    nodes: str | None = Field(default="nodes_per_block", description="Key for the node count")
    reservation: str | None = Field(default=None, description="Key for a reservation")


class StartConfig(BaseModel):
    """How relay starts the person's own endpoint when it is offline."""

    model_config = ConfigDict(extra="forbid")

    adapter: str = Field(description="Adapter that runs the command on a login node: worker or ssh")
    config: dict[str, Any] = Field(
        default_factory=dict[str, Any], description="That adapter's settings"
    )
    command: list[str] = Field(
        default_factory=lambda: ["globus-compute-endpoint", "start", "clio-relay"],
        min_length=1,
        description="Command that starts the endpoint",
    )
    timeout_s: float | None = Field(
        default=None,
        gt=0,
        description="Optional limit on the start command. By default it runs until it ends",
    )


class GlobusConfig(BaseModel):
    """Settings for the ``globus`` adapter."""

    model_config = ConfigDict(extra="forbid")

    endpoint: str = Field(pattern=UUID_PATTERN, description="Globus Compute endpoint id")
    scheduler: Literal["pbs", "slurm", "none"] | None = Field(
        default=None, description="The scheduler behind the endpoint"
    )
    queues: list[str] = Field(default_factory=list[str], description="Queues the site offers")
    user_config: dict[str, Any] = Field(
        default_factory=dict[str, Any],
        description="Fixed values for the endpoint's user configuration, such as worker_init",
    )
    user_config_keys: UserConfigKeys = Field(default_factory=UserConfigKeys)
    walltime_format: Literal["hh:mm:ss", "minutes", "seconds"] = "hh:mm:ss"
    job_root: str = Field(
        default="~/.clio-relay/jobs", description="Directory on the site for job directories"
    )
    cancel_poll_s: float = Field(
        default=5.0, gt=0, description="How often a running job checks for a cancel"
    )
    inline_input_limit: int = Field(
        default=6 * 1024 * 1024,
        ge=0,
        description="Most bytes of inputs sent inside the task; Compute caps a task's payload",
    )
    collection: str | None = Field(
        default=None, pattern=UUID_PATTERN, description="The site's Globus collection"
    )
    collection_root: str = Field(
        default="/~/", description="Directory on the collection that relative paths start from"
    )
    content_dir: str = Field(
        default=".clio-relay/content", description="Directory on the collection for staged content"
    )
    local_collection: str | None = Field(
        default=None, pattern=UUID_PATTERN, description="A collection on relay's own machine"
    )
    local_path: str | None = Field(
        default=None, description="Path of local_dir as that collection shows it"
    )
    local_dir: str | None = Field(
        default=None, description="Directory on relay's machine that the local collection shares"
    )
    start: StartConfig | None = Field(
        default=None, description="Start the person's own endpoint when it is offline"
    )


def format_walltime(duration: timedelta, style: str) -> str | int:
    """Render a walltime the way the endpoint's template expects."""
    seconds = max(1, int(duration.total_seconds()))
    if style == "seconds":
        return seconds
    if style == "minutes":
        return max(1, (seconds + 59) // 60)
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class GlobusAdapter(BaseAdapter[GlobusConfig]):
    """Globus Compute and Globus Transfer."""

    name: ClassVar[str] = "globus"
    Config: ClassVar[type[BaseModel]] = GlobusConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "files"})

    def __init__(self, config: GlobusConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        self.identity: tokens.TokenProvider = tokens.provider("globus")
        self.records = context.state_dir / "globus-jobs"

    async def _compute_service(self) -> ComputeService:
        token = await tokens.bearer(
            self.identity, tokens.COMPUTE_RESOURCE_SERVER, what="the Globus Compute endpoint"
        )
        return SdkComputeService(self.config.endpoint, token, self.context.state_dir)

    async def _transfer_service(self) -> TransferService:
        token = await tokens.bearer(
            self.identity, tokens.TRANSFER_RESOURCE_SERVER, what="Globus Transfer"
        )
        return SdkTransferService(token)

    async def describe(self) -> SiteInfo:
        facts: dict[str, Any] = {
            "endpoint": self.config.endpoint,
            "job_root": self.config.job_root,
            "files": "globus transfer" if self.config.collection else "no collection set",
            "cancel": "a running job is stopped through a marker its task polls",
        }
        signed = await tokens.status_of(self.identity)
        facts["sign_in"] = signed
        if signed == "signed_in":
            try:
                info = await (await self._compute_service()).endpoint()
            except Refusal as refusal:
                facts["endpoint_status"] = "unknown"
                facts["endpoint_error"] = refusal.message
            else:
                facts["endpoint_status"] = info.status
                facts.update(info.facts)
        return SiteInfo(
            adapter=self.name,
            scheduler=self.config.scheduler,
            queues=list(self.config.queues),
            facts=facts,
            verbs=sorted(self.implements),
        )

    async def sign_in_needed(self) -> SignInNeed | None:
        if await tokens.status_of(self.identity) == "signed_in":
            return None
        return SignInNeed(
            provider="globus",
            message="Sign in to Globus so relay can use the site's Compute endpoint and collection",
        )

    def _argv(self, spec: JobSpec) -> list[str]:
        if spec.executable is None:
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                "the globus adapter runs an executable; a package job needs the package runner",
            )
        argv = [spec.executable, *spec.arguments]
        launcher = spec.launcher
        if launcher in (None, "single"):
            return argv
        if launcher not in LAUNCHERS:
            raise Refusal(
                RefusalReason.INVALID_SPEC,
                f"the globus adapter knows the launchers {', '.join(LAUNCHERS)}, not {launcher}",
            )
        count = spec.resources.process_count
        return [launcher, *(["-n", str(count)] if count else []), *argv]

    def user_config(self, spec: JobSpec) -> dict[str, Any]:
        """Map the job's resources and attributes onto the endpoint's user configuration."""
        values: dict[str, Any] = dict(self.config.user_config)
        keys = self.config.user_config_keys
        attributes = spec.attributes
        if keys.account and attributes.account:
            values[keys.account] = attributes.account
        if keys.queue and attributes.queue_name:
            values[keys.queue] = attributes.queue_name
        if keys.walltime and attributes.duration is not None:
            values[keys.walltime] = format_walltime(
                attributes.duration, self.config.walltime_format
            )
        if keys.nodes:
            values[keys.nodes] = spec.resources.nodes()
        if keys.reservation and attributes.reservation_id:
            values[keys.reservation] = attributes.reservation_id
        for name, value in attributes.custom_attributes.items():
            if name.startswith("globus."):
                values[name.removeprefix("globus.")] = value
        return values

    def _inputs(self, spec: JobSpec) -> list[dict[str, str]]:
        items: list[dict[str, str]] = []
        total = 0
        for item in spec.inputs:
            data = self.context.content.get(item.sha256)
            total += len(data)
            if total > self.config.inline_input_limit:
                raise Refusal(
                    RefusalReason.INVALID_SPEC,
                    f"the job's inputs pass {self.config.inline_input_limit} bytes, the most "
                    "the globus adapter sends inside a task; put large inputs on the site "
                    "with files and pass their path",
                )
            items.append(
                {
                    "name": item.name,
                    "sha256": item.sha256,
                    "data": base64.b64encode(data).decode("ascii"),
                }
            )
        return items

    def _record_path(self, operation_id: str) -> Path:
        return self.records / f"{operation_id}.json"

    def _load(self, operation_id: str) -> dict[str, Any] | None:
        try:
            loaded: Any = json.loads(self._record_path(operation_id).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return cast(dict[str, Any], loaded) if isinstance(loaded, dict) else None

    def _job_dir(self, operation_id: str) -> str:
        return f"{self.config.job_root.rstrip('/')}/{operation_id}"

    async def _ensure_started(self, service: ComputeService) -> None:
        start = self.config.start
        if start is None:
            return
        info: EndpointInfo = await service.endpoint()
        if info.status == "online":
            return
        helper = registry.build(
            start.adapter,
            start.config,
            AdapterContext(
                site_id=self.context.site_id,
                state_dir=self.context.state_dir / "endpoint-start",
                content=self.context.content,
            ),
        )

        async def quiet(silent_s: float, next_check_s: float) -> None:
            log.info(
                "the endpoint start on %s has printed nothing for %.0f s; checking again in %.0f s",
                self.context.site_id,
                silent_s,
                next_check_s,
            )

        result = await helper.exec(
            list(start.command),
            timeout_s=start.timeout_s,
            max_output_bytes=65536,
            on_stall=quiet,
        )
        if result.timed_out:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"the endpoint is {info.status} and starting it through {start.adapter} "
                f"did not finish within the site's timeout_s ({start.timeout_s} s)",
            )
        if result.exit_code != 0:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"the endpoint is {info.status} and starting it through {start.adapter} "
                f"failed: {(result.stderr or result.stdout).strip()[-500:]}",
                exit_code=result.exit_code,
            )

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        argv = self._argv(spec)
        inputs = self._inputs(spec)
        user_config = self.user_config(spec)
        service = await self._compute_service()
        await self._ensure_started(service)
        job_dir = self._job_dir(operation_id)
        payload: dict[str, Any] = {
            "mode": "run",
            "operation_id": operation_id,
            "job_dir": job_dir,
            "argv": argv,
            "directory": spec.directory,
            "environment": dict(spec.environment),
            "inherit_environment": spec.inherit_environment,
            "stdin_path": spec.stdin_path,
            "stdout_path": spec.stdout_path,
            "stderr_path": spec.stderr_path,
            "inputs": inputs,
            "poll_s": self.config.cancel_poll_s,
        }
        record: dict[str, Any] = {
            "operation_id": operation_id,
            "endpoint": self.config.endpoint,
            "job_dir": job_dir,
            "user_config": user_config,
            "submitted_at": time.time(),
            "task_id": None,
        }
        write_json_atomic(self._record_path(operation_id), record)
        task_id = await service.submit(payload, user_config)
        record["task_id"] = task_id
        write_json_atomic(self._record_path(operation_id), record)
        return self._handle(record)

    def _handle(self, record: dict[str, Any]) -> JobHandle:
        return JobHandle(
            adapter=self.name,
            native_id=str(record["task_id"]),
            data={
                "operation_id": record["operation_id"],
                "endpoint": record["endpoint"],
                "job_dir": record["job_dir"],
            },
        )

    async def find(self, operation_id: str) -> JobHandle | None:
        record = self._load(operation_id)
        if record is None or not record.get("task_id"):
            return None
        return self._handle(record)

    def _from_task(self, info: TaskInfo, record: dict[str, Any] | None) -> JobStatus:
        cancelling = bool(record and record.get("cancel_requested_at"))
        if info.pending:
            state = JobState.RUNNING if info.status == "running" else JobState.QUEUED
            message = (
                "cancel requested; the job stops when its task next checks"
                if cancelling
                else f"Globus Compute reports {info.status}"
            )
            return JobStatus(state=state, native_state=info.status, message=message)
        if info.result is None:
            return JobStatus(
                state=JobState.FAILED,
                native_state=info.status,
                message=info.error or "the task ended with no result",
            )
        result = info.result
        raw_state = str(result.get("state", ""))
        state = _STATES.get(raw_state, JobState.FAILED)
        outputs = [
            OutputRef(name=str(item["name"]), path=str(item["path"]), size=item.get("size"))
            for item in cast(list[dict[str, Any]], result.get("outputs") or [])
        ]
        exit_code = result.get("exit_code")
        message = result.get("message")
        if state == JobState.FAILED and not message:
            message = str(result.get("stderr_tail") or "")[-500:] or None
        return JobStatus(
            state=state,
            exit_code=exit_code if isinstance(exit_code, int) else None,
            native_state=raw_state or info.status,
            message=str(message) if message else None,
            outputs=outputs,
        )

    async def status(self, handle: JobHandle) -> JobStatus:
        operation_id = str(handle.data.get("operation_id", ""))
        record = self._load(operation_id) if operation_id else None
        if record is not None and isinstance(record.get("final"), dict):
            return JobStatus.model_validate(record["final"])
        if handle.native_id is None:
            raise Refusal(RefusalReason.OPERATION_NOT_FOUND, "the job has no Compute task id")
        service = await self._compute_service()
        status = self._from_task(await service.task(handle.native_id), record)
        if status.state.final and record is not None:
            record["final"] = status.model_dump(mode="json")
            write_json_atomic(self._record_path(operation_id), record)
        return status

    async def cancel(self, handle: JobHandle) -> JobStatus:
        current = await self.status(handle)
        if current.state.final:
            return current
        operation_id = str(handle.data.get("operation_id", ""))
        record = self._load(operation_id) or {
            "operation_id": operation_id,
            "endpoint": handle.data.get("endpoint", self.config.endpoint),
            "job_dir": handle.data.get("job_dir", self._job_dir(operation_id)),
            "task_id": handle.native_id,
            "user_config": {},
        }
        service = await self._compute_service()
        cancel_task = await service.submit(
            {"mode": "cancel", "job_dir": record["job_dir"], "operation_id": operation_id},
            cast(dict[str, Any], record.get("user_config") or {}),
        )
        record["cancel_task_id"] = cancel_task
        record["cancel_requested_at"] = time.time()
        write_json_atomic(self._record_path(operation_id), record)
        return await self.status(handle)

    async def files(self) -> FileAccess:
        if self.config.collection is None:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "files through Globus needs the site's collection; set collection in the "
                "site's globus settings, or route files to another adapter",
            )
        return GlobusFileAccess(
            await self._transfer_service(),
            collection=self.config.collection,
            root=self.config.collection_root,
            content_dir=self.config.content_dir,
            local_collection=self.config.local_collection,
            local_path=self.config.local_path,
            local_dir=Path(self.config.local_dir).expanduser() if self.config.local_dir else None,
        )
