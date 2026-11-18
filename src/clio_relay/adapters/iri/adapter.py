"""The ``iri`` adapter (RL13): the DOE Integrated Research Infrastructure Facility API.

The IRI API is the same at every facility that runs it (NERSC, ALCF), so one
adapter serves them all. Jobs go to ``/compute`` as PSI/J job specs, files to
``/filesystem``. Tokens are Globus Auth bearer tokens for the facility's IRI
scope, from relay's identity layer, or from a command the facility provides.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

import httpx
from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters._process import run_bounded
from clio_relay.adapters.base import AdapterContext, BaseAdapter, SignInNeed, SiteInfo, Verb
from clio_relay.adapters.globus import tokens
from clio_relay.adapters.iri.client import FILE_SIZE_LIMIT, IriClient
from clio_relay.adapters.iri.files import IriFileAccess
from clio_relay.jobs.handles import FileAccess, JobHandle, JobState, JobStatus, OutputRef
from clio_relay.jobs.records import write_json_atomic
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

STATES = {
    "new": JobState.QUEUED,
    "queued": JobState.QUEUED,
    "held": JobState.QUEUED,
    "active": JobState.RUNNING,
    "completed": JobState.COMPLETED,
    "failed": JobState.FAILED,
    "canceled": JobState.CANCELLED,
}


class IriConfig(BaseModel):
    """Settings for the ``iri`` adapter."""

    model_config = ConfigDict(extra="forbid")

    base_url: str = Field(description="The facility's IRI API, such as https://api.iri.nersc.gov")
    api_version: Literal["v1", "v2"] = Field(default="v2", description="API version to use")
    resource_id: str = Field(
        min_length=1, description="Compute resource id, from /compute/resources"
    )
    filesystem_resource_id: str | None = Field(
        default=None, description="Filesystem resource id; the compute resource id when empty"
    )
    identity: str = Field(default="globus", description="Relay identity that holds the token")
    resource_server: str | None = Field(
        default=None, description="Globus resource server of the facility's IRI scope"
    )
    scope: str | None = Field(default=None, description="The facility's IRI scope, for sign-in")
    token_command: list[str] | None = Field(
        default=None,
        min_length=1,
        description="A command that prints an access token, used instead of the identity",
    )
    root: str | None = Field(
        default=None, description="Directory on the facility that relative paths start from"
    )
    job_root: str | None = Field(
        default=".clio-relay/jobs",
        description="Directory for job directories, under root when relative; empty for none",
    )
    content_dir: str = Field(
        default=".clio-relay/content", description="Directory for staged content"
    )
    poll_s: float = Field(default=1.0, gt=0, description="First wait between file-task polls")
    stall_s: float = Field(
        default=600.0,
        gt=0,
        description="How long a file task may stay unchanged before relay gives up",
    )


def iri_job_spec(spec: JobSpec, *, job_dir: str | None) -> dict[str, Any]:
    """Render relay's job spec as the IRI ``JobSpec`` body."""
    if spec.executable is None:
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "the iri adapter runs an executable; a package job needs the package runner",
        )

    def in_job(path: str | None, default: str | None) -> str | None:
        if path is None:
            return default
        if job_dir is None or path.startswith(("/", "~")):
            return path
        return f"{job_dir}/{path}"

    resources = {
        key: value
        for key, value in spec.resources.model_dump().items()
        if value is not None and key != "exclusive_node_use"
    }
    if spec.resources.exclusive_node_use:
        resources["exclusive_node_use"] = True
    attributes: dict[str, Any] = {
        "duration": max(1, int(spec.attributes.duration.total_seconds())),
        "queue_name": spec.attributes.queue_name,
        "account": spec.attributes.account,
        "reservation_id": spec.attributes.reservation_id,
    }
    attributes = {key: value for key, value in attributes.items() if value is not None}
    if spec.attributes.custom_attributes:
        attributes["custom_attributes"] = {
            key: str(value) for key, value in spec.attributes.custom_attributes.items()
        }
    body: dict[str, Any] = {
        "executable": spec.executable,
        "arguments": list(spec.arguments),
        "directory": in_job(spec.directory, job_dir),
        "name": spec.name,
        "inherit_environment": spec.inherit_environment,
        "environment": dict(spec.environment),
        "stdin_path": in_job(spec.stdin_path, None),
        "stdout_path": in_job(spec.stdout_path, f"{job_dir}/stdout" if job_dir else None),
        "stderr_path": in_job(spec.stderr_path, f"{job_dir}/stderr" if job_dir else None),
        "launcher": spec.launcher,
        "resources": resources or None,
        "attributes": attributes,
    }
    return {key: value for key, value in body.items() if value is not None}


class IriAdapter(BaseAdapter[IriConfig]):
    """The IRI Facility API."""

    name: ClassVar[str] = "iri"
    Config: ClassVar[type[BaseModel]] = IriConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "files"})

    def __init__(self, config: IriConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        self.identity: tokens.TokenProvider = tokens.provider(config.identity)
        self.records = context.state_dir / "iri-jobs"
        self.client = IriClient(
            config.base_url,
            config.api_version,
            self._token,
            transport=self._transport(),
            poll_s=config.poll_s,
            stall_s=config.stall_s,
        )

    def _transport(self) -> httpx.AsyncBaseTransport | None:
        return None

    async def _token(self) -> str:
        command = self.config.token_command
        if command:
            result = await run_bounded(list(command), timeout_s=300, max_output_bytes=65536)
            token = result.stdout.strip().splitlines()[-1].strip() if result.stdout.strip() else ""
            if result.exit_code != 0 or not token:
                raise Refusal(
                    RefusalReason.NEEDS_SIGN_IN,
                    f"the token command {command[0]} gave no token: "
                    f"{(result.stderr or result.stdout).strip()[-300:]}",
                    provider=self.config.identity,
                )
            return token
        if self.config.resource_server is None:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "the iri adapter needs resource_server (the facility's IRI scope) or token_command",
            )
        return await tokens.bearer(
            self.identity,
            self.config.resource_server,
            what=f"the IRI API at {self.config.base_url}",
            provider=self.config.identity,
        )

    def _root_path(self, path: str) -> str | None:
        if path.startswith(("/", "~")):
            return path
        if self.config.root is None:
            return None
        return f"{self.config.root.rstrip('/')}/{path}"

    def _job_dir(self, operation_id: str) -> str | None:
        if not self.config.job_root:
            return None
        base = self._root_path(self.config.job_root)
        return f"{base.rstrip('/')}/{operation_id}" if base else None

    def _files(self) -> IriFileAccess:
        return IriFileAccess(
            self.client,
            resource_id=self.config.filesystem_resource_id or self.config.resource_id,
            root=self.config.root,
            content_dir=self.config.content_dir,
        )

    async def describe(self) -> SiteInfo:
        facts: dict[str, Any] = {
            "base_url": self.config.base_url,
            "api_version": self.config.api_version,
            "resource_id": self.config.resource_id,
            "file_size_limit": FILE_SIZE_LIMIT,
        }
        if self.config.token_command is None:
            facts["sign_in"] = await tokens.status_of(self.identity)
        if facts.get("sign_in", "signed_in") == "signed_in":
            try:
                found = await self.client.resources()
            except Refusal as refusal:
                facts["resources_error"] = refusal.message
            else:
                for item in found:
                    if item.get("id") == self.config.resource_id:
                        facts["resource_name"] = item.get("name")
                        status: Any = item.get("current_status")
                        facts["resource_status"] = status
                facts["resources"] = [str(item.get("id")) for item in found]
        return SiteInfo(adapter=self.name, facts=facts, verbs=sorted(self.implements))

    async def sign_in_needed(self) -> SignInNeed | None:
        if self.config.token_command is not None:
            return None
        if await tokens.status_of(self.identity) == "signed_in":
            return None
        return SignInNeed(
            provider=self.config.identity,
            message=f"Sign in so relay can use the IRI API at {self.config.base_url}"
            + (f" (scope {self.config.scope})" if self.config.scope else ""),
        )

    def _record_path(self, operation_id: str) -> Path:
        return self.records / f"{operation_id}.json"

    def _load(self, operation_id: str) -> dict[str, Any] | None:
        try:
            loaded: Any = json.loads(self._record_path(operation_id).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return cast(dict[str, Any], loaded) if isinstance(loaded, dict) else None

    def _idempotency_key(self, operation_id: str) -> str:
        site = self.context.site_id
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"clio-relay:{site}:{operation_id}"))

    async def _stage_inputs(self, payloads: list[tuple[str, bytes]], job_dir: str | None) -> None:
        if job_dir is None:
            if payloads:
                raise Refusal(
                    RefusalReason.INVALID_SPEC,
                    "inputs need a job directory; set root and job_root in the site's iri settings",
                )
            return
        files = self._files()
        await files.mkdir(job_dir)
        for name, data in payloads:
            await files.write(f"{job_dir}/{name}", data, overwrite=True)

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        job_dir = self._job_dir(operation_id)
        body = iri_job_spec(spec, job_dir=job_dir)
        payloads = [(item.name, self.context.content.get(item.sha256)) for item in spec.inputs]
        for name, data in payloads:
            if len(data) > FILE_SIZE_LIMIT:
                raise Refusal(
                    RefusalReason.INVALID_SPEC,
                    f"input {name} is {len(data)} bytes; the IRI API moves files up to "
                    f"{FILE_SIZE_LIMIT} bytes, so put it on the site with globus and pass its path",
                )
        record: dict[str, Any] = {
            "operation_id": operation_id,
            "resource_id": self.config.resource_id,
            "job_dir": job_dir,
            "job_id": None,
            "submitted_at": time.time(),
        }
        write_json_atomic(self._record_path(operation_id), record)
        await self._stage_inputs(payloads, job_dir)
        job = await self.client.submit(
            self.config.resource_id, body, idempotency_key=self._idempotency_key(operation_id)
        )
        job_id = job.get("id")
        if not job_id:
            raise Refusal(RefusalReason.SUBMIT_FAILED, "the IRI API returned no job id")
        record["job_id"] = str(job_id)
        write_json_atomic(self._record_path(operation_id), record)
        return self._handle(record)

    def _handle(self, record: dict[str, Any]) -> JobHandle:
        return JobHandle(
            adapter=self.name,
            native_id=str(record["job_id"]),
            data={
                "operation_id": record["operation_id"],
                "resource_id": record["resource_id"],
                "job_dir": record.get("job_dir"),
            },
        )

    async def find(self, operation_id: str) -> JobHandle | None:
        record = self._load(operation_id)
        if record is None or not record.get("job_id"):
            return None
        return self._handle(record)

    def _status(self, job: dict[str, Any], job_dir: str | None) -> JobStatus:
        raw: Any = job.get("status") or {}
        status = cast(dict[str, Any], raw) if isinstance(raw, dict) else {}
        native = str(status.get("state", "new"))
        state = STATES.get(native, JobState.QUEUED)
        exit_code = status.get("exit_code")
        outputs: list[OutputRef] = []
        if state.final and job_dir is not None:
            outputs = [
                OutputRef(name="stdout", path=f"{job_dir}/stdout"),
                OutputRef(name="stderr", path=f"{job_dir}/stderr"),
            ]
        return JobStatus(
            state=state,
            exit_code=exit_code if isinstance(exit_code, int) else None,
            native_state=native,
            message=str(status["message"]) if status.get("message") else None,
            outputs=outputs,
        )

    async def status(self, handle: JobHandle) -> JobStatus:
        operation_id = str(handle.data.get("operation_id", ""))
        record = self._load(operation_id) if operation_id else None
        if record is not None and isinstance(record.get("final"), dict):
            return JobStatus.model_validate(record["final"])
        if handle.native_id is None:
            raise Refusal(RefusalReason.OPERATION_NOT_FOUND, "the job has no IRI job id")
        resource_id = str(handle.data.get("resource_id") or self.config.resource_id)
        job_dir = handle.data.get("job_dir")
        job = await self.client.job(resource_id, handle.native_id)
        status = self._status(job, str(job_dir) if job_dir else None)
        if status.state.final and record is not None:
            record["final"] = status.model_dump(mode="json")
            write_json_atomic(self._record_path(operation_id), record)
        return status

    async def cancel(self, handle: JobHandle) -> JobStatus:
        current = await self.status(handle)
        if current.state.final or handle.native_id is None:
            return current
        resource_id = str(handle.data.get("resource_id") or self.config.resource_id)
        await self.client.cancel(resource_id, handle.native_id)
        return await self.status(handle)

    async def files(self) -> FileAccess:
        return self._files()
