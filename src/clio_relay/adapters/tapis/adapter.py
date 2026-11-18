"""The ``tapis`` adapter (RL46 to RL50): jobs and files through Tapis v3.

Relay submits every job to one Tapis app of its own, the launcher (RL47),
whose only argument is a record of the job spec. The app's archive and the
inputs live in relay's work directory on the system, and the job's
directory is ``<work dir>/jobs/<operation id>``. Inputs reach the job by
``tapislocal`` file inputs, so Tapis moves no bytes for them.

The person's TACC sign-in (RL48) comes from :mod:`clio_relay.identity.tacc`.
Every Tapis call is made with the person's own token.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, ClassVar, cast

from pydantic import BaseModel

from clio_relay.adapters.base import AdapterContext, BaseAdapter, SignInNeed, SiteInfo, Verb
from clio_relay.adapters.tapis import launcher, mapping
from clio_relay.adapters.tapis.client import TapisCallError, acall, make_client
from clio_relay.adapters.tapis.config import TapisConfig
from clio_relay.adapters.tapis.files import TapisFiles
from clio_relay.identity.tacc import PROVIDER, TaccIdentity, TaccSettings
from clio_relay.jobs.handles import FileAccess, JobHandle, JobState, JobStatus, OutputRef
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

TAPIS_ID_MAX = 80
_EXIT_IN_MESSAGE = re.compile(r"exit(?:ed)?(?: with)? code[ :=]*(-?\d+)", re.IGNORECASE)
_HIDDEN_OUTPUTS = ("tapisjob", "relay.")


class TapisAdapter(BaseAdapter[TapisConfig]):
    """A Tapis system, as the person who signed in."""

    name: ClassVar[str] = "tapis"
    Config: ClassVar[type[BaseModel]] = TapisConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "files"})

    def __init__(
        self,
        config: TapisConfig,
        context: AdapterContext,
        *,
        identity: TaccIdentity | None = None,
    ) -> None:
        super().__init__(config, context)
        self.identity = identity or TaccIdentity(
            settings=TaccSettings.resolve(
                None,
                base_url=config.base_url,
                tenant_id=config.tenant_id,
                client_id=config.client_id,
            )
        )
        self._client_cache: tuple[str, Any] | None = None
        self._system: dict[str, Any] | None = None
        self._apps: set[tuple[str, str]] = set()
        self.records_dir = context.state_dir / "jobs"

    # sign-in and the client

    async def sign_in_needed(self) -> SignInNeed | None:
        if await self.identity.token() is not None:
            return None
        if self.identity.pending() is not None:
            result = await self.identity.poll()
            if result.state == "done":
                return None
        step = await self.identity.begin()
        return SignInNeed(provider=PROVIDER, message=self.identity.message(step), url=step.url)

    async def sign_in_done(self, answer: dict[str, Any]) -> None:
        if self.identity.pending() is not None:
            await self.identity.poll(force=True)

    async def _needs_sign_in(self) -> Refusal:
        need = await self.sign_in_needed()
        if need is None:
            return Refusal(RefusalReason.UNREACHABLE, "the TACC sign-in changed; try again")
        return Refusal(
            RefusalReason.NEEDS_SIGN_IN,
            need.message,
            provider=PROVIDER,
            url=need.url,
            site=self.context.site_id,
        )

    async def client(self) -> Any:
        """Return a Tapis client holding the person's current access token."""
        token = await self.identity.access_token()
        if token is None:
            raise await self._needs_sign_in()
        cached = self._client_cache
        if cached is not None and cached[0] == hashlib.sha256(token.encode()).hexdigest():
            return cached[1]
        made = make_client(self.config.base_url, self.config.tenant_id, token)
        self._client_cache = (hashlib.sha256(token.encode()).hexdigest(), made)
        return made

    async def tapis_call(self, service: str, operation: str, **kwargs: Any) -> Any:
        client = await self.client()
        try:
            return await acall(client, service, operation, **kwargs)
        except TapisCallError as exc:
            if exc.unauthorized:
                self._client_cache = None
                raise await self._needs_sign_in() from None
            raise

    async def username(self) -> str:
        status = await self.identity.status()
        if not status.account:
            raise await self._needs_sign_in()
        return status.account

    # the system

    async def system_id(self) -> str:
        if self.config.system_id:
            return self.config.system_id
        return f"clio-relay.{await self.username()}.{self.context.site_id}"

    def forget_system(self) -> None:
        """Drop the cached system record, after registering or removing it."""
        self._system = None

    async def system(self) -> dict[str, Any]:
        """Return the Tapis system record, fetched once per adapter."""
        if self._system is None:
            system_id = await self.system_id()
            try:
                found = await self.tapis_call("systems", "getSystem", systemId=system_id)
            except TapisCallError as exc:
                if exc.not_found:
                    raise Refusal(
                        RefusalReason.INVALID_CONFIG,
                        f"Tapis has no system {system_id} for this person. Register the "
                        "host first (python -m clio_relay.adapters.tapis register-host)",
                        site=self.context.site_id,
                        system=system_id,
                    ) from None
                raise exc.refusal(system=system_id) from None
            self._system = cast(dict[str, Any], found)
        return self._system

    def work_dir(self, system: dict[str, Any]) -> str:
        if self.config.work_dir:
            return self.config.work_dir.rstrip("/") or "/"
        found = str(system.get("jobWorkingDir") or "")
        if found.startswith("/") and "$" not in found and "${" not in found:
            return found.rstrip("/") or "/"
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            "the system's job working directory is not a plain path; set work_dir",
            site=self.context.site_id,
            job_working_dir=found or None,
        )

    @staticmethod
    def host_path(system: dict[str, Any], path: str) -> str:
        """Return the host's path for a Tapis path on ``system``."""
        root = str(system.get("rootDir") or "/").rstrip("/")
        return f"{root}/{path.lstrip('/')}"

    async def _files(self) -> TapisFiles:
        system = await self.system()
        return TapisFiles(self.client, str(system["id"]), self.work_dir(system))

    async def files(self) -> FileAccess:
        return await self._files()

    # describe

    async def describe(self) -> SiteInfo:
        facts: dict[str, Any] = {
            "tenant": self.config.tenant_id,
            "base_url": self.config.base_url,
            "authn": self.config.authn,
            "launcher": self.config.launcher,
        }
        status = await self.identity.status()
        facts["signed_in"] = status.state == "signed_in"
        facts["account"] = status.account
        info = SiteInfo(adapter=self.name, verbs=sorted(self.implements))
        if status.state != "signed_in":
            facts["system_id"] = self.config.system_id
            info.facts = facts
            return info
        system = await self.system()
        system_id = str(system["id"])
        facts["system_id"] = system_id
        info.address = str(system.get("host") or "") or None
        scheduler = str(system.get("batchScheduler") or "").lower()
        info.scheduler = scheduler if scheduler and scheduler != "none" else "none"
        queues = cast(list[dict[str, Any]], system.get("batchLogicalQueues") or [])
        info.queues = [str(item.get("name")) for item in queues if item.get("name")]
        info.limits = {
            str(item.get("name")): {
                key: item[key]
                for key in ("maxNodeCount", "maxMinutes", "maxCoresPerNode", "maxMemoryMB")
                if item.get(key) is not None
            }
            for item in queues
            if item.get("name")
        }
        facts["job_type"] = mapping.job_type(system)
        try:
            await self.tapis_call(
                "systems",
                "checkUserCredential",
                systemId=system_id,
                userName=await self.username(),
            )
            facts["credential"] = "ok"
        except TapisCallError as exc:
            facts["credential"] = f"refused: {exc.message}"
        app_id, app_version = await self.app_ref()
        facts["app"] = {"id": app_id, "version": app_version}
        info.facts = facts
        return info

    # the launcher app

    async def app_ref(self) -> tuple[str, str]:
        username = await self.username()
        system_id = await self.system_id()
        if system_id.startswith(f"clio-relay.{username}."):
            app_id = system_id
        else:
            app_id = f"clio-relay.{username}.{system_id}"
        if len(app_id) > TAPIS_ID_MAX:
            digest = hashlib.sha256(system_id.encode()).hexdigest()[:12]
            app_id = f"clio-relay.{username}.{digest}"[:TAPIS_ID_MAX]
        return app_id, launcher.version()

    async def ensure_app(self) -> tuple[str, str]:
        """Register relay's launcher app on the system once, with its archive in place."""
        ref = await self.app_ref()
        if ref in self._apps:
            return ref
        app_id, app_version = ref
        try:
            await self.tapis_call("apps", "getApp", appId=app_id, appVersion=app_version)
        except TapisCallError as exc:
            if not exc.not_found:
                raise exc.refusal(app=app_id) from None
            await self._create_app(app_id, app_version)
        self._apps.add(ref)
        return ref

    async def _create_app(self, app_id: str, app_version: str) -> None:
        system = await self.system()
        files = await self._files()
        path = f"{files.work_dir}/launcher/{launcher.archive_name()}"
        if not await files.exists(path):
            await files.write(path, launcher.archive(), overwrite=True)
        body = mapping.app_definition(
            app_id=app_id,
            app_version=app_version,
            system_id=str(system["id"]),
            container_image=path,
        )
        try:
            await self.tapis_call("apps", "createAppVersion", **body)
        except TapisCallError as exc:
            if exc.status != 409:
                raise exc.refusal(RefusalReason.SUBMIT_FAILED, app=app_id) from None

    # run

    async def _stage_inputs(self, files: TapisFiles, digests: list[str]) -> None:
        for digest in digests:
            if await files.has(digest):
                continue
            if not self.context.content.has(digest):
                raise Refusal(
                    RefusalReason.INPUT_MISSING,
                    f"content {digest} is neither on the site nor in relay's store",
                    digest=digest,
                )
            await files.stage(self.context.content.get(digest))

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        guarded = self.config.launcher == "guarded"
        if guarded and spec.package is None:
            raise launcher.guarded_refusal()
        system = await self.system()
        kind = mapping.job_type(system)
        if kind is None:
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "relay runs Tapis jobs on Slurm systems and on systems with no scheduler",
                scheduler=system.get("batchScheduler"),
                verb="run",
            )
        files = await self._files()
        digests = [item.sha256 for item in spec.inputs]
        if spec.package is not None:
            digests.append(spec.package.sha256)
        await self._stage_inputs(files, digests)
        app_id, app_version = await self.ensure_app()
        job_dir = f"{files.work_dir}/jobs/{operation_id}"
        content_host = self.host_path(system, f"{files.work_dir}/content")
        fields = launcher.record(
            spec,
            content_dir=content_host,
            package_root=self.host_path(system, f"{files.work_dir}/packages"),
            guarded=guarded,
        )
        request = mapping.job_request(
            spec,
            operation_id=operation_id,
            app_id=app_id,
            app_version=app_version,
            system_id=str(system["id"]),
            system_type=kind,
            job_dir=job_dir,
            content_dir_host=content_host,
            launcher_arg=launcher.encode(fields),
        )
        try:
            submitted = cast(dict[str, Any], await self.tapis_call("jobs", "submitJob", **request))
        except TapisCallError as exc:
            raise exc.refusal(RefusalReason.SUBMIT_FAILED, site=self.context.site_id) from None
        handle = self._handle(str(submitted["uuid"]), str(system["id"]), job_dir)
        self._remember(operation_id, handle)
        return handle

    def _handle(self, uuid: str, system_id: str, job_dir: str) -> JobHandle:
        return JobHandle(
            adapter=self.name,
            native_id=uuid,
            data={"system_id": system_id, "job_dir": job_dir},
        )

    def _remember(self, operation_id: str, handle: JobHandle) -> None:
        self.records_dir.mkdir(parents=True, exist_ok=True)
        path = self.records_dir / f"{operation_id}.json"
        partial = path.with_suffix(".part")
        partial.write_text(handle.model_dump_json(), encoding="utf-8")
        partial.replace(path)

    # observe

    async def status(self, handle: JobHandle) -> JobStatus:
        uuid = self._uuid(handle)
        try:
            job = cast(dict[str, Any], await self.tapis_call("jobs", "getJobStatus", jobUuid=uuid))
            if mapping.task_state(str(job.get("status") or "")).final:
                job = cast(dict[str, Any], await self.tapis_call("jobs", "getJob", jobUuid=uuid))
        except TapisCallError as exc:
            if exc.not_found:
                raise Refusal(
                    RefusalReason.OPERATION_NOT_FOUND, f"Tapis has no job {uuid}", job=uuid
                ) from None
            raise exc.refusal(job=uuid) from None
        return await self._observe(handle, job)

    async def _observe(self, handle: JobHandle, job: dict[str, Any]) -> JobStatus:
        native = str(job.get("status") or "PENDING")
        state = mapping.task_state(native)
        message = str(job.get("lastMessage") or "") or None
        status = JobStatus(state=state, native_state=native, message=message)
        if not state.final:
            status.message = f"Tapis job {native}"
        if state in (JobState.COMPLETED, JobState.FAILED):
            status.exit_code = await self._exit_code(handle, message)
            if state == JobState.COMPLETED and status.exit_code is None:
                status.exit_code = 0
        if state == JobState.COMPLETED:
            status.outputs = await self._outputs(handle)
        return status

    async def _exit_code(self, handle: JobHandle, message: str | None) -> int | None:
        job_dir = str(handle.data.get("job_dir") or "")
        if job_dir:
            files = await self._files()
            try:
                text = await files.read(f"{job_dir}/relay.exitcode")
                return int(text.decode("utf-8", "replace").strip())
            except (Refusal, ValueError):
                pass
        found = _EXIT_IN_MESSAGE.search(message or "")
        return int(found.group(1)) if found else None

    async def _outputs(self, handle: JobHandle) -> list[OutputRef]:
        job_dir = str(handle.data.get("job_dir") or "")
        if not job_dir:
            return []
        files = await self._files()
        try:
            entries = await files.list(job_dir)
        except Refusal:
            return []
        outputs: list[OutputRef] = []
        for entry in entries:
            name = entry.path.rsplit("/", 1)[-1]
            if entry.kind != "file" or name.startswith(_HIDDEN_OUTPUTS):
                continue
            outputs.append(OutputRef(name=name, path=f"{job_dir}/{name}", size=entry.size))
        return outputs

    async def cancel(self, handle: JobHandle) -> JobStatus:
        uuid = self._uuid(handle)
        try:
            await self.tapis_call("jobs", "cancelJob", jobUuid=uuid)
        except TapisCallError as exc:
            # Tapis refuses to cancel a job that already ended; observe it instead.
            if exc.status is None or exc.status >= 500:
                raise exc.refusal(job=uuid) from None
        return await self.status(handle)

    async def find(self, operation_id: str) -> JobHandle | None:
        path = self.records_dir / f"{operation_id}.json"
        if path.is_file():
            return JobHandle.model_validate_json(path.read_text(encoding="utf-8"))
        try:
            found = await self.tapis_call(
                "jobs",
                "getJobSearchList",
                _tapis_query_parameters={
                    "description.eq": mapping.description(operation_id),
                    "select": "uuid,status,created,description,execSystemId,execSystemExecDir",
                },
            )
        except TapisCallError as exc:
            raise exc.refusal(operation=operation_id) from None
        jobs = [job for job in cast(list[dict[str, Any]], found or []) if job.get("uuid")]
        if not jobs:
            return None
        latest = max(jobs, key=lambda job: str(job.get("created") or ""))
        handle = self._handle(
            str(latest["uuid"]),
            str(latest.get("execSystemId") or await self.system_id()),
            str(latest.get("execSystemExecDir") or ""),
        )
        self._remember(operation_id, handle)
        return handle

    @staticmethod
    def _uuid(handle: JobHandle) -> str:
        if not handle.native_id:
            raise Refusal(RefusalReason.OPERATION_NOT_FOUND, "the job handle has no Tapis id")
        return handle.native_id
