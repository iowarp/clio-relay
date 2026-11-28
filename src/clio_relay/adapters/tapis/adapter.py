"""The ``tapis`` adapter (RL46 to RL50): jobs and files through Tapis v3.

Relay submits every job to one Tapis app of its own, the launcher (RL47),
whose only argument names a record of the job spec. The app's archive and the
inputs live in relay's work directory on the system, and the job's
directory is ``<work dir>/jobs/<operation id>``. Inputs reach the job by
``tapislocal`` file inputs, so Tapis moves no bytes for them.

``exec`` is a launcher job too: a FORK job on the login host when the system
allows one, else a batch job (:func:`mapping.exec_job_type`). Relay's JARVIS
root is under the work directory, so setup, the package listing and
``packages(op="add")`` run through ``exec`` as on every adapter. A package job
is JARVIS's, as over ssh (:mod:`clio_relay.jarvis_remote`): the pipeline and
the site script go into the job directory through the files API, ``exec`` runs
``jarvis ppl submit`` there (to the system's scheduler, or JARVIS's ``local``
one with none), and status, cancel, stop and move are ``jarvis execution``
commands run the same way.

The person's TACC sign-in (RL48) comes from :mod:`clio_relay.identity.tacc`.
Every Tapis call is made with the person's own token.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import posixpath
import re
import time
import uuid
from contextlib import suppress
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pydantic import BaseModel

from clio_relay import jarvis_remote, jarvis_submit
from clio_relay.adapters.base import (
    AdapterContext,
    BaseAdapter,
    Reach,
    SignInNeed,
    SiteInfo,
    Verb,
    api_place,
    not_implemented,
)
from clio_relay.adapters.tapis import launcher, mapping
from clio_relay.adapters.tapis.client import TapisCallError, acall, make_client
from clio_relay.adapters.tapis.config import TapisConfig
from clio_relay.adapters.tapis.files import TapisFiles
from clio_relay.identity.tacc import PROVIDER, TaccIdentity, TaccSettings
from clio_relay.jobs.handles import (
    STALL_S,
    ExecResult,
    FileAccess,
    JobHandle,
    JobState,
    JobStatus,
    OutputRef,
    StallReport,
)
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

if TYPE_CHECKING:
    from clio_relay.adapters.tapis.register import SshHost
    from clio_relay.sites.models import SiteEntry

TAPIS_ID_MAX = 80
# What on_site_added keeps so a system missing after the sign-in can be registered.
WAY_IN_FILE = "way-in.json"
WAY_IN_KEYS = ("ssh_profile", "ssh_config", "ssh_options")
_EXIT_IN_MESSAGE = re.compile(r"exit(?:ed)?(?: with)? code[ :=]*(-?\d+)", re.IGNORECASE)
_HIDDEN_OUTPUTS = ("tapisjob", "relay.")
RECORD_FILE = "relay.record"
EXEC_POLL_S = 3.0
BOUNDED = 'n=$1; shift; "$@" > >(head -c "$n") 2> >(head -c "$n" >&2); rc=$?; wait; exit $rc'


class TapisAdapter(BaseAdapter[TapisConfig]):
    """A Tapis system, as the person who signed in."""

    name: ClassVar[str] = "tapis"
    Config: ClassVar[type[BaseModel]] = TapisConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "exec", "files"})
    not_offered: ClassVar[dict[str, str]] = {
        "serve": (
            "a Tapis job's port is reachable only on the system's network, and Tapis "
            "offers no tunnel to a job's node; add an ssh alternative to this site for serve"
        )
    }
    runs_packages: ClassVar[bool] = True

    @classmethod
    def reaches(cls, config: BaseModel) -> Reach | None:
        assert isinstance(config, TapisConfig)
        # Before relay registers a system there is no system to compare.
        if not config.system_id:
            return None
        return Reach(api_place(config.base_url, config.tenant_id, config.system_id))

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
        self._registered_here = False
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
        """Return the Tapis system record, fetched once per adapter.

        A relay-named system that is missing is registered here, from the way in
        saved when the site was added: the site is usually added before the
        person signs in, so its own set-up could not register it then.
        """
        if self._system is None:
            system_id = await self.system_id()
            try:
                found = await self.tapis_call("systems", "getSystem", systemId=system_id)
            except TapisCallError as exc:
                if exc.not_found and await self._register_on_first_use():
                    return await self.system()
                if exc.not_found:
                    raise Refusal(
                        RefusalReason.INVALID_CONFIG,
                        f"Tapis has no system {system_id} for this person. Add or switch to "
                        "the site again to register the host, or run "
                        "python -m clio_relay.adapters.tapis register-host",
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
            job_working_dir=found or None,
        )

    def root_dir(self, system: dict[str, Any]) -> str:
        """The system's ``rootDir`` on the host; Tapis paths are relative to it."""
        found = str(system.get("rootDir") or "/")
        if found.startswith("/") and "HOST_EVAL" not in found and "$" not in found:
            return found.rstrip("/") or "/"
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"the Tapis system's rootDir {found} is not a plain host path, so relay cannot "
            "map its Tapis paths to the host's; use a system whose rootDir is one",
            root_dir=found,
        )

    async def _files(self) -> TapisFiles:
        system = await self.system()
        return TapisFiles(
            self.client,
            str(system["id"]),
            self.work_dir(system),
            staging=self.context.state_dir / "parts",
            root_dir=self.root_dir(system),
        )

    async def files(self) -> FileAccess:
        return await self._files()

    async def jarvis_home(self) -> str | None:
        return (await self._files()).root

    # setting the site up

    async def on_site_added(self, site: SiteEntry) -> dict[str, Any] | None:
        """Register the person's host with Tapis, unless its credential already works.

        Safe to run again: a credential that passes Tapis's check is left alone.
        The site's way in is saved first, so a site added before the sign-in is
        registered on its first use after it.
        """
        from clio_relay.adapters.tapis.register import ssh_for_site

        self._save_way_in(site)
        return await self._set_up(ssh_for_site(site), site.address)

    async def _set_up(self, ssh: SshHost | None, address: str | None) -> dict[str, Any]:
        from clio_relay.adapters.tapis.register import register_host

        username = await self.username()
        system_id = await self.system_id()
        try:
            await self.tapis_call(
                "systems", "checkUserCredential", systemId=system_id, userName=username
            )
        except TapisCallError:
            pass
        else:
            return {"system_id": system_id, "registered": False, "credential": "ok"}
        registration = await register_host(self, ssh, address=address)
        return {"registered": True, "credential": "ok", **asdict(registration)}

    @property
    def _way_in_file(self) -> Path:
        return self.context.state_dir / WAY_IN_FILE

    def _save_way_in(self, site: SiteEntry) -> None:
        """Keep what registering needs: the ssh profile names and the address. No secrets."""
        ssh = site.config if site.adapter == "ssh" else site.alternatives.get("ssh")
        record = {
            "address": site.address,
            "ssh": {key: ssh[key] for key in WAY_IN_KEYS if ssh.get(key)} if ssh else None,
        }
        self._way_in_file.parent.mkdir(parents=True, exist_ok=True)
        self._way_in_file.write_text(json.dumps(record), encoding="utf-8")

    async def _register_on_first_use(self) -> bool:
        """Register a missing relay-named system once; True when it was tried."""
        if self.config.system_id or self._registered_here or not self._way_in_file.is_file():
            return False
        from clio_relay.adapters.tapis.register import SshHost

        self._registered_here = True
        record = cast(dict[str, Any], json.loads(self._way_in_file.read_text(encoding="utf-8")))
        ssh = cast(dict[str, Any] | None, record.get("ssh"))
        address = cast(str | None, record.get("address"))
        await self._set_up(SshHost.from_alternative(ssh) if ssh else None, address)
        self.forget_system()
        return True

    # describe

    async def describe(self) -> SiteInfo:
        facts: dict[str, Any] = {
            "tenant": self.config.tenant_id,
            "base_url": self.config.base_url,
            "authn": self.config.authn,
            "not_offered": dict(self.not_offered),
            "needs": "bash 4.4 or later on the system's host, for exec's bounded output",
        }
        status = await self.identity.status()
        facts["signed_in"] = status.state == "signed_in"
        facts["account"] = status.account
        facts["credentials"] = {"stored_in": status.stored_in, "path": status.store_path}
        info = SiteInfo(adapter=self.name, verbs=sorted(self.implements))
        if status.state != "signed_in":
            facts["system_id"] = self.config.system_id
            info.facts = facts
            return info
        system = await self.system()
        system_id = str(system["id"])
        facts["system_id"] = system_id
        info.address = str(system.get("host") or "") or None
        info.scheduler = str(system.get("batchScheduler") or "") or None
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
        facts["exec_job_type"] = mapping.exec_job_type(system)
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
        path = f"{files.root}/launcher/{launcher.archive_name()}"
        if await files.kind(path) != "file":
            await files.write(path, launcher.archive(), overwrite=True)
        body = mapping.app_definition(
            app_id=app_id,
            app_version=app_version,
            system_id=str(system["id"]),
            container_image=files.tapis_path(path),
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
        if spec.steps():
            handle = cast(JobHandle, await self._jarvis(spec, operation_id, "submit"))
            self._remember(operation_id, handle)
            return handle
        system = await self.system()
        kind = mapping.job_type(system)
        if kind is None:
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "relay runs Tapis jobs on Slurm systems and on systems with no scheduler",
                scheduler=system.get("batchScheduler"),
                verb="run",
            )
        handle = await self._submit(spec, operation_id, system, kind, await self._files())
        self._remember(operation_id, handle)
        return handle

    async def install_check(self, spec: JobSpec, *, operation_id: str) -> dict[str, Any]:
        return cast(dict[str, Any], await self._jarvis(spec, operation_id, "check"))

    async def plan_job(self, spec: JobSpec, *, operation_id: str) -> dict[str, Any] | None:
        if not spec.steps():
            return None
        return cast(dict[str, Any] | None, await self._jarvis(spec, operation_id, "plan"))

    async def _jarvis_job(
        self, spec: JobSpec, operation_id: str
    ) -> tuple[dict[str, Any], dict[str, object], dict[str, Any]]:
        """The system, the job's pipeline and its handle data, for JARVIS's job directory."""
        system, files = await self.system(), await self._files()
        name = str(system.get("batchScheduler") or "").lower()
        scheduler = name if system.get("canRunBatch") and name not in ("", "none") else "local"
        root = files.root
        job_dir = f"{root}/jobs/{operation_id}"
        document = jarvis_remote.document(
            spec, operation_id, job_dir, scheduler=scheduler, job_name=f"relay-{operation_id}"
        )
        data = {"root": root, "job_dir": job_dir, "job_name": f"relay-{operation_id}"}
        return system, document, {**data, "system_id": str(system["id"])}

    async def _jarvis(
        self, spec: JobSpec, operation_id: str, mode: jarvis_submit.SiteMode
    ) -> JobHandle | dict[str, Any] | None:
        """Run the site script's ``mode`` through :meth:`exec`, a check in ``checks/<op>``."""
        _system, document, data = await self._jarvis_job(spec, operation_id)
        files = await self._files()
        await self._stage_inputs(files, [item.sha256 for item in spec.inputs])
        run_dir = f"{files.root}/checks/{operation_id}" if mode == "check" else data["job_dir"]
        for name, text in (
            (jarvis_submit.PIPELINE_FILE, jarvis_submit.render_pipeline(document)),
            (jarvis_submit.SCRIPT_FILE, jarvis_submit.SITE_SCRIPT),
        ):
            await files.write(f"{run_dir}/{name}", text.encode(), overwrite=True)
        command = jarvis_submit.submit_command(
            spec, operation_id=operation_id, jarvis="", home=data["root"], mode=mode, graph=""
        )
        links = [(item.name, files.content_path(item.sha256)) for item in spec.inputs]
        script = jarvis_remote.submit_script(run_dir, command, links, mode=mode)
        return jarvis_remote.submitted(
            self.name,
            jarvis_remote.parse_sections(await self._sh(script, f"the {mode}")),
            mode=mode,
            spec=spec,
            site=self.context.title,
            install_walltime_s=None,
            data=data,
        )

    async def install(
        self, spec: JobSpec, *, operation_id: str, answers: dict[str, Any]
    ) -> JobHandle:
        """Submit ``ppl install +submit`` from the job directory, the site script written first."""
        _, document, data = await self._jarvis_job(spec, operation_id)
        files = await self._files()
        await files.write(
            f"{data['job_dir']}/{jarvis_submit.SCRIPT_FILE}",
            jarvis_submit.SITE_SCRIPT.encode(),
            overwrite=True,
        )
        command = jarvis_submit.install_command(
            spec,
            operation_id=operation_id,
            jarvis="",
            answers=answers,
            pipeline=jarvis_submit.install_pipeline(document, None, answers),
            home=data["root"],
            graph="",
        )
        text = await self._sh(jarvis_remote.install_script(data["job_dir"], command), "the install")
        return jarvis_remote.installed(
            self.name,
            text,
            spec=spec,
            site=self.context.title,
            install_walltime_s=None,
            login_mode="detached",
            data=data,
        )

    async def _sh(self, script: str, what: str) -> str:
        """Run ``script`` with ``sh`` on the system through :meth:`exec`; its stdout."""
        result = await self.exec(["sh", "-c", script], max_output_bytes=1 << 20)
        if result.exit_code != 0:
            error = result.stderr.strip() or result.stdout.strip()
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"{what} did not finish on {self.context.title}: {error[-500:] or 'no output'}",
            )
        return result.stdout

    async def _submit(
        self,
        spec: JobSpec,
        operation_id: str,
        system: dict[str, Any],
        kind: str,
        files: TapisFiles,
    ) -> JobHandle:
        """Submit ``spec`` to the launcher app as a ``kind`` job, its record in the job's dir."""
        await self._stage_inputs(files, [item.sha256 for item in spec.inputs])
        app_id, app_version = await self.ensure_app()
        job_dir = f"{files.root}/jobs/{operation_id}"
        content_host = posixpath.dirname(files.content_path("x"))
        fields = launcher.record(spec, content_dir=content_host)
        record = f"{job_dir}/{RECORD_FILE}"
        await files.write(record, launcher.encode(fields).encode("ascii"), overwrite=True)
        request = mapping.job_request(
            spec,
            operation_id=operation_id,
            app_id=app_id,
            app_version=app_version,
            system_id=str(system["id"]),
            system_type=kind,
            job_dir=files.tapis_path(job_dir),
            content_dir_host=content_host,
            launcher_arg="@" + record,
            max_minutes=mapping.max_minutes(spec, system, kind),
            exec_prefix=bool(system.get("enableCmdPrefix")),
        )
        try:
            submitted = cast(dict[str, Any], await self.tapis_call("jobs", "submitJob", **request))
        except TapisCallError as exc:
            raise exc.refusal(RefusalReason.SUBMIT_FAILED) from None
        return self._handle(str(submitted["uuid"]), str(system["id"]), job_dir)

    # exec

    async def exec(
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        """Run ``command`` in relay's work directory as one launcher job, and wait for it."""
        if not command:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "give a command")
        system = await self.system()
        kind = mapping.exec_job_type(system)
        if kind is None:
            raise Refusal(
                RefusalReason.VERB_NOT_IMPLEMENTED,
                "this Tapis system runs neither FORK nor Slurm jobs, so relay cannot exec there",
                verb="exec",
            )
        files = await self._files()
        # The launcher writes output to files, so the site keeps only what relay reads.
        keep = str(max_output_bytes + 1)
        spec = JobSpec.model_validate(
            {
                "executable": "bash",
                "arguments": ["-c", BOUNDED, "relay-exec", keep, *command],
                "directory": files.root,
                "attributes": {
                    "duration": None if timeout_s is None else max(60, int(timeout_s) + 60)
                },
            }
        )
        operation_id = f"exec-{uuid.uuid4().hex[:16]}"
        started = time.monotonic()
        handle = await self._submit(spec, operation_id, system, kind, files)
        timed_out = False
        try:
            while not (status := await self.status(handle)).state.final:
                waited = time.monotonic() - started
                if timeout_s is not None and waited > timeout_s:
                    timed_out = True
                    status = await self.cancel(handle)
                    break
                if on_stall is not None and waited >= STALL_S:
                    await on_stall(waited, EXEC_POLL_S)
                await asyncio.sleep(EXEC_POLL_S)
        except asyncio.CancelledError:
            with suppress(Exception):
                await self.cancel(handle)
            raise
        out = await self._read_bounded(files, f"{handle.data['job_dir']}/stdout", max_output_bytes)
        err = await self._read_bounded(files, f"{handle.data['job_dir']}/stderr", max_output_bytes)
        with suppress(Refusal, TapisCallError):
            await files.remove_tree(str(handle.data["job_dir"]))
        return ExecResult(
            argv=list(command),
            exit_code=None if timed_out else status.exit_code,
            stdout=out[:max_output_bytes].decode("utf-8", "replace"),
            stderr=err[:max_output_bytes].decode("utf-8", "replace"),
            stdout_truncated=len(out) > max_output_bytes,
            stderr_truncated=len(err) > max_output_bytes,
            timed_out=timed_out,
            duration_s=time.monotonic() - started,
        )

    @staticmethod
    async def _read_bounded(files: TapisFiles, path: str, limit: int) -> bytes:
        try:
            return await files.read(path, limit=limit + 1)
        except Refusal:
            return b""

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
        if handle.data.get("mode") == "jarvis":
            found, _ = await jarvis_remote.status(self._sh, handle, site=self.context.title)
            return found
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
        if handle.data.get("mode") == "jarvis":
            return await jarvis_remote.control(self._sh, handle, "cancel", self.status)
        uuid = self._uuid(handle)
        try:
            await self.tapis_call("jobs", "cancelJob", jobUuid=uuid)
        except TapisCallError as exc:
            # Tapis refuses to cancel a job that already ended; observe it instead.
            if exc.status is None or exc.status >= 500:
                raise exc.refusal(job=uuid) from None
        return await self.status(handle)

    async def stop(self, handle: JobHandle) -> JobStatus:
        """``jarvis execution stop`` for a JARVIS job; any other job is cancelled."""
        if handle.data.get("mode") == "jarvis":
            return await jarvis_remote.control(self._sh, handle, "stop", self.status)
        return await self.cancel(handle)

    async def move_queue(self, handle: JobHandle, queue_name: str) -> None:
        if handle.data.get("mode") != "jarvis":
            raise not_implemented(self.name, "move_queue")
        await jarvis_remote.move(self._sh, handle, queue_name, site=self.context.title)

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
        tapis_dir = str(latest.get("execSystemExecDir") or "")
        handle = self._handle(
            str(latest["uuid"]),
            str(latest.get("execSystemId") or await self.system_id()),
            (await self._files()).host_path(tapis_dir) if tapis_dir else "",
        )
        self._remember(operation_id, handle)
        return handle

    @staticmethod
    def _uuid(handle: JobHandle) -> str:
        if not handle.native_id:
            raise Refusal(RefusalReason.OPERATION_NOT_FOUND, "the job handle has no Tapis id")
        return handle.native_id
