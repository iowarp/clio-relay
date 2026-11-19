"""A relay adapter for the Acme facility's REST API. Copy this package to start yours.

The adapter is one class, :class:`AcmeAdapter`, plus :class:`AcmeFiles` for
the ``files`` verb. Everything it uses comes from ``clio_relay.adapters.base``.
Rename ``acme``, keep the shape, and replace each ``self._call`` with your
facility's API. docs/adding-an-adapter.md walks through every method.
"""

from __future__ import annotations

import posixpath
from datetime import UTC, datetime
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, ConfigDict, Field

from clio_relay.adapters.base import (
    BaseAdapter,
    FileEntry,
    JobHandle,
    JobSpec,
    JobState,
    JobStatus,
    Kind,
    OutputRef,
    Refusal,
    RefusalReason,
    SignInNeed,
    SiteFiles,
    SiteInfo,
    Verb,
)

# The facility's job states, mapped onto relay's five.
STATES = {
    "PENDING": JobState.QUEUED,
    "RUNNING": JobState.RUNNING,
    "DONE": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
    "CANCELLED": JobState.CANCELLED,
}


class AcmeConfig(BaseModel):
    """A site's settings. Its JSON Schema is the Compute tab's form for this adapter."""

    model_config = ConfigDict(extra="forbid")

    url: str = Field(description="The facility's API, such as https://api.acme.example/v1")
    token_secret: str = Field(
        default="acme-token", description="The name relay keeps the API token under"
    )
    work_dir: str | None = Field(
        default=None, description="Relay's work directory on the site; <scratch>/relay if unset"
    )


class AcmeFiles(SiteFiles):
    """The six file primitives, over the facility's ``/fs`` endpoints."""

    def __init__(self, adapter: AcmeAdapter, *, root: str, home: str) -> None:
        super().__init__(root=root, home=home, staging=adapter.context.state_dir / "parts")
        self.api = adapter

    async def kind(self, path: str) -> Kind:
        response = await self.api.call("GET", "/fs/kind", params={"path": path})
        found: Kind = response.json()["kind"]
        return found

    async def describe(self, path: str) -> FileEntry:
        return _entry((await self.api.call("GET", "/fs/stat", params={"path": path})).json())

    async def children(self, path: str) -> list[FileEntry]:
        listed = (await self.api.call("GET", "/fs/list", params={"path": path})).json()
        return [_entry(item) for item in listed]

    async def get(self, path: str, offset: int, limit: int | None) -> bytes:
        params = {"path": path, "offset": offset, "limit": -1 if limit is None else limit}
        return (await self.api.call("GET", "/fs/read", params=params)).content

    async def put(self, path: str, data: bytes) -> None:
        await self.api.call("PUT", "/fs/write", params={"path": path}, content=data)

    async def remove(self, path: str) -> None:
        await self.api.call("DELETE", "/fs", params={"path": path})


def _entry(item: dict[str, Any]) -> FileEntry:
    return FileEntry(
        path=item["path"],
        kind=item["kind"],
        size=item["size"] if item["kind"] == "file" else None,
        modified=datetime.fromtimestamp(item["mtime"], UTC),
    )


class AcmeAdapter(BaseAdapter[AcmeConfig]):
    """Runs jobs and moves files on an Acme facility."""

    name: ClassVar[str] = "acme"
    Config: ClassVar[type[BaseModel]] = AcmeConfig
    implements: ClassVar[frozenset[Verb]] = frozenset({"describe", "run", "files"})

    # Talking to the facility

    def _token(self) -> str | None:
        return self.context.secret(self.config.token_secret)

    async def call(self, method: str, path: str, **request: Any) -> httpx.Response:
        """Call the facility's API, turning every failure into a typed refusal."""
        token = self._token()
        if not token:
            raise Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"relay has no Acme API token; add {self.config.token_secret} on relay's page",
                provider=f"keyring:{self.config.token_secret}",
            )
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(base_url=self.config.url, headers=headers) as client:
            try:
                response = await client.request(method, path, timeout=60, **request)
            except httpx.HTTPError as exc:
                raise Refusal(
                    RefusalReason.UNREACHABLE, f"Acme did not answer: {exc}", url=self.config.url
                ) from exc
        if response.status_code in (401, 403):
            raise Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                "Acme refused the API token; add a new one on relay's page",
                provider=f"keyring:{self.config.token_secret}",
            )
        if response.is_error:
            reason = RefusalReason.UNREACHABLE
            if response.is_client_error:
                reason = RefusalReason.INVALID_ARGUMENTS
            raise Refusal(reason, f"Acme said {response.status_code}: {response.text[:300]}")
        return response

    # Sign-in

    async def sign_in_needed(self) -> SignInNeed | None:
        if self._token():
            return None
        return SignInNeed(
            provider=f"keyring:{self.config.token_secret}",
            message="Paste your Acme API token. Relay keeps it in your keyring.",
        )

    # describe

    async def describe(self) -> SiteInfo:
        who = (await self.call("GET", "/whoami")).json()
        return SiteInfo(
            adapter=self.name,
            address=self.config.url,
            facts={"user": who["user"], "home": who["home"]},
            verbs=sorted(self.implements),
        )

    # files

    async def files(self) -> AcmeFiles:
        who = (await self.call("GET", "/whoami")).json()
        root = self.config.work_dir or posixpath.join(who["scratch"], "relay")
        return AcmeFiles(self, root=root, home=who["home"])

    # run, status, cancel, find

    def _job_name(self, operation_id: str) -> str:
        return f"relay-{operation_id}"

    async def run(self, spec: JobSpec, *, operation_id: str) -> JobHandle:
        if spec.executable is None:
            raise Refusal(
                RefusalReason.INVALID_SPEC, "Acme runs commands; a package job is not one"
            )
        files = await self.files()
        job_dir = files.resolve(spec.directory or f"jobs/{operation_id}")
        for item in spec.inputs:
            data = self.context.content.get(item.sha256)
            await files.put(posixpath.join(job_dir, item.name), data)
        body = {
            "name": self._job_name(operation_id),
            "command": [spec.executable, *spec.arguments],
            "workdir": job_dir,
            "env": spec.environment,
        }
        created = (await self.call("POST", "/jobs", json=body)).json()
        return JobHandle(adapter=self.name, native_id=created["id"], data={"dir": job_dir})

    async def _status(self, handle: JobHandle, job: dict[str, Any]) -> JobStatus:
        state = STATES[job["state"]]
        outputs: list[OutputRef] = []
        if state.final:
            files = await self.files()
            for entry in await files.children(handle.data["dir"]):
                if entry.kind == "file":
                    name = posixpath.basename(entry.path)
                    outputs.append(OutputRef(name=name, path=entry.path, size=entry.size))
        return JobStatus(
            state=state, exit_code=job["exit_code"], native_state=job["state"], outputs=outputs
        )

    async def status(self, handle: JobHandle) -> JobStatus:
        job = (await self.call("GET", f"/jobs/{handle.native_id}")).json()
        return await self._status(handle, job)

    async def cancel(self, handle: JobHandle) -> JobStatus:
        job = (await self.call("DELETE", f"/jobs/{handle.native_id}")).json()
        return await self._status(handle, job)

    async def find(self, operation_id: str) -> JobHandle | None:
        params = {"name": self._job_name(operation_id)}
        found = (await self.call("GET", "/jobs", params=params)).json()
        if not found:
            return None
        files = await self.files()
        job_dir = files.resolve(f"jobs/{operation_id}")
        return JobHandle(adapter=self.name, native_id=found[0]["id"], data={"dir": job_dir})
