"""``files`` for the ``globus`` adapter, over Globus Transfer.

Listing and stat go to the site's collection directly. Reading and writing move
the bytes between the site's collection and a collection on relay's own machine
(Globus Connect Personal, for example), so both ends need a collection, as RL12
says. Transfer tasks are polled until they end. A task that keeps faulting is
cancelled and refused; there is no time limit on a task that is making progress.
"""

from __future__ import annotations

import asyncio
import hashlib
import posixpath
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from clio_relay.jobs.file_rules import check_relative, no_delete, whole_writes_only
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

INSTALL_HINT = "install the globus extra: pip install 'clio-relay[globus]'"
MAX_FAULTS = 10


class TransferService(Protocol):
    """The calls the adapter makes on Globus Transfer."""

    async def ls(self, collection: str, path: str) -> list[dict[str, Any]]:
        """List a directory. Refuse with ``path_not_found`` when it is missing."""
        ...

    async def copy(self, source: str, source_path: str, destination: str, dest_path: str) -> None:
        """Copy one file between collections, making missing directories, and wait for it."""
        ...


def _not_found(path: str) -> Refusal:
    return Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} does not exist", path=path)


def _http_status(exc: BaseException) -> int | None:
    value = getattr(exc, "http_status", None)
    return value if isinstance(value, int) else None


class SdkTransferService:
    """Globus Transfer through the Globus SDK."""

    def __init__(self, token: str, *, poll_s: float = 2.0, max_poll_s: float = 30.0) -> None:
        self.token = token
        self.poll_s = poll_s
        self.max_poll_s = max_poll_s
        self._client: Any = None

    def _tc(self) -> Any:
        if self._client is None:
            try:
                import globus_sdk
            except ImportError as exc:
                raise Refusal(
                    RefusalReason.ADAPTER_UNAVAILABLE,
                    f"the globus adapter needs the Globus SDK for files; {INSTALL_HINT}",
                ) from exc
            self._client = globus_sdk.TransferClient(
                authorizer=globus_sdk.AccessTokenAuthorizer(self.token)
            )
        return self._client

    def _fail(self, exc: Exception, path: str, doing: str) -> Refusal:
        status = _http_status(exc)
        if status == 404:
            return _not_found(path)
        if status in (401, 403) and "PermissionDenied" not in str(getattr(exc, "code", "")):
            return Refusal(
                RefusalReason.NEEDS_SIGN_IN,
                f"Globus Transfer refused the sign-in while {doing}; sign in to Globus again",
                provider="globus",
            )
        return Refusal(RefusalReason.UNREACHABLE, f"Globus Transfer failed {doing}: {exc}")

    def _ls(self, collection: str, path: str) -> list[dict[str, Any]]:
        try:
            response: Any = self._tc().operation_ls(collection, path=path)
        except Refusal:
            raise
        except Exception as exc:
            raise self._fail(exc, path, f"listing {path}") from exc
        data: Any = response.get("DATA", []) if hasattr(response, "get") else []
        return [cast(dict[str, Any], item) for item in cast(list[Any], data)]

    async def _wait(self, task_id: str, what: str) -> None:
        delay = self.poll_s
        while True:
            task: Any = await asyncio.to_thread(self._tc().get_task, task_id)
            status = str(task.get("status", ""))
            if status == "SUCCEEDED":
                return
            if status == "FAILED":
                reason = task.get("fatal_error") or task.get("nice_status") or "failed"
                raise Refusal(RefusalReason.UNREACHABLE, f"Globus Transfer {what}: {reason}")
            faults = int(task.get("faults") or 0)
            if faults >= MAX_FAULTS:
                await asyncio.to_thread(self._tc().cancel_task, task_id)
                raise Refusal(
                    RefusalReason.UNREACHABLE,
                    f"Globus Transfer {what} stalled after {faults} faults "
                    f"({task.get('nice_status')}); relay cancelled it",
                    task_id=task_id,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.max_poll_s)

    def _submit(self, source: str, source_path: str, destination: str, dest_path: str) -> str:
        try:
            import globus_sdk
        except ImportError as exc:  # pragma: no cover - _tc already imported it
            raise Refusal(RefusalReason.ADAPTER_UNAVAILABLE, INSTALL_HINT) from exc
        data = globus_sdk.TransferData(
            source_endpoint=source,
            destination_endpoint=destination,
            label="clio-relay files",
            verify_checksum=True,
        )
        data.add_item(source_path, dest_path)
        try:
            response: Any = self._tc().submit_transfer(data)
        except Exception as exc:
            raise self._fail(exc, source_path, f"copying {source_path}") from exc
        return str(response["task_id"])

    async def ls(self, collection: str, path: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._ls, collection, path)

    async def copy(self, source: str, source_path: str, destination: str, dest_path: str) -> None:
        task_id = await asyncio.to_thread(self._submit, source, source_path, destination, dest_path)
        await self._wait(task_id, f"copying {source_path} to {dest_path}")


def _modified(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace(" ", "T").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _entry(item: dict[str, Any], path: str) -> FileEntry:
    kind_raw = str(item.get("type", "file"))
    kind: Literal["file", "dir", "other"] = (
        "dir" if kind_raw == "dir" else "file" if kind_raw == "file" else "other"
    )
    size = item.get("size")
    return FileEntry(
        path=path,
        kind=kind,
        size=int(size) if kind == "file" and isinstance(size, int | float) else None,
        modified=_modified(item.get("last_modified")),
    )


class GlobusFileAccess:
    """File access on a site's Globus collection.

    Relative paths are under ``root`` on the site's collection. ``local_dir`` is
    the directory on relay's machine that ``local_collection`` shares at
    ``local_path``; reads and writes pass through it.
    """

    def __init__(
        self,
        transfer: TransferService,
        *,
        collection: str,
        root: str,
        content_dir: str,
        local_collection: str | None,
        local_path: str | None,
        local_dir: Path | None,
    ) -> None:
        self.transfer = transfer
        self.collection = collection
        self.root = root if root.endswith("/") else root + "/"
        self.content_dir = content_dir
        self.local_collection = local_collection
        self.local_path = local_path
        self.local_dir = local_dir

    def resolve(self, path: str) -> str:
        """Return the path on the site's collection."""
        check_relative(path)
        if path.startswith("/"):
            return posixpath.normpath(path)
        return posixpath.normpath(posixpath.join(self.root, path or "."))

    def _staging(self) -> tuple[str, str, Path]:
        if self.local_collection is None or self.local_path is None or self.local_dir is None:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "reading and writing files through Globus needs a collection on relay's "
                "machine; set local_collection, local_path and local_dir in the site's "
                "globus settings",
            )
        return self.local_collection, self.local_path, self.local_dir

    async def list(self, path: str) -> list[FileEntry]:
        target = self.resolve(path)
        try:
            items = await self.transfer.ls(self.collection, target)
        except Refusal as refusal:
            if refusal.reason != RefusalReason.PATH_NOT_FOUND:
                raise
            return [await self.stat(path)]
        return [_entry(item, posixpath.join(target, str(item.get("name", "")))) for item in items]

    async def stat(self, path: str) -> FileEntry:
        target = self.resolve(path)
        parent, name = posixpath.split(target.rstrip("/"))
        if not name:
            return FileEntry(path=target, kind="dir")
        try:
            items = await self.transfer.ls(self.collection, parent or "/")
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                raise _not_found(path) from refusal
            raise
        for item in items:
            if item.get("name") == name:
                return _entry(item, target)
        raise _not_found(path)

    async def _exists(self, path: str) -> FileEntry | None:
        try:
            return await self.stat(path)
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return None
            raise

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        entry = await self.stat(path)
        if entry.kind != "file":
            raise _not_found(path)
        local_collection, local_path, local_dir = self._staging()
        name = f"relay-read-{uuid.uuid4().hex}"
        await self.transfer.copy(
            self.collection, entry.path, local_collection, posixpath.join(local_path, name)
        )
        staged = local_dir / name

        def _read() -> bytes:
            try:
                with staged.open("rb") as handle:
                    handle.seek(max(0, offset))
                    return handle.read(-1 if limit is None else max(0, limit))
            finally:
                staged.unlink(missing_ok=True)

        return await asyncio.to_thread(_read)

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        whole_writes_only(path, offset, final)
        if not overwrite and await self._exists(path) is not None:
            raise Refusal(
                RefusalReason.PATH_EXISTS, f"{path} exists; pass overwrite to replace it", path=path
            )
        local_collection, local_path, local_dir = self._staging()
        name = f"relay-write-{uuid.uuid4().hex}"
        staged = local_dir / name
        await asyncio.to_thread(local_dir.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(staged.write_bytes, data)
        try:
            target = self.resolve(path)
            await self.transfer.copy(
                local_collection, posixpath.join(local_path, name), self.collection, target
            )
        finally:
            await asyncio.to_thread(staged.unlink, missing_ok=True)
        return await self.stat(path)

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        raise no_delete(path)

    def _cas(self, digest: str) -> str:
        return posixpath.join(self.content_dir, digest)

    async def stage(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        if not await self.has(digest):
            await self.write(self._cas(digest), data, overwrite=True)
        return digest

    async def has(self, digest: str) -> bool:
        return await self._exists(self._cas(digest)) is not None
