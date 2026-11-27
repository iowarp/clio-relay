"""``files`` for the ``globus`` adapter, over Globus Transfer.

Listing and stat go to the site's collection directly. Reading and writing move
the bytes between the site's collection and a collection on relay's own machine
(Globus Connect Personal, for example), so both ends need a collection, as RL12
says. Transfer tasks are polled until they end. A task that keeps faulting is
cancelled and refused; there is no time limit on a task that is making progress.
The rules of docs/files.md come from :class:`~clio_relay.adapters.site_files.SiteFiles`.
"""

from __future__ import annotations

import asyncio
import posixpath
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from clio_relay.adapters.site_files import Kind, SiteFiles
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

INSTALL_HINT = "install the globus extra: pip install 'clio-relay[globus]'"
MAX_FAULTS = 10
HOME = "/~"
"""How a Globus collection names the user's home directory."""
PARTS_DIR = ".relay-parts"
"""Where chunked uploads collect, inside the local collection's directory."""


class TransferService(Protocol):
    """The calls the adapter makes on Globus Transfer."""

    async def ls(self, collection: str, path: str) -> list[dict[str, Any]]:
        """List a directory. Refuse with ``path_not_found`` when it is missing."""
        ...

    async def copy(self, source: str, source_path: str, destination: str, dest_path: str) -> None:
        """Copy one file between collections, making missing directories, and wait for it."""
        ...

    async def delete(self, collection: str, path: str, *, recursive: bool) -> None:
        """Delete a file, a symlink or a directory, and wait for it."""
        ...

    async def rename(self, collection: str, old: str, new: str) -> None:
        """Move ``old`` to ``new`` on one collection."""
        ...

    async def mkdir(self, collection: str, path: str) -> None:
        """Make one directory. A directory that exists already is fine."""
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

    def _submit_delete(self, collection: str, path: str, recursive: bool) -> str:
        try:
            import globus_sdk
        except ImportError as exc:  # pragma: no cover - _tc already imported it
            raise Refusal(RefusalReason.ADAPTER_UNAVAILABLE, INSTALL_HINT) from exc
        data = globus_sdk.DeleteData(collection, label="clio-relay files", recursive=recursive)
        data.add_item(path)
        try:
            response: Any = self._tc().submit_delete(data)
        except Exception as exc:
            raise self._fail(exc, path, f"deleting {path}") from exc
        return str(response["task_id"])

    def _operation(self, name: str, path: str, doing: str, *args: str) -> None:
        try:
            getattr(self._tc(), name)(*args)
        except Refusal:
            raise
        except Exception as exc:
            if name == "operation_mkdir" and "Exists" in str(getattr(exc, "code", "")):
                return
            raise self._fail(exc, path, doing) from exc

    async def ls(self, collection: str, path: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._ls, collection, path)

    async def copy(self, source: str, source_path: str, destination: str, dest_path: str) -> None:
        task_id = await asyncio.to_thread(self._submit, source, source_path, destination, dest_path)
        await self._wait(task_id, f"copying {source_path} to {dest_path}")

    async def delete(self, collection: str, path: str, *, recursive: bool) -> None:
        task_id = await asyncio.to_thread(self._submit_delete, collection, path, recursive)
        await self._wait(task_id, f"deleting {path}")

    async def rename(self, collection: str, old: str, new: str) -> None:
        await asyncio.to_thread(
            self._operation, "operation_rename", old, f"moving {old}", collection, old, new
        )

    async def mkdir(self, collection: str, path: str) -> None:
        await asyncio.to_thread(
            self._operation, "operation_mkdir", path, f"making {path}", collection, path
        )


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


class GlobusFiles(SiteFiles):
    """``files`` on a site's Globus collection, from the six primitives of :class:`SiteFiles`.

    Listing and stat go to the site's collection. Bytes move between it and
    ``local_collection``, a collection on relay's machine that shares
    ``local_dir`` at ``local_path``. A chunked upload collects in
    ``local_dir/.relay-parts``, inside that collection, so its final chunk
    moves the whole file with one transfer.
    """

    def __init__(
        self,
        transfer: TransferService,
        *,
        collection: str,
        root: str,
        local_collection: str | None,
        local_path: str | None,
        local_dir: Path | None,
        state_dir: Path,
    ) -> None:
        staging = local_dir / PARTS_DIR if local_dir is not None else state_dir / "parts"
        super().__init__(root=root, home=HOME, staging=staging)
        self.transfer = transfer
        self.collection = collection
        self.local_collection = local_collection
        self.local_path = local_path
        self.local_dir = local_dir

    def _local(self) -> tuple[str, str, Path]:
        if self.local_collection is None or self.local_path is None or self.local_dir is None:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "reading and writing files through Globus needs a collection on relay's "
                "machine; set local_collection, local_path and local_dir in the site's "
                "globus settings",
            )
        return self.local_collection, self.local_path, self.local_dir

    async def _item(self, path: str) -> dict[str, Any] | None:
        """The listing entry for ``path`` from its parent, or None when it is missing."""
        parent, name = posixpath.split(path)
        if not name or name == "~" or path in (self.root, self.home):
            # The collection's top and the home directory may have no listable parent.
            try:
                await self.transfer.ls(self.collection, path)
            except Refusal as refusal:
                if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                    return None
                raise
            return {"name": name, "type": "dir"}
        try:
            items = await self.transfer.ls(self.collection, parent or "/")
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return None
            raise
        return next((item for item in items if item.get("name") == name), None)

    async def kind(self, path: str) -> Kind:
        item = await self._item(path)
        if item is None:
            return "missing"
        if item.get("link_target") or item.get("type") == "invalid_symlink":
            return "link"
        return "dir" if item.get("type") == "dir" else "file"

    async def describe(self, path: str) -> FileEntry:
        item = await self._item(path)
        if item is None:
            raise _not_found(path)
        return _entry(item, path)

    async def children(self, path: str) -> list[FileEntry]:
        items = await self.transfer.ls(self.collection, path)
        named = sorted(items, key=lambda item: str(item.get("name", "")))
        return [_entry(item, posixpath.join(path, str(item.get("name", "")))) for item in named]

    async def get(self, path: str, offset: int, limit: int | None) -> bytes:
        local_collection, local_path, local_dir = self._local()
        name = f"relay-read-{uuid.uuid4().hex}"
        await self.transfer.copy(
            self.collection, path, local_collection, posixpath.join(local_path, name)
        )
        staged = local_dir / name

        def _read() -> bytes:
            try:
                with staged.open("rb") as handle:
                    handle.seek(offset)
                    return handle.read(-1 if limit is None else limit)
            finally:
                staged.unlink(missing_ok=True)

        return await asyncio.to_thread(_read)

    async def put(self, path: str, data: bytes) -> None:
        _, _, local_dir = self._local()
        staged = local_dir / f"relay-write-{uuid.uuid4().hex}"
        await asyncio.to_thread(local_dir.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(staged.write_bytes, data)
        try:
            await self.upload(path, staged)
        finally:
            await asyncio.to_thread(staged.unlink, missing_ok=True)

    async def upload(self, path: str, local: Path) -> None:
        local_collection, local_path, local_dir = self._local()
        try:
            relative = local.relative_to(local_dir)
        except ValueError:
            await self.put(path, await asyncio.to_thread(local.read_bytes))
            return
        source = posixpath.join(local_path, relative.as_posix())
        await self.transfer.copy(local_collection, source, self.collection, path)

    async def remove(self, path: str) -> None:
        # Transfer deletes a directory, even an empty one, only with recursive set.
        recursive = await self.kind(path) == "dir"
        await self.transfer.delete(self.collection, path, recursive=recursive)

    async def remove_tree(self, path: str) -> None:
        await self.transfer.delete(self.collection, path, recursive=True)
