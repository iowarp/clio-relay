"""File access on the site for the ``worker`` adapter, over the worker's link."""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any

from clio_relay.jobs.handles import FileEntry

if TYPE_CHECKING:
    from clio_relay.adapters.worker.adapter import WorkerAdapter

_READ_CHUNK_BYTES = 4 * 1024 * 1024


class WorkerFileAccess:
    """File access on the site, over the worker's link."""

    def __init__(self, adapter: WorkerAdapter) -> None:
        self._adapter = adapter

    async def _call(self, op: str, body: dict[str, Any], timeout_s: float = 300) -> Any:
        return await self._adapter.call(f"/files/{op}", body, timeout_s=timeout_s)

    async def list(self, path: str) -> list[FileEntry]:
        reported = await self._call("list", {"path": path})
        return [FileEntry.model_validate(entry) for entry in reported]

    async def stat(self, path: str) -> FileEntry:
        return FileEntry.model_validate(await self._call("stat", {"path": path}))

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        chunks: list[bytes] = []
        position = max(0, offset)
        remaining = limit
        while remaining is None or remaining > 0:
            size = _READ_CHUNK_BYTES if remaining is None else min(remaining, _READ_CHUNK_BYTES)
            encoded = await self._call("read", {"path": path, "offset": position, "limit": size})
            chunk = base64.b64decode(encoded)
            chunks.append(chunk)
            position += len(chunk)
            if remaining is not None:
                remaining -= len(chunk)
            if len(chunk) < size:
                break
        return b"".join(chunks)

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        body: dict[str, Any] = {
            "path": path,
            "data": base64.b64encode(data).decode("ascii"),
            "overwrite": overwrite,
        }
        if offset == 0 and final:
            return FileEntry.model_validate(await self._call("write", body))
        # A separate op, so a site that predates chunks refuses it instead of
        # writing one chunk as the whole file.
        body.update(offset=offset, final=final)
        return FileEntry.model_validate(await self._call("write-chunk", body))

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        await self._call("delete", {"path": path, "recursive": recursive})

    async def stage(self, data: bytes) -> str:
        digest = await self._call("stage", {"data": base64.b64encode(data).decode("ascii")})
        return str(digest)

    async def has(self, digest: str) -> bool:
        return bool(await self._call("has", {"digest": digest}))
