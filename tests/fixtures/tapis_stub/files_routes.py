"""The stub's Tapis Files routes, with the semantics of tapis-files over SSH.

These follow tapis-files (commit 6943d3f) and the `tapipy` 25.4.0 spec:

- ``listFiles`` pages with ``limit`` (default 1000) and ``offset``, and lists a
  file as itself. A symlink is ``symbolic_link``.
- ``getContents`` takes ``range: start,count``: the SSH client runs
  ``dd ibs=1 skip=start count=count``, and the header parser refuses
  ``start > count``.
- ``insert`` makes the parent directories and overwrites.
- ``delete`` removes a directory with everything in it, and a symlink as itself.
- ``getStatInfo`` follows links only when ``followLinks`` is true.
- Paths are taken from the system's ``rootDir`` and may not climb out of it.
"""

# FastAPI registers the route functions through their decorators.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import os
import posixpath
import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request, Response

from .replies import envelope, error

if TYPE_CHECKING:
    from .app import StubState

LIST_LIMIT = 1000


def file_info(path: Path, rel: str) -> dict[str, Any]:
    st = path.lstat()
    if path.is_symlink():
        kind = "symbolic_link"
    else:
        kind = "dir" if path.is_dir() else "file" if path.is_file() else "other"
    return {
        "mimeType": None,
        "type": kind,
        "owner": str(st.st_uid),
        "group": str(st.st_gid),
        "nativePermissions": oct(st.st_mode & 0o777)[2:],
        "url": f"tapis://stub/{rel}",
        "lastModified": datetime.fromtimestamp(st.st_mtime, UTC).isoformat(),
        "name": path.name,
        "path": rel,
        "size": st.st_size,
    }


def add_file_routes(app: FastAPI, state: StubState, user: Callable[[Request], str | None]) -> None:
    """Add the Files endpoints to ``app``."""

    def place(request: Request, system_id: str, path: str) -> tuple[Path, str] | Response:
        if user(request) is None:
            return error(401, "Invalid token")
        if system_id not in state.systems:
            return error(404, f"System not found: {system_id}")
        rel = posixpath.normpath("/" + path).lstrip("/")
        if ".." in rel.split("/"):
            return error(400, f"Path is outside the system's root: {path}")
        return state.host_path(system_id, rel), rel

    @app.get("/v3/files/ops/{system_id}/{path:path}")
    async def list_files(request: Request, system_id: str, path: str) -> Response:
        found = place(request, system_id, path)
        if isinstance(found, Response):
            return found
        target, rel = found
        if not os.path.lexists(target):
            return error(404, f"Path not found: {path}")
        if not target.is_dir() or target.is_symlink():
            return envelope([file_info(target, rel)])
        limit = int(request.query_params.get("limit") or LIST_LIMIT)
        offset = int(request.query_params.get("offset") or 0)
        items = sorted(target.iterdir(), key=lambda item: item.name)[offset : offset + limit]
        prefix = f"{rel}/" if rel else ""
        return envelope([file_info(item, f"{prefix}{item.name}") for item in items])

    @app.get("/v3/files/content/{system_id}/{path:path}")
    async def get_contents(request: Request, system_id: str, path: str) -> Response:
        found = place(request, system_id, path)
        if isinstance(found, Response):
            return found
        target, _ = found
        if target.is_dir():
            return error(400, f"Path is a directory: {path}")
        if not target.is_file():
            return error(404, f"Path not found: {path}")
        data = target.read_bytes()
        byte_range = request.headers.get("range")
        if byte_range:
            first, _, second = byte_range.partition(",")
            start, count = int(first), int(second)
            if start < 0 or start > count:
                return error(400, f"Invalid range: {byte_range}")
            data = data[start : start + count]
        return Response(content=data, media_type="application/octet-stream")

    @app.post("/v3/files/ops/{system_id}/{path:path}")
    async def insert(request: Request, system_id: str, path: str) -> Response:
        found = place(request, system_id, path)
        if isinstance(found, Response):
            return found
        target, _ = found
        form = await request.form()
        upload = form.get("file")
        if upload is None or isinstance(upload, str):
            return error(400, "file is required")
        if target.is_dir():
            return error(400, f"Path is a directory: {path}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(await upload.read())
        return envelope(None, "File uploaded")

    @app.delete("/v3/files/ops/{system_id}/{path:path}")
    async def delete(request: Request, system_id: str, path: str) -> Response:
        found = place(request, system_id, path)
        if isinstance(found, Response):
            return found
        target, _ = found
        if not os.path.lexists(target):
            return error(404, f"Path not found: {path}")
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        else:
            target.unlink()
        return envelope(None, "deleted")

    @app.get("/v3/files/utils/linux/{system_id}/{path:path}")
    async def stat_info(request: Request, system_id: str, path: str) -> Response:
        found = place(request, system_id, path)
        if isinstance(found, Response):
            return found
        target, rel = found
        follow = request.query_params.get("followLinks", "false").lower() == "true"
        if not (target.exists() if follow else os.path.lexists(target)):
            return error(404, f"Path not found: {path}")
        st = target.stat() if follow else target.lstat()
        link = not follow and target.is_symlink()
        return envelope(
            {
                "absolutePath": "/" + rel,
                "uid": st.st_uid,
                "gid": st.st_gid,
                "size": st.st_size,
                "perms": oct(st.st_mode & 0o777)[2:],
                "accessTime": int(st.st_atime),
                "modifyTime": int(st.st_mtime),
                "dir": not link and target.is_dir(),
                "link": link,
            }
        )
