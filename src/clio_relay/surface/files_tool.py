"""The ``files`` tool's registration: its arguments and what it tells the agent."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import InputRequiredToolResult, ToolResult
from pydantic import Field

from clio_relay.surface.files_verb import FilesArgs
from clio_relay.surface.results import CHANGE

if TYPE_CHECKING:
    from clio_relay.surface.server import RelayServer


def install(server: FastMCP[Any], relay: RelayServer) -> None:
    """Add the ``files`` tool to ``server``."""

    @server.tool(
        name="files",
        title="List, read, write, delete and stage files on a site",
        annotations=CHANGE,
    )
    async def files(  # pyright: ignore[reportUnusedFunction]
        ctx: Context,
        site: Annotated[str, Field(description="The site id")],
        op: Literal["list", "stat", "read", "write", "delete", "stage", "upload", "download"],
        path: Annotated[
            str | None,
            Field(description="Absolute, ~/ for home, or relative to relay's work directory"),
        ] = None,
        content: Annotated[str | None, Field(description="Text to write or stage")] = None,
        content_base64: Annotated[
            str | None, Field(description="Bytes to write or stage, base64")
        ] = None,
        offset: Annotated[
            int, Field(ge=0, description="Where a read starts, or where a write chunk goes")
        ] = 0,
        limit: Annotated[int | None, Field(ge=1)] = None,
        overwrite: bool = False,
        final: Annotated[bool, Field(description="False while more chunks follow")] = True,
        recursive: Annotated[bool, Field(description="Delete a non-empty directory")] = False,
        local_path: Annotated[
            str | None,
            Field(
                description="upload: the file on the relay host to send. download: "
                "where on the relay host to save it. Absolute, or ~/"
            ),
        ] = None,
    ) -> ToolResult | InputRequiredToolResult:
        """To move a file that exists on this machine, use upload (local_path to
        path) or download (path to local_path): relay streams the bytes itself,
        checks the SHA-256 on the site and returns only metadata. Never send a file
        as base64 chunks you could name by path. write and read move content
        inline, up to 4 MiB; write chunks (offset, final=false, then final=true) are
        for bytes that exist only in the conversation. stage puts bytes in the
        site's content store and returns the SHA-256 a JobSpec input names. On a
        guarded site, delete and an upload or write that replaces a file wait for
        the person's yes. list, stat and read only read: files_read does them
        and never asks."""
        return await relay.files(
            ctx,
            site,
            FilesArgs(
                op=op,
                path=path,
                content=content,
                content_base64=content_base64,
                offset=offset,
                limit=limit,
                overwrite=overwrite,
                final=final,
                recursive=recursive,
                local_path=local_path,
            ),
        )
