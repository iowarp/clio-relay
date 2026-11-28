"""Relay's read tools: each only reads, says so in its annotations, and never asks.

A host that runs only reads, or runs them without a question, can honour that
per tool: ``describe`` (relay, a site, an operation or a service),
``exec_read`` (a command on the guard's read-only list, on every site),
``files_read`` (list, stat, read, has) and ``packages_read`` (the packages a
site's JARVIS root holds).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

import mcp_types
from fastmcp import Context, FastMCP
from fastmcp.tools import InputRequiredToolResult, ToolResult
from pydantic import Field

from clio_relay.jobs.refusals import Refusal
from clio_relay.surface.elicit import refusal_result
from clio_relay.surface.files_verb import MAX_INLINE_BYTES, FilesArgs
from clio_relay.surface.package_tool import packages as packages_op
from clio_relay.surface.results import READ, record_result, text

if TYPE_CHECKING:
    from clio_relay.surface.server import RelayServer


def _shown(value: dict[str, Any]) -> ToolResult:
    return ToolResult(
        content=[mcp_types.TextContent(type="text", text=text(value))], structured_content=value
    )


def install(server: FastMCP[Any], relay: RelayServer) -> None:
    """Add the read tools to ``server``."""

    @server.tool(name="describe", title="Describe relay, a site, or an operation", annotations=READ)
    async def describe(  # pyright: ignore[reportUnusedFunction]
        site: Annotated[str | None, Field(description="A site id")] = None,
        job: Annotated[
            str | None, Field(description="An operation id, a service's included")
        ] = None,
    ) -> ToolResult:
        """Without arguments, list the sites. With site, describe it, its routes and
        the JARVIS packages its root holds (packages, with their parameters),
        and return its guide (skill.text) when it has one: read it before the first
        run there. describe reaches the site, and a site relay cannot reach comes
        back with a refusal or the sign-in it needs; the site's config is how relay
        reaches it, never a host or file to check yourself. With job, observe one
        operation, a service from serve included:
        its state, when relay last looked (result.observed_at), the latest step
        (result.progress) and the outputs. This only reads."""
        try:
            if job is not None:
                return record_result(relay.service, await relay.service.refresh(job))
            described = await relay.service.describe(site)
        except Refusal as refusal:
            return refusal_result(refusal)
        skill = cast("dict[str, Any] | None", described.get("skill")) if site else None
        guide = skill.get("text") if skill else None
        if not (skill and isinstance(guide, str) and guide):
            return _shown(described)
        listed = {**described, "skill": {**skill, "text": "(below)"}}
        content = [
            mcp_types.TextContent(type="text", text=text(listed)),
            mcp_types.TextContent(type="text", text=guide),
        ]
        return ToolResult(content=content, structured_content=described)

    @server.tool(name="exec_read", title="Run one read-only command on a site", annotations=READ)
    async def exec_read(  # pyright: ignore[reportUnusedFunction]
        site: Annotated[str, Field(description="The site id")],
        command: Annotated[list[str], Field(min_length=1, description="argv, no shell")],
        ctx: Context,
        timeout_s: Annotated[
            float | None,
            Field(gt=0, description="Optional limit; by default the command runs until it ends"),
        ] = None,
        max_output_bytes: Annotated[int, Field(ge=1, le=MAX_INLINE_BYTES)] = 65536,
    ) -> ToolResult | InputRequiredToolResult:
        """Run one command that only reads, on any site, and return bounded output.
        Relay's guard checks the argv against its read-only list whatever the
        site's policy: files (ls, cat, grep, find without actions), the scheduler
        (squeue, sinfo, sacct, scontrol show), the machine (hostname, nproc),
        modules and quotas, JARVIS reads (ppl list, ppl status, and exactly
        jarvis sched info +json, jarvis rg facts show +json, jarvis rg skill list
        +json, jarvis rg skill show NAME +json) and Spack queries (spack find,
        info, spec). Anything else is refused: use exec."""
        return await relay.exec(ctx, site, command, timeout_s, max_output_bytes, read_only=True)

    @server.tool(name="packages_read", title="List the packages relay can run", annotations=READ)
    async def packages_read(  # pyright: ignore[reportUnusedFunction]
        site: Annotated[str, Field(description="The site id")],
        match: Annotated[
            str | None,
            Field(description="Keep the packages whose name, title or summary has these words"),
        ] = None,
    ) -> ToolResult:
        """List the JARVIS packages the site's JARVIS root holds, as the site lists
        them now: each one's version, review digest, summary and git source. Look
        here for the application before writing a package: an application's
        package takes its own input (a script, an input bundle, a settings file),
        so a new case is that input, not a new package. match ("lammps",
        "molecular dynamics") narrows the list. run takes a package by its name.
        This only reads."""
        try:
            return _shown(await packages_op(relay.service, site, match=match))
        except Refusal as refusal:
            return refusal_result(refusal)

    @server.tool(name="files_read", title="List, stat and read files on a site", annotations=READ)
    async def files_read(  # pyright: ignore[reportUnusedFunction]
        ctx: Context,
        site: Annotated[str, Field(description="The site id")],
        op: Literal["list", "stat", "read", "has"],
        path: Annotated[
            str | None,
            Field(description="Absolute, ~/ for home, or relative to relay's work directory"),
        ] = None,
        offset: Annotated[int, Field(ge=0, description="Where a read starts")] = 0,
        limit: Annotated[int | None, Field(ge=1)] = None,
        digest: Annotated[str | None, Field(description="A SHA-256, for has")] = None,
    ) -> ToolResult | InputRequiredToolResult:
        """The reads of files, on any site, with no question: list a directory, stat
        a path, read a file (up to 4 MiB a call, for content you need to see) and
        has, whether the site's content store holds a SHA-256. To copy a file to
        this machine, use files op download with local_path. Writes, deletes,
        stage, upload and download are in files."""
        arguments = FilesArgs(op=op, path=path, offset=offset, limit=limit, digest=digest)
        return await relay.files(ctx, site, arguments)
