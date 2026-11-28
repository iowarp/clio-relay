"""The ``install`` tool: a package's software, installed on a site ahead of any run.

It is ``run``'s install task on its own (:mod:`clio_relay.surface.install_op`):
JARVIS checks the site, relay asks what JARVIS asks, and JARVIS installs in a
job on the site's compute nodes. Nothing is launched. A site without JARVIS
gets relay's jarvis-cd first, after the person's yes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Annotated

from fastmcp import Context
from fastmcp.tools import InputRequiredToolResult, ToolResult
from pydantic import Field

from clio_relay.jobs.spec import JobSpec

if TYPE_CHECKING:
    from clio_relay.surface.server import RelayServer

Tool = Callable[..., Awaitable[ToolResult | InputRequiredToolResult]]


def install_tool(relay: RelayServer) -> Tool:
    """Return the ``install`` tool's function, for the server to register as a task tool."""

    async def install(
        ctx: Context,
        site: Annotated[str | None, Field(description="The site id")] = None,
        spec: Annotated[
            JobSpec | None,
            Field(description="The package job a run would submit: package {name, parameters}"),
        ] = None,
        cancel: Annotated[
            str | None, Field(description="An install operation id to cancel")
        ] = None,
        resume: Annotated[
            str | None, Field(description="An install operation id waiting on a person")
        ] = None,
        wait_s: Annotated[
            float, Field(ge=0, le=300, description="Seconds to wait without a task")
        ] = 20,
    ) -> ToolResult | InputRequiredToolResult:
        """Install a package's software (or a pipeline's) on a site ahead of a
        run, and run nothing. JARVIS checks what the site has; when something is
        missing, relay asks the person to approve the install and any choice JARVIS
        needs, then JARVIS builds it as a job on the site's compute nodes and checks
        again. A site not set up gets its JARVIS root first (jarvis-cd and the site's
        packages), after a yes. The
        operation completes with what is installed; describe(job=...) observes it."""
        return await relay.run(ctx, site, spec, cancel, resume, wait_s, kind="install")

    return install
