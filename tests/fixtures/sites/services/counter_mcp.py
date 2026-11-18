"""A small MCP server that keeps a counter between calls.

It runs over stdio or streamable HTTP. ``count_slowly`` is task capable, so a client
that negotiates the tasks extension gets an MCP task with progress. Every result
reports the session that made the call and how many distinct sessions the server has
seen, so a test can show that one session served a series of calls.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field
from typing import Any

from fastmcp import Context, FastMCP
from fastmcp_tasks import TasksExtension

SERVER_NAME = "clio-relay-fixture-counter"


@dataclass
class CounterState:
    """The process-wide counter and the sessions that touched it."""

    count: int = 0
    calls: int = 0
    sessions: set[str] = field(default_factory=lambda: set[str]())

    def observe(self, ctx: Context) -> str:
        """Record one call from ``ctx``'s session and return the session id."""
        self.calls += 1
        session_id = ctx.session_id
        self.sessions.add(session_id)
        return session_id

    def report(self, session_id: str) -> dict[str, Any]:
        """Return the state a tool result carries."""
        return {
            "count": self.count,
            "calls": self.calls,
            "session_id": session_id,
            "sessions_seen": len(self.sessions),
            "pid": os.getpid(),
        }


def build_server(state: CounterState | None = None) -> FastMCP:
    """Build the counter server around ``state``."""
    counter = state if state is not None else CounterState()
    mcp = FastMCP(SERVER_NAME)
    mcp.add_extension(TasksExtension())

    @mcp.tool
    def increment(ctx: Context, by: int = 1) -> dict[str, Any]:
        """Add ``by`` to the counter and return the new state."""
        session_id = counter.observe(ctx)
        counter.count += by
        return counter.report(session_id)

    @mcp.tool
    def read(ctx: Context) -> dict[str, Any]:
        """Return the counter without changing it."""
        return counter.report(counter.observe(ctx))

    @mcp.tool
    def reset(ctx: Context) -> dict[str, Any]:
        """Set the counter to zero and forget the sessions seen."""
        counter.count = 0
        counter.calls = 0
        counter.sessions.clear()
        return counter.report(counter.observe(ctx))

    @mcp.tool(task=True)
    async def count_slowly(ctx: Context, steps: int = 3, delay_seconds: float = 0.5) -> dict[str, Any]:
        """Add one per step, reporting progress, as a task when the client negotiates one."""
        session_id = counter.observe(ctx)
        for step in range(steps):
            await asyncio.sleep(delay_seconds)
            counter.count += 1
            await ctx.report_progress(step + 1, steps, f"step {step + 1} of {steps}")
        result = counter.report(session_id)
        result["background_task"] = ctx.is_background_task
        return result

    _ = (increment, read, reset, count_slowly)
    return mcp


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description="Counter MCP fixture server.")
    parser.add_argument("--transport", choices=("stdio", "http"), default="stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--path", default="/mcp")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    server = build_server()
    if args.transport == "stdio":
        server.run(transport="stdio", show_banner=False)
    else:
        server.run(
            transport="http",
            show_banner=False,
            host=args.host,
            port=args.port,
            path=args.path,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
