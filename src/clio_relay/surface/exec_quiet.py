"""Tell the person when an ``exec`` call goes quiet, and count how often it did."""

from __future__ import annotations

from typing import Any

from fastmcp import Context


class QuietReports:
    """The ``on_stall`` callback for one exec call.

    Each report is a progress note on the call. There is no time limit: the
    note says the command still runs and that cancelling the call stops it.
    """

    def __init__(self, ctx: Context, *, read_only: bool) -> None:
        self._ctx = ctx
        self._read_only = read_only
        self._stalls: list[float] = []

    async def __call__(self, idle_s: float, next_s: float) -> None:
        self._stalls.append(idle_s)
        note = (
            f"no output for {idle_s:.0f} s; the command is still running; relay checks "
            f"again in {next_s:.0f} s; cancel the call to stop it"
        )
        if self._read_only:
            note += "; it only reads, so running it again is safe"
        await self._ctx.report_progress(idle_s, message=note)

    def summary(self) -> dict[str, Any] | None:
        """The quiet stretches, for a client that sent no progress token and saw none."""
        if not self._stalls:
            return None
        return {"reports": len(self._stalls), "longest_s": round(max(self._stalls), 1)}
