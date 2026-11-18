"""LQ42's live client: the official ``mcp`` SDK, with no tasks and no elicitation.

Not a test module. Each step opens its own connection, so every step after the
first is a reconnect. Over stdio it starts ``clio-relay-next serve``; over
Streamable HTTP it talks to a relay someone else started. Each step prints one
JSON line. Run it on Ares with the relay's venv::

    python tests/asks/lq42_live.py --site slurm gate
    python tests/asks/lq42_live.py --url https://host:port/mcp --token-file F \\
        --operator-file G --cacert C --site slurm gate
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import httpx2
from mcp import Client
from mcp.client.stdio import StdioServerParameters
from mcp.client.streamable_http import streamable_http_client

ENTRY = "from clio_relay.cli_next import main; main()"


def spec(tag: str, seconds: int = 0) -> dict[str, Any]:
    """Return a small Slurm job that prints its node and a tag."""
    script = f"hostname; echo lq42-{tag}; sleep {seconds}"
    return {
        "executable": "/bin/sh",
        "arguments": ["-c", script],
        "attributes": {"queue_name": "debug", "duration": "PT5M"},
    }


class Live:
    """One way to reach relay, and the person's side of the page."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.token = Path(args.token_file).read_text().strip() if args.token_file else None
        self.operator = (
            Path(args.operator_file).read_text().strip() if args.operator_file else None
        )
        self.verify: Any = args.cacert or True

    @asynccontextmanager
    async def client(self) -> AsyncIterator[Client]:
        """Connect the way any MCP host would, advertising nothing extra."""
        if self.args.url is None:
            params = StdioServerParameters(
                command=sys.executable, args=["-c", ENTRY, "serve"], env=dict(os.environ)
            )
            async with Client(params, mode=self.args.mode) as client:
                yield client
            return
        headers = {"Authorization": f"Bearer {self.token}"}
        async with httpx2.AsyncClient(headers=headers, timeout=120, verify=self.verify) as http:
            transport = streamable_http_client(self.args.url, http_client=http)
            async with Client(transport, mode=self.args.mode) as client:
                yield client

    def page(self, url: str, action: str | None = None, *, auth: bool = True) -> dict[str, Any]:
        """GET the page; with an action, also POST the form the way a browser does."""
        credential = ("operator", self.operator) if auth and self.operator else None
        with httpx.Client(timeout=30, verify=self.verify, auth=credential) as http:
            got = http.get(url)
            shown: dict[str, Any] = {"get": got.status_code}
            if action is None or got.status_code != 200:
                return shown
            token = re.search(r"name='t' value='([^']+)'", got.text)
            csrf = re.search(r"name='csrf' value='([^']+)'", got.text)
            assert token is not None and csrf is not None, got.text
            form = {"t": token.group(1), "csrf": csrf.group(1), "action": action}
            target = url.split("?", 1)[0]
            shown["post"] = http.post(target, data=form).status_code
            shown["post_again"] = http.post(target, data=form).status_code
            return shown


def structured(result: Any) -> dict[str, Any]:
    """Return a tool result's structured content."""
    content = result.structured_content
    assert isinstance(content, dict), result
    return dict(content)  # pyright: ignore[reportUnknownArgumentType]


def operation(result: Any) -> dict[str, Any]:
    """Return the operation a result names, refusal or not."""
    shown = structured(result)
    if "refusal" in shown:
        refusal = shown["refusal"]
        return {
            "refusal": refusal["reason"],
            "operation_id": refusal["data"].get("operation_id"),
            "asks": refusal["data"].get("asks", []),
        }
    return {
        "state": shown.get("state"),
        "operation_id": shown.get("operation_id"),
        "native_id": shown.get("native_id"),
        "job_name": (shown.get("result") or {}).get("job_name"),
        "spec_digest": shown.get("spec_digest"),
        "outputs": shown.get("outputs"),
    }


async def call(client: Client, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Call a tool and return the operation it names."""
    return operation(await client.call_tool(tool, arguments, raise_on_error=False))


async def ask_for(live: Live, client: Client, tag: str, seconds: int = 0) -> dict[str, Any]:
    """Start a run that the spend gate stops; return the refusal."""
    gated = await call(client, "run", {"site": live.args.site, "spec": spec(tag, seconds)})
    assert gated.get("refusal") == "needs_approval", gated
    return gated


async def describe(client: Client, operation_id: str) -> dict[str, Any]:
    """Observe one operation."""
    shown = structured(await client.call_tool("describe", {"job": operation_id}))
    return {
        "state": shown.get("state"),
        "native_id": shown.get("native_id"),
        "asks": [(ask["ask_id"], ask["state"]) for ask in shown.get("asks", [])],
        "job_name": (shown.get("result") or {}).get("job_name"),
    }


async def step_gate(live: Live) -> dict[str, Any]:
    """The gate: approve on the page, retry twice, and submit once."""
    async with live.client() as client:
        gated = await ask_for(live, client, "gate")
        op = gated["operation_id"]
        (ask,) = gated["asks"]
        out: dict[str, Any] = {"step": "gate", "operation_id": op, "ask_id": ask["ask_id"]}
        out["page_host"] = ask["page"].split("/relay/")[0].split("?")[0]
        if live.operator:
            out["page_without_operator"] = live.page(ask["page"], auth=False)
        out["page_get"] = [live.page(ask["page"]), live.page(ask["page"])]
        out["after_get"] = await describe(client, op)
        out["answer"] = live.page(ask["page"], "accept")
        out["retry_1"] = await call(client, "run", {"resume": op, "wait_s": 0})
        out["retry_2"] = await call(client, "run", {"resume": op, "wait_s": 0})
        out["changed_args"] = await call(
            client, "run", {"site": live.args.site, "spec": spec("gate-changed")}
        )
        out["changed_resume"] = await call(
            client,
            "run",
            {"resume": op, "site": live.args.site, "spec": spec("gate-changed"), "wait_s": 0},
        )
        return out


async def step_decline(live: Live) -> dict[str, Any]:
    """A decline submits nothing."""
    async with live.client() as client:
        gated = await ask_for(live, client, "decline")
        op = gated["operation_id"]
        out: dict[str, Any] = {"step": "decline", "operation_id": op}
        out["answer"] = live.page(gated["asks"][0]["page"], "decline")
        out["retry"] = await call(client, "run", {"resume": op, "wait_s": 0})
        out["after"] = await describe(client, op)
        return out


async def step_unanswered(live: Live) -> dict[str, Any]:
    """No answer submits nothing; the operation keeps waiting."""
    async with live.client() as client:
        gated = await ask_for(live, client, "unanswered")
        op = gated["operation_id"]
        retried = await call(client, "run", {"resume": op, "wait_s": 2})
        return {"step": "unanswered", "operation_id": op, "retry": retried}


async def step_approve_only(live: Live) -> dict[str, Any]:
    """Approve on the page and leave; the next connection submits."""
    async with live.client() as client:
        gated = await ask_for(live, client, live.args.tag, live.args.seconds)
        op = gated["operation_id"]
        answer = live.page(gated["asks"][0]["page"], "accept")
        return {"step": "approve-only", "operation_id": op, "answer": answer}


async def step_resume(live: Live) -> dict[str, Any]:
    """Reconnect, resume or observe an operation, and read its output."""
    op = live.args.operation
    async with live.client() as client:
        out: dict[str, Any] = {"step": "resume", "operation_id": op}
        out["before"] = await describe(client, op)
        out["resume"] = await call(client, "run", {"resume": op, "wait_s": live.args.wait})
        out["after"] = await describe(client, op)
        if out["after"]["state"] == "completed":
            stdout = f"jobs/{op}/stdout"
            read = structured(
                await client.call_tool(
                    "files", {"site": live.args.site, "op": "read", "path": stdout}
                )
            )
            out["stdout"] = read.get("text", read)
        return out


async def step_cancel(live: Live) -> dict[str, Any]:
    """Cancel through run."""
    op = live.args.operation
    async with live.client() as client:
        cancelled = await call(client, "run", {"cancel": op})
        return {"step": "cancel", "operation_id": op, "cancel": cancelled}


STEPS = {
    "gate": step_gate,
    "decline": step_decline,
    "unanswered": step_unanswered,
    "approve-only": step_approve_only,
    "resume": step_resume,
    "cancel": step_cancel,
}


def main() -> None:
    """Run one step and print its JSON line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=sorted(STEPS))
    parser.add_argument("--site", required=True)
    parser.add_argument("--mode", default="legacy", choices=["legacy", "auto"])
    parser.add_argument("--url")
    parser.add_argument("--token-file")
    parser.add_argument("--operator-file")
    parser.add_argument("--cacert")
    parser.add_argument("--operation")
    parser.add_argument("--tag", default="run")
    parser.add_argument("--seconds", type=int, default=0)
    parser.add_argument("--wait", type=float, default=0)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(STEPS[args.step](Live(args))), default=str), flush=True)


if __name__ == "__main__":
    main()
