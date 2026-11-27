"""W13's live client for LQ16 to LQ21: the official ``mcp`` SDK, no tasks, no elicitation.

Not a test module. Each step opens its own stdio connection to
``clio-relay serve`` (so each step is also a reconnect) and prints one
JSON line. The person's side of the page is plain HTTP, like a browser. Run it
on Ares with relay's venv and a guarded site::

    python tests/guard/guard_live.py --site fxg lq16
    python tests/guard/guard_live.py --site fxg lq17 --package w13demo.hello --sha256 H
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx
from mcp import Client
from mcp.client.stdio import StdioServerParameters

ENTRY = "from clio_relay.cli import main; main()"


@asynccontextmanager
async def connect() -> AsyncGenerator[Client]:
    params = StdioServerParameters(
        command=sys.executable, args=["-c", ENTRY, "serve"], env=dict(os.environ)
    )
    async with Client(params, mode="legacy") as client:
        yield client


def structured(result: Any) -> dict[str, Any]:
    content: Any = result.structured_content
    assert isinstance(content, dict), result
    return cast(dict[str, Any], content)


async def tool(client: Client, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return structured(await client.call_tool(name, arguments))


def page(url: str, action: str | None = None) -> dict[str, Any]:
    """GET an ask's page and, with an action, post its form the way a browser does."""
    with httpx.Client(timeout=30) as http:
        got = http.get(url)
        text = re.sub(r"<[^>]+>", " ", got.text)
        shown: dict[str, Any] = {"get": got.status_code, "text": re.sub(r"\s+", " ", text)}
        if action is None or got.status_code != 200:
            return shown
        token = re.search(r"name='t' value='([^']+)'", got.text)
        csrf = re.search(r"name='csrf' value='([^']+)'", got.text)
        assert token is not None and csrf is not None, got.text
        form = {"t": token.group(1), "csrf": csrf.group(1), "action": action}
        shown["post"] = http.post(url.split("?", 1)[0], data=form).status_code
        return shown


def refusal(shown: dict[str, Any]) -> dict[str, Any] | None:
    return shown.get("refusal")


def brief(shown: dict[str, Any]) -> dict[str, Any]:
    found = refusal(shown)
    if found is not None:
        data = found.get("data", {})
        return {
            "refusal": found["reason"],
            "message": found["message"],
            "operation_id": data.get("operation_id"),
            "asks": data.get("asks", []),
            "data": {k: v for k, v in data.items() if k not in ("asks",)},
        }
    return {
        key: shown.get(key)
        for key in ("operation_id", "state", "native_id", "error", "result", "stdout_tail")
        if key in shown
    }


async def squeue(client: Client, site: str) -> str:
    done = await tool(
        client,
        "exec",
        {"site": site, "command": ["squeue", "-h", "-o", "%i %j %T"], "max_output_bytes": 8192},
    )
    return done.get("stdout", json.dumps(done))


def package_spec(args: argparse.Namespace, **parameters: Any) -> dict[str, Any]:
    return {
        "package": {
            "name": args.package,
            "version": args.version,
            "sha256": args.sha256,
            "parameters": {"greeting": args.greeting, **parameters},
        },
        "attributes": {"queue_name": "debug"},
    }


async def step_lq16(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        free = await tool(
            client,
            "run",
            {"site": args.site, "spec": {"executable": "/bin/sh", "arguments": ["-c", "hostname"]}},
        )
        return {"step": "lq16", "free_form": brief(free), "squeue": await squeue(client, args.site)}


async def step_lq18(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        out: dict[str, Any] = {"step": "lq18"}
        for label, command in (
            ("rm", ["rm", "-rf", "relay-w13-canary"]),
            ("sh", ["sh", "-c", "squeue"]),
            ("scancel", ["scancel", "1"]),
            ("jarvis_run", [args.jarvis, "ppl", "run"]),
            ("squeue", ["squeue", "-h", "-o", "%i %j %T"]),
            ("ls_canary", ["ls", "-d", "relay-w13-canary"]),
            ("jarvis_list", [args.jarvis, "repo", "list"]),
        ):
            done = await tool(
                client,
                "exec",
                {"site": args.site, "command": command, "max_output_bytes": 8192},
            )
            out[label] = (
                brief(done)
                if refusal(done)
                else {
                    "exit_code": done.get("exit_code"),
                    "stdout": done.get("stdout"),
                    "stderr": done.get("stderr"),
                }
            )
        return out


async def step_ask(args: argparse.Namespace) -> dict[str, Any]:
    """Start a package run and show its asks and pages (LQ17 first half, LQ19 fields)."""
    async with connect() as client:
        started = brief(await tool(client, "run", {"site": args.site, "spec": package_spec(args)}))
        out: dict[str, Any] = {"step": "ask", "run": started}
        out["pages"] = [
            {"kind": ask["kind"], "page": ask["page"], **page(ask["page"])}
            for ask in started.get("asks", [])
        ]
        out["squeue"] = await squeue(client, args.site)
        return out


async def step_answer(args: argparse.Namespace) -> dict[str, Any]:
    """Answer every ask of an operation on its page, then resume."""
    async with connect() as client:
        waiting = brief(await tool(client, "run", {"resume": args.operation}))
        out: dict[str, Any] = {"step": "answer", "operation_id": args.operation}
        out["answers"] = [
            {
                "ask_id": ask["ask_id"],
                "kind": ask["kind"],
                **{k: v for k, v in page(ask["page"], args.action).items() if k != "text"},
            }
            for ask in waiting.get("asks", [])
            if ask.get("page") and (not args.ask or ask["ask_id"] in args.ask)
        ]
        out["resume"] = brief(
            await tool(client, "run", {"resume": args.operation, "wait_s": args.wait})
        )
        out["squeue"] = await squeue(client, args.site)
        return out


async def step_resume(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        out: dict[str, Any] = {"step": "resume", "operation_id": args.operation}
        out["resume"] = brief(
            await tool(client, "run", {"resume": args.operation, "wait_s": args.wait})
        )
        out["describe"] = await tool(client, "describe", {"job": args.operation})
        out["squeue"] = await squeue(client, args.site)
        return out


async def step_warm(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        spec = {
            "executable": "clio-relay-warm",
            "resources": {"node_count": 1},
            "attributes": {"queue_name": args.queue},
        }
        started = brief(await tool(client, "run", {"site": args.site, "spec": spec}))
        out: dict[str, Any] = {"step": "warm", "run": started}
        out["pages"] = [
            {"kind": ask["kind"], "page": ask["page"], **page(ask["page"])}
            for ask in started.get("asks", [])
        ]
        return out


async def step_srun(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        out: dict[str, Any] = {"step": "srun", "native_id": args.native_id}
        held = ["srun", f"--jobid={args.native_id}", "--overlap"]
        for label, command in (
            ("held", [*held, "hostname"]),
            ("held_squeue", [*held, "squeue", "-h", "-s"]),
            ("held_rm", [*held, "rm", "-f", "x"]),
            ("other_job", ["srun", "--jobid=1", "--overlap", "hostname"]),
        ):
            done = await tool(
                client, "exec", {"site": args.site, "command": command, "max_output_bytes": 8192}
            )
            out[label] = (
                brief(done)
                if refusal(done)
                else {
                    "exit_code": done.get("exit_code"),
                    "stdout": done.get("stdout"),
                }
            )
        out["squeue"] = await squeue(client, args.site)
        return out


async def step_squeue(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        return {"step": "squeue", "squeue": await squeue(client, args.site)}


async def step_exec(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        done = await tool(
            client,
            "exec",
            {"site": args.site, "command": args.command, "max_output_bytes": 65536},
        )
        shown = brief(done) if refusal(done) else done
        return {"step": "exec", "command": args.command, "result": shown}


STEPS = {
    "lq16": step_lq16,
    "lq18": step_lq18,
    "ask": step_ask,
    "answer": step_answer,
    "resume": step_resume,
    "warm": step_warm,
    "srun": step_srun,
    "squeue": step_squeue,
    "exec": step_exec,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=sorted(STEPS))
    parser.add_argument("--site", required=True)
    parser.add_argument("--jarvis", default="jarvis")
    parser.add_argument("--package", default="w13demo.hello")
    parser.add_argument("--version", default="0.1.0")
    parser.add_argument("--sha256", default="")
    parser.add_argument("--greeting", default="hello-from-w13")
    parser.add_argument("--operation")
    parser.add_argument("--action", default="accept", choices=["accept", "decline"])
    parser.add_argument("--wait", type=float, default=0)
    parser.add_argument("--ask", action="append", help="answer only these asks")
    parser.add_argument("--queue", default="debug")
    parser.add_argument("--native-id")
    parser.add_argument("command", nargs="*")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(STEPS[args.step](args)), default=str), flush=True)


if __name__ == "__main__":
    main()
