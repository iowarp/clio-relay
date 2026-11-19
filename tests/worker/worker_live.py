"""W8's live client for the worker adapter: the official ``mcp`` SDK, no tasks.

Not a test module. Each step opens its own stdio connection to
``clio-relay serve`` and prints one JSON line. Sign-ins and approvals are
answered only on relay's page, over plain HTTP like a browser. Run it on a host
whose relay has ``worker`` sites::

    python tests/worker/worker_live.py --site w8o signin
    python tests/worker/worker_live.py --site w8o files --tag cold
    python tests/worker/worker_live.py --site w8o job --tag cold
    python tests/worker/worker_live.py --site w8o drop-site --ssh-config F --ssh-host H
    python tests/worker/worker_live.py --site w8o drop-relay
    python tests/worker/worker_live.py --site w8o observe --operation OP --path P
    python tests/worker/worker_live.py --site w8g guarded --package demo.hello --sha256 H
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import httpx
from mcp import Client
from mcp.client.stdio import StdioServerParameters

ENTRY = "from clio_relay.cli import main; main()"
# Stall detection for the recovery loops, not a limit on any job.
RECOVERY_ATTEMPTS = 60


@asynccontextmanager
async def connect() -> AsyncGenerator[Client]:
    params = StdioServerParameters(
        command=sys.executable, args=["-c", ENTRY, "serve"], env=dict(os.environ)
    )
    async with Client(params, mode="legacy") as client:
        yield client


async def tool(client: Client, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    result: Any = await client.call_tool(name, arguments)
    content: Any = result.structured_content
    assert isinstance(content, dict), result
    return cast(dict[str, Any], content)


def page(url: str, action: str | None = None) -> dict[str, Any]:
    """GET an ask's page and, with an action, post its form the way a browser does."""
    with httpx.Client(timeout=30) as http:
        got = http.get(url)
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", got.text))
        shown: dict[str, Any] = {"get": got.status_code, "text": text}
        if action is None or got.status_code != 200 or "name='csrf'" not in got.text:
            return shown
        token = re.search(r"name='t' value='([^']+)'", got.text)
        csrf = re.search(r"name='csrf' value='([^']+)'", got.text)
        assert token is not None and csrf is not None, got.text
        fields = dict(re.findall(r"type='hidden' name='([^']+)' value='([^']*)'", got.text))
        form = {**fields, "t": token.group(1), "csrf": csrf.group(1), "action": action}
        shown["post"] = http.post(url.split("?", 1)[0], data=form).status_code
        return shown


def brief(shown: dict[str, Any]) -> dict[str, Any]:
    found = shown.get("refusal")
    if found is not None:
        data = found.get("data", {})
        return {
            "refusal": found["reason"],
            "message": found["message"],
            "operation_id": data.get("operation_id"),
            "asks": data.get("asks", []),
        }
    return shown


async def sign_in_page(url: str) -> dict[str, Any]:
    """Start a sign-in on its page and wait until the page says how it ended."""
    out: dict[str, Any] = {"start": {k: v for k, v in page(url, "start").items() if k != "text"}}
    polls = 0
    while True:
        polls += 1
        shown = page(url)
        if "Waiting for the site" not in shown["text"]:
            out["page"] = shown["text"][-600:]
            break
        await asyncio.sleep(2)
    out["page_polls"] = polls
    return out


async def answer_asks(asks: list[dict[str, Any]]) -> list[Any]:
    """Answer each ask on its page the way a person would."""
    answered: list[Any] = []
    for ask in asks:
        if not ask.get("page"):
            continue
        if ask.get("kind") == "url":
            signed = await sign_in_page(ask["page"])
            answered.append({"ask_id": ask.get("ask_id"), "kind": "url", **signed})
            continue
        shown = page(ask["page"])
        posted = page(ask["page"], "accept")
        answered.append(
            {
                "ask_id": ask.get("ask_id"),
                "kind": ask.get("kind"),
                "question": shown["text"][-900:],
                "post": posted.get("post"),
            }
        )
    return answered


async def settle(client: Client, started: dict[str, Any]) -> tuple[dict[str, Any], list[Any]]:
    """Answer a run's asks and resume it until relay stops asking."""
    rounds: list[Any] = []
    operation = started.get("operation_id")
    asking = ("needs_approval", "needs_sign_in")
    while operation and started.get("refusal") in asking and started.get("asks"):
        rounds.append(await answer_asks(started["asks"]))
        started = brief(await tool(client, "run", {"resume": operation, "wait_s": 5}))
        rounds.append({"resumed": {k: v for k, v in started.items() if k != "asks"}})
    return started, rounds


async def sign_in(client: Client, site: str, spec: dict[str, Any] | None = None) -> dict[str, Any]:
    """Probe the site; if it needs a sign-in, sign in through a small run's page.

    A guarded site refuses a free-form run before it asks for anything, so the
    caller passes a package spec there.
    """
    probe = brief(await tool(client, "files", {"site": site, "op": "list", "path": "."}))
    out: dict[str, Any] = {"probe": probe}
    if probe.get("refusal") != "needs_sign_in":
        return out
    spec = spec or {"executable": "/bin/sh", "arguments": ["-c", "echo w8-signed-in"]}
    started = brief(await tool(client, "run", {"site": site, "spec": spec, "wait_s": 5}))
    out["started"] = started
    started, out["rounds"] = await settle(client, started)
    if started.get("operation_id") and not started.get("refusal"):
        out["final"] = await wait_final(client, started["operation_id"])
    out["after"] = brief(await tool(client, "files", {"site": site, "op": "list", "path": "."}))
    return out


async def files_round_trip(client: Client, site: str, tag: str) -> dict[str, Any]:
    path = f"w8-proof/{tag}.txt"
    text = f"w8 {tag} {time.time_ns()}\n"
    out: dict[str, Any] = {}
    out["write"] = brief(
        await tool(
            client,
            "files",
            {"site": site, "op": "write", "path": path, "content": text, "overwrite": True},
        )
    )
    read = brief(await tool(client, "files", {"site": site, "op": "read", "path": path}))
    out["read"] = read
    out["read_matches"] = read.get("text") == text
    out["stat"] = brief(await tool(client, "files", {"site": site, "op": "stat", "path": path}))
    out["list"] = brief(
        await tool(client, "files", {"site": site, "op": "list", "path": "w8-proof"})
    )
    blob = os.urandom(300_000)
    digest = hashlib.sha256(blob).hexdigest()
    staged = brief(
        await tool(
            client,
            "files",
            {"site": site, "op": "stage", "content_base64": base64.b64encode(blob).decode()},
        )
    )
    out["stage"] = {"expected": digest, "answer": staged, "matches": digest in json.dumps(staged)}
    out["has"] = brief(await tool(client, "files", {"site": site, "op": "has", "digest": digest}))
    out["path"] = path
    return out


async def wait_final(client: Client, operation_id: str) -> dict[str, Any]:
    polls = 0
    while True:
        polls += 1
        seen = await tool(client, "describe", {"job": operation_id})
        if seen.get("refusal") or seen.get("state") in ("completed", "failed", "cancelled"):
            return {"polls": polls, **seen}
        await asyncio.sleep(3)


async def run_job(
    client: Client, site: str, tag: str, args: argparse.Namespace | None = None
) -> dict[str, Any]:
    """Run a job and wait for it: free-form, or the package when ``args`` names one."""
    out: dict[str, Any] = {}
    if args is not None and args.package:
        spec = package_spec(args, tag)
        marker = f"w8-{tag}"
    else:
        marker = f"w8-input-{tag}-{time.time_ns()}\n"
        staged = await tool(client, "files", {"site": site, "op": "stage", "content": marker})
        out["staged"] = brief(staged)
        digest = hashlib.sha256(marker.encode()).hexdigest()
        spec = {
            "executable": "/bin/sh",
            "arguments": ["-c", "cat in.txt; hostname; echo w8-job-done"],
            "inputs": [{"name": "in.txt", "sha256": digest}],
        }
    started = brief(await tool(client, "run", {"site": site, "spec": spec, "wait_s": 5}))
    out["started"] = started
    started, out["approvals"] = await settle(client, started)
    operation = started.get("operation_id")
    if operation and not started.get("refusal"):
        final = await wait_final(client, operation)
        out["final"] = final
        out["marker_in_output"] = marker.strip() in json.dumps(final)
    return out


def package_spec(args: argparse.Namespace, greeting: str) -> dict[str, Any]:
    return {
        "package": {
            "name": args.package,
            "version": args.version,
            "sha256": args.sha256,
            "parameters": {"greeting": f"w8-{greeting}"},
        }
    }


async def step_signin(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        spec = package_spec(args, "signin") if args.package else None
        return {"step": "signin", "site": args.site, **await sign_in(client, args.site, spec)}


async def step_files(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        return {"step": "files", **await files_round_trip(client, args.site, args.tag)}


async def step_job(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        return {"step": "job", **await run_job(client, args.site, args.tag)}


async def recover(client: Client, site: str) -> list[dict[str, Any]]:
    """Call files until it answers again, recording every refusal on the way."""
    seen: list[dict[str, Any]] = []
    for _ in range(RECOVERY_ATTEMPTS):
        answer = brief(await tool(client, "files", {"site": site, "op": "list", "path": "."}))
        seen.append({"t": round(time.time(), 1), **answer})
        if not answer.get("refusal"):
            return seen
        await asyncio.sleep(2)
    return seen


async def step_drop_site(args: argparse.Namespace) -> dict[str, Any]:
    """Kill the site's frpc under a held link, then use the link again."""
    async with connect() as client:
        out: dict[str, Any] = {"step": "drop-site"}
        out["before"] = brief(
            await tool(client, "files", {"site": args.site, "op": "list", "path": "."})
        )
        script = 'pkill -f -- "$1/run/frpc.toml"; echo killed=$?'
        killed = subprocess.run(  # noqa: S603
            [
                "ssh",
                "-F",
                args.ssh_config,
                args.ssh_host,
                "bash",
                "-c",
                f"'{script}'",
                "_",
                args.remote_dir,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        out["kill"] = (killed.stdout + killed.stderr).strip()
        out["recovery"] = await recover(client, args.site)
        out["job"] = await run_job(client, args.site, f"{args.tag}-after-site-drop", args)
        out["files"] = await files_round_trip(client, args.site, f"{args.tag}-after-site-drop")
        return out


def _descendant_frpc(root: int) -> list[int]:
    parents: dict[int, int] = {}
    names: dict[int, str] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            names[int(entry.name)] = (entry / "comm").read_text().strip()
        except OSError:
            continue
        parents[int(entry.name)] = int(fields[1])
    found: list[int] = []
    for pid, name in names.items():
        walk = pid
        while walk > 1 and walk in parents:
            walk = parents[walk]
            if walk == root:
                if name == "frpc":
                    found.append(pid)
                break
    return found


async def step_drop_relay(args: argparse.Namespace) -> dict[str, Any]:
    """Kill relay's own frpc visitor under a held link, then use the link again."""
    async with connect() as client:
        out: dict[str, Any] = {"step": "drop-relay"}
        out["before"] = brief(
            await tool(client, "files", {"site": args.site, "op": "list", "path": "."})
        )
        visitors = _descendant_frpc(os.getpid())
        for pid in visitors:
            os.kill(pid, 9)  # SIGKILL, as a crash would
        out["killed_visitors"] = visitors
        out["recovery"] = await recover(client, args.site)
        out["visitors_after"] = _descendant_frpc(os.getpid())
        out["job"] = await run_job(client, args.site, f"{args.tag}-after-relay-drop", args)
        out["files"] = await files_round_trip(client, args.site, f"{args.tag}-after-relay-drop")
        return out


async def step_observe(args: argparse.Namespace) -> dict[str, Any]:
    """From a fresh relay process: observe an earlier job and read an earlier file."""
    async with connect() as client:
        out: dict[str, Any] = {"step": "observe"}
        if args.operation:
            out["describe"] = await tool(client, "describe", {"job": args.operation})
        if args.path:
            out["read"] = brief(
                await tool(client, "files", {"site": args.site, "op": "read", "path": args.path})
            )
        return out


async def step_guarded(args: argparse.Namespace) -> dict[str, Any]:
    async with connect() as client:
        out: dict[str, Any] = {"step": "guarded"}
        out["free_form"] = brief(
            await tool(
                client,
                "run",
                {"site": args.site, "spec": {"executable": "/bin/sh", "arguments": ["-c", "id"]}},
            )
        )
        out["exec_rm"] = brief(
            await tool(client, "exec", {"site": args.site, "command": ["rm", "-rf", "w8-proof"]})
        )
        ls = await tool(client, "exec", {"site": args.site, "command": ["ls", "-d", "."]})
        out["exec_ls"] = {k: ls.get(k) for k in ("refusal", "exit_code", "stdout")}
        out["files"] = await files_round_trip(client, args.site, f"{args.tag}-guarded")
        spec = package_spec(args, args.tag)
        started = brief(await tool(client, "run", {"site": args.site, "spec": spec, "wait_s": 5}))
        out["started"] = started
        started, out["approvals"] = await settle(client, started)
        operation = started.get("operation_id")
        if operation and not started.get("refusal"):
            final = await wait_final(client, operation)
            out["final"] = final
            out["greeting_in_output"] = f"w8-{args.tag}" in json.dumps(final)
        return out


STEPS = {
    "signin": step_signin,
    "files": step_files,
    "job": step_job,
    "drop-site": step_drop_site,
    "drop-relay": step_drop_relay,
    "observe": step_observe,
    "guarded": step_guarded,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", required=True)
    parser.add_argument("--tag", default="live")
    parser.add_argument("--operation")
    parser.add_argument("--path")
    parser.add_argument("--ssh-config")
    parser.add_argument("--ssh-host")
    parser.add_argument("--remote-dir")
    parser.add_argument("--package")
    parser.add_argument("--version", default="0.1.0")
    parser.add_argument("--sha256")
    parser.add_argument("step", choices=sorted(STEPS))
    args = parser.parse_args()
    print(json.dumps(asyncio.run(STEPS[args.step](args)), default=str), flush=True)


if __name__ == "__main__":
    main()
