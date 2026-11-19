"""The ``files`` verb: small files inline, larger ones in chunks.

On a guarded site a delete or a replace asks the person first: in the call
when the client can show a form, else as a ``needs_approval`` refusal.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

import mcp_types
from fastmcp import Context
from fastmcp.tools import InputRequiredToolResult, ToolResult

from clio_relay.adapters.hooks import Ask
from clio_relay.jobs.file_rules import CHUNK_BYTES
from clio_relay.jobs.handles import FileAccess
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.elicit import (
    elicit_request,
    elicitation_modes,
    modern,
    refusal_result,
    response_parts,
)
from clio_relay.surface.files_approval import (
    FILES_KIND,
    NeedsPerson,
    approval_action,
    approve,
    used,
)
from clio_relay.surface.service import RelayService

MAX_INLINE_BYTES = CHUNK_BYTES


def _need(value: str | None, name: str) -> str:
    if not value:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"this op needs {name}")
    return value


def _payload(content: str | None, content_base64: str | None) -> bytes:
    if (content is None) == (content_base64 is None):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS, "give exactly one of content or content_base64"
        )
    if content is not None:
        data = content.encode("utf-8")
    else:
        try:
            data = base64.b64decode(content_base64 or "", validate=True)
        except ValueError as exc:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "content_base64 is not base64") from exc
    if len(data) > MAX_INLINE_BYTES:
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"inline content is limited to {MAX_INLINE_BYTES} bytes; send larger files "
            "in chunks with offset and final",
        )
    return data


@dataclass(frozen=True)
class FilesArgs:
    """The ``files`` tool's arguments."""

    op: str
    path: str | None = None
    content: str | None = None
    content_base64: str | None = None
    offset: int = 0
    limit: int | None = None
    overwrite: bool = False
    final: bool = True
    recursive: bool = False
    digest: str | None = None


async def files_call(
    service: RelayService, site: str, args: FilesArgs
) -> tuple[dict[str, Any], str]:
    """Do one files op and return its structured result and text. Refuses on error."""
    if args.path and args.path.startswith("relay://"):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            "relay:// names a relay resource, not a file on the site; describe(site=...) "
            "returns the site's guide as skill.text",
            path=args.path,
        )
    entry = service.site(site)
    adapter = service.adapter_for(entry, "files")
    access = await adapter.files()
    approved = await _approved(service, entry, access, args)
    structured, text = await _dispatch(access, args)
    if approved is not None and (args.op == "delete" or args.final):
        used(service, approved)
    return structured, text


async def _approved(
    service: RelayService, entry: SiteEntry, access: FileAccess, args: FilesArgs
) -> Ask | None:
    """On a guarded site, the person's yes this op needs, or None when it needs none."""
    if args.op not in {"delete", "write"} or not args.path:
        return None
    action = await approval_action(
        entry,
        access,
        args.op,
        args.path,
        overwrite=args.overwrite,
        offset=args.offset,
        final=args.final,
        recursive=args.recursive,
    )
    if action is None:
        return None
    return await approve(service, entry, args.op, args.path, action, recursive=args.recursive)


async def _dispatch(access: FileAccess, args: FilesArgs) -> tuple[dict[str, Any], str]:
    op, path, offset = args.op, args.path, args.offset
    structured: dict[str, Any]
    if op == "list":
        listing = await access.list(path or "")
        structured = {"entries": [item.model_dump(mode="json") for item in listing]}
        text = "\n".join(f"{item.kind[0]} {item.size or 0:>12} {item.path}" for item in listing)
    elif op == "stat":
        stat = await access.stat(_need(path, "path"))
        structured = stat.model_dump(mode="json")
        text = json.dumps(structured, indent=2, sort_keys=True, default=str)
    elif op == "read":
        cap = min(args.limit or MAX_INLINE_BYTES, MAX_INLINE_BYTES)
        data = await access.read(_need(path, "path"), offset=offset, limit=cap)
        structured = {"path": path, "offset": offset, "size": len(data)}
        try:
            text = data.decode("utf-8")
            structured["text"] = text
        except UnicodeDecodeError:
            structured["content_base64"] = base64.b64encode(data).decode("ascii")
            text = f"{len(data)} bytes of binary data, in structuredContent.content_base64"
    elif op == "write":
        data = _payload(args.content, args.content_base64)
        written = await access.write(
            _need(path, "path"), data, overwrite=args.overwrite, offset=offset, final=args.final
        )
        structured = written.model_dump(mode="json")
        if args.final:
            text = f"wrote {written.size} bytes to {written.path}"
        else:
            structured["resume_at"] = written.size
            text = (
                f"{written.path} holds {written.size} bytes; send the next chunk at offset "
                f"{written.size}, and the last one with final"
            )
    elif op == "stage":
        data = _payload(args.content, args.content_base64)
        staged = await access.stage(data)
        structured = {"sha256": staged, "size": len(data)}
        text = f"staged {len(data)} bytes as {staged}"
    elif op == "has":
        present = await access.has(_need(args.digest, "digest"))
        structured = {"sha256": args.digest, "present": present}
        text = "present" if present else "absent"
    elif op == "delete":
        target = _need(path, "path")
        await access.delete(target, recursive=args.recursive)
        structured = {"path": target, "deleted": True}
        text = f"deleted {target}"
    else:
        raise Refusal(
            RefusalReason.VERB_NOT_IMPLEMENTED, f"files op {op!r} is not available yet", op=op
        )
    return structured, text


async def files_tool(
    service: RelayService, ctx: Context, site: str, args: FilesArgs
) -> ToolResult | InputRequiredToolResult:
    """The ``files`` tool: one files op, with the person's yes when the site needs it."""
    try:
        if ctx.request_state and ctx.input_responses is not None:
            _answer(service, site, args, ctx.request_state, ctx.input_responses)
        try:
            structured, text = await files_call(service, site, args)
        except NeedsPerson as waiting:
            ask = waiting.ask
            if "form" not in elicitation_modes(ctx):
                return _needs_person(service, ask)
            if modern(ctx):
                return InputRequiredToolResult(
                    mcp_types.InputRequiredResult(
                        input_requests={ask.ask_id: elicit_request(ask)},
                        request_state=ask.ask_id,
                    )
                )
            response = await ctx.session.elicit_form(
                message=ask.message,
                requested_schema=ask.requested_schema or {"type": "object", "properties": {}},  # pyright: ignore[reportArgumentType]
            )
            action, content = response_parts(response)
            service.answer(ask.ask_id, action, content, by="elicitation")
            try:
                structured, text = await files_call(service, site, args)
            except NeedsPerson as still:
                return _needs_person(service, still.ask)
    except Refusal as refusal:
        return refusal_result(refusal)
    return ToolResult(
        content=[mcp_types.TextContent(type="text", text=text)],
        structured_content=structured,
    )


def _answer(
    service: RelayService, site: str, args: FilesArgs, ask_id: str, responses: dict[str, Any]
) -> None:
    """Record the answer a retried files call carries, for this site and path only."""
    ask = service.asks.get(ask_id)
    if ask is None or ask_id not in responses:
        return
    bound = ask.binding
    if bound.get("kind") != FILES_KIND or ask.site != site or bound.get("path") != args.path:
        return
    action, content = response_parts(responses[ask_id])
    service.answer(ask_id, action, content, by="input_required")


def _needs_person(service: RelayService, ask: Ask) -> ToolResult:
    refusal = Refusal(
        RefusalReason.NEEDS_APPROVAL,
        "the site is guarded, so relay asks the person before it deletes or replaces "
        "a file. Open the page, or run `clio-relay asks answer`, then call files "
        "again with the same arguments",
        ask_id=ask.ask_id,
        asks=[
            {
                "ask_id": ask.ask_id,
                "kind": ask.kind,
                "message": ask.message,
                "page": service.ask_provider.page_address(ask),
            }
        ],
    )
    return refusal_result(refusal)
