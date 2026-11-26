"""The ``files`` verb: small files inline, any file by its local path.

On a guarded site a delete or a replace asks the person first: in the call
when the client can show a form, else as a ``needs_approval`` refusal.
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Any

import mcp_types
from fastmcp import Context
from fastmcp.tools import InputRequiredToolResult, ToolResult

from clio_relay import site_writes
from clio_relay.adapters.hooks import Ask, ask_detail
from clio_relay.jobs.file_rules import CHUNK_BYTES, part_path
from clio_relay.jobs.handles import FileAccess
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.elicit import (
    elicit_inline,
    elicitation_modes,
    input_required,
    modern,
    reads_ask_meta,
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
from clio_relay.surface.files_transfer import download, upload
from clio_relay.surface.service import RelayService

MAX_INLINE_BYTES = CHUNK_BYTES

logger = logging.getLogger(__name__)


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
            f"inline content is limited to {MAX_INLINE_BYTES} bytes; move a larger file "
            "with op upload and its local_path",
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
    local_path: str | None = None


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
    replaced = await _replaces(access, args)
    deleting = await _deleting(service, site, access, args)
    try:
        structured, text = await _dispatch(access, args)
    except Exception:
        if args.op == "upload" and args.path:
            await _left_part(service, site, access, args.path)
        raise
    _note(service, site, args, structured, replaced, deleting)
    if approved is not None and (args.op == "delete" or args.final):
        used(service, approved)
    return structured, text


async def _existing(access: FileAccess, path: str) -> str | None:
    """The site's absolute path for ``path`` when something is there, else None."""
    try:
        return (await access.stat(path)).path
    except Refusal as refusal:
        if refusal.reason is RefusalReason.PATH_NOT_FOUND:
            return None
        raise


async def _replaces(access: FileAccess, args: FilesArgs) -> str | None:
    """The file an overwrite would replace: relay records only what it made."""
    whole = args.op == "upload" or (args.op == "write" and args.final)
    if not (whole and args.overwrite and args.path):
        return None
    return await _existing(access, args.path)


async def _deleting(
    service: RelayService, site: str, access: FileAccess, args: FilesArgs
) -> str | None:
    """The site's path a delete removes, when relay's record of the site has files."""
    if args.op != "delete" or not args.path:
        return None
    if not site_writes.ledger_path(service.paths.state_dir, site).exists():
        return None
    return await _existing(access, args.path)


async def _left_part(service: RelayService, site: str, access: FileAccess, path: str) -> None:
    """Record the part file an upload cut short left, so a site clean finds it."""
    try:
        found = await access.stat(part_path(path))
        site_writes.record(service.paths.state_dir, site, found.path, found.size)
    except (Refusal, OSError):
        logger.info("no part file of %s to record", path, exc_info=True)


def _note(
    service: RelayService,
    site: str,
    args: FilesArgs,
    structured: dict[str, Any],
    replaced: str | None,
    deleting: str | None,
) -> None:
    """Keep relay's record of what it wrote on the site (:mod:`clio_relay.site_writes`)."""
    state_dir = service.paths.state_dir
    try:
        if args.op == "delete" and deleting:
            site_writes.forget(state_dir, site, [deleting])
        elif args.op == "write" and not args.final:
            site_writes.record(state_dir, site, str(structured["path"]), structured.get("size"))
        elif args.op in {"write", "upload"}:
            path = str(structured["path"])
            ours = replaced is None or replaced in site_writes.written(state_dir, site)
            site_writes.finished(state_dir, site, path, structured.get("size"), record_it=ours)
    except OSError:
        logger.warning("recording relay's write on %s", site, exc_info=True)


async def _approved(
    service: RelayService, entry: SiteEntry, access: FileAccess, args: FilesArgs
) -> Ask | None:
    """On a guarded site, the person's yes this op needs, or None when it needs none."""
    if args.op not in {"delete", "write", "upload"} or not args.path:
        return None
    whole = args.op == "upload"
    action = await approval_action(
        entry,
        access,
        "write" if whole else args.op,
        args.path,
        overwrite=args.overwrite,
        offset=0 if whole else args.offset,
        final=True if whole else args.final,
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
            text = (
                f"{len(data)} bytes of binary data, in structuredContent.content_base64. "
                "To save a file on this machine, use op download with local_path instead"
            )
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
    elif op in {"upload", "download"}:
        return await _transfer(access, args)
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


async def _transfer(access: FileAccess, args: FilesArgs) -> tuple[dict[str, Any], str]:
    path = _need(args.path, "path")
    if args.op == "upload":
        moved = await upload(access, args.local_path, path, overwrite=args.overwrite)
        text = f"uploaded {moved['local_path']} to {moved['path']}"
    else:
        moved = await download(access, path, args.local_path, overwrite=args.overwrite)
        text = f"downloaded {moved['path']} to {moved['local_path']}"
    text += f": {moved['size']} bytes, SHA-256 {moved['sha256']} (checked: {moved['verified']})"
    return moved, text


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
                    input_required([ask], ask.ask_id, reads_meta=reads_ask_meta(ctx))
                )
            action, content = response_parts(await elicit_inline(ctx, ask))
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
                "detail": ask_detail(ask),
                "page": service.ask_provider.page_address(ask),
            }
        ],
    )
    return refusal_result(refusal)
