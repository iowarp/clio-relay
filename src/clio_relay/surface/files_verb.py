"""The ``files`` verb: small files inline, larger ones staged by digest."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

from clio_relay.jobs.file_rules import CHUNK_BYTES
from clio_relay.jobs.refusals import Refusal, RefusalReason
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
    entry = service.site(site)
    adapter = service.adapter_for(entry, "files")
    access = await adapter.files()
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

