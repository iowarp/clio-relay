"""Relay's Streamable HTTP request body limit: room for one full ``files`` chunk.

The MCP SDK refuses request bodies over 4 MiB with a 413, and FastMCP does not
expose the setting. A 4 MiB ``files`` chunk is 5.6 MB once base64 encoded, so
relay raises its own server's limit. The limit is still there: a body over it is
refused before it is parsed.
"""

from __future__ import annotations

from typing import Any

from fastmcp.server import http as fastmcp_http
from mcp.server.transport_security import RequestBodyLimitMiddleware

from clio_relay.jobs.file_rules import CHUNK_BYTES

# A chunk in base64 is 4/3 of its size; the rest is room for the JSON-RPC envelope
# and for text content that JSON escapes.
MAX_HTTP_BODY_BYTES = 2 * CHUNK_BYTES

_Base = fastmcp_http.FastMCPStreamableHTTPSessionManager


class _ChunkSizedSessionManager(_Base):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Read loosely so the check stays: a newer SDK may wrap it differently.
        limit = getattr(self, "asgi_app", None)
        if not isinstance(limit, RequestBodyLimitMiddleware):
            raise RuntimeError("the MCP SDK no longer limits bodies where relay expects it")
        limit.max_body_size = MAX_HTTP_BODY_BYTES
        self.max_request_body_size = MAX_HTTP_BODY_BYTES


def allow_chunk_bodies() -> None:
    """Make FastMCP's HTTP session manager accept a full ``files`` chunk."""
    fastmcp_http.FastMCPStreamableHTTPSessionManager = _ChunkSizedSessionManager
