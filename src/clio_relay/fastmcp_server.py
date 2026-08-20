"""Native FastMCP server and relay-backed SEP-2663 task projection.

This module is the thin composition root the rest of the codebase and the
test suite import (``clio_relay.fastmcp_server``). Every real concern lives
in an owner module (iowarp/clio-relay#231 decomposition):

- ``mcp_task_projection.py`` -- SEP-2663 status/timing constants and the
  relay state-map projection.
- ``mcp_agent_input_guard.py`` -- the post-admission agent input elicitation
  guard.
- ``mcp_session_state_codec.py`` -- the compatibility MCP session state
  (de)serializer.
- ``mcp_task_runtime.py`` -- ``RelayMcpRuntime``, the queue/profile/task-
  projection operations for one MCP server.
- ``mcp_tool_provider.py`` -- ``RelayTool``/``RelayToolProvider``, the
  FastMCP tool/provider wiring.
- ``mcp_tasks_extension.py`` -- ``RelayTasksExtension``, the SEP-2663 wire
  adapter.

Owner-module re-exports below. Each extracted concern is re-imported here
under its original name so every existing
``from clio_relay.fastmcp_server import X`` caller and every
``clio_relay.fastmcp_server.X`` qualified/monkeypatch access keeps resolving
unchanged -- a pure move, not a behavior change. See each owner module's own
docstring for what it owns.

**Patch-seam note.** ``call_mcp_tool``, ``mcp_tool_definitions_and_remote_
catalog``, ``status_mcp_job``, and ``wait_mcp_job`` are imported here for
real (not merely re-exported): every owner module above reaches them through
a function-local ``import clio_relay.fastmcp_server as fastmcp_server`` at
its own call site rather than importing the four names directly, so that the
existing test suite's ``monkeypatch.setattr(fastmcp_server_module,
"call_mcp_tool", ...)``-style patches (test_fastmcp_server.py) keep
intercepting the exact call sites they did before this module became a
facade. Do not remove these four imports even though nothing in this file's
own body calls them directly -- they are the patch target, not dead code.
"""

from __future__ import annotations

import hmac
import logging
from typing import Any

from fastmcp import FastMCP
from fastmcp.server.auth import AccessToken, AuthProvider, TokenVerifier

from clio_relay import __version__
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.mcp_agent_input_guard import (
    _AGENT_INPUT_KEY,  # noqa: F401
    _AGENT_INPUT_REQUEST_STATE_SCHEMA,  # noqa: F401
    _AGENT_TASK_TOOL_NAMES,  # noqa: F401
    _MAX_AGENT_INPUT_MESSAGE_BYTES,  # noqa: F401
    _agent_input_enabled,  # noqa: F401
    _agent_input_request_state,  # noqa: F401
    _post_admission_agent_input_guard,  # noqa: F401
    _requests_document,  # noqa: F401
)
from clio_relay.mcp_server import (
    McpSessionState,  # noqa: F401
    call_mcp_tool,  # noqa: F401 -- monkeypatch seam, see module docstring
    mcp_tool_definitions_and_remote_catalog,  # noqa: F401 -- monkeypatch seam
    mcp_tool_result_failed,  # noqa: F401
    normalize_mcp_profile,  # noqa: F401
    serialize_mcp_tool_result,  # noqa: F401
    static_mcp_tool_names,  # noqa: F401
    status_mcp_job,  # noqa: F401 -- monkeypatch seam, see module docstring
    wait_mcp_job,  # noqa: F401 -- monkeypatch seam, see module docstring
)
from clio_relay.mcp_session_state_codec import (
    SESSION_STATE_KEY,  # noqa: F401
    _load_session,  # noqa: F401
    _save_session,  # noqa: F401
    _session_from_json,  # noqa: F401
    _session_to_json,  # noqa: F401
)
from clio_relay.mcp_task_projection import (
    _RELAY_STATE_PROJECTIONS,  # noqa: F401
    RELAY_STATE_MAP,  # noqa: F401
    TASK_POLL_INTERVAL_MS,  # noqa: F401
    TASK_TTL_MS,  # noqa: F401
    McpTaskStatus,  # noqa: F401
    RelayStateMapRow,  # noqa: F401
    _relay_state_projection,  # noqa: F401
)
from clio_relay.mcp_task_runtime import (
    MAX_TASK_ARGUMENT_BYTES,  # noqa: F401
    RelayMcpRuntime,
    _call_tool_result_document,  # noqa: F401
)
from clio_relay.mcp_tasks_extension import RelayTasksExtension
from clio_relay.mcp_tool_provider import (
    RelayTool,  # noqa: F401
    RelayToolProvider,
    _definitions_with_revision,  # noqa: F401
    _task_capable_tool_name,  # noqa: F401
)

logger = logging.getLogger(__name__)

JSON = dict[str, Any]


class RelayBearerTokenVerifier(TokenVerifier):
    """Constant-time verifier for the existing clio-relay API bearer token."""

    def __init__(self, token: str, *, base_url: str) -> None:
        super().__init__(base_url=base_url, resource_base_url=base_url)
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        """Accept only the configured relay API token."""
        if not hmac.compare_digest(token, self._token):
            return None
        return AccessToken(
            token=token,
            client_id="clio-relay-mcp",
            scopes=[],
            subject="clio-relay",
        )


def create_fastmcp_server(
    *,
    settings: RelaySettings | None = None,
    profile: str = "user",
    queue: ClioCoreQueue | None = None,
    http_base_url: str | None = None,
) -> FastMCP[dict[str, Any]]:
    """Create the native FastMCP relay server without a second task backend."""
    resolved = settings or RelaySettings.from_env()
    runtime = RelayMcpRuntime(settings=resolved, profile=profile, queue=queue)
    auth: AuthProvider | None = None
    if http_base_url is not None:
        if resolved.api_token is None:
            raise ValueError("CLIO_RELAY_API_TOKEN is required for MCP HTTP transport")
        auth = RelayBearerTokenVerifier(resolved.api_token, base_url=http_base_url)
    server: FastMCP[dict[str, Any]] = FastMCP(
        "clio-relay",
        version=__version__,
        instructions=(
            "Durable relay operations. Long-running virtual remote and JARVIS tools "
            "may be returned through io.modelcontextprotocol/tasks. Relay jobs remain "
            "the sole execution and cancellation authority."
        ),
        providers=[RelayToolProvider(runtime)],
        lifespan=runtime.lifespan,
        auth=auth,
        tasks=False,
        strict_input_validation=True,
    )
    server.add_extension(RelayTasksExtension(runtime))
    return server


def run_fastmcp_stdio(
    *,
    settings: RelaySettings | None = None,
    profile: str = "user",
) -> None:
    """Run the native FastMCP server over stdio."""
    create_fastmcp_server(settings=settings, profile=profile).run(
        transport="stdio",
        show_banner=False,
    )


def run_fastmcp_http(
    *,
    settings: RelaySettings | None = None,
    profile: str = "user",
    host: str = "127.0.0.1",
    port: int = 8766,
    path: str = "/mcp",
) -> None:
    """Run authenticated Streamable HTTP with the existing relay API token."""
    normalized_path = "/" + path.strip("/")
    public_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    base_url = f"http://{public_host}:{port}{normalized_path}"
    create_fastmcp_server(
        settings=settings,
        profile=profile,
        http_base_url=base_url,
    ).run(
        transport="http",
        host=host,
        port=port,
        path=normalized_path,
        show_banner=False,
    )
