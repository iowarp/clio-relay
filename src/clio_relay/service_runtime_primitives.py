"""Foundational, zero-dependency helpers shared across the service-runtime split.

Extracted from ``service_runtime.py`` (#231 rework slice): the untyped-dict
coercion helpers (``_object``/``_optional_int``/``_optional_str``) used to
narrow JSON/dict record fields, two small transport/config validators
(``_require_server_addr``, ``_frp_proxy_type``), the durable-resource
gateway-binding helper (``_bind_cleanup_resource_to_gateway``), and the
best-effort local process-group rollback helper
(``_terminate_just_started_process_group``). These have no dependency on any
other piece of ``service_runtime.py`` -- every other extracted owner module
(and the supervisor class that remains in ``service_runtime.py``) imports
from here, never the reverse.

Also holds the two module constants used by more than one sibling module:
``_OWNERSHIP_INTENT_SCHEMA`` (the supervisor, its result types, and the
scheduler-contract validators all stamp/check this schema id) and
``_LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS`` (the wall-clock bound shared by
this module's own rollback helper and the connector-identity module's local
cleanup commands).
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from contextlib import suppress
from typing import Literal, cast

from clio_relay.errors import ConfigurationError
from clio_relay.session_wire_models import CleanupResource

_OWNERSHIP_INTENT_SCHEMA = "clio-relay.gateway-ownership-intent.v1"
_LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS = 30.0


def _terminate_just_started_process_group(pid: int) -> None:
    """Best-effort rollback for a process whose durable identity capture failed."""
    if os.name == "nt":
        with suppress(subprocess.TimeoutExpired):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
                timeout=_LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS,
            )
        return
    with suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGTERM)
    time.sleep(0.1)
    with suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGKILL)


def _object(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _bind_cleanup_resource_to_gateway(
    resource: CleanupResource,
    gateway_session_id: str,
) -> CleanupResource:
    """Bind connector cleanup evidence to its exact durable gateway record."""
    return resource.model_copy(
        update={
            "metadata": {
                **resource.metadata,
                "gateway_session_id": gateway_session_id,
            }
        }
    )


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _require_server_addr(server_addr: str, cluster: str) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(f"frp server address is not configured for cluster {cluster}")


def _frp_proxy_type(transport_mode: str) -> Literal["stcp", "xtcp"]:
    normalized = transport_mode.strip().lower().replace("_", "-")
    if normalized in {"frp-stcp", "frp-stcp-wss", "stcp", "relay"}:
        return "stcp"
    if normalized in {"frp-xtcp", "frp-xtcp-wss", "xtcp", "direct", "nat-bypass"}:
        return "xtcp"
    raise ConfigurationError(f"unsupported service runtime transport mode: {transport_mode}")
