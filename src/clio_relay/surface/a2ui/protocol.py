"""What a client says about A2UI, and how relay wraps A2UI messages (HS1, HS2).

A client declares A2UI with the ``org.a2ui/a2ui`` extension. At ``2026-07-28``
the declaration travels in each request's ``_meta``; at ``2025-11-25`` it is in
the session's capabilities. Its settings name the protocol versions and, per
version, the catalog ids the client renders. Relay sends A2UI only to a client
that renders relay's catalog or the Basic catalog it is built from.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, cast

import mcp_types
from fastmcp import Context

EXTENSION_ID = "org.a2ui/a2ui"
MIME_TYPE = "application/a2ui+json"
RELAY_CATALOG_ID = "https://iowarp.ai/a2ui/catalogs/clio-relay-compute/v1"
BASIC_CATALOG_ID = "https://a2ui.org/specification/v0_9/catalogs/basic/catalog.json"
VERSIONS = ("v0.9.1", "v0.9")

Message = dict[str, Any]


@dataclass(frozen=True)
class A2UIClient:
    """The A2UI a client renders: the message version and the catalog relay uses."""

    version: str
    catalog_id: str


def _settings(ctx: Context) -> dict[str, Any] | None:
    try:
        found = ctx.client_extension_settings(EXTENSION_ID)
    except (AttributeError, RuntimeError):
        found = None
    if found is not None:
        return dict(found)
    try:
        capabilities = ctx.session.client_capabilities
    except (AttributeError, RuntimeError):
        return None
    extensions: Any = getattr(capabilities, "extensions", None) if capabilities else None
    if not isinstance(extensions, dict):
        return None
    legacy: Any = cast(dict[str, Any], extensions).get(EXTENSION_ID)
    return cast(dict[str, Any], legacy) if isinstance(legacy, dict) else None


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str)]  # pyright: ignore[reportUnknownVariableType]


def a2ui_client(ctx: Context | None) -> A2UIClient | None:
    """Return what the requesting client renders, or None when it renders no A2UI relay sends."""
    if ctx is None:
        return None
    settings = _settings(ctx)
    if settings is None:
        return None
    offered = _strings(settings.get("versions")) or list(VERSIONS)
    capabilities: Any = settings.get("clientCapabilities") or {}
    if not isinstance(capabilities, dict):
        capabilities = {}
    per_versions = cast(dict[str, Any], capabilities)
    for version in VERSIONS:
        if version not in offered:
            continue
        per_version: Any = per_versions.get(version)
        catalogs = _strings(
            cast(dict[str, Any], per_version).get("supportedCatalogIds")
            if isinstance(per_version, dict)
            else None
        )
        for catalog_id in (RELAY_CATALOG_ID, BASIC_CATALOG_ID):
            if catalog_id in catalogs:
                return A2UIClient(version=version, catalog_id=catalog_id)
    return None


def embedded(uri: str, messages: list[Message]) -> mcp_types.EmbeddedResource:
    """Wrap A2UI messages as the embedded resource a tool result carries (HS1)."""
    return mcp_types.EmbeddedResource(
        type="resource",
        resource=mcp_types.TextResourceContents(
            uri=uri, mime_type=MIME_TYPE, text=json.dumps(messages)
        ),
    )
