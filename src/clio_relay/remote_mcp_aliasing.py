"""Virtual remote MCP tool alias assignment and collision resolution.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5). This module owns the
naming concern for virtual catalog assembly: turning a candidate's readable
base alias into a bounded, MCP-interoperable tool name
(:func:`_bounded_base_alias`, :func:`_safe_name`), deterministically
resolving collisions between candidates that share one base alias
(:func:`_assign_aliases`, :func:`_collision_alias`, :func:`_alias_with_suffix`),
and the visibility-profile predicate that gates which candidates a caller's
declared profile admits (:func:`_profile_allows`).

Catalog assembly itself (``build_virtual_remote_mcp_catalog`` and its
``_Candidate`` dataclass) stays in ``remote_mcp.py`` for now -- it is the
primary caller of every function here, not a peer concern being split out
in this slice. ``_Candidate`` is referenced only in a type annotation
below (:func:`_assign_aliases`'s ``grouped`` parameter), never at runtime,
so it is imported under ``TYPE_CHECKING`` rather than needing a deferred
function-scope import: with ``from __future__ import annotations`` active,
annotations are lazy strings, and this module never constructs or inspects
a ``_Candidate`` instance's type at runtime, only its already-bound
``base_alias`` attribute (plain duck typing).

None of these six functions have any caller outside remote_mcp.py's own
catalog-assembly code (confirmed by grep across ``src/`` and ``tests/``
before the move), so remote_mcp.py imports them directly with no
re-export needed.
"""

from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clio_relay.cluster_config import RemoteMcpProfile
    from clio_relay.remote_mcp import _Candidate

MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH = 64
MAX_VIRTUAL_REMOTE_MCP_CANDIDATES = 10_000

_SAFE_NAME_PATTERN = re.compile(r"[^a-z0-9_]+")


def _assign_aliases(
    grouped: dict[str, list[_Candidate]],
    *,
    reserved_names: set[str],
) -> dict[str, str]:
    bases: dict[str, list[str]] = {}
    for identity, candidates in grouped.items():
        base = _bounded_base_alias(candidates[0].base_alias)
        bases.setdefault(base, []).append(identity)
    all_bases = set(bases)
    assigned: dict[str, str] = {}
    used = set(reserved_names)
    for base, identities in sorted(bases.items()):
        sorted_identities = sorted(identities)
        if len(sorted_identities) == 1 and base not in used:
            identity = sorted_identities[0]
            assigned[identity] = base
            used.add(base)
            continue
        for identity in sorted_identities:
            alias = _collision_alias(
                base,
                identity,
                blocked=used | all_bases,
            )
            assigned[identity] = alias
            used.add(alias)
    return assigned


def _collision_alias(base: str, identity: str, *, blocked: set[str]) -> str:
    maximum_suffix_length = MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH - len("remote_")
    for length in range(10, min(len(identity), maximum_suffix_length) + 1):
        candidate = _alias_with_suffix(base, identity[:length])
        if candidate not in blocked:
            return candidate
    for nonce in range(1, len(blocked) + MAX_VIRTUAL_REMOTE_MCP_CANDIDATES + 2):
        suffix = hashlib.sha256(f"{identity}\0{nonce}".encode("ascii")).hexdigest()[
            :maximum_suffix_length
        ]
        candidate = f"remote_{suffix}"
        if candidate not in blocked:
            return candidate
    raise ValueError("could not assign a unique bounded remote MCP alias")


def _bounded_base_alias(base: str) -> str:
    """Bound one readable generated alias to the MCP interoperability limit."""
    if len(base) <= MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH:
        return base
    suffix = hashlib.sha256(base.encode("utf-8")).hexdigest()[:10]
    return _alias_with_suffix(base, suffix)


def _alias_with_suffix(base: str, suffix: str) -> str:
    """Append a stable suffix without exceeding the MCP tool-name limit."""
    head_length = MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH - len(suffix) - 1
    if head_length < 1:
        raise ValueError("remote MCP alias suffix leaves no readable prefix")
    head = base[:head_length].rstrip("_")
    if not head:
        head = "remote"[:head_length]
    return f"{head}_{suffix}"


def _profile_allows(profiles: list[RemoteMcpProfile], profile: str) -> bool:
    if profile == "all":
        return True
    normalized = "user" if profile in {"", "agent", "user"} else profile
    return normalized in profiles


def _safe_name(value: str) -> str:
    normalized = _SAFE_NAME_PATTERN.sub("_", value.strip().lower()).strip("_")
    if normalized:
        return normalized
    return f"unnamed_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:10]}"
