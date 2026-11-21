"""The site's scheduler facts, as JARVIS reports them (``jarvis sched info +json``).

Relay runs no scheduler reads of its own for ``describe``: JARVIS's
``jarvis.sched-info.v1`` document has the partitions and their limits, the MPI
candidates an install would offer, the resource graph's network, the site
skills and what is missing (``issues``). It is light, so it runs on the login
host. This module turns it into :class:`~clio_relay.adapters.base.PartitionInfo`
for the guard and ``describe``.
"""

from __future__ import annotations

import json
import re
from typing import Any, cast

from clio_relay.adapters.base import PartitionInfo

SCHEMA = "jarvis.sched-info.v1"


def report(text: str) -> dict[str, Any] | None:
    """Return the ``jarvis.sched-info.v1`` document in ``text``, or None."""
    decoder = json.JSONDecoder()
    for match in reversed(list(re.finditer(r"(?m)^\s*\{", text))):
        try:
            found, _end = decoder.raw_decode(text[match.start() :].lstrip())
        except ValueError:
            continue
        if isinstance(found, dict) and cast(dict[str, Any], found).get("schema_version") == SCHEMA:
            return cast(dict[str, Any], found)
    return None


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _names(value: object) -> list[str]:
    if isinstance(value, str):
        return [item for item in value.split(",") if item and item.lower() not in ("(null)", "n/a")]
    if isinstance(value, list):
        return [str(item) for item in cast(list[object], value) if item]
    return []


def partitions(document: dict[str, Any] | None) -> list[PartitionInfo]:
    """Return the partitions JARVIS reported, one entry each."""
    listed: object = (document or {}).get("partitions")
    if not isinstance(listed, list):
        return []
    found: list[PartitionInfo] = []
    for raw in cast(list[object], listed):
        if not isinstance(raw, dict):
            continue
        entry = cast(dict[str, Any], raw)
        name = entry.get("partition") or entry.get("name")
        if not isinstance(name, str) or not name:
            continue
        max_time = entry.get("max_time")
        found.append(
            PartitionInfo(
                name=name,
                default=bool(entry.get("default")),
                nodes=_int(entry.get("total_nodes")) or 0,
                cpus_per_node=_int(entry.get("cpus_per_node")),
                memory_mb=_int(entry.get("memory_mb_per_node")),
                max_time=str(max_time) if max_time is not None else None,
                max_time_s=_int(entry.get("max_time_s")),
                allocated=_int(entry.get("allocated_nodes")) or 0,
                idle=_int(entry.get("idle_nodes")) or 0,
                other=_int(entry.get("other_nodes")) or 0,
                gres=_names(entry.get("gres")),
            )
        )
    return found


def site_facts(document: dict[str, Any] | None) -> dict[str, Any]:
    """Return what ``describe`` shows of the document besides the partitions."""
    if not document:
        return {}
    keys = ("provider", "providers_supported", "account", "in_allocation", "observed_at")
    facts: dict[str, Any] = {key: document.get(key) for key in keys if key in document}
    facts["site_skills"] = document.get("site_skills") or []
    facts["issues"] = document.get("issues") or []
    return facts
