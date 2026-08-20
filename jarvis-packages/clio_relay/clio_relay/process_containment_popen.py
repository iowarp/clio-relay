"""Popen keyword-argument helpers marking and detecting relay-owned groups.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).
None of these names are individually replaced by the test suite, so callers
in other owner modules import them directly rather than through the facade.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from typing import Any

from clio_relay.process_containment_types import CONTAINMENT_ENV, CONTAINMENT_VALUE


def owner_environment(environment: Mapping[str, str] | None) -> dict[str, str]:
    """Return an execution environment marking one relay-owned process tree."""
    owned = dict(os.environ if environment is None else environment)
    owned[CONTAINMENT_ENV] = CONTAINMENT_VALUE
    return owned


def owner_popen_kwargs() -> dict[str, Any]:
    """Return platform flags that create the outer relay-owned process group."""
    return {
        "start_new_session": os.name != "nt",
        "creationflags": (
            int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
        ),
    }


def nested_popen_kwargs(environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Keep embedded processes in the relay group, or own a group when standalone."""
    source = os.environ if environment is None else environment
    if source.get(CONTAINMENT_ENV) == CONTAINMENT_VALUE:
        return {"start_new_session": False, "creationflags": 0}
    return owner_popen_kwargs()


def inherited_relay_containment(environment: Mapping[str, str] | None = None) -> bool:
    """Return whether the current embedded runner belongs to a relay-owned group."""
    source = os.environ if environment is None else environment
    return source.get(CONTAINMENT_ENV) == CONTAINMENT_VALUE
