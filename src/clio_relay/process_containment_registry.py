"""Live registry of relay-owned processes shared by spawn and termination.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).
`_OWNED_PROCESSES` and its guards are mutated in place by
`process_containment_spawn` and `process_containment_termination` -- both
import the same dict/lock/set objects defined here rather than copies, so
registration performed by one is immediately visible to the other.
"""

from __future__ import annotations

import threading

from clio_relay.process_containment_types import _OwnedProcessState

_OWNED_PROCESSES: dict[int, _OwnedProcessState] = {}
_OWNED_PROCESSES_LOCK = threading.Lock()
_OWNED_PROCESSES_RELEASING: set[int] = set()


def _register_owned_process(process_id: int, state: _OwnedProcessState) -> None:
    with _OWNED_PROCESSES_LOCK:
        if process_id in _OWNED_PROCESSES:
            raise RuntimeError(f"process containment was already registered: {process_id}")
        _OWNED_PROCESSES[process_id] = state
