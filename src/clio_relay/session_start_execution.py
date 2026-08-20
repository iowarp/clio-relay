"""Owned remote relay session start execution -- facade.

split/session-start-execution-w3 (#231): this module's real content moved to
three owner modules, each a single cohesive concern --

* ``session_start_promotion.py``: the ``_OwnedSessionQueue`` typed
  core-queue surface, the ``_RecoveredStartProbe`` liveness stand-in, and
  ``_promote_resumable_contained_start`` (crash-surviving start promotion
  for an already-running owned API).
* ``session_start_identity_challenge.py``:
  ``execute_owned_session_identity_challenge`` (signs one nonce only after
  pinned metadata and live leader verification).
* ``session_start_execution_core.py``: ``execute_owned_session_start`` (the
  exact cluster-local start path) -- ~910 lines of crash-recovery start
  logic that does not decompose along a clean second seam without
  restructuring the function itself; see that module's own docstring.

This file stays the resident facade, re-exporting every name above under
its original name so no other module's imports change --
session_lifecycle.py still reaches ``execute_owned_session_start`` /
``execute_owned_session_identity_challenge`` via
``from clio_relay.session_start_execution import (...)`` for its own
cli.py-compatibility re-export block, and tests still reach
``session_start_execution._promote_resumable_contained_start`` /
``session_start_execution.sys`` (the shared ``sys`` module singleton, so
patching ``sys.executable`` through this facade's import still affects the
one live read in session_start_execution_core.py) unchanged.
"""

from __future__ import annotations

import sys  # noqa: F401 -- re-exported for `session_start_execution.sys.executable` patch sites

from clio_relay.session_start_execution_core import (
    execute_owned_session_start,  # noqa: F401
)
from clio_relay.session_start_identity_challenge import (
    execute_owned_session_identity_challenge,  # noqa: F401
)
from clio_relay.session_start_promotion import (
    _OwnedSessionQueue,  # noqa: F401
    _promote_resumable_contained_start,  # noqa: F401 -- tests call this directly
    _RecoveredStartProbe,  # noqa: F401
)
