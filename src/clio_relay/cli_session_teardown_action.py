"""Assemble ``session teardown``'s ``action`` callable from its extracted
phases (iowarp/clio-relay#231 continuation, ``cli_session_teardown.py``
split).

The pre-split module's ``action()`` was one nested closure whose first
few lines set up ``remote_execution``/``queue``/the worker-observation
evidence, then ran the whole body inline. :func:`build_teardown_action`
does the same setup (now writing onto the shared
:class:`~clio_relay.cli_session_teardown_state._TeardownState` instead
of closing over local variables) and then simply calls each extracted
phase in the same order the original body executed them:
``cli_session_teardown_recovery`` first (returning early, exactly as
the original's finalized-retry branch did, when it fully handles an
already-closed retry), then ``cli_session_teardown_jobs``, then
``cli_session_teardown_finalize``.
"""

from __future__ import annotations

from collections.abc import Callable

import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.cli_session_teardown_finalize as cli_session_teardown_finalize
import clio_relay.cli_session_teardown_jobs as cli_session_teardown_jobs
import clio_relay.cli_session_teardown_recovery as cli_session_teardown_recovery
import clio_relay.remote_cli as remote_cli
from clio_relay.cli_session_teardown_state import _TeardownState


def build_teardown_action(state: _TeardownState) -> Callable[[], None]:
    """Return the zero-argument ``action`` callable ``guarded_action``/
    ``locked_action``/``cli._run_or_exit`` invoke, composed from the phases
    this split moved into their own modules."""

    def action() -> None:
        # Deferred: ``cli.py`` imports ``cli_session_teardown`` (this
        # action's caller module) at module scope, so a module-scope import
        # of ``cli`` here would cycle back through it.
        import clio_relay.cli as cli

        state.remote_execution = remote_cli.should_execute_on_cluster(state.definition)
        state.queue = cli._managed_queue_from_env()
        state.cleanup_worker_info, state.cleanup_worker_error = (
            cli_remote_worker_attach._observe_worker_before_cleanup(state.definition)
        )
        if cli_session_teardown_recovery._resolve_teardown_recovery(state):
            return
        cli_session_teardown_jobs._run_teardown_jobs_phase(state)
        cli_session_teardown_finalize._run_teardown_finalize_phase(state)

    return action
