"""Deferred handle onto the currently active ``mcp_call.runner`` facade module.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).

Every owner module under ``clio_relay/mcp_call/`` (``server_artifact_identity.py``,
``session_runtime.py``, and friends) needs to call a handful of names --
``_run_mcp_session``, ``_server_artifact_identity``, ``_resolve_executable``,
``_open_process``, ``_file_identity``, ``_install_parent_termination_handlers``,
``_restore_parent_termination_handlers``, ``_server_artifact_digest``,
``_python_console_distribution_identity``, ``_persistent_tool_launcher_shebang``,
and the ``TOOLS_LIST_MAX_*``/``MCP_CALL_MAX_RESPONSE_BYTES`` bounds -- in a way
that observes a *test* monkeypatching that same name directly on the
``clio_relay.mcp_call.runner`` facade module (``monkeypatch.setattr(runner,
"_run_mcp_session", fake)``).

That rules out both of the two ordinary options:

* A plain ``from clio_relay.mcp_call.runner import NAME`` binds a private copy
  in the *caller's own* module globals at import time; monkeypatching the
  facade's copy of the same name never touches that separate binding.
* Resolving the facade by its dotted name (``import clio_relay.mcp_call.runner
  as runner``) does not work here either: ``tests/test_mcp_call_runner.py``
  loads ``runner.py`` with ``importlib.util.spec_from_file_location`` under a
  synthetic module name and never registers it in ``sys.modules`` under
  ``clio_relay.mcp_call.runner`` -- an ordinary import of that dotted path
  would import a second, independent, unpatched execution of the file
  instead of the exact module object the test is holding.

The one thing that reliably identifies "the exact module object the test (or
production caller) is holding" is the module's own ``globals()`` dict: for an
executing module, ``globals()`` *is* ``module.__dict__`` -- the same object
``setattr(module, name, value)`` (what ``monkeypatch.setattr`` does) mutates.
So ``runner.py`` registers its own ``globals()`` here, once, right after its
imports (``register(globals())``); every owner module then reads through the
returned proxy at *call time*, never at import time, so a later monkeypatch on
the registered dict is always visible. This module has zero dependencies on
any other ``clio_relay`` module and lives at the ``clio_relay`` top level (not
nested under ``mcp_call/``) so it resolves the same way in an editable dev
install as every other extracted owner module -- see the sibling owner
modules' docstrings for that constraint.
"""

from __future__ import annotations

from typing import Any

_active_namespace: dict[str, Any] | None = None


def register(namespace: dict[str, Any]) -> None:
    """Record the live ``globals()`` of the currently executing runner facade."""
    global _active_namespace
    _active_namespace = namespace


class _FacadeProxy:
    """Attribute access resolves against the registered facade's live globals."""

    __slots__ = ()

    def __getattr__(self, name: str) -> Any:
        if _active_namespace is None:
            raise RuntimeError(
                "clio_relay.mcp_call.runner has not registered itself with "
                "_mcp_call_runner_facade yet -- it must import this module and "
                "call register(globals()) before any owner module can reach "
                "back through the facade"
            )
        try:
            return _active_namespace[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


_PROXY = _FacadeProxy()


def facade() -> _FacadeProxy:
    """Return a proxy for deferred, monkeypatch-observing facade attribute access."""
    return _PROXY
