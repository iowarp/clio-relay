"""Acceptance evidence for the built-in virtual JARVIS MCP tools.

This module is now a thin facade (clio-relay split/jarvis-mcp-validation):
the evidence-building logic that used to live here directly was moved to a
set of owner modules, one per concern --

* ``jarvis_mcp_validation_core.py`` -- shared JSON/type primitives
  (``JSON``, the ``_UNBOUND_JARVIS_IDENTITY`` sentinel, ``_mapping``,
  ``_check``, ...).
* ``jarvis_mcp_validation_contract.py`` -- local/remote JARVIS MCP tool
  contract validation.
* ``jarvis_mcp_validation_package_search.py`` -- bounded package-discovery
  (``jarvis_describe``) call evidence.
* ``jarvis_mcp_validation_execution_query.py`` -- post-run unified execution
  query (``jarvis_get_execution``) evidence.
* ``jarvis_mcp_validation_progress_semantics.py`` -- one native progress
  event's quantitative/phase semantics.
* ``jarvis_mcp_validation_lifecycle_progress.py`` -- execution-query lifecycle
  and package-progress evidence sampled across repeated queries.
* ``jarvis_mcp_validation_live_progress.py`` -- live native progress
  notification (``jarvis_run``) evidence.
* ``jarvis_mcp_validation_report.py`` -- ``build_jarvis_mcp_validation_report``,
  the top-level orchestrator that assembles every check above into one
  report.

Every symbol below is re-imported under its original name so every existing
``from clio_relay.jarvis_mcp_validation import X`` caller, every
``clio_relay.jarvis_mcp_validation.X`` qualified/monkeypatch access (cli.py,
cli_jarvis_mcp_validate.py), and the private
``_jarvis_query_lifecycle_progress_evidence`` access
``tests/test_jarvis_mcp_validation.py`` makes directly on this module all
keep resolving unchanged -- a pure move, not a behavior change. See each
owner module's own docstring for what it owns.
"""

from __future__ import annotations

from clio_relay.jarvis_mcp_validation_lifecycle_progress import (
    _jarvis_query_lifecycle_progress_evidence,  # noqa: F401
)
from clio_relay.jarvis_mcp_validation_report import (
    build_jarvis_mcp_validation_report,  # noqa: F401
)
