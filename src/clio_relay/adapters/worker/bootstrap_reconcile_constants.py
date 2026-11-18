"""Shared schema identifiers and platform primitives for bootstrap reconciliation.

Every ``bootstrap_reconcile_*`` owner module reads these constants. Keeping
them in one leaf module with no ``clio_relay`` imports of its own is what
keeps the whole split's import graph acyclic (iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import cast

BOOTSTRAP_DESIRED_STATE_SCHEMA = "clio-relay.bootstrap-desired-state.v1"
BOOTSTRAP_RECEIPT_SCHEMA = "clio-relay.bootstrap-receipt.v2"
BOOTSTRAP_TRANSACTION_SCHEMA = "clio-relay.bootstrap-transaction.v1"
MANAGED_JARVIS_REPO_PATH = "~/.local/share/clio-relay/clio_relay"
LEGACY_MANAGED_JARVIS_REPO_PATH = "~/.local/share/clio-relay/managed-jarvis-repo"
MAX_JARVIS_CONFIG_BYTES = 1024 * 1024
MAX_JARVIS_REPOS_BYTES = 4 * 1024 * 1024
MAX_JARVIS_GRAPH_BYTES = 64 * 1024 * 1024
MAX_JARVIS_DISTRIBUTION_METADATA_BYTES = 1024 * 1024
MAX_JARVIS_DISTRIBUTION_RECORD_BYTES = 64 * 1024 * 1024
BOOTSTRAP_LOCK_TIMEOUT_SECONDS = 30.0
_O_BINARY = cast(int, getattr(os, "O_BINARY", 0))
_O_NOFOLLOW = cast(int, getattr(os, "O_NOFOLLOW", 0))
_FCHMOD = cast(
    Callable[[int, int], None] | None,
    getattr(os, "fchmod", None),  # noqa: B009 - absent from Windows typing/runtime
)
_GETUID = cast(Callable[[], int] | None, getattr(os, "getuid", None))
_AT_FDCWD = -100
_RENAME_EXCHANGE = 2
