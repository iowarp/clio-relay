"""Full-mode jarvis-venv staging over an existing managed environment (#254).

Owner module for the staging semantics a full-mode ``cluster bootstrap``
needs when ``~/.local/share/clio-relay/jarvis-venv`` already exists (built
by an earlier successful full bootstrap). The historical guard refused
outright, promising "a staged generation" precondition nothing implemented
-- the relay could never redeploy itself onto an already-bootstrapped host.

The fix: build and verify the replacement environment at a path this
transaction owns (never touching the live one), then promote it with one
atomic pathname exchange (``renameat2(RENAME_EXCHANGE)``, via
``bootstrap_reconcile._atomic_exchange_paths``) inside the same fenced
activation window as the rest of the generation. An exchange swaps two
existing pathnames in a single kernel operation, so at every instant either
observable before or after it the live pathname names a complete
environment -- old or new, never absent or half-cleared. The pre-exchange
content is then given a distinct ``.retired-<timestamp>`` name (a plain
rename of the now-vacated staged pathname, which does not touch the live
name at all) and kept, never deleted.
"""

from __future__ import annotations

import os
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    _atomic_exchange_paths,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.errors import ConfigurationError

JARVIS_VENV_STAGED_OWNED_NAME = "jarvis_venv_staged"
_LIVE_DIR_NAME = "jarvis-venv"


def _relay_state_dir(home: Path | None) -> Path:
    lexical_home = Path(str(home or Path.home())).expanduser().absolute()
    return lexical_home / ".local/share/clio-relay"


def staged_jarvis_venv_path(*, home: Path | None = None, invocation_id: str) -> Path:
    """Return the per-invocation staging path this transaction owns."""
    return _relay_state_dir(home) / f"{_LIVE_DIR_NAME}.staging-{invocation_id}"


def retired_jarvis_venv_path(*, home: Path | None = None, retired_at: str) -> Path:
    """Return the timestamped retire target for a superseded jarvis-venv."""
    return _relay_state_dir(home) / f"{_LIVE_DIR_NAME}.retired-{retired_at}"


def jarvis_venv_staging_plan(
    *, home: Path | None = None, invocation_id: str
) -> dict[str, str] | None:
    """Return the staged jarvis-venv plan for a full-mode reconcile, or None.

    None means the virgin path applies unchanged: no managed jarvis-venv
    exists yet, so full-mode reconcile builds it directly at its live name
    as it always has. A managed jarvis-venv existing means full-mode
    reconcile must build+verify its replacement at a path this transaction
    owns instead of refusing (clio-relay#254) -- this plan names that path.
    """
    live = _relay_state_dir(home) / _LIVE_DIR_NAME
    if not live.exists() and not live.is_symlink():
        return None
    if live.is_symlink() or not live.is_dir():
        raise ConfigurationError("existing jarvis execution environment is not one owned directory")
    return {
        "schema_version": "clio-relay.jarvis-venv-staging-plan.v1",
        "live": str(live),
        "staged": str(staged_jarvis_venv_path(home=home, invocation_id=invocation_id)),
    }


def promote_staged_jarvis_venv(
    *,
    home: Path | None = None,
    invocation_id: str,
    retired_at: str,
    staged_identity: tuple[int, int],
) -> dict[str, object]:
    """Promote a built+verified staged jarvis-venv to live, retiring the old one.

    ``staged_identity`` is the (device, inode) pair the ``mkdir-owned``
    journal action recorded for ``jarvis_venv_staged`` the moment it was
    created -- see ``owned_paths`` on the transaction journal (clio-relay
    #247's recovery reasons from the same journal). Idempotent for crash
    recovery: it proves which pathname currently holds the staged content.
    If the live directory already carries that identity, a prior call's
    exchange is proven complete and only the retire rename is finished --
    the exchange is never re-attempted against an already-promoted pair,
    which would silently swap the live environment back to the retired one.
    """
    base = _relay_state_dir(home)
    live = base / _LIVE_DIR_NAME
    staged = staged_jarvis_venv_path(home=home, invocation_id=invocation_id)
    retired = retired_jarvis_venv_path(home=home, retired_at=retired_at)
    if retired.exists() or retired.is_symlink():
        raise ConfigurationError(f"jarvis-venv retire target already exists: {retired}")
    if live.is_symlink() or not live.is_dir():
        raise ConfigurationError("live jarvis-venv is not one owned directory")
    live_details = live.lstat()
    if (live_details.st_dev, live_details.st_ino) != staged_identity:
        if staged.is_symlink() or not staged.is_dir():
            raise ConfigurationError(f"staged jarvis-venv is unavailable for promotion: {staged}")
        staged_details = staged.lstat()
        if (staged_details.st_dev, staged_details.st_ino) != staged_identity:
            raise ConfigurationError("staged jarvis-venv identity changed before promotion")
        _atomic_exchange_paths(live, staged)
        live_details = live.lstat()
        if (live_details.st_dev, live_details.st_ino) != staged_identity:
            raise ConfigurationError("jarvis-venv exchange did not promote the staged copy")
    # The exchange is now proven complete (just now, or by an earlier
    # interrupted call). `staged`, if still present, names the pre-swap
    # (old, retirement-bound) content -- promoting it never touches `live`.
    if staged.exists() or staged.is_symlink():
        os.rename(staged, retired)
    return {
        "schema_version": "clio-relay.jarvis-venv-promotion.v1",
        "live": str(live),
        "retired": str(retired),
    }
