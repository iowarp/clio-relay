#!/usr/bin/env python3
"""Fast release-identity preflight (iowarp/clio-relay#198, #231 R7).

Verifies every registered release-identity pin (`clio_relay.release_pins`'s
`PINSITES` table) currently agrees with the rest of its value_group, and
that no unregistered site pins the same value elsewhere in the tree. This is
the "seconds, not the full local-release battery" check the design doc
(`docs/design/relay-architecture-2026-08.md` §7) names as R7's target: a
version-identity mismatch that today only surfaces after a multi-minute
`release validate-local` run (or worse, in CI) fails here immediately.

All the real logic lives in `clio_relay.release_pins` -- this script only
locates the repository root, calls the preflight, and renders the report
(the same thin-script pattern as `check_file_size.py` /
`check_no_class_in_function.py`).

Run as part of CI (blocking) and locally::

    uv run python scripts/check_release_identity.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from clio_relay.release_pins import render_preflight, run_preflight


def _repo_root() -> Path:
    """Return the repository root (parent of the ``scripts`` directory)."""
    return Path(__file__).resolve().parent.parent


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Return 0 if the preflight passes, 1 on any failure."""
    del argv
    result = run_preflight(_repo_root())
    for line in render_preflight(result):
        print(line)
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
