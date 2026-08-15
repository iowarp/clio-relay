#!/usr/bin/env python3
"""Bump one or more release-identity axes (iowarp/clio-relay#198, #231 R7).

Rewrites every registered mutable site (`clio_relay.release_pins`'s
`PINSITES` table) for the axes named on the command line, then recomputes
the acceptance-matrix self-digest strictly last (design doc
`docs/design/relay-architecture-2026-08.md` §7's ordering rule). All the
real logic -- reading, writing, and the digest recompute -- lives in
`clio_relay.release_pins`; this script only parses arguments and renders the
per-site diff (the same thin-script pattern `check_release_identity.py`
follows).

Four independent axes, each optional:

* ``--relay-version X.Y.Z`` -- clio-relay's own release version.
* ``--kit-version X.Y.Z --kit-wheel-sha256 HEX`` -- the default *bootstrap*
  install pin (``jarvis_mcp.py``, CI, docs). Both are required together:
  the wheel's SHA-256 for a new version cannot be derived, only supplied
  (the same reason the CI workflow itself pins it as a literal).
* ``--acceptance-kit-version X.Y.Z --acceptance-kit-wheel-sha256 HEX`` --
  the ares *acceptance-policy* pin (``docs/release-gate-1.0.yaml``), a
  DIFFERENT axis from ``--kit-version`` on purpose (clio-relay #190/#199,
  commits 41b912c/eef50b5): it records what a past live ares run actually
  had installed, not "whatever the bootstrap default currently is". Move
  it only after re-running the ares acceptance suite against the new kit
  version and collecting real evidence -- never to just mirror
  ``--kit-version``'s text.
* ``--contract-version vX.Y [--contract-sha256 HEX --contract-wire-sha256
  HEX --contract-artifact-sha256 HEX]`` -- the JARVIS MCP user contract
  revision. The digest arguments are optional: omitting them leaves the
  content-identity value_groups unchanged (reported, never silently
  dropped) while the id-literal sites still move.

The preflight (``check_release_identity.py``) never fails just because the
bootstrap and acceptance-policy kit axes currently differ -- that is
allowed, by design; it reports the difference as an INFO note.

Examples::

    uv run python scripts/bump_release_version.py --dry-run \\
        --kit-version 2.7.3 --kit-wheel-sha256 <hex>

    uv run python scripts/bump_release_version.py \\
        --relay-version 1.6.7
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from clio_relay.release_pins import BumpTargets, PinChange, apply_bump, plan_bump


def _repo_root() -> Path:
    """Return the repository root (parent of the ``scripts`` directory)."""
    return Path(__file__).resolve().parent.parent


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--relay-version", help="New clio-relay release version (e.g. 1.6.7).")
    parser.add_argument(
        "--kit-version", help="New pinned clio-kit distribution version (bootstrap default axis)."
    )
    parser.add_argument(
        "--kit-wheel-sha256", help="SHA-256 of the new clio-kit release wheel (bootstrap axis)."
    )
    parser.add_argument(
        "--acceptance-kit-version",
        help="New clio-kit distribution version for the ares acceptance-policy fixture "
        "(docs/release-gate-1.0.yaml) -- a DIFFERENT axis from --kit-version; move it only "
        "with real re-certification evidence, never to mirror --kit-version.",
    )
    parser.add_argument(
        "--acceptance-kit-wheel-sha256",
        help="SHA-256 of the clio-kit wheel for the ares acceptance-policy fixture.",
    )
    parser.add_argument(
        "--contract-version", help="New JARVIS MCP user contract revision (e.g. v3.8)."
    )
    parser.add_argument("--contract-sha256", help="New contract content SHA-256.")
    parser.add_argument("--contract-wire-sha256", help="New contract canonical tools-wire SHA-256.")
    parser.add_argument("--contract-artifact-sha256", help="New contract bundled artifact SHA-256.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the per-site diff without writing anything.",
    )
    args = parser.parse_args(argv)
    if bool(args.kit_version) != bool(args.kit_wheel_sha256):
        parser.error("--kit-version and --kit-wheel-sha256 must be given together")
    if bool(args.acceptance_kit_version) != bool(args.acceptance_kit_wheel_sha256):
        parser.error(
            "--acceptance-kit-version and --acceptance-kit-wheel-sha256 must be given together"
        )
    if not any(
        (
            args.relay_version,
            args.kit_version,
            args.acceptance_kit_version,
            args.contract_version,
        )
    ):
        parser.error(
            "at least one of --relay-version/--kit-version/--acceptance-kit-version/"
            "--contract-version is required"
        )
    return args


def _targets(args: argparse.Namespace) -> BumpTargets:
    return BumpTargets(
        relay_version=args.relay_version,
        bootstrap_kit_version=args.kit_version,
        bootstrap_kit_wheel_sha256=args.kit_wheel_sha256,
        acceptance_kit_version=args.acceptance_kit_version,
        acceptance_kit_wheel_sha256=args.acceptance_kit_wheel_sha256,
        contract_version=args.contract_version,
        contract_sha256=args.contract_sha256,
        contract_wire_sha256=args.contract_wire_sha256,
        contract_artifact_sha256=args.contract_artifact_sha256,
    )


def _render_change(change: PinChange, *, dry_run: bool) -> str:
    if change.skipped_reason is not None:
        return (
            f"DRIFT: {change.site.id} ({change.site.path}:{change.site.line}): "
            f"{change.skipped_reason}"
        )
    verb = "would change" if dry_run else "changed"
    return (
        f"{verb}: {change.site.id} ({change.site.path}:{change.site.line}): "
        f"{change.old_value!r} -> {change.new_value!r}"
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Return 0 on success, 1 if any registered site drifted."""
    args = _parse_args(argv)
    targets = _targets(args)
    root = _repo_root()
    changes = plan_bump(root, targets) if args.dry_run else apply_bump(root, targets)
    for change in changes:
        print(_render_change(change, dry_run=args.dry_run))
    drifted = [change for change in changes if change.skipped_reason is not None]
    if not changes:
        print("no changes: every targeted site already holds the new value")
    print(f"{len(changes) - len(drifted)} site(s) {'would change' if args.dry_run else 'changed'}")
    if drifted:
        print(f"FAIL: {len(drifted)} registered site(s) drifted -- fix them before bumping")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
