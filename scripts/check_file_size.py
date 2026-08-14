#!/usr/bin/env python3
"""Ratchet guard against god-files in the clio_relay source tree.

This check exists to prevent god-files from re-accreting now that the
owner-module decomposition (iowarp/clio-relay#231) is under way. It walks
``src/clio_relay/**/*.py`` and ``jarvis-packages/clio_relay/**/*.py`` and
enforces a per-file line-count ratchet:

* A file **not** in :data:`RATCHET_BASELINE` may not exceed
  :data:`DEFAULT_MAX_LINES` -- a brand-new god-file fails the check.
* A file **in** :data:`RATCHET_BASELINE` (a known-oversized module still
  awaiting decomposition) may not exceed its *recorded* line count -- it can
  shrink but never grow past where it is today.
* A :data:`RATCHET_BASELINE` entry that no longer names a file on disk is
  itself a failure: a stale entry silently hides a file that either moved or
  was deleted without cleaning up the ratchet, and would otherwise mask a
  new file quietly reusing the same relative path.

The baseline may only ratchet DOWN. When a file is brought under the cap, or
merely shrinks, the check reports the ratchet-down and the same change that
shrank it updates :data:`RATCHET_BASELINE` (lowering the number, or removing
the entry once the file is under ``DEFAULT_MAX_LINES``). Ratchet-down reports
are advisory: they do not fail the build.

Run as part of CI (blocking) and locally::

    uv run python scripts/check_file_size.py
    uv run python scripts/check_file_size.py --max 600
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

# Default maximum number of lines a single *non-baselined* source module may
# contain. New files must stay under this cap.
DEFAULT_MAX_LINES = 800

# Per-file ratchet baseline: the modules over DEFAULT_MAX_LINES at their
# current line counts, measured against the tree at iowarp/clio-relay#231
# (item 2). This mapping may only ratchet DOWN -- when a file shrinks, lower
# its number here (or drop the entry once it falls under DEFAULT_MAX_LINES)
# in the same change. Paths are relative to the repository root and use
# forward slashes.
RATCHET_BASELINE: dict[str, int] = {
    # #231 R6: +28 net lines -- the T3 record-time head+tail bound (doc §6.4)
    # applied where _write_mcp_result builds the durable result document:
    # a bounded_payload import, the bound_stream_capture call pair, and the
    # two new stdout_truncation/stderr_truncation result fields. No deletion
    # offsets it -- this is genuinely new structure the doc's own §6.4/§6.5
    # ledger names as never having existed before R6, not a fixable
    # regression. A justified, minimal ratchet-up.
    "jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py": 5786,
    "jarvis-packages/clio_relay/clio_relay/process_containment.py": 2678,
    "src/clio_relay/bootstrap.py": 8733,
    "src/clio_relay/bootstrap_journal.py": 1497,
    "src/clio_relay/bootstrap_reconcile.py": 4462,
    # #231 R3: +34 net lines closing the fourth error surface (doc §6.2) --
    # a shared _write_response + _error_document + _error_from_exception
    # replace the bare {"error": message} shape at the two exception-path
    # call sites, with the door_errors import kept function-local to avoid
    # a real import cycle (browser_gateway -> door_errors -> storage_runtime
    # -> core_queue -> browser_gateway). +25 more from the opus re-review's
    # F7+F14: a typed _RequestBodyTooLargeError so the oversize branch gets
    # its own payload_too_large reason instead of blanket configuration_error.
    # A justified, minimal ratchet-up both times.
    "src/clio_relay/browser_gateway.py": 885,
    # #231 R7: +13 net lines -- compute_release_acceptance_matrix_sha256 is
    # extracted from validate_release_acceptance_matrix's inline hashlib.sha256
    # call so release_pins.py's bump command can reuse the exact same digest
    # computation (doc's §7 "derived-digest-with-ordering" rule: never
    # recompute the hash independently). Single owner, ground rule 1 -- a
    # justified, minimal ratchet-up.
    "src/clio_relay/ci_validation.py": 3787,
    # #231 R6 review fixes: +22 net lines -- F6, `job read-artifact` exits 1
    # on a T2 refusal (is_delivery_refusal) instead of a silent 0 alongside
    # a body that says result_available: false; F5, the shared
    # `_decode_artifact_envelope` (four callers) reports a refusal's own
    # message/code instead of the generic "must use base64 encoding". A
    # justified, minimal ratchet-up.
    # #231 R7: +20 net lines -- `release preflight`, one thin command
    # registration delegating entirely to `release_pins.run_preflight`/
    # `render_preflight` (ground rule 2: cli.py parses and renders only). A
    # justified, minimal ratchet-up.
    # #231 R8(i): -18 net lines -- the monkeypatch-seam rework (doc §4.6/§9)
    # converts every collaborator symbol cli.py imported by bare name into a
    # module-attribute call site (`import clio_relay.X as X`, `X.symbol(...)`
    # instead of `from clio_relay.X import symbol`), so tests patch the
    # symbol where it is looked up (the owner module) and survive future
    # command-module extraction. Net negative: the added `import ... as ...`
    # lines are outweighed by the removed multi-line `from ... import (...)`
    # blocks they replace. A ratchet-down.
    "src/clio_relay/cli.py": 19335,
    # #231 R5: +16 net lines -- FrpTransportConfig gains proxy_name +
    # identity_anchor (the §8.3 typed opt-in frp_transport.py's build_transport
    # refusal reads) plus the IdentityAnchor type alias and its docstring. No
    # deletion offsets it: these are two new, real config fields, not a fixable
    # regression.
    "src/clio_relay/cluster_config.py": 1863,
    "src/clio_relay/core_queue.py": 16137,
    "src/clio_relay/deployment.py": 1243,
    # #231 R6 review fixes: +9 net lines -- F4, `_write_recovered_jarvis_
    # run_result`'s `recovered_document` now nulls `stdout_truncation`/
    # `stderr_truncation` alongside the blanked `stdout`/`stderr`, instead
    # of inheriting a stale populated record from the spread source
    # document. A justified, minimal ratchet-up.
    "src/clio_relay/endpoint.py": 8719,
    "src/clio_relay/fastmcp_server.py": 1212,
    # #231 R3: +24 net lines (door_errors import + the ONE global
    # Exception-handler function + its registration) -- deliberately not
    # offset by deleting any of the 107 existing HTTPException sites, which
    # the same slice's design doc explicitly keeps in place (§6.2). +35 more
    # from the opus re-review's F5/F15: a logger + a hardcoded fallback
    # document so the handler survives door_errors itself failing, plus the
    # corrected build_middleware_stack docstring. A justified, minimal
    # ratchet-up rather than a same-file deletion this slice does not own.
    # #231 R6 review fixes: +29 more -- F2, `GET /artifacts/{id}/content`
    # routes an over-budget read through door_errors' existing
    # payload_too_large door (413) instead of answering 200 with a body
    # that merely says result_available: false. A justified, minimal
    # ratchet-up.
    "src/clio_relay/http_api.py": 3151,
    "src/clio_relay/input_staging.py": 814,
    "src/clio_relay/installation.py": 3718,
    "src/clio_relay/jarvis_execution.py": 875,
    "src/clio_relay/jarvis_mcp.py": 947,
    "src/clio_relay/jarvis_mcp_validation.py": 2671,
    # #231 R6 review fixes: +11 net lines -- F5, `_load_source` checks
    # is_delivery_refusal on the source envelope FIRST and raises the
    # refusal's own message/code, instead of the generic "is not a base64
    # envelope" that misdescribes why the artifact is unavailable. A
    # justified, minimal ratchet-up.
    "src/clio_relay/jarvis_service_runtime.py": 1169,
    # #231 R6 review fixes: +20 net lines -- F5, both `_verify_completed_
    # job`'s inline artifact/provenance checks and the new shared
    # `_delivery_refusal_error` helper now recognize a T2 refusal by its
    # own message/code before falling into the generic "not base64
    # encoded" complaint. A justified, minimal ratchet-up.
    "src/clio_relay/live_acceptance.py": 5447,
    # #231 R6: +10 net lines -- _verified_local_mcp_result now checks
    # bounded_payload.is_delivery_refusal on the envelope
    # relay_ops.read_artifact_bytes returns, so a T2 refusal (the durable
    # mcp_result artifact itself over MAX_ARTIFACT_CONTENT_BYTES) surfaces
    # as-is instead of falling into _decode_verified_mcp_result's generic
    # malformed-envelope ValueError. R6 review fixes: +13 more (5930 ->
    # 5943) -- F1, _mcp_tool_result_failed discriminates on the refusal
    # shape (is_delivery_refusal + delivery.status) instead of one named
    # code, via a new shared _delivery_refusal_failed helper; F5, the same
    # helper is also checked against the tool's own top-level result (not
    # only nested under mcp_result), covering relay_read_artifact reading a
    # too-large artifact directly; F7, _bounded_mcp_result's inline failure
    # dict migrated onto bounded_payload.build_delivery_refusal, retiring
    # the local MCP_RESULT_DELIVERY_SCHEMA constant (single owner, the
    # slice's own rule). Each pass justified, minimal.
    "src/clio_relay/mcp_server.py": 5943,
    "src/clio_relay/mcp_stdio_validation.py": 1269,
    "src/clio_relay/models.py": 2296,
    "src/clio_relay/process_containment.py": 2678,
    "src/clio_relay/queue_management.py": 1671,
    "src/clio_relay/queue_validation.py": 1530,
    # #231 R5: +28 net lines -- an `identity_anchor` property (derived from
    # cluster config, independent of link state, §8.3) plus stamping it on
    # every `channel_event(...)` call site (9) and surfacing it in
    # `event_report()`/`_retired_report()`. No logic here is rewritten --
    # `_verify_bootstrap`'s own checks are untouched -- only this wiring is
    # new, so nothing in the file was a candidate for deletion first.
    # R5 opus review fix set: +52 more (1006 -> 1058) -- `build_transport`
    # moves inside `_establish`'s try (R2, a typed refusal must reach the
    # ledger as `establish_failed`, not propagate with a dangling
    # `establishing`), `authorization_required` gates on
    # `transport.requires_user_authorization` instead of the connection-level
    # setting (R7), `identity_anchor` prefers the held link's own snapshot
    # over live config (R9), `close()` reads back a residual
    # `config_cleanup_error` (R3), plus anchor-aware wording corrections
    # (R12). Real behavioral fixes from a security-relevant review, not a
    # deletable regression.
    "src/clio_relay/remote_connection.py": 1058,
    # #231 R6 review fixes: +11 net lines -- F5,
    # `_control_query_discovery_artifact_bytes` checks is_delivery_refusal
    # on the envelope FIRST and raises the refusal's own message/code,
    # instead of the generic "encoding is unsupported" that misdescribes
    # why the artifact is unavailable. A justified, minimal ratchet-up.
    "src/clio_relay/remote_mcp.py": 5319,
    "src/clio_relay/retention.py": 944,
    "src/clio_relay/runtime_metadata.py": 1749,
    "src/clio_relay/scheduler_providers.py": 1153,
    "src/clio_relay/service_runtime.py": 10163,
    "src/clio_relay/session_lifecycle.py": 8326,
    "src/clio_relay/spool.py": 964,
    "src/clio_relay/storage_policy.py": 1826,
    "src/clio_relay/storage_runtime.py": 1111,
    # #231 R4: local-visitor spawn/health/cleanup delegates to the new
    # frp_link.py substrate (HeldFrpVisitor) instead of duplicating it;
    # run_frp_http_probe collapses into a thin proxy_type="stcp" wrapper
    # around _run_frp_http_probe_with_proxy_type. -100 net (1849 -> 1749).
    # +46 more from the R4 opus review fix set (F2/F3/F9): a residual-
    # secret-config-file resource in the cleanup ledger (F3, a real new
    # resource kind, not fixable by deletion), the visitor-failure-message
    # prefix helper (F2), and threading `visitor` through as Optional so a
    # spawn failure still reaches cleanup (F9). Evaluated ground rule 5
    # (remove/redesign first) and rejected -- this is the review's required
    # safety/diagnostic behavior, not accreted duplication. 1749 -> 1795.
    "src/clio_relay/transport_probe.py": 1795,
    "src/clio_relay/validation_report.py": 5458,
}

# Roots of the source tree to scan, relative to the repository root. Tests
# (tests/, jarvis-packages/clio_relay/*/tests, ...) are intentionally excluded.
SRC_ROOTS: tuple[str, ...] = ("src/clio_relay", "jarvis-packages/clio_relay")


class Failure(NamedTuple):
    """A file (or baseline entry) that breaks the ratchet (fails the check)."""

    rel: str
    # Named line_count, not count: NamedTuple is a tuple subclass and a field
    # literally named "count" overrides tuple.count() with an incompatible
    # type under strict type checking (reportIncompatibleMethodOverride).
    line_count: int
    kind: str  # "new" (non-baselined over cap), "regressed" (over recorded), or "stale"
    limit: int  # the cap broken (new/regressed), or the last recorded count (stale)


class RatchetDown(NamedTuple):
    """A baselined file that shrank -- advisory, not a failure."""

    rel: str
    line_count: int  # see the line_count comment on Failure above
    baseline: int
    under_cap: bool  # True once line_count <= max_lines (drop the entry entirely)


class Result(NamedTuple):
    """Outcome of a scan: failures fail the build, ratchet_downs are advisory."""

    failures: list[Failure]
    ratchet_downs: list[RatchetDown]


def _repo_root() -> Path:
    """Return the repository root (parent of the ``scripts`` directory)."""
    return Path(__file__).resolve().parent.parent


def _count_lines(path: Path) -> int:
    """Return the number of lines in ``path``."""
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return sum(1 for _ in handle)


def check_file_size(
    scan_roots: Sequence[Path],
    *,
    rel_to: Path | None = None,
    max_lines: int = DEFAULT_MAX_LINES,
    baseline: dict[str, int] | None = None,
) -> Result:
    """Evaluate the per-file line-count ratchet under ``scan_roots``.

    Args:
        scan_roots: Directory trees to walk for ``*.py`` files. Must be
            non-empty.
        rel_to: Base directory used to compute the forward-slash relative
            path that keys into ``baseline``. Defaults to ``scan_roots[0]``.
        max_lines: Cap applied to files not present in ``baseline``.
        baseline: Per-file recorded line counts. Defaults to
            :data:`RATCHET_BASELINE`.

    Returns:
        A :class:`Result` splitting build-failing offenders (including stale
        baseline entries) from advisory ratchet-down reports.

    Raises:
        ValueError: If ``scan_roots`` is empty.
    """
    if not scan_roots:
        raise ValueError("check_file_size requires at least one scan root")
    if baseline is None:
        baseline = RATCHET_BASELINE
    base = rel_to if rel_to is not None else scan_roots[0]

    failures: list[Failure] = []
    ratchet_downs: list[RatchetDown] = []
    seen: set[str] = set()
    for scan_root in scan_roots:
        for path in sorted(scan_root.rglob("*.py")):
            rel = path.relative_to(base).as_posix()
            seen.add(rel)
            count = _count_lines(path)
            recorded = baseline.get(rel)
            if recorded is None:
                if count > max_lines:
                    failures.append(Failure(rel, count, "new", max_lines))
                continue
            if count > recorded:
                failures.append(Failure(rel, count, "regressed", recorded))
            elif count < recorded:
                ratchet_downs.append(
                    RatchetDown(rel, count, recorded, under_cap=count <= max_lines)
                )

    for rel in sorted(baseline):
        if rel not in seen:
            failures.append(Failure(rel, 0, "stale", baseline[rel]))

    failures.sort(key=lambda entry: entry.rel)
    return Result(failures=failures, ratchet_downs=ratchet_downs)


def _print_report(result: Result, max_lines: int) -> None:
    """Print the ratchet report (failures then advisory ratchet-downs)."""
    for entry in result.ratchet_downs:
        if entry.under_cap:
            print(
                f"OK (ratchet down): {entry.rel} is now {entry.line_count} lines "
                f"(<= {max_lines}) -- remove it from RATCHET_BASELINE in "
                "scripts/check_file_size.py."
            )
        else:
            print(
                f"OK (ratchet down): {entry.rel} shrank {entry.baseline} -> "
                f"{entry.line_count} -- lower its RATCHET_BASELINE entry to "
                f"{entry.line_count} in scripts/check_file_size.py."
            )

    if not result.failures:
        roots = " and ".join(SRC_ROOTS)
        print(
            f"OK: no file under {roots} exceeds its ratchet baseline "
            f"(cap {max_lines} for new files)."
        )
        return

    print(f"FAIL: {len(result.failures)} file(s) break the size ratchet (#231):")
    for entry in result.failures:
        if entry.kind == "new":
            print(f"  {entry.rel}:{entry.line_count} (new file exceeds cap {entry.limit})")
        elif entry.kind == "regressed":
            print(
                f"  {entry.rel}:{entry.line_count} (regressed past recorded baseline {entry.limit})"
            )
        else:
            print(
                f"  {entry.rel} (stale RATCHET_BASELINE entry: recorded {entry.limit} lines "
                "but this path no longer exists; remove it from RATCHET_BASELINE in "
                "scripts/check_file_size.py)"
            )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Return 0 if the ratchet holds, 1 on any failure."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max",
        type=int,
        default=DEFAULT_MAX_LINES,
        help=f"Cap for non-baselined files (default: {DEFAULT_MAX_LINES}).",
    )
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    result = check_file_size(
        [repo_root / root for root in SRC_ROOTS],
        rel_to=repo_root,
        max_lines=args.max,
    )
    _print_report(result, args.max)
    return 1 if result.failures else 0


if __name__ == "__main__":
    sys.exit(main())
