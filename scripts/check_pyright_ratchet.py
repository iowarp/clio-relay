#!/usr/bin/env python3
"""Ratchet guard against the repo's strict-pyright error backlog (clio-relay#270).

``local.pyright`` (:func:`clio_relay.release_validation.run_local_release_validation`)
used to demand a bare ``uv run --no-sync pyright`` exit 0 across the whole
strict-mode tree (``[tool.pyright]`` in ``pyproject.toml``: ``typeCheckingMode
= "strict"``, ``include = ["src", "jarvis-packages", "tests", "scripts"]``).
That was never a real gate: forensics on iowarp/clio-relay#270 traced the
check to commit 93095eb (2026-07-13, "complete production 1.0 relay
contracts"), which predates every 1.x release, and reproduced the same
order-of-magnitude error count (~7900) on the v1.6.7 release tag under the
identical locked toolchain (``uv 0.11.28`` / ``pyright 1.1.411``, both pinned
in ``uv.lock`` at both commits) -- so the strict-mode tree has apparently
never actually reached zero errors under this check. What made the failure
look small was a second, independent bug: the old check captured pyright's
whole plain-text dump into one formatted exception string
(``_run_check``), and GitHub Actions truncates a single log line past
roughly 1MB, silently cutting the middle out instead of reporting it -- a CI
viewer only ever saw the first ~9 and last ~7 errors and had no way to know
thousands more existed in between.

This check replaces the bare exit-0 demand with a REPO-WIDE ERROR-COUNT
ratchet, the same discipline :mod:`check_file_size` and
:mod:`check_no_class_in_function` already apply per-file:

* The total pyright strict-mode error count may not exceed
  :data:`BASELINE_TOTAL`. Exceeding it fails the build.
* At or under the baseline, the check passes. Reaching a NEW LOWER total
  prints an advisory ratchet-down message -- not a failure -- telling the
  committer to lower :data:`BASELINE_TOTAL` in the same change.
* The baseline may only ratchet DOWN. A justified, reviewed increase must
  record why in a comment next to :data:`BASELINE_TOTAL`, the same rule
  :data:`check_file_size.RATCHET_BASELINE` follows per-file.

Structured JSON (``pyright --outputjson``), not the plain-text dump, is
load-bearing here: it is what avoids the 1MB single-line truncation trap
above, and it lets this check keep its own failure output small -- naming
only the most-erroring files -- no matter how large the real diagnostic
count is. Never widen this script to print every diagnostic; that
reintroduces the exact failure mode it exists to fix.

The remaining backlog is tracked in iowarp/clio-relay#271 -- see that issue
for the full error-class breakdown (leading-underscore names imported across
the #231/#775 decomposition's owner-module split, and the
``_ServiceRuntimeStartMixin`` family's sibling-mixin attribute access, which
needs a real typing design, not mechanical renames) and the burn-down
direction.

Run as part of CI (blocking) and locally::

    uv run python scripts/check_pyright_ratchet.py
    uv run python scripts/check_pyright_ratchet.py --baseline 7000
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, NamedTuple

# Repo-wide pyright strict-mode error-count ratchet. Measured on a properly
# LOCKED toolchain (``uv sync --locked --all-groups`` -- uv 0.11.28, pyright
# 1.1.411, Python 3.12.0, all pinned in uv.lock) against
# origin/develop @ fba4cdf (iowarp/clio-relay#270, landing the _register/
# bounded_file_io/clio_kit_wheel_archive public renames and the
# test_validation_report.py writer-lock repoint). This total may only
# ratchet DOWN: when a change fixes errors, lower this number in the same
# change. It must never ratchet up except for a reviewed, justified reason
# recorded here.
#
# clio-relay#265/#259 merge fixup: the #265 outputs_missing verdict and the
# #259 stderr-channel residual added 19 new strict-mode errors (7843 -> 7862
# on CI; 7845 -> 7864 measured locally on Windows, which runs ~2 higher than
# Linux CI for the same tree -- both offsets agree exactly). All 19 traced to
# genuinely new `self.queue`/`self.storage_runtime` access and one
# cross-mixin method-return-type gap in four owner modules
# (JarvisDispatchMixin, JobExecutionMixin, ProgressIngestMixin,
# ResultFinalizationMixin) plus one dict-literal-narrowing and one now-
# redundant cast in files this merge touched. Fixed with real typing (per-
# mixin ``TYPE_CHECKING``-only attribute stubs for the two composing-class
# attributes strict pyright cannot otherwise resolve across a mixin split,
# an explicit ``cast`` at the one cross-mixin method-return call site, an
# explicit ``dict[str, object]`` test annotation, and one dropped
# now-unnecessary cast) -- no ignores, no suppressions. Each stub also
# resolved every OTHER pre-existing `self.queue`/`self.storage_runtime`
# access already in those same four classes, not just the 19 new ones: net
# -182 (7845 -> 7663 on Windows). Verified zero new diagnostics anywhere in
# the repo via a full baseline-vs-fixed diff, not just the total dropping.
#
# clio-relay#209 one-pass cold bootstrap (adversarial-review B3 fix): the
# new files (bootstrap_one_pass_script.py, the cli_cluster_deploy.py/
# bootstrap_pin.py/cli_remote_worker_probe.py target-identity additions,
# the extended test_bootstrap_preflight_transport.py/test_cli_cluster_
# deploy.py suites) landed with explicit annotations on every previously-
# untyped dict literal and lambda strict pyright flagged, plus the
# established per-import private-usage pragma (the reportPrivateUsage
# ignore paired with the SLF001 suppression, as used elsewhere in the
# bootstrap family) for the two cross-module bootstrap-family constants
# this slice's script composer borrows -- net -5 (7663 -> 7658 on
# Windows), not a regression to absorb.
#
# clio-relay#265/#183/#162/#248 "honest verdict" slice, adversarial-review
# fix round: the new cross-mixin `self.provider`/`_refuse_empty_jarvis_
# pipeline` call sites (endpoint_jarvis_dispatch.py/endpoint_job_
# execution.py) added 5 new strict-mode errors (7663 -> 7668) -- fixed with
# the same real-typing precedent as the #265/#259 note above (TYPE_CHECKING
# stubs + a call-site cast, no ignores). While in there, #271's own
# direction was applied to the four private names this slice's new
# jarvis_pipeline_precheck.py imports (`_trusted_jarvis_mcp_result`,
# `_minimal_mcp_runner_environment`, `_endpoint_mcp_runner_command` in
# endpoint_jarvis_recovery.py; `_write_private_json_file` in
# endpoint_recovery_directory.py): every caller across the endpoint
# decomposition already imports them, so the leading underscore was pure
# reportPrivateUsage noise, not a real privacy boundary -- promoted to
# public and every import site (including execution_watch.py, which shares
# all four, and endpoint.py's forward re-exports) repointed. Net: 7663 ->
# 7636, verified via a full baseline-vs-fixed diff.
#
# Third-round adversarial-review verification pass (item 4): the
# jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py blocker fix
# (importing structured_result_from_protocol_result instead of the
# renamed-away _structured_result) plus the two test call-site fixes and
# the new import-smoke guard test removed 5 more reportPrivateUsage-
# adjacent errors than they added. Measured (not hardcoded from the
# reviewer's own prediction, confirmed to match it) via
# `uv run --no-sync python scripts/check_pyright_ratchet.py` from the
# synced worktree venv: 7636 -> 7631.
#
# Post-rebase onto develop (which carries #209's own -5): the two deltas
# compound independently (disjoint files); re-measured on the rebased tree
# at 7626 and ratcheted down accordingly.
#
# clio-relay#278 adversarial-review fix round (D3): the six ``register_*_
# routes(..., auth_dependency: object, ...)`` signatures across http_api_
# routes_{artifacts,jobs,queue,session,events,gateway}.py (two of which --
# session.py, gateway.py -- also carry a ``session_submission_dependency:
# object`` parameter) were the root cause of every "Argument of type
# list[object] cannot be assigned to parameter dependencies of type
# Sequence[Depends] | None" reportArgumentType error on this surface -- 50
# instances total, not just the one #278's own new execution-scoped route
# added (which had been suppressed with a scoped ignore instead of fixing
# the root; the ratchet forbids exactly that kind of one-site accounting).
# Retyped all eight parameters to ``fastapi.params.Depends`` (the real
# runtime type -- every call site already passes ``Depends(...)``); purely
# mechanical, no call-site or behavior changes (verified: create_app()
# still builds and the full http_api/artifact-listing test batteries still
# pass byte-for-byte). Measured via `uv run --no-sync python scripts/
# check_pyright_ratchet.py`: 7626 -> 7574.
BASELINE_TOTAL = 7574

# How many of the most-erroring files to name when the ratchet breaks.
# Pyright's own multi-thousand-line dump is exactly what caused this
# problem (see the module docstring) -- this check must never reproduce
# that mistake, so its own failure output stays small no matter how large
# the real diagnostic count is.
MAX_NAMED_FILES = 10


class Result(NamedTuple):
    """Outcome of one ``pyright --outputjson`` run against the ratchet."""

    total: int
    baseline: int
    top_files: list[tuple[str, int]]  # (repo-relative path, error count), desc by count


def _repo_root() -> Path:
    """Return the repository root (parent of the ``scripts`` directory)."""
    return Path(__file__).resolve().parent.parent


def run_pyright_json(repo_root: Path) -> dict[str, Any]:
    """Run pyright in JSON mode from the repo root and return its parsed report.

    ``--outputjson`` is load-bearing, not cosmetic: the plain-text dump this
    check replaces is what GitHub Actions silently truncated past ~1MB on a
    single log line (see the module docstring). Structured JSON, parsed here
    and reported through :func:`_print_report`'s small, bounded summary,
    sidesteps that failure mode entirely regardless of how many diagnostics
    pyright reports.

    Invocation is DIRECT (``sys.executable -m pyright``), never a nested
    ``uv run``: this script already executes inside the project environment
    (release_validation launches it via ``uv run --no-sync``), and adding a
    third uv layer wedged CI's validate-local for its full 60-minute job
    timeout (job 96602335205, 2026-08-20 — an orphaned uv child at teardown,
    zero output for an hour). The timeout is a generous typed runaway
    backstop for the ONE pyright exchange, never a tuning knob: expiry is a
    loud failure naming the bound, not a silent cancel.
    """
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "pyright", "--outputjson"],
            cwd=repo_root,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=1500,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            "pyright --outputjson exceeded the 1500s runaway backstop for a "
            "single full-repo pass (normal passes complete in minutes); the "
            "check environment is wedged, not merely slow"
        ) from exc
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "pyright --outputjson produced no parseable JSON on stdout "
            f"(exit {completed.returncode}); stderr tail: {completed.stderr[-2000:]}"
        ) from exc


def evaluate(
    report: dict[str, Any],
    *,
    repo_root: Path,
    baseline: int = BASELINE_TOTAL,
) -> Result:
    """Count strict-mode errors in ``report`` and rank the most-erroring files.

    Args:
        report: Parsed ``pyright --outputjson`` output.
        repo_root: Repository root, used to render repo-relative paths in
            the ranked file list.
        baseline: Ratchet baseline to compare the counted total against.

    Returns:
        The counted total, the baseline it was compared to, and up to
        :data:`MAX_NAMED_FILES` (path, count) pairs for the most-erroring
        files, most errors first.

    Raises:
        RuntimeError: If pyright's own reported ``summary.errorCount``
            disagrees with the number of ``severity == "error"`` entries in
            ``generalDiagnostics`` -- a signal the report format changed
            under us and this ratchet should not be trusted blindly.
    """
    diagnostics = report.get("generalDiagnostics", [])
    errors = [diagnostic for diagnostic in diagnostics if diagnostic.get("severity") == "error"]
    total = len(errors)

    summary_count = report.get("summary", {}).get("errorCount")
    if summary_count is not None and summary_count != total:
        raise RuntimeError(
            f"pyright summary.errorCount ({summary_count}) disagrees with the "
            f"counted severity=='error' diagnostics ({total}) -- the report "
            "format may have changed; investigate before trusting this ratchet."
        )

    counts: dict[str, int] = {}
    for diagnostic in errors:
        file_path = diagnostic.get("file", "<unknown>")
        try:
            rel = Path(file_path).resolve().relative_to(repo_root).as_posix()
        except ValueError:
            rel = file_path
        counts[rel] = counts.get(rel, 0) + 1

    top_files = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:MAX_NAMED_FILES]
    return Result(total=total, baseline=baseline, top_files=top_files)


def _print_report(result: Result) -> None:
    """Print the ratchet report. Always small: never one line per diagnostic."""
    if result.total > result.baseline:
        print(
            f"FAIL: pyright strict-mode error count {result.total} exceeds the "
            f"ratchet baseline {result.baseline} (clio-relay#270)."
        )
        print(f"Top {len(result.top_files)} most-erroring file(s):")
        for rel, count in result.top_files:
            print(f"  {count:5d}  {rel}")
        print(
            "If this is a genuine new regression, fix it. If the ratchet itself "
            "needs to move (a justified, reviewed increase), raise BASELINE_TOTAL "
            "in scripts/check_pyright_ratchet.py with a comment recording why."
        )
        return
    if result.total < result.baseline:
        print(
            f"OK (ratchet down): pyright strict-mode error count is {result.total}, "
            f"under the recorded baseline {result.baseline}. Lower BASELINE_TOTAL to "
            f"{result.total} in scripts/check_pyright_ratchet.py in this same change."
        )
        return
    print(
        f"OK: pyright strict-mode error count is {result.total}, at the ratchet "
        "baseline (clio-relay#270; burn-down tracked in the linked issue)."
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Return 0 if the ratchet holds (or ratchets down), 1 if broken."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline",
        type=int,
        default=BASELINE_TOTAL,
        help=f"Ratchet baseline to compare against (default: {BASELINE_TOTAL}).",
    )
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    report = run_pyright_json(repo_root)
    result = evaluate(report, repo_root=repo_root, baseline=args.baseline)
    _print_report(result)
    return 1 if result.total > result.baseline else 0


if __name__ == "__main__":
    sys.exit(main())
