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
BASELINE_TOTAL = 7843

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
    """
    completed = subprocess.run(
        ["uv", "run", "--no-sync", "pyright", "--outputjson"],
        cwd=repo_root,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
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
