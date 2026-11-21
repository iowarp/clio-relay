#!/usr/bin/env python3
"""Ratchet guard against god-files in the clio_relay source tree.

This check exists to prevent god-files from re-accreting now that the
owner-module decomposition (iowarp/clio-relay#231) is under way. It walks
``src/clio_relay/**/*.py`` and
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
    # Files still over DEFAULT_MAX_LINES, each at its current length. Lower an entry
    # when its file shrinks and drop it once the file is under the limit.
    # Recorded in the relay middle-man refactor: the rework's merges had grown
    # these past the cap with the baseline left empty, so the check already
    # failed at 21fffaa5. Each is held at its length here and may only shrink.
    "src/clio_relay/adapters/local.py": 919,
    "src/clio_relay/adapters/ssh/adapter.py": 1253,
    "src/clio_relay/adapters/worker/adapter.py": 1080,
    "src/clio_relay/frp_link.py": 812,
    "src/clio_relay/jarvis_submit.py": 2076,
    "src/clio_relay/serve/runtime.py": 948,
    "src/clio_relay/surface/server.py": 939,
    "src/clio_relay/surface/service.py": 983,
}

# Roots of the source tree to scan, relative to the repository root. Tests
# (tests/, ...) are intentionally excluded.
SRC_ROOTS: tuple[str, ...] = ("src/clio_relay",)

# ---------------------------------------------------------------------------
# Distribution guard (iowarp/clio-relay#280, v1.7.0 campaign lane R-G G2).
#
# The per-file ratchet above stops INDIVIDUAL files regrowing, but a tree can
# stay "green" while its median file marches toward the cap. Owner ruling:
# 90% of files under 500 lines is the accountability bar. Enforced as a
# RATCHETING baseline per root family: the recorded percentage may only rise
# (toward, then holding at, DISTRIBUTION_TARGET_PERCENT); CI fails when the
# measured percentage drops below its recorded baseline. When a change
# improves the distribution, the advisory tells the committer to raise the
# recorded number in the same change -- identical discipline to
# RATCHET_BASELINE, pointed at the distribution instead of one file.
# ---------------------------------------------------------------------------

#: The sweet-spot bound the distribution is measured against (files strictly
#: under this line count are "healthy"). The 800 hard cap above is the
#: backstop, never a license.
SWEET_SPOT_LINES = 500

#: The standing target the baselines ratchet toward. Not enforced directly --
#: the enforced number is each family's recorded baseline below.
DISTRIBUTION_TARGET_PERCENT = 90.0

#: Recorded percentage of src-family files under SWEET_SPOT_LINES, floored to
#: two decimals at measurement time. May only RISE, except when deleting
#: small files lowers the share without any file growing; record why here.
#: 92.15 (141/153) after the old queue, storage and models layers were deleted.
#: 92.11 (140/152) after the globus and iri fakes moved to tests.
#: 92.81 (142/153) after the conformance kit's target moved to its own module.
#: 93.54 (145/155) after the package catalog and package progress modules.
#: 93.58 (146/156) after the server instructions module.
#: 93.63 (147/157) after the Slurm path patterns module.
#: 89.59 (155/173) recorded in the middle-man refactor: the rework's merges had
#: dropped the share below the floor, so the check already failed at 21fffaa5.
SRC_DISTRIBUTION_BASELINE_PERCENT = 89.59

#: Same guard for tests/. May only RISE.
#: 95.45 (105/110) after the old queue, storage and models tests were deleted.
TESTS_DISTRIBUTION_BASELINE_PERCENT = 95.72

TESTS_ROOTS: tuple[str, ...] = ("tests",)

#: How many of the largest over-sweet-spot files to name in reports, so the
#: next fix target is named, not hunted.
DISTRIBUTION_WORST_FILES = 10


class DistributionResult(NamedTuple):
    """Outcome of one root family's distribution measurement."""

    label: str
    total_files: int
    files_under: int
    percent: float  # exact, un-floored
    baseline_percent: float
    worst: list[tuple[str, int]]  # (rel, line_count), largest first

    @property
    def regressed(self) -> bool:
        """True when the measured percentage fell below the recorded baseline."""
        # The baseline is recorded FLOORED to two decimals, so an unchanged
        # tree always measures >= its own baseline; the epsilon only guards
        # float representation, never masks a real one-file regression (one
        # file crossing the bound moves the percentage by >= 100/total_files,
        # orders of magnitude above it).
        return self.percent + 1e-9 < self.baseline_percent

    @property
    def improved_floor(self) -> float:
        """The floored-to-2-decimals value to record when the tree improved."""
        import math

        return math.floor(self.percent * 100) / 100


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


def check_size_distribution(
    scan_roots: Sequence[Path],
    *,
    label: str,
    rel_to: Path | None = None,
    sweet_spot: int = SWEET_SPOT_LINES,
    baseline_percent: float,
) -> DistributionResult:
    """Measure one root family's percentage of files under ``sweet_spot``.

    Args:
        scan_roots: Directory trees to walk for ``*.py`` files. Must be
            non-empty.
        label: Family name used in reports (``"src"``/``"tests"``).
        rel_to: Base for the relative paths named in the worst-offender list.
        sweet_spot: The healthy line-count bound (strictly under).
        baseline_percent: The recorded ratchet floor for this family.

    Returns:
        The measured :class:`DistributionResult` (judgment is the caller's,
        via :attr:`DistributionResult.regressed`).

    Raises:
        ValueError: If ``scan_roots`` is empty or contains no ``*.py`` files
            (a distribution over nothing is meaningless and would mask a
            mis-pointed scan root as 100% healthy).
    """
    if not scan_roots:
        raise ValueError("check_size_distribution requires at least one scan root")
    base = rel_to if rel_to is not None else scan_roots[0]
    counts: list[tuple[str, int]] = []
    for scan_root in scan_roots:
        for path in sorted(scan_root.rglob("*.py")):
            counts.append((path.relative_to(base).as_posix(), _count_lines(path)))
    if not counts:
        raise ValueError(f"no *.py files under the {label} distribution scan roots")
    files_under = sum(1 for _rel, count in counts if count < sweet_spot)
    worst = sorted(
        (entry for entry in counts if entry[1] >= sweet_spot),
        key=lambda entry: -entry[1],
    )[:DISTRIBUTION_WORST_FILES]
    return DistributionResult(
        label=label,
        total_files=len(counts),
        files_under=files_under,
        percent=files_under / len(counts) * 100,
        baseline_percent=baseline_percent,
        worst=worst,
    )


def _print_distribution_report(result: DistributionResult, *, sweet_spot: int) -> None:
    """Print one family's distribution verdict (regression fails the build)."""
    summary = (
        f"{result.label}: {result.files_under}/{result.total_files} files "
        f"({result.percent:.2f}%) under {sweet_spot} lines "
        f"(ratchet floor {result.baseline_percent:.2f}%, "
        f"target {DISTRIBUTION_TARGET_PERCENT:.0f}%)"
    )
    if result.regressed:
        print(f"FAIL (distribution): {summary}")
        print("  the percentage fell below its recorded floor -- largest offenders:")
        for rel, count in result.worst:
            print(f"  {count:6d} {rel}")
    elif result.improved_floor > result.baseline_percent:
        print(
            f"OK (distribution ratchet up available): {summary} -- raise "
            f"{result.label.upper()}_DISTRIBUTION_BASELINE_PERCENT to "
            f"{result.improved_floor:.2f} in scripts/check_file_size.py in this "
            "same change."
        )
    else:
        print(f"OK (distribution): {summary}")


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

    # #280: the distribution guard runs alongside the per-file ratchet --
    # both must hold.
    distribution_failed = False
    for label, roots, floor in (
        ("src", SRC_ROOTS, SRC_DISTRIBUTION_BASELINE_PERCENT),
        ("tests", TESTS_ROOTS, TESTS_DISTRIBUTION_BASELINE_PERCENT),
    ):
        distribution = check_size_distribution(
            [repo_root / root for root in roots],
            label=label,
            rel_to=repo_root,
            baseline_percent=floor,
        )
        _print_distribution_report(distribution, sweet_spot=SWEET_SPOT_LINES)
        distribution_failed = distribution_failed or distribution.regressed

    return 1 if (result.failures or distribution_failed) else 0


if __name__ == "__main__":
    sys.exit(main())
