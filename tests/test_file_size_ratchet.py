"""Tests for the per-file line-count ratchet (iowarp/clio-relay#231, item 2).

Proves the guard fails when a non-baselined file exceeds the cap, a baselined
file grows past its recorded count, or a baseline entry no longer names a
real file; passes when every file is at or under its limit; and reports a
ratchet-down (without failing) when a baselined file shrinks. Every synthetic
case runs against a throwaway ``tmp_path`` tree through the checker's pure
``check_file_size`` entry point -- no repository file is ever mutated. The
real source tree is separately pinned at its recorded baseline so drift
fails CI.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_file_size import (
    DEFAULT_MAX_LINES,
    RATCHET_BASELINE,
    SRC_ROOTS,
    _print_report,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    check_file_size,
)


def _write(tree: Path, rel: str, lines: int) -> Path:
    """Write a ``rel`` module under ``tree`` with ``lines`` newline-terminated lines."""
    path = tree / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x = 1\n" * lines, encoding="utf-8")
    return path


def test_a_new_oversized_module_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A non-baselined file over the cap fails, naming the exact file and cap.

    This is the sabotage twin: a checker that silently accepted an 801-line
    new file, or that reported the wrong file/cap, would pass a looser
    assertion but must fail this exact one.
    """
    _write(tmp_path, "big.py", 801)

    result = check_file_size([tmp_path], max_lines=800, baseline={})

    assert result.failures == [("big.py", 801, "new", 800)]
    assert not result.ratchet_downs

    _print_report(result, 800)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    out = capsys.readouterr().out
    assert "big.py:801 (new file exceeds cap 800)" in out


def test_a_baselined_module_may_not_regrow(tmp_path: Path) -> None:
    """A baselined file that grew past its recorded count regresses and fails."""
    _write(tmp_path, "known.py", 1201)

    result = check_file_size([tmp_path], max_lines=800, baseline={"known.py": 1200})

    assert result.failures == [("known.py", 1201, "regressed", 1200)]
    assert not result.ratchet_downs


def test_a_baselined_module_at_its_recorded_count_passes(tmp_path: Path) -> None:
    """A baselined file exactly at its recorded count does not offend."""
    _write(tmp_path, "known.py", 1200)

    result = check_file_size([tmp_path], max_lines=800, baseline={"known.py": 1200})

    assert not result.failures
    assert not result.ratchet_downs


def test_a_shrunken_module_reports_a_ratchet_down_without_failing(tmp_path: Path) -> None:
    """A baselined file that shrank is advisory (exit clean), not a failure."""
    _write(tmp_path, "known.py", 1000)

    result = check_file_size([tmp_path], max_lines=800, baseline={"known.py": 1200})

    assert not result.failures
    assert result.ratchet_downs == [("known.py", 1000, 1200, False)]

    # Still over the default cap, so the entry stays -- just at a lower number.
    assert result.ratchet_downs[0].under_cap is False


def test_a_shrunken_module_that_drops_under_the_cap_reports_removal(tmp_path: Path) -> None:
    """A baselined file that fell under the cap should be dropped from baseline."""
    _write(tmp_path, "known.py", 500)

    result = check_file_size([tmp_path], max_lines=800, baseline={"known.py": 1200})

    assert not result.failures
    assert len(result.ratchet_downs) == 1
    assert result.ratchet_downs[0].under_cap is True


def test_every_baseline_entry_names_an_existing_file(tmp_path: Path) -> None:
    """A baseline entry whose file no longer exists on disk fails the check.

    A stale entry silently hides a moved/deleted file and would mask a new
    file quietly reusing the same relative path, so it must fail loudly
    rather than degrade into a mere advisory.
    """
    _write(tmp_path, "present.py", 10)

    result = check_file_size([tmp_path], max_lines=800, baseline={"gone.py": 900})

    assert result.failures == [("gone.py", 0, "stale", 900)]
    assert not result.ratchet_downs


def test_a_clean_tree_reports_neither_failures_nor_ratchet_downs(tmp_path: Path) -> None:
    """A tree with nothing baselined and nothing over cap is entirely quiet."""
    _write(tmp_path, "small.py", 5)

    result = check_file_size([tmp_path], max_lines=800, baseline={})

    assert not result.failures
    assert not result.ratchet_downs


def test_check_scans_every_declared_root(tmp_path: Path) -> None:
    """Passing multiple scan roots finds an offender in either one."""
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _write(first, "ok.py", 5)
    _write(second, "offender.py", 900)

    result = check_file_size([first, second], rel_to=tmp_path, max_lines=800, baseline={})

    assert [f.rel for f in result.failures] == ["second/offender.py"]


def test_real_tree_holds_at_recorded_baseline() -> None:
    """The live source tree passes at the checked-in baseline (regression pin)."""
    repo_root = Path(__file__).resolve().parents[1]
    result = check_file_size(
        [repo_root / root for root in SRC_ROOTS],
        rel_to=repo_root,
    )
    assert not result.failures, [f._asdict() for f in result.failures]


def test_baseline_entries_all_exist_in_the_real_tree() -> None:
    """Every baselined path in the real tree must point at a real file."""
    repo_root = Path(__file__).resolve().parents[1]
    missing = [rel for rel in RATCHET_BASELINE if not (repo_root / rel).is_file()]
    assert not missing, missing


def test_default_max_lines_matches_the_documented_cap() -> None:
    """Guard against the cap silently drifting away from the documented 800."""
    assert DEFAULT_MAX_LINES == 800
