"""A worker site describes itself with the ``ssh`` adapter's script and parser.

Without it, a site reached through relay's worker listed only queue names, so
CLIO saw no partitions, limits or resource graph and fell back to raw ``sinfo``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters import local
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.local import LocalAdapter, LocalConfig
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import ExecResult

SECTIONS = (
    "@@scheduler slurm\n@@host login1\n@@cpus 40\n"
    "@@queues\ncompute\ndebug\n"
    "@@sinfo\n"
    "compute*|up|17/5/0/22|2-00:00:00|40|47759|(null)|2|10|2|1-infinite\n"
    "debug|up|4/2/0/6|4-00:00:00|40|47661+|(null)|2|10|2|2-4\n"
    "@@graph_path /home/me/.jarvis/resource_graph.yaml\n@@graph_source site\n"
    "@@nosched\n"
)


def _adapter(tmp_path: Path) -> LocalAdapter:
    state = tmp_path / "state"
    context = AdapterContext(site_id="here", state_dir=state, content=ContentStore(state))
    return LocalAdapter(LocalConfig(scheduler="slurm"), context)


def _answer(monkeypatch: pytest.MonkeyPatch, stdout: str, exit_code: int = 0) -> list[list[str]]:
    calls: list[list[str]] = []

    async def run_bounded(argv: list[str], **_: Any) -> ExecResult:
        calls.append(argv)
        return ExecResult(argv=argv, exit_code=exit_code, stdout=stdout, stderr="", duration_s=0)

    monkeypatch.setattr(local, "POSIX", True)
    monkeypatch.setattr(local, "run_bounded", run_bounded)
    return calls


def test_a_posix_site_reports_partitions_limits_and_its_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _answer(monkeypatch, SECTIONS)
    adapter = _adapter(tmp_path)
    info = asyncio.run(adapter.describe())
    assert calls[0][:2] == ["sh", "-c"] and "@@sinfo" in calls[0][2]
    rows = {partition.name: partition for partition in info.partitions}
    assert set(rows) == {"compute", "debug"}
    assert (rows["compute"].nodes, rows["compute"].min_nodes, rows["compute"].max_nodes) == (
        22,
        1,
        None,
    )
    assert (rows["debug"].min_nodes, rows["debug"].max_nodes) == (2, 4)
    assert info.facts["sched"]["source"] == "sinfo"
    assert info.facts["resource_graph"]["source"] == "site"
    assert info.facts["work_dir"] == str(adapter.work_dir)
    assert info.scheduler == "slurm"


@pytest.mark.parametrize(("stdout", "code"), [("", 0), (SECTIONS, 2)])
def test_a_script_that_cannot_run_falls_back_to_the_queue_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stdout: str, code: int
) -> None:
    calls = _answer(monkeypatch, stdout, code)
    info = asyncio.run(_adapter(tmp_path).describe())
    assert calls[1] == ["sinfo", "-h", "-o", "%R"]
    assert info.partitions == []
