"""A site's published JARVIS resource graph (the ``jarvis_graph`` setting).

The person confirms the site publishes a graph, and its path is recorded in the
site settings: relay names nothing itself. With it set, every relay JARVIS call
exports that graph, describe returns it as the site's graph, and clean keeps it.
JARVIS owns its content. Unset, the graph stays under the CLIO site home.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from pydantic import ValidationError

from clio_relay import jarvis_submit
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.local import LocalConfig
from clio_relay.adapters.ssh import SshAdapter, SshConfig, scripts
from clio_relay.adapters.worker.adapter import WorkerConfig
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.spec import JobSpec
from clio_relay.site_prune import kept, site_keeps

HOME = "/home/me/.clio-relay"
PUBLISHED = "~/site-record/resource_graph.yaml"
OWN = f"{HOME}/jarvis-cd/site/resource_graph.yaml"
SPEC = JobSpec.model_validate(
    {"package": {"name": "demo.hello", "version": "1", "sha256": "b" * 64}}
)


def _ssh(tmp_path: Path, **config: object) -> SshAdapter:
    settings = SshConfig.model_validate({"ssh_profile": "example", "remote_dir": HOME, **config})
    context = AdapterContext(
        site_id="unit", state_dir=tmp_path / "state", content=ContentStore(tmp_path / "cas")
    )
    return SshAdapter(settings, context)


def test_the_setting_is_a_path_on_the_site_or_empty() -> None:
    assert jarvis_submit.published_graph("") == ""
    assert jarvis_submit.published_graph(" /shared/graph.yaml ") == "/shared/graph.yaml"
    assert jarvis_submit.published_graph(PUBLISHED) == PUBLISHED
    with pytest.raises(ValueError, match="absolute or ~/"):
        jarvis_submit.published_graph("graph.yaml")
    for config in (SshConfig, LocalConfig, WorkerConfig):
        assert "jarvis_graph" in config.model_fields
    with pytest.raises(ValidationError):
        SshConfig.model_validate({"ssh_profile": "x", "jarvis_graph": "relative.yaml"})


def test_unset_keeps_the_homes_own_graph() -> None:
    assert jarvis_submit.site_graph(HOME) == OWN
    assert jarvis_submit.site_environment(SPEC, HOME)[jarvis_submit.GRAPH_ENV] == OWN
    assert f"G={OWN}" in scripts.jarvis_command(HOME)[2]


def test_every_jarvis_call_exports_the_published_graph() -> None:
    settings = jarvis_submit.site_environment(SPEC, HOME, PUBLISHED)
    assert settings[jarvis_submit.GRAPH_ENV] == PUBLISHED
    # The decisions stay the home's own: only the graph is the site's.
    assert settings[jarvis_submit.DECISIONS_ENV] == jarvis_submit.site_decisions(HOME)
    command = jarvis_submit.submit_command(
        SPEC, operation_id="op_1", jarvis="", home=HOME, graph=PUBLISHED
    )
    assert f"{jarvis_submit.GRAPH_ENV}={PUBLISHED}" in command
    expected = 'G="$HOME"/site-record/resource_graph.yaml'
    for script in (
        scripts.jarvis_command(HOME, "", PUBLISHED)[2],
        scripts.sched_script(HOME, "", PUBLISHED),
        scripts.graph_script(HOME, "", PUBLISHED),
        scripts.describe_script("slurm", HOME, "", PUBLISHED),
        scripts.exec_script(["jarvis", "rg", "facts", "show"], HOME, "", PUBLISHED),
    ):
        assert expected in script and OWN not in script
    collect = scripts.graph_collect_command(HOME, "slurm", "", PUBLISHED)[2]
    assert expected in collect and 'mkdir -p "$(dirname "$G")"' in collect
    # Relay's own collection package stays under the site home.
    assert f"S={HOME}/jarvis-cd/site;" in collect


def test_describe_returns_the_published_graph_as_the_sites(tmp_path: Path) -> None:
    adapter = _ssh(tmp_path, jarvis_graph=PUBLISHED)
    sent: list[str] = []

    async def ssh(script: str, what: str) -> str:
        sent.append(script)
        return "@@scheduler slurm\n@@graph_path /home/me/site-record/resource_graph.yaml\n"

    adapter._ssh = ssh  # type: ignore[method-assign]
    info = asyncio.run(adapter.describe())
    graph = info.facts["resource_graph"]
    assert graph["path"] == "/home/me/site-record/resource_graph.yaml"
    assert graph["published"] is True
    assert 'G="$HOME"/site-record/resource_graph.yaml' in sent[0]
    assert _ssh(tmp_path / "own").config.jarvis_graph == ""


def test_clean_keeps_the_published_graph_whatever_else_goes(tmp_path: Path) -> None:
    keep = site_keeps(HOME, PUBLISHED)
    assert keep[-1] == PUBLISHED
    assert kept(keep, spack=False, decisions=False) == [PUBLISHED]
    assert PUBLISHED not in site_keeps(HOME)
    targets, spared = _ssh(tmp_path, jarvis_graph=PUBLISHED).clean_targets()
    assert targets == [HOME] and PUBLISHED in spared
