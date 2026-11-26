"""A site's JARVIS resource graph: the ``jarvis_graph`` setting, else the site's own.

With the setting, the person confirmed a path and relay exports it on every
JARVIS call. Without it, the site's ``$JARVIS_RESOURCE_GRAPH`` names the graph
the cluster publishes, and relay reads it but never writes it. Only when neither
names a readable graph does relay fall back to one under the CLIO site home,
which collection fills. JARVIS owns the content, and clean keeps a published graph.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import shutil
import subprocess
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


def test_unset_takes_the_sites_variable_before_the_homes_own_graph() -> None:
    assert jarvis_submit.site_graph(HOME) == OWN
    settings = jarvis_submit.site_environment(SPEC, HOME)
    assert settings[jarvis_submit.GRAPH_ENV] == ""
    assert settings[jarvis_submit.GRAPH_OWN_ENV] == OWN
    command = scripts.jarvis_command(HOME)[2]
    assert f"G=''; GO={OWN}" in command
    # The site's own $JARVIS_RESOURCE_GRAPH comes first, from the call or a login shell;
    # the home's graph is only the fallback, and relay never makes the site's directory.
    assert '"${JARVIS_RESOURCE_GRAPH:-}"' in command and "bash -lc" in command
    assert "G=$GO; GS=relay" in command
    # JARVIS (1.9.16) only reads a named graph; its own graph and state stay in relay's home.
    assert 'export JARVIS_ROOT="$R" JARVIS_RESOURCE_GRAPH="$JG" JARVIS_SITE_HOME="$SH"' in command


def test_collect_refuses_the_graph_the_site_publishes() -> None:
    collect = scripts.graph_collect_command(HOME, "slurm")[2]
    assert '[ "$GS" != site ] ||' in collect and "exit 64" in collect
    assert "@@graph_source $GS" in scripts.graph_script(HOME)


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
        assert expected in script
    collect = scripts.graph_collect_command(HOME, "slurm", "", PUBLISHED)[2]
    assert expected in collect and 'mkdir -p "$SH"' in collect
    assert 'JARVIS_RESOURCE_GRAPH="$JG" JARVIS_SITE_HOME="$SH"' in collect
    # Relay's own collection package stays under the site home.
    assert f"S={HOME}/jarvis-cd/site;" in collect


@pytest.mark.skipif(shutil.which("sh") is None or os.name == "nt", reason="POSIX shell")
def test_jarvis_keeps_its_own_graph_in_relays_home_and_never_the_sites(tmp_path: Path) -> None:
    home = tmp_path / "home"
    own = home / "jarvis-cd" / "site" / "resource_graph.yaml"
    published = tmp_path / "cluster" / "resource_graph.yaml"
    published.parent.mkdir(parents=True)
    published.write_text("net: []\n")
    lines = jarvis_submit.graph_lines(str(home), "", shlex.quote)
    report = 'printf "%s|%s|%s|%s\\n" "$GS" "$G" "$JG" "$SH"\n'

    def lookup(site: str, setting: str = "") -> list[str]:
        script = jarvis_submit.graph_lines(str(home), setting, shlex.quote) if setting else lines
        done = subprocess.run(
            ["sh", "-c", script + report],
            env={"PATH": os.environ["PATH"], "HOME": str(tmp_path), "JARVIS_RESOURCE_GRAPH": site},
            capture_output=True,
            text=True,
            check=True,
        )
        return done.stdout.strip().split("|")

    site_home = str(own.parent)
    # The site's graph is exported as it is, and JARVIS's own writes go to relay's home.
    assert lookup(str(published)) == ["site", str(published), str(published), site_home]
    # With none, relay exports no site graph: JARVIS keeps its own in JARVIS_SITE_HOME.
    assert lookup(str(tmp_path / "missing.yaml")) == ["relay", str(own), "", site_home]
    # A named graph inside relay's site home would be JARVIS's own, so it is relay's.
    own.parent.mkdir(parents=True)
    own.write_text("net: []\n")
    assert lookup("", str(own))[0] == "relay"


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
    assert graph["published"] is True and graph["source"] == "setting"
    assert 'G="$HOME"/site-record/resource_graph.yaml' in sent[0]
    assert _ssh(tmp_path / "own").config.jarvis_graph == ""


def test_describe_reports_the_sites_variable_and_offers_no_collection(tmp_path: Path) -> None:
    adapter = _ssh(tmp_path)

    async def ssh(script: str, what: str) -> str:
        return (
            "@@scheduler slurm\n@@graph_path /home/me/.jarvis/resource_graph.yaml\n"
            "@@graph_source site\n@@site_home /home/me/.clio-relay/jarvis-cd/site\n"
        )

    adapter._ssh = ssh  # type: ignore[method-assign]
    graph = asyncio.run(adapter.describe()).facts["resource_graph"]
    assert graph["source"] == "site" and graph["published"] is True
    assert graph["site_home"] == "/home/me/.clio-relay/jarvis-cd/site"
    assert graph["path"] == "/home/me/.jarvis/resource_graph.yaml"
    assert "collect" not in graph
    # Before placement relay's JARVIS cannot read net, and the note says null is not empty.
    assert graph["net"] is None and "not empty" in graph["note"]


def test_describe_offers_collection_only_without_a_site_graph(tmp_path: Path) -> None:
    adapter = _ssh(tmp_path)

    async def ssh(script: str, what: str) -> str:
        return f"@@scheduler slurm\n@@graph_path {OWN}\n@@graph_source relay\n"

    adapter._ssh = ssh  # type: ignore[method-assign]
    graph = asyncio.run(adapter.describe()).facts["resource_graph"]
    assert graph["source"] == "relay" and graph["published"] is False
    assert "collect" in graph


def test_clean_keeps_the_published_graph_whatever_else_goes(tmp_path: Path) -> None:
    keep = site_keeps(HOME, PUBLISHED)
    assert keep[-1] == PUBLISHED
    assert kept(keep, spack=False, decisions=False) == [PUBLISHED]
    assert PUBLISHED not in site_keeps(HOME)
    targets, spared = _ssh(tmp_path, jarvis_graph=PUBLISHED).clean_targets()
    assert targets == [HOME] and PUBLISHED in spared
