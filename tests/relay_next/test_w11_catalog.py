"""The Polaris, Aurora and DeltaAI catalog entries and their skills (RL4, RL34)."""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay import catalog
from clio_relay.adapters.globus.adapter import GlobusConfig
from clio_relay.surface.skills import skill_link


@pytest.mark.parametrize("site_id", ["polaris", "aurora", "deltaai"])
def test_entry_is_guarded_with_a_skill_and_a_source(site_id: str) -> None:
    entry = catalog.site(site_id)
    assert entry.adapter == "globus"
    assert entry.identity == "globus"
    assert entry.policy == "guarded"
    assert entry.spend.ask == "always"
    assert entry.source is not None and entry.source.startswith("https://docs.")
    assert entry.skill is None or entry.skill in catalog.skills()
    assert entry.to_site().id == site_id


@pytest.mark.parametrize("site_id", ["ares", "polaris"])
def test_relay_ships_no_cluster_facts_for_a_site(site_id: str) -> None:
    entry = catalog.site(site_id)
    assert entry.skill is None
    assert entry.id != "ares" or entry.defaults.queue is None


@pytest.mark.parametrize("site_id", ["polaris", "deltaai"])
def test_facility_endpoint_entries_are_valid_globus_settings(site_id: str) -> None:
    config = GlobusConfig.model_validate(catalog.site(site_id).config)
    assert config.endpoint
    assert config.queues


def test_aurora_waits_for_the_persons_own_endpoint() -> None:
    config = catalog.site("aurora").config
    assert "endpoint" not in config
    GlobusConfig.model_validate({**config, "endpoint": "00000000-0000-4000-8000-000000000000"})


def test_polaris_alternatives_name_iri_and_worker() -> None:
    entry = catalog.site("polaris")
    assert list(entry.alternatives) == ["iri", "worker"]
    assert entry.alternatives["iri"]["base_url"] == "https://api.alcf.anl.gov"


@pytest.mark.parametrize("site_id", ["ares", "polaris"])
def test_a_site_added_while_the_catalog_linked_a_skill_keeps_no_broken_link(
    site_id: str, tmp_path: Path
) -> None:
    entry = catalog.site(site_id).to_site()
    retired = "ares" if site_id == "ares" else "alcf-polaris"
    assert retired in catalog.RETIRED_SKILLS and retired not in catalog.skills()
    stale = entry.model_copy(update={"skill": retired})
    assert skill_link(stale, tmp_path) is None
    (tmp_path / "skills").mkdir()
    (tmp_path / "skills" / f"{retired}.md").write_text("# Mine\n", "utf-8")
    assert (skill_link(stale, tmp_path) or {})["title"] == "Mine"
    lost = entry.model_copy(update={"skill": "no-such-skill"})
    assert (skill_link(lost, tmp_path) or {})["uri"] is None
