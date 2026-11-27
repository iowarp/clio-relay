"""Every catalog site is valid: its file, its adapter's settings, its skill and alternatives.

A setting only the person can know, such as their own endpoint or resource id, may be
missing; anything the entry does set must be valid.
"""

from __future__ import annotations

from typing import Any

import pytest

from clio_relay import catalog
from clio_relay.adapters import registry
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import CatalogEntry

ENTRIES = catalog.sites()


@pytest.mark.parametrize("entry", ENTRIES, ids=[entry.id for entry in ENTRIES])
def test_catalog_site_is_valid(entry: CatalogEntry) -> None:
    assert entry.name and entry.description, "a catalog site says what it is"
    if entry.skill is not None:
        assert entry.skill in catalog.skills(), f"no skill {entry.skill!r} in catalog/skills"
    configs = {entry.adapter: entry.config, **entry.alternatives}
    for adapter, config in configs.items():
        assert adapter in registry.names(), f"no adapter is registered as {adapter!r}"
        try:
            registry.validate_config(adapter, config)
        except Refusal as refusal:
            if refusal.reason == RefusalReason.ADAPTER_UNAVAILABLE:
                continue  # its extra is not installed here
            errors: list[dict[str, Any]] = refusal.data.get("errors", [])
            wrong = [error for error in errors if error["type"] != "missing"]
            if refusal.reason != RefusalReason.INVALID_CONFIG or not errors or wrong:
                raise


def test_catalog_schema_is_the_entry_model() -> None:
    assert catalog.schema() == CatalogEntry.model_json_schema()


def test_entries_of_one_machine_are_one_site_with_its_protocols() -> None:
    found = catalog.grouped()
    ids = [site.id for site in found]
    assert len(ids) == len(set(ids)), "each machine is listed once"
    assert ids[0] == "ares" and ids[-1] == "local", "featured sites first, this computer last"
    assert "ares-tapis" not in ids
    ares = found[0]
    assert [(way.adapter, way.entry) for way in ares.ways] == [
        ("ssh", "ares"),
        ("tapis", "ares-tapis"),
        ("worker", "ares"),
    ]
    assert ares.way(None).adapter == "ssh", "the first protocol is the suggested one"
    assert ares.way("tapis").entry == "ares-tapis"
    assert ares.way("not-a-protocol").adapter == "ssh"
    for site in found:
        assert [way.adapter for way in site.ways] == list(
            dict.fromkeys(way.adapter for way in site.ways)
        ), f"{site.id} offers each protocol once"
        for way in site.ways:
            entry = catalog.site(way.entry)
            assert way.adapter == entry.adapter or way.adapter in entry.alternatives
