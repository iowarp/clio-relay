"""Relay's CLIO plugin agrees with the server it launches."""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any

from clio_relay.surface.a2ui import RELAY_CATALOG_ID, TAB_URI
from clio_relay.surface.a2ui.components import COMPONENTS
from clio_relay.surface.settings import RelayPaths

ROOT = Path(__file__).resolve().parents[2]
PLUGIN = ROOT / "plugin"
CATALOG = PLUGIN / "ai.iowarp.clio" / "a2ui" / "clio-relay-compute"


def _json(path: Path) -> Any:
    return json.loads(path.read_text("utf-8"))


def test_the_shipped_catalog_is_the_components_the_page_draws_with() -> None:
    catalog = _json(CATALOG / "catalog.json")
    clio = _json(CATALOG / "catalog.clio.json")
    assert catalog["catalogId"] == clio["catalogId"] == RELAY_CATALOG_ID
    assert set(catalog["components"]) == set(COMPONENTS)
    assert set(clio["implements"]) == set(COMPONENTS)
    assert all(row["kernel"] == name for name, row in clio["implements"].items())
    assert "openUrl" in catalog["functions"]
    refs = set(re.findall(r"#/components/([\w.-]+)", json.dumps(catalog)))
    assert refs == set(COMPONENTS)
    assert (CATALOG / clio["instructions"]).is_file()


def test_the_manifest_names_the_page_the_server_serves() -> None:
    manifest = _json(PLUGIN / "plugin.json")
    assert "title" not in manifest
    clio = manifest["extensions"]["ai.iowarp.clio"]
    assert clio["title"] == "Relay"
    # Compute is a page in CLIO's left bar, not a tab; its id must not be a tab's.
    assert "tabs" not in clio
    page = clio["navigation"]
    assert (page["id"], page["title"], page["resource"]) == ("compute", "Compute", TAB_URI)
    assert [row["provider"] for row in clio["accounts"]] == ["globus"]
    assert clio["elicitation"] == {"loopback": True, "origins": ["https://tacc.tapis.io"]}
    assert "server" not in page  # mcp.json has a single server


def test_the_launch_maps_the_globus_grant_and_runs_relay() -> None:
    servers = _json(PLUGIN / "mcp.json")["mcpServers"]
    assert list(servers) == ["relay"]
    server = servers["relay"]
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text("utf-8"))["project"]["scripts"]
    assert server["args"][-2:] == ["clio-relay", "serve"]
    assert "clio-relay" in scripts
    source = server["args"][server["args"].index("--from") + 1]
    pinned = r"clio-relay @ git\+https://github\.com/iowarp/clio-relay@[0-9a-f]{40}"
    assert re.fullmatch(pinned, source)
    assert server["env"] == {
        "CLIO_RELAY_CONFIG_DIR": "${PLUGIN_DATA}/config",
        "CLIO_RELAY_STATE_DIR": "${PLUGIN_DATA}/state",
        "CLIO_RELAY_GRANT_FILE_GLOBUS": "${CLIO_ACCOUNT_GLOBUS_FILE}",
    }
    launched = {
        "CLIO_RELAY_CONFIG_DIR": "/plugin-data/config",
        "CLIO_RELAY_STATE_DIR": "/plugin-data/state",
        "CLIO_RELAY_GRANT_FILE_GLOBUS": "/plugin-data/accounts/globus.json",
    }
    paths = RelayPaths.from_env(launched)
    assert paths.config_dir == Path("/plugin-data/config")
    assert paths.state_dir == Path("/plugin-data/state")
    assert paths.grant_files == {"globus": Path("/plugin-data/accounts/globus.json")}


def test_the_setup_file_is_the_sites_file_relay_writes() -> None:
    clio = _json(PLUGIN / "plugin.json")["extensions"]["ai.iowarp.clio"]
    env = _json(PLUGIN / "mcp.json")["mcpServers"]["relay"]["env"]
    data = Path("/plugin-data")
    launched = {key: value.replace("${PLUGIN_DATA}", str(data)) for key, value in env.items()}
    sites = RelayPaths.from_env(launched).sites_file
    assert [data / path for path in clio["setup"]["files"]] == [sites]


def test_the_skills_index_points_at_describe_for_the_site_guide() -> None:
    text = (PLUGIN / "skills" / "use-relay" / "SKILL.md").read_text("utf-8")
    assert text.startswith("---\nname: use-relay\n")
    assert "describe(site=...)" in text
    assert "second text block" in text
    assert "`files` refuses `relay://` URIs" in text
