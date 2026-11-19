"""The frp link settings and the frpc lookup."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, FrpTransportConfig
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError


def _site(name: str = "alpha") -> ClusterDefinition:
    return ClusterDefinition(
        name=name,
        ssh_host="alpha-login",
        frp_transport=FrpTransportConfig(server_addr="relay.example.test", proxy_name="p"),
    )


def test_registry_round_trips_through_json() -> None:
    registry = ClusterRegistry(clusters={"alpha": _site()})
    again = ClusterRegistry.model_validate_json(registry.model_dump_json())
    assert again.require("alpha").frp_transport.server_addr == "relay.example.test"


def test_require_names_the_missing_site() -> None:
    with pytest.raises(ConfigurationError, match="beta"):
        ClusterRegistry().require("beta")


def test_registry_keys_must_match_names() -> None:
    with pytest.raises(ValidationError, match="must match"):
        ClusterRegistry(clusters={"beta": _site("alpha")})


@pytest.mark.parametrize("host", ["-oProxyCommand=x", "a b", "a\nb"])
def test_ssh_host_is_one_destination(host: str) -> None:
    with pytest.raises(ValidationError, match="ssh_host"):
        ClusterDefinition(name="alpha", ssh_host=host)


def test_unknown_fields_are_refused() -> None:
    with pytest.raises(ValidationError):
        ClusterDefinition.model_validate({"name": "a", "ssh_host": "h", "spool_dir": "/x"})


def test_frpc_bin_prefers_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIO_RELAY_FRPC_BIN", "/opt/frp/frpc")
    assert RelaySettings.from_env().frpc_bin == "/opt/frp/frpc"


def test_frpc_bin_falls_back_to_the_name(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("CLIO_RELAY_FRPC_BIN", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    assert RelaySettings.from_env().frpc_bin == "frpc"
