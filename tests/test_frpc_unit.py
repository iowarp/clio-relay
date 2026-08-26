"""Pure rendering tests for the cluster-side frpc proxy config/unit (clio-relay#279).

No process spawned, no ssh, no filesystem writes -- every function under test
here is deterministic given its inputs, matching ``frp_link.py``'s own test
style.
"""

from __future__ import annotations

import pytest

from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.control_channel import TransportIdentityAnchorRequired
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frpc_unit import (
    frpc_proxy_config_digest,
    frpc_proxy_paths,
    render_frpc_proxy_env_file,
    render_frpc_proxy_toml,
    render_frpc_proxy_unit,
    require_frp_identity_anchor,
    validate_frpc_proxy_service_name,
)


def _definition(
    *,
    cluster: str = "ares",
    identity_anchor: str | None = "preshared_link_secret",
    server_addr: str = "relay.example.org",
) -> ClusterDefinition:
    return ClusterDefinition(
        name=cluster,
        ssh_host=f"{cluster}-login",
        frp_transport=FrpTransportConfig(
            server_addr=server_addr,
            identity_anchor=identity_anchor,  # type: ignore[arg-type]
        ),
    )


@pytest.fixture(autouse=True)
def _frp_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "TOKEN-VALUE-ABC")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "SECRET-VALUE-XYZ")


# --- naming ------------------------------------------------------------


def test_frpc_proxy_paths_are_deterministic_and_namespaced_from_the_worker_unit() -> None:
    paths = frpc_proxy_paths("ares")

    assert paths.unit_name == "clio-relay-frpc-proxy-ares.service"
    assert paths.unit_name != "clio-relay-worker-ares.service"
    assert paths.toml_unit_path.startswith("%h/.config/clio-relay/")
    assert paths.toml_shell_path.startswith("$HOME/.config/clio-relay/")
    assert paths.env_shell_path.endswith(".env")
    assert paths.receipt_shell_path.endswith("-receipt.json")


def test_frpc_proxy_paths_are_stable_across_repeated_calls() -> None:
    assert frpc_proxy_paths("ares") == frpc_proxy_paths("ares")


def test_validate_frpc_proxy_service_name_accepts_generated_names() -> None:
    validate_frpc_proxy_service_name(frpc_proxy_paths("ares").unit_name)


@pytest.mark.parametrize(
    "unit_name",
    [
        "relay-stcp",
        "clio-relay-worker-ares.service",
        "clio-relay-frpc-proxy-ares.service; rm -rf /",
    ],
)
def test_validate_frpc_proxy_service_name_rejects_anything_else(unit_name: str) -> None:
    with pytest.raises(RelayError):
        validate_frpc_proxy_service_name(unit_name)


# --- identity anchor refusal --------------------------------------------


def test_require_frp_identity_anchor_accepts_the_preshared_link_secret_anchor() -> None:
    require_frp_identity_anchor(
        _definition(identity_anchor="preshared_link_secret"), cluster="ares"
    )


def test_require_frp_identity_anchor_refuses_when_unset() -> None:
    with pytest.raises(TransportIdentityAnchorRequired, match="identity anchor"):
        require_frp_identity_anchor(_definition(identity_anchor=None), cluster="ares")


def test_render_frpc_proxy_toml_refuses_without_identity_anchor() -> None:
    with pytest.raises(TransportIdentityAnchorRequired):
        render_frpc_proxy_toml(_definition(identity_anchor=None), cluster="ares", local_port=8765)


def test_render_frpc_proxy_env_file_refuses_without_identity_anchor() -> None:
    with pytest.raises(TransportIdentityAnchorRequired):
        render_frpc_proxy_env_file(_definition(identity_anchor=None), cluster="ares")


def test_render_frpc_proxy_toml_refuses_blank_server_addr() -> None:
    with pytest.raises(ConfigurationError):
        render_frpc_proxy_toml(_definition(server_addr=""), cluster="ares", local_port=8765)


# --- TOML rendering: secrets never inline -------------------------------


def test_toml_reuses_render_proxy_config_and_never_inlines_secrets() -> None:
    definition = _definition()

    toml_text = render_frpc_proxy_toml(definition, cluster="ares", local_port=8765)

    assert "TOKEN-VALUE-ABC" not in toml_text
    assert "SECRET-VALUE-XYZ" not in toml_text
    assert 'auth.token = "{{ .Envs.CLIO_RELAY_FRP_TOKEN }}"' in toml_text
    assert 'secretKey = "{{ .Envs.CLIO_RELAY_STCP_SECRET }}"' in toml_text
    assert 'name = "ares-owned-session"' in toml_text
    assert 'type = "stcp"' in toml_text
    assert "localPort = 8765" in toml_text


def test_toml_supports_xtcp_proxy_type() -> None:
    definition = _definition()

    toml_text = render_frpc_proxy_toml(
        definition, cluster="ares", local_port=8765, proxy_type="xtcp"
    )

    assert 'type = "xtcp"' in toml_text


def test_toml_respects_an_explicit_declared_proxy_name() -> None:
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        frp_transport=FrpTransportConfig(
            server_addr="relay.example.org",
            identity_anchor="preshared_link_secret",
            proxy_name="ares-custom-proxy",
        ),
    )

    toml_text = render_frpc_proxy_toml(definition, cluster="ares", local_port=8765)

    assert 'name = "ares-custom-proxy"' in toml_text


def test_toml_respects_custom_env_binding_names() -> None:
    definition = ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        frp_transport=FrpTransportConfig(
            server_addr="relay.example.org",
            identity_anchor="preshared_link_secret",
            token_env="ARES_TOKEN",
            stcp_secret_env="ARES_SECRET",
        ),
    )

    toml_text = render_frpc_proxy_toml(definition, cluster="ares", local_port=8765)

    assert 'auth.token = "{{ .Envs.ARES_TOKEN }}"' in toml_text
    assert 'secretKey = "{{ .Envs.ARES_SECRET }}"' in toml_text


# --- env file: secrets bound, never inline in the TOML ------------------


def test_env_file_binds_the_real_secrets_the_toml_template_references() -> None:
    definition = _definition()

    env_text = render_frpc_proxy_env_file(definition, cluster="ares")

    assert (
        env_text
        == "CLIO_RELAY_FRP_TOKEN=TOKEN-VALUE-ABC\nCLIO_RELAY_STCP_SECRET=SECRET-VALUE-XYZ\n"
    )


def test_env_file_refuses_when_a_declared_secret_binding_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)

    with pytest.raises(ConfigurationError, match="stcp/xtcp pairing secret"):
        render_frpc_proxy_env_file(_definition(), cluster="ares")


def test_env_file_uses_an_explicit_env_mapping_over_os_environ() -> None:
    definition = _definition()

    env_text = render_frpc_proxy_env_file(
        definition,
        cluster="ares",
        env={"CLIO_RELAY_FRP_TOKEN": "override-token", "CLIO_RELAY_STCP_SECRET": "override-secret"},
    )

    assert "override-token" in env_text
    assert "override-secret" in env_text
    assert "TOKEN-VALUE-ABC" not in env_text


# --- unit rendering -------------------------------------------------------


def test_unit_wires_environment_file_and_exec_start_to_the_toml() -> None:
    paths = frpc_proxy_paths("ares")

    unit_text = render_frpc_proxy_unit(cluster="ares", paths=paths)

    assert f"EnvironmentFile={paths.env_unit_path}" in unit_text
    assert f"ExecStart=%h/.local/bin/frpc -c {paths.toml_unit_path}" in unit_text
    assert "Restart=on-failure" in unit_text
    assert "RestartSec=5" in unit_text
    assert "WantedBy=default.target" in unit_text
    assert "Type=simple" in unit_text


def test_unit_never_carries_a_secret() -> None:
    paths = frpc_proxy_paths("ares")

    unit_text = render_frpc_proxy_unit(cluster="ares", paths=paths)

    assert "TOKEN-VALUE-ABC" not in unit_text
    assert "SECRET-VALUE-XYZ" not in unit_text
    assert "Envs" not in unit_text


def test_unit_rejects_a_non_positive_restart_delay() -> None:
    paths = frpc_proxy_paths("ares")

    with pytest.raises(RelayError):
        render_frpc_proxy_unit(cluster="ares", paths=paths, restart_sec=0)


def test_unit_escapes_a_cluster_name_percent_sign() -> None:
    paths = frpc_proxy_paths("ares")

    unit_text = render_frpc_proxy_unit(cluster="ares-100%-test", paths=paths)

    assert "Description=clio-relay frpc proxy for ares-100%%-test" in unit_text


def test_unit_rejects_a_cluster_name_with_an_embedded_newline() -> None:
    paths = frpc_proxy_paths("ares")

    with pytest.raises(RelayError):
        render_frpc_proxy_unit(cluster="ares\nWantedBy=malicious.target", paths=paths)


# --- config digest ---------------------------------------------------------


def test_config_digest_is_stable_for_identical_text() -> None:
    text = render_frpc_proxy_toml(_definition(), cluster="ares", local_port=8765)

    assert frpc_proxy_config_digest(text) == frpc_proxy_config_digest(text)


def test_config_digest_changes_when_the_rendered_toml_changes() -> None:
    definition = _definition()
    first = render_frpc_proxy_toml(definition, cluster="ares", local_port=8765)
    second = render_frpc_proxy_toml(definition, cluster="ares", local_port=9999)

    assert frpc_proxy_config_digest(first) != frpc_proxy_config_digest(second)
