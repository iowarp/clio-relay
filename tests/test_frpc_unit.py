"""Pure rendering tests for the cluster-side frpc proxy config/unit (clio-relay#279).

No process spawned, no ssh, no filesystem writes -- every function under test
here is deterministic given its inputs, matching ``frp_link.py``'s own test
style.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.control_channel import TransportIdentityAnchorRequired
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frpc_unit import (
    _env_file_line,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    escape_env_file_value,
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

    assert env_text == (
        'CLIO_RELAY_FRP_TOKEN="TOKEN-VALUE-ABC"\nCLIO_RELAY_STCP_SECRET="SECRET-VALUE-XYZ"\n'
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


# --- D4 (adversarial review, clio-relay#279): env-file quoting/escaping -----
#
# Two independent defenses tested separately: the ESCAPING ALGORITHM itself
# (byte-exact, matching what the review verified round-trips on real
# systemd) via `escape_env_file_value` directly, and the REFUSAL that makes
# a quote/backslash/control-character secret unreachable through the public
# `render_frpc_proxy_env_file` path (frp's own env-template substitution is
# not proven TOML-string-safe -- see that function's own docstring).


@pytest.mark.parametrize(
    ("raw", "expected_escaped"),
    [
        ("plain-value-123", "plain-value-123"),
        ("value with spaces", "value with spaces"),
        ("value$with$dollars", "value$with$dollars"),
        ("value`with`backticks", "value`with`backticks"),
        ("value#with#hash", "value#with#hash"),
        ("value'with'singlequotes", "value'with'singlequotes"),
    ],
)
def test_escape_env_file_value_passes_through_ordinary_characters_unchanged(
    raw: str, expected_escaped: str
) -> None:
    assert escape_env_file_value(raw) == expected_escaped


def test_escape_env_file_value_escapes_backslash_and_double_quote() -> None:
    """The escaping algorithm itself, decoupled from the refusal gate.

    A secret containing `\\`/`"` never reaches this through
    ``render_frpc_proxy_env_file`` (refused upstream, see below), but the
    algorithm must still be provably correct on its own -- this is exactly
    what the adversarial review verified byte-exact on real systemd.
    """
    assert escape_env_file_value('has "quotes" inside') == 'has \\"quotes\\" inside'
    assert escape_env_file_value("has\\backslash\\inside") == "has\\\\backslash\\\\inside"
    assert escape_env_file_value('back\\slash then "quote') == 'back\\\\slash then \\"quote'


def _resolved_bash() -> str | None:
    return shutil.which("bash")


def _real_systemd_user_reachable(bash: str) -> bool:
    probe = subprocess.run(
        [
            bash,
            "-c",
            "command -v systemd-run >/dev/null 2>&1 && systemctl --user is-system-running",
        ],
        capture_output=True,
        check=False,
        timeout=15,
    )
    return probe.returncode == 0


@pytest.mark.parametrize(
    "raw",
    [
        "plain-value-123",
        "value with spaces",
        "value$with$dollars",
        "value`with`backticks",
        "value#with#hash",
        "value'with'singlequotes",
    ],
)
def test_env_file_line_round_trips_byte_exact_through_real_systemd_environmentfile_parsing(
    raw: str,
) -> None:
    """Prove the rendered ``NAME="value"`` line is what systemd's own parser reads back.

    ``systemd-run --user --pipe --wait --collect --property=EnvironmentFile=``
    starts a real, throwaway transient unit whose environment is populated
    by systemd's OWN ``EnvironmentFile=`` parser -- the exact directive
    ``render_frpc_proxy_unit`` puts in the persistent proxy unit -- then
    ``--pipe`` streams the child's stdout back here and ``--collect``
    garbage-collects the transient unit once it exits. This is the same
    proof shape the adversarial review ran against real systemd; it is
    skipped, never failed, when a real systemd user instance is
    unavailable (this sandbox has one; CI need not). A plain ``/tmp`` root
    generated here (never pytest's Windows ``tmp_path``) matches this
    module's siblings: a WSL-hosted ``bash.exe`` cannot resolve a
    ``C:\\...`` path.
    """
    bash = _resolved_bash()
    if bash is None or not _real_systemd_user_reachable(bash):
        pytest.skip("requires a reachable systemd --user instance with systemd-run")

    from uuid import uuid4

    root = f"/tmp/clio-relay-env-roundtrip-test_{uuid4().hex}"
    line = f'CLIO_RELAY_ENV_ROUNDTRIP_TEST="{escape_env_file_value(raw)}"'
    harness = (
        f'mkdir -p "{root}"\n'
        f"cat > \"{root}/roundtrip.env\" <<'__ROUNDTRIP_ENV__'\n"
        f"{line}\n"
        "__ROUNDTRIP_ENV__\n"
        f"systemd-run --user --pipe --wait --collect "
        f'--property="EnvironmentFile={root}/roundtrip.env" -- '
        '/bin/sh -c \'printf "%s" "$CLIO_RELAY_ENV_ROUNDTRIP_TEST"\'\n'
    )
    try:
        result = subprocess.run(
            [bash, "-s"],
            input=harness.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=20,
        )
        stdout = result.stdout.decode("utf-8", errors="replace")
        assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
        assert raw in stdout
    finally:
        subprocess.run(
            [bash, "-c", f'rm -rf -- "{root}"'], capture_output=True, check=False, timeout=15
        )


# --- refusal: quote/backslash/control characters -----------------------


@pytest.mark.parametrize(
    "secret",
    [
        'has "a quote" in it',
        "has\\a backslash",
        "has\ta tab",
        "has\x01a control char",
    ],
)
def test_env_file_refuses_a_secret_frps_template_substitution_could_not_toml_escape(
    secret: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", secret)

    with pytest.raises(RelayError, match="double quote|backslash|control character"):
        render_frpc_proxy_env_file(_definition(), cluster="ares")


def test_env_file_refuses_an_empty_secret() -> None:
    """Exercise ``_env_file_line``'s own empty-value check directly.

    An upstream env-binding check (``_require_env_binding``) already refuses
    a genuinely-unset/empty declared secret before this is ever reached
    through the public ``render_frpc_proxy_env_file`` path -- this proves
    the module's own belt-and-suspenders check in isolation.
    """
    with pytest.raises(RelayError):
        _env_file_line("CLIO_RELAY_FRP_TOKEN", "")
