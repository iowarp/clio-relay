from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tarfile
import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

from clio_relay import __version__
from clio_relay.application_profiles import (
    install_cluster_app_over_ssh,
    render_cluster_app_install_script,
)
from clio_relay.bootstrap import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
    FRP_LINUX_AMD64_SHA256,
    FRPC_LINUX_AMD64_SHA256,
    FRPS_LINUX_AMD64_SHA256,
    JARVIS_CD_VERSION,
    JARVIS_CD_WHEEL_FILENAME,
    JARVIS_CD_WHEEL_SHA256,
    JARVIS_CD_WHEEL_URL,
    JARVIS_UTIL_COMMIT,
    UV_LINUX_AMD64_SHA256,
    UV_VERSION,
    assert_clean_git_checkout,
    create_bootstrap_archive,
    install_local_frp,
    render_linux_user_bootstrap_script,
)
from clio_relay.errors import ConfigurationError, RelayError
from tests.plugin_fakes import FakeEntryPoint, FakeEntryPoints


def test_bootstrap_uses_exact_public_jarvis_cd_release_pin() -> None:
    """Keep bootstrap and the locked MCP child on the same public JARVIS wheel."""
    assert JARVIS_CD_VERSION == "1.3.15"
    assert JARVIS_CD_WHEEL_FILENAME == "jarvis_cd-1.3.15-py3-none-any.whl"
    assert JARVIS_CD_WHEEL_URL == (
        "https://github.com/grc-iit/jarvis-cd/releases/download/"
        "v1.3.15/jarvis_cd-1.3.15-py3-none-any.whl"
    )
    assert JARVIS_CD_WHEEL_SHA256 == (
        "3276b4db592934acc34e55eb16ce0ab9496bf9143ad38a768de4f9e58a8738e8"
    )


def _write_test_relay_wheel(
    path: Path,
    *,
    metadata_name: str = "clio-relay",
    metadata_version: str = __version__,
) -> None:
    """Write a minimal wheel with explicit core metadata for bootstrap tests."""
    dist_info = f"clio_relay-{__version__}.dist-info"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"{dist_info}/METADATA",
            (f"Metadata-Version: 2.4\nName: {metadata_name}\nVersion: {metadata_version}\n\n"),
        )
        archive.writestr(
            f"{dist_info}/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr(f"{dist_info}/RECORD", "")


def _assert_bootstrap_rejected_before_remote_call(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    wheel: Path,
    expected_error: str,
    relay_artifact_sha256: str | None = None,
) -> None:
    """Require local wheel validation to fail before any remote command."""
    from clio_relay import bootstrap

    remote_calls: list[list[str]] = []

    def record_remote_call(
        command: list[str],
        *,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        remote_calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(bootstrap, "_run", record_remote_call)
    with pytest.raises(ConfigurationError, match=expected_error):
        bootstrap.bootstrap_cluster_over_ssh(
            bootstrap_profile="linux-user",
            ssh_host="ares",
            source_root=tmp_path,
            relay_wheel=wheel,
            relay_artifact_sha256=relay_artifact_sha256,
        )
    assert remote_calls == []


@dataclass(frozen=True)
class SiteBootstrapProfile:
    """Test-only application profile supplied through an entry point."""

    name: str = "site-stack"

    def render_install_script(self) -> str:
        """Return a generic site-owned installer."""
        return "set -euo pipefail\nprintf 'site_stack=ready\\n'\n"


def _install_site_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    entries = FakeEntryPoints(
        [
            FakeEntryPoint(
                name="site-stack",
                group="clio_relay.application_profiles",
                loaded=SiteBootstrapProfile(),
            )
        ]
    )
    monkeypatch.setattr("clio_relay.application_profiles.entry_points", lambda: entries)


def test_linux_user_bootstrap_script_installs_required_components() -> None:
    script = render_linux_user_bootstrap_script(frp_version="0.69.1")

    assert 'FRP_VERSION="0.69.1"' in script
    assert 'ARCHIVE="frp_${FRP_VERSION}_linux_amd64.tar.gz"' in script
    assert f'FRP_SHA256="{FRP_LINUX_AMD64_SHA256}"' in script
    assert f'FRPC_SHA256="{FRPC_LINUX_AMD64_SHA256}"' in script
    assert f'FRPS_SHA256="{FRPS_LINUX_AMD64_SHA256}"' in script
    assert "sha256sum --check --strict -" in script
    assert f'UV_VERSION="{UV_VERSION}"' in script
    assert f'UV_SHA256="{UV_LINUX_AMD64_SHA256}"' in script
    assert "https://astral.sh/uv/install.sh" not in script
    assert "uv python install 3.12" in script
    assert 'export UV_TOOL_DIR="$HOME/.local/share/uv/tools"' in script
    assert 'export UV_TOOL_BIN_DIR="$HOME/.local/bin"' in script
    assert 'uv venv --python 3.12 --seed --clear "$JARVIS_VENV"' in script
    assert "python3 -m venv" not in script
    assert "CLIO_RELAY_AGENT_NPM_PACKAGE" in script
    assert "CLIO_RELAY_AGENT_NPM_BIN" in script
    assert 'npm install -g "$AGENT_NPM_PACKAGE"' in script
    assert "CLIO_RELAY_AGENT_BIN" in script
    assert "AGENT_NPM_PACKAGE=${CLIO_RELAY_AGENT_NPM_PACKAGE:-''}" in script
    assert "AGENT_NPM_BIN=${CLIO_RELAY_AGENT_NPM_BIN:-''}" in script
    assert "CLIO_RELAY_AGENT_ADAPTER=exec" in script
    assert "CLIO_RELAY_AGENT_ARGS=''" in script
    assert "github.com/grc-iit/jarvis-cd.git" not in script
    assert f'JARVIS_UTIL_COMMIT="{JARVIS_UTIL_COMMIT}"' in script
    assert f'JARVIS_CD_VERSION="{JARVIS_CD_VERSION}"' in script
    assert f'JARVIS_CD_WHEEL_URL="{JARVIS_CD_WHEEL_URL}"' in script
    assert f'JARVIS_CD_WHEEL_SHA256="{JARVIS_CD_WHEEL_SHA256}"' in script
    assert f'JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL_DIR/{JARVIS_CD_WHEEL_FILENAME}"' in script
    assert 'fetch --depth 1 origin "$JARVIS_UTIL_COMMIT"' in script
    assert 'fetch --depth 1 origin "$JARVIS_CD_COMMIT"' not in script
    assert 'curl -L --fail --retry 3 -o "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL_URL"' in script
    assert (
        'echo "$JARVIS_CD_WHEEL_SHA256 *$JARVIS_CD_STAGING" | sha256sum --check --strict -'
    ) in script
    assert "pull --ff-only" not in script
    assert 'python -m pip install -e "$HOME/.local/src/jarvis' not in script
    assert f"JARVIS_MCP_INSTALL_SPEC={CLIO_KIT_JARVIS_MCP_WHEEL_URL}" in script
    assert f"JARVIS_MCP_ARTIFACT_SHA256={CLIO_KIT_JARVIS_MCP_WHEEL_SHA256}" in script
    assert f'"{CLIO_KIT_JARVIS_MCP_WHEEL_URL}")' in script
    assert f'JARVIS_MCP_VERSION="{CLIO_KIT_JARVIS_MCP_VERSION}"' in script
    assert (
        f'JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"'
    ) in script
    assert "--proto '=https' --proto-redir '=https' --tlsv1.2" in script
    assert "--retry 3 --retry-all-errors --retry-max-time 180" in script
    assert 'JARVIS_MCP_REQUESTED_SOURCE="github_release"' in script
    assert "uv tool install --force --python 3.12 --no-config \\" in script
    assert '--default-index https://pypi.org/simple "$JARVIS_MCP_INSTALL_TARGET"' in script
    digest_check = (
        'echo "$JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH" | '
        "  sha256sum --check --strict -"
    )
    assert digest_check in script.replace("\\\n", "")
    assert script.index("JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH") < (
        script.index("uv tool install --force")
    )
    assert 'uvx --refresh --no-config --from "$JARVIS_MCP_INSTALL_TARGET"' not in script
    assert 'JARVIS_MCP_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-kit"' in script
    assert 'JARVIS_MCP_UV_EXECUTABLE="$(command -v uv)"' in script
    assert '"$JARVIS_MCP_EXECUTABLE" --help' in script
    assert 'JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"' in script
    assert "from clio_relay.mcp_call.runner import mcp_server_artifact_identity" in script
    assert "clio_kit_server_artifact = mcp_server_artifact_identity(" in script
    assert "verify_relay_jarvis_cd_lock=True" in script
    assert 'locked_server_runtime.get("server_name") == "jarvis"' in script
    assert 'jarvis_cd_lock_binding.get("resolved_dependency_entry_count") == 1' in script
    assert 'jarvis_cd_lock_binding.get("observed_resolved_dependency_entries")' in script
    assert 'jarvis_cd_lock_binding.get("observed_version") == expected_jarvis_cd_version' in script
    assert 'jarvis_cd_lock_binding.get("observed_source_url") == expected_jarvis_cd_url' in script
    assert (
        'jarvis_cd_lock_binding.get("observed_wheel_sha256") == expected_jarvis_cd_sha256' in script
    )
    assert 'jarvis_cd_lock_binding.get("observed_metadata_requirement_entries")' in script
    assert "locked_server_runtime=locked_server_runtime" in script
    assert (
        "runtime_artifact_path=(str(component_artifact) if component_artifact else None)" in script
    )
    assert "runtime_command=runtime_command" in script
    assert '"provider": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON"]' in script
    assert '"clio-kit": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"]' in script
    assert "clio-kit.git@main#subdirectory=clio-kit-mcp-servers/jarvis" not in script
    assert script.count("status --porcelain=v1 --untracked-files=all") == 1
    assert 'ln -sf "$JARVIS_VENV/bin/jarvis-mcp" "$HOME/.local/bin/jarvis-mcp"' not in script
    assert 'RELAY_INSTALL_SPEC="$DEST"' in script
    assert "uv tool install --force --python 3.12 --no-config" in script
    assert '--with "$JARVIS_CD_WHEEL" "$RELAY_INSTALL_TARGET"' in script
    assert 'RELAY_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-relay"' in script
    assert 'RELAY_PROVIDER_PYTHON="$(sed -n' in script
    assert "relay-venv312" not in script
    assert 'uv pip install --refresh-package clio-relay "$RELAY_INSTALL_TARGET"' not in script
    assert "uv pip install --no-deps --refresh-package jarvis-cd" not in script
    assert (
        'python -m pip install --isolated --index-url https://pypi.org/simple "$JARVIS_CD_WHEEL"'
    ) in script
    assert "JARVIS-CD was not installed from the verified release wheel" in script
    assert "from clio_relay.installation import verify_distribution_file_source" in script
    assert "verify_distribution_file_source(" in script
    assert 'direct_url.get("url") != wheel.as_uri()' not in script
    assert 'verify_jarvis_cd_distribution "$RELAY_PROVIDER_PYTHON"' in script
    assert 'verify_jarvis_cd_distribution "$JARVIS_VENV/bin/python"' in script
    assert 'entry_point.group == "clio_relay.package_progress_adapters"' not in script
    assert "probe_jarvis_native_execution_capability" in script
    assert "probe_clio_kit_native_execution_contract" in script
    assert "probe_persistent_uv_tool_identity" in script
    assert 'distribution="clio-relay"' in script
    assert 'entry_point="clio-relay"' in script
    assert "persistent_tool=relay_persistent_tool" in script
    assert (
        'uv pip install --python "$JARVIS_VENV/bin/python" \\\n'
        "  --default-index https://pypi.org/simple \\\n"
        '  --refresh-package clio-relay "$RELAY_INSTALL_TARGET"'
    ) in script
    assert "import clio_relay.bounded_command.pkg" in script
    assert "clio_relay.mcp_call.pkg" in script
    assert "clio_relay.remote_agent.pkg" in script
    assert "write_install_receipt" in script
    assert '"schema_version": "clio-relay.bootstrap-receipt.v1"' in script
    assert "\"invocation_id\": 'manual'" in script
    assert "install_receipt_sha256 = hashlib.sha256" in script
    assert "temporary.write_text" in script
    assert "os.chmod(temporary, 0o600)" in script
    assert "os.replace(temporary, destination)" in script
    assert "bootstrap_invocation_id=" in script
    assert "ComponentArtifactIdentity" in script
    assert '"jarvis-cd": ComponentArtifactIdentity(' in script
    assert 'requested_source="github_release"' in script
    assert "artifact_sha256=jarvis_cd_wheel_sha256" in script
    assert "artifact_sha256=component_artifact_sha256" in script
    assert '"provider": sys.executable' in script
    assert '"execution": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"]' in script
    assert "native_execution=clio_kit_native_execution" in script
    assert "persistent_tool=persistent_clio_kit_tool" in script
    assert "native_execution=jarvis_execution_native_execution" in script
    assert "jarvis_cd_entry_points" not in script
    assert 'requested_source=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE"]' in script
    assert "relay_artifact_sha256=" in script
    assert 'jarvis repo add "$DEST/jarvis-packages/clio_relay" --force true' in script
    assert '"$HOME/.local/share/clio-relay/jarvis-shared" || true' not in script
    assert "python -m pip install --upgrade pip setuptools wheel" not in script
    assert "spack install" not in script
    assert "site_stack=ready" not in script
    assert "CLIO_RELAY_CORE_DIR" in script
    assert "clio-relay init" in script
    assert "done < <(compgen -e)" in script
    assert "UV_*|PIP_*) unset" in script
    assert "--index-url https://pypi.org/simple --no-deps --only-binary=:all:" in script
    assert "\r" not in script


def test_wheel_embeds_jarvis_package_modules_in_the_installed_namespace() -> None:
    """JARVIS must import relay packages after the relay distribution is installed."""
    with Path("pyproject.toml").open("rb") as stream:
        project = tomllib.load(stream)
    force_include = project["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]

    assert force_include == {
        "jarvis-packages": "clio_relay/assets/jarvis-packages",
        "jarvis-packages/clio_relay/clio_relay/_jarvis_api.py": "clio_relay/_jarvis_api.py",
        "jarvis-packages/clio_relay/clio_relay/bounded_command": "clio_relay/bounded_command",
        "jarvis-packages/clio_relay/clio_relay/mcp_call": "clio_relay/mcp_call",
        "jarvis-packages/clio_relay/clio_relay/remote_agent": "clio_relay/remote_agent",
    }


def test_linux_user_bootstrap_embedded_python_programs_compile() -> None:
    """Reject escaping defects in every Python heredoc before cluster bootstrap."""
    script = render_linux_user_bootstrap_script(cluster="test-cluster")
    pattern = re.compile(
        r"<<'(?P<marker>__CLIO_RELAY_[A-Z0-9_]+__)'\n"
        r"(?P<body>.*?)(?=\n(?P=marker)(?:\n|$))",
        re.DOTALL,
    )
    programs = list(pattern.finditer(script))

    assert {program.group("marker") for program in programs} == {
        "__CLIO_RELAY_BOOTSTRAP_RECEIPT__",
        "__CLIO_RELAY_INSTALL_RECEIPT__",
        "__CLIO_RELAY_NATIVE_JARVIS_PROBE__",
        "__CLIO_RELAY_PYPI_DIGEST__",
        "__CLIO_RELAY_WORKER_LIFETIME_FD__",
        "__CLIO_RELAY_WORKER_WRITER_PROOF__",
    }
    for program in programs:
        marker = program.group("marker")
        compile(program.group("body"), f"bootstrap:{marker}", "exec")


def test_local_clio_kit_bootstrap_wheel_must_match_release_pin() -> None:
    with pytest.raises(ConfigurationError, match="requires its expected wheel SHA-256"):
        render_linux_user_bootstrap_script(
            jarvis_mcp_install_spec="/tmp/clio_kit-2.3.1-py3-none-any.whl"
        )

    with pytest.raises(ConfigurationError, match="requires the released clio-kit wheel"):
        render_linux_user_bootstrap_script(
            jarvis_mcp_install_spec="/tmp/clio_kit-2.3.1-py3-none-any.whl",
            jarvis_mcp_artifact_sha256="d" * 64,
        )

    script = render_linux_user_bootstrap_script(
        jarvis_mcp_install_spec=f"/tmp/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}",
        jarvis_mcp_artifact_sha256=CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    )
    assert "JARVIS_MCP_ARTIFACT_SHA256=" + CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 in script
    assert script.index("JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH") < (
        script.index("uv tool install --force")
    )


def test_clio_kit_exact_version_override_requires_its_own_digest() -> None:
    with pytest.raises(ConfigurationError, match="requires its expected wheel SHA-256"):
        render_linux_user_bootstrap_script(
            jarvis_mcp_install_spec=f"clio-kit=={CLIO_KIT_JARVIS_MCP_VERSION}"
        )

    with pytest.raises(ConfigurationError, match="requires the released clio-kit version"):
        render_linux_user_bootstrap_script(
            jarvis_mcp_install_spec="clio-kit==0.0.0",
            jarvis_mcp_artifact_sha256=CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        )


def test_bootstrap_uv_pin_matches_release_policy() -> None:
    """Keep the verified cluster bootstrap toolchain aligned with the release gate."""
    policy_path = Path(__file__).parents[1] / "docs" / "release-gate-1.0.yaml"
    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))

    assert policy["required_uv_version"] == UV_VERSION
    assert UV_LINUX_AMD64_SHA256 == (
        "e490a6464492183c5d4534a5527fb4440f7f2bb2f228162ad7e4afe076dc0224"
    )


def test_linux_user_bootstrap_script_expands_dest_wheel_install_spec() -> None:
    script = render_linux_user_bootstrap_script(
        relay_install_spec="$DEST/wheels/clio_relay-0.9.16-py3-none-any.whl",
        relay_artifact_sha256="e" * 64,
    )
    expected = 'RELAY_INSTALL_SPEC="$DEST"/wheels/clio_relay-0.9.16-py3-none-any.whl'

    assert expected in script
    assert "RELAY_ARTIFACT_SHA256=" + "e" * 64 in script
    assert script.index("RELAY_ARTIFACT_SHA256 *$RELAY_ARTIFACT_PATH") < script.index(
        '--with "$JARVIS_CD_WHEEL"'
    )


def test_relay_bootstrap_wheel_requires_preinstall_digest() -> None:
    with pytest.raises(ConfigurationError, match="requires its expected SHA-256"):
        render_linux_user_bootstrap_script(
            relay_install_spec="$DEST/wheels/clio_relay-1.0.0-py3-none-any.whl"
        )


def test_linux_user_bootstrap_script_accepts_explicit_npm_agent() -> None:
    script = render_linux_user_bootstrap_script(
        agent_adapter="codex",
        agent_npm_package="@openai/codex",
        agent_npm_bin="codex",
        agent_args=["--model", "gpt-5-codex"],
    )

    assert "AGENT_NPM_PACKAGE=${CLIO_RELAY_AGENT_NPM_PACKAGE:-@openai/codex}" in script
    assert "AGENT_NPM_BIN=${CLIO_RELAY_AGENT_NPM_BIN:-codex}" in script
    assert 'AGENT_BIN="$HOME/.local/bin/$AGENT_NPM_BIN"' in script
    assert "CLIO_RELAY_AGENT_ADAPTER=codex" in script
    assert "CLIO_RELAY_AGENT_ARGS='--model gpt-5-codex'" in script


def test_external_application_profile_is_explicit_cluster_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_site_profile(monkeypatch)

    script = render_cluster_app_install_script(app_name="site-stack")

    assert "site_stack=ready" in script
    assert "\r" not in script


def test_cluster_app_install_rejects_unknown_app() -> None:
    with pytest.raises(ConfigurationError, match="unsupported cluster app"):
        render_cluster_app_install_script(app_name="missing-site-stack")


def test_cluster_app_install_sends_lf_script_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_site_profile(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        del args
        calls.append(kwargs)
        return subprocess.CompletedProcess(
            ["ssh", "host", "bash", "-s"],
            0,
            stdout=b"ok\n",
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = install_cluster_app_over_ssh(ssh_host="host", app_name="site-stack")

    assert result == ["ok"]
    script = calls[0]["input"]
    assert isinstance(script, bytes)
    assert b"\r" not in script
    assert calls[0]["capture_output"] is True


def test_bootstrap_runner_decodes_remote_output_as_utf8(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del args
        calls.append(kwargs)
        return subprocess.CompletedProcess(["ssh", "host"], 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    from clio_relay import bootstrap

    result = bootstrap._run(["ssh", "host"])  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert result.stdout == "ok"
    assert calls[0]["encoding"] == "utf-8"
    assert calls[0]["errors"] == "replace"


def test_bootstrap_over_ssh_returns_the_matching_durable_invocation_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from clio_relay import bootstrap

    calls: list[list[str]] = []
    uploaded_scripts: list[str] = []
    receipt_document: dict[str, object] = {
        "schema_version": "clio-relay.bootstrap-receipt.v1",
        "invocation_id": "bootstrap_abc",
        "bootstrap_profile": "linux-user",
        "relay_install_spec": "clio-relay==1.0.0",
        "install_receipt_sha256": "a" * 64,
        "completed_at": "2026-07-11T00:00:00Z",
    }

    def fake_create_bootstrap_archive(
        *,
        source_root: Path,
        archive: Path,
        relay_wheel: Path | None,
    ) -> bootstrap.BootstrapArchive:
        assert source_root == tmp_path
        assert relay_wheel is None
        archive.write_bytes(b"bootstrap archive")
        return bootstrap.BootstrapArchive(archive=archive, install_spec="clio-relay==1.0.0")

    def fake_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] == "scp" and command[-1].endswith("/clio-relay-bootstrap.sh"):
            uploaded_scripts.append(Path(command[1]).read_text(encoding="utf-8"))
        if command[-2:] == [
            "bash",
            "/tmp/clio-relay-bootstrap_abc/clio-relay-bootstrap.sh",
        ]:
            return subprocess.CompletedProcess(
                command,
                0,
                "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json\n",
                "",
            )
        if command[-2:] == ["cat", "$HOME/.local/share/clio-relay/bootstrap-receipt.json"]:
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(receipt_document),
                "",
            )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", fake_create_bootstrap_archive)
    monkeypatch.setattr(bootstrap, "_run", fake_run)
    monkeypatch.setattr(bootstrap, "uuid4", lambda: type("Uuid", (), {"hex": "abc"})())

    def fake_which(executable: str) -> str:
        return executable

    monkeypatch.setattr(bootstrap.shutil, "which", fake_which)

    lines = bootstrap.bootstrap_cluster_over_ssh(
        bootstrap_profile="linux-user",
        ssh_host="ares",
        source_root=tmp_path,
    )

    assert lines[0].startswith("bootstrap_receipt=")
    receipt_line = next(line for line in lines if line.startswith("bootstrap_receipt_json="))
    receipt = json.loads(receipt_line.partition("=")[2])
    assert receipt["invocation_id"] == "bootstrap_abc"
    assert [
        "ssh",
        "ares",
        "cat",
        "$HOME/.local/share/clio-relay/bootstrap-receipt.json",
    ] in calls
    assert calls[-1] == [
        "ssh",
        "ares",
        "rm",
        "-rf",
        "--",
        "/tmp/clio-relay-bootstrap_abc",
    ]
    assert any(
        command[-1] == "ares:/tmp/clio-relay-bootstrap_abc/clio-relay-head.tar" for command in calls
    )
    assert uploaded_scripts
    assert 'exec 9>"$HOME/.local/share/clio-relay/bootstrap.lock"' in uploaded_scripts[0]
    assert "sha256sum --check --strict" in uploaded_scripts[0]
    assert "/tmp/clio-relay-head.tar" not in uploaded_scripts[0]

    receipt_document["relay_install_spec"] = "unreviewed-source"
    with pytest.raises(RelayError, match="relay_install_spec"):
        bootstrap.bootstrap_cluster_over_ssh(
            bootstrap_profile="linux-user",
            ssh_host="ares",
            source_root=tmp_path,
        )
    assert calls[-1][-1] == "/tmp/clio-relay-bootstrap_abc"


def test_bootstrap_over_ssh_rejects_option_like_destination(tmp_path: Path) -> None:
    from clio_relay import bootstrap

    with pytest.raises(ConfigurationError, match="non-option destination"):
        bootstrap.bootstrap_cluster_over_ssh(
            bootstrap_profile="linux-user",
            ssh_host="-oProxyCommand=evil",
            source_root=tmp_path,
        )


def test_bootstrap_over_ssh_rejects_invalid_relay_wheel_filename_before_ssh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel = tmp_path / f"clio_relay-{__version__}-release-check.whl"
    _write_test_relay_wheel(wheel)
    _assert_bootstrap_rejected_before_remote_call(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        wheel=wheel,
        expected_error="wheel filename is not canonical",
    )


def test_bootstrap_over_ssh_rejects_relay_wheel_digest_mismatch_before_ssh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    _write_test_relay_wheel(wheel)
    _assert_bootstrap_rejected_before_remote_call(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        wheel=wheel,
        expected_error="SHA-256 does not match its pin",
        relay_artifact_sha256="0" * 64,
    )


def test_bootstrap_over_ssh_rejects_foreign_wheel_distribution_before_ssh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel = tmp_path / f"other_project-{__version__}-py3-none-any.whl"
    _write_test_relay_wheel(wheel, metadata_name="other-project")
    _assert_bootstrap_rejected_before_remote_call(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        wheel=wheel,
        expected_error="distribution must be clio-relay",
    )


@pytest.mark.parametrize(
    ("metadata_name", "metadata_version", "expected_error"),
    [
        ("other-project", __version__, "METADATA Name does not match"),
        ("clio-relay", "9.9.9", "METADATA Version does not match"),
    ],
)
def test_bootstrap_over_ssh_rejects_relay_wheel_metadata_mismatch_before_ssh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    metadata_name: str,
    metadata_version: str,
    expected_error: str,
) -> None:
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    _write_test_relay_wheel(
        wheel,
        metadata_name=metadata_name,
        metadata_version=metadata_version,
    )
    _assert_bootstrap_rejected_before_remote_call(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        wheel=wheel,
        expected_error=expected_error,
    )


def test_relay_wheel_preflight_accepts_canonical_filename_and_matching_metadata(
    tmp_path: Path,
) -> None:
    from clio_relay import bootstrap

    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    _write_test_relay_wheel(wheel)

    observed = bootstrap._validate_relay_bootstrap_wheel(  # pyright: ignore[reportPrivateUsage]
        wheel
    )

    assert observed == hashlib.sha256(wheel.read_bytes()).hexdigest()


def test_bootstrap_over_ssh_rejects_non_regular_relay_wheel_before_ssh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    wheel.mkdir()
    _assert_bootstrap_rejected_before_remote_call(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        wheel=wheel,
        expected_error="must be one regular file",
    )


def test_local_frp_install_publishes_only_a_verified_staged_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "frp" / "bin"
    staged_destinations: list[Path] = []

    def fake_install(staging: Path, _version: str) -> None:
        staged_destinations.append(staging)
        (staging / "frpc.exe").write_bytes(b"verified-frpc")
        (staging / "frps.exe").write_bytes(b"verified-frps")

    def accept_pair(_frpc: Path, _frps: Path) -> None:
        return None

    monkeypatch.setattr("clio_relay.bootstrap.platform.system", lambda: "Windows")
    monkeypatch.setattr("clio_relay.bootstrap.platform.machine", lambda: "AMD64")
    monkeypatch.setattr("clio_relay.bootstrap._install_frp_from_release_archive", fake_install)
    monkeypatch.setattr("clio_relay.bootstrap._assert_frp_pair", accept_pair)

    installed = install_local_frp(destination)

    assert installed == destination / "frpc.exe"
    assert installed.read_bytes() == b"verified-frpc"
    assert (destination / "frps.exe").read_bytes() == b"verified-frps"
    assert staged_destinations[0] != destination
    assert not staged_destinations[0].parent.exists()


def test_local_frp_install_removes_destination_pair_when_final_verification_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "frp" / "bin"

    def fake_install(staging: Path, _version: str) -> None:
        (staging / "frpc.exe").write_bytes(b"staged-frpc")
        (staging / "frps.exe").write_bytes(b"staged-frps")

    def quarantine_destination(frpc: Path, _frps: Path) -> None:
        if frpc.parent == destination:
            raise ConfigurationError("installed executable cannot be hashed: quarantined")

    monkeypatch.setattr("clio_relay.bootstrap.platform.system", lambda: "Windows")
    monkeypatch.setattr("clio_relay.bootstrap.platform.machine", lambda: "AMD64")
    monkeypatch.setattr("clio_relay.bootstrap._install_frp_from_release_archive", fake_install)
    monkeypatch.setattr(
        "clio_relay.bootstrap._assert_frp_pair",
        quarantine_destination,
    )

    with pytest.raises(ConfigurationError, match="cannot be hashed: quarantined"):
        install_local_frp(destination)

    assert not (destination / "frpc.exe").exists()
    assert not (destination / "frps.exe").exists()
    assert not list(destination.parent.glob(".clio-relay-frp-*"))


def test_local_frp_install_never_publishes_a_staged_hash_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "frp" / "bin"

    def fake_install(staging: Path, _version: str) -> None:
        (staging / "frpc.exe").write_bytes(b"wrong-frpc")
        (staging / "frps.exe").write_bytes(b"wrong-frps")

    monkeypatch.setattr("clio_relay.bootstrap.platform.system", lambda: "Windows")
    monkeypatch.setattr("clio_relay.bootstrap.platform.machine", lambda: "AMD64")
    monkeypatch.setattr("clio_relay.bootstrap._install_frp_from_release_archive", fake_install)

    with pytest.raises(ConfigurationError, match="SHA-256 mismatch"):
        install_local_frp(destination)

    assert not (destination / "frpc.exe").exists()
    assert not (destination / "frps.exe").exists()
    assert not list(destination.parent.glob(".clio-relay-frp-*"))


def test_bootstrap_refuses_dirty_git_checkout(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "clio-relay"\n',
        encoding="utf-8",
    )
    (tmp_path / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    (tmp_path / "tracked.txt").write_text("dirty\n", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="deploys git HEAD"):
        assert_clean_git_checkout(tmp_path)


def test_bootstrap_archive_uses_clean_git_checkout(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "clio-relay"\n',
        encoding="utf-8",
    )
    (tmp_path / "tracked.txt").write_text("clean\n", encoding="utf-8")
    (tmp_path / "jarvis-packages" / "clio_relay").mkdir(parents=True)
    (tmp_path / "jarvis-packages" / "clio_relay" / "README.md").write_text(
        "package\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )

    deployment = create_bootstrap_archive(
        source_root=tmp_path,
        archive=tmp_path / "bootstrap.tar",
    )

    assert deployment.install_spec == "$DEST"
    with tarfile.open(deployment.archive) as archive:
        names = archive.getnames()
    assert "tracked.txt" in names
    assert "jarvis-packages/clio_relay/README.md" in names


def test_bootstrap_archive_uses_packaged_assets_without_git_checkout(tmp_path: Path) -> None:
    deployment = create_bootstrap_archive(
        source_root=tmp_path / "not-a-repo",
        archive=tmp_path / "bootstrap.tar",
    )

    assert deployment.install_spec == f"clio-relay=={__version__}"
    with tarfile.open(deployment.archive) as archive:
        names = archive.getnames()
    assert any(name.startswith("jarvis-packages/clio_relay/") for name in names)
    assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)


def test_bootstrap_archive_can_include_local_relay_wheel(tmp_path: Path) -> None:
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    _write_test_relay_wheel(wheel)

    deployment = create_bootstrap_archive(
        source_root=tmp_path / "not-a-repo",
        archive=tmp_path / "bootstrap.tar",
        relay_wheel=wheel,
    )

    assert deployment.install_spec == f"$DEST/wheels/{wheel.name}"
    with tarfile.open(deployment.archive) as archive:
        names = archive.getnames()
    assert f"wheels/{wheel.name}" in names
    assert any(name.startswith("jarvis-packages/clio_relay/") for name in names)


def test_bootstrap_archive_ignores_unrelated_git_checkout(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "other-project"\n',
        encoding="utf-8",
    )
    (tmp_path / "unrelated.txt").write_text("do not deploy\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )

    deployment = create_bootstrap_archive(
        source_root=tmp_path,
        archive=tmp_path / "bootstrap.tar",
    )

    assert deployment.install_spec == f"clio-relay=={__version__}"
    with tarfile.open(deployment.archive) as archive:
        names = archive.getnames()
    assert "unrelated.txt" not in names
    assert any(name.startswith("jarvis-packages/clio_relay/") for name in names)
