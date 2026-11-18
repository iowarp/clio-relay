"""Cross-repository checks for clio-kit's shipped locked-server contracts."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import cast

import pytest

from clio_relay.errors import ConfigurationError
from clio_relay.installation import (
    ComponentArtifactIdentity,
    probe_persistent_uv_tool_identity,
    write_install_receipt,
)
from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_runtime_identity,
)


def _verified_locked_jarvis_runtime() -> dict[str, object]:
    """Return complete receipt evidence for the relay's built-in JARVIS child."""
    expectation = jarvis_cd_lock_binding_expectation()
    return {
        "schema_version": "clio-kit.locked-server.v4",
        "server_name": "jarvis",
        "locked_runtime_verified": True,
        "jarvis_cd_lock_binding": {
            "schema_version": "clio-relay.jarvis-cd-lock-binding.v1",
            "dependency": "jarvis-cd",
            "verified": True,
            "error": None,
            "expected_version": expectation["version"],
            "expected_url": expectation["url"],
            "expected_sha256": expectation["sha256"],
            "observed_version": expectation["version"],
            "observed_source_url": expectation["url"],
            "observed_wheel_url": expectation["url"],
            "observed_wheel_sha256": expectation["sha256"],
            "jarvis_mcp_package_entry_count": 1,
            "resolved_dependency_entry_count": 1,
            "observed_resolved_dependency_entries": [{"name": "jarvis-cd"}],
            "metadata_requirement_entry_count": 1,
            "observed_metadata_requirement_entries": [
                {"name": "jarvis-cd", "url": expectation["url"]}
            ],
            "observed_metadata_requirement_urls": [expectation["url"]],
            "package_entry_count": 1,
            "wheel_entry_count": 1,
        },
    }


UV_TOOL_PROBE_VERSION = "0.0.0"


def _build_uv_tool_layout_probe_wheel(tmp_path: Path) -> Path:
    """Build a dependency-free wheel for exercising uv tool layout and provenance."""
    distribution = "clio_kit"
    package = "clio_kit_probe"
    dist_info = f"{distribution}-{UV_TOOL_PROBE_VERSION}.dist-info"
    wheel = tmp_path / f"{distribution}-{UV_TOOL_PROBE_VERSION}-py3-none-any.whl"
    members = {
        f"{package}/__init__.py": (
            b"def main() -> None:\n"
            b'    """Run the uv tool layout probe entry point."""\n'
            b"    return None\n"
        ),
        f"{dist_info}/METADATA": (
            "Metadata-Version: 2.1\n"
            "Name: clio-kit\n"
            f"Version: {UV_TOOL_PROBE_VERSION}\n"
            "Summary: Dependency-free clio-relay uv tool layout probe\n"
            "\n"
        ).encode(),
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\n"
            b"Generator: clio-relay-test\n"
            b"Root-Is-Purelib: true\n"
            b"Tag: py3-none-any\n"
            b"\n"
        ),
        f"{dist_info}/entry_points.txt": (
            f"[console_scripts]\nclio-kit = {package}:main\n"
        ).encode(),
    }
    record_path = f"{dist_info}/RECORD"
    record_rows = [
        f"{name},sha256={base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b'=').decode('ascii')},{len(payload)}"
        for name, payload in sorted(members.items())
    ]
    members[record_path] = ("\n".join([*record_rows, f"{record_path},,"]) + "\n").encode()
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in sorted(members.items()):
            archive.writestr(name, payload)
    return wheel


def test_install_receipt_exposes_verified_locked_jarvis_dependency_edge(
    tmp_path: Path,
) -> None:
    locked_server_runtime = _verified_locked_jarvis_runtime()
    receipt_path = tmp_path / "install-receipt.json"
    receipt = write_install_receipt(
        install_spec="checkout",
        path=receipt_path,
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                install_spec="clio-kit.whl",
                requested_source="wheel",
                runtime_command=["clio-kit", "mcp-server", "jarvis"],
                locked_server_runtime=locked_server_runtime,
            )
        },
    )

    runtime_identity = jarvis_mcp_runtime_identity(receipt)
    persisted = json.loads(receipt_path.read_text(encoding="utf-8"))

    assert runtime_identity["locked_server_runtime_verified"] is True
    assert runtime_identity["locked_server_runtime"] == locked_server_runtime
    assert (
        persisted["component_artifacts"]["clio-kit"]["locked_server_runtime"]
        == locked_server_runtime
    )

    receipt_runtime = cast(
        dict[str, object],
        receipt.component_artifacts["clio-kit"].locked_server_runtime,
    )
    binding = cast(dict[str, object], receipt_runtime["jarvis_cd_lock_binding"])
    binding["resolved_dependency_entry_count"] = 0
    invalid_runtime_identity = jarvis_mcp_runtime_identity(receipt)
    assert invalid_runtime_identity["locked_server_runtime_verified"] is False
    assert invalid_runtime_identity["artifact_identity_verified"] is False

    binding["resolved_dependency_entry_count"] = 1
    receipt_runtime["schema_version"] = "clio-kit.locked-server.v5"
    assert jarvis_mcp_runtime_identity(receipt)["locked_server_runtime_verified"] is False


def test_real_uv_tool_install_binds_external_launcher_to_receipt_and_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove uv's external launcher is bound without pretending it is in the wheel RECORD."""
    uv = shutil.which("uv")
    assert uv is not None
    probe_wheel = _build_uv_tool_layout_probe_wheel(tmp_path)
    tool_directory = tmp_path / "tools"
    tool_bin_directory = tmp_path / "bin"
    monkeypatch.setenv("UV_TOOL_DIR", str(tool_directory))
    monkeypatch.setenv("UV_TOOL_BIN_DIR", str(tool_bin_directory))
    monkeypatch.setenv("UV_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("UV_LINK_MODE", "copy")
    completed = subprocess.run(
        [
            uv,
            "tool",
            "install",
            "--force",
            "--python",
            sys.executable,
            "--offline",
            "--no-index",
            "--no-config",
            str(probe_wheel),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    executable = tool_bin_directory / ("clio-kit.exe" if os.name == "nt" else "clio-kit")
    environment = tool_directory / "clio-kit"
    provider = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

    identity = probe_persistent_uv_tool_identity(
        uv_executable=uv,
        tool_executable=str(executable),
        provider_interpreter=str(provider),
        source_artifact=probe_wheel,
        distribution="clio-kit",
        distribution_version=UV_TOOL_PROBE_VERSION,
        entry_point="clio-kit",
    )

    assert Path(identity.tool_executable) == executable.absolute()
    assert Path(identity.tool_executable).parent == tool_bin_directory.absolute()
    assert Path(identity.provider_interpreter) == provider.absolute()
    assert Path(identity.distribution_console_script_path).is_relative_to(environment.resolve())
    assert identity.tool_executable_sha256 == identity.distribution_console_script_sha256
    assert Path(identity.uv_receipt_path) == (environment / "uv-receipt.toml").resolve()
    receipt = Path(identity.uv_receipt_path).read_text(encoding="utf-8")
    assert str(executable).replace("\\", "/") in receipt
    assert probe_wheel.as_posix() in receipt

    monkeypatch.delenv("CLIO_RELAY_JARVIS_MCP_COMMAND", raising=False)
    install_receipt = write_install_receipt(
        install_spec="checkout",
        path=tmp_path / "install-receipt.json",
        components={"clio-kit": UV_TOOL_PROBE_VERSION},
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                distribution_version=UV_TOOL_PROBE_VERSION,
                install_spec=str(probe_wheel),
                requested_source="wheel",
                artifact_filename=probe_wheel.name,
                artifact_sha256=hashlib.sha256(probe_wheel.read_bytes()).hexdigest(),
                runtime_artifact_path=str(probe_wheel),
                runtime_command=[str(executable), "mcp-server", "jarvis"],
                runtime_interpreters={"provider": str(provider)},
                runtime_executables={"clio-kit": str(executable), "uv": uv},
                persistent_tool=identity,
                locked_server_runtime=_verified_locked_jarvis_runtime(),
            )
        },
    )
    with monkeypatch.context() as unrelated_environment:
        unrelated_environment.setenv("UV_TOOL_DIR", str(tmp_path / "unrelated-tools"))
        unrelated_environment.setenv("UV_TOOL_BIN_DIR", str(tmp_path / "unrelated-bin"))
        runtime_identity = jarvis_mcp_runtime_identity(install_receipt)
    assert runtime_identity["persistent_tool_verified"] is True
    assert runtime_identity["artifact_identity_verified"] is True

    internal_launcher = Path(identity.distribution_console_script_path)
    executable.unlink()
    executable.write_bytes(b"substituted-external-launcher")
    if os.name != "nt":
        executable.chmod(0o755)
    with pytest.raises(ConfigurationError, match="launcher"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )

    shutil.copy2(internal_launcher, executable)
    receipt_path = Path(identity.uv_receipt_path)
    receipt_payload = receipt_path.read_bytes()
    receipt_text = receipt_payload.decode("utf-8")
    recorded_executable = str(executable).replace("\\", "/")
    substituted_executable = str(tool_bin_directory / "other-clio-kit").replace("\\", "/")
    assert receipt_text.count(recorded_executable) == 1
    receipt_path.write_text(
        receipt_text.replace(recorded_executable, substituted_executable),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="receipt does not own"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )
    receipt_path.write_bytes(receipt_payload)

    recorded_source = probe_wheel.as_posix()
    assert receipt_text.count(recorded_source) == 1
    other_wheel = tmp_path / "other" / probe_wheel.name
    other_wheel.parent.mkdir()
    other_wheel.write_bytes(probe_wheel.read_bytes())
    for substituted_source in (
        other_wheel.as_posix(),
        (tmp_path / "missing" / probe_wheel.name).as_posix(),
        "relative-wheel.whl",
        tmp_path.as_posix(),
    ):
        receipt_path.write_text(
            receipt_text.replace(recorded_source, substituted_source),
            encoding="utf-8",
        )
        with pytest.raises(ConfigurationError, match="does not bind the source wheel"):
            probe_persistent_uv_tool_identity(
                uv_executable=uv,
                tool_executable=str(executable),
                provider_interpreter=str(provider),
                source_artifact=probe_wheel,
                distribution="clio-kit",
                distribution_version=UV_TOOL_PROBE_VERSION,
                entry_point="clio-kit",
            )
    receipt_path.write_text("[tool\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="receipt is invalid"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )
    receipt_path.write_bytes(receipt_payload)

    module_path = Path(
        subprocess.run(
            [
                str(provider),
                "-I",
                "-c",
                "import clio_kit_probe; print(clio_kit_probe.__file__)",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    )
    module_path.write_bytes(module_path.read_bytes() + b"\n# substituted\n")
    with pytest.raises(ConfigurationError, match="RECORD member digest mismatch"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )


def test_real_uv_tool_install_accepts_canonical_mount_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept one uv tool whose visible home path resolves through a mount alias."""
    uv = shutil.which("uv")
    assert uv is not None
    built_wheel = _build_uv_tool_layout_probe_wheel(tmp_path)
    canonical_root = tmp_path / "canonical-home"
    canonical_source = canonical_root / "source"
    canonical_source.mkdir(parents=True)
    canonical_wheel = canonical_source / built_wheel.name
    shutil.copy2(built_wheel, canonical_wheel)

    if os.name == "nt":
        intermediate = canonical_root / "lexical-alias"
        intermediate.mkdir()
        visible_root = intermediate / ".."
    else:
        visible_root = tmp_path / "visible-home"
        visible_root.symlink_to(canonical_root, target_is_directory=True)
        assert visible_root.absolute() != visible_root.resolve()

    visible_wheel = visible_root / "source" / built_wheel.name
    tool_directory = visible_root / "tools"
    tool_bin_directory = visible_root / "bin"
    monkeypatch.setenv("UV_TOOL_DIR", str(tool_directory))
    monkeypatch.setenv("UV_TOOL_BIN_DIR", str(tool_bin_directory))
    monkeypatch.setenv("UV_CACHE_DIR", str(visible_root / "cache"))
    monkeypatch.setenv("UV_LINK_MODE", "copy")
    completed = subprocess.run(
        [
            uv,
            "tool",
            "install",
            "--force",
            "--python",
            sys.executable,
            "--offline",
            "--no-index",
            "--no-config",
            str(visible_wheel),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    executable = tool_bin_directory / ("clio-kit.exe" if os.name == "nt" else "clio-kit")
    environment = tool_directory / "clio-kit"
    provider = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    direct_url = json.loads(
        subprocess.run(
            [
                str(provider),
                "-I",
                "-c",
                (
                    "from importlib.metadata import distribution; "
                    "print(distribution('clio-kit').read_text('direct_url.json'))"
                ),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
    )
    receipt_text = (environment / "uv-receipt.toml").read_text(encoding="utf-8")
    if os.name != "nt":
        assert direct_url["url"] == visible_wheel.as_uri()
        assert visible_wheel.as_posix() in receipt_text
        assert provider.absolute().is_relative_to(environment.absolute())
        assert provider.parent.resolve().is_relative_to(environment.resolve())

    identity = probe_persistent_uv_tool_identity(
        uv_executable=uv,
        tool_executable=str(executable),
        provider_interpreter=str(provider),
        source_artifact=visible_wheel,
        distribution="clio-kit",
        distribution_version=UV_TOOL_PROBE_VERSION,
        entry_point="clio-kit",
    )

    assert Path(identity.environment_prefix) == environment.resolve()
    assert Path(identity.provider_interpreter).parent.resolve() == provider.parent.resolve()
    assert Path(identity.source_artifact_path) == canonical_wheel.resolve()
    assert Path(identity.uv_receipt_path).is_relative_to(environment.resolve())
