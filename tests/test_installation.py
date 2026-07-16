from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from collections.abc import Callable
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import clio_relay.installation as installation_module
from clio_relay.errors import ConfigurationError
from clio_relay.installation import (
    CLIO_KIT_JARVIS_CONTRACT_ID,
    INSTALL_RECEIPT_PATH_ENV,
    INSTALL_RECEIPT_SCHEMA,
    ComponentArtifactIdentity,
    NativeJarvisExecutionCapability,
    PersistentUvToolIdentity,
    installation_info,
    load_install_receipt,
    probe_clio_kit_native_execution_contract,
    verify_distribution_file_source,
    verify_remote_clio_kit_native_execution_component,
    verify_remote_native_jarvis_component,
    verify_remote_worker_info,
    write_install_receipt,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    JARVIS_MCP_COMMAND_ENV,
    jarvis_mcp_command,
    jarvis_user_contract,
)
from clio_relay.validation_report import InstallSource, InstallSourceKind, SoftwareIdentity


def test_distribution_file_source_accepts_a_canonical_filesystem_alias(
    tmp_path: Path,
) -> None:
    canonical_home = tmp_path / "mnt" / "common" / "operator"
    canonical_home.mkdir(parents=True)
    wheel = canonical_home / "jarvis_cd-1.2.2-py3-none-any.whl"
    wheel.write_bytes(b"verified-wheel")
    lexical_home = tmp_path / "home" / "operator"
    lexical_home.parent.mkdir()
    try:
        lexical_home.symlink_to(canonical_home, target_is_directory=True)
        source = lexical_home / wheel.name
    except OSError:
        source = canonical_home / ".." / canonical_home.name / wheel.name

    resolved = verify_distribution_file_source(
        direct_url_text=json.dumps({"url": source.as_uri(), "archive_info": {}}),
        expected_artifact=wheel,
    )

    assert resolved == wheel.resolve()


@pytest.mark.parametrize(
    ("direct_url_text", "message"),
    [
        ("not-json", "not valid JSON"),
        ("[]", "must contain an object"),
        ("{}", "does not name a source URL"),
        (json.dumps({"url": "https://example.invalid/wheel.whl"}), "not a local file URL"),
        (json.dumps({"url": "file://remote.example/wheel.whl"}), "must not contain an authority"),
        (json.dumps({"url": "file:relative.whl"}), "path must be absolute"),
        (json.dumps({"url": "file:///wheel.whl?source=other"}), "query or fragment"),
        (json.dumps({"url": "file:///wheel.whl#other"}), "query or fragment"),
        (json.dumps({"url": "file://[invalid/wheel.whl"}), "source URL is not valid"),
    ],
)
def test_distribution_file_source_rejects_ambiguous_metadata(
    tmp_path: Path,
    direct_url_text: str,
    message: str,
) -> None:
    wheel = tmp_path / "jarvis_cd-1.2.2-py3-none-any.whl"
    wheel.write_bytes(b"verified-wheel")

    with pytest.raises(ConfigurationError, match=message):
        verify_distribution_file_source(
            direct_url_text=direct_url_text,
            expected_artifact=wheel,
        )


def test_distribution_file_source_rejects_a_different_existing_artifact(
    tmp_path: Path,
) -> None:
    expected = tmp_path / "expected.whl"
    other = tmp_path / "other.whl"
    expected.write_bytes(b"same-bytes")
    other.write_bytes(b"same-bytes")

    with pytest.raises(ConfigurationError, match="does not match the verified wheel"):
        verify_distribution_file_source(
            direct_url_text=json.dumps({"url": other.as_uri()}),
            expected_artifact=expected,
        )


def test_distribution_file_source_rejects_a_decoded_nul_path(tmp_path: Path) -> None:
    wheel = tmp_path / "jarvis_cd-1.2.2-py3-none-any.whl"
    wheel.write_bytes(b"verified-wheel")

    with pytest.raises(ConfigurationError, match="cannot be resolved"):
        verify_distribution_file_source(
            direct_url_text=json.dumps({"url": f"{wheel.as_uri()}%00"}),
            expected_artifact=wheel,
        )


def test_distribution_file_source_decodes_file_url_percent_escapes_once(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "jarvis%20cd.whl"
    wheel.write_bytes(b"verified-wheel")
    source_url = wheel.as_uri()
    assert "%2520" in source_url

    resolved = verify_distribution_file_source(
        direct_url_text=json.dumps({"url": source_url}),
        expected_artifact=wheel,
    )

    assert resolved == wheel.resolve()


def test_native_jarvis_runtime_accepts_canonical_source_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep provider and execution provenance true across one home-mount alias."""
    canonical_home = tmp_path / "mnt" / "common" / "operator"
    canonical_home.mkdir(parents=True)
    wheel = canonical_home / "jarvis_cd-1.2.2-py3-none-any.whl"
    wheel.write_bytes(b"verified-jarvis-wheel")
    visible_home = tmp_path / "home" / "operator"
    visible_home.parent.mkdir()
    try:
        visible_home.symlink_to(canonical_home, target_is_directory=True)
        visible_wheel = visible_home / wheel.name
    except OSError:
        alias_directory = canonical_home / "alias"
        alias_directory.mkdir()
        visible_wheel = alias_directory / ".." / wheel.name
    source_url = {"value": visible_wheel.as_uri()}

    def read_direct_url(_name: str) -> str:
        return json.dumps({"url": source_url["value"]})

    distribution = cast(
        metadata.Distribution,
        SimpleNamespace(
            name="jarvis-cd",
            version="1.2.2",
            entry_points=[],
            read_text=read_direct_url,
        ),
    )
    capability = NativeJarvisExecutionCapability(
        operations=[
            "execution_handle.progress",
            "pipeline.get_execution",
            "pipeline.get_execution_progress",
            "pipeline.run",
        ]
    )

    def probe_execution(_python: str | None, _distribution: str) -> dict[str, object]:
        return {
            "executable": sys.executable,
            "distribution": "jarvis-cd",
            "distribution_version": "1.2.2",
            "direct_url": source_url["value"],
            "entry_points": [],
        }

    def find_distribution(_name: str) -> metadata.Distribution:
        return distribution

    def probe_capability(_python: str | None) -> NativeJarvisExecutionCapability:
        return capability

    def match_jarvis_executable(
        _executable: str | None,
        _python: str | None,
        *,
        runtime_command: list[str],
    ) -> bool:
        return bool(runtime_command)

    monkeypatch.setattr(installation_module.metadata, "distribution", find_distribution)
    monkeypatch.setattr(installation_module, "_probe_python_distribution", probe_execution)
    monkeypatch.setattr(
        installation_module,
        "probe_jarvis_native_execution_capability",
        probe_capability,
    )
    monkeypatch.setattr(
        installation_module,
        "_jarvis_executable_matches_interpreter",
        match_jarvis_executable,
    )
    runtime_identity_probe_name = "_native_jarvis_component_runtime_identity"
    runtime_identity_probe = cast(
        Callable[[ComponentArtifactIdentity], dict[str, object]],
        getattr(installation_module, runtime_identity_probe_name),
    )
    component = ComponentArtifactIdentity(
        distribution="jarvis-cd",
        distribution_version="1.2.2",
        install_spec=str(wheel),
        requested_source="wheel",
        artifact_filename=wheel.name,
        artifact_sha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
        runtime_artifact_path=str(wheel.resolve()),
        runtime_command=[sys.executable, "-m", "jarvis_cd"],
        runtime_interpreters={"provider": sys.executable, "execution": sys.executable},
        runtime_executables={"jarvis": sys.executable},
        native_execution=capability,
    )

    identity = runtime_identity_probe(component)

    assert identity["provider_interpreter_verified"] is True
    assert identity["execution_source_verified"] is True
    assert identity["runtime_artifact_path_verified"] is True
    assert identity["execution_interpreter_verified"] is True
    assert identity["verified"] is True

    other = canonical_home / "other" / wheel.name
    other.parent.mkdir()
    other.write_bytes(wheel.read_bytes())
    source_url["value"] = other.as_uri()
    substituted = runtime_identity_probe(component)

    assert substituted["provider_interpreter_verified"] is False
    assert substituted["execution_source_verified"] is False
    assert substituted["runtime_artifact_path_verified"] is False
    assert substituted["execution_interpreter_verified"] is False
    assert substituted["verified"] is False


def test_jarvis_launcher_matches_a_uv_managed_python_symlink(tmp_path: Path) -> None:
    """Bind JARVIS to the venv bin directory without following Python out of it."""
    environment_bin = tmp_path / "environment" / "bin"
    environment_bin.mkdir(parents=True)
    python = environment_bin / ("python.exe" if os.name == "nt" else "python")
    jarvis = environment_bin / ("jarvis.exe" if os.name == "nt" else "jarvis")
    if os.name == "nt":
        shutil.copy2(sys.executable, python)
        shutil.copy2(sys.executable, jarvis)
        executable = jarvis
    else:
        managed_python = tmp_path / "uv" / "python" / "bin" / "python3.12"
        managed_python.parent.mkdir(parents=True)
        managed_python.symlink_to(Path(sys.executable).resolve())
        python.symlink_to(managed_python)
        jarvis.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        jarvis.chmod(0o755)
        external_bin = tmp_path / "external-bin"
        external_bin.mkdir()
        executable = external_bin / "jarvis"
        executable.symlink_to(jarvis)
        assert python.resolve().parent != environment_bin.resolve()

    matcher_name = "_jarvis_executable_matches_interpreter"
    matcher = cast(Callable[..., bool], getattr(installation_module, matcher_name))

    assert matcher(str(executable), str(python), runtime_command=[str(executable), "--help"])

    other = tmp_path / "other-bin" / executable.name
    other.parent.mkdir()
    if os.name == "nt":
        shutil.copy2(sys.executable, other)
    else:
        other.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        other.chmod(0o755)
    assert not matcher(str(other), str(python), runtime_command=[str(other), "--help"])


def test_install_receipt_binds_running_package_to_wheel_bytes(tmp_path: Path) -> None:
    wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    wheel.write_bytes(b"candidate-wheel")
    receipt_path = tmp_path / "install-receipt.json"

    receipt = write_install_receipt(
        install_spec=str(wheel),
        artifact_path=wheel,
        path=receipt_path,
        components={"jarvis-cd": "a" * 40, "clio-kit": "2.2.6"},
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                distribution_version="2.2.6",
                install_spec="clio-kit==2.2.6",
                requested_source="pypi",
                artifact_filename="clio_kit-2.2.6-py3-none-any.whl",
                artifact_sha256="c" * 64,
            )
        },
    )
    loaded = load_install_receipt(receipt_path)
    info = installation_info(receipt_path)

    assert receipt.schema_version == INSTALL_RECEIPT_SCHEMA
    assert receipt.requested_source == "wheel"
    assert receipt.artifact_filename == wheel.name
    assert receipt.artifact_sha256 == hashlib.sha256(b"candidate-wheel").hexdigest()
    assert loaded == receipt
    assert loaded.components == {"jarvis-cd": "a" * 40, "clio-kit": "2.2.6"}
    assert loaded.component_artifacts["clio-kit"].requested_source == "pypi"
    assert info["receipt_matches_install"] is True
    assert not list(tmp_path.glob("*.tmp"))


def test_install_receipt_labels_exact_version_spec_as_pypi(tmp_path: Path) -> None:
    wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    wheel.write_bytes(b"published-wheel")

    receipt = write_install_receipt(
        install_spec="clio-relay==1.0.0",
        artifact_path=wheel,
        path=tmp_path / "receipt.json",
    )

    assert receipt.requested_source == "pypi"


def test_installation_info_uses_verified_uv_tool_receipt_without_bootstrap_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_bootstrap = tmp_path / "missing-install-receipt.json"
    uv_receipt = tmp_path / "tools" / "clio-relay" / "uv-receipt.toml"
    uv_receipt.parent.mkdir(parents=True)
    uv_receipt.write_text("[tool]\n", encoding="utf-8")
    version = metadata.version("clio-relay")
    source_url = (
        "https://github.com/iowarp/clio-relay/releases/download/"
        f"v{version}/clio_relay-{version}-py3-none-any.whl"
    )
    source = InstallSource(
        kind=InstallSourceKind.WHEEL,
        detected_kind=InstallSourceKind.WHEEL,
        reference=source_url,
        launcher="uv-tool",
        package_path=str(tmp_path / "site-packages" / "clio_relay"),
        distribution_version=version,
        artifact_sha256="a" * 64,
        direct_url={"url": source_url, "archive_info": {}},
        artifact_identity_verified=True,
        released_artifact=True,
        launcher_verified=True,
        launcher_receipt={
            "verified": True,
            "uv_tool_receipt": {
                "path": str(uv_receipt),
                "verified": True,
            },
        },
    )

    monkeypatch.delenv(INSTALL_RECEIPT_PATH_ENV, raising=False)
    monkeypatch.setattr(
        installation_module,
        "default_install_receipt_path",
        lambda: missing_bootstrap,
    )

    def detect_source(**_kwargs: object) -> InstallSource:
        return source

    monkeypatch.setattr(
        installation_module,
        "detect_install_source",
        detect_source,
    )

    info = installation_info()
    receipt = cast(dict[str, object], info["receipt"])
    install_source = cast(dict[str, object], info["install_source"])

    assert info["receipt_origin"] == "uv-tool"
    assert info["receipt_matches_install"] is True
    assert receipt["install_spec"] == source_url
    assert receipt["requested_source"] == "wheel"
    assert receipt["artifact_sha256"] == "a" * 64
    assert install_source["artifact_identity_verified"] is True
    assert install_source["launcher_verified"] is True
    assert install_source["released_artifact"] is True
    assert info["component_runtime"] == {}


def test_remote_native_jarvis_component_requires_runtime_capability_provenance(
    tmp_path: Path,
) -> None:
    capability = NativeJarvisExecutionCapability(
        operations=[
            "execution_handle.progress",
            "pipeline.get_execution",
            "pipeline.get_execution_progress",
            "pipeline.run",
        ]
    )
    receipt = write_install_receipt(
        install_spec="checkout",
        path=tmp_path / "receipt.json",
        components={"jarvis-cd": "1.2.2"},
        component_artifacts={
            "jarvis-cd": ComponentArtifactIdentity(
                distribution="jarvis_cd",
                distribution_version="1.2.2",
                install_spec=(
                    "https://github.com/grc-iit/jarvis-cd/releases/download/"
                    "v1.2.2/jarvis_cd-1.2.2-py3-none-any.whl"
                ),
                requested_source="github_release",
                artifact_filename="jarvis_cd-1.2.2-py3-none-any.whl",
                artifact_sha256="a" * 64,
                runtime_artifact_path="/home/test/wheels/jarvis_cd-1.2.2-py3-none-any.whl",
                runtime_command=["/home/test/jarvis-venv/bin/jarvis", "--help"],
                runtime_interpreters={
                    "provider": "/home/test/relay-venv/bin/python",
                    "execution": "/home/test/jarvis-venv/bin/python",
                },
                runtime_executables={"jarvis": "/home/test/jarvis-venv/bin/jarvis"},
                native_execution=capability,
                entry_points=["clio_relay.package_progress_adapters:lammps"],
            )
        },
    )
    runtime = {
        "verified": True,
        "distribution_identity_verified": True,
        "entry_points_visible": True,
        "runtime_artifact_path_verified": True,
        "artifact_sha256_verified": True,
        "provider_interpreter_verified": True,
        "execution_interpreter_verified": True,
        "execution_distribution_identity_verified": True,
        "execution_entry_points_visible": True,
        "execution_source_verified": True,
        "jarvis_executable_verified": True,
        "provider_native_execution_capability_verified": True,
        "execution_native_execution_capability_verified": True,
        "native_execution_capability_verified": True,
    }
    info: dict[str, object] = {"installation": {"component_runtime": {"jarvis-cd": runtime}}}

    assert verify_remote_native_jarvis_component(info, receipt) == runtime
    runtime["native_execution_capability_verified"] = False
    with pytest.raises(ConfigurationError, match="native_execution_capability_verified"):
        verify_remote_native_jarvis_component(info, receipt)


def test_legacy_progress_entry_point_cannot_replace_native_jarvis_capability(
    tmp_path: Path,
) -> None:
    receipt = write_install_receipt(
        install_spec="checkout",
        path=tmp_path / "receipt.json",
        component_artifacts={
            "jarvis-cd": ComponentArtifactIdentity(
                distribution="jarvis_cd",
                distribution_version="1.2.2",
                install_spec=(
                    "https://github.com/grc-iit/jarvis-cd/releases/download/"
                    "v1.2.2/jarvis_cd-1.2.2-py3-none-any.whl"
                ),
                requested_source="github_release",
                artifact_sha256="a" * 64,
                runtime_artifact_path="/home/test/jarvis_cd.whl",
                runtime_interpreters={
                    "provider": "/home/test/relay/bin/python",
                    "execution": "/home/test/jarvis/bin/python",
                },
                runtime_executables={"jarvis": "/home/test/jarvis/bin/jarvis"},
                entry_points=["clio_relay.package_progress_adapters:lammps"],
            )
        },
    )

    with pytest.raises(ConfigurationError, match="incomplete provenance"):
        verify_remote_native_jarvis_component(
            {"installation": {"component_runtime": {"jarvis-cd": {"verified": True}}}},
            receipt,
        )


def test_remote_clio_kit_component_requires_receipt_bound_native_contract(
    tmp_path: Path,
) -> None:
    capability = NativeJarvisExecutionCapability(
        operations=[
            "jarvis_get_execution",
            "jarvis_run",
        ],
        contract_id=CLIO_KIT_JARVIS_CONTRACT_ID,
        contract_schema_version="clio-kit.mcp-user-contract.v1",
        contract_sha256="b" * 64,
    )
    receipt = write_install_receipt(
        install_spec="checkout",
        path=tmp_path / "receipt.json",
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                distribution_version="2.3.1",
                install_spec="clio-kit==2.3.1",
                requested_source="pypi",
                artifact_sha256="c" * 64,
                runtime_artifact_path="/home/test/clio_kit.whl",
                runtime_command=[
                    "uvx",
                    "--from",
                    "/home/test/clio_kit.whl",
                    "clio-kit",
                    "mcp-server",
                    "jarvis",
                ],
                native_execution=capability,
            )
        },
    )
    runtime = {
        "artifact_identity_verified": True,
        "command_matches_receipt": True,
        "native_execution_capability_verified": True,
        "native_execution_capability": capability.model_dump(mode="json"),
    }
    info: dict[str, object] = {"installation": {"component_runtime": {"clio-kit": runtime}}}

    assert verify_remote_clio_kit_native_execution_component(info, receipt) == runtime
    runtime["native_execution_capability"] = {
        **capability.model_dump(mode="json"),
        "contract_sha256": "d" * 64,
    }
    with pytest.raises(ConfigurationError, match="changed from its receipt"):
        verify_remote_clio_kit_native_execution_component(info, receipt)


def test_clio_kit_probe_requires_unified_progress_and_artifact_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _clio_kit_jarvis_contract_document()

    def probe(_command: list[str], *, label: str) -> dict[str, object]:
        assert label == "clio-kit native execution contract"
        return document

    monkeypatch.setattr(
        installation_module,
        "_run_json_probe",
        probe,
    )

    capability = probe_clio_kit_native_execution_contract(
        ["/home/user/.local/bin/clio-kit", "mcp-server", "jarvis"]
    )

    assert capability.operations == ["jarvis_get_execution", "jarvis_run"]
    assert capability.contract_sha256 == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256


def test_clio_kit_probe_rejects_execution_query_without_artifact_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _clio_kit_jarvis_contract_document()
    tools = cast(list[dict[str, object]], document["tools"])
    query = next(tool for tool in tools if tool["name"] == "jarvis_get_execution")
    input_schema = cast(dict[str, object], query["inputSchema"])
    properties = cast(dict[str, object], input_schema["properties"])
    properties.pop("artifacts")

    def probe(_command: list[str], *, label: str) -> dict[str, object]:
        assert label == "clio-kit native execution contract"
        return document

    monkeypatch.setattr(
        installation_module,
        "_run_json_probe",
        probe,
    )

    with pytest.raises(ConfigurationError, match="query surface did not match"):
        probe_clio_kit_native_execution_contract(
            ["/home/user/.local/bin/clio-kit", "mcp-server", "jarvis"]
        )


def test_remote_worker_identity_is_bound_to_fresh_running_endpoint(tmp_path: Path) -> None:
    wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    wheel.write_bytes(b"candidate-wheel")
    receipt_path = tmp_path / "receipt.json"
    receipt = write_install_receipt(
        install_spec=str(wheel),
        artifact_path=wheel,
        path=receipt_path,
    )
    installation = installation_info(receipt_path)
    runtime: dict[str, object] = {
        "schema_version": "clio-relay.worker-runtime-info.v1",
        "cluster": "ares",
        "fresh": True,
        "process_running": True,
        "identity_matches_current": True,
        "running": True,
        "scheduler_provider": "slurm",
        "endpoint": {
            "role": "worker",
            "cluster": "ares",
            "pid": 123,
            "metadata": {"scheduler_provider": "slurm"},
        },
        "installation": installation,
        "endpoint_installation": installation,
        "target_identity": {
            "verified": True,
            "hostname": "ares-login",
            "ssh_host_key_sha256": ["SHA256:test"],
            "scheduler_cluster_name": "ares",
        },
    }

    verified = verify_remote_worker_info(
        runtime,
        expected_cluster="ares",
        expected_version=receipt.distribution_version,
        expected_software=SoftwareIdentity.model_validate(installation["software"]),
        expected_artifact_sha256=receipt.artifact_sha256,
        expected_source="wheel",
    )

    assert verified == receipt
    runtime["identity_matches_current"] = False
    with pytest.raises(ConfigurationError, match="identity_matches_current"):
        verify_remote_worker_info(
            runtime,
            expected_cluster="ares",
            expected_version=receipt.distribution_version,
            expected_software=SoftwareIdentity.model_validate(installation["software"]),
            expected_artifact_sha256=receipt.artifact_sha256,
            expected_source="wheel",
        )


def test_jarvis_mcp_defaults_to_persistent_receipt_bound_clio_kit_tool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay_wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    relay_wheel.write_bytes(b"relay-wheel")
    clio_kit_wheel = tmp_path / "clio_kit-2.3.1-py3-none-any.whl"
    clio_kit_wheel.write_bytes(b"clio-kit-wheel")
    tool = tmp_path / "clio-kit.exe"
    tool.write_bytes(b"persistent-tool")
    uv = tmp_path / "uv.exe"
    uv.write_bytes(b"uv")
    persistent_tool = PersistentUvToolIdentity(
        uv_executable=str(uv.resolve()),
        uv_version="0.11.28",
        uv_executable_sha256=hashlib.sha256(b"uv").hexdigest(),
        tool_directory=str(tmp_path / "tools"),
        tool_bin_directory=str(tmp_path),
        environment_prefix=str(tmp_path / "tools" / "clio-kit"),
        provider_interpreter=sys.executable,
        provider_interpreter_sha256="a" * 64,
        tool_executable=str(tool.resolve()),
        tool_executable_resolved=str(tool.resolve()),
        tool_executable_sha256=hashlib.sha256(b"persistent-tool").hexdigest(),
        distribution_console_script_path=str(tool.resolve()),
        distribution_console_script_sha256=hashlib.sha256(b"persistent-tool").hexdigest(),
        uv_receipt_path=str(tmp_path / "tools" / "clio-kit" / "uv-receipt.toml"),
        uv_receipt_sha256="d" * 64,
        distribution="clio-kit",
        distribution_version="2.3.1",
        distribution_metadata_path=str(tmp_path / "clio-kit.dist-info"),
        entry_point="clio-kit",
        source_artifact_path=str(clio_kit_wheel.resolve()),
        source_artifact_sha256=hashlib.sha256(b"clio-kit-wheel").hexdigest(),
        record_path=str(tmp_path / "clio-kit.dist-info" / "RECORD"),
        record_sha256="b" * 64,
        runtime_closure_sha256="c" * 64,
        runtime_file_count=10,
        runtime_bytes=1_024,
        pyvenv_uv_version="0.11.28",
    )
    command = [str(tool), "mcp-server", "jarvis"]
    receipt_path = tmp_path / "install-receipt.json"
    write_install_receipt(
        install_spec=str(relay_wheel),
        artifact_path=relay_wheel,
        path=receipt_path,
        components={"clio-kit": "2.3.1"},
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                distribution_version="2.3.1",
                install_spec="clio-kit==2.3.1",
                requested_source="pypi",
                artifact_filename=clio_kit_wheel.name,
                artifact_sha256=hashlib.sha256(b"clio-kit-wheel").hexdigest(),
                runtime_artifact_path=str(clio_kit_wheel),
                runtime_command=command,
                runtime_interpreters={"provider": sys.executable},
                runtime_executables={"clio-kit": str(tool), "uv": str(uv)},
                persistent_tool=persistent_tool,
            )
        },
    )

    def persistent_identity(**_kwargs: object) -> PersistentUvToolIdentity:
        return persistent_tool

    monkeypatch.setattr(
        "clio_relay.installation.probe_persistent_uv_tool_identity",
        persistent_identity,
    )
    monkeypatch.setenv(INSTALL_RECEIPT_PATH_ENV, str(receipt_path))
    monkeypatch.delenv(JARVIS_MCP_COMMAND_ENV, raising=False)

    assert jarvis_mcp_command() == command
    runtime = cast(dict[str, dict[str, object]], installation_info()["component_runtime"])
    assert runtime["clio-kit"]["artifact_identity_verified"] is True
    assert runtime["clio-kit"]["command_matches_receipt"] is True
    assert runtime["clio-kit"]["launcher"] == "uv tool"
    assert runtime["clio-kit"]["persistent_tool_verified"] is True


def test_receipt_bound_jarvis_mcp_refuses_changed_clio_kit_wheel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay_wheel = tmp_path / "clio_relay.whl"
    relay_wheel.write_bytes(b"relay")
    clio_kit_wheel = tmp_path / "clio_kit.whl"
    clio_kit_wheel.write_bytes(b"expected")
    command = [str(tmp_path / "clio-kit"), "mcp-server", "jarvis"]
    receipt_path = tmp_path / "receipt.json"
    write_install_receipt(
        install_spec=str(relay_wheel),
        artifact_path=relay_wheel,
        path=receipt_path,
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                distribution_version="2.3.1",
                install_spec="clio-kit==2.3.1",
                requested_source="pypi",
                artifact_filename=clio_kit_wheel.name,
                artifact_sha256=hashlib.sha256(b"expected").hexdigest(),
                runtime_artifact_path=str(clio_kit_wheel),
                runtime_command=command,
                runtime_interpreters={"provider": sys.executable},
                runtime_executables={"clio-kit": command[0]},
            )
        },
    )
    clio_kit_wheel.write_bytes(b"changed")
    monkeypatch.setenv(INSTALL_RECEIPT_PATH_ENV, str(receipt_path))
    monkeypatch.delenv(JARVIS_MCP_COMMAND_ENV, raising=False)

    with pytest.raises(ValueError, match="SHA-256 does not match"):
        jarvis_mcp_command()


def test_jarvis_mcp_override_cannot_masquerade_as_receipt_bound_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relay_wheel = tmp_path / "clio_relay.whl"
    relay_wheel.write_bytes(b"relay")
    clio_kit_wheel = tmp_path / "clio_kit.whl"
    clio_kit_wheel.write_bytes(b"component")
    receipt_command = [str(tmp_path / "clio-kit"), "mcp-server", "jarvis"]
    receipt_path = tmp_path / "receipt.json"
    write_install_receipt(
        install_spec=str(relay_wheel),
        artifact_path=relay_wheel,
        path=receipt_path,
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                distribution_version="2.3.1",
                install_spec="clio-kit==2.3.1",
                requested_source="pypi",
                artifact_filename=clio_kit_wheel.name,
                artifact_sha256=hashlib.sha256(b"component").hexdigest(),
                runtime_artifact_path=str(clio_kit_wheel),
                runtime_command=receipt_command,
                runtime_interpreters={"provider": sys.executable},
                runtime_executables={"clio-kit": receipt_command[0]},
            )
        },
    )
    override = [str(tmp_path / "other-clio-kit"), "mcp-server", "jarvis"]
    monkeypatch.setenv(INSTALL_RECEIPT_PATH_ENV, str(receipt_path))
    monkeypatch.setenv(JARVIS_MCP_COMMAND_ENV, json.dumps(override))

    assert jarvis_mcp_command() == override
    runtime = cast(dict[str, dict[str, object]], installation_info()["component_runtime"])[
        "clio-kit"
    ]
    assert runtime["command_matches_receipt"] is False
    assert runtime["artifact_identity_verified"] is False


def _clio_kit_jarvis_contract_document() -> dict[str, object]:
    tools = [
        {
            "name": name,
            "title": None,
            "description": definition["description"],
            "inputSchema": definition["inputSchema"],
            "outputSchema": definition["outputSchema"],
            "annotations": definition["annotations"],
        }
        for name, definition in sorted(jarvis_user_contract().items())
    ]
    return {
        "schema_version": "clio-kit.mcp-user-contract.v1",
        "contract_id": CLIO_KIT_JARVIS_CONTRACT_ID,
        "contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
        "tools": tools,
    }
