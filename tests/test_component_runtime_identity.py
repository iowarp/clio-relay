from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Callable
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import clio_relay.component_runtime_identity as component_runtime_identity_module
from clio_relay.installation_receipt_models import (
    ComponentArtifactIdentity,
    NativeJarvisExecutionCapability,
)


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
            "execution_store.resolve_service_runtime_authority",
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

    def probe_record_closure(
        _python: str | None,
        _distribution_name: str,
        _expected_artifact: Path | None,
        *,
        environment: dict[str, str] | None = None,
    ) -> dict[str, object]:
        del environment
        return {
            "schema_version": "clio-relay.python-record-closure.v1",
            "verified": True,
            "tree_scanned": False,
            "tree_copied": False,
        }

    monkeypatch.setattr(
        component_runtime_identity_module.metadata, "distribution", find_distribution
    )
    monkeypatch.setattr(
        component_runtime_identity_module, "_probe_python_distribution", probe_execution
    )
    monkeypatch.setattr(
        component_runtime_identity_module,
        "_probe_python_distribution_record_closure",
        probe_record_closure,
    )
    monkeypatch.setattr(
        component_runtime_identity_module,
        "probe_jarvis_native_execution_capability",
        probe_capability,
    )
    monkeypatch.setattr(
        component_runtime_identity_module,
        "_jarvis_executable_matches_interpreter",
        match_jarvis_executable,
    )
    runtime_identity_probe_name = "_native_jarvis_component_runtime_identity"
    runtime_identity_probe = cast(
        Callable[[ComponentArtifactIdentity], dict[str, object]],
        getattr(component_runtime_identity_module, runtime_identity_probe_name),
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


def test_relay_execution_runtime_requires_exact_wheel_closure_and_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained JARVIS interpreter must prove the exact relay package runtime."""
    wheel = tmp_path / "clio_relay-1.5.0-py3-none-any.whl"
    wheel.write_bytes(b"verified-relay-wheel")
    import_verified = {"value": True}

    def probe_execution(_python: str | None, _distribution: str) -> dict[str, object]:
        return {
            "executable": sys.executable,
            "distribution": "clio-relay",
            "distribution_version": "1.5.0",
            "direct_url": wheel.as_uri(),
            "entry_points": [],
        }

    def probe_record_closure(
        _python: str | None,
        _distribution_name: str,
        _expected_artifact: Path | None,
        *,
        environment: dict[str, str] | None = None,
    ) -> dict[str, object]:
        del environment
        return {
            "schema_version": "clio-relay.python-record-closure.v1",
            "verified": True,
            "tree_scanned": False,
            "tree_copied": False,
        }

    def probe_imports(
        _python: str | None,
        *,
        expected_version: str | None,
    ) -> dict[str, object]:
        assert expected_version == "1.5.0"
        return {"verified": import_verified["value"], "error": None}

    monkeypatch.setattr(
        component_runtime_identity_module, "_probe_python_distribution", probe_execution
    )
    monkeypatch.setattr(
        component_runtime_identity_module,
        "_probe_python_distribution_record_closure",
        probe_record_closure,
    )
    monkeypatch.setattr(
        component_runtime_identity_module,
        "_probe_relay_execution_imports",
        probe_imports,
    )
    component = ComponentArtifactIdentity(
        distribution="clio-relay",
        distribution_version="1.5.0",
        install_spec=str(wheel),
        requested_source="wheel",
        artifact_filename=wheel.name,
        artifact_sha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
        runtime_artifact_path=str(wheel),
        runtime_interpreters={"provider": sys.executable, "execution": sys.executable},
    )

    identity = component_runtime_identity_module._relay_execution_runtime_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        component
    )

    assert identity["execution_runtime_verified"] is True
    assert identity["execution_record_closure_verified"] is True
    assert identity["execution_imports_verified"] is True

    import_verified["value"] = False
    rejected = component_runtime_identity_module._relay_execution_runtime_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        component
    )
    assert rejected["execution_runtime_verified"] is False
    assert rejected["execution_imports_verified"] is False
