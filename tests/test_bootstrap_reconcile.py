"""Focused contracts for idempotent, crash-safe bootstrap reconciliation."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import yaml

import clio_relay.bootstrap_reconcile as bootstrap_reconcile_module
from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
    bootstrap_invocation_lock,
    execution_environment_identity,
    inspect_exact_bootstrap_noop,
    inspect_jarvis_state,
    inspect_prepared_generation,
    jarvis_wrapper_payload,
    make_bootstrap_receipt,
    plan_bootstrap_reconcile,
    reconcile_managed_jarvis_repository,
    repair_managed_jarvis_binding,
    validate_jarvis_builtin_result,
    write_jarvis_wrapper,
)
from clio_relay.errors import ConfigurationError


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _desired(*, uv_sha256: str, frpc_sha256: str, frps_sha256: str) -> BootstrapDesiredState:
    return BootstrapDesiredState(
        cluster="cluster-a",
        core_dir="~/.local/share/clio-relay/core",
        spool_dir="~/.local/share/clio-relay/spool",
        worker_service="clio-relay-endpoint-cluster-a.service",
        relay_install_spec="clio-relay==1.5.0",
        relay_artifact_sha256="a" * 64,
        relay_source_identity=f"wheel:sha256:{'a' * 64}",
        frp_version="0.69.1",
        frpc_sha256=frpc_sha256,
        frps_sha256=frps_sha256,
        uv_version="0.11.28",
        uv_sha256=uv_sha256,
        jarvis_util_commit="commit",
        jarvis_cd_version="1.4.4",
        jarvis_cd_wheel_url="https://example.test/jarvis.whl",
        jarvis_cd_wheel_sha256="c" * 64,
        clio_kit_install_spec="https://example.test/clio-kit.whl",
        clio_kit_version="2.3.1",
        clio_kit_artifact_sha256="d" * 64,
        agent_adapter="exec",
    )


def _write_jarvis_state(home: Path, desired: BootstrapDesiredState) -> tuple[Path, bytes, bytes]:
    root = home / ".ppi-jarvis"
    root.mkdir(parents=True)
    for relative in (
        ".local/share/clio-relay/jarvis-config",
        ".local/share/clio-relay/jarvis-private",
        ".local/share/clio-relay/jarvis-shared",
        ".local/share/clio-relay/managed-jarvis-repo",
    ):
        (home / relative).mkdir(parents=True)
    config = {
        "config_dir": str((home / ".local/share/clio-relay/jarvis-config").resolve()),
        "private_dir": str((home / ".local/share/clio-relay/jarvis-private").resolve()),
        "shared_dir": str((home / ".local/share/clio-relay/jarvis-shared").resolve()),
        "current_pipeline": "operator-pipeline",
        "hostfile": "/operator/hostfile",
        "operator_extension": {"preserve": True},
    }
    config_bytes = yaml.safe_dump(config, sort_keys=False).encode()
    repos_bytes = yaml.safe_dump(
        {
            "repos": [
                str((home / ".local/share/clio-relay/managed-jarvis-repo").absolute()),
                "/operator/clio_relay",
            ]
        },
        sort_keys=False,
    ).encode()
    graph_bytes = b"storage:\n  nvme:\n    capacity: 100\nnetwork:\n  fabric: infiniband\n"
    (root / "jarvis_config.yaml").write_bytes(config_bytes)
    (root / "repos.yaml").write_bytes(repos_bytes)
    (root / "resource_graph.yaml").write_bytes(graph_bytes)
    return root, config_bytes, graph_bytes


def _installation_info(desired: BootstrapDesiredState) -> dict[str, object]:
    return {
        "schema_version": "clio-relay.installation-info.v1",
        "receipt_matches_install": True,
        "receipt": {
            "install_spec": desired.relay_install_spec,
            "artifact_sha256": desired.relay_artifact_sha256,
            "deployment_fingerprint": desired.fingerprint,
            "deployment_manifest": desired.model_dump(mode="json"),
            "generation": desired.fingerprint,
            "components": {
                "clio-relay": "1.5.0",
                "clio-kit": desired.clio_kit_version,
                "jarvis-cd": desired.jarvis_cd_version,
                "jarvis-util": desired.jarvis_util_commit,
            },
        },
        "component_runtime": {
            "clio-relay": {"persistent_tool_verified": True},
            "clio-kit": {
                "artifact_identity_verified": True,
                "command_matches_receipt": True,
                "locked_server_runtime_verified": True,
                "native_execution_capability_verified": True,
                "persistent_tool_verified": True,
            },
            "jarvis-cd": {"verified": True},
        },
    }


def test_exact_noop_is_read_only_and_preserves_operator_jarvis_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bin_dir = tmp_path / ".local/bin"
    bin_dir.mkdir(parents=True)
    binaries = {"uv": b"uv", "frpc": b"frpc", "frps": b"frps"}
    for name, content in binaries.items():
        path = bin_dir / name
        path.write_bytes(content)
        path.chmod(0o755)
    desired = _desired(
        uv_sha256=_digest(binaries["uv"]),
        frpc_sha256=_digest(binaries["frpc"]),
        frps_sha256=_digest(binaries["frps"]),
    )
    root, config_before, graph_before = _write_jarvis_state(tmp_path, desired)
    generation = tmp_path / ".local/share/clio-relay/generations" / desired.fingerprint
    generation.mkdir(parents=True)

    def inspect_active_generation(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[str, str]:
        return desired.fingerprint, str(generation.resolve())

    monkeypatch.setattr(
        "clio_relay.bootstrap_reconcile._inspect_active_generation",
        inspect_active_generation,
    )
    receipt_path = tmp_path / ".local/share/clio-relay/install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (
            receipt_path,
            root / "jarvis_config.yaml",
            root / "repos.yaml",
            root / "resource_graph.yaml",
        )
    }

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return _installation_info(desired)

    def run_identity(
        *_args: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["uv", "--version"], 0, "uv 0.11.28\n", "")

    monkeypatch.setattr("clio_relay.bootstrap_reconcile.installation_info", read_installation)
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "run_bounded_process",
        run_identity,
    )

    inspection = inspect_exact_bootstrap_noop(
        desired,
        home=tmp_path,
        service_was_active=True,
        service_was_enabled=True,
        queue_evidence={
            "schema_version": "clio-relay.queue-readiness.v1",
            "complete": True,
            "sealed": True,
            "repair_required": False,
        },
        worker_evidence={
            "schema_version": "clio-relay.worker-runtime-info.v1",
            "cluster": "cluster-a",
            "fresh": True,
            "process_running": True,
            "identity_matches_current": True,
            "running": True,
        },
    )

    assert inspection.exact_match is True
    assert inspection.reasons == []
    assert inspection.active_generation == desired.fingerprint
    assert inspection.current_generation_target == str(generation.resolve())
    assert (root / "jarvis_config.yaml").read_bytes() == config_before
    assert (root / "resource_graph.yaml").read_bytes() == graph_before
    assert {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in before} == before


def test_matching_receipt_with_tampered_runtime_is_not_a_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bin_dir = tmp_path / ".local/bin"
    bin_dir.mkdir(parents=True)
    for name, content in (("uv", b"uv"), ("frpc", b"frpc"), ("frps", b"frps")):
        path = bin_dir / name
        path.write_bytes(content)
        path.chmod(0o755)
    desired = _desired(
        uv_sha256=_digest(b"uv"),
        frpc_sha256=_digest(b"frpc"),
        frps_sha256=_digest(b"frps"),
    )
    _write_jarvis_state(tmp_path, desired)
    receipt_path = tmp_path / ".local/share/clio-relay/install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    info = _installation_info(desired)
    runtime = info["component_runtime"]
    assert isinstance(runtime, dict)
    runtime["clio-relay"] = {"persistent_tool_verified": False}

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return info

    def run_identity(
        *_args: object,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["uv", "--version"], 0, "uv 0.11.28\n", "")

    monkeypatch.setattr("clio_relay.bootstrap_reconcile.installation_info", read_installation)
    monkeypatch.setattr(
        subprocess,
        "run",
        run_identity,
    )

    inspection = inspect_exact_bootstrap_noop(
        desired,
        home=tmp_path,
        service_was_active=False,
        service_was_enabled=True,
        queue_evidence={
            "schema_version": "clio-relay.queue-readiness.v1",
            "complete": True,
            "sealed": True,
            "repair_required": False,
        },
        worker_evidence=None,
    )

    assert inspection.exact_match is False
    assert "clio-relay persistent tool identity did not verify" in inspection.reasons
    assert "managed endpoint service is inactive" in inspection.reasons


def test_first_legacy_upgrade_plans_relay_only_and_reuses_verified_components(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bin_dir = tmp_path / ".local/bin"
    bin_dir.mkdir(parents=True)
    for name, content in (("uv", b"uv"), ("frpc", b"frpc"), ("frps", b"frps")):
        path = bin_dir / name
        path.write_bytes(content)
        path.chmod(0o755)
    desired = _desired(
        uv_sha256=_digest(b"uv"),
        frpc_sha256=_digest(b"frpc"),
        frps_sha256=_digest(b"frps"),
    )
    _write_jarvis_state(tmp_path, desired)
    legacy_python = (
        tmp_path
        / ".local/share/clio-relay/jarvis-venv"
        / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    legacy_python.parent.mkdir(parents=True)
    legacy_python.write_bytes(b"python")
    legacy_python.chmod(0o755)
    legacy_jarvis = legacy_python.parent / ("jarvis.exe" if os.name == "nt" else "jarvis")
    legacy_jarvis.write_bytes(b"jarvis")
    legacy_jarvis.chmod(0o755)
    jarvis_util_checkout = tmp_path / ".local/src/jarvis-util"
    (jarvis_util_checkout / ".git").mkdir(parents=True)
    receipt_path = tmp_path / ".local/share/clio-relay/install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    clio_kit_wheel = tmp_path / "clio-kit.whl"
    jarvis_wheel = tmp_path / "jarvis-cd.whl"
    clio_kit_wheel.write_bytes(b"clio-kit")
    jarvis_wheel.write_bytes(b"jarvis")
    desired = desired.model_copy(
        update={
            "clio_kit_artifact_sha256": _digest(b"clio-kit"),
            "jarvis_cd_wheel_sha256": _digest(b"jarvis"),
        }
    )
    info = _installation_info(desired)
    receipt = cast(dict[str, object], info["receipt"])
    receipt.pop("deployment_fingerprint")
    receipt.pop("deployment_manifest")
    receipt["component_artifacts"] = {
        "clio-kit": {
            "artifact_sha256": desired.clio_kit_artifact_sha256,
            "runtime_artifact_path": str(clio_kit_wheel),
            "runtime_interpreters": {"provider": "/tools/clio-kit/python"},
            "runtime_executables": {"clio-kit": "/tools/clio-kit/bin/clio-kit"},
        },
        "jarvis-cd": {
            "artifact_sha256": desired.jarvis_cd_wheel_sha256,
            "runtime_artifact_path": str(jarvis_wheel),
            "runtime_interpreters": {"execution": str(legacy_python)},
            "runtime_executables": {"jarvis": str(legacy_jarvis)},
        },
    }

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return info

    monkeypatch.setattr("clio_relay.bootstrap_reconcile.installation_info", read_installation)

    def identity_command(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] != [str(bin_dir / "uv"), "--version"]:
            raise AssertionError(command)  # pragma: no cover
        return subprocess.CompletedProcess(command, 0, "uv 0.11.28\n", "")

    def bounded_identity(command: list[str], *, maximum: int) -> str:
        assert maximum > 0
        if command[:3] == ["git", "-C", str(jarvis_util_checkout)]:
            return "commit" if "rev-parse" in command else ""
        if command[0] == str(legacy_python):
            return json.dumps(
                {
                    "name": "jarvis-util",
                    "direct_url": json.dumps({"url": jarvis_util_checkout.resolve().as_uri()}),
                    "record": True,
                }
            )
        raise AssertionError(command)  # pragma: no cover

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "run_bounded_process",
        identity_command,
    )
    monkeypatch.setattr(
        "clio_relay.bootstrap_reconcile._bounded_subprocess",
        bounded_identity,
    )

    plan = plan_bootstrap_reconcile(desired, home=tmp_path)

    assert plan.mode == "relay-only", plan.reasons
    assert plan.component_actions == {
        "clio-relay": "replace",
        "jarvis-cd": "reuse",
        "jarvis-util": "reuse",
        "clio-kit": "reuse",
        "frp": "reuse",
        "uv": "reuse",
    }
    assert plan.reusable_paths["jarvis_execution_environment"] == str(
        (tmp_path / ".local/share/clio-relay/jarvis-venv").resolve()
    )


def test_existing_jarvis_144_plans_staged_component_upgrade_to_148(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact legacy Ares layout enters the fenced staged-upgrade path."""
    bin_dir = tmp_path / ".local/bin"
    bin_dir.mkdir(parents=True)
    for name, content in (("uv", b"uv"), ("frpc", b"frpc"), ("frps", b"frps")):
        path = bin_dir / name
        path.write_bytes(content)
        path.chmod(0o755)
    clio_kit_wheel = tmp_path / "clio_kit-2.5.23-py3-none-any.whl"
    clio_kit_wheel.write_bytes(b"clio-kit-2.5.23")
    desired = _desired(
        uv_sha256=_digest(b"uv"),
        frpc_sha256=_digest(b"frpc"),
        frps_sha256=_digest(b"frps"),
    )
    desired = desired.model_copy(
        update={
            "jarvis_cd_version": "1.4.8",
            "jarvis_cd_wheel_url": (
                "https://github.com/grc-iit/jarvis-cd/releases/download/"
                "v1.4.8/jarvis_cd-1.4.8-py3-none-any.whl"
            ),
            "jarvis_cd_wheel_sha256": (
                "ebf5e5f375b921f20c79075d461926431a5a017ca8b45e598878a89b229b3935"
            ),
            "clio_kit_version": "2.5.23",
            "clio_kit_artifact_sha256": _digest(b"clio-kit-2.5.23"),
        }
    )
    jarvis_root, _config_before, _graph_before = _write_jarvis_state(tmp_path, desired)
    (jarvis_root / "repos.yaml").write_text(
        yaml.safe_dump({"repos": ["/operator/clio_relay"]}, sort_keys=False),
        encoding="utf-8",
    )
    legacy_python = (
        tmp_path
        / ".local/share/clio-relay/jarvis-venv"
        / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    legacy_python.parent.mkdir(parents=True)
    legacy_python.write_bytes(b"python")
    legacy_python.chmod(0o755)
    base_python = tmp_path / "uv-python" / legacy_python.name
    base_python.parent.mkdir()
    base_python.write_bytes(b"base-python")
    base_python.chmod(0o755)
    legacy_jarvis_target = legacy_python.parent / ("jarvis.exe" if os.name == "nt" else "jarvis")
    legacy_jarvis_target.write_bytes(b"jarvis")
    legacy_jarvis_target.chmod(0o755)
    legacy_jarvis = bin_dir / legacy_jarvis_target.name
    legacy_jarvis.write_bytes(b"stable-wrapper-link")
    legacy_jarvis.chmod(0o755)
    resolve_path = Path.resolve

    def resolve_stable_wrapper(path: Path, strict: bool = False) -> Path:
        if path == legacy_python:
            return resolve_path(base_python, strict=strict)
        if path == legacy_jarvis:
            return resolve_path(legacy_jarvis_target, strict=strict)
        return resolve_path(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", resolve_stable_wrapper)
    jarvis_util_checkout = tmp_path / ".local/src/jarvis-util"
    (jarvis_util_checkout / ".git").mkdir(parents=True)
    receipt_path = tmp_path / ".local/share/clio-relay/install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    info = _installation_info(desired)
    receipt = cast(dict[str, object], info["receipt"])
    components = cast(dict[str, object], receipt["components"])
    components["jarvis-cd"] = "1.4.4"
    components["clio-kit"] = "2.5.22"
    component_runtime = info["component_runtime"]
    assert isinstance(component_runtime, dict)
    component_runtime["clio-kit"] = {
        "error": "uv tool directory is unavailable",
        "persistent_tool_verified": False,
    }
    component_artifacts: dict[str, object] = {
        "clio-kit": {
            "runtime_interpreters": {"provider": "/old/clio-kit/python"},
            "runtime_executables": {"clio-kit": "/old/clio-kit/clio-kit"},
        },
        "jarvis-cd": {
            "runtime_interpreters": {"execution": str(legacy_python)},
            "runtime_executables": {"jarvis": str(legacy_jarvis)},
        },
    }
    receipt["component_artifacts"] = component_artifacts

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return info

    monkeypatch.setattr("clio_relay.bootstrap_reconcile.installation_info", read_installation)

    def identity_command(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command[:2] == [str(bin_dir / "uv"), "--version"]
        return subprocess.CompletedProcess(command, 0, "uv 0.11.28\n", "")

    def bounded_identity(command: list[str], *, maximum: int) -> str:
        assert maximum > 0
        if command[:3] == ["git", "-C", str(jarvis_util_checkout)]:
            return "commit" if "rev-parse" in command else ""
        if command[0] == str(legacy_python):
            return json.dumps(
                {
                    "name": "jarvis-util",
                    "direct_url": json.dumps({"url": jarvis_util_checkout.resolve().as_uri()}),
                    "record": True,
                }
            )
        raise AssertionError(command)  # pragma: no cover

    monkeypatch.setattr(bootstrap_reconcile_module, "run_bounded_process", identity_command)
    monkeypatch.setattr(
        "clio_relay.bootstrap_reconcile._bounded_subprocess",
        bounded_identity,
    )

    plan = plan_bootstrap_reconcile(desired, home=tmp_path)

    assert desired.jarvis_cd_version == "1.4.8"
    assert plan.mode == "component-upgrade", plan.reasons
    assert plan.component_actions == {
        "clio-relay": "replace",
        "jarvis-cd": "replace",
        "jarvis-util": "reuse",
        "clio-kit": "replace",
        "frp": "reuse",
        "uv": "reuse",
    }
    assert plan.reusable_paths["jarvis_execution_python"] == str(legacy_python)
    assert plan.reusable_paths["jarvis_execution_executable"] == str(legacy_jarvis_target.resolve())
    assert plan.reasons == [
        "clio-kit version requires a staged upgrade",
        "jarvis-cd version requires a staged upgrade",
    ]

    read_regular = bootstrap_reconcile_module._read_regular_bounded_with_identity  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    swapped = False

    def swap_launcher_after_read(
        path: Path,
        *,
        maximum: int,
    ) -> tuple[bytes, tuple[int, int, int, int, int, int]]:
        nonlocal swapped
        payload, identity = read_regular(path, maximum=maximum)
        if path == legacy_jarvis_target.resolve() and not swapped:
            swapped = True
            path.unlink()
            path.write_bytes(b"replacement-jarvis")
            path.chmod(0o755)
        return payload, identity

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_read_regular_bounded_with_identity",
        swap_launcher_after_read,
    )
    raced = plan_bootstrap_reconcile(desired, home=tmp_path)
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_read_regular_bounded_with_identity",
        read_regular,
    )

    assert raced.mode == "full"
    assert "legacy JARVIS execution environment is not reusable" in raced.reasons

    components["clio-kit"] = desired.clio_kit_version
    artifacts = receipt["component_artifacts"]
    artifacts["clio-kit"] = {
        "artifact_sha256": desired.clio_kit_artifact_sha256,
        "runtime_artifact_path": str(clio_kit_wheel),
        "runtime_interpreters": {"provider": "/old/clio-kit/python"},
        "runtime_executables": {"clio-kit": "/old/clio-kit/clio-kit"},
    }

    unsafe_reuse = plan_bootstrap_reconcile(desired, home=tmp_path)

    assert unsafe_reuse.mode == "full"
    assert unsafe_reuse.reasons == [
        "jarvis-cd version requires a staged upgrade",
        "clio-kit live runtime is not reusable",
    ]


def test_uv_version_output_accepts_only_pinned_bounded_target_triple() -> None:
    """Accept uv's real target suffix without weakening the pinned version boundary."""
    matches = bootstrap_reconcile_module._uv_version_output_matches  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    for accepted in (
        "uv 0.11.28",
        "uv 0.11.28\n",
        "uv 0.11.28\r\n",
        "uv 0.11.28 (x86_64-unknown-linux-gnu)",
        "uv 0.11.28 (x86_64-unknown-linux-gnu)\n",
    ):
        assert matches(accepted, expected_version="0.11.28")

    for rejected in (
        "uv 0.11.27",
        "uv 0.11.28 ()",
        "uv 0.11.28 (x86_64 unknown linux gnu)",
        "uv 0.11.28 (x86_64-unknown-linux-gnu) extra",
        "uv 0.11.28 (x86_64-unknown-linux-gnu)(extra)",
        "uv 0.11.28\n\n",
        " uv 0.11.28",
        "uv 0.11.28\x00",
        "uv 0.11.28 (" + "a" * 129 + ")",
    ):
        assert not matches(rejected, expected_version="0.11.28")


def test_existing_operator_jarvis_roots_are_adopted_without_mutation(tmp_path: Path) -> None:
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    root, config_before, graph_before = _write_jarvis_state(tmp_path, desired)
    config = yaml.safe_load(config_before)
    operator_shared = tmp_path / "operator-shared"
    operator_shared.mkdir()
    config["shared_dir"] = str(operator_shared.resolve())
    mismatched = yaml.safe_dump(config, sort_keys=False).encode()
    (root / "jarvis_config.yaml").write_bytes(mismatched)

    evidence = inspect_jarvis_state(desired, home=tmp_path)

    assert evidence.initialized is True
    assert evidence.roots["shared_dir"] == str(operator_shared.resolve())
    assert (root / "jarvis_config.yaml").read_bytes() == mismatched
    assert (root / "resource_graph.yaml").read_bytes() == graph_before


def test_managed_repo_reconcile_preserves_same_name_operator_repository(tmp_path: Path) -> None:
    repos_file = tmp_path / "repos.yaml"
    operator_repo = tmp_path / "operator/clio_relay"
    previous_managed = tmp_path / "legacy/clio_relay"
    managed_repo = tmp_path / "relay/managed-jarvis-repo"
    repos_file.write_text(
        yaml.safe_dump(
            {
                "repos": [str(operator_repo.absolute()), str(previous_managed.absolute())],
                "operator_extension": {"preserve": True},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    evidence = reconcile_managed_jarvis_repository(
        repos_file,
        managed_repo,
        previous_managed_repos=(previous_managed,),
    )
    document = yaml.safe_load(repos_file.read_text(encoding="utf-8"))

    assert evidence["action"] == "updated"
    assert evidence["added_managed_repos"] == [str(managed_repo.absolute())]
    assert evidence["removed_previous_managed_repos"] == [str(previous_managed.absolute())]
    assert document["repos"] == [
        str(managed_repo.absolute()),
        str(operator_repo.absolute()),
    ]
    assert document["operator_extension"] == {"preserve": True}
    before = repos_file.read_bytes()
    reused = reconcile_managed_jarvis_repository(repos_file, managed_repo)
    assert reused["action"] == "reused"
    assert repos_file.read_bytes() == before


def test_managed_repo_reconcile_refuses_concurrent_operator_edit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos_file = tmp_path / "repos.yaml"
    repos_file.write_text("repos:\n  - /operator/original\n", encoding="utf-8")
    operator_update = b"repos:\n  - /operator/concurrent\n"
    original_read = bootstrap_reconcile_module._read_regular_bounded_with_identity  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    call_count = 0

    def mutate_before_compare(
        path: Path,
        *,
        maximum: int,
    ) -> tuple[bytes, tuple[int, int, int, int, int, int]]:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            repos_file.write_bytes(operator_update)
        return original_read(path, maximum=maximum)

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_read_regular_bounded_with_identity",
        mutate_before_compare,
    )

    with pytest.raises(ConfigurationError, match="changed during reconciliation"):
        reconcile_managed_jarvis_repository(
            repos_file,
            tmp_path / "relay/managed-jarvis-repo",
        )

    assert repos_file.read_bytes() == operator_update
    assert not list(tmp_path.glob(".repos.yaml.*.tmp"))


def test_managed_repo_repair_refuses_unproven_broken_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken link is existing untrusted state, not an absent managed path."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    expected = (
        tmp_path
        / ".local/share/clio-relay/generations"
        / desired.fingerprint
        / "source/jarvis-packages/clio_relay"
    )
    expected.mkdir(parents=True)
    managed = tmp_path / ".local/share/clio-relay/managed-jarvis-repo"
    original_lstat = Path.lstat

    def simulated_lstat(path: Path) -> os.stat_result | SimpleNamespace:
        if path == managed:
            return SimpleNamespace(st_mode=stat.S_IFLNK)
        return original_lstat(path)

    def redirected_readlink(_path: os.PathLike[str] | str) -> str:
        return str(tmp_path / "attacker-controlled/missing")

    monkeypatch.setattr(Path, "lstat", simulated_lstat)
    monkeypatch.setattr(
        bootstrap_reconcile_module.os,
        "readlink",
        redirected_readlink,
    )

    with pytest.raises(ConfigurationError, match="not proven by an earlier receipt"):
        repair_managed_jarvis_binding(desired, home=tmp_path)


def test_transaction_journal_makes_relay_activation_forward_only(tmp_path: Path) -> None:
    path = tmp_path / "transaction.json"
    journal = BootstrapTransactionJournal(
        invocation_id="bootstrap-1",
        desired_fingerprint="a" * 64,
    )
    for state in (
        BootstrapTransactionState.INSPECTED,
        BootstrapTransactionState.PREPARING,
        BootstrapTransactionState.PREPARED,
        BootstrapTransactionState.FENCING,
        BootstrapTransactionState.FENCED,
        BootstrapTransactionState.ACTIVATING,
        BootstrapTransactionState.ACTIVATED,
    ):
        if state is BootstrapTransactionState.PREPARED:
            journal.prepared_generation = "a" * 64
        journal.advance(state)
        journal.persist(path)
    assert journal.irreversible_boundary is True
    assert journal.recovery_mode == "forward"

    journal.advance(BootstrapTransactionState.MIGRATION_STARTED)
    journal.persist(path)

    loaded = BootstrapTransactionJournal.load(path)
    assert loaded.irreversible_boundary is True
    assert loaded.recovery_mode == "forward"
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "migration_started"


def test_bootstrap_invocation_lock_is_private_and_bounded(tmp_path: Path) -> None:
    with bootstrap_invocation_lock(home=tmp_path, timeout_seconds=1) as path:
        assert path == tmp_path / ".local/share/clio-relay/bootstrap.lock"
        if os.name != "nt":
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
        with (
            pytest.raises(ConfigurationError, match="timed out acquiring"),
            bootstrap_invocation_lock(home=tmp_path, timeout_seconds=0.05),
        ):
            raise AssertionError("a concurrent bootstrap lock was acquired")


def test_bootstrap_invocation_lock_refuses_redirected_lock_path(tmp_path: Path) -> None:
    with bootstrap_invocation_lock(home=tmp_path, timeout_seconds=1) as lock_path:
        pass
    lock_path.unlink()
    if os.name == "nt":
        target = tmp_path / "outside-lock-directory"
        target.mkdir()
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(lock_path), str(target)],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, result.stderr
    else:
        target = tmp_path / "outside-lock"
        target.write_bytes(b"outside")
        lock_path.symlink_to(target)

    with (
        pytest.raises(ConfigurationError, match="private bootstrap lock"),
        bootstrap_invocation_lock(home=tmp_path, timeout_seconds=0.1),
    ):
        raise AssertionError("a redirected bootstrap lock was acquired")


def test_transaction_journal_rejects_skip_and_tamper(tmp_path: Path) -> None:
    journal = BootstrapTransactionJournal(
        invocation_id="bootstrap-1",
        desired_fingerprint="a" * 64,
    )
    with pytest.raises(ConfigurationError, match="invalid bootstrap transaction transition"):
        journal.advance(BootstrapTransactionState.ACTIVATED)

    path = tmp_path / "transaction.json"
    path.write_text('{"state":"migration_started"}', encoding="utf-8")
    with pytest.raises(ConfigurationError, match="journal is invalid"):
        BootstrapTransactionJournal.load(path)


def test_noop_receipt_records_no_scheduler_or_generation_gc(tmp_path: Path) -> None:
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    _write_jarvis_state(tmp_path, desired)
    inspection = inspect_jarvis_state(desired, home=tmp_path)
    from clio_relay.bootstrap_reconcile import (  # noqa: PLC0415
        BootstrapInspection,
        BootstrapReadinessEvidence,
    )

    exact = BootstrapInspection(
        exact_match=True,
        desired_fingerprint=desired.fingerprint,
        jarvis_state=inspection,
        readiness=BootstrapReadinessEvidence(queue_ready=True),
    )
    receipt = make_bootstrap_receipt(
        invocation_id="bootstrap-1",
        desired=desired,
        outcome="noop_verified",
        inspection=exact,
        started_at=datetime.now(UTC),
        transaction=None,
        previous_generation="a" * 64,
        active_generation="a" * 64,
    )

    assert receipt["outcome"] == "noop_verified"
    preservation = receipt["preservation"]
    assert isinstance(preservation, dict)
    assert preservation == {
        "scheduler_jobs_cancelled": False,
        "old_generations_retained": True,
        "jarvis_init_on_existing_root": False,
    }


def test_desired_fingerprint_is_canonical_and_field_sensitive() -> None:
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    reconstructed = BootstrapDesiredState.model_validate(
        json.loads(json.dumps(desired.model_dump(mode="json"), sort_keys=False))
    )
    changed = desired.model_copy(update={"agent_args": ["--model", "haiku"]})

    assert reconstructed.fingerprint == desired.fingerprint
    assert changed.fingerprint != desired.fingerprint


def test_desired_fingerprint_binds_jarvis_graph_profile_and_build_policy() -> None:
    """Graph activation policy is explicit deployment identity, not a cluster-name guess."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    profiled = desired.model_copy(update={"jarvis_resource_graph_profile": "ares"})
    fallback = profiled.model_copy(update={"allow_jarvis_resource_graph_build": True})

    assert desired.fingerprint != profiled.fingerprint
    assert profiled.fingerprint != fallback.fingerprint


def test_builtin_graph_result_rejects_unavailable_profile_still_in_catalog() -> None:
    """Only a genuine catalog miss may authorize the explicit build fallback."""
    with pytest.raises(ValueError, match="unavailable JARVIS builtin graph evidence"):
        validate_jarvis_builtin_result(
            {
                "schema_version": "jarvis.resource-graph-builtin.v1",
                "profile": "ares",
                "action": "unavailable",
                "available": False,
                "source": None,
                "source_sha256": None,
                "catalog": ["ares"],
            },
            requested_profile="ares",
        )


def test_prepared_generation_refuses_a_tampered_relay_owned_jarvis_wrapper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    execution_root = tmp_path / "jarvis-venv"
    execution_bin = execution_root / ("Scripts" if os.name == "nt" else "bin")
    execution_bin.mkdir(parents=True)
    execution_python = execution_bin / ("python.exe" if os.name == "nt" else "python")
    legacy_jarvis = execution_bin / ("jarvis.exe" if os.name == "nt" else "jarvis")
    for path, payload in (
        (execution_python, b"python-runtime"),
        (legacy_jarvis, b"legacy-launcher"),
    ):
        path.write_bytes(payload)
        path.chmod(0o755)
    execution_identity = execution_environment_identity(
        execution_root,
        executables={"python": execution_python, "jarvis": legacy_jarvis},
    )
    generation = tmp_path / "generation"
    generation_bin = generation / "bin"
    generation_bin.mkdir(parents=True)
    wrapper = generation_bin / "jarvis"
    wrapper_evidence = write_jarvis_wrapper(wrapper, execution_python)
    simulated_launchers: set[Path] = set()
    for name in ("clio-relay", "clio-kit"):
        target = tmp_path / f"{name}-target"
        target.write_bytes(name.encode())
        target.chmod(0o755)
        launcher = generation_bin / name
        os.link(target, launcher)
        simulated_launchers.add(launcher)
    original_is_symlink = Path.is_symlink

    def simulated_is_symlink(path: Path) -> bool:
        return path in simulated_launchers or original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)
    receipt_path = generation / "install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    plan = {
        "mode": "relay-only",
        "desired_fingerprint": desired.fingerprint,
        "component_actions": {"clio-relay": "replace"},
    }
    manifest = {
        "schema_version": "clio-relay.bootstrap-generation.v1",
        "fingerprint": desired.fingerprint,
        "plan": plan,
        "legacy_execution_identity": execution_identity,
        "jarvis_wrapper_sha256": wrapper_evidence["sha256"],
        "install_receipt": str(receipt_path),
    }
    (generation / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (generation / ".prepared").open("w", encoding="ascii", newline="\n") as stream:
        stream.write(desired.fingerprint + "\n")

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return _installation_info(desired)

    monkeypatch.setattr("clio_relay.bootstrap_reconcile.installation_info", read_installation)

    evidence = inspect_prepared_generation(
        desired,
        generation=generation,
        legacy_execution_identity=execution_identity,
    )

    launcher_targets = cast(dict[str, object], evidence["launcher_targets"])
    assert launcher_targets["jarvis"] == str(wrapper)
    assert wrapper.read_bytes() == jarvis_wrapper_payload(execution_python)

    wrapper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    wrapper.chmod(0o755)
    with pytest.raises(ConfigurationError, match="JARVIS wrapper identity changed"):
        inspect_prepared_generation(
            desired,
            generation=generation,
            legacy_execution_identity=execution_identity,
        )


def test_active_generation_rejects_coordinated_manifest_and_wrapper_tamper(
    tmp_path: Path,
) -> None:
    """A rewritten manifest cannot authorize a different JARVIS interpreter."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)

    def make_execution_root(name: str) -> tuple[Path, Path, Path, dict[str, object]]:
        root = tmp_path / name
        bin_dir = root / ("Scripts" if os.name == "nt" else "bin")
        bin_dir.mkdir(parents=True)
        python = bin_dir / ("python.exe" if os.name == "nt" else "python")
        jarvis = bin_dir / ("jarvis.exe" if os.name == "nt" else "jarvis")
        python.write_bytes((name + "-python").encode())
        jarvis.write_bytes((name + "-jarvis").encode())
        python.chmod(0o755)
        jarvis.chmod(0o755)
        identity = execution_environment_identity(
            root,
            executables={"python": python, "jarvis": jarvis},
        )
        return root, python, jarvis, identity

    _original_root, original_python, _original_jarvis, original_identity = make_execution_root(
        "original"
    )
    generation = tmp_path / "generation"
    (generation / "bin").mkdir(parents=True)
    wrapper = generation / "bin/jarvis"
    wrapper_evidence = write_jarvis_wrapper(wrapper, original_python)
    receipt_path = generation / "install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    manifest: dict[str, object] = {
        "schema_version": "clio-relay.bootstrap-generation.v1",
        "fingerprint": desired.fingerprint,
        "plan": {
            "mode": "relay-only",
            "desired_fingerprint": desired.fingerprint,
            "component_actions": {"clio-relay": "replace"},
        },
        "legacy_execution_identity": original_identity,
        "jarvis_wrapper_sha256": wrapper_evidence["sha256"],
        "install_receipt": str(receipt_path),
    }
    manifest_path = generation / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    installation = _installation_info(desired)
    receipt = installation["receipt"]
    assert isinstance(receipt, dict)
    receipt["component_artifacts"] = {
        "jarvis-cd": {"runtime_interpreters": {"execution": str(original_python)}}
    }

    bootstrap_reconcile_module._verify_active_generation_jarvis_wrapper(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        generation,
        desired=desired,
        installation=installation,
    )

    _replacement_root, replacement_python, _replacement_jarvis, replacement_identity = (
        make_execution_root("replacement")
    )
    wrapper.unlink()
    replacement_wrapper = write_jarvis_wrapper(wrapper, replacement_python)
    manifest["legacy_execution_identity"] = replacement_identity
    manifest["jarvis_wrapper_sha256"] = replacement_wrapper["sha256"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ConfigurationError, match="not bound to its install receipt"):
        bootstrap_reconcile_module._verify_active_generation_jarvis_wrapper(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            generation,
            desired=desired,
            installation=installation,
        )


def test_transaction_persistence_cleans_temporary_on_replace_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = BootstrapTransactionJournal(
        invocation_id="bootstrap-1",
        desired_fingerprint="a" * 64,
    )

    def fail_replace(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("disk failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="disk failure"):
        journal.persist(tmp_path / "transaction.json")

    assert list(tmp_path.iterdir()) == []


def test_identity_command_collector_rejects_output_beyond_its_memory_bound() -> None:
    with pytest.raises(ConfigurationError, match="output exceeded its bound"):
        bootstrap_reconcile_module._bounded_subprocess(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            [sys.executable, "-c", "import os; os.write(1, b'x' * 65536)"],
            maximum=64,
        )
