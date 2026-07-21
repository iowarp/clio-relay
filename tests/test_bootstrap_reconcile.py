"""Focused contracts for idempotent, crash-safe bootstrap reconciliation."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
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
    BootstrapActivationPath,
    BootstrapDesiredState,
    BootstrapReconcilePlan,
    BootstrapReplacementProviderEvidence,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
    bootstrap_invocation_lock,
    execution_environment_identity,
    finish_staged_activation,
    inspect_exact_bootstrap_noop,
    inspect_jarvis_state,
    inspect_prepared_generation,
    jarvis_wrapper_payload,
    make_bootstrap_receipt,
    plan_bootstrap_reconcile,
    reconcile_managed_jarvis_repository,
    reconcile_staged_activation_links,
    repair_managed_jarvis_binding,
    resolve_receipt_bound_jarvis_python,
    validate_jarvis_builtin_result,
    write_jarvis_wrapper,
)
from clio_relay.errors import ConfigurationError
from clio_relay.installation import PersistentUvToolIdentity
from clio_relay.validation_report import sha256_file


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _create_directory_alias(alias: Path, target: Path) -> None:
    """Create an ancestor-path alias on POSIX or unprivileged Windows."""
    if os.name == "nt":
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(alias), str(target)],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise AssertionError(result.stdout + result.stderr)
        return
    alias.symlink_to(target, target_is_directory=True)


def _simulate_file_symlink(
    monkeypatch: pytest.MonkeyPatch,
    *,
    path: Path,
    target: Path,
) -> None:
    """Expose one file as a symlink without requiring Windows symlink privilege."""
    details = path.lstat()
    symlink_details = SimpleNamespace(
        st_dev=details.st_dev,
        st_ino=details.st_ino,
        st_mode=stat.S_IFLNK | 0o777,
        st_size=len(str(target)),
        st_mtime_ns=details.st_mtime_ns,
        st_ctime_ns=details.st_ctime_ns,
        st_nlink=details.st_nlink,
    )
    original_lstat = Path.lstat
    original_readlink = os.readlink

    def simulated_lstat(candidate: Path) -> os.stat_result:
        if candidate == path:
            return cast(os.stat_result, symlink_details)
        return original_lstat(candidate)

    def simulated_readlink(candidate: Any, *args: Any, **kwargs: Any) -> Any:
        if Path(candidate) == path:
            return str(target)
        return original_readlink(candidate, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", simulated_lstat)
    monkeypatch.setattr(os, "readlink", simulated_readlink)


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
            "clio-relay": {
                "persistent_tool_verified": True,
                "execution_runtime_verified": True,
            },
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


def test_managed_repository_converges_home_alias_to_one_canonical_stable_path(
    tmp_path: Path,
) -> None:
    """Repository migration neither loses nor duplicates a stable path through HOME aliases."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    canonical_home = tmp_path / "canonical-home"
    canonical_home.mkdir()
    lexical_home = tmp_path / "home-alias"
    _create_directory_alias(lexical_home, canonical_home)
    root, _config, _graph = _write_jarvis_state(lexical_home, desired)
    managed = lexical_home / ".local/share/clio-relay/managed-jarvis-repo"
    previous = lexical_home / ".local/src/clio-relay/jarvis-packages/clio_relay"
    previous.mkdir(parents=True)
    operator = "/operator/clio_relay"
    repos_file = root / "repos.yaml"
    repos_file.write_text(
        yaml.safe_dump(
            {"repos": [str(managed.absolute()), str(previous.absolute()), operator]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    evidence = reconcile_managed_jarvis_repository(
        repos_file,
        managed,
        previous_managed_repos=(previous,),
        exchange_identity=desired.fingerprint,
    )

    canonical_managed = str(canonical_home / ".local/share/clio-relay/managed-jarvis-repo")
    canonical_previous = str(canonical_home / ".local/src/clio-relay/jarvis-packages/clio_relay")
    assert yaml.safe_load(repos_file.read_text(encoding="utf-8"))["repos"] == [
        canonical_managed,
        operator,
    ]
    assert evidence["managed_repo"] == canonical_managed
    assert evidence["added_managed_repos"] == [canonical_managed]
    assert evidence["removed_previous_managed_repos"] == [canonical_previous]
    assert inspect_jarvis_state(desired, home=lexical_home).managed_repo_registered is True

    repeated = reconcile_managed_jarvis_repository(
        repos_file,
        managed,
        previous_managed_repos=(previous,),
        exchange_identity=desired.fingerprint,
    )
    assert repeated["action"] == "reused"

    repos_file.write_text(
        yaml.safe_dump(
            {"repos": [str(managed.absolute()), canonical_managed, operator]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="multiple path aliases"):
        reconcile_managed_jarvis_repository(repos_file, managed)


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

    runtime["clio-relay"] = {
        "persistent_tool_verified": True,
        "execution_runtime_verified": False,
    }
    execution_mismatch = inspect_exact_bootstrap_noop(
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

    assert execution_mismatch.exact_match is False
    assert "clio-relay JARVIS execution runtime did not verify" in execution_mismatch.reasons


@pytest.mark.parametrize(
    "relay_provider",
    [
        "verified-current-execution",
        "verified-old",
        "unverified-old",
        "staged-candidate",
        "tampered-candidate",
    ],
)
def test_first_legacy_upgrade_stages_an_unbound_relay_execution_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relay_provider: str,
) -> None:
    bin_dir = tmp_path / ".local/bin"
    bin_dir.mkdir(parents=True)
    for name, content in (
        ("uv", b"uv"),
        ("frpc", b"frpc"),
        ("frps", b"frps"),
        ("clio-relay", b"relay"),
    ):
        path = bin_dir / name
        path.write_bytes(content)
        path.chmod(0o755)
    desired = _desired(
        uv_sha256=_digest(b"uv"),
        frpc_sha256=_digest(b"frpc"),
        frps_sha256=_digest(b"frps"),
    )
    _write_jarvis_state(tmp_path, desired)
    (tmp_path / ".local/share/clio-relay/managed-jarvis-repo").rmdir()
    managed_generation = "b" * 64 if relay_provider == "staged-candidate" else None
    execution_root = tmp_path / ".local/share/clio-relay/jarvis-venv"
    if managed_generation is not None:
        generation_root = tmp_path / ".local/share/clio-relay/generations" / managed_generation
        execution_root = generation_root / "jarvis-venv"
        execution_root.mkdir(parents=True)
        _create_directory_alias(
            tmp_path / ".local/share/clio-relay/current",
            generation_root,
        )

        def captured_activation_paths(*, home: Path) -> dict[str, BootstrapActivationPath]:
            assert home == tmp_path
            return {
                "current": BootstrapActivationPath(
                    path=str(home / ".local/share/clio-relay/current"),
                    kind="symlink",
                ),
                "install_receipt": BootstrapActivationPath(
                    path=str(home / ".local/share/clio-relay/install-receipt.json"),
                    kind="file_or_symlink",
                ),
                "relay_launcher": BootstrapActivationPath(
                    path=str(home / ".local/bin/clio-relay"),
                    kind="file_or_symlink",
                ),
                "jarvis_launcher": BootstrapActivationPath(
                    path=str(home / ".local/bin/jarvis"),
                    kind="file_or_symlink",
                ),
                "managed_repo": BootstrapActivationPath(
                    path=str(home / ".local/share/clio-relay/managed-jarvis-repo"),
                    kind="symlink",
                ),
            }

        monkeypatch.setattr(
            bootstrap_reconcile_module,
            "_capture_reconcile_activation_paths",
            captured_activation_paths,
        )
    legacy_python = execution_root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    legacy_python.parent.mkdir(parents=True)
    legacy_python.write_bytes(b"python")
    legacy_python.chmod(0o755)
    legacy_jarvis = legacy_python.parent / ("jarvis.exe" if os.name == "nt" else "jarvis")
    legacy_jarvis.write_bytes(b"jarvis")
    legacy_jarvis.chmod(0o755)
    stable_jarvis = bin_dir / "jarvis"
    stable_jarvis.write_bytes(b"stable-jarvis")
    stable_jarvis.chmod(0o755)
    jarvis_util_checkout = tmp_path / ".local/src/jarvis-util"
    (jarvis_util_checkout / ".git").mkdir(parents=True)
    receipt_path = tmp_path / ".local/share/clio-relay/install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    clio_kit_wheel = tmp_path / "clio-kit.whl"
    jarvis_wheel = tmp_path / "jarvis-cd.whl"
    relay_wheel = tmp_path / "clio-relay.whl"
    clio_kit_wheel.write_bytes(b"clio-kit")
    jarvis_wheel.write_bytes(b"jarvis")
    relay_wheel.write_bytes(b"relay-wheel")
    relay_digest = _digest(b"relay-wheel")
    desired = desired.model_copy(
        update={
            "relay_artifact_sha256": relay_digest,
            "relay_source_identity": f"wheel:sha256:{relay_digest}",
            "clio_kit_artifact_sha256": _digest(b"clio-kit"),
            "jarvis_cd_wheel_sha256": _digest(b"jarvis"),
        }
    )
    info = _installation_info(desired)
    if relay_provider not in {"verified-current-execution", "verified-old"}:
        component_runtime = cast(dict[str, object], info["component_runtime"])
        component_runtime["clio-relay"] = {
            "persistent_tool_verified": False,
            "error": "source artifact is unavailable",
        }
    replacement_provider = None
    if relay_provider in {"staged-candidate", "tampered-candidate"}:
        info["receipt_matches_install"] = False
        replacement_provider = BootstrapReplacementProviderEvidence.model_construct(
            desired_fingerprint=desired.fingerprint,
            relay_install_spec=desired.relay_install_spec,
            preparing_root=str(tmp_path / ".local/share/clio-relay/preparing/invocation"),
            extracted_source_root=str(
                tmp_path / ".local/share/clio-relay/preparing/invocation/source"
            ),
            source_archive_sha256="f" * 64,
            persistent_tool=PersistentUvToolIdentity.model_construct(),
        )

        def verify_replacement(
            _desired: BootstrapDesiredState,
            _evidence: BootstrapReplacementProviderEvidence,
            *,
            home: Path | None = None,
        ) -> None:
            assert home == tmp_path
            if relay_provider == "tampered-candidate":
                raise ConfigurationError("candidate RECORD closure changed")

        monkeypatch.setattr(
            bootstrap_reconcile_module,
            "_verify_bootstrap_replacement_provider",
            verify_replacement,
        )
    receipt = cast(dict[str, object], info["receipt"])
    if managed_generation is not None:
        receipt["generation"] = managed_generation
    receipt.pop("deployment_fingerprint")
    receipt.pop("deployment_manifest")
    relay_component: dict[str, object] = {
        "runtime_executables": {
            "clio-relay": (
                "/stale/unbound/clio-relay"
                if relay_provider == "staged-candidate"
                else str(bin_dir / "clio-relay")
            )
        },
    }
    if relay_provider == "verified-current-execution":
        relay_component.update(
            {
                "install_spec": desired.relay_install_spec,
                "artifact_sha256": desired.relay_artifact_sha256,
                "runtime_artifact_path": str(relay_wheel),
                "runtime_interpreters": {"execution": str(legacy_python)},
            }
        )
    receipt["component_artifacts"] = {
        "clio-relay": relay_component,
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

    plan = plan_bootstrap_reconcile(
        desired,
        home=tmp_path,
        replacement_provider=replacement_provider,
    )

    if relay_provider == "unverified-old":
        assert plan.mode == "full"
        assert plan.reasons == [
            "clio-relay live provider is not reusable: source artifact is unavailable"
        ]
        return
    if relay_provider == "tampered-candidate":
        assert plan.mode == "full"
        assert plan.reasons == [
            "candidate replacement provider did not verify: candidate RECORD closure changed"
        ]
        return

    expected_mode = (
        "relay-only" if relay_provider == "verified-current-execution" else "component-upgrade"
    )
    assert plan.mode == expected_mode, plan.reasons
    assert plan.component_actions == {
        "clio-relay": "replace",
        "jarvis-cd": "reuse" if expected_mode == "relay-only" else "replace",
        "jarvis-util": "reuse",
        "clio-kit": "reuse" if expected_mode == "relay-only" else "replace",
        "frp": "reuse",
        "uv": "reuse",
    }
    assert plan.reusable_paths["jarvis_execution_environment"] == str(execution_root.resolve())
    if expected_mode == "relay-only":
        assert plan.reusable_paths["clio-relay_artifact"] == str(relay_wheel.resolve())
    else:
        assert plan.reasons == ["relay JARVIS execution runtime requires a staged replacement"]
    assert plan.activation_paths["current"].before is None
    assert plan.activation_paths["managed_repo"].before is None
    if managed_generation is None:
        assert plan.activation_paths["install_receipt"].before is not None
    else:
        assert plan.activation_paths["install_receipt"].before is None
    if relay_provider == "staged-candidate":
        component_runtime = cast(dict[str, object], info["component_runtime"])
        clio_kit_runtime = cast(dict[str, object], component_runtime["clio-kit"])
        clio_kit_runtime["native_execution_capability_verified"] = False
        rejected = plan_bootstrap_reconcile(
            desired,
            home=tmp_path,
            replacement_provider=replacement_provider,
        )
        assert rejected.mode == "full"
        assert rejected.reasons == ["clio-kit live runtime is not reusable"]
    elif relay_provider in {"verified-current-execution", "verified-old"}:
        relay_artifact = cast(
            dict[str, object],
            cast(dict[str, object], receipt["component_artifacts"])["clio-relay"],
        )
        relay_artifact["runtime_executables"] = {"clio-relay": "/stale/unbound/clio-relay"}
        rejected = plan_bootstrap_reconcile(desired, home=tmp_path)
        assert rejected.mode == "full"
        assert rejected.reasons == ["clio-relay launcher is not bound to its install receipt"]


def test_active_managed_generation_jarvis_environment_is_reusable(tmp_path: Path) -> None:
    """A retained generation tool is reusable only while its generation is active."""
    generation = "a" * 64
    relay_root = tmp_path / ".local/share/clio-relay"
    generation_root = relay_root / "generations" / generation
    environment = generation_root / "jarvis-venv"
    environment.mkdir(parents=True)
    current = relay_root / "current"
    _create_directory_alias(current, generation_root)

    observed = bootstrap_reconcile_module._managed_generation_jarvis_environment(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        {"generation": generation},
        execution_environment=environment,
        home=tmp_path,
    )

    assert observed == environment.resolve(strict=True)
    assert (
        bootstrap_reconcile_module._managed_generation_jarvis_environment(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            {"generation": "not-a-generation"},
            execution_environment=environment,
            home=tmp_path,
        )
        is None
    )


def test_retained_generation_jarvis_environment_is_reusable(tmp_path: Path) -> None:
    """A relay-only generation may retain a receipt-bound prior JARVIS environment."""
    active_generation = "a" * 64
    execution_generation = "b" * 64
    relay_root = tmp_path / ".local/share/clio-relay"
    active_root = relay_root / "generations" / active_generation
    environment = relay_root / "generations" / execution_generation / "jarvis-venv"
    active_root.mkdir(parents=True)
    environment.mkdir(parents=True)
    _create_directory_alias(relay_root / "current", active_root)

    observed = bootstrap_reconcile_module._managed_generation_jarvis_environment(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        {"generation": active_generation},
        execution_environment=environment,
        home=tmp_path,
    )

    assert observed == environment.resolve(strict=True)


def test_generation_jarvis_environment_accepts_home_directory_alias(tmp_path: Path) -> None:
    """Receipt paths may use a lexical home alias that resolves to the managed root."""
    generation = "a" * 64
    relay_root = tmp_path / ".local/share/clio-relay"
    generation_root = relay_root / "generations" / generation
    environment = generation_root / "jarvis-venv"
    environment.mkdir(parents=True)
    _create_directory_alias(relay_root / "current", generation_root)
    alias = tmp_path / "home-alias"
    _create_directory_alias(alias, tmp_path)

    observed = bootstrap_reconcile_module._managed_generation_jarvis_environment(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        {"generation": generation},
        execution_environment=alias
        / ".local/share/clio-relay/generations"
        / generation
        / "jarvis-venv",
        home=tmp_path,
    )

    assert observed == environment.resolve(strict=True)


def test_generation_jarvis_environment_rejects_unowned_layout(tmp_path: Path) -> None:
    """A receipt cannot reuse a JARVIS directory outside the managed generation shape."""
    active_generation = "a" * 64
    relay_root = tmp_path / ".local/share/clio-relay"
    active_root = relay_root / "generations" / active_generation
    active_root.mkdir(parents=True)
    environment = tmp_path / "jarvis-venv"
    environment.mkdir()
    _create_directory_alias(relay_root / "current", active_root)

    assert (
        bootstrap_reconcile_module._managed_generation_jarvis_environment(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            {"generation": active_generation},
            execution_environment=environment,
            home=tmp_path,
        )
        is None
    )


def test_replacement_provider_attests_real_private_uv_tool(
    tmp_path: Path,
) -> None:
    """The replacement proof is derived from a real wheel, uv receipt, and RECORD."""
    uv_source = shutil.which("uv")
    assert uv_source is not None
    project = Path(__file__).parents[1]
    built = tmp_path / "built"
    subprocess.run(
        [uv_source, "build", "--wheel", "--out-dir", str(built)],
        cwd=project,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    wheels = list(built.glob("clio_relay-*.whl"))
    assert len(wheels) == 1
    version = wheels[0].name.removeprefix("clio_relay-").removesuffix("-py3-none-any.whl")

    canonical_home = tmp_path / "canonical-home"
    canonical_home.mkdir()
    home = tmp_path / "home"
    _create_directory_alias(home, canonical_home)
    preparing_parent = home / ".local/share/clio-relay/preparing"
    preparing_root = preparing_parent / "active"
    preparing_root.mkdir(parents=True, mode=0o700)
    preparing_parent.chmod(0o700)
    preparing_root.chmod(0o700)
    uv_executable = preparing_root / "pinned-uv"
    shutil.copy2(uv_source, uv_executable)
    uv_executable.chmod(0o500)
    uv_version = (
        subprocess.run(
            [str(uv_executable), "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        .stdout.strip()
        .split()[1]
    )
    source_root = preparing_root / "source"
    source_root.mkdir()
    wheel = preparing_root / wheels[0].name
    shutil.copy2(wheels[0], wheel)
    tool_directory = preparing_root / "uv-tools"
    tool_bin_directory = preparing_root / "uv-bin"
    environment = {
        **os.environ,
        "UV_TOOL_DIR": str(tool_directory),
        "UV_TOOL_BIN_DIR": str(tool_bin_directory),
        "UV_CACHE_DIR": str(preparing_root / "uv-cache"),
        "UV_PYTHON_INSTALL_DIR": str(home / ".local/share/clio-relay/uv-python"),
        "UV_PYTHON_DOWNLOADS": "never",
    }
    subprocess.run(
        [
            str(uv_executable),
            "tool",
            "install",
            "--force",
            "--python",
            "3.12",
            "--no-config",
            "--default-index",
            "https://pypi.org/simple",
            str(wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
        env=environment,
    )
    launcher = tool_bin_directory / ("clio-relay.exe" if os.name == "nt" else "clio-relay")
    provider = (
        tool_directory / "clio-relay" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    assert launcher.is_file()
    assert provider.is_file()
    wheel_sha256 = sha256_file(wheel)
    desired = _desired(
        uv_sha256=sha256_file(uv_executable),
        frpc_sha256="b" * 64,
        frps_sha256="c" * 64,
    ).model_copy(
        update={
            "relay_install_spec": f"clio-relay=={version}",
            "relay_artifact_sha256": wheel_sha256,
            "relay_source_identity": f"wheel:sha256:{wheel_sha256}",
            "uv_version": uv_version,
        }
    )
    desired_path = tmp_path / "desired.json"
    desired_path.write_text(desired.model_dump_json(), encoding="utf-8")
    proof = subprocess.run(
        [
            str(provider),
            "-I",
            "-c",
            "\n".join(
                [
                    "import json,sys",
                    "from pathlib import Path",
                    "from clio_relay.bootstrap_reconcile import (",
                    "    BootstrapDesiredState,prove_bootstrap_replacement_provider,",
                    ")",
                    "desired=BootstrapDesiredState.model_validate_json(Path(sys.argv[1]).read_text())",
                    "evidence=prove_bootstrap_replacement_provider(",
                    "    desired,",
                    "    uv_executable=Path(sys.argv[2]),",
                    "    tool_executable=Path(sys.argv[3]),",
                    "    source_artifact=Path(sys.argv[4]),",
                    "    tool_directory=Path(sys.argv[5]),",
                    "    tool_bin_directory=Path(sys.argv[6]),",
                    "    preparing_root=Path(sys.argv[7]),",
                    "    extracted_source_root=Path(sys.argv[8]),",
                    "    source_archive_sha256='f'*64,",
                    "    home=Path(sys.argv[9]),",
                    ")",
                    "print(json.dumps(evidence.model_dump(mode='json'),sort_keys=True))",
                ]
            ),
            str(desired_path),
            str(uv_executable),
            str(launcher),
            str(wheel),
            str(tool_directory),
            str(tool_bin_directory),
            str(preparing_root),
            str(source_root),
            str(home),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert proof.returncode == 0, proof.stdout + proof.stderr
    evidence = json.loads(proof.stdout)
    assert evidence["schema_version"] == "clio-relay.bootstrap-replacement-provider.v1"
    assert evidence["persistent_tool"]["source_artifact_sha256"] == wheel_sha256


def test_existing_jarvis_144_plans_staged_component_upgrade_to_148(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact legacy Ares layout enters the fenced staged-upgrade path."""
    bin_dir = tmp_path / ".local/bin"
    bin_dir.mkdir(parents=True)
    for name, content in (
        ("uv", b"uv"),
        ("frpc", b"frpc"),
        ("frps", b"frps"),
        ("clio-relay", b"relay"),
    ):
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
    (tmp_path / ".local/share/clio-relay/managed-jarvis-repo").rmdir()
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
    legacy_jarvis = bin_dir / "jarvis"
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
    component_runtime["jarvis-cd"] = {
        "error": "installed JARVIS-CD version is stale",
        "verified": False,
    }
    component_artifacts: dict[str, object] = {
        "clio-relay": {
            "runtime_executables": {"clio-relay": str(bin_dir / "clio-relay")},
        },
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
    assert plan.activation_paths["current"].before is None
    assert plan.activation_paths["managed_repo"].before is None
    assert plan.activation_paths["relay_launcher"].before is not None
    assert plan.activation_paths["jarvis_launcher"].before is not None

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
    previous_managed.parent.mkdir(parents=True)
    managed_repo.parent.mkdir(parents=True)
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
    managed_repo = tmp_path / "relay/managed-jarvis-repo"
    managed_repo.parent.mkdir(parents=True)
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

    with pytest.raises(ConfigurationError, match="changed .*reconciliation"):
        reconcile_managed_jarvis_repository(
            repos_file,
            managed_repo,
        )

    assert repos_file.read_bytes() == operator_update
    assert not list(tmp_path.glob(".repos.yaml.*.tmp"))


def test_managed_repo_reconcile_cleans_a_proven_previous_registration(tmp_path: Path) -> None:
    """An existing managed alias does not prevent exact legacy-path cleanup."""
    repos_file = tmp_path / "repos.yaml"
    managed = tmp_path / "managed-jarvis-repo"
    previous = tmp_path / "legacy/clio_relay"
    operator = tmp_path / "operator/clio_relay"
    repos_file.write_text(
        yaml.safe_dump(
            {
                "repos": [
                    str(managed.absolute()),
                    str(previous.absolute()),
                    str(operator.absolute()),
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    evidence = reconcile_managed_jarvis_repository(
        repos_file,
        managed,
        previous_managed_repos=(previous,),
        exchange_identity="a" * 64,
    )

    assert evidence["action"] == "updated"
    assert evidence["added_managed_repos"] == []
    assert evidence["removed_previous_managed_repos"] == [str(previous.absolute())]
    assert yaml.safe_load(repos_file.read_text(encoding="utf-8"))["repos"] == [
        str(managed.absolute()),
        str(operator.absolute()),
    ]


@pytest.mark.parametrize("race_boundary", ["before_exchange", "after_exchange", "crash"])
def test_managed_repo_atomic_exchange_preserves_or_recovers_racing_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race_boundary: str,
) -> None:
    """The exact exchange boundary never silently overwrites an operator edit."""
    repos_file = tmp_path / "repos.yaml"
    repos_file.write_text("repos:\n  - /operator/original\n", encoding="utf-8")
    operator_update = b"repos:\n  - /operator/concurrent\n"
    managed = tmp_path / "managed-jarvis-repo"
    original_exchange = bootstrap_reconcile_module._atomic_exchange_paths  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    exchanged = False

    def raced_exchange(left: Path, right: Path) -> None:
        nonlocal exchanged
        if exchanged:
            original_exchange(left, right)
            return
        exchanged = True
        if race_boundary == "before_exchange":
            repos_file.write_bytes(operator_update)
        original_exchange(left, right)
        if race_boundary == "after_exchange":
            repos_file.write_bytes(operator_update)
        if race_boundary == "crash":
            raise RuntimeError("crash after atomic exchange")

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_atomic_exchange_paths",
        raced_exchange,
    )

    if race_boundary == "crash":
        with pytest.raises(RuntimeError, match="crash after atomic exchange"):
            reconcile_managed_jarvis_repository(
                repos_file,
                managed,
                exchange_identity="b" * 64,
            )
        monkeypatch.setattr(
            bootstrap_reconcile_module,
            "_atomic_exchange_paths",
            original_exchange,
        )
        recovered = reconcile_managed_jarvis_repository(
            repos_file,
            managed,
            exchange_identity="b" * 64,
        )
        assert recovered["action"] == "updated"
        assert yaml.safe_load(repos_file.read_text(encoding="utf-8"))["repos"] == [
            str(managed.absolute()),
            "/operator/original",
        ]
    else:
        with pytest.raises(ConfigurationError, match="changed .*reconciliation"):
            reconcile_managed_jarvis_repository(
                repos_file,
                managed,
                exchange_identity="b" * 64,
            )
        assert repos_file.read_bytes() == operator_update
    assert not list(tmp_path.glob(".repos.yaml.*.exchange"))


@pytest.mark.parametrize("race_boundary", ["before_exchange", "after_exchange", "crash"])
def test_stable_link_atomic_exchange_preserves_or_recovers_racing_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race_boundary: str,
) -> None:
    """Stable launcher exchange restores a raced object or resumes after a crash."""
    destination = tmp_path / "launcher"
    destination.write_bytes(b"legacy")
    destination.chmod(0o755)
    snapshot = bootstrap_reconcile_module._capture_activation_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        destination,
        kind="file_or_symlink",
        maximum=1024,
        allow_absent=False,
    )
    target = tmp_path / "new-launcher"
    target.write_bytes(b"new")
    target.chmod(0o755)
    operator_update = b"operator-concurrent"
    original_exchange = bootstrap_reconcile_module._atomic_exchange_paths  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    simulated_links: dict[Path, Path] = {}
    if os.name == "nt":
        real_is_symlink = Path.is_symlink
        real_resolve = Path.resolve
        real_readlink = os.readlink

        def simulated_symlink_to(
            path: Path,
            link_target: Path | str,
            target_is_directory: bool = False,
        ) -> None:
            del target_is_directory
            if path.exists() or path in simulated_links:
                raise FileExistsError(path)
            path.write_bytes(b"simulated-symlink")
            simulated_links[path] = Path(link_target)

        def simulated_is_symlink(path: Path) -> bool:
            return path in simulated_links or real_is_symlink(path)

        def simulated_readlink(path: Path | str) -> str:
            candidate = Path(path)
            if candidate in simulated_links:
                return str(simulated_links[candidate])
            return real_readlink(path)

        def simulated_resolve(path: Path, strict: bool = False) -> Path:
            candidate = path
            for _ in range(4):
                if candidate not in simulated_links:
                    return real_resolve(candidate, strict=strict)
                candidate = simulated_links[candidate]
            raise AssertionError("simulated symlink chain did not terminate")

        real_exchange = original_exchange

        def simulated_exchange(left: Path, right: Path) -> None:
            left_target = simulated_links.pop(left, None)
            right_target = simulated_links.pop(right, None)
            real_exchange(left, right)
            if left_target is not None:
                simulated_links[right] = left_target
            if right_target is not None:
                simulated_links[left] = right_target

        original_exchange = simulated_exchange
        monkeypatch.setattr(Path, "symlink_to", simulated_symlink_to)
        monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)
        monkeypatch.setattr(Path, "resolve", simulated_resolve)
        monkeypatch.setattr(bootstrap_reconcile_module.os, "readlink", simulated_readlink)
    exchanged = False

    def raced_exchange(left: Path, right: Path) -> None:
        nonlocal exchanged
        if exchanged:
            original_exchange(left, right)
            return
        exchanged = True
        if race_boundary == "before_exchange":
            destination.write_bytes(operator_update)
        original_exchange(left, right)
        if race_boundary == "after_exchange":
            destination.unlink()
            simulated_links.pop(destination, None)
            destination.write_bytes(operator_update)
        if race_boundary == "crash":
            raise RuntimeError("crash after atomic exchange")

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_atomic_exchange_paths",
        raced_exchange,
    )
    reconcile = bootstrap_reconcile_module._reconcile_activation_symlink  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    if race_boundary == "crash":
        with pytest.raises(RuntimeError, match="crash after atomic exchange"):
            reconcile(
                snapshot,
                expected_target=target,
                label="stable launcher",
                exchange_identity="c" * 64,
            )
        monkeypatch.setattr(
            bootstrap_reconcile_module,
            "_atomic_exchange_paths",
            original_exchange,
        )
        assert (
            reconcile(
                snapshot,
                expected_target=target,
                label="stable launcher",
                exchange_identity="c" * 64,
            )
            == "retargeted"
        )
        assert os.readlink(destination) == str(target)
    else:
        with pytest.raises(ConfigurationError, match="changed .*atomic activation"):
            reconcile(
                snapshot,
                expected_target=target,
                label="stable launcher",
                exchange_identity="c" * 64,
            )
        assert destination.read_bytes() == operator_update
    assert not list(tmp_path.glob(".launcher.*.exchange"))


def test_atomic_exchange_preflight_restores_and_cleans_every_directory(
    tmp_path: Path,
) -> None:
    """The exact exchange primitive is exercised without retaining probe state."""
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()

    evidence = bootstrap_reconcile_module.verify_atomic_exchange_support(
        (first, second, first),
        identity="d" * 64,
    )

    assert evidence["directories"] == [str(first), str(second)]
    assert list(first.iterdir()) == []
    assert list(second.iterdir()) == []


def test_staged_activation_adopts_legacy_paths_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-generation install converges every stable path to canonical links."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    share = tmp_path / ".local/share/clio-relay"
    bin_dir = tmp_path / ".local/bin"
    generation = share / "generations" / desired.fingerprint
    (generation / "bin").mkdir(parents=True)
    (generation / "source/jarvis-packages/clio_relay").mkdir(parents=True)
    bin_dir.mkdir(parents=True)
    for path, payload in (
        (share / "install-receipt.json", b"legacy-receipt"),
        (bin_dir / "clio-relay", b"legacy-relay"),
        (bin_dir / "jarvis", b"legacy-jarvis"),
        (generation / "install-receipt.json", b"new-receipt"),
        (generation / "bin/clio-relay", b"new-relay"),
        (generation / "bin/jarvis", b"new-jarvis"),
    ):
        path.write_bytes(payload)
        path.chmod(0o755)
    activation_paths = bootstrap_reconcile_module._capture_reconcile_activation_paths(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        home=tmp_path,
    )
    plan = BootstrapReconcilePlan(
        mode="component-upgrade",
        desired_fingerprint=desired.fingerprint,
        component_actions={"clio-relay": "replace"},
        activation_paths=activation_paths,
    )

    simulated_links: dict[Path, Path] = {}
    if os.name == "nt":
        original_is_symlink = Path.is_symlink
        original_resolve = Path.resolve
        original_readlink = os.readlink
        original_replace = os.replace

        def simulated_target(path: Path) -> Path:
            candidate = path
            for _ in range(10):
                matches: list[tuple[int, Path, Path]] = []
                for link, target in simulated_links.items():
                    try:
                        relative = candidate.relative_to(link)
                    except ValueError:
                        continue
                    matches.append((len(link.parts), target, relative))
                if not matches:
                    return candidate
                _length, target, relative = max(matches, key=lambda item: item[0])
                candidate = target / relative
            raise AssertionError("simulated symlink chain did not terminate")

        def simulated_symlink_to(
            path: Path,
            target: Path | str,
            target_is_directory: bool = False,
        ) -> None:
            del target_is_directory
            if path.exists() or path in simulated_links:
                raise FileExistsError(path)
            path.write_bytes(b"simulated-symlink")
            simulated_links[path] = Path(target)

        def simulated_is_symlink(path: Path) -> bool:
            return path in simulated_links or original_is_symlink(path)

        def simulated_resolve(path: Path, strict: bool = False) -> Path:
            return original_resolve(simulated_target(path), strict=strict)

        def simulated_readlink(path: Path | str) -> str:
            candidate = Path(path)
            if candidate in simulated_links:
                return str(simulated_links[candidate])
            return original_readlink(path)

        def simulated_replace(source: Path | str, destination: Path | str) -> None:
            source_path = Path(source)
            destination_path = Path(destination)
            original_replace(source_path, destination_path)
            if source_path in simulated_links:
                simulated_links[destination_path] = simulated_links.pop(source_path)

        monkeypatch.setattr(Path, "symlink_to", simulated_symlink_to)
        monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)
        monkeypatch.setattr(Path, "resolve", simulated_resolve)
        monkeypatch.setattr(bootstrap_reconcile_module.os, "readlink", simulated_readlink)
        monkeypatch.setattr(bootstrap_reconcile_module.os, "replace", simulated_replace)

    first = reconcile_staged_activation_links(plan, generation=generation, home=tmp_path)
    second = reconcile_staged_activation_links(plan, generation=generation, home=tmp_path)
    first_actions = cast(dict[str, str], first["actions"])
    second_actions = cast(dict[str, str], second["actions"])

    assert first_actions == {
        "current": "created",
        "install_receipt": "retargeted",
        "relay_launcher": "retargeted",
        "jarvis_launcher": "retargeted",
        "managed_repo": "created",
    }
    assert set(second_actions.values()) == {"reused"}
    expected_targets = {
        share / "current": generation,
        share / "install-receipt.json": share / "current/install-receipt.json",
        bin_dir / "clio-relay": share / "current/bin/clio-relay",
        bin_dir / "jarvis": share / "current/bin/jarvis",
        share / "managed-jarvis-repo": share / "current/source/jarvis-packages/clio_relay",
    }
    assert {path: bootstrap_reconcile_module.os.readlink(path) for path in expected_targets} == {
        path: str(target) for path, target in expected_targets.items()
    }


def test_staged_activation_rejects_manifest_path_substitution_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A signed plan cannot redirect any stable activation destination."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    share = tmp_path / ".local/share/clio-relay"
    generation = share / "generations" / desired.fingerprint
    generation.mkdir(parents=True)
    activation_paths = {
        "current": BootstrapActivationPath(
            path=str(tmp_path / "attacker/current"),
            kind="symlink",
        ),
        "install_receipt": BootstrapActivationPath(
            path=str(share / "install-receipt.json"),
            kind="file_or_symlink",
        ),
        "relay_launcher": BootstrapActivationPath(
            path=str(tmp_path / ".local/bin/clio-relay"),
            kind="file_or_symlink",
        ),
        "jarvis_launcher": BootstrapActivationPath(
            path=str(tmp_path / ".local/bin/jarvis"),
            kind="file_or_symlink",
        ),
        "managed_repo": BootstrapActivationPath(
            path=str(share / "managed-jarvis-repo"),
            kind="symlink",
        ),
    }
    plan = BootstrapReconcilePlan(
        mode="component-upgrade",
        desired_fingerprint=desired.fingerprint,
        component_actions={"clio-relay": "replace"},
        activation_paths=activation_paths,
    )

    def unexpected_symlink(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("activation wrote before validating destinations")

    monkeypatch.setattr(Path, "symlink_to", unexpected_symlink)

    with pytest.raises(ConfigurationError, match="destination changed: current"):
        reconcile_staged_activation_links(plan, generation=generation, home=tmp_path)

    assert not (tmp_path / "attacker/current").exists()


def test_finish_staged_activation_rejects_manifest_tamper_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery uses the journaled manifest digest instead of trusting disk state."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    generation = tmp_path / "generation"
    generation.mkdir()
    (generation / "manifest.json").write_bytes(b'{"tampered":true}\n')

    def unexpected_mutation(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("tampered manifest reached activation")

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "reconcile_staged_activation_links",
        unexpected_mutation,
    )
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "reconcile_managed_jarvis_repository",
        unexpected_mutation,
    )

    with pytest.raises(ConfigurationError, match="manifest changed before activation"):
        finish_staged_activation(
            desired,
            generation=generation,
            expected_manifest_sha256="f" * 64,
            home=tmp_path,
        )


def test_staged_activation_preserves_a_raced_absent_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exclusive link creation fails closed without clobbering a racing writer."""
    destination = tmp_path / "stable-link"
    target = tmp_path / "target"
    target.mkdir()
    snapshot = BootstrapActivationPath(path=str(destination), kind="symlink")

    def race_symlink(
        path: Path,
        _target: Path,
        target_is_directory: bool = False,
    ) -> None:
        del target_is_directory
        assert path == destination
        path.write_bytes(b"operator-owned")
        raise FileExistsError(path)

    monkeypatch.setattr(Path, "symlink_to", race_symlink)

    with pytest.raises(ConfigurationError, match="appeared before activation"):
        bootstrap_reconcile_module._reconcile_activation_symlink(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            snapshot,
            expected_target=target,
            label="stable test link",
        )

    assert destination.read_bytes() == b"operator-owned"


def test_staged_activation_refuses_a_changed_legacy_launcher(tmp_path: Path) -> None:
    """The activation fence cannot replace a launcher changed after planning."""
    destination = tmp_path / "legacy-launcher"
    destination.write_bytes(b"original")
    destination.chmod(0o755)
    snapshot = bootstrap_reconcile_module._capture_activation_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        destination,
        kind="file_or_symlink",
        maximum=1024,
        allow_absent=False,
    )
    destination.write_bytes(b"operator-replacement")
    target = tmp_path / "new-launcher"
    target.write_bytes(b"new")

    with pytest.raises(ConfigurationError, match="changed after bootstrap inspection"):
        bootstrap_reconcile_module._reconcile_activation_symlink(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            snapshot,
            expected_target=target,
            label="stable relay launcher",
        )

    assert destination.read_bytes() == b"operator-replacement"


@pytest.mark.parametrize("crash_boundary", ["before_repository", "after_replace", "completed"])
def test_staged_activation_resumes_across_repository_crash_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_boundary: str,
) -> None:
    """Forward recovery completes exact repository migration after every boundary."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    execution_root = tmp_path / ".local/share/clio-relay/jarvis-venv"
    execution_bin = execution_root / ("Scripts" if os.name == "nt" else "bin")
    execution_bin.mkdir(parents=True)
    execution_python = execution_bin / ("python.exe" if os.name == "nt" else "python")
    execution_jarvis = execution_bin / ("jarvis.exe" if os.name == "nt" else "jarvis")
    for path in (execution_python, execution_jarvis):
        path.write_bytes(path.name.encode())
        path.chmod(0o755)
    legacy_identity = execution_environment_identity(
        execution_root,
        executables={"python": execution_python, "jarvis": execution_jarvis},
    )
    generation = tmp_path / ".local/share/clio-relay/generations" / desired.fingerprint
    generation.mkdir(parents=True)
    generation_receipt = generation / "install-receipt.json"
    generation_receipt.write_text("{}\n", encoding="utf-8")
    plan = BootstrapReconcilePlan(
        mode="component-upgrade",
        desired_fingerprint=desired.fingerprint,
        component_actions={"clio-relay": "replace"},
        reusable_paths={
            "jarvis_execution_environment": str(execution_root),
            "jarvis_execution_python": str(execution_python),
            "jarvis_execution_executable": str(execution_jarvis),
        },
    )
    manifest = {
        "schema_version": "clio-relay.bootstrap-generation.v1",
        "fingerprint": desired.fingerprint,
        "plan": plan.model_dump(mode="json"),
        "legacy_execution_identity": legacy_identity,
        "active_execution_identity": legacy_identity,
        "jarvis_wrapper_sha256": "1" * 64,
        "install_receipt": str(generation_receipt),
        "install_receipt_sha256": _digest(generation_receipt.read_bytes()),
    }
    manifest_bytes = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    (generation / "manifest.json").write_bytes(manifest_bytes)
    manifest_sha256 = _digest(manifest_bytes)
    jarvis_root = tmp_path / ".ppi-jarvis"
    jarvis_root.mkdir()
    repos_file = jarvis_root / "repos.yaml"
    previous_repo = tmp_path / ".local/src/clio-relay/jarvis-packages/clio_relay"
    operator_repo = tmp_path / "operator/clio_relay"
    repos_file.write_text(
        yaml.safe_dump(
            {"repos": [str(operator_repo.absolute()), str(previous_repo.absolute())]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    link_calls = 0

    def simulated_links(
        _plan: BootstrapReconcilePlan,
        *,
        generation: Path,
        home: Path | None = None,
    ) -> dict[str, object]:
        nonlocal link_calls
        del generation, home
        link_calls += 1
        action = "created" if link_calls == 1 else "reused"
        return {
            "schema_version": "clio-relay.bootstrap-activation.v1",
            "generation": desired.fingerprint,
            "actions": {"managed_repo": action},
        }

    def simulated_inspection(
        _desired: BootstrapDesiredState,
        *,
        generation: Path,
        legacy_execution_identity: dict[str, object],
    ) -> dict[str, object]:
        del generation, legacy_execution_identity
        return {"manifest_sha256": manifest_sha256}

    def simulated_stable_link(path: Path, *, expected: Path, label: str) -> Path:
        del path, label
        return expected

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "inspect_prepared_generation",
        simulated_inspection,
    )
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "reconcile_staged_activation_links",
        simulated_links,
    )
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_verify_stable_symlink",
        simulated_stable_link,
    )
    original_readlink = os.readlink
    managed_repo = tmp_path / ".local/share/clio-relay/managed-jarvis-repo"
    managed_target = tmp_path / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay"

    def simulated_readlink(path: Path | str) -> str:
        if Path(path) == managed_repo:
            return str(managed_target)
        return original_readlink(path)

    monkeypatch.setattr(
        bootstrap_reconcile_module.os,
        "readlink",
        simulated_readlink,
    )
    original_reconcile = reconcile_managed_jarvis_repository
    repository_calls = 0

    def crashable_reconcile(
        path: Path,
        managed: Path,
        *,
        previous_managed_repos: tuple[Path, ...] = (),
        exchange_identity: str | None = None,
    ) -> dict[str, object]:
        nonlocal repository_calls
        repository_calls += 1
        if repository_calls == 1 and crash_boundary == "before_repository":
            raise RuntimeError("crash before repository migration")
        evidence = original_reconcile(
            path,
            managed,
            previous_managed_repos=previous_managed_repos,
            exchange_identity=exchange_identity,
        )
        if repository_calls == 1 and crash_boundary == "after_replace":
            raise RuntimeError("crash after repository replacement")
        return evidence

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "reconcile_managed_jarvis_repository",
        crashable_reconcile,
    )

    if crash_boundary != "completed":
        with pytest.raises(RuntimeError, match="crash"):
            finish_staged_activation(
                desired,
                generation=generation,
                expected_manifest_sha256=manifest_sha256,
                home=tmp_path,
            )
    first_completed = finish_staged_activation(
        desired,
        generation=generation,
        expected_manifest_sha256=manifest_sha256,
        home=tmp_path,
    )
    second_completed = finish_staged_activation(
        desired,
        generation=generation,
        expected_manifest_sha256=manifest_sha256,
        home=tmp_path,
    )

    repositories = yaml.safe_load(repos_file.read_text(encoding="utf-8"))["repos"]
    assert repositories == [str(managed_repo.absolute()), str(operator_repo.absolute())]
    first_repository = cast(dict[str, object], first_completed["jarvis_repository"])
    second_repository = cast(dict[str, object], second_completed["jarvis_repository"])
    second_update = cast(dict[str, object], second_repository["repositories"])
    assert second_update["action"] == "reused"
    assert first_repository["target"] == str(managed_target)
    assert link_calls >= 2


def test_managed_repo_repair_refuses_unproven_broken_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken link is existing untrusted state, not an absent managed path."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    generation = tmp_path / ".local/share/clio-relay/generations" / desired.fingerprint
    expected = generation / "source/jarvis-packages/clio_relay"
    expected.mkdir(parents=True)
    current = tmp_path / ".local/share/clio-relay/current"
    managed = tmp_path / ".local/share/clio-relay/managed-jarvis-repo"
    original_lstat = Path.lstat
    original_readlink = os.readlink
    original_resolve = Path.resolve
    original_verify = bootstrap_reconcile_module._verify_stable_symlink  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    managed_identity = original_lstat(expected)

    def simulated_resolve(path: Path, strict: bool = False) -> Path:
        if path == current / "source/jarvis-packages/clio_relay":
            return original_resolve(expected, strict=strict)
        return original_resolve(path, strict=strict)

    def simulated_verify(path: Path, *, expected: Path, label: str) -> Path:
        if path == current:
            return generation
        return original_verify(path, expected=expected, label=label)

    def simulated_lstat(path: Path) -> os.stat_result | SimpleNamespace:
        if path == managed:
            return SimpleNamespace(
                st_dev=managed_identity.st_dev,
                st_ino=managed_identity.st_ino,
                st_mode=stat.S_IFLNK,
                st_size=1,
                st_mtime_ns=managed_identity.st_mtime_ns,
                st_ctime_ns=managed_identity.st_ctime_ns,
            )
        return original_lstat(path)

    def simulated_readlink(path: Path | str) -> str:
        if Path(path) == managed:
            return str(tmp_path / "attacker-controlled/missing")
        return original_readlink(path)

    monkeypatch.setattr(Path, "lstat", simulated_lstat)
    monkeypatch.setattr(Path, "resolve", simulated_resolve)
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_verify_stable_symlink",
        simulated_verify,
    )
    monkeypatch.setattr(
        bootstrap_reconcile_module.os,
        "readlink",
        simulated_readlink,
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
    execution_root = tmp_path / "legacy-jarvis-venv"
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
    active_root = tmp_path / "active-jarvis-venv"
    active_bin = active_root / ("Scripts" if os.name == "nt" else "bin")
    active_bin.mkdir(parents=True)
    active_python = active_bin / ("python.exe" if os.name == "nt" else "python")
    active_jarvis = active_bin / ("jarvis.exe" if os.name == "nt" else "jarvis")
    for path, payload in (
        (active_python, b"active-python-runtime"),
        (active_jarvis, b"active-jarvis-launcher"),
    ):
        path.write_bytes(payload)
        path.chmod(0o755)
    active_identity = execution_environment_identity(
        active_root,
        executables={"python": active_python, "jarvis": active_jarvis},
    )
    generation = tmp_path / "generation"
    generation_bin = generation / "bin"
    generation_bin.mkdir(parents=True)
    wrapper = generation_bin / "jarvis"
    wrapper_evidence = write_jarvis_wrapper(wrapper, active_python)
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
        "mode": "component-upgrade",
        "desired_fingerprint": desired.fingerprint,
        "component_actions": {"clio-relay": "replace"},
    }
    manifest = {
        "schema_version": "clio-relay.bootstrap-generation.v1",
        "fingerprint": desired.fingerprint,
        "plan": plan,
        "legacy_execution_identity": execution_identity,
        "active_execution_identity": active_identity,
        "jarvis_wrapper_sha256": wrapper_evidence["sha256"],
        "install_receipt": str(receipt_path),
        "install_receipt_sha256": _digest(receipt_path.read_bytes()),
    }
    (generation / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (generation / ".prepared").open("w", encoding="ascii", newline="\n") as stream:
        stream.write(desired.fingerprint + "\n")
    installation = _installation_info(desired)
    receipt = cast(dict[str, object], installation["receipt"])
    receipt["component_artifacts"] = {
        "jarvis-cd": {"runtime_interpreters": {"execution": str(active_python)}}
    }

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return installation

    monkeypatch.setattr(
        "clio_relay.bootstrap_reconcile.installation_info",
        read_installation,
    )

    evidence = inspect_prepared_generation(
        desired,
        generation=generation,
        legacy_execution_identity=execution_identity,
    )

    launcher_targets = cast(dict[str, object], evidence["launcher_targets"])
    assert launcher_targets["jarvis"] == str(wrapper)
    assert wrapper.read_bytes() == jarvis_wrapper_payload(active_python)

    artifacts = cast(dict[str, object], receipt["component_artifacts"])
    jarvis_artifact = cast(dict[str, object], artifacts["jarvis-cd"])
    interpreters = cast(dict[str, object], jarvis_artifact["runtime_interpreters"])
    interpreters["execution"] = str(execution_python)
    with pytest.raises(ConfigurationError, match="not bound to its install receipt"):
        inspect_prepared_generation(
            desired,
            generation=generation,
            legacy_execution_identity=execution_identity,
        )
    interpreters["execution"] = str(active_python)

    receipt_before = receipt_path.read_bytes()
    receipt_path.write_text('{"changed":true}\n', encoding="utf-8")
    with pytest.raises(ConfigurationError, match="manifest identity changed"):
        inspect_prepared_generation(
            desired,
            generation=generation,
            legacy_execution_identity=execution_identity,
        )
    receipt_path.write_bytes(receipt_before)

    wrapper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    wrapper.chmod(0o755)
    with pytest.raises(ConfigurationError, match="JARVIS wrapper identity changed"):
        inspect_prepared_generation(
            desired,
            generation=generation,
            legacy_execution_identity=execution_identity,
        )


def test_jarvis_wrapper_executes_the_lexical_venv_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolving a venv Python must not bypass its installed site-packages."""
    lexical_python = tmp_path / "venv/bin/python"
    resolved_python = tmp_path / "base/bin/python3"
    lexical_python.parent.mkdir(parents=True)
    resolved_python.parent.mkdir(parents=True)
    for path in (lexical_python, resolved_python):
        path.write_bytes(b"python")
        path.chmod(0o755)
    original_resolve = Path.resolve

    def simulated_venv_resolve(path: Path, strict: bool = False) -> Path:
        if path == lexical_python:
            return original_resolve(resolved_python, strict=strict)
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", simulated_venv_resolve)

    payload = jarvis_wrapper_payload(lexical_python).decode("utf-8")

    assert f"exec {shlex.quote(str(lexical_python))}" in payload
    assert f"exec {shlex.quote(str(resolved_python))}" not in payload


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
        "active_execution_identity": original_identity,
        "jarvis_wrapper_sha256": wrapper_evidence["sha256"],
        "install_receipt": str(receipt_path),
        "install_receipt_sha256": _digest(receipt_path.read_bytes()),
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
    manifest["active_execution_identity"] = replacement_identity
    manifest["jarvis_wrapper_sha256"] = replacement_wrapper["sha256"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ConfigurationError, match="not bound to its install receipt"):
        bootstrap_reconcile_module._verify_active_generation_jarvis_wrapper(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            generation,
            desired=desired,
            installation=installation,
        )


def test_managed_jarvis_interpreter_is_bound_to_receipt_and_lexical_home(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Managed selection preserves the lexical venv while proving canonical identity."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    canonical_home = tmp_path / "canonical-home"
    canonical_home.mkdir()
    lexical_home = tmp_path / "home-alias"
    _create_directory_alias(lexical_home, canonical_home)

    generation = lexical_home / ".local/share/clio-relay/generations" / desired.fingerprint
    execution_root = generation / "jarvis-venv"
    execution_bin = execution_root / ("Scripts" if os.name == "nt" else "bin")
    execution_bin.mkdir(parents=True)
    execution_python = execution_bin / ("python.exe" if os.name == "nt" else "python")
    execution_jarvis = execution_bin / ("jarvis.exe" if os.name == "nt" else "jarvis")
    for path in (execution_python, execution_jarvis):
        path.write_bytes(path.name.encode())
        path.chmod(0o755)
    execution_identity = execution_environment_identity(
        execution_root,
        executables={"python": execution_python, "jarvis": execution_jarvis},
    )
    generation_bin = generation / "bin"
    generation_bin.mkdir()
    (lexical_home / ".local/bin").mkdir(parents=True)
    wrapper = write_jarvis_wrapper(generation_bin / "jarvis", execution_python)
    receipt_path = generation / "install-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    manifest = {
        "schema_version": "clio-relay.bootstrap-generation.v1",
        "fingerprint": desired.fingerprint,
        "plan": {
            "mode": "component-upgrade",
            "desired_fingerprint": desired.fingerprint,
            "component_actions": {"jarvis-cd": "replace"},
        },
        "legacy_execution_identity": execution_identity,
        "active_execution_identity": execution_identity,
        "jarvis_wrapper_sha256": wrapper["sha256"],
        "install_receipt": str(receipt_path),
        "install_receipt_sha256": _digest(receipt_path.read_bytes()),
    }
    (generation / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    installation = _installation_info(desired)
    receipt = cast(dict[str, object], installation["receipt"])
    receipt["component_artifacts"] = {
        "jarvis-cd": {"runtime_interpreters": {"execution": str(execution_python)}}
    }

    bootstrap_reconcile_module._verify_active_generation_jarvis_wrapper(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        generation.resolve(strict=True),
        desired=desired,
        installation=installation,
    )

    stable_paths: list[tuple[Path, Path]] = []

    def verify_stable(path: Path, *, expected: Path, label: str) -> Path:
        del label
        stable_paths.append((path, expected))
        return expected.resolve(strict=True)

    def read_installation(path: Path | None = None) -> dict[str, object]:
        assert path == lexical_home / ".local/share/clio-relay/install-receipt.json"
        return installation

    def classify_launcher(path: Path, *, lexical_home: Path) -> bool:
        assert path == lexical_home / ".local/bin/jarvis"
        assert lexical_home == tmp_path / "home-alias"
        return True

    monkeypatch.setattr(bootstrap_reconcile_module, "_verify_stable_symlink", verify_stable)
    monkeypatch.setattr(bootstrap_reconcile_module, "installation_info", read_installation)
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_relay_managed_jarvis_launcher_selected",
        classify_launcher,
    )

    selected = resolve_receipt_bound_jarvis_python(
        str(canonical_home / ".local/bin/jarvis"),
        home=lexical_home,
    )

    assert selected == str(execution_python)
    assert len(stable_paths) == 3
    assert Path(cast(str, selected)).resolve().is_relative_to(canonical_home.resolve())
    assert cast(str, selected).startswith(str(lexical_home))


def test_conventional_home_jarvis_launcher_without_relay_ownership_is_unmanaged(
    tmp_path: Path,
) -> None:
    """A normal user-installed launcher may use the conventional local-bin path."""
    launcher = tmp_path / ".local/bin/jarvis"
    launcher.parent.mkdir(parents=True)
    launcher.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    launcher.chmod(0o755)

    selected = resolve_receipt_bound_jarvis_python(
        str(launcher),
        home=tmp_path,
    )

    assert selected is None


def test_conventional_home_jarvis_external_symlink_is_unmanaged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A uv- or pipx-style local-bin symlink remains outside relay ownership."""
    launcher = tmp_path / ".local/bin/jarvis"
    launcher.parent.mkdir(parents=True)
    launcher.write_text("external-link-placeholder\n", encoding="utf-8")
    external_target = tmp_path / ".local/share/uv/tools/jarvis-cd/bin/jarvis"
    _simulate_file_symlink(
        monkeypatch,
        path=launcher,
        target=external_target,
    )

    selected = resolve_receipt_bound_jarvis_python(
        str(launcher),
        home=tmp_path,
    )

    assert selected is None


@pytest.mark.parametrize("receipt_payload", [None, "not-json\n"])
def test_proven_managed_jarvis_launcher_fails_closed_on_missing_or_corrupt_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_payload: str | None,
) -> None:
    """Receipt loss cannot downgrade an already proven relay activation to unmanaged."""
    launcher = tmp_path / ".local/bin/jarvis"
    launcher.parent.mkdir(parents=True)
    launcher.write_text("relay-managed-placeholder\n", encoding="utf-8")
    receipt = tmp_path / ".local/share/clio-relay/install-receipt.json"
    receipt.parent.mkdir(parents=True)
    if receipt_payload is not None:
        receipt.write_text(receipt_payload, encoding="utf-8")
    _simulate_file_symlink(
        monkeypatch,
        path=launcher,
        target=tmp_path / ".local/share/clio-relay/current/bin/jarvis",
    )

    with pytest.raises(ConfigurationError, match="installation receipt is invalid"):
        resolve_receipt_bound_jarvis_python(
            str(launcher),
            home=tmp_path,
        )


def test_managed_jarvis_interpreter_fails_closed_on_unverified_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A managed launcher cannot fall back to ambient Python after receipt failure."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    installation = _installation_info(desired)
    runtime = cast(dict[str, object], installation["component_runtime"])
    runtime["jarvis-cd"] = {"verified": False}

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return installation

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "installation_info",
        read_installation,
    )

    def classify_launcher(_path: Path, *, lexical_home: Path) -> bool:
        return lexical_home == tmp_path

    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_relay_managed_jarvis_launcher_selected",
        classify_launcher,
    )

    with pytest.raises(ConfigurationError, match="runtime did not verify"):
        resolve_receipt_bound_jarvis_python(
            str(tmp_path / ".local/bin/jarvis"),
            home=tmp_path,
        )


def test_managed_jarvis_interpreter_requires_relay_execution_packages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker selection rejects a JARVIS venv without receipt-bound relay packages."""
    desired = _desired(uv_sha256="a" * 64, frpc_sha256="b" * 64, frps_sha256="c" * 64)
    installation = _installation_info(desired)
    runtime = cast(dict[str, object], installation["component_runtime"])
    runtime["clio-relay"] = {
        "persistent_tool_verified": True,
        "execution_runtime_verified": False,
    }

    def read_installation(_path: Path | None = None) -> dict[str, object]:
        return installation

    def classify_launcher(_path: Path, *, lexical_home: Path) -> bool:
        return lexical_home == tmp_path

    monkeypatch.setattr(bootstrap_reconcile_module, "installation_info", read_installation)
    monkeypatch.setattr(
        bootstrap_reconcile_module,
        "_relay_managed_jarvis_launcher_selected",
        classify_launcher,
    )

    with pytest.raises(ConfigurationError, match="execution runtime did not verify"):
        resolve_receipt_bound_jarvis_python(
            str(tmp_path / ".local/bin/jarvis"),
            home=tmp_path,
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
