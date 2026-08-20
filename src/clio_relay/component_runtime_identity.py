"""Verify current-process evidence for receipt-bound component launchers.

Extracted from ``installation.py`` (iowarp/clio-relay#231): this owns
``_component_runtime_identity`` and its four helpers -- the layer that
re-probes each receipt-bound component (the relay's own persistent uv tool,
clio-kit's native JARVIS contract, and any other native-execution component)
against the RUNNING process's evidence, composing the lower-level probes
(``persistent_uv_tool_probe``, ``native_jarvis_contract``,
``python_distribution_probe``, ``wheel_record_closure``) into one identity
document per component.
"""

from __future__ import annotations

import json
import sys
from importlib import metadata
from pathlib import Path
from typing import cast

from clio_relay.bounded_process import BoundedProcessError, run_bounded_process
from clio_relay.errors import ConfigurationError
from clio_relay.installation_receipt_models import (
    ComponentArtifactIdentity,
    InstallReceipt,
    NativeJarvisExecutionCapability,
)
from clio_relay.native_jarvis_contract import (
    _native_capability_matches_component,
    probe_clio_kit_native_execution_contract,
    probe_jarvis_native_execution_capability,
)
from clio_relay.persistent_uv_tool_probe import probe_persistent_uv_tool_identity
from clio_relay.python_distribution_probe import (
    _direct_url_source_matches,
    _distribution_direct_url,
    _distribution_progress_entry_points,
    _jarvis_executable_matches_interpreter,
    _normalized_distribution_name,
    _probe_python_distribution,
)
from clio_relay.validation_report import sha256_file
from clio_relay.wheel_record_closure import _probe_python_distribution_record_closure


def _component_runtime_identity(receipt: InstallReceipt) -> dict[str, object]:
    """Return current process evidence for receipt-bound component launchers."""
    identities: dict[str, object] = {}
    relay_component = receipt.component_artifacts.get("clio-relay")
    if relay_component is not None and relay_component.persistent_tool is not None:
        relay_identity = persistent_component_runtime_identity(
            relay_component,
            entry_point="clio-relay",
        )
        relay_identity.update(_relay_execution_runtime_identity(relay_component))
        identities["clio-relay"] = relay_identity
    if "clio-kit" in receipt.component_artifacts:
        from clio_relay.jarvis_mcp import jarvis_mcp_runtime_identity

        component = receipt.component_artifacts["clio-kit"]
        runtime_identity = jarvis_mcp_runtime_identity(receipt)
        expected_capability = component.native_execution
        if runtime_identity.get("artifact_identity_verified") is not True:
            observed_capability = None
            runtime_identity["native_execution_error"] = str(
                runtime_identity.get("error")
                or "receipt-bound clio-kit runtime identity did not verify"
            )
        else:
            try:
                observed_capability = probe_clio_kit_native_execution_contract(
                    component.runtime_command
                )
            except ConfigurationError as exc:
                observed_capability = None
                runtime_identity["native_execution_error"] = str(exc)
        runtime_identity.update(
            {
                "native_execution_capability": (
                    observed_capability.model_dump(mode="json")
                    if observed_capability is not None
                    else None
                ),
                "native_execution_capability_verified": (
                    expected_capability is not None
                    and observed_capability == expected_capability
                    and _native_capability_matches_component(
                        expected_capability,
                        component_name="clio-kit",
                    )
                ),
            }
        )
        identities["clio-kit"] = runtime_identity
    for component_name, component in receipt.component_artifacts.items():
        if component.native_execution is not None and component_name != "clio-kit":
            identities[component_name] = _native_jarvis_component_runtime_identity(component)
    return identities


def persistent_component_runtime_identity(
    component: ComponentArtifactIdentity,
    *,
    entry_point: str,
) -> dict[str, object]:
    """Re-probe one receipt-bound persistent uv tool without mutating it."""
    expected = component.persistent_tool
    if expected is None:
        return {
            "persistent_tool_verified": False,
            "error": "component receipt omitted persistent uv tool identity",
        }
    uv_executable = component.runtime_executables.get("uv")
    tool_executable = component.runtime_executables.get(entry_point)
    provider_interpreter = component.runtime_interpreters.get("provider")
    artifact_path = component.runtime_artifact_path
    if (
        uv_executable is None
        or tool_executable is None
        or provider_interpreter is None
        or artifact_path is None
        or component.distribution_version is None
    ):
        return {
            "persistent_tool_verified": False,
            "error": "component receipt omitted persistent uv tool runtime fields",
        }
    try:
        observed = probe_persistent_uv_tool_identity(
            uv_executable=uv_executable,
            tool_executable=tool_executable,
            provider_interpreter=provider_interpreter,
            source_artifact=Path(artifact_path),
            distribution=component.distribution,
            distribution_version=component.distribution_version,
            entry_point=entry_point,
            tool_directory=expected.tool_directory,
            tool_bin_directory=expected.tool_bin_directory,
        )
    except ConfigurationError as exc:
        return {"persistent_tool_verified": False, "error": str(exc)}
    return {
        "persistent_tool_verified": observed == expected,
        "expected": expected.model_dump(mode="json"),
        "observed": observed.model_dump(mode="json"),
        "error": None if observed == expected else "persistent uv tool identity changed",
    }


def _relay_execution_runtime_identity(
    component: ComponentArtifactIdentity,
) -> dict[str, object]:
    """Verify relay package bytes and imports in the JARVIS execution interpreter."""
    execution_python = component.runtime_interpreters.get("execution")
    runtime_path: Path | None = None
    try:
        if component.runtime_artifact_path is not None:
            runtime_path = Path(component.runtime_artifact_path).expanduser().resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        runtime_path = None
    artifact_sha256_verified = (
        runtime_path is not None
        and runtime_path.is_file()
        and component.artifact_filename == runtime_path.name
        and component.artifact_sha256 is not None
        and sha256_file(runtime_path) == component.artifact_sha256
    )
    execution = _probe_python_distribution(execution_python, component.distribution)
    execution_distribution_identity_verified = (
        execution.get("distribution") is not None
        and _normalized_distribution_name(str(execution["distribution"]))
        == _normalized_distribution_name(component.distribution)
        and execution.get("distribution_version") == component.distribution_version
    )
    execution_source_verified = runtime_path is not None and _direct_url_source_matches(
        execution.get("direct_url"),
        expected_artifact=runtime_path,
    )
    try:
        execution_interpreter_verified = (
            execution_python is not None
            and execution.get("executable") is not None
            and Path(str(execution["executable"])).resolve(strict=True)
            == Path(execution_python).expanduser().resolve(strict=True)
            and execution_distribution_identity_verified
            and execution_source_verified
        )
    except (OSError, RuntimeError, ValueError):
        execution_interpreter_verified = False
    execution_record_closure = (
        _probe_python_distribution_record_closure(
            execution_python,
            component.distribution,
            runtime_path,
        )
        if artifact_sha256_verified
        else {
            "verified": False,
            "error": "retained relay wheel artifact identity is not verified",
            "tree_scanned": False,
            "tree_copied": False,
        }
    )
    import_probe = _probe_relay_execution_imports(
        execution_python,
        expected_version=component.distribution_version,
    )
    execution_runtime_verified = (
        artifact_sha256_verified
        and execution_interpreter_verified
        and execution_record_closure.get("verified") is True
        and import_probe.get("verified") is True
    )
    return {
        "execution_runtime_verified": execution_runtime_verified,
        "execution_interpreter": execution_python,
        "execution_interpreter_verified": execution_interpreter_verified,
        "execution_distribution_identity_verified": (execution_distribution_identity_verified),
        "execution_source_verified": execution_source_verified,
        "execution_artifact_sha256_verified": artifact_sha256_verified,
        "execution_record_closure": execution_record_closure,
        "execution_record_closure_verified": (execution_record_closure.get("verified") is True),
        "execution_imports": import_probe,
        "execution_imports_verified": import_probe.get("verified") is True,
    }


def _probe_relay_execution_imports(
    python: str | None,
    *,
    expected_version: str | None,
) -> dict[str, object]:
    """Import the exact relay packages JARVIS executes in one isolated interpreter."""
    if python is None or expected_version is None:
        return {
            "verified": False,
            "error": "relay execution interpreter or version is not configured",
        }
    script = """
import json
import sys
from importlib.metadata import version

import clio_relay
import clio_relay.bounded_command.pkg
import clio_relay.mcp_call.pkg
import clio_relay.remote_agent.pkg
import jarvis_cd

observed = version("clio-relay")
print(json.dumps({
    "executable": sys.executable,
    "distribution_version": observed,
    "imports": [
        "clio_relay",
        "clio_relay.bounded_command.pkg",
        "clio_relay.mcp_call.pkg",
        "clio_relay.remote_agent.pkg",
        "jarvis_cd",
    ],
}, sort_keys=True))
"""
    try:
        completed = run_bounded_process(
            [python, "-I", "-c", script],
            timeout_seconds=10,
            stdout_maximum_bytes=64 * 1024,
            stderr_maximum_bytes=16 * 1024,
        )
    except (OSError, BoundedProcessError) as exc:
        return {"verified": False, "error": f"{type(exc).__name__}: {exc}"}
    if completed.returncode != 0:
        return {
            "verified": False,
            "error": completed.stderr.strip() or completed.stdout.strip(),
        }
    try:
        loaded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        return {"verified": False, "error": f"invalid relay import probe JSON: {exc}"}
    if not isinstance(loaded, dict):
        return {"verified": False, "error": "relay import probe was not an object"}
    result = {str(key): value for key, value in cast(dict[object, object], loaded).items()}
    expected_imports = [
        "clio_relay",
        "clio_relay.bounded_command.pkg",
        "clio_relay.mcp_call.pkg",
        "clio_relay.remote_agent.pkg",
        "jarvis_cd",
    ]
    result["verified"] = (
        result.get("distribution_version") == expected_version
        and result.get("imports") == expected_imports
    )
    if result["verified"] is not True:
        result["error"] = "relay execution imports did not match the receipt"
    else:
        result["error"] = None
    return result


def _native_jarvis_component_runtime_identity(
    component: ComponentArtifactIdentity,
) -> dict[str, object]:
    """Verify native JARVIS API visibility in its execution interpreter."""
    try:
        provider_distribution = metadata.distribution(component.distribution)
    except metadata.PackageNotFoundError:
        provider_distribution = None
    installed_entry_points = (
        _distribution_progress_entry_points(provider_distribution)
        if provider_distribution is not None
        else []
    )
    expected_entry_points = sorted(component.entry_points)
    provider_direct_url = (
        _distribution_direct_url(provider_distribution) if provider_distribution is not None else {}
    )
    runtime_path = (
        Path(component.runtime_artifact_path).expanduser().resolve()
        if component.runtime_artifact_path is not None
        else None
    )
    artifact_sha256_verified = (
        runtime_path is not None
        and runtime_path.is_file()
        and component.artifact_sha256 is not None
        and sha256_file(runtime_path) == component.artifact_sha256
    )
    provider_distribution_identity_verified = (
        provider_distribution is not None
        and _normalized_distribution_name(provider_distribution.name)
        == _normalized_distribution_name(component.distribution)
        and provider_distribution.version == component.distribution_version
    )
    entry_points_visible = set(expected_entry_points).issubset(installed_entry_points)
    provider_python = component.runtime_interpreters.get("provider")
    provider_source_verified = runtime_path is not None and _direct_url_source_matches(
        provider_direct_url,
        expected_artifact=runtime_path,
    )
    provider_interpreter_verified = (
        provider_python is not None
        and Path(provider_python).expanduser().resolve() == Path(sys.executable).resolve()
        and provider_distribution_identity_verified
        and provider_source_verified
    )
    execution_python = component.runtime_interpreters.get("execution")
    execution = _probe_python_distribution(execution_python, component.distribution)
    execution_record_closure = (
        _probe_python_distribution_record_closure(
            execution_python,
            component.distribution,
            runtime_path,
        )
        if artifact_sha256_verified
        else {
            "verified": False,
            "error": "retained wheel artifact identity is not verified",
            "tree_scanned": False,
            "tree_copied": False,
        }
    )
    execution_distribution_identity_verified = (
        execution.get("distribution") is not None
        and _normalized_distribution_name(str(execution["distribution"]))
        == _normalized_distribution_name(component.distribution)
        and execution.get("distribution_version") == component.distribution_version
    )
    execution_entry_points = execution.get("entry_points")
    execution_entry_points_visible = isinstance(execution_entry_points, list) and set(
        expected_entry_points
    ).issubset({str(value) for value in cast(list[object], execution_entry_points)})
    execution_source_verified = runtime_path is not None and _direct_url_source_matches(
        execution.get("direct_url"),
        expected_artifact=runtime_path,
    )
    runtime_artifact_path_verified = (
        runtime_path is not None
        and runtime_path.is_file()
        and component.artifact_filename == runtime_path.name
        and execution_source_verified
    )
    distribution_identity_verified = execution_distribution_identity_verified
    execution_interpreter_verified = (
        execution_python is not None
        and execution.get("executable") is not None
        and Path(str(execution["executable"])).resolve()
        == Path(execution_python).expanduser().resolve()
        and execution_distribution_identity_verified
        and execution_source_verified
    )
    expected_native_capability = component.native_execution
    execution_native_capability: NativeJarvisExecutionCapability | None = None
    execution_native_error: str | None = None
    try:
        execution_native_capability = probe_jarvis_native_execution_capability(execution_python)
    except ConfigurationError as exc:
        execution_native_error = str(exc)
    execution_native_execution_capability_verified = (
        expected_native_capability is not None
        and execution_native_capability == expected_native_capability
    )
    native_execution_capability_verified = (
        expected_native_capability is not None
        and _native_capability_matches_component(
            expected_native_capability,
            component_name="jarvis-cd",
        )
        and execution_native_execution_capability_verified
    )
    jarvis_executable = component.runtime_executables.get("jarvis")
    jarvis_executable_verified = _jarvis_executable_matches_interpreter(
        jarvis_executable,
        execution_python,
        runtime_command=component.runtime_command,
    )
    verified = (
        distribution_identity_verified
        and runtime_artifact_path_verified
        and artifact_sha256_verified
        and execution_interpreter_verified
        and execution_record_closure.get("verified") is True
        and native_execution_capability_verified
        and jarvis_executable_verified
    )
    return {
        "verified": verified,
        "distribution": execution.get("distribution"),
        "distribution_version": execution.get("distribution_version"),
        "distribution_identity_verified": distribution_identity_verified,
        "entry_points": installed_entry_points,
        "entry_points_visible": entry_points_visible,
        "compatibility_entry_points_declared": bool(expected_entry_points),
        "compatibility_entry_points_visible": entry_points_visible,
        "runtime_artifact_path": str(runtime_path) if runtime_path is not None else None,
        "runtime_artifact_path_verified": runtime_artifact_path_verified,
        "artifact_sha256": (
            sha256_file(runtime_path)
            if runtime_path is not None and runtime_path.is_file()
            else None
        ),
        "artifact_sha256_verified": artifact_sha256_verified,
        "provider_interpreter": provider_python,
        "provider_interpreter_verified": provider_interpreter_verified,
        "provider_distribution_identity_verified": provider_distribution_identity_verified,
        "execution_interpreter": execution_python,
        "execution_interpreter_verified": execution_interpreter_verified,
        "execution_distribution_identity_verified": execution_distribution_identity_verified,
        "execution_entry_points_visible": execution_entry_points_visible,
        "execution_source_verified": execution_source_verified,
        "execution": execution,
        "execution_record_closure": execution_record_closure,
        "execution_record_closure_verified": (execution_record_closure.get("verified") is True),
        "native_execution_capability": (
            expected_native_capability.model_dump(mode="json")
            if expected_native_capability is not None
            else None
        ),
        "provider_native_execution_capability": None,
        "provider_native_execution_capability_verified": None,
        "provider_native_execution_error": "not required by the native execution boundary",
        "execution_native_execution_capability": (
            execution_native_capability.model_dump(mode="json")
            if execution_native_capability is not None
            else None
        ),
        "execution_native_execution_capability_verified": (
            execution_native_execution_capability_verified
        ),
        "execution_native_execution_error": execution_native_error,
        "native_execution_capability_verified": native_execution_capability_verified,
        "jarvis_executable": jarvis_executable,
        "jarvis_executable_verified": jarvis_executable_verified,
    }
