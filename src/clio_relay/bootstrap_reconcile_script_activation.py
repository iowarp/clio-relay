"""Rendered-script fragment: the new generation's receipt build and its activation-link
swap.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations


def reconcile_script_activation(
    *,
    worker_fence: str,
    worker_recheck: str,
    init_command: str,
    worker_restart: str,
    rendered_core_dir: str,
    rendered_spool_dir: str,
    rendered_agent_adapter: str,
    rendered_agent_args: str,
    rendered_relay_install_spec: str,
    rendered_relay_artifact_sha256: str,
    rendered_jarvis_mcp_install_spec: str,
    rendered_jarvis_mcp_artifact_sha256: str,
    rendered_source_archive: str,
    rendered_source_archive_sha256: str,
    invocation_id: str,
    candidate_uv_install_program: str,
    staged_provider_exec_program: str,
    staged_provider_environment_sanitizer: str,
) -> str:
    """Render: the new generation's receipt build and its activation-link swap."""
    return f"""    export BOOTSTRAP_GENERATION RELAY_INSTALL_SPEC RELAY_ARTIFACT_PATH
    export RELAY_ARTIFACT_SHA256 RELAY_EXECUTABLE RELAY_PROVIDER_PYTHON
    export JARVIS_CD_WHEEL CLIO_KIT_EXECUTABLE ACTIVE_JARVIS_PYTHON
    export BOOTSTRAP_RELAY_DOWNLOAD_COUNT BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT
    export BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT JARVIS_MCP_ARTIFACT_PATH
    export JARVIS_MCP_INSTALL_SPEC JARVIS_MCP_ARTIFACT_SHA256
    export CLIO_KIT_PROVIDER_PYTHON
    "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_GENERATION_RECEIPT__'
import json
import os
import sys
from importlib.metadata import distribution
from pathlib import Path

from clio_relay.bootstrap_reconcile import BootstrapDesiredState
from clio_relay.contract_gate import evaluate_degradation, probe_surface_contract_identity
from clio_relay.installation import (
    CLIO_KIT_MCP_CONTRACT_SCHEMA,
    ComponentArtifactIdentity,
    load_install_receipt,
    probe_clio_kit_native_execution_contract,
    probe_jarvis_native_execution_capability,
    probe_persistent_uv_tool_identity,
    write_install_receipt,
)
from clio_relay.jarvis_mcp import jarvis_mcp_server_artifact_verified
from clio_relay.mcp_call.runner import mcp_server_artifact_identity
from clio_relay.remote_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_ID,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID,
    CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID,
)
from clio_relay.validation_report import sha256_file

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
generation = Path(os.environ["BOOTSTRAP_GENERATION"])
old = load_install_receipt(Path.home() / ".local/share/clio-relay/install-receipt.json")
relay_artifact_text = os.environ["RELAY_ARTIFACT_PATH"]
relay_artifact = Path(relay_artifact_text).resolve() if relay_artifact_text else None
relay_distribution = distribution("clio-relay")
relay_persistent = None
if relay_artifact is not None:
    relay_persistent = probe_persistent_uv_tool_identity(
        uv_executable=str(Path.home() / ".local/bin/uv"),
        tool_executable=os.environ["RELAY_EXECUTABLE"],
        provider_interpreter=os.environ["RELAY_PROVIDER_PYTHON"],
        source_artifact=relay_artifact,
        distribution="clio-relay",
        distribution_version=relay_distribution.version,
        entry_point="clio-relay",
        tool_directory=str(generation / "tools"),
        tool_bin_directory=str(generation / "bin"),
    )
relay_component = ComponentArtifactIdentity(
    distribution=relay_distribution.name,
    distribution_version=relay_distribution.version,
    install_spec=os.environ["RELAY_INSTALL_SPEC"],
    requested_source=(
        "pypi"
        if os.environ["RELAY_INSTALL_SPEC"].startswith("clio-relay==")
        else ("wheel" if relay_artifact is not None else "checkout")
    ),
    artifact_filename=(relay_artifact.name if relay_artifact is not None else None),
    artifact_sha256=(sha256_file(relay_artifact) if relay_artifact is not None else None),
    runtime_artifact_path=(str(relay_artifact) if relay_artifact is not None else None),
    runtime_command=[os.environ["RELAY_EXECUTABLE"], "installation-info"],
    runtime_interpreters={{
        "provider": os.environ["RELAY_PROVIDER_PYTHON"],
        "execution": os.environ["ACTIVE_JARVIS_PYTHON"],
    }},
    runtime_executables={{
        "clio-relay": os.environ["RELAY_EXECUTABLE"],
        "uv": str(Path.home() / ".local/bin/uv"),
    }},
    persistent_tool=relay_persistent,
)
components = dict(old.components)
components["clio-relay"] = relay_distribution.version
component_artifacts = {{
    **old.component_artifacts,
    "clio-relay": relay_component,
}}
contract_surfaces = dict(old.contract_surfaces)
contract_degradations = list(old.contract_degradations)
if os.environ["BOOTSTRAP_PLAN_MODE"] == "component-upgrade":
    clio_kit_wheel = Path(os.environ["JARVIS_MCP_ARTIFACT_PATH"]).resolve(strict=True)
    clio_kit_sha256 = os.environ["JARVIS_MCP_ARTIFACT_SHA256"]
    if sha256_file(clio_kit_wheel) != clio_kit_sha256:
        raise SystemExit("staged clio-kit wheel digest changed before receipt creation")
    clio_kit_executable = os.environ["CLIO_KIT_EXECUTABLE"]
    clio_kit_provider = os.environ["CLIO_KIT_PROVIDER_PYTHON"]
    clio_kit_command = [clio_kit_executable, "mcp-server", "jarvis"]
    # clio-relay#242: bootstrap-time enumeration is INTEGRITY-only and
    # per-surface -- it never fails the whole cluster bootstrap just because
    # jarvis shipped behind this relay's pin. probe_surface_contract_identity
    # negotiates down to whichever known contract id clio-kit actually
    # answers to and records it on the receipt regardless of outcome; the
    # strict, deep-shape probe below only runs (and can only succeed) when
    # the shipped id already meets the current pin.
    jarvis_surface = probe_surface_contract_identity(
        [clio_kit_executable],
        surface="jarvis",
        candidate_contract_ids=(
            CLIO_KIT_JARVIS_USER_CONTRACT_ID,
            CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID,
        ),
        contract_schema_version=CLIO_KIT_MCP_CONTRACT_SCHEMA,
        sha256_by_id=CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID,
    )
    contract_surfaces["jarvis"] = jarvis_surface
    clio_kit_native = (
        probe_clio_kit_native_execution_contract(clio_kit_command)
        if jarvis_surface.meets_requirement
        else None
    )
    jarvis_degradation = evaluate_degradation(
        jarvis_surface, tracking_issue="iowarp/clio-relay#242"
    )
    if jarvis_degradation is not None:
        contract_degradations.append(jarvis_degradation)
        print(
            "WARNING: contract_surface_degraded surface=jarvis have="
            + jarvis_degradation.have + " need=" + jarvis_degradation.need
            + " issue=" + jarvis_degradation.tracking_issue,
            file=sys.stderr,
        )
    clio_kit_persistent = probe_persistent_uv_tool_identity(
        uv_executable=str(Path.home() / ".local/bin/uv"),
        tool_executable=clio_kit_executable,
        provider_interpreter=clio_kit_provider,
        source_artifact=clio_kit_wheel,
        distribution="clio-kit",
        distribution_version=desired.clio_kit_version,
        entry_point="clio-kit",
        tool_directory=str(generation / "tools"),
        tool_bin_directory=str(generation / "bin"),
    )
    clio_kit_server = mcp_server_artifact_identity(
        clio_kit_executable,
        ["mcp-server", "jarvis"],
        verify_relay_jarvis_cd_lock=True,
    )
    if not jarvis_mcp_server_artifact_verified(clio_kit_server):
        raise SystemExit("staged clio-kit server artifact did not verify its JARVIS lock")
    component_artifacts["clio-kit"] = ComponentArtifactIdentity(
        distribution="clio-kit",
        distribution_version=desired.clio_kit_version,
        install_spec=os.environ["JARVIS_MCP_INSTALL_SPEC"],
        requested_source="github_release",
        artifact_filename=clio_kit_wheel.name,
        artifact_sha256=clio_kit_sha256,
        runtime_artifact_path=str(clio_kit_wheel),
        runtime_command=clio_kit_command,
        runtime_interpreters={{"provider": clio_kit_provider}},
        runtime_executables={{
            "clio-kit": clio_kit_executable,
            "uv": str(Path.home() / ".local/bin/uv"),
        }},
        native_execution=clio_kit_native,
        persistent_tool=clio_kit_persistent,
        locked_server_runtime=clio_kit_server["nested_runtime"],
    )

    jarvis_wheel = Path(os.environ["JARVIS_CD_WHEEL"]).resolve(strict=True)
    if sha256_file(jarvis_wheel) != desired.jarvis_cd_wheel_sha256:
        raise SystemExit("staged JARVIS-CD wheel digest changed before receipt creation")
    jarvis_python = os.environ["ACTIVE_JARVIS_PYTHON"]
    jarvis_executable = str(Path(jarvis_python).parent / "jarvis")
    component_artifacts["jarvis-cd"] = ComponentArtifactIdentity(
        distribution="jarvis-cd",
        distribution_version=desired.jarvis_cd_version,
        install_spec=desired.jarvis_cd_wheel_url,
        requested_source="github_release",
        artifact_filename=jarvis_wheel.name,
        artifact_sha256=desired.jarvis_cd_wheel_sha256,
        runtime_artifact_path=str(jarvis_wheel),
        runtime_command=[jarvis_executable, "--help"],
        runtime_interpreters={{
            "provider": os.environ["RELAY_PROVIDER_PYTHON"],
            "execution": jarvis_python,
        }},
        runtime_executables={{"jarvis": jarvis_executable}},
        native_execution=probe_jarvis_native_execution_capability(jarvis_python),
    )
    components["clio-kit"] = desired.clio_kit_version
    components["jarvis-cd"] = desired.jarvis_cd_version
write_install_receipt(
    install_spec=desired.relay_install_spec,
    artifact_path=relay_artifact,
    path=generation / "install-receipt.json",
    components=components,
    component_artifacts=component_artifacts,
    contract_surfaces=contract_surfaces,
    contract_degradations=contract_degradations,
    deployment_fingerprint=desired.fingerprint,
    deployment_manifest=desired.model_dump(mode="json"),
    generation=desired.fingerprint,
)
__CLIO_RELAY_GENERATION_RECEIPT__
    CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json" \
      "$RELAY_EXECUTABLE" installation-info >"$BOOTSTRAP_GENERATION/installation-info.json"
    export CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json"
    "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_VERIFY_GENERATION__'
import json
import os
import sys
from pathlib import Path

info = json.loads(Path(os.environ["BOOTSTRAP_GENERATION"] + "/installation-info.json").read_text())
runtime = info.get("component_runtime", {{}})
jarvis_runtime = runtime.get("jarvis-cd", {{}})
# clio-relay#242: a RECORDED, below-pin jarvis surface is not a runtime
# verification failure -- bootstrap already proved and logged the gap via
# probe_surface_contract_identity when the receipt was minted. Only skip the
# strict native-execution self-consistency check for that known, typed case;
# every other reason this could be unverified still fails the generation.
receipt_payload = info.get("receipt", {{}})
jarvis_surface = receipt_payload.get("contract_surfaces", {{}}).get("jarvis")
jarvis_surface_degraded = (
    isinstance(jarvis_surface, dict) and jarvis_surface.get("meets_requirement") is False
)
if jarvis_surface_degraded:
    print(
        "WARNING: contract_surface_degraded surface=jarvis have="
        + str(jarvis_surface.get("shipped_contract_id"))
        + " need=" + str(jarvis_surface.get("required_contract_id"))
        + " -- jarvis submission refuses at use-time until clio-kit ships the pin "
        "(iowarp/clio-relay#242)",
        file=sys.stderr,
    )
checks = {{
    "receipt_matches_install": info.get("receipt_matches_install") is True,
    "clio-relay.persistent_tool_verified": (
        runtime.get("clio-relay", {{}}).get("persistent_tool_verified") is True
    ),
    "clio-relay.execution_runtime_verified": (
        runtime.get("clio-relay", {{}}).get("execution_runtime_verified") is True
    ),
    "clio-kit.persistent_tool_verified": (
        runtime.get("clio-kit", {{}}).get("persistent_tool_verified") is True
    ),
    "clio-kit.native_execution_capability_verified": (
        jarvis_surface_degraded
        or runtime.get("clio-kit", {{}}).get("native_execution_capability_verified") is True
    ),
    "jarvis-cd.verified": jarvis_runtime.get("verified") is True,
}}
if checks["jarvis-cd.verified"] is False:
    record_closure = jarvis_runtime.get("execution_record_closure", {{}})
    record_closure_error_code = (
        record_closure.get("error_code") if isinstance(record_closure, dict) else None
    )
    checks.update({{
        "jarvis-cd.distribution_identity_verified": (
            jarvis_runtime.get("distribution_identity_verified") is True
        ),
        "jarvis-cd.runtime_artifact_path_verified": (
            jarvis_runtime.get("runtime_artifact_path_verified") is True
        ),
        "jarvis-cd.artifact_sha256_verified": (
            jarvis_runtime.get("artifact_sha256_verified") is True
        ),
        "jarvis-cd.execution_interpreter_verified": (
            jarvis_runtime.get("execution_interpreter_verified") is True
        ),
        "jarvis-cd.execution_record_closure_verified": (
            jarvis_runtime.get("execution_record_closure_verified") is True
        ),
        "jarvis-cd.native_execution_capability_verified": (
            jarvis_runtime.get("native_execution_capability_verified") is True
        ),
        "jarvis-cd.jarvis_executable_verified": (
            jarvis_runtime.get("jarvis_executable_verified") is True
        ),
    }})
    if (
        isinstance(record_closure_error_code, str)
        and 0 < len(record_closure_error_code) <= 96
        and all(
            character.isascii() and (character.isalnum() or character == "-")
            for character in record_closure_error_code
        )
    ):
        checks[
            "jarvis-cd.execution_record_closure_error." + record_closure_error_code
        ] = False
failed_checks = sorted(name for name, verified in checks.items() if not verified)
if failed_checks:
    raise SystemExit(
        "prepared relay generation runtime identity did not verify: "
        + ", ".join(failed_checks)
    )
__CLIO_RELAY_VERIFY_GENERATION__
    unset CLIO_RELAY_INSTALL_RECEIPT
    BOOTSTRAP_LEGACY_IDENTITY_AFTER="$(
      bootstrap_candidate_action execution-boundary \
        "$LEGACY_JARVIS_VENV" "$LEGACY_JARVIS_PYTHON" "$LEGACY_JARVIS_EXECUTABLE"
    )"
    if [ "$BOOTSTRAP_LEGACY_IDENTITY_AFTER" != "$BOOTSTRAP_LEGACY_IDENTITY" ]; then
      echo "legacy JARVIS execution environment changed during preparation" >&2
      return 1
    fi
    "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_GENERATION_MANIFEST__'
import json
import os
from pathlib import Path

from clio_relay.validation_report import sha256_file

generation = Path(os.environ["BOOTSTRAP_GENERATION"])
manifest = {{
    "schema_version": "clio-relay.bootstrap-generation.v1",
    "fingerprint": os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
    "plan": json.loads(os.environ["BOOTSTRAP_PLAN_JSON"]),
    "legacy_execution_identity": json.loads(os.environ["BOOTSTRAP_LEGACY_IDENTITY"]),
    "active_execution_identity": json.loads(os.environ["BOOTSTRAP_ACTIVE_IDENTITY"]),
    "jarvis_wrapper_sha256": sha256_file(generation / "bin/jarvis"),
    "install_receipt": str(generation / "install-receipt.json"),
    "install_receipt_sha256": sha256_file(generation / "install-receipt.json"),
}}
path = generation / "manifest.json"
temporary = generation / ".manifest.tmp"
with temporary.open("x", encoding="utf-8", newline="\\n") as stream:
    stream.write(json.dumps(manifest, indent=2, sort_keys=True) + "\\n")
    stream.flush()
    os.fsync(stream.fileno())
os.replace(temporary, path)
descriptor = os.open(generation, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
prepared = generation / ".prepared"
prepared_temporary = generation / ".prepared.tmp"
with prepared_temporary.open("x", encoding="ascii", newline="\\n") as stream:
    stream.write(manifest["fingerprint"] + "\\n")
    stream.flush()
    os.fsync(stream.fileno())
os.replace(prepared_temporary, prepared)
descriptor = os.open(generation, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
__CLIO_RELAY_GENERATION_MANIFEST__
  fi
  export BOOTSTRAP_RELAY_DOWNLOAD_COUNT BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT
  export BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT
  export BOOTSTRAP_GENERATION LEGACY_JARVIS_VENV
  RELAY_EXECUTABLE="$BOOTSTRAP_GENERATION/bin/clio-relay"
  if [ ! -L "$RELAY_EXECUTABLE" ]; then
    echo "prepared generation relay launcher is not a symbolic link" >&2
    return 1
  fi
  RELAY_PROVIDER_PYTHON="$BOOTSTRAP_GENERATION/tools/clio-relay/bin/python"
  if [ ! -x "$RELAY_PROVIDER_PYTHON" ]; then
    echo "prepared generation provider is unavailable" >&2
    return 1
  fi
  BOOTSTRAP_LEGACY_IDENTITY_AFTER="$(
    bootstrap_candidate_action execution-boundary \
      "$LEGACY_JARVIS_VENV" "$LEGACY_JARVIS_PYTHON" "$LEGACY_JARVIS_EXECUTABLE"
  )"
  if [ "$BOOTSTRAP_LEGACY_IDENTITY_AFTER" != "$BOOTSTRAP_LEGACY_IDENTITY" ]; then
    echo "legacy JARVIS execution environment changed before activation" >&2
    return 1
  fi
  BOOTSTRAP_PREPARED_INSPECTION="$(
    CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json" \
      "$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_VERIFY_PREPARED_GENERATION__'
import json
import os
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    inspect_prepared_generation,
)

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
inspection = inspect_prepared_generation(
    desired,
    generation=Path(os.environ["BOOTSTRAP_GENERATION"]),
    legacy_execution_identity=json.loads(os.environ["BOOTSTRAP_LEGACY_IDENTITY"]),
)
print(json.dumps(inspection, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_VERIFY_PREPARED_GENERATION__
  )"
  export BOOTSTRAP_PREPARED_INSPECTION
  BOOTSTRAP_PREPARED_MANIFEST_SHA256="$(
    python3 - <<'__CLIO_RELAY_PREPARED_MANIFEST_SHA256__'
import json
import os

print(json.loads(os.environ["BOOTSTRAP_PREPARED_INSPECTION"])["manifest_sha256"])
__CLIO_RELAY_PREPARED_MANIFEST_SHA256__
  )"
  case "$BOOTSTRAP_PREPARED_MANIFEST_SHA256" in
    (*[!0-9a-f]*|'') echo "prepared manifest identity is invalid" >&2; return 1 ;;
  esac
  if [ "${{#BOOTSTRAP_PREPARED_MANIFEST_SHA256}}" -ne 64 ]; then
    echo "prepared manifest identity has an invalid length" >&2
    return 1
  fi
  (
    BOOTSTRAP_STAGED_GENERATION="$BOOTSTRAP_GENERATION"
    BOOTSTRAP_STAGED_MANIFEST_SHA256="$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
    export BOOTSTRAP_STAGED_GENERATION BOOTSTRAP_STAGED_MANIFEST_SHA256
    bootstrap_provider_exec -c \
      'import clio_relay,jarvis_cd; print("staged_provider=sealed_memfd")'
  ) >/dev/null
  bootstrap_candidate_action journal-phase prepared_manifest \
    "$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
  bootstrap_candidate_action exchange-preflight \
    "$HOME/.local/share/clio-relay" \
    "$HOME/.local/bin" \
    "$(dirname "$JARVIS_REPOS_FILE")" >/dev/null
  BOOTSTRAP_PREPARE_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_PREPARE_STARTED_NS BOOTSTRAP_PREPARE_COMPLETED_NS
  bootstrap_candidate_action journal-advance prepared
  bootstrap_candidate_action journal-advance fencing

{worker_fence}

  if [ "$BOOTSTRAP_SERVICE_ACTIVE_BEFORE" = "1" ] && [ "$WORKER_WAS_ACTIVE" != "1" ]; then
    echo "endpoint service activity changed before fencing" >&2
    return 1
  fi
  if [ "$BOOTSTRAP_SERVICE_ACTIVE_BEFORE" = "0" ] && [ "$WORKER_WAS_ACTIVE" != "0" ]; then
    echo "endpoint service activity changed before fencing" >&2
    return 1
  fi
  bootstrap_candidate_action journal-advance fenced
  trap bootstrap_reconcile_transaction_exit EXIT
  echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
    sha256sum --check --strict -
  echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
    sha256sum --check --strict -
  bootstrap_candidate_action journal-advance activating

  bootstrap_use_staged_provider \
    "$BOOTSTRAP_GENERATION" "$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
  BOOTSTRAP_STAGED_ACTIVATION="$(
    bootstrap_candidate_action finish-activation \
      "$BOOTSTRAP_GENERATION" "$BOOTSTRAP_PREPARED_MANIFEST_SHA256"
  )"
  export BOOTSTRAP_STAGED_ACTIVATION
  BOOTSTRAP_JARVIS_REPO_RECONCILIATION="$(
    python3 - <<'__CLIO_RELAY_STAGED_REPOSITORY_EVIDENCE__'
import json
import os

activation = json.loads(os.environ["BOOTSTRAP_STAGED_ACTIVATION"])
print(json.dumps(activation["jarvis_repository"], sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_STAGED_REPOSITORY_EVIDENCE__
  )"
  export BOOTSTRAP_JARVIS_REPO_RECONCILIATION
  bootstrap_verify_stable_activation_links
  "$HOME/.local/share/clio-relay/current/bin/clio-relay" installation-info >/dev/null
  "$HOME/.local/share/clio-relay/current/bin/clio-relay" --help >/dev/null
  bootstrap_candidate_action journal-advance activated
  echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
    sha256sum --check --strict -
  echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
    sha256sum --check --strict -

"""
