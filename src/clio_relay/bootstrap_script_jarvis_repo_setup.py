"""Rendered-script fragment: JARVIS venv/repo staging ahead of the candidate reconcile
transaction.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations


def script_jarvis_repo_setup(
    *,
    artifact_fetch_function: str,
    candidate_bounded_process_sha256: str,
    candidate_errors_sha256: str,
    candidate_process_containment_sha256: str,
    candidate_provider_build_info_sha256: str,
    candidate_reconcile_sha256: str,
    candidate_safe_archive_sha256: str,
    candidate_uv_install_program: str,
    cluster: str | None,
    frp_version: str,
    init_command: str,
    invocation_id: str,
    ownership_proof_adoption_python: str,
    pinned_uv_copy_program: str,
    preparing_root_program: str,
    relay_only_reconcile: str,
    rendered_agent_adapter: str,
    rendered_agent_args: str,
    rendered_agent_npm_bin: str,
    rendered_agent_npm_package: str,
    rendered_allow_jarvis_resource_graph_build: str,
    rendered_bootstrap_journal_source: str,
    rendered_candidate_package_sources: str,
    rendered_candidate_relay_install_spec: str,
    rendered_core_dir: str,
    rendered_desired_state: str,
    rendered_jarvis_mcp_artifact_sha256: str,
    rendered_jarvis_mcp_install_spec: str,
    rendered_jarvis_resource_graph_profile: str,
    rendered_relay_artifact_sha256: str,
    rendered_relay_install_spec: str,
    rendered_source_archive: str,
    rendered_source_archive_sha256: str,
    rendered_spool_dir: str,
    shared_directory_mkdir_owned_helper: str,
    stable_activation_link_adoption: str,
    worker_fence: str,
    worker_recheck: str,
    worker_restart: str,
    worker_service: str,
) -> str:
    """Render: JARVIS venv/repo staging ahead of the candidate reconcile transaction."""
    return """from clio_relay.errors import ConfigurationError
from clio_relay.installation import verify_distribution_file_source

wheel = Path(sys.argv[1]).resolve()
expected_sha256 = sys.argv[2]
expected_version = sys.argv[3]
if hashlib.sha256(wheel.read_bytes()).hexdigest() != expected_sha256:
    raise SystemExit("JARVIS-CD release wheel digest changed after installation")
installed = distribution("jarvis_cd")
if installed.version != expected_version:
    raise SystemExit("JARVIS-CD installed version does not match the release pin")
try:
    verify_distribution_file_source(
        direct_url_text=installed.read_text("direct_url.json"),
        expected_artifact=wheel,
    )
except ConfigurationError as exc:
    raise SystemExit(
        f"JARVIS-CD was not installed from the verified release wheel: {exc}"
    ) from exc
print(f"jarvis_cd_distribution={installed.name}=={installed.version}")
__CLIO_RELAY_NATIVE_JARVIS_PROBE__
}
verify_jarvis_cd_distribution "$RELAY_PROVIDER_PYTHON"
verify_jarvis_cd_distribution "$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC="$RELAY_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_ARTIFACT="$RELAY_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE="$RELAY_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON="$RELAY_PROVIDER_PYTHON"
export CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE="$RELAY_UV_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT="$JARVIS_UTIL_COMMIT"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION="$JARVIS_CD_VERSION"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL="$JARVIS_CD_WHEEL_URL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256="$JARVIS_CD_WHEEL_SHA256"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON="$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE="$JARVIS_VENV/bin/jarvis"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC="$JARVIS_MCP_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT="$JARVIS_MCP_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT_SHA256="$JARVIS_MCP_ARTIFACT_SHA256"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE="$JARVIS_MCP_REQUESTED_SOURCE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION="$JARVIS_MCP_VERSION"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE="$JARVIS_MCP_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON="$JARVIS_MCP_PROVIDER_PYTHON"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE="$JARVIS_MCP_UV_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_DESIRED_STATE="$BOOTSTRAP_DESIRED_STATE"
export CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_TRANSACTION_ROOT/install-receipt.json"
"$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_INSTALL_RECEIPT__'
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
    probe_persistent_uv_tool_identity,
    probe_clio_kit_native_execution_contract,
    probe_jarvis_native_execution_capability,
    write_install_receipt,
)
from clio_relay.mcp_call.runner import mcp_server_artifact_identity
from clio_relay.remote_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_ID,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID,
    CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID,
)
from clio_relay.validation_report import sha256_file

artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_ARTIFACT"]
desired_payload = json.loads(os.environ["CLIO_RELAY_BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
relay_artifact = Path(artifact_value).resolve() if artifact_value else None
relay_distribution = distribution("clio-relay")
relay_persistent_tool = None
if relay_artifact is not None:
    relay_persistent_tool = probe_persistent_uv_tool_identity(
        uv_executable=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE"],
        tool_executable=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
        provider_interpreter=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON"],
        source_artifact=relay_artifact,
        distribution="clio-relay",
        distribution_version=relay_distribution.version,
        entry_point="clio-relay",
    )
component_artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT"]
component_artifact = Path(component_artifact_value).resolve() if component_artifact_value else None
component_artifact_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT_SHA256"]
if component_artifact is None or sha256_file(component_artifact) != component_artifact_sha256:
    raise SystemExit("clio-kit wheel digest changed after persistent-tool installation")
component_version = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION"] or None
component_spec = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC"]
jarvis_cd_wheel = Path(os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL"]).resolve()
jarvis_cd_wheel_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256"]
if sha256_file(jarvis_cd_wheel) != jarvis_cd_wheel_sha256:
    raise SystemExit("jarvis-cd receipt wheel digest does not match bootstrap pin")
jarvis_cd_distribution = distribution("jarvis_cd")
if jarvis_cd_distribution.version != os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"]:
    raise SystemExit("jarvis-cd receipt version does not match the released wheel pin")
runtime_command = [
    os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
    "mcp-server",
    "jarvis",
]
if not runtime_command:
    raise SystemExit("clio-kit native JARVIS contract requires a persistent uv tool")
# clio-relay#242: bootstrap-time enumeration is INTEGRITY-only and
# per-surface -- see the identical comment in the component-upgrade
# reconcile heredoc above. The strict, deep-shape probe only runs (and can
# only succeed) once the shipped id already meets the current pin.
jarvis_surface = probe_surface_contract_identity(
    [runtime_command[0]],
    surface="jarvis",
    candidate_contract_ids=(
        CLIO_KIT_JARVIS_USER_CONTRACT_ID,
        CLIO_KIT_JARVIS_USER_LEGACY_CONTRACT_ID,
    ),
    contract_schema_version=CLIO_KIT_MCP_CONTRACT_SCHEMA,
    sha256_by_id=CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID,
)
contract_surfaces = {"jarvis": jarvis_surface}
contract_degradations = []
clio_kit_native_execution = (
    probe_clio_kit_native_execution_contract(runtime_command)
    if jarvis_surface.meets_requirement
    else None
)
jarvis_degradation = evaluate_degradation(jarvis_surface, tracking_issue="iowarp/clio-relay#242")
if jarvis_degradation is not None:
    contract_degradations.append(jarvis_degradation)
    print(
        "WARNING: contract_surface_degraded surface=jarvis have="
        + jarvis_degradation.have + " need=" + jarvis_degradation.need
        + " issue=" + jarvis_degradation.tracking_issue,
        file=sys.stderr,
    )
persistent_clio_kit_tool = probe_persistent_uv_tool_identity(
    uv_executable=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE"],
    tool_executable=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
    provider_interpreter=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON"],
    source_artifact=component_artifact,
    distribution="clio-kit",
    distribution_version=component_version,
    entry_point="clio-kit",
)
clio_kit_server_artifact = mcp_server_artifact_identity(
    runtime_command[0],
    runtime_command[1:],
    verify_relay_jarvis_cd_lock=True,
)
locked_server_runtime = clio_kit_server_artifact.get("nested_runtime")
if not isinstance(locked_server_runtime, dict):
    raise SystemExit("clio-kit JARVIS runtime omitted locked-server evidence")
jarvis_cd_lock_binding = locked_server_runtime.get("jarvis_cd_lock_binding")
if not isinstance(jarvis_cd_lock_binding, dict):
    raise SystemExit("clio-kit JARVIS runtime omitted jarvis-cd lock binding")
expected_jarvis_cd_url = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL"]
expected_jarvis_cd_version = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"]
expected_jarvis_cd_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256"]
if not (
    clio_kit_server_artifact.get("verified") is True
    and locked_server_runtime.get("schema_version") == "clio-kit.locked-server.v4"
    and locked_server_runtime.get("server_name") == "jarvis"
    and locked_server_runtime.get("locked_runtime_verified") is True
    and jarvis_cd_lock_binding.get("schema_version")
    == "clio-relay.jarvis-cd-lock-binding.v1"
    and jarvis_cd_lock_binding.get("dependency") == "jarvis-cd"
    and jarvis_cd_lock_binding.get("verified") is True
    and jarvis_cd_lock_binding.get("error") is None
    and jarvis_cd_lock_binding.get("expected_version") == expected_jarvis_cd_version
    and jarvis_cd_lock_binding.get("expected_url") == expected_jarvis_cd_url
    and jarvis_cd_lock_binding.get("expected_sha256") == expected_jarvis_cd_sha256
    and jarvis_cd_lock_binding.get("observed_version") == expected_jarvis_cd_version
    and jarvis_cd_lock_binding.get("observed_source_url") == expected_jarvis_cd_url
    and jarvis_cd_lock_binding.get("observed_wheel_url") == expected_jarvis_cd_url
    and jarvis_cd_lock_binding.get("observed_wheel_sha256") == expected_jarvis_cd_sha256
    and jarvis_cd_lock_binding.get("jarvis_mcp_package_entry_count") == 1
    and jarvis_cd_lock_binding.get("resolved_dependency_entry_count") == 1
    and jarvis_cd_lock_binding.get("observed_resolved_dependency_entries")
    == [{"name": "jarvis-cd"}]
    and jarvis_cd_lock_binding.get("metadata_requirement_entry_count") == 1
    and jarvis_cd_lock_binding.get("observed_metadata_requirement_entries")
    == [{"name": "jarvis-cd", "url": expected_jarvis_cd_url}]
    and jarvis_cd_lock_binding.get("observed_metadata_requirement_urls")
    == [expected_jarvis_cd_url]
    and jarvis_cd_lock_binding.get("package_entry_count") == 1
    and jarvis_cd_lock_binding.get("wheel_entry_count") == 1
):
    raise SystemExit(
        "clio-kit locked JARVIS dependency does not match the relay jarvis-cd release pin"
    )
jarvis_execution_native_execution = probe_jarvis_native_execution_capability(
    os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"]
)
receipt = write_install_receipt(
    install_spec=desired.relay_install_spec,
    artifact_path=Path(artifact_value) if artifact_value else None,
    components={
        "clio-relay": relay_distribution.version,
        "clio-kit": component_version or component_spec,
        "jarvis-cd": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"],
        "jarvis-util": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT"],
    },
    component_artifacts={
        "clio-relay": ComponentArtifactIdentity(
            distribution=relay_distribution.name,
            distribution_version=relay_distribution.version,
            install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"],
            requested_source=(
                "pypi"
                if os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"].startswith("clio-relay==")
                else ("wheel" if relay_artifact is not None else "checkout")
            ),
            artifact_filename=(relay_artifact.name if relay_artifact is not None else None),
            artifact_sha256=(sha256_file(relay_artifact) if relay_artifact is not None else None),
            runtime_artifact_path=(str(relay_artifact) if relay_artifact is not None else None),
            runtime_command=[
                os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
                "installation-info",
            ],
            runtime_interpreters={
                "provider": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON"],
                "execution": os.environ[
                    "CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"
                ],
            },
            runtime_executables={
                "clio-relay": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
                "uv": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE"],
            },
            persistent_tool=relay_persistent_tool,
        ),
        "clio-kit": ComponentArtifactIdentity(
            distribution="clio-kit",
            distribution_version=component_version,
            install_spec=component_spec,
            requested_source=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE"],
            artifact_filename=(component_artifact.name if component_artifact else None),
            artifact_sha256=component_artifact_sha256,
            runtime_artifact_path=(str(component_artifact) if component_artifact else None),
            runtime_command=runtime_command,
            runtime_interpreters={
                "provider": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON"],
            },
            runtime_executables={
                "clio-kit": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
                "uv": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE"],
            },
            native_execution=clio_kit_native_execution,
            persistent_tool=persistent_clio_kit_tool,
            locked_server_runtime=locked_server_runtime,
        ),
        "jarvis-cd": ComponentArtifactIdentity(
            distribution=jarvis_cd_distribution.name,
            distribution_version=jarvis_cd_distribution.version,
            install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL"],
            requested_source="github_release",
            artifact_filename=jarvis_cd_wheel.name,
            artifact_sha256=jarvis_cd_wheel_sha256,
            runtime_artifact_path=str(jarvis_cd_wheel),
            runtime_command=[
                os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
                "--help",
            ],
            runtime_interpreters={
                "provider": sys.executable,
                "execution": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"],
            },
            runtime_executables={
                "jarvis": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
            },
            native_execution=jarvis_execution_native_execution,
        ),
    },
    contract_surfaces=contract_surfaces,
    contract_degradations=contract_degradations,
    deployment_fingerprint=desired.fingerprint,
    deployment_manifest=desired.model_dump(mode="json"),
    generation=desired.fingerprint,
)
print(f"relay_install_receipt={receipt.schema_version}")
print(f"relay_artifact_sha256={receipt.artifact_sha256 or 'none'}")
__CLIO_RELAY_INSTALL_RECEIPT__
BOOTSTRAP_COMPONENTS_IDENTITY="$(bootstrap_path_set_identity \
  "$CLIO_RELAY_INSTALL_RECEIPT" \
  "$JARVIS_VENV/bin/python" \
  "$JARVIS_VENV/bin/jarvis" \
  "$RELAY_EXECUTABLE" \
  "$JARVIS_MCP_EXECUTABLE" \
  "$HOME/.local/bin/frpc" \
  "$HOME/.local/bin/frps" \
  "$HOME/.local/bin/uv")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  components_prepared "$BOOTSTRAP_COMPONENTS_IDENTITY"

if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 0 ]; then
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_config
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_private
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_shared
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_state
  BOOTSTRAP_JARVIS_INIT_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  "$JARVIS_VENV/bin/jarvis" init \
    "$HOME/.local/share/clio-relay/jarvis-config" \
    "$HOME/.local/share/clio-relay/jarvis-private" \
    "$HOME/.local/share/clio-relay/jarvis-shared"
  BOOTSTRAP_JARVIS_INIT_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_JARVIS_INIT_DURATION_NS=$((
    BOOTSTRAP_JARVIS_INIT_COMPLETED_NS - BOOTSTRAP_JARVIS_INIT_STARTED_NS
  ))
  BOOTSTRAP_JARVIS_INIT_IDENTITY="$(bootstrap_path_set_identity \
    "$JARVIS_CONFIG_FILE" "$JARVIS_REPOS_FILE" "$JARVIS_GRAPH_FILE")"
  bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
    jarvis_initialized "$BOOTSTRAP_JARVIS_INIT_IDENTITY"
  BOOTSTRAP_JARVIS_GRAPH_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE="$BOOTSTRAP_TRANSACTION_ROOT/jarvis-builtin-result.json"
  : > "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"
  chmod 0600 "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"
  if ! timeout --signal=TERM --kill-after=2s 30s \
    "$JARVIS_VENV/bin/jarvis" rg load-builtin \
      "$JARVIS_RESOURCE_GRAPH_PROFILE" +json \
      > "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"; then
    echo "JARVIS builtin resource graph activation failed" >&2
    exit 1
  fi
  BOOTSTRAP_JARVIS_BUILTIN_ACTION="$(
    "$RELAY_PROVIDER_PYTHON" - \
      "$BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE" \
      "$JARVIS_RESOURCE_GRAPH_PROFILE" \
      "$JARVIS_GRAPH_FILE" <<'__CLIO_RELAY_JARVIS_BUILTIN_RESULT__'
import hashlib
import json
import stat
import sys
from pathlib import Path

from clio_relay.bootstrap_reconcile import validate_jarvis_builtin_result

result_path = Path(sys.argv[1])
requested_profile = sys.argv[2]
active_graph_path = Path(sys.argv[3])
before = result_path.lstat()
if (
    not stat.S_ISREG(before.st_mode)
    or before.st_nlink != 1
    or not 0 < before.st_size <= 64 * 1024
):
    raise SystemExit("JARVIS builtin graph result is not one bounded regular file")
payload = result_path.read_bytes()
after = result_path.lstat()
if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
    after.st_dev,
    after.st_ino,
    after.st_size,
    after.st_mtime_ns,
):
    raise SystemExit("JARVIS builtin graph result changed during validation")
try:
    result = json.loads(payload)
except (UnicodeDecodeError, json.JSONDecodeError) as exc:
    raise SystemExit("JARVIS builtin graph result is not valid JSON") from exc
if not isinstance(result, dict):
    raise SystemExit("JARVIS builtin graph result is not an object")
try:
    validate_jarvis_builtin_result(result, requested_profile=requested_profile)
except ValueError as exc:
    raise SystemExit(f"JARVIS builtin graph result is invalid: {exc}") from exc
action = result["action"]
if action == "loaded":
    source_sha256 = result["source_sha256"]
    assert isinstance(source_sha256, str)
    # JARVIS NORMALIZES the graph as it activates it -- it expands the USER
    # variable, fills derived fields such as 1m_seqwrite_bw and needs_root, and
    # rewrites shared -- so the activated file is a DERIVATIVE of the builtin,
    # never a byte copy of it. Requiring byte equality here made every fresh
    # bootstrap fail with "does not match builtin evidence" (#158).
    #
    # Verify instead exactly what jarvis attests: that the source it read is
    # the packaged builtin carrying the digest it reported. That is the real
    # integrity property -- the graph came from our packaged profile and not
    # from somewhere else on the host. The activated file's own digest is
    # recorded as activation evidence rather than being asserted equal to it.
    source_path = Path(str(result["source"]))
    source_before = source_path.lstat()
    if not stat.S_ISREG(source_before.st_mode) or not 0 < source_before.st_size <= 64 * 1024 * 1024:
        raise SystemExit("packaged JARVIS resource graph source is not one bounded regular file")
    source_digest = hashlib.sha256()
    with source_path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            source_digest.update(chunk)
    if source_digest.hexdigest() != source_sha256:
        raise SystemExit("packaged JARVIS resource graph source does not match builtin evidence")
    graph_before = active_graph_path.lstat()
    if not stat.S_ISREG(graph_before.st_mode) or not 0 < graph_before.st_size <= 64 * 1024 * 1024:
        raise SystemExit("activated JARVIS resource graph is not one bounded regular file")
    digest = hashlib.sha256()
    with active_graph_path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    graph_after = active_graph_path.lstat()
    if (
        graph_before.st_dev,
        graph_before.st_ino,
        graph_before.st_size,
        graph_before.st_mtime_ns,
    ) != (
        graph_after.st_dev,
        graph_after.st_ino,
        graph_after.st_size,
        graph_after.st_mtime_ns,
    ):
        raise SystemExit("activated JARVIS resource graph changed during validation")
print(action)
__CLIO_RELAY_JARVIS_BUILTIN_RESULT__
  )"
  case "$BOOTSTRAP_JARVIS_BUILTIN_ACTION" in
    loaded)
      JARVIS_GRAPH_ACTION="loaded"
      ;;
    unavailable)
      if [ "$ALLOW_JARVIS_RESOURCE_GRAPH_BUILD" != "1" ]; then
        echo "requested JARVIS builtin resource graph is unavailable;" \
          "build fallback is disabled" >&2
        exit 1
      fi
      "$JARVIS_VENV/bin/jarvis" rg build +no_benchmark
      JARVIS_GRAPH_ACTION="built"
      ;;
    *)
      echo "JARVIS builtin resource graph validator returned an invalid action" >&2
      exit 1
      ;;
  esac
  BOOTSTRAP_JARVIS_GRAPH_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_JARVIS_GRAPH_DURATION_NS=$((
    BOOTSTRAP_JARVIS_GRAPH_COMPLETED_NS - BOOTSTRAP_JARVIS_GRAPH_STARTED_NS
  ))
  BOOTSTRAP_JARVIS_GRAPH_IDENTITY="$(bootstrap_path_set_identity "$JARVIS_GRAPH_FILE")"
  bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
    "resource_graph_$JARVIS_GRAPH_ACTION" "$BOOTSTRAP_JARVIS_GRAPH_IDENTITY"
  JARVIS_INIT_ACTION="initialized"
  BOOTSTRAP_JARVIS_COMMANDS_JSON="$(
    "$JARVIS_VENV/bin/python" - \
      "$HOME" "$JARVIS_RESOURCE_GRAPH_PROFILE" "$JARVIS_GRAPH_ACTION" \
      <<'__CLIO_RELAY_JARVIS_COMMANDS__'
import json
import sys

home, profile, graph_action = sys.argv[1:]
commands = [
    [
        "jarvis",
        "init",
        f"{home}/.local/share/clio-relay/jarvis-config",
        f"{home}/.local/share/clio-relay/jarvis-private",
        f"{home}/.local/share/clio-relay/jarvis-shared",
    ],
    ["jarvis", "rg", "load-builtin", profile, "+json"],
]
if graph_action == "built":
    commands.append(["jarvis", "rg", "build", "+no_benchmark"])
print(json.dumps(commands, separators=(",", ":")))
__CLIO_RELAY_JARVIS_COMMANDS__
  )"
else
  BOOTSTRAP_JARVIS_INIT_DURATION_NS=0
  BOOTSTRAP_JARVIS_GRAPH_DURATION_NS=0
  BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE=""
  JARVIS_INIT_ACTION="preserved"
  JARVIS_GRAPH_ACTION="preserved"
  BOOTSTRAP_JARVIS_COMMANDS_JSON='[]'
fi
MANAGED_JARVIS_REPO="$HOME/.local/share/clio-relay/clio_relay"
MANAGED_JARVIS_REPO_TARGET="$HOME/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
if [ -L "$MANAGED_JARVIS_REPO" ]; then
  if [ "$(readlink "$MANAGED_JARVIS_REPO")" != "$MANAGED_JARVIS_REPO_TARGET" ]; then
    echo "relay-managed JARVIS repository link points to an unexpected target" >&2
    exit 1
  fi
elif [ -e "$MANAGED_JARVIS_REPO" ]; then
  echo "relay-managed JARVIS repository path is not a symbolic link" >&2
  exit 1
else
  bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \
    managed_repo "$MANAGED_JARVIS_REPO_TARGET"
fi
export MANAGED_JARVIS_REPO JARVIS_REPOS_FILE
"$RELAY_PROVIDER_PYTHON" - "$DEST/jarvis-packages/clio_relay" \
  "$HOME/.local/share/clio-relay/managed-jarvis-repo" \
  <<'__CLIO_RELAY_JARVIS_REPO_RECONCILE__'
import os
import sys
from pathlib import Path

"""
