"""Rendered-script fragment: the managed JARVIS repository reconcile, the relay-only
reconcile transaction dispatch, and the final receipt/version echo.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations

from clio_relay.worker_lifetime_lock import WORKER_LIFETIME_GUARD_FD_ENV


def script_commit(
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
    """
    Render: the managed JARVIS repository reconcile, the relay-only reconcile
    transaction dispatch, and the final receipt/version echo.
    """
    return f"""from clio_relay.bootstrap_reconcile import (
    _relay_owned_jarvis_builtin_repositories,
    reconcile_managed_jarvis_repository,
)

relay_owned_builtin_repos = _relay_owned_jarvis_builtin_repositories(home=Path.home())

evidence = reconcile_managed_jarvis_repository(
    Path(os.environ["JARVIS_REPOS_FILE"]),
    Path(os.environ["MANAGED_JARVIS_REPO"]),
    managed_builtin_repo=Path(os.environ["JARVIS_REPOS_FILE"]).parent / "builtin",
    previous_managed_repos=(
        Path(sys.argv[1]),
        Path(sys.argv[2]),
        *relay_owned_builtin_repos,
    ),
)
print(f"jarvis_managed_repo={{evidence['action']}}")
__CLIO_RELAY_JARVIS_REPO_RECONCILE__
BOOTSTRAP_MANAGED_REPO_IDENTITY="$(bootstrap_path_set_identity \
  "$JARVIS_REPOS_FILE" "$MANAGED_JARVIS_REPO")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  managed_repository_reconciled "$BOOTSTRAP_MANAGED_REPO_IDENTITY"

BOOTSTRAP_VERIFIED_DESIRED_FINGERPRINT="$(
  "$RELAY_PROVIDER_PYTHON" -c \
    'import json,os; from clio_relay.bootstrap_reconcile import BootstrapDesiredState; '\
'value=json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"]); '\
'value["agent_npm_package"]=os.environ["AGENT_NPM_PACKAGE"] or None; '\
'value["agent_npm_bin"]=os.environ["AGENT_NPM_BIN"] or None; '\
'print(BootstrapDesiredState.model_validate(value).fingerprint)'
)"
if [ "$BOOTSTRAP_VERIFIED_DESIRED_FINGERPRINT" != \
     "$BOOTSTRAP_DESIRED_FINGERPRINT" ]; then
  echo "fresh bootstrap desired fingerprint changed after provider installation" >&2
  exit 1
fi
if [ "$ACTIVATION_STAGING_MODE" != "1" ] && \
   {{ [ -e "$HOME/.local/share/clio-relay/current" ] || \
      [ -L "$HOME/.local/share/clio-relay/current" ]; }}; then
  echo "fresh bootstrap found an existing current generation pointer" >&2
  exit 1
fi
RELAY_TOOL_EXECUTABLE="$(readlink -f "$RELAY_EXECUTABLE")"
JARVIS_TOOL_EXECUTABLE="$(readlink -f "$JARVIS_VENV/bin/jarvis")"
CLIO_KIT_TOOL_EXECUTABLE="$(readlink -f "$JARVIS_MCP_EXECUTABLE")"
test -x "$RELAY_TOOL_EXECUTABLE"
test -x "$JARVIS_TOOL_EXECUTABLE"
test -x "$CLIO_KIT_TOOL_EXECUTABLE"
mkdir -m 0700 "$BOOTSTRAP_GENERATION/bin"
ln -s "$RELAY_TOOL_EXECUTABLE" "$BOOTSTRAP_GENERATION/bin/clio-relay"
ln -s "$CLIO_KIT_TOOL_EXECUTABLE" "$BOOTSTRAP_GENERATION/bin/clio-kit"
mv "$CLIO_RELAY_INSTALL_RECEIPT" "$BOOTSTRAP_GENERATION/install-receipt.json"
export CLIO_RELAY_INSTALL_RECEIPT="$BOOTSTRAP_GENERATION/install-receipt.json"
export BOOTSTRAP_GENERATION JARVIS_VENV JARVIS_TOOL_EXECUTABLE
"$RELAY_PROVIDER_PYTHON" - "$BOOTSTRAP_GENERATION" \
  <<'__CLIO_RELAY_FULL_GENERATION_MANIFEST__'
import json
import os
import sys
from pathlib import Path

from clio_relay.bootstrap_full_activation_staging import (
    capture_full_mode_activation_paths_json,
)
from clio_relay.bootstrap_reconcile import (
    BootstrapReconcilePlan,
    execution_environment_identity,
    write_jarvis_wrapper,
)
from clio_relay.validation_report import sha256_file

generation = Path(sys.argv[1])
execution_root = Path(os.environ["JARVIS_VENV"])
execution_python = execution_root / "bin/python"
jarvis_executable = Path(os.environ["JARVIS_TOOL_EXECUTABLE"])
execution_identity = execution_environment_identity(
    execution_root,
    executables={{
        "python": execution_python,
        "jarvis": jarvis_executable,
    }},
)
wrapper = write_jarvis_wrapper(generation / "bin/jarvis", execution_python)
fingerprint = os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"]
plan = BootstrapReconcilePlan(
    mode="full",
    desired_fingerprint=fingerprint,
    reasons=["fresh cluster bootstrap"],
    component_actions={{
        "clio-relay": "replace",
        "jarvis-cd": "replace",
        "jarvis-util": "replace",
        "clio-kit": "replace",
        "frp": "replace",
        "uv": "replace",
    }},
)
# clio-relay#257: carries the pre-swap snapshot durably in the manifest, so
# a later crash-recovery promotion reads back the SAME one, never fresh.
manifest = {{
    "schema_version": "clio-relay.bootstrap-generation.v1",
    "fingerprint": fingerprint,
    "plan": plan.model_dump(mode="json"),
    "full_activation_paths": capture_full_mode_activation_paths_json(Path.home()),
    "legacy_execution_identity": execution_identity,
    "active_execution_identity": execution_identity,
    "jarvis_wrapper_sha256": wrapper["sha256"],
    "install_receipt": str(generation / "install-receipt.json"),
    "install_receipt_sha256": sha256_file(generation / "install-receipt.json"),
}}
for name, payload in (
    ("manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\\n"),
    (".prepared", manifest["fingerprint"] + "\\n"),
):
    path = generation / name
    with path.open("x", encoding="utf-8", newline="\\n") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
descriptor = os.open(generation, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
__CLIO_RELAY_FULL_GENERATION_MANIFEST__
BOOTSTRAP_GENERATION_MANIFEST_SHA256="$(
  sha256sum "$BOOTSTRAP_GENERATION/manifest.json" | awk '{{print $1}}'
)"
export BOOTSTRAP_GENERATION_MANIFEST_SHA256
BOOTSTRAP_GENERATION_IDENTITY="$(bootstrap_path_set_identity \
  "$BOOTSTRAP_GENERATION/manifest.json" \
  "$BOOTSTRAP_GENERATION/.prepared" \
  "$BOOTSTRAP_GENERATION/install-receipt.json" \
  "$BOOTSTRAP_GENERATION/bin/clio-relay" \
  "$BOOTSTRAP_GENERATION/bin/clio-kit" \
  "$BOOTSTRAP_GENERATION/bin/jarvis" \
  "$BOOTSTRAP_GENERATION/source")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  generation_prepared "$BOOTSTRAP_GENERATION_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  prepared "$BOOTSTRAP_DESIRED_FINGERPRINT"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" activating
if [ "$ACTIVATION_STAGING_MODE" = "1" ]; then
  # clio-relay#257: current was never claimed absent -- promote it now, in
  # the same fenced "activating" window a virgin host creates it in directly.
  bootstrap_candidate_action full-activation-reconcile \
    "$BOOTSTRAP_GENERATION" "$BOOTSTRAP_GENERATION_MANIFEST_SHA256" >/dev/null
else
  bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" \
    current "$BOOTSTRAP_GENERATION"
fi
{stable_activation_link_adoption}
BOOTSTRAP_ACTIVATION_IDENTITY="$(bootstrap_path_set_identity \
  "$HOME/.local/share/clio-relay/current" \
  "$HOME/.local/share/clio-relay/install-receipt.json" \
  "$HOME/.local/bin/clio-relay" \
  "$HOME/.local/bin/jarvis" \
  "$HOME/.local/share/clio-relay/clio_relay")"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  generation_activated "$BOOTSTRAP_ACTIVATION_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" activated
BOOTSTRAP_FULL_PREPARE_COMPLETED_NS="$(
  python3 -c 'import time; print(time.monotonic_ns())'
)"

{worker_recheck}
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" migration_started
if [ "$JARVIS_STAGING_MODE" = "1" ]; then
  # clio-relay#254: promote the built+verified staged jarvis-venv now, at
  # the same fenced boundary that just activated the rest of the
  # generation. `irreversible_boundary` is already durable (full mode sets
  # it here for every transaction), so #247's state-aware forward recovery
  # completes this exact promotion if interrupted -- idempotently, and
  # without ever observing the live path absent or half-cleared.
  bootstrap_candidate_action recovery-jarvis-venv-promote \
    "$BOOTSTRAP_INVOCATION_ID" "$(date -u +%Y%m%dT%H%M%SZ)" >/dev/null
fi
BOOTSTRAP_QUEUE_ACTION=verified_read_only
BOOTSTRAP_QUEUE_DURATION_NS=0
BOOTSTRAP_QUEUE_BEFORE="$(
  CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    "$HOME/.local/bin/clio-relay" queue readiness-info 2>/dev/null || true
)"
export BOOTSTRAP_QUEUE_BEFORE
if ! python3 -c \
  'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_QUEUE_BEFORE"]); '\
'sys.exit(0 if value.get("complete") is True else 1)' \
  2>/dev/null; then
  BOOTSTRAP_QUEUE_ACTION=audited_and_sealed
  BOOTSTRAP_QUEUE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  CLIO_RELAY_CORE_DIR={rendered_core_dir} \
  CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
  CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
  CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
  CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
  CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
  CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
  {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
  {init_command}
  BOOTSTRAP_QUEUE_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_QUEUE_DURATION_NS=$((
    BOOTSTRAP_QUEUE_COMPLETED_NS - BOOTSTRAP_QUEUE_STARTED_NS
  ))
fi
BOOTSTRAP_QUEUE_EVIDENCE="$(
  CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    "$HOME/.local/bin/clio-relay" queue readiness-info
)"
BOOTSTRAP_QUEUE_IDENTITY="$(
  BOOTSTRAP_QUEUE_EVIDENCE="$BOOTSTRAP_QUEUE_EVIDENCE" \
    python3 - <<'__CLIO_RELAY_FRESH_QUEUE_IDENTITY__'
import hashlib
import json
import os

value = json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"])
payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_QUEUE_IDENTITY__
)"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  queue_migrated "$BOOTSTRAP_QUEUE_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" migrated

BOOTSTRAP_SERVICE_RESTART_COUNT=0
BOOTSTRAP_SERVICE_START_COUNT=0
BOOTSTRAP_SERVICE_STOP_COUNT=0
BOOTSTRAP_SERVICE_ENABLE_COUNT=0
BOOTSTRAP_SERVICE_ACTIVE_AFTER=0
BOOTSTRAP_SERVICE_ENABLED_BEFORE=0
BOOTSTRAP_SERVICE_PENDING_INSTALL=0
if [ -n "$WORKER_SERVICE_NAME" ] && \
   systemctl --user is-enabled --quiet "$WORKER_SERVICE_NAME"; then
  BOOTSTRAP_SERVICE_ENABLED_BEFORE=1
fi
if [ "$WORKER_WAS_ACTIVE" = "1" ] || \
   {{ [ -n "$WORKER_SERVICE_NAME" ] && \
      [ "${{WORKER_LOAD_STATE:-unknown}}" = "loaded" ]; }}; then
  bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" starting
fi
if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
  BOOTSTRAP_SERVICE_STOP_COUNT=1
  BOOTSTRAP_SERVICE_RESTART_COUNT=1
{worker_restart}
  BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
elif [ -n "$WORKER_SERVICE_NAME" ]; then
  if [ "${{WORKER_LOAD_STATE:-unknown}}" != "loaded" ]; then
    BOOTSTRAP_SERVICE_PENDING_INSTALL=1
  else
    if [ "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" != "1" ]; then
      systemctl --user enable "$WORKER_SERVICE_NAME"
      BOOTSTRAP_SERVICE_ENABLE_COUNT=1
    fi
    BOOTSTRAP_SERVICE_START_COUNT=1
    if ! bootstrap_bounded_worker_restart; then
      echo "managed endpoint worker did not become ready after full bootstrap" >&2
      exit 1
    fi
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  fi
fi

BOOTSTRAP_WORKER_EVIDENCE=""
if [ "$BOOTSTRAP_SERVICE_ACTIVE_AFTER" = "1" ]; then
  for _BOOTSTRAP_READY_ATTEMPT in $(seq 1 90); do
    if BOOTSTRAP_WORKER_EVIDENCE="$(
      CLIO_RELAY_CORE_DIR={rendered_core_dir} \
        "$HOME/.local/bin/clio-relay" endpoint worker-info \
          --cluster "$WORKER_CLUSTER_NAME" --freshness-seconds 120 \
          --readiness-only 2>/dev/null
    )"; then
      export BOOTSTRAP_WORKER_EVIDENCE
      if python3 -c \
        'import json,os,sys; value=json.loads(os.environ["BOOTSTRAP_WORKER_EVIDENCE"]); '\
'sys.exit(0 if value.get("running") is True else 1)'; then
        break
      fi
    fi
    BOOTSTRAP_WORKER_EVIDENCE=""
    sleep 2
  done
  if [ -z "$BOOTSTRAP_WORKER_EVIDENCE" ]; then
    echo "endpoint worker did not publish bounded ready identity after full bootstrap" >&2
    exit 1
  fi
fi
BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=unknown
BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON=unknown
if [ "$BOOTSTRAP_SERVICE_PENDING_INSTALL" = "1" ]; then
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=false
  BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON=false
elif [ -n "$WORKER_SERVICE_NAME" ]; then
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=true
  BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON=true
fi
BOOTSTRAP_SERVICE_IDENTITY="$(
  BOOTSTRAP_QUEUE_EVIDENCE="$BOOTSTRAP_QUEUE_EVIDENCE" \
  BOOTSTRAP_WORKER_EVIDENCE="$BOOTSTRAP_WORKER_EVIDENCE" \
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON="$BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON" \
  BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON="$BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON" \
  BOOTSTRAP_SERVICE_PENDING_INSTALL="$BOOTSTRAP_SERVICE_PENDING_INSTALL" \
    python3 - <<'__CLIO_RELAY_FRESH_SERVICE_IDENTITY__'
import hashlib
import json
import os

worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
value = {{
    "queue": json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"]),
    "worker": json.loads(worker_text) if worker_text else None,
    "active": os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON"],
    "enabled": os.environ["BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON"],
    "pending_install": os.environ["BOOTSTRAP_SERVICE_PENDING_INSTALL"] == "1",
}}
payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_SERVICE_IDENTITY__
)"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  service_verified "$BOOTSTRAP_SERVICE_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" service_verified
BOOTSTRAP_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
export BOOTSTRAP_QUEUE_ACTION BOOTSTRAP_QUEUE_DURATION_NS BOOTSTRAP_QUEUE_EVIDENCE
export BOOTSTRAP_WORKER_EVIDENCE BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON
export BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON BOOTSTRAP_SERVICE_PENDING_INSTALL
export BOOTSTRAP_SERVICE_RESTART_COUNT BOOTSTRAP_SERVICE_START_COUNT
export BOOTSTRAP_SERVICE_STOP_COUNT BOOTSTRAP_SERVICE_ENABLE_COUNT
export BOOTSTRAP_FULL_PREPARE_STARTED_NS BOOTSTRAP_FULL_PREPARE_COMPLETED_NS
export BOOTSTRAP_JARVIS_INIT_DURATION_NS BOOTSTRAP_COMPLETED_NS
export BOOTSTRAP_JARVIS_GRAPH_DURATION_NS BOOTSTRAP_JARVIS_COMMANDS_JSON
export BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE JARVIS_INIT_ACTION JARVIS_GRAPH_ACTION
export BOOTSTRAP_FRP_DOWNLOADED BOOTSTRAP_UV_DOWNLOADED
export BOOTSTRAP_JARVIS_UTIL_DOWNLOADED BOOTSTRAP_JARVIS_CD_DOWNLOADED
export BOOTSTRAP_CLIO_KIT_DOWNLOADED BOOTSTRAP_RELAY_DOWNLOAD_COUNT

"$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_BOOTSTRAP_RECEIPT__'
import json
import os
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
    JarvisStateEvidence,
    canonical_json_sha256,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)
from clio_relay.installation import load_install_receipt

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_value = os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON"]
service_active = (
    True if service_value == "true" else (False if service_value == "false" else None)
)
enabled_value = os.environ["BOOTSTRAP_SERVICE_ENABLED_AFTER_JSON"]
service_enabled = (
    True if enabled_value == "true" else (False if enabled_value == "false" else None)
)
worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_active,
    service_was_enabled=service_enabled,
    queue_evidence=json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"]),
    worker_evidence=json.loads(worker_text) if worker_text else None,
)
service_pending_install = os.environ["BOOTSTRAP_SERVICE_PENDING_INSTALL"] == "1"
pending_reasons = {{
    "managed endpoint service is inactive",
    "managed endpoint service is disabled",
}}
if not inspection.exact_match and not (
    service_pending_install and set(inspection.reasons) == pending_reasons
):
    raise SystemExit(
        "full bootstrap did not pass exact inspection: " + repr(inspection.reasons)
    )
install_receipt = load_install_receipt()
prepare_duration = (
    int(os.environ["BOOTSTRAP_FULL_PREPARE_COMPLETED_NS"])
    - int(os.environ["BOOTSTRAP_FULL_PREPARE_STARTED_NS"])
) / 1_000_000_000
components = {{}}
for name in ("clio-relay", "clio-kit", "jarvis-cd", "jarvis-util", "frp", "uv"):
    artifact = install_receipt.component_artifacts.get(name)
    observed = (
        artifact.model_dump(mode="json")
        if artifact is not None
        else {{"identity": install_receipt.components.get(name)}}
    )
    components[name] = {{
        "action": "prepared",
        "observed_identity": observed,
        "duration_seconds": prepare_duration,
    }}
download_sources = {{
    "frp": f"github-release:{{desired.frp_version}}",
    "uv": f"github-release:{{desired.uv_version}}",
    "jarvis-util": f"git-commit:{{desired.jarvis_util_commit}}",
    "jarvis-cd": desired.jarvis_cd_wheel_url,
    "clio-kit": desired.clio_kit_install_spec,
    "clio-relay": desired.relay_install_spec,
}}
download_flags = {{
    "frp": "BOOTSTRAP_FRP_DOWNLOADED",
    "uv": "BOOTSTRAP_UV_DOWNLOADED",
    "jarvis-util": "BOOTSTRAP_JARVIS_UTIL_DOWNLOADED",
    "jarvis-cd": "BOOTSTRAP_JARVIS_CD_DOWNLOADED",
    "clio-kit": "BOOTSTRAP_CLIO_KIT_DOWNLOADED",
    "clio-relay": "BOOTSTRAP_RELAY_DOWNLOAD_COUNT",
}}
downloads = [
    {{"component": name, "source": download_sources[name]}}
    for name, flag in download_flags.items()
    if os.environ[flag] == "1"
]
transaction = BootstrapTransactionJournal.load(
    Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"])
)
if transaction.mode != "full" or transaction.desired_fingerprint != desired.fingerprint:
    raise SystemExit("full bootstrap transaction identity changed before commit")
transaction.record_phase(
    "final_inspection",
    canonical_json_sha256(inspection.model_dump(mode="json")),
)
transaction.advance(BootstrapTransactionState.COMMITTED)
transaction.persist(Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"]))
completed_ns = int(os.environ["BOOTSTRAP_COMPLETED_NS"])
started_ns = int(os.environ["BOOTSTRAP_INVOCATION_STARTED_NS"])
receipt = make_bootstrap_receipt(
    invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
    desired=desired,
    outcome="full",
    inspection=inspection,
    started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
    transaction=transaction,
    previous_generation=None,
    active_generation=desired.fingerprint,
    components=components,
    duration_seconds=(completed_ns - started_ns) / 1_000_000_000,
    downloads=downloads,
    service_restart_count=int(os.environ["BOOTSTRAP_SERVICE_RESTART_COUNT"]),
    service_start_count=int(os.environ["BOOTSTRAP_SERVICE_START_COUNT"]),
    service_stop_count=int(os.environ["BOOTSTRAP_SERVICE_STOP_COUNT"]),
    service_enable_count=int(os.environ["BOOTSTRAP_SERVICE_ENABLE_COUNT"]),
    service_pending_install=service_pending_install,
    queue_action=os.environ["BOOTSTRAP_QUEUE_ACTION"],
    queue_duration_seconds=(
        int(os.environ["BOOTSTRAP_QUEUE_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_init_action=os.environ["JARVIS_INIT_ACTION"],
    jarvis_init_duration_seconds=(
        int(os.environ["BOOTSTRAP_JARVIS_INIT_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_graph_action=os.environ["JARVIS_GRAPH_ACTION"],
    jarvis_graph_duration_seconds=(
        int(os.environ["BOOTSTRAP_JARVIS_GRAPH_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_builtin_result=(
        json.loads(Path(os.environ["BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"]).read_bytes())
        if os.environ["BOOTSTRAP_JARVIS_BUILTIN_RESULT_FILE"]
        else None
    ),
    jarvis_commands=json.loads(os.environ["BOOTSTRAP_JARVIS_COMMANDS_JSON"]),
    jarvis_state_before=JarvisStateEvidence(
        initialized=False,
        root=inspection.jarvis_state.root,
    ),
    payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
    payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
)
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
write_bootstrap_receipt(destination, receipt)
print(f"bootstrap_receipt={{destination}}")
print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_BOOTSTRAP_RECEIPT__

echo "frpc=$("$HOME/.local/bin/frpc" --version)"
echo "frps=$("$HOME/.local/bin/frps" --version)"
if [ -x "$AGENT_BIN" ]; then
  echo "agent=$("$AGENT_BIN" --version)"
fi
echo "jarvis=$("$HOME/.local/bin/jarvis" --help | head -n 1)"
echo "relay=$(clio-relay --help | head -n 1)"
"""
