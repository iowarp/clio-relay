"""Rendered-script fragment: migration/service verification, the transaction's forward-
recovery driver, and the staged-generation receipt commit.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations

import shlex

from clio_relay.worker_lifetime_lock import WORKER_LIFETIME_GUARD_FD_ENV


def reconcile_script_commit(
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
    """
    Render: migration/service verification, the transaction's forward-recovery driver,
    and the staged-generation receipt commit.
    """
    return f"""  bootstrap_candidate_action journal-advance migration_started
{worker_recheck}
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
    BOOTSTRAP_QUEUE_DURATION_NS=$((BOOTSTRAP_QUEUE_COMPLETED_NS - BOOTSTRAP_QUEUE_STARTED_NS))
  fi
  bootstrap_candidate_action journal-advance migrated
  BOOTSTRAP_SERVICE_ACTIVE_AFTER=0
  BOOTSTRAP_SERVICE_RESTART_COUNT=0
  BOOTSTRAP_SERVICE_START_COUNT=0
  BOOTSTRAP_SERVICE_STOP_COUNT=0
  BOOTSTRAP_SERVICE_ENABLE_COUNT=0
  if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
    BOOTSTRAP_SERVICE_STOP_COUNT=1
    BOOTSTRAP_SERVICE_RESTART_COUNT=1
    bootstrap_candidate_action journal-advance starting
  elif [ -n "$WORKER_SERVICE_NAME" ]; then
    if [ "${{WORKER_LOAD_STATE:-unknown}}" != "loaded" ]; then
      echo "managed endpoint unit is unavailable; install it before bootstrap:" \
        "$WORKER_SERVICE_NAME" >&2
      return 1
    fi
    if [ "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" != "1" ]; then
      systemctl --user enable "$WORKER_SERVICE_NAME"
      BOOTSTRAP_SERVICE_ENABLE_COUNT=1
    fi
    BOOTSTRAP_SERVICE_START_COUNT=1
    bootstrap_candidate_action journal-advance starting
    if ! bootstrap_bounded_worker_restart; then
      echo "managed endpoint worker did not become ready after reconcile" >&2
      return 1
    fi
  fi
{worker_restart}
  if [ -n "$WORKER_SERVICE_NAME" ]; then
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  fi

  BOOTSTRAP_QUEUE_EVIDENCE="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info
  )"
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
      echo "endpoint worker did not publish bounded ready identity" >&2
      return 1
    fi
  fi
  export BOOTSTRAP_QUEUE_EVIDENCE BOOTSTRAP_WORKER_EVIDENCE
  BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=false
  if [ "$BOOTSTRAP_SERVICE_ACTIVE_AFTER" = "1" ]; then
    BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON=true
  fi
  export BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON
  export BOOTSTRAP_QUEUE_ACTION BOOTSTRAP_QUEUE_DURATION_NS
  export BOOTSTRAP_SERVICE_RESTART_COUNT BOOTSTRAP_SERVICE_START_COUNT
  export BOOTSTRAP_SERVICE_STOP_COUNT BOOTSTRAP_SERVICE_ENABLE_COUNT
  bootstrap_candidate_action journal-advance service_verified
  bootstrap_candidate_action journal-advance committed

  BOOTSTRAP_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_COMPLETED_NS
  bootstrap_provider_exec - <<'__CLIO_RELAY_RECONCILE_RECEIPT__'
import json
import os
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    JarvisStateEvidence,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)
from clio_relay.installation import load_install_receipt

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_was_active = os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER_JSON"] == "true"
queue = json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"])
worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
worker = json.loads(worker_text) if worker_text else None
inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_was_active,
    service_was_enabled=(True if desired.worker_service is not None else None),
    queue_evidence=queue,
    worker_evidence=worker,
)
if not inspection.exact_match:
    raise SystemExit(
        "reconciled generation did not pass exact inspection: " + repr(inspection.reasons)
    )
install_receipt = load_install_receipt()
plan = json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])
plan_duration = (
    int(os.environ["BOOTSTRAP_PLAN_COMPLETED_NS"])
    - int(os.environ["BOOTSTRAP_PLAN_STARTED_NS"])
) / 1_000_000_000
prepare_duration = (
    int(os.environ["BOOTSTRAP_PREPARE_COMPLETED_NS"])
    - int(os.environ["BOOTSTRAP_PREPARE_STARTED_NS"])
) / 1_000_000_000
components = {{}}
for name, action in plan["component_actions"].items():
    artifact = install_receipt.component_artifacts.get(name)
    observed = (
        artifact.model_dump(mode="json")
        if artifact is not None
        else {{"identity": install_receipt.components.get(name)}}
    )
    receipt_action = (
        "replaced"
        if action == "replace" and plan["mode"] == "component-upgrade"
        else ("prepared" if action == "replace" else "reused")
    )
    components[name] = {{
        "action": receipt_action,
        "observed_identity": observed,
        "duration_seconds": prepare_duration if action == "replace" else plan_duration,
    }}
for name in ("frp", "uv", "jarvis-util"):
    components.setdefault(
        name,
        {{
            "action": "reused",
            "observed_identity": {{"identity": install_receipt.components.get(name)}},
            "duration_seconds": plan_duration,
        }},
    )
transaction = BootstrapTransactionJournal.load(Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"]))
started_ns = min(
    int(os.environ["BOOTSTRAP_PLAN_STARTED_NS"]),
    int(os.environ["BOOTSTRAP_PREPARE_STARTED_NS"]),
)
completed_ns = int(os.environ["BOOTSTRAP_COMPLETED_NS"])
duration = (completed_ns - started_ns) / 1_000_000_000
receipt = make_bootstrap_receipt(
    invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
    desired=desired,
    outcome="reconciled",
    inspection=inspection,
    started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
    transaction=transaction,
    previous_generation=os.environ["BOOTSTRAP_PREVIOUS_GENERATION"],
    active_generation=os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
    components=components,
    duration_seconds=duration,
    downloads=[
        *(
            [{{"component": "clio-relay", "source": desired.relay_install_spec}}]
            if os.environ["BOOTSTRAP_RELAY_DOWNLOAD_COUNT"] == "1"
            else []
        ),
        *(
            [{{"component": "jarvis-cd", "source": desired.jarvis_cd_wheel_url}}]
            if os.environ["BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT"] == "1"
            else []
        ),
        *(
            [{{"component": "clio-kit", "source": desired.clio_kit_install_spec}}]
            if os.environ["BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT"] == "1"
            else []
        ),
    ],
    service_restart_count=int(os.environ["BOOTSTRAP_SERVICE_RESTART_COUNT"]),
    service_start_count=int(os.environ["BOOTSTRAP_SERVICE_START_COUNT"]),
    service_stop_count=int(os.environ["BOOTSTRAP_SERVICE_STOP_COUNT"]),
    service_enable_count=int(os.environ["BOOTSTRAP_SERVICE_ENABLE_COUNT"]),
    queue_action=os.environ["BOOTSTRAP_QUEUE_ACTION"],
    queue_duration_seconds=(
        int(os.environ["BOOTSTRAP_QUEUE_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_state_before=JarvisStateEvidence(
        **{{
            **inspection.jarvis_state.model_dump(mode="json"),
            "config_sha256": os.environ["BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE"],
            "repos_sha256": os.environ["BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE"],
            "resource_graph_sha256": os.environ["BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE"],
        }}
    ),
    jarvis_repo_reconciliation=json.loads(
        os.environ["BOOTSTRAP_JARVIS_REPO_RECONCILIATION"]
    ),
    payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
    payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
)
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
write_bootstrap_receipt(destination, receipt)
print(f"bootstrap_receipt={{destination}}")
print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_RECONCILE_RECEIPT__
  trap - EXIT
  bootstrap_release_worker_lifetime_guard || true
  bootstrap_cleanup_preparing_root
}}

bootstrap_repair_transaction_exit() {{
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    echo "bootstrap readiness repair did not complete; queue migration state is retained" >&2
  fi
  bootstrap_restore_fenced_worker_on_failure "$status"
  bootstrap_release_worker_lifetime_guard 2>/dev/null || true
  bootstrap_cleanup_preparing_root || true
  exit "$status"
}}

bootstrap_reuse_repair() {{
  BOOTSTRAP_INVOCATION_ID={shlex.quote(invocation_id)}
  BOOTSTRAP_DESIRED_FINGERPRINT="$(
    python3 -c \
      'import json,os; print(json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])["desired_fingerprint"])'
  )"
  WORKER_SERVICE_NAME="$(
    python3 -c \
      'import json,os; value=json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"]); '\
'print(value["worker_service"] or "")'
  )"
  BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$BOOTSTRAP_INVOCATION_ID"
  BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
  BOOTSTRAP_PREVIOUS_GENERATION="legacy"
  if [ -L "$HOME/.local/share/clio-relay/current" ]; then
    BOOTSTRAP_PREVIOUS_GENERATION="$(bootstrap_active_generation_identity)"
  fi
  BOOTSTRAP_SERVICE_ACTIVE_BEFORE="unknown"
  BOOTSTRAP_SERVICE_ENABLED_BEFORE=0
  if [ -n "$WORKER_SERVICE_NAME" ]; then
    if systemctl --user is-active --quiet "$WORKER_SERVICE_NAME"; then
      BOOTSTRAP_SERVICE_ACTIVE_BEFORE=1
    else
      BOOTSTRAP_SERVICE_ACTIVE_BEFORE=0
    fi
    if systemctl --user is-enabled --quiet "$WORKER_SERVICE_NAME"; then
      BOOTSTRAP_SERVICE_ENABLED_BEFORE=1
    fi
  fi
  export BOOTSTRAP_INVOCATION_ID BOOTSTRAP_DESIRED_FINGERPRINT
  export BOOTSTRAP_TRANSACTION_JOURNAL BOOTSTRAP_PREVIOUS_GENERATION
  export BOOTSTRAP_SERVICE_ACTIVE_BEFORE BOOTSTRAP_SERVICE_ENABLED_BEFORE
  export WORKER_SERVICE_NAME
  mkdir -p "$BOOTSTRAP_TRANSACTION_ROOT"
  bootstrap_candidate_action journal-create
  bootstrap_candidate_action journal-advance inspected
  bootstrap_candidate_action journal-advance preparing
  bootstrap_candidate_action journal-advance prepared
  bootstrap_candidate_action journal-advance fencing

{worker_fence}

  bootstrap_candidate_action journal-advance fenced
  bootstrap_candidate_action journal-advance activating
  BOOTSTRAP_JARVIS_BINDING_REPAIR="$(
    bootstrap_candidate_action repair-managed-binding
  )"
  export BOOTSTRAP_JARVIS_BINDING_REPAIR
  BOOTSTRAP_JARVIS_BINDING_REPAIR_SHA256="$(
    printf '%s' "$BOOTSTRAP_JARVIS_BINDING_REPAIR" | sha256sum | awk '{{print $1}}'
  )"
  bootstrap_candidate_action journal-phase managed_repository_repaired \
    "$BOOTSTRAP_JARVIS_BINDING_REPAIR_SHA256"
  bootstrap_candidate_action journal-advance activated
  bootstrap_candidate_action journal-advance migration_started
  trap bootstrap_repair_transaction_exit EXIT
{worker_recheck}
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
    BOOTSTRAP_QUEUE_DURATION_NS=$((BOOTSTRAP_QUEUE_COMPLETED_NS - BOOTSTRAP_QUEUE_STARTED_NS))
  fi
  bootstrap_candidate_action journal-advance migrated
  BOOTSTRAP_SERVICE_ACTIVE_AFTER=0
  BOOTSTRAP_SERVICE_RESTART_COUNT=0
  BOOTSTRAP_SERVICE_START_COUNT=0
  BOOTSTRAP_SERVICE_STOP_COUNT=0
  BOOTSTRAP_SERVICE_ENABLE_COUNT=0
  if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
    BOOTSTRAP_SERVICE_STOP_COUNT=1
    BOOTSTRAP_SERVICE_RESTART_COUNT=1
    bootstrap_candidate_action journal-advance starting
{worker_restart}
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  elif [ -n "$WORKER_SERVICE_NAME" ]; then
    if [ "${{WORKER_LOAD_STATE:-unknown}}" != "loaded" ]; then
      echo "managed endpoint unit is unavailable; install it before bootstrap:" \
        "$WORKER_SERVICE_NAME" >&2
      return 1
    fi
    if [ "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" != "1" ]; then
      systemctl --user enable "$WORKER_SERVICE_NAME"
      BOOTSTRAP_SERVICE_ENABLE_COUNT=1
    fi
    BOOTSTRAP_SERVICE_START_COUNT=1
    bootstrap_candidate_action journal-advance starting
    if ! bootstrap_bounded_worker_restart; then
      echo "managed endpoint worker did not become ready during repair" >&2
      return 1
    fi
    BOOTSTRAP_SERVICE_ACTIVE_AFTER=1
  fi
  BOOTSTRAP_QUEUE_EVIDENCE="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info
  )"
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
      echo "endpoint worker did not publish bounded ready identity after repair" >&2
      return 1
    fi
  fi
  export BOOTSTRAP_QUEUE_EVIDENCE BOOTSTRAP_WORKER_EVIDENCE
  export BOOTSTRAP_SERVICE_ACTIVE_AFTER BOOTSTRAP_SERVICE_RESTART_COUNT
  export BOOTSTRAP_SERVICE_START_COUNT BOOTSTRAP_SERVICE_STOP_COUNT
  export BOOTSTRAP_SERVICE_ENABLE_COUNT
  export BOOTSTRAP_QUEUE_ACTION BOOTSTRAP_QUEUE_DURATION_NS
  bootstrap_candidate_action journal-advance service_verified
  bootstrap_candidate_action journal-advance committed
  BOOTSTRAP_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_COMPLETED_NS
  CURRENT_RELAY_PROVIDER="$BOOTSTRAP_CURRENT_PROVIDER"
  test -x "$CURRENT_RELAY_PROVIDER"
  "$CURRENT_RELAY_PROVIDER" - <<'__CLIO_RELAY_REPAIR_RECEIPT__'
import json
import os
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapTransactionJournal,
    JarvisStateEvidence,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)
from clio_relay.installation import load_install_receipt

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_active_after = os.environ["BOOTSTRAP_SERVICE_ACTIVE_AFTER"] == "1"
worker_text = os.environ["BOOTSTRAP_WORKER_EVIDENCE"]
inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_active_after,
    service_was_enabled=(True if desired.worker_service is not None else None),
    queue_evidence=json.loads(os.environ["BOOTSTRAP_QUEUE_EVIDENCE"]),
    worker_evidence=json.loads(worker_text) if worker_text else None,
)
if not inspection.exact_match:
    raise SystemExit("readiness repair did not pass exact inspection: " + repr(inspection.reasons))
binding_repair = json.loads(os.environ["BOOTSTRAP_JARVIS_BINDING_REPAIR"])
if (
    binding_repair.get("schema_version") != "clio-relay.bootstrap-binding-repair.v1"
    or binding_repair.get("after") != inspection.jarvis_state.model_dump(mode="json")
    or not isinstance(binding_repair.get("before"), dict)
    or not isinstance(binding_repair.get("binding"), dict)
):
    raise SystemExit("readiness repair JARVIS binding evidence is invalid")
started_ns = int(os.environ["BOOTSTRAP_INVOCATION_STARTED_NS"])
completed_ns = int(os.environ["BOOTSTRAP_COMPLETED_NS"])
duration = (completed_ns - started_ns) / 1_000_000_000
install_receipt = load_install_receipt()
transaction = BootstrapTransactionJournal.load(Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"]))
receipt = make_bootstrap_receipt(
    invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
    desired=desired,
    outcome="repaired",
    inspection=inspection,
    started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
    transaction=transaction,
    previous_generation=os.environ["BOOTSTRAP_PREVIOUS_GENERATION"],
    active_generation=install_receipt.generation or os.environ["BOOTSTRAP_PREVIOUS_GENERATION"],
    duration_seconds=duration,
    downloads=[],
    service_restart_count=int(os.environ["BOOTSTRAP_SERVICE_RESTART_COUNT"]),
    service_start_count=int(os.environ["BOOTSTRAP_SERVICE_START_COUNT"]),
    service_stop_count=int(os.environ["BOOTSTRAP_SERVICE_STOP_COUNT"]),
    service_enable_count=int(os.environ["BOOTSTRAP_SERVICE_ENABLE_COUNT"]),
    queue_action=os.environ["BOOTSTRAP_QUEUE_ACTION"],
    queue_duration_seconds=(
        int(os.environ["BOOTSTRAP_QUEUE_DURATION_NS"]) / 1_000_000_000
    ),
    jarvis_state_before=JarvisStateEvidence.model_validate(binding_repair["before"]),
    jarvis_repo_reconciliation=binding_repair["binding"],
    payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
    payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
)
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
write_bootstrap_receipt(destination, receipt)
print(f"bootstrap_receipt={{destination}}")
print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_REPAIR_RECEIPT__
  trap - EXIT
  bootstrap_release_worker_lifetime_guard || true
  bootstrap_cleanup_preparing_root
}}
"""
