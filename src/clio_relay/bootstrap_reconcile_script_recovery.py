"""Rendered-script fragment: staged-provider exec wrapper, candidate-action dispatch
(journal/recovery/activation actions), generation-identity helpers, and the start of
interrupted-repair recovery.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations

from clio_relay.bootstrap_constants import UV_LINUX_AMD64_EXECUTABLE_SHA256


def reconcile_script_recovery(
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
    Render: staged-provider exec wrapper, candidate-action dispatch
    (journal/recovery/activation actions), generation-identity helpers, and the start of
    interrupted-repair recovery.
    """
    return f"""
bootstrap_plan_value() {{
  local field="$1"
  python3 - "$field" <<'__CLIO_RELAY_PLAN_VALUE__'
import json
import os
import sys

value = json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])
for part in sys.argv[1].split("."):
    value = value[part]
if not isinstance(value, str):
    raise SystemExit("bootstrap plan value is not a string")
print(value)
__CLIO_RELAY_PLAN_VALUE__
}}

bootstrap_provider_exec() (
{staged_provider_environment_sanitizer}
  if [ -n "${{BOOTSTRAP_STAGED_GENERATION:-}}" ]; then
    exec python3 -I -c {staged_provider_exec_program} \
      "$BOOTSTRAP_STAGED_GENERATION" \
      "$BOOTSTRAP_STAGED_MANIFEST_SHA256" "$@"
  elif [ "${{BOOTSTRAP_CANDIDATE_PROVIDER_READY:-0}}" = "1" ]; then
    exec python3 -I -c {candidate_uv_install_program} \
      verify-installed-and-exec \
      "$BOOTSTRAP_PINNED_UV" {UV_LINUX_AMD64_EXECUTABLE_SHA256} \
      "$BOOTSTRAP_CANDIDATE_ARTIFACT" "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" \
      "$BOOTSTRAP_CANDIDATE_TOOL_DIR" "$BOOTSTRAP_CANDIDATE_BIN_DIR" \
      "$BOOTSTRAP_CANDIDATE_CACHE_DIR" \
      "$BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR" \
      "$BOOTSTRAP_CANDIDATE_PROVIDER_SHA256" -I "$@"
  else
    exec "$BOOTSTRAP_RECOVERY_PROVIDER" -I "$@"
  fi
)

bootstrap_candidate_action() {{
  local action="$1"
  shift
  bootstrap_provider_exec - "$BOOTSTRAP_CANDIDATE_RECONCILE" "$action" "$@" \
    <<'__CLIO_RELAY_CANDIDATE_ACTION__'
import importlib.util
import json
import os
import sys
from pathlib import Path

path, action, *arguments = sys.argv[1:]
if os.environ.get("BOOTSTRAP_STAGED_GENERATION") and action != "repair-legacy-cursors":
    from clio_relay import bootstrap_reconcile as module
else:
    candidate_root = os.environ["BOOTSTRAP_CANDIDATE_PYTHON_ROOT"]
    if not sys.path or sys.path[0] != candidate_root:
        sys.path.insert(0, candidate_root)
    name = "clio_relay.bootstrap_reconcile_candidate_action"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise SystemExit("could not load candidate bootstrap reconciler")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
from clio_relay import bootstrap_jarvis_staging, bootstrap_recovery
from clio_relay import bootstrap_full_activation_staging
journal_path = Path(os.environ["BOOTSTRAP_TRANSACTION_JOURNAL"])
if action == "journal-create":
    service_value = os.environ["BOOTSTRAP_SERVICE_ACTIVE_BEFORE"]
    journal = module.BootstrapTransactionJournal(
        invocation_id=os.environ["BOOTSTRAP_INVOCATION_ID"],
        desired_fingerprint=os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
        mode=os.environ.get("BOOTSTRAP_PLAN_MODE", "relay-only"),
        state=module.BootstrapTransactionState.LOCKED,
        previous_generation=os.environ["BOOTSTRAP_PREVIOUS_GENERATION"] or None,
        service_name=os.environ["WORKER_SERVICE_NAME"] or None,
        service_was_active=(
            True if service_value == "1" else (False if service_value == "0" else None)
        ),
        service_was_enabled=(
            True
            if os.environ.get("BOOTSTRAP_SERVICE_ENABLED_BEFORE") == "1"
            else (
                False
                if os.environ.get("BOOTSTRAP_SERVICE_ENABLED_BEFORE") == "0"
                else None
            )
        ),
        phase_identities={{"locked": os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"]}},
    )
    journal.persist(journal_path)
elif action == "journal-advance":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    target = module.BootstrapTransactionState(arguments[0])
    if target is module.BootstrapTransactionState.PREPARED:
        journal.prepared_generation = os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"]
    journal.advance(target)
    journal.persist(journal_path)
elif action == "journal-phase":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    journal.record_phase(arguments[0], arguments[1])
    journal.persist(journal_path)
elif action == "journal-state":
    print(module.BootstrapTransactionJournal.load(journal_path).state.value)
elif action == "recovery-plan":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    payload = journal.model_dump(mode="json")
    payload["recovery_mode"] = journal.recovery_mode
    payload["recovery_needs_staged_identity"] = (
        bootstrap_recovery.recovery_needs_staged_identity(journal)
    )
    payload["recovery_needs_jarvis_swap"] = (
        bootstrap_jarvis_staging.JARVIS_VENV_STAGED_OWNED_NAME in journal.owned_paths
    )
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
elif action == "recovery-prepared-manifest":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    try:
        print(
            bootstrap_recovery.require_phase_identity(
                journal, arguments[0], journal_path=journal_path
            )
        )
    except module.ConfigurationError as exc:
        raise SystemExit(str(exc)) from None
elif action == "recovery-complete-active":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
    desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
    desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
    desired = module.BootstrapDesiredState.model_validate(desired_payload)
    try:
        evidence = bootstrap_recovery.complete_active_generation_recovery(journal, desired)
    except module.ConfigurationError as exc:
        raise SystemExit(str(exc)) from None
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
elif action == "recovery-jarvis-venv-promote":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    owned = journal.owned_paths.get(bootstrap_jarvis_staging.JARVIS_VENV_STAGED_OWNED_NAME)
    identity = owned.identity if owned is not None else None
    if identity is None:
        raise SystemExit("bootstrap transaction omitted its staged jarvis-venv identity")
    try:
        evidence = bootstrap_jarvis_staging.promote_staged_jarvis_venv(
            invocation_id=arguments[0],
            retired_at=arguments[1],
            staged_identity=(identity.device, identity.inode),
        )
    except module.ConfigurationError as exc:
        raise SystemExit(str(exc)) from None
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
elif action == "full-activation-reconcile":
    # clio-relay#257: see bootstrap_full_activation_staging.
    promote = bootstrap_full_activation_staging.promote_full_mode_activation_links_from_manifest
    try:
        evidence = promote(
            Path(arguments[0]),
            expected_manifest_sha256=arguments[1],
            desired_fingerprint=os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
        )
    except module.ConfigurationError as exc:
        raise SystemExit(str(exc)) from None
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
elif action == "recovery-complete":
    journal = module.BootstrapTransactionJournal.load(journal_path)
    journal.complete_recovery()
    journal.persist(journal_path)
elif action == "execution-boundary":
    root = Path(arguments[0])
    print(
        json.dumps(
            module.execution_environment_identity(
                root,
                executables={{
                    "python": Path(arguments[1]),
                    "jarvis": Path(arguments[2]),
                }},
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "jarvis-wrapper":
    print(
        json.dumps(
            module.write_jarvis_wrapper(Path(arguments[0]), Path(arguments[1])),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "finish-activation":
    if os.environ.get("BOOTSTRAP_STAGED_GENERATION"):
        receipt_payload = module._read_regular_bounded(
            Path(arguments[0]) / "install-receipt.json",
            maximum=4 * 1024 * 1024,
        )
        receipt_document = json.loads(receipt_payload)
        desired_payload = receipt_document.get("deployment_manifest")
        if not isinstance(desired_payload, dict):
            raise SystemExit("staged install receipt omitted its desired state")
    else:
        desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
        desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
        desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
    desired = module.BootstrapDesiredState.model_validate(desired_payload)
    print(
        json.dumps(
            module.finish_staged_activation(
                desired,
                generation=Path(arguments[0]),
                expected_manifest_sha256=arguments[1],
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "repair-legacy-cursors":
    print(
        json.dumps(
            module.repair_legacy_cursor_permissions_for_upgrade(Path(arguments[0])),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "repair-managed-binding":
    desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
    desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
    desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
    desired = module.BootstrapDesiredState.model_validate(desired_payload)
    before = module.inspect_jarvis_state(desired)
    binding = module.repair_managed_jarvis_binding(
        desired,
        previous_managed_repos=(
            Path.home() / ".local/src/clio-relay/jarvis-packages/clio_relay",
        ),
    )
    after = module.inspect_jarvis_state(desired)
    print(
        json.dumps(
            {{
                "schema_version": "clio-relay.bootstrap-binding-repair.v1",
                "before": before.model_dump(mode="json"),
                "after": after.model_dump(mode="json"),
                "binding": binding,
            }},
            sort_keys=True,
            separators=(",", ":"),
        )
    )
elif action == "exchange-preflight":
    print(
        json.dumps(
            module.verify_atomic_exchange_support(
                tuple(Path(value) for value in arguments),
                identity=os.environ["BOOTSTRAP_DESIRED_FINGERPRINT"],
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
else:
    raise SystemExit(f"unknown candidate bootstrap action: {{action}}")
__CLIO_RELAY_CANDIDATE_ACTION__
}}

bootstrap_use_staged_provider() {{
  local generation="$1"
  local expected_manifest_sha256="$2"
  if [ -L "$generation" ] || [ ! -d "$generation" ]; then
    echo "staged bootstrap generation is not one owned directory" >&2
    return 1
  fi
  case "$expected_manifest_sha256" in
    (*[!0-9a-f]*|'') echo "staged manifest digest is invalid" >&2; return 1 ;;
  esac
  if [ "${{#expected_manifest_sha256}}" -ne 64 ]; then
    echo "staged manifest digest has an invalid length" >&2
    return 1
  fi
  BOOTSTRAP_STAGED_GENERATION="$generation"
  BOOTSTRAP_STAGED_MANIFEST_SHA256="$expected_manifest_sha256"
  export BOOTSTRAP_STAGED_GENERATION BOOTSTRAP_STAGED_MANIFEST_SHA256
  bootstrap_provider_exec -c \
    'import clio_relay,jarvis_cd; print("staged_provider=sealed_memfd")' >/dev/null
}}

bootstrap_require_stable_link() {{
  local path="$1"
  local expected="$2"
  if [ ! -L "$path" ] || [ "$(readlink "$path")" != "$expected" ]; then
    echo "bootstrap stable activation link changed: $path" >&2
    return 1
  fi
}}

bootstrap_verify_stable_activation_links() {{
  bootstrap_require_stable_link \
    "$HOME/.local/share/clio-relay/install-receipt.json" \
    "$HOME/.local/share/clio-relay/current/install-receipt.json"
  bootstrap_require_stable_link "$HOME/.local/bin/clio-relay" \
    "$HOME/.local/share/clio-relay/current/bin/clio-relay"
  bootstrap_require_stable_link "$HOME/.local/bin/jarvis" \
    "$HOME/.local/share/clio-relay/current/bin/jarvis"
  bootstrap_require_stable_link \
    "$HOME/.local/share/clio-relay/clio_relay" \
      "$HOME/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
}}

bootstrap_active_generation_identity() {{
  local current target prefix identity
  current="$HOME/.local/share/clio-relay/current"
  if [ ! -L "$current" ]; then
    echo "bootstrap current generation pointer is not a symbolic link" >&2
    return 1
  fi
  target="$(readlink -f "$current")"
  prefix="$(readlink -f "$HOME/.local/share/clio-relay/generations")/"
  case "$target" in
    "$prefix"*) identity="${{target#"$prefix"}}" ;;
    *)
      echo "bootstrap current pointer does not name one managed generation" >&2
      return 1
      ;;
  esac
  case "$identity" in
    (*[!0-9a-f]*|'')
      echo "bootstrap current generation identity is invalid" >&2
      return 1
      ;;
  esac
  if [ "${{#identity}}" -ne 64 ]; then
    echo "bootstrap current generation identity has an invalid length" >&2
    return 1
  fi
  echo "$identity"
}}

bootstrap_active_generation_provider() {{
  local identity generations provider
  identity="$(bootstrap_active_generation_identity)" || return 1
  generations="$(readlink -f "$HOME/.local/share/clio-relay/generations")"
  provider="$generations/$identity/tools/clio-relay/bin/python"
  if [ ! -f "$provider" ] || [ ! -x "$provider" ]; then
    echo "bootstrap active generation provider is unavailable" >&2
    return 1
  fi
  echo "$provider"
}}

bootstrap_reconcile_transaction_exit() {{
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    local state
    state="$(bootstrap_candidate_action journal-state 2>/dev/null || true)"
    case "$state" in
      activating|activated|migration_started|migrated|starting|service_verified)
        echo "bootstrap reconcile crossed its forward-only activation boundary;" \
          "new generation retained for forward recovery" >&2
        ;;
      *)
        if [ "$WORKER_WAS_ACTIVE" = "1" ] && [ "$WORKER_RESTARTED" != "1" ]; then
          bootstrap_bounded_worker_restart || true
        fi
        ;;
    esac
  fi
  bootstrap_release_worker_lifetime_guard 2>/dev/null || true
  bootstrap_cleanup_preparing_root || true
  exit "$status"
}}

bootstrap_recovery_value() {{
  local field="$1"
  python3 - "$field" <<'__CLIO_RELAY_RECOVERY_VALUE__'
import json
import os
import sys

value = json.loads(os.environ["BOOTSTRAP_RECOVERY_JSON"])[sys.argv[1]]
if value is None:
    print("")
elif isinstance(value, bool):
    print("1" if value else "0")
elif isinstance(value, str):
    print(value)
else:
    raise SystemExit("bootstrap recovery field has an invalid type")
__CLIO_RELAY_RECOVERY_VALUE__
}}

bootstrap_recover_service() {{
  local service_name="$1"
  [ -n "$service_name" ] || return 0
  if [ "$(systemctl --user show "$service_name" --property=LoadState --value)" != \
       "loaded" ]; then
    echo "bootstrap recovery requires the registered endpoint service:" \
      "$service_name" >&2
    return 1
  fi
  systemctl --user enable "$service_name"
  systemctl --user start "$service_name"
  for _BOOTSTRAP_RECOVERY_START_ATTEMPT in $(seq 1 90); do
    if systemctl --user is-active --quiet "$service_name"; then
      return 0
    fi
    sleep 2
  done
  echo "bootstrap recovery could not restore endpoint service: $service_name" >&2
  return 1
}}

bootstrap_fence_recovered_service() {{
  local service_name="$1"
  local load_state active_state stopped_state
  [ -n "$service_name" ] || return 0
  load_state="$(
    systemctl --user show "$service_name" --property=LoadState --value
  )" || return 1
  active_state="$(
    systemctl --user show "$service_name" --property=ActiveState --value
  )" || return 1
  case "$load_state:$active_state" in
    loaded:active|loaded:activating|loaded:reloading|loaded:deactivating)
      systemctl --user stop "$service_name"
      stopped_state="$(
        systemctl --user show "$service_name" --property=ActiveState --value
      )" || return 1
      case "$stopped_state" in
        inactive|failed) return 0 ;;
        *)
          echo "bootstrap recovery could not fence endpoint service: $service_name" >&2
          return 1
          ;;
      esac
      ;;
    loaded:inactive|loaded:failed|masked:inactive|not-found:inactive) return 0 ;;
    *)
      echo "bootstrap recovery found unknown endpoint service state: " \
        "$load_state:$active_state:$service_name" >&2
      return 1
      ;;
  esac
}}

bootstrap_recover_interrupted_repair() {{
  local service_was_active="$1"
  local cluster_name="$2"
  local interrupted_service_name="$3"
  local recovery_service_should_run=0
  local recovery_queue recovery_worker recovery_worker_ready
  if [ "$service_was_active" = "1" ]; then
    recovery_service_should_run=1
  fi

{worker_fence}

  if [ -n "$interrupted_service_name" ] && \
     [ "$interrupted_service_name" != "$WORKER_SERVICE_NAME" ]; then
    echo "bootstrap repair recovery service identity changed:" \
      "$interrupted_service_name != $WORKER_SERVICE_NAME" >&2
    return 1
  fi

  # A power loss releases the lifetime lock after the original fence.  Preserve
  # both the journaled pre-transaction state and an operator restart observed
  # at recovery entry, then keep the exact queue writer fence until readiness is
  # sealed or the service restart relinquishes it.
  if [ "$WORKER_WAS_ACTIVE" = "1" ]; then
    recovery_service_should_run=1
  elif [ "$service_was_active" = "1" ]; then
    WORKER_WAS_ACTIVE=1
    WORKER_STOP_CONFIRMED=1
  fi

{worker_recheck}

  # The managed repository/link operation is exact and idempotent.  Re-running
  # it covers interruption before, during, or after the original activation
  # without depending on staged-generation evidence that repair never records.
  bootstrap_candidate_action repair-managed-binding >/dev/null

"""
