"""Rendered-script fragment: the rest of interrupted-repair recovery, the queue-readiness
wait, and the new candidate generation's package/provider preparation.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations

import shlex

from clio_relay.bootstrap_constants import (
    JARVIS_CD_VERSION,
    JARVIS_CD_WHEEL_FILENAME,
    JARVIS_CD_WHEEL_SHA256,
    JARVIS_CD_WHEEL_URL,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)
from clio_relay.worker_lifetime_lock import (
    WORKER_LIFETIME_GUARD_FD_ENV,
    WORKER_LIFETIME_LOCK_NAME,
)


def reconcile_script_generation_prepare(
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
    Render: the rest of interrupted-repair recovery, the queue-readiness wait, and the
    new candidate generation's package/provider preparation.
    """
    return f"""  recovery_queue="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info 2>/dev/null || true
  )"
  export recovery_queue
  if ! python3 -c \
    'import json,os,sys; value=json.loads(os.environ["recovery_queue"]); '\
'sys.exit(0 if value.get("complete") is True else 1)' \
    2>/dev/null; then
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
    CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
    CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
    CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
    CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
    CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
    CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
    {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
    {init_command}
  fi
  recovery_queue="$(
    CLIO_RELAY_CORE_DIR={rendered_core_dir} \
      "$HOME/.local/bin/clio-relay" queue readiness-info
  )"
  export recovery_queue
  if ! python3 -c \
    'import json,os,sys; value=json.loads(os.environ["recovery_queue"]); '\
'sys.exit(0 if value.get("complete") is True else 1)'; then
    echo "bootstrap repair recovery did not establish queue readiness" >&2
    return 1
  fi

  if [ "$recovery_service_should_run" = "1" ] && [ -n "$WORKER_SERVICE_NAME" ]; then
    if ! bootstrap_bounded_worker_restart; then
      echo "bootstrap repair recovery could not restore endpoint service:" \
        "$WORKER_SERVICE_NAME" >&2
      return 1
    fi
    recovery_worker_ready=0
    for _BOOTSTRAP_REPAIR_RECOVERY_WORKER_ATTEMPT in $(seq 1 90); do
      recovery_worker="$(
        CLIO_RELAY_CORE_DIR={rendered_core_dir} \
          "$HOME/.local/bin/clio-relay" endpoint worker-info \
            --cluster "$cluster_name" --freshness-seconds 120 2>/dev/null || true
      )"
      if printf '%s\\n' "$recovery_worker" | python3 -c \
        'import json,sys; value=json.load(sys.stdin); '\
'sys.exit(0 if value.get("running") is True else 1)' 2>/dev/null; then
        recovery_worker_ready=1
        break
      fi
      sleep 2
    done
    if [ "$recovery_worker_ready" != "1" ]; then
      echo "bootstrap repair recovery did not observe a ready worker" >&2
      return 1
    fi
  fi
  BOOTSTRAP_REPAIR_RECOVERY_ACTIVE=1
}}

bootstrap_recover_previous_transaction() {{
  BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
  export BOOTSTRAP_TRANSACTION_JOURNAL
  BOOTSTRAP_RECOVERY_JSON="$(bootstrap_candidate_action recovery-plan)"
  export BOOTSTRAP_RECOVERY_JSON
  local recovery_mode interrupted_mode interrupted_invocation service_name
  local service_was_active cluster_name
  recovery_mode="$(bootstrap_recovery_value recovery_mode)"
  interrupted_mode="$(bootstrap_recovery_value mode)"
  interrupted_invocation="$(bootstrap_recovery_value invocation_id)"
  service_name="$(bootstrap_recovery_value service_name)"
  service_was_active="$(bootstrap_recovery_value service_was_active)"
  cluster_name="$(
    python3 -c \
      'import json,os; print(json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])["cluster"] or "")'
  )"
  case "$interrupted_invocation" in
    (*[!A-Za-z0-9_.-]*|'')
      echo "bootstrap transaction has an invalid invocation identity" >&2
      return 1
      ;;
  esac
  BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$interrupted_invocation"
  BOOTSTRAP_ROLLBACK_DIR="$BOOTSTRAP_TRANSACTION_ROOT/rollback"
  export BOOTSTRAP_TRANSACTION_ROOT BOOTSTRAP_ROLLBACK_DIR
  case "$recovery_mode" in
    discard)
      if [ "$service_was_active" = "1" ]; then
        bootstrap_recover_service "$service_name"
      fi
      ;;
    rollback)
      echo "legacy automatic bootstrap rollback is disabled because activation" \
        "identities cannot be proved; operator reconciliation is required" >&2
      return 1
      ;;
    forward)
      if [ "$interrupted_mode" = "repair" ]; then
        bootstrap_recover_interrupted_repair \
          "$service_was_active" "$cluster_name" "$service_name"
      else
        local recovery_needs_staged recovery_needs_swap interrupted_state
        recovery_needs_staged="$(bootstrap_recovery_value recovery_needs_staged_identity)"
        recovery_needs_swap="$(bootstrap_recovery_value recovery_needs_jarvis_swap)"
        interrupted_state="$(bootstrap_recovery_value state)"
        if [ "$recovery_needs_staged" = "1" ]; then
          local prepared_generation prepared_manifest_sha256 recovery_generation
          prepared_generation="$(bootstrap_recovery_value prepared_generation)"
          case "$prepared_generation" in
            (*[!0-9a-f]*|'')
              echo "bootstrap forward recovery has an invalid generation" >&2
              return 1
              ;;
          esac
          [ "${{#prepared_generation}}" -eq 64 ] || return 1
          prepared_manifest_sha256="$(
            bootstrap_candidate_action recovery-prepared-manifest prepared_manifest
          )"
          case "$prepared_manifest_sha256" in
            (*[!0-9a-f]*|'')
              echo "bootstrap forward recovery has an invalid manifest identity" >&2
              return 1
              ;;
          esac
          [ "${{#prepared_manifest_sha256}}" -eq 64 ] || return 1
          recovery_generation="$HOME/.local/share/clio-relay/generations/$prepared_generation"
          if [ -L "$recovery_generation" ] || [ ! -d "$recovery_generation" ]; then
            echo "bootstrap forward recovery generation identity changed" >&2
            return 1
          fi
          if [ "$JARVIS_EXISTING_FILE_COUNT" -ne 3 ] || \
             [ -z "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE" ] || \
             [ -z "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE" ]; then
            echo "bootstrap forward recovery cannot prove preserved JARVIS state" >&2
            return 1
          fi
          echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
            sha256sum --check --strict -
          echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
            sha256sum --check --strict -
          bootstrap_fence_recovered_service "$service_name"
          bootstrap_use_staged_provider "$recovery_generation" "$prepared_manifest_sha256"
          bootstrap_candidate_action finish-activation \
            "$recovery_generation" "$prepared_manifest_sha256" >/dev/null
          bootstrap_verify_stable_activation_links
          echo "$BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE *$JARVIS_CONFIG_FILE" | \
            sha256sum --check --strict -
          echo "$BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE *$JARVIS_GRAPH_FILE" | \
            sha256sum --check --strict -
        else
          # Activation (and, for a full-mode staging transaction, the
          # jarvis-venv promotion at the same fenced boundary) already
          # completed durably -- #247: never re-derive a staged identity
          # once the ACTIVE generation is itself the proof.
          bootstrap_candidate_action recovery-complete-active >/dev/null
          if [ "$recovery_needs_swap" = "1" ]; then
            bootstrap_candidate_action recovery-jarvis-venv-promote \
              "$interrupted_invocation" "recovery-$(date -u +%Y%m%dT%H%M%SZ)" >/dev/null
          fi
        fi
        if [ "$interrupted_state" != "service_verified" ]; then
          mkdir -p -- {rendered_core_dir}
          exec 8<>"{rendered_core_dir}/{WORKER_LIFETIME_LOCK_NAME}"
          WORKER_LIFETIME_GUARD_FD=8
          if ! flock -n 8; then
            echo "bootstrap forward recovery cannot prove exclusive queue ownership" >&2
            return 1
          fi
          {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
            bootstrap_candidate_action repair-legacy-cursors {rendered_core_dir} >/dev/null
          CLIO_RELAY_CORE_DIR={rendered_core_dir} \
          CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
          {WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
            "$HOME/.local/bin/clio-relay" init --migrate-legacy-output
          CLIO_RELAY_CORE_DIR={rendered_core_dir} \
            "$HOME/.local/bin/clio-relay" queue readiness-info >/dev/null
          exec 8>&-
          if [ "$service_was_active" = "1" ] && [ -n "$service_name" ]; then
            bootstrap_recover_service "$service_name"
            local recovery_worker recovery_worker_ready
            recovery_worker_ready=0
            for _BOOTSTRAP_RECOVERY_WORKER_ATTEMPT in $(seq 1 90); do
              recovery_worker="$(
                CLIO_RELAY_CORE_DIR={rendered_core_dir} \
                  "$HOME/.local/bin/clio-relay" endpoint worker-info \
                    --cluster "$cluster_name" --freshness-seconds 120 2>/dev/null || true
              )"
              if printf '%s\\n' "$recovery_worker" | python3 -c \
                'import json,sys; value=json.load(sys.stdin); '\
'sys.exit(0 if value.get("running") is True else 1)' 2>/dev/null; then
                recovery_worker_ready=1
                break
              fi
              sleep 2
            done
            if [ "$recovery_worker_ready" != "1" ]; then
              echo "bootstrap forward recovery did not observe a ready worker" >&2
              return 1
            fi
          fi
        fi
      fi
      ;;
    none)
      return 0
      ;;
    *)
      echo "bootstrap transaction has an invalid recovery mode" >&2
      return 1
      ;;
  esac
  bootstrap_candidate_action recovery-complete
  if [ "${{BOOTSTRAP_REPAIR_RECOVERY_ACTIVE:-0}}" = "1" ]; then
    bootstrap_release_worker_lifetime_guard
    trap - EXIT
  fi
}}

bootstrap_relay_only_reconcile() {{
  BOOTSTRAP_INVOCATION_ID={shlex.quote(invocation_id)}
  WORKER_SERVICE_NAME="$(
    python3 -c \
      'import json,os; value=json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"]); '\
'print(value["worker_service"] or "")'
  )"
  BOOTSTRAP_DESIRED_FINGERPRINT="$(
    python3 -c \
      'import json,os; print(json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])["desired_fingerprint"])'
  )"
  case "$BOOTSTRAP_DESIRED_FINGERPRINT" in
    (*[!0-9a-f]*|'') echo "invalid desired generation fingerprint" >&2; return 1 ;;
  esac
  if [ "${{#BOOTSTRAP_DESIRED_FINGERPRINT}}" -ne 64 ]; then
    echo "invalid desired generation fingerprint length" >&2
    return 1
  fi
  BOOTSTRAP_GENERATIONS_ROOT="$HOME/.local/share/clio-relay/generations"
  BOOTSTRAP_GENERATION="$BOOTSTRAP_GENERATIONS_ROOT/$BOOTSTRAP_DESIRED_FINGERPRINT"
  BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$BOOTSTRAP_INVOCATION_ID"
  BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
  BOOTSTRAP_ROLLBACK_DIR="$BOOTSTRAP_TRANSACTION_ROOT/rollback"
  BOOTSTRAP_PREVIOUS_GENERATION="legacy"
  if [ -L "$HOME/.local/share/clio-relay/current" ]; then
    BOOTSTRAP_PREVIOUS_GENERATION="$(bootstrap_active_generation_identity)"
  elif [ -e "$HOME/.local/share/clio-relay/current" ]; then
    echo "bootstrap current generation pointer is not a symbolic link" >&2
    return 1
  fi
  BOOTSTRAP_SERVICE_ACTIVE_BEFORE="unknown"
  BOOTSTRAP_SERVICE_ENABLED_BEFORE=0
  if [ -n "${{WORKER_SERVICE_NAME:-}}" ]; then
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
  mkdir -p "$BOOTSTRAP_GENERATIONS_ROOT" "$BOOTSTRAP_TRANSACTION_ROOT"
  bootstrap_candidate_action journal-create
  bootstrap_candidate_action journal-advance inspected
  bootstrap_candidate_action journal-advance preparing
  BOOTSTRAP_PREPARE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_RELAY_DOWNLOAD_COUNT=0
  BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT=0
  BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT=0

  if [ -e "$BOOTSTRAP_GENERATION" ]; then
    if [ ! -f "$BOOTSTRAP_GENERATION/.prepared" ]; then
      if [ -L "$HOME/.local/share/clio-relay/current" ]; then
        BOOTSTRAP_INCOMPLETE_CURRENT_TARGET="$(
          readlink -f "$HOME/.local/share/clio-relay/current" || true
        )"
        if [ -z "$BOOTSTRAP_INCOMPLETE_CURRENT_TARGET" ]; then
          echo "active generation pointer could not be resolved" >&2
          return 1
        fi
        if [ "$BOOTSTRAP_INCOMPLETE_CURRENT_TARGET" = \
             "$(readlink -f "$BOOTSTRAP_GENERATION")" ]; then
          echo "incomplete generation is active; recovery is required" >&2
          return 1
        fi
      fi
      rm -rf -- "$BOOTSTRAP_GENERATION"
    fi
  fi
  LEGACY_JARVIS_VENV="$(bootstrap_plan_value reusable_paths.jarvis_execution_environment)"
  LEGACY_JARVIS_PYTHON="$(bootstrap_plan_value reusable_paths.jarvis_execution_python)"
  LEGACY_JARVIS_EXECUTABLE="$(
    bootstrap_plan_value reusable_paths.jarvis_execution_executable
  )"
  if [ "$LEGACY_JARVIS_PYTHON" != "$LEGACY_JARVIS_VENV/bin/python" ] || \
     [ "$LEGACY_JARVIS_EXECUTABLE" != "$LEGACY_JARVIS_VENV/bin/jarvis" ] || \
     [ ! -x "$LEGACY_JARVIS_PYTHON" ] || [ ! -x "$LEGACY_JARVIS_EXECUTABLE" ]; then
    echo "legacy JARVIS executables do not match the retained execution boundary" >&2
    return 1
  fi
  JARVIS_CD_WHEEL=""
  CLIO_KIT_EXECUTABLE=""
  ACTIVE_JARVIS_VENV="$LEGACY_JARVIS_VENV"
  ACTIVE_JARVIS_PYTHON="$LEGACY_JARVIS_PYTHON"
  JARVIS_MCP_INSTALL_SPEC=""
  JARVIS_MCP_ARTIFACT_SHA256=""
  JARVIS_MCP_ARTIFACT_PATH=""
  CLIO_KIT_PROVIDER_PYTHON=""
  REUSED_RELAY_ARTIFACT=""
  if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ]; then
    JARVIS_CD_WHEEL="$(bootstrap_plan_value reusable_paths.jarvis-cd_artifact)"
    CLIO_KIT_EXECUTABLE="$(
      bootstrap_plan_value reusable_paths.clio-kit_clio-kit_executable
    )"
    REUSED_RELAY_ARTIFACT="$(
      bootstrap_plan_value reusable_paths.clio-relay_artifact
    )"
  else
    JARVIS_CD_WHEEL="$BOOTSTRAP_GENERATION/artifacts/{JARVIS_CD_WHEEL_FILENAME}"
    CLIO_KIT_EXECUTABLE="$BOOTSTRAP_GENERATION/bin/clio-kit"
    ACTIVE_JARVIS_VENV="$BOOTSTRAP_GENERATION/jarvis-venv"
    ACTIVE_JARVIS_PYTHON="$BOOTSTRAP_GENERATION/jarvis-venv/bin/python"
    JARVIS_MCP_INSTALL_SPEC={rendered_jarvis_mcp_install_spec}
    JARVIS_MCP_ARTIFACT_SHA256={rendered_jarvis_mcp_artifact_sha256}
    JARVIS_MCP_ARTIFACT_PATH="$BOOTSTRAP_GENERATION/artifacts/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"
  fi
  BOOTSTRAP_LEGACY_IDENTITY="$(
    bootstrap_candidate_action execution-boundary \
      "$LEGACY_JARVIS_VENV" "$LEGACY_JARVIS_PYTHON" "$LEGACY_JARVIS_EXECUTABLE"
  )"
  export BOOTSTRAP_LEGACY_IDENTITY
  if [ ! -f "$BOOTSTRAP_GENERATION/.prepared" ]; then
    mkdir -m 0700 "$BOOTSTRAP_GENERATION"
    mkdir -p "$BOOTSTRAP_GENERATION/bin" "$BOOTSTRAP_GENERATION/tools"
    SOURCE_ARCHIVE={rendered_source_archive}
    SOURCE_ARCHIVE_SHA256={rendered_source_archive_sha256}
    if [ -z "$SOURCE_ARCHIVE_SHA256" ]; then
      echo "relay-only reconcile requires a verified source archive digest" >&2
      return 1
    fi
    echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -
    bootstrap_safe_extract \
      "$BOOTSTRAP_PLAN_PROVIDER" "$SOURCE_ARCHIVE" "$BOOTSTRAP_GENERATION/source"

    DEST="$BOOTSTRAP_GENERATION/source"
    if [ "$BOOTSTRAP_PLAN_MODE" = "component-upgrade" ]; then
      STAGED_JARVIS_VENV="$BOOTSTRAP_GENERATION/jarvis-venv"
      "$HOME/.local/bin/uv" venv --python 3.12 --seed "$STAGED_JARVIS_VENV"
      "$STAGED_JARVIS_VENV/bin/python" -m pip install --isolated \
        --index-url https://pypi.org/simple \
        -r "$HOME/.local/src/jarvis-util/requirements.txt"
      "$STAGED_JARVIS_VENV/bin/python" -m pip install --isolated --no-deps \
        "$HOME/.local/src/jarvis-util"

      mkdir -m 0700 "$BOOTSTRAP_GENERATION/artifacts"
      JARVIS_CD_STAGING="$(mktemp "${{JARVIS_CD_WHEEL}}.XXXXXX")"
      bootstrap_fetch_exact_artifact \\
        "{JARVIS_CD_WHEEL_URL}" "{JARVIS_CD_WHEEL_SHA256}" "$JARVIS_CD_STAGING"
      mv "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL"
      echo "{JARVIS_CD_WHEEL_SHA256} *$JARVIS_CD_WHEEL" | \
        sha256sum --check --strict -
      BOOTSTRAP_JARVIS_CD_DOWNLOAD_COUNT=1
      "$STAGED_JARVIS_VENV/bin/python" -m pip install --isolated \
        --index-url https://pypi.org/simple "$JARVIS_CD_WHEEL"
      JARVIS_VERSION_PROBE='from importlib.metadata import version; '
      JARVIS_VERSION_PROBE+='assert version("jarvis-cd") == "{JARVIS_CD_VERSION}"'
      "$ACTIVE_JARVIS_PYTHON" -c "$JARVIS_VERSION_PROBE"

      if [ "$JARVIS_MCP_INSTALL_SPEC" != "{CLIO_KIT_JARVIS_MCP_WHEEL_URL}" ]; then
        echo "staged component upgrade requires the released clio-kit wheel URL" >&2
        return 1
      fi
      curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
        --retry 3 --retry-all-errors --retry-max-time 180 \
        --connect-timeout 20 --max-time 180 \
        --output "$JARVIS_MCP_ARTIFACT_PATH" "$JARVIS_MCP_INSTALL_SPEC"
      echo "$JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH" | \
        sha256sum --check --strict -
      BOOTSTRAP_CLIO_KIT_DOWNLOAD_COUNT=1
      UV_TOOL_DIR="$BOOTSTRAP_GENERATION/tools" \
      UV_TOOL_BIN_DIR="$BOOTSTRAP_GENERATION/bin" \
        "$HOME/.local/bin/uv" tool install --force --python 3.12 --no-config \
          --default-index https://pypi.org/simple "$JARVIS_MCP_ARTIFACT_PATH"
      test -x "$CLIO_KIT_EXECUTABLE"
      CLIO_KIT_PROVIDER_PYTHON="$BOOTSTRAP_GENERATION/tools/clio-kit/bin/python"
      test -x "$CLIO_KIT_PROVIDER_PYTHON"
      test "$("$CLIO_KIT_PROVIDER_PYTHON" -c \
        'from importlib.metadata import version; print(version("clio-kit"))')" = \
        "{CLIO_KIT_JARVIS_MCP_VERSION}"
      "$CLIO_KIT_EXECUTABLE" --help >/dev/null
    fi
    RELAY_INSTALL_SPEC={rendered_relay_install_spec}
    RELAY_ARTIFACT_SHA256={rendered_relay_artifact_sha256}
    RELAY_INSTALL_TARGET="$RELAY_INSTALL_SPEC"
    RELAY_ARTIFACT_PATH=""
    if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ]; then
      RELAY_ARTIFACT_PATH="$REUSED_RELAY_ARTIFACT"
      RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
    else
      case "$RELAY_INSTALL_SPEC" in
        clio-relay==*)
          DOWNLOAD_DIR="$DEST/downloaded-wheels"
          mkdir -p "$DOWNLOAD_DIR"
          "$LEGACY_JARVIS_PYTHON" -m pip download --isolated \
            --disable-pip-version-check --no-cache-dir --index-url https://pypi.org/simple \
            --no-deps --only-binary=:all: --dest "$DOWNLOAD_DIR" "$RELAY_INSTALL_SPEC"
          mapfile -t RELAY_WHEELS < <(
            find "$DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_relay-*.whl' -print
          )
          if [ "${{#RELAY_WHEELS[@]}}" -ne 1 ]; then
            echo "expected exactly one downloaded clio-relay wheel" >&2
            return 1
          fi
          RELAY_ARTIFACT_PATH="${{RELAY_WHEELS[0]}}"
          RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
          BOOTSTRAP_RELAY_DOWNLOAD_COUNT=1
          if [ -z "$RELAY_ARTIFACT_SHA256" ]; then
            RELAY_VERSION="${{RELAY_INSTALL_SPEC#clio-relay==}}"
            RELAY_ARTIFACT_SHA256="$(
              "$LEGACY_JARVIS_PYTHON" - \
                "$RELAY_VERSION" "$(basename "$RELAY_ARTIFACT_PATH")" \
                <<'__CLIO_RELAY_RECONCILE_PYPI_DIGEST__'
import json
import re
import sys
from urllib.parse import quote
from urllib.request import urlopen

version, filename = sys.argv[1:]
with urlopen(
    f"https://pypi.org/pypi/clio-relay/{{quote(version, safe='')}}/json",
    timeout=30,
) as response:
    content = response.read(4 * 1024 * 1024 + 1)
if len(content) > 4 * 1024 * 1024:
    raise SystemExit("PyPI clio-relay metadata exceeds the bounded response size")
document = json.loads(content)
matches = [
    item
    for item in document.get("urls", [])
    if item.get("filename") == filename and item.get("packagetype") == "bdist_wheel"
]
if len(matches) != 1:
    raise SystemExit("PyPI did not return one exact clio-relay wheel identity")
digest = matches[0].get("digests", {{}}).get("sha256")
if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{{64}}", digest) is None:
    raise SystemExit("PyPI clio-relay wheel identity omitted a valid SHA-256")
print(digest)
__CLIO_RELAY_RECONCILE_PYPI_DIGEST__
            )"
          fi
          ;;
        *.whl)
          RELAY_ARTIFACT_PATH="$RELAY_INSTALL_SPEC"
          ;;
      esac
    fi
    if [ -n "$RELAY_ARTIFACT_PATH" ]; then
      test -n "$RELAY_ARTIFACT_SHA256"
      echo "$RELAY_ARTIFACT_SHA256 *$RELAY_ARTIFACT_PATH" | \
        sha256sum --check --strict -
    fi
    UV_TOOL_DIR="$BOOTSTRAP_GENERATION/tools" \
    UV_TOOL_BIN_DIR="$BOOTSTRAP_GENERATION/bin" \
      "$HOME/.local/bin/uv" tool install --force --python 3.12 --no-config \
        --default-index https://pypi.org/simple --with "$JARVIS_CD_WHEEL" \
        "$RELAY_INSTALL_TARGET"
    RELAY_EXECUTABLE="$BOOTSTRAP_GENERATION/bin/clio-relay"
    RELAY_PROVIDER_PYTHON="$BOOTSTRAP_GENERATION/tools/clio-relay/bin/python"
    test -x "$RELAY_EXECUTABLE" -a -x "$RELAY_PROVIDER_PYTHON"
    if [ "$BOOTSTRAP_PLAN_MODE" = "component-upgrade" ]; then
      "$HOME/.local/bin/uv" pip install --python "$ACTIVE_JARVIS_PYTHON" \
        --default-index https://pypi.org/simple \
        --refresh-package clio-relay "$RELAY_INSTALL_TARGET"
    fi
    bootstrap_candidate_action jarvis-wrapper \
      "$BOOTSTRAP_GENERATION/bin/jarvis" "$ACTIVE_JARVIS_PYTHON"
    ACTIVE_JARVIS_EXECUTABLE="$ACTIVE_JARVIS_VENV/bin/jarvis"
    BOOTSTRAP_ACTIVE_IDENTITY="$(
      bootstrap_candidate_action execution-boundary \
        "$ACTIVE_JARVIS_VENV" "$ACTIVE_JARVIS_PYTHON" "$ACTIVE_JARVIS_EXECUTABLE"
    )"
    export BOOTSTRAP_ACTIVE_IDENTITY
    if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ]; then
      ln -s "$CLIO_KIT_EXECUTABLE" "$BOOTSTRAP_GENERATION/bin/clio-kit"
    fi
    JARVIS_PACKAGE_PROBE='import clio_relay, jarvis_cd; '
    JARVIS_PACKAGE_PROBE+='import clio_relay.bounded_command.pkg; '
    JARVIS_PACKAGE_PROBE+='import clio_relay.mcp_call.pkg; '
    JARVIS_PACKAGE_PROBE+='import clio_relay.remote_agent.pkg'
    "$RELAY_PROVIDER_PYTHON" -I -c "$JARVIS_PACKAGE_PROBE"
    "$ACTIVE_JARVIS_PYTHON" -I -c "$JARVIS_PACKAGE_PROBE"

"""
