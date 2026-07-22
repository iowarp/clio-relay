"""Sudo-less endpoint deployment helpers."""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from math import isfinite
from pathlib import Path
from typing import Literal

from clio_relay.cluster_config import ClusterDefinition, WorkerCapacityPolicy
from clio_relay.errors import RelayError
from clio_relay.identifiers import filesystem_key
from clio_relay.installation import INSTALL_RECEIPT_PATH_ENV
from clio_relay.jarvis_mcp import JARVIS_MCP_COMMAND_ENV, JARVIS_MCP_SPACK_COMMAND_ENV
from clio_relay.remote_values import (
    remote_value_expands_home,
    render_systemd_remote_path,
    render_systemd_remote_value,
)
from clio_relay.worker_concurrency import KindConcurrencyInput, kind_concurrency_metadata

_SYSTEMD_UNQUOTED_ARGUMENT = re.compile(r"[A-Za-z0-9_./:@%+=,{}-]+\Z")
_SYSTEMD_SERVICE_NAME = re.compile(r"clio-relay-worker-[a-z0-9_-]+\.service\Z")

ENDPOINT_SERVICE_SYSTEMD_START_TIMEOUT_SECONDS = 300
ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS = 330
ENDPOINT_SERVICE_START_POLL_SECONDS = 2
ENDPOINT_SERVICE_START_PROGRESS_SECONDS = 15
ENDPOINT_SERVICE_CONTROL_TIMEOUT_SECONDS = 10
ENDPOINT_SERVICE_SSH_SETUP_MARGIN_SECONDS = 90
ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS = float(
    ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS + ENDPOINT_SERVICE_SSH_SETUP_MARGIN_SECONDS
)


def render_bounded_user_service_activation_helper(
    *,
    observation_timeout_seconds: int | None = None,
    poll_seconds: int | None = None,
    progress_seconds: int | None = None,
) -> str:
    """Render one asynchronous, bounded systemd activation observer.

    Callers set ``CLIO_RELAY_ENDPOINT_SERVICE_NAME`` and
    ``CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION`` to ``start`` or ``restart``, then
    invoke ``clio_relay_endpoint_activate_bounded``. A per-service file lock
    serializes the exact state/job preflight and optional enqueue across
    processes. The helper enqueues at most one systemd job. A timed-out observer
    never cancels or duplicates an activation that systemd still reports as in
    progress.
    """
    observation_timeout = (
        ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS
        if observation_timeout_seconds is None
        else observation_timeout_seconds
    )
    selected_poll_seconds = (
        ENDPOINT_SERVICE_START_POLL_SECONDS if poll_seconds is None else poll_seconds
    )
    selected_progress_seconds = (
        ENDPOINT_SERVICE_START_PROGRESS_SECONDS if progress_seconds is None else progress_seconds
    )
    for name, value in (
        ("observation timeout", observation_timeout),
        ("poll interval", selected_poll_seconds),
        ("progress interval", selected_progress_seconds),
    ):
        if type(value) is not int or value < 1:
            raise RelayError(f"endpoint service {name} must be a positive integer")
    if observation_timeout <= selected_poll_seconds:
        raise RelayError("endpoint service observation timeout must exceed its poll interval")

    return f"""CLIO_RELAY_ENDPOINT_ACTIVATION_ATTEMPTED=0
CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=not-attempted
CLIO_RELAY_ENDPOINT_INITIAL_STATE_VERIFIED=0
CLIO_RELAY_ENDPOINT_INITIAL_ACTIVE_STATE=unknown
CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID=unknown
CLIO_RELAY_ENDPOINT_INITIAL_RESULT=unknown
CLIO_RELAY_ENDPOINT_LOAD_STATE=unknown
CLIO_RELAY_ENDPOINT_ACTIVE_STATE=unknown
CLIO_RELAY_ENDPOINT_SUB_STATE=unknown
CLIO_RELAY_ENDPOINT_RESULT=unknown
CLIO_RELAY_ENDPOINT_CONTROL_PID=unknown
CLIO_RELAY_ENDPOINT_EXEC_MAIN_CODE=unknown
CLIO_RELAY_ENDPOINT_EXEC_MAIN_STATUS=unknown
CLIO_RELAY_ENDPOINT_TIMEOUT_START_USEC=unknown
CLIO_RELAY_ENDPOINT_INVOCATION_ID=unknown
CLIO_RELAY_ENDPOINT_JOB_ID=unknown
CLIO_RELAY_ENDPOINT_JOB_TYPE=unknown
CLIO_RELAY_ENDPOINT_JOB_STATE=unknown
CLIO_RELAY_ENDPOINT_JOB_OBSERVED=0
CLIO_RELAY_ENDPOINT_ENQUEUE_CONFIRMED=0
CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED=0
CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED=0
CLIO_RELAY_ENDPOINT_TRACKED_JOB_ID=none
CLIO_RELAY_ENDPOINT_RESTART_JOB_OBSERVED=0
CLIO_RELAY_ENDPOINT_OBSERVED_TRANSITION=0
CLIO_RELAY_ENDPOINT_ELAPSED_SECONDS=0
CLIO_RELAY_ENDPOINT_ALLOW_ENQUEUE_GRACE=0

clio_relay_endpoint_read_activation_state() {{
  CLIO_RELAY_ENDPOINT_LOAD_STATE=unknown
  CLIO_RELAY_ENDPOINT_ACTIVE_STATE=unknown
  CLIO_RELAY_ENDPOINT_SUB_STATE=unknown
  CLIO_RELAY_ENDPOINT_RESULT=unknown
  CLIO_RELAY_ENDPOINT_CONTROL_PID=unknown
  CLIO_RELAY_ENDPOINT_EXEC_MAIN_CODE=unknown
  CLIO_RELAY_ENDPOINT_EXEC_MAIN_STATUS=unknown
  CLIO_RELAY_ENDPOINT_TIMEOUT_START_USEC=unknown
  CLIO_RELAY_ENDPOINT_INVOCATION_ID=unknown
  if ! CLIO_RELAY_ENDPOINT_SHOW_OUTPUT="$(
    timeout --signal=TERM --kill-after=2s 5s \
      systemctl --user show "$CLIO_RELAY_ENDPOINT_SERVICE_NAME" --no-pager \
      --property=LoadState --property=ActiveState --property=SubState \
      --property=Result --property=ControlPID --property=ExecMainCode \
      --property=ExecMainStatus --property=TimeoutStartUSec \
      --property=InvocationID
  )"; then
    return 1
  fi
  while IFS='=' read -r key value; do
    case "$key" in
      LoadState) CLIO_RELAY_ENDPOINT_LOAD_STATE="$value" ;;
      ActiveState) CLIO_RELAY_ENDPOINT_ACTIVE_STATE="$value" ;;
      SubState) CLIO_RELAY_ENDPOINT_SUB_STATE="$value" ;;
      Result) CLIO_RELAY_ENDPOINT_RESULT="$value" ;;
      ControlPID) CLIO_RELAY_ENDPOINT_CONTROL_PID="$value" ;;
      ExecMainCode) CLIO_RELAY_ENDPOINT_EXEC_MAIN_CODE="$value" ;;
      ExecMainStatus) CLIO_RELAY_ENDPOINT_EXEC_MAIN_STATUS="$value" ;;
      TimeoutStartUSec) CLIO_RELAY_ENDPOINT_TIMEOUT_START_USEC="$value" ;;
      InvocationID) CLIO_RELAY_ENDPOINT_INVOCATION_ID="$value" ;;
    esac
  done < <(printf '%s\n' "$CLIO_RELAY_ENDPOINT_SHOW_OUTPUT")
  [ "$CLIO_RELAY_ENDPOINT_LOAD_STATE" != unknown ] && \
    [ "$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" != unknown ]
}}

clio_relay_endpoint_read_activation_job() {{
  CLIO_RELAY_ENDPOINT_JOB_ID=unknown
  CLIO_RELAY_ENDPOINT_JOB_TYPE=unknown
  CLIO_RELAY_ENDPOINT_JOB_STATE=unknown
  if ! CLIO_RELAY_ENDPOINT_JOB_OUTPUT="$(
    timeout --signal=TERM --kill-after=2s 5s \
      systemctl --user list-jobs --no-legend --plain --no-pager \
      "$CLIO_RELAY_ENDPOINT_SERVICE_NAME"
  )"; then
    return 1
  fi
  CLIO_RELAY_ENDPOINT_JOB_MATCHES=0
  while read -r job_id unit_name job_type job_state _remaining; do
    [ -n "${{job_id:-}}" ] || continue
    [ "${{unit_name:-}}" = "$CLIO_RELAY_ENDPOINT_SERVICE_NAME" ] || continue
    CLIO_RELAY_ENDPOINT_JOB_MATCHES=$((CLIO_RELAY_ENDPOINT_JOB_MATCHES + 1))
    CLIO_RELAY_ENDPOINT_JOB_ID="$job_id"
    CLIO_RELAY_ENDPOINT_JOB_TYPE="${{job_type:-unknown}}"
    CLIO_RELAY_ENDPOINT_JOB_STATE="${{job_state:-unknown}}"
  done < <(printf '%s\n' "$CLIO_RELAY_ENDPOINT_JOB_OUTPUT")
  if [ "$CLIO_RELAY_ENDPOINT_JOB_MATCHES" -gt 1 ]; then
    CLIO_RELAY_ENDPOINT_JOB_ID=ambiguous
    CLIO_RELAY_ENDPOINT_JOB_TYPE=ambiguous
    CLIO_RELAY_ENDPOINT_JOB_STATE=ambiguous
    return 1
  fi
  if [ "$CLIO_RELAY_ENDPOINT_JOB_MATCHES" = 1 ]; then
    CLIO_RELAY_ENDPOINT_JOB_OBSERVED=1
  else
    CLIO_RELAY_ENDPOINT_JOB_ID=none
    CLIO_RELAY_ENDPOINT_JOB_TYPE=none
    CLIO_RELAY_ENDPOINT_JOB_STATE=none
  fi
  return 0
}}

clio_relay_endpoint_emit_activation_observation() {{
  printf 'endpoint_service.activation service=%s action=%s elapsed_seconds=%s '\
'load_state=%s active_state=%s sub_state=%s result=%s control_pid=%s '\
'exec_main_code=%s exec_main_status=%s timeout_start_usec=%s invocation_id=%s '\
'job_id=%s job_type=%s job_state=%s outcome=%s\n' \
    "$CLIO_RELAY_ENDPOINT_SERVICE_NAME" \
    "$CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION" \
    "$CLIO_RELAY_ENDPOINT_ELAPSED_SECONDS" \
    "$CLIO_RELAY_ENDPOINT_LOAD_STATE" \
    "$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" \
    "$CLIO_RELAY_ENDPOINT_SUB_STATE" \
    "$CLIO_RELAY_ENDPOINT_RESULT" \
    "$CLIO_RELAY_ENDPOINT_CONTROL_PID" \
    "$CLIO_RELAY_ENDPOINT_EXEC_MAIN_CODE" \
    "$CLIO_RELAY_ENDPOINT_EXEC_MAIN_STATUS" \
    "$CLIO_RELAY_ENDPOINT_TIMEOUT_START_USEC" \
    "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" \
    "$CLIO_RELAY_ENDPOINT_JOB_ID" \
    "$CLIO_RELAY_ENDPOINT_JOB_TYPE" \
    "$CLIO_RELAY_ENDPOINT_JOB_STATE" \
    "$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"
}}

clio_relay_endpoint_emit_activation_failure() {{
  clio_relay_endpoint_emit_activation_observation >&2
  printf 'endpoint_service.activation.operator_hint=journalctl --user '\
'--unit=%s --lines=50 --no-pager\n' \
    "$CLIO_RELAY_ENDPOINT_SERVICE_NAME" >&2
}}

clio_relay_endpoint_reject_activation_provenance() {{
  CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED=0
  CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED=1
}}

clio_relay_endpoint_activation_job_is_compatible() {{
  [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] || return 1
  if [ "$CLIO_RELAY_ENDPOINT_TRACKED_JOB_ID" != none ] && \
     [ "$CLIO_RELAY_ENDPOINT_JOB_ID" != \
       "$CLIO_RELAY_ENDPOINT_TRACKED_JOB_ID" ]; then
    clio_relay_endpoint_reject_activation_provenance
    return 1
  fi
  case "$CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION:$CLIO_RELAY_ENDPOINT_JOB_TYPE" in
    start:start|start:restart) ;;
    restart:restart)
      CLIO_RELAY_ENDPOINT_RESTART_JOB_OBSERVED=1
      ;;
    restart:start)
      if [ "$CLIO_RELAY_ENDPOINT_RESTART_JOB_OBSERVED" != 1 ] && \
         [ "$CLIO_RELAY_ENDPOINT_ENQUEUE_CONFIRMED" != 1 ]; then
        clio_relay_endpoint_reject_activation_provenance
        return 1
      fi
      ;;
    *)
      clio_relay_endpoint_reject_activation_provenance
      return 1
      ;;
  esac
  if [ "$CLIO_RELAY_ENDPOINT_TRACKED_JOB_ID" = none ]; then
    CLIO_RELAY_ENDPOINT_TRACKED_JOB_ID="$CLIO_RELAY_ENDPOINT_JOB_ID"
  fi
  CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED=1
  return 0
}}

clio_relay_endpoint_activation_is_complete() {{
  [ "$CLIO_RELAY_ENDPOINT_LOAD_STATE" = loaded ] || return 1
  [ "$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" = active ] || return 1
  [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED" = 1 ] || return 1
  [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] || return 1
  if [ "$CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION" != restart ]; then
    return 0
  fi
  [ "$CLIO_RELAY_ENDPOINT_INITIAL_STATE_VERIFIED" = 1 ] || return 1
  if [ "$CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID" != unknown ] && \
     [ -n "$CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID" ] && \
     [ "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" != unknown ] && \
     [ -n "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" ]; then
    [ "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" != \
      "$CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID" ]
    return
  fi
  [ "$CLIO_RELAY_ENDPOINT_OBSERVED_TRANSITION" = 1 ]
}}

clio_relay_endpoint_inactive_is_terminal() {{
  [ "$CLIO_RELAY_ENDPOINT_JOB_OBSERVED" = 1 ] && return 0
  [ "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" != unknown ] && \
    [ -n "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" ] && \
    [ "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" != \
      "$CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID" ]
}}

clio_relay_endpoint_failed_is_terminal() {{
  [ "$CLIO_RELAY_ENDPOINT_JOB_OBSERVED" = 1 ] && return 0
  if [ "$CLIO_RELAY_ENDPOINT_RESULT" != \
       "$CLIO_RELAY_ENDPOINT_INITIAL_RESULT" ] && \
     [ "$CLIO_RELAY_ENDPOINT_RESULT" != unknown ]; then
    return 0
  fi
  [ "$CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID" != unknown ] && \
    [ -n "$CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID" ] && \
    [ "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" != unknown ] && \
    [ -n "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" ] && \
    [ "$CLIO_RELAY_ENDPOINT_INVOCATION_ID" != \
      "$CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID" ]
}}

clio_relay_endpoint_update_observed_transition() {{
  [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED" = 1 ] || return 0
  [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] || return 0
  if [ "$CLIO_RELAY_ENDPOINT_LOAD_STATE" = loaded ] && \
     [ "$CLIO_RELAY_ENDPOINT_INITIAL_ACTIVE_STATE" != active ] && \
     [ "$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" = active ]; then
    CLIO_RELAY_ENDPOINT_OBSERVED_TRANSITION=1
    return 0
  fi
  case "$CLIO_RELAY_ENDPOINT_LOAD_STATE:$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" in
    loaded:activating|loaded:deactivating|loaded:reloading|loaded:maintenance|loaded:refreshing|loaded:inactive|loaded:failed)
      CLIO_RELAY_ENDPOINT_OBSERVED_TRANSITION=1
      ;;
  esac
}}

clio_relay_endpoint_classify_activation_snapshot() {{
  CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=0
  CLIO_RELAY_ENDPOINT_SNAPSHOT_SUCCESS=0
  CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=unverified
  if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" = ambiguous ]; then
    clio_relay_endpoint_reject_activation_provenance
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
    CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=1
    return 0
  fi
  if [ "$CLIO_RELAY_ENDPOINT_JOB_READ" = 0 ] && \
     [ "$CLIO_RELAY_ENDPOINT_JOB_ID" != none ] && \
     ! clio_relay_endpoint_activation_job_is_compatible; then
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
    CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=1
    return 0
  fi
  if [ "$CLIO_RELAY_ENDPOINT_STATE_READ" != 0 ]; then
    return 0
  fi
  if [ "$CLIO_RELAY_ENDPOINT_JOB_READ" != 0 ]; then
    case "$CLIO_RELAY_ENDPOINT_LOAD_STATE:$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" in
      loaded:activating|loaded:deactivating|loaded:reloading|loaded:maintenance|loaded:refreshing)
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
        ;;
      masked:*|not-found:*|bad-setting:*|error:*)
        clio_relay_endpoint_reject_activation_provenance
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=failed
        CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=1
        ;;
    esac
    return 0
  fi
  clio_relay_endpoint_update_observed_transition
  if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" = none ] && \
     clio_relay_endpoint_activation_is_complete; then
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=active
    CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=1
    CLIO_RELAY_ENDPOINT_SNAPSHOT_SUCCESS=1
    return 0
  fi
  case "$CLIO_RELAY_ENDPOINT_LOAD_STATE:$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" in
    masked:*|not-found:*|bad-setting:*|error:*)
      clio_relay_endpoint_reject_activation_provenance
      CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=failed
      CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=1
      ;;
    loaded:activating|loaded:deactivating|loaded:reloading|loaded:maintenance|loaded:refreshing)
      CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      ;;
    loaded:inactive)
      if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" != none ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      elif [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] && \
           clio_relay_endpoint_inactive_is_terminal; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=failed
        CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=1
      elif [ "$CLIO_RELAY_ENDPOINT_ALLOW_ENQUEUE_GRACE" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_ENQUEUE_CONFIRMED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] && \
           [ "$CLIO_RELAY_ENDPOINT_JOB_OBSERVED" = 0 ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      fi
      ;;
    loaded:failed)
      if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" != none ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      elif [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] && \
           clio_relay_endpoint_failed_is_terminal; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=failed
        CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL=1
      elif [ "$CLIO_RELAY_ENDPOINT_ALLOW_ENQUEUE_GRACE" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_ENQUEUE_CONFIRMED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] && \
           [ "$CLIO_RELAY_ENDPOINT_JOB_OBSERVED" = 0 ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      fi
      ;;
    loaded:active)
      if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" != none ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      elif [ "$CLIO_RELAY_ENDPOINT_ALLOW_ENQUEUE_GRACE" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_ENQUEUE_CONFIRMED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED" = 1 ] && \
           [ "$CLIO_RELAY_ENDPOINT_PROVENANCE_REJECTED" = 0 ] && \
           [ "$CLIO_RELAY_ENDPOINT_JOB_OBSERVED" = 0 ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      fi
      ;;
    *)
      if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" != none ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      fi
      ;;
  esac
}}

clio_relay_endpoint_wait_for_active() {{
  CLIO_RELAY_ENDPOINT_WAIT_STARTED=$SECONDS
  CLIO_RELAY_ENDPOINT_NEXT_PROGRESS=0
  CLIO_RELAY_ENDPOINT_LAST_SIGNATURE=
  while true; do
    CLIO_RELAY_ENDPOINT_ELAPSED_SECONDS=$((SECONDS - CLIO_RELAY_ENDPOINT_WAIT_STARTED))
    CLIO_RELAY_ENDPOINT_STATE_READ=1
    if clio_relay_endpoint_read_activation_state; then
      CLIO_RELAY_ENDPOINT_STATE_READ=0
    fi
    CLIO_RELAY_ENDPOINT_JOB_READ=1
    if clio_relay_endpoint_read_activation_job; then
      CLIO_RELAY_ENDPOINT_JOB_READ=0
    fi
    CLIO_RELAY_ENDPOINT_ALLOW_ENQUEUE_GRACE=1
    clio_relay_endpoint_classify_activation_snapshot
    if [ "$CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL" = 1 ]; then
      if [ "$CLIO_RELAY_ENDPOINT_SNAPSHOT_SUCCESS" = 1 ]; then
        clio_relay_endpoint_emit_activation_observation
        return 0
      fi
      clio_relay_endpoint_emit_activation_failure
      return 1
    fi
    if [ "$CLIO_RELAY_ENDPOINT_ELAPSED_SECONDS" -ge {observation_timeout} ]; then
      if [ "$CLIO_RELAY_ENDPOINT_STATE_READ" != 0 ]; then
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=unverified
      elif [ "$CLIO_RELAY_ENDPOINT_JOB_READ" != 0 ]; then
        case "$CLIO_RELAY_ENDPOINT_LOAD_STATE:$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" in
          loaded:activating|loaded:deactivating|loaded:reloading|loaded:maintenance|loaded:refreshing)
            CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
            ;;
          *)
            CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=unverified
            ;;
        esac
      elif [ "$CLIO_RELAY_ENDPOINT_JOB_ID" = none ]; then
        case "$CLIO_RELAY_ENDPOINT_LOAD_STATE:$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" in
          loaded:activating|loaded:deactivating|loaded:reloading|loaded:maintenance|loaded:refreshing)
            CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
            ;;
          *)
            CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=unverified
            ;;
        esac
      fi
      clio_relay_endpoint_emit_activation_failure
      return 1
    fi
    CLIO_RELAY_ENDPOINT_SIGNATURE="$CLIO_RELAY_ENDPOINT_LOAD_STATE:"\
"$CLIO_RELAY_ENDPOINT_ACTIVE_STATE:$CLIO_RELAY_ENDPOINT_SUB_STATE:"\
"$CLIO_RELAY_ENDPOINT_RESULT:$CLIO_RELAY_ENDPOINT_INVOCATION_ID:"\
"$CLIO_RELAY_ENDPOINT_JOB_ID:$CLIO_RELAY_ENDPOINT_JOB_TYPE:"\
"$CLIO_RELAY_ENDPOINT_JOB_STATE:$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"
    if [ "$CLIO_RELAY_ENDPOINT_SIGNATURE" != \
         "$CLIO_RELAY_ENDPOINT_LAST_SIGNATURE" ] || \
       [ "$CLIO_RELAY_ENDPOINT_ELAPSED_SECONDS" -ge \
         "$CLIO_RELAY_ENDPOINT_NEXT_PROGRESS" ]; then
      clio_relay_endpoint_emit_activation_observation
      CLIO_RELAY_ENDPOINT_LAST_SIGNATURE="$CLIO_RELAY_ENDPOINT_SIGNATURE"
      CLIO_RELAY_ENDPOINT_NEXT_PROGRESS=$((
        CLIO_RELAY_ENDPOINT_ELAPSED_SECONDS + {selected_progress_seconds}
      ))
    fi
    sleep {selected_poll_seconds}
  done
}}

clio_relay_endpoint_activate_bounded() {{
  case "${{CLIO_RELAY_ENDPOINT_SERVICE_NAME:-}}" in
    '') echo "endpoint service name is required" >&2; return 2 ;;
    *[!A-Za-z0-9_.@-]*) echo "endpoint service name is unsafe" >&2; return 2 ;;
  esac
  case "${{CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION:-}}" in
    start|restart) ;;
    *) echo "endpoint service activation action must be start or restart" >&2; return 2 ;;
  esac
  command -v timeout >/dev/null 2>&1 || {{
    echo "timeout is required to bound endpoint service activation" >&2
    return 2
  }}
  command -v systemctl >/dev/null 2>&1 || {{
    echo "systemctl is required to activate the endpoint service" >&2
    return 2
  }}
  command -v flock >/dev/null 2>&1 || {{
    echo "flock is required to serialize endpoint service activation" >&2
    return 2
  }}
  if [ "$CLIO_RELAY_ENDPOINT_ACTIVATION_ATTEMPTED" = 1 ]; then
    CLIO_RELAY_ENDPOINT_STATE_READ=1
    if clio_relay_endpoint_read_activation_state; then
      CLIO_RELAY_ENDPOINT_STATE_READ=0
    fi
    CLIO_RELAY_ENDPOINT_JOB_READ=1
    if clio_relay_endpoint_read_activation_job; then
      CLIO_RELAY_ENDPOINT_JOB_READ=0
    fi
    CLIO_RELAY_ENDPOINT_ALLOW_ENQUEUE_GRACE=0
    clio_relay_endpoint_classify_activation_snapshot
    if [ "$CLIO_RELAY_ENDPOINT_SNAPSHOT_TERMINAL" = 1 ]; then
      if [ "$CLIO_RELAY_ENDPOINT_SNAPSHOT_SUCCESS" = 1 ]; then
        clio_relay_endpoint_emit_activation_observation
        return 0
      fi
      clio_relay_endpoint_emit_activation_failure
      return 1
    fi
    if [ "$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME" = in-progress ]; then
      clio_relay_endpoint_emit_activation_observation
    else
      clio_relay_endpoint_emit_activation_failure
    fi
    return 1
  fi
  CLIO_RELAY_ENDPOINT_ACTIVATION_ATTEMPTED=1
  if [ -n "${{XDG_RUNTIME_DIR:-}}" ]; then
    CLIO_RELAY_ENDPOINT_ACTIVATION_LOCK_DIR="$XDG_RUNTIME_DIR/clio-relay/activation-locks"
  elif [ -n "${{HOME:-}}" ]; then
    CLIO_RELAY_ENDPOINT_ACTIVATION_LOCK_DIR="$HOME/.local/share/clio-relay/activation-locks"
  else
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=preflight-unverified
    clio_relay_endpoint_emit_activation_failure
    return 1
  fi
  CLIO_RELAY_ENDPOINT_ACTIVATION_LOCK_PATH="$CLIO_RELAY_ENDPOINT_ACTIVATION_LOCK_DIR/$CLIO_RELAY_ENDPOINT_SERVICE_NAME.lock"
  if ! (umask 077 && mkdir -p -- "$CLIO_RELAY_ENDPOINT_ACTIVATION_LOCK_DIR") || \
     ! chmod 700 -- "$CLIO_RELAY_ENDPOINT_ACTIVATION_LOCK_DIR"; then
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=preflight-unverified
    clio_relay_endpoint_emit_activation_failure
    return 1
  fi
  if ! exec 7>"$CLIO_RELAY_ENDPOINT_ACTIVATION_LOCK_PATH"; then
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=preflight-unverified
    clio_relay_endpoint_emit_activation_failure
    return 1
  fi
  if ! timeout --signal=TERM --kill-after=2s \
       {ENDPOINT_SERVICE_CONTROL_TIMEOUT_SECONDS}s flock --exclusive 7; then
    exec 7>&-
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=preflight-unverified
    clio_relay_endpoint_emit_activation_failure
    return 1
  fi
  if ! clio_relay_endpoint_read_activation_state; then
    flock --unlock 7 || true
    exec 7>&-
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=preflight-unverified
    clio_relay_endpoint_emit_activation_failure
    return 1
  fi
  CLIO_RELAY_ENDPOINT_INITIAL_STATE_VERIFIED=1
  CLIO_RELAY_ENDPOINT_INITIAL_ACTIVE_STATE="$CLIO_RELAY_ENDPOINT_ACTIVE_STATE"
  CLIO_RELAY_ENDPOINT_INITIAL_INVOCATION_ID="$CLIO_RELAY_ENDPOINT_INVOCATION_ID"
  CLIO_RELAY_ENDPOINT_INITIAL_RESULT="$CLIO_RELAY_ENDPOINT_RESULT"
  if ! clio_relay_endpoint_read_activation_job; then
    if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" = ambiguous ]; then
      clio_relay_endpoint_reject_activation_provenance
    fi
    flock --unlock 7 || true
    exec 7>&-
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=preflight-unverified
    clio_relay_endpoint_emit_activation_failure
    return 1
  fi
  if [ "$CLIO_RELAY_ENDPOINT_LOAD_STATE" != loaded ]; then
    flock --unlock 7 || true
    exec 7>&-
    case "$CLIO_RELAY_ENDPOINT_LOAD_STATE" in
      masked|not-found|bad-setting|error)
        clio_relay_endpoint_reject_activation_provenance
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=failed
        ;;
      *)
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=preflight-unverified
        ;;
    esac
    clio_relay_endpoint_emit_activation_failure
    return 1
  fi
  case "$CLIO_RELAY_ENDPOINT_JOB_ID" in
    none) ;;
    *)
      flock --unlock 7 || true
      exec 7>&-
      CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      if ! clio_relay_endpoint_activation_job_is_compatible; then
        clio_relay_endpoint_emit_activation_failure
        return 1
      fi
      clio_relay_endpoint_emit_activation_observation
      clio_relay_endpoint_wait_for_active
      return
      ;;
  esac
  case "$CLIO_RELAY_ENDPOINT_ACTIVE_STATE" in
    active|inactive|failed) ;;
    *)
      flock --unlock 7 || true
      exec 7>&-
      CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
      clio_relay_endpoint_emit_activation_failure
      return 1
      ;;
  esac
  if timeout --signal=TERM --kill-after=2s \
       {ENDPOINT_SERVICE_CONTROL_TIMEOUT_SECONDS}s \
       systemctl --user "$CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION" --no-block \
       "$CLIO_RELAY_ENDPOINT_SERVICE_NAME"; then
    CLIO_RELAY_ENDPOINT_ENQUEUE_CONFIRMED=1
    CLIO_RELAY_ENDPOINT_PROVENANCE_VERIFIED=1
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
    flock --unlock 7 || true
    exec 7>&-
  else
    CLIO_RELAY_ENDPOINT_ENQUEUE_STATUS=$?
    case "$CLIO_RELAY_ENDPOINT_ENQUEUE_STATUS" in
      124|137) CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=enqueue-unverified ;;
      *) CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=failed ;;
    esac
    CLIO_RELAY_ENDPOINT_POST_ENQUEUE_INVENTORY=verified
    clio_relay_endpoint_read_activation_state || \
      CLIO_RELAY_ENDPOINT_POST_ENQUEUE_INVENTORY=unverified
    if ! clio_relay_endpoint_read_activation_job; then
      CLIO_RELAY_ENDPOINT_POST_ENQUEUE_INVENTORY=unverified
      if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" = ambiguous ]; then
        clio_relay_endpoint_reject_activation_provenance
      fi
    fi
    flock --unlock 7 || true
    exec 7>&-
    if [ "$CLIO_RELAY_ENDPOINT_POST_ENQUEUE_INVENTORY" != verified ]; then
      CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=enqueue-unverified
      clio_relay_endpoint_emit_activation_failure
      return 1
    fi
    case "$CLIO_RELAY_ENDPOINT_LOAD_STATE" in
      masked|not-found|bad-setting|error)
        clio_relay_endpoint_reject_activation_provenance
        CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=failed
        clio_relay_endpoint_emit_activation_failure
        return 1
        ;;
    esac
    if [ "$CLIO_RELAY_ENDPOINT_JOB_ID" = none ]; then
      clio_relay_endpoint_emit_activation_failure
      return 1
    fi
    if ! clio_relay_endpoint_activation_job_is_compatible; then
      clio_relay_endpoint_emit_activation_failure
      return 1
    fi
    CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME=in-progress
  fi
  clio_relay_endpoint_wait_for_active
}}
""".replace("\r\n", "\n")


def render_endpoint_user_service(
    *,
    cluster: str,
    definition: ClusterDefinition,
    relay_bin: str = "%h/.local/bin/clio-relay",
    concurrency: int | None = None,
    control_query_concurrency: int | None = None,
    kind_concurrency: KindConcurrencyInput | None = None,
) -> str:
    """Render a user-level systemd service for a configured worker endpoint."""
    capacity = definition.worker_capacity
    selected_concurrency = capacity.concurrency if concurrency is None else concurrency
    selected_control_concurrency = (
        capacity.control_query_concurrency
        if control_query_concurrency is None
        else control_query_concurrency
    )
    selected_kind_concurrency = (
        capacity.kind_concurrency if kind_concurrency is None else kind_concurrency
    )
    if selected_concurrency < 2:
        raise RelayError("managed worker concurrency must be at least 2")
    if selected_control_concurrency < 1:
        raise RelayError("managed worker control-query concurrency must be at least 1")
    if selected_control_concurrency >= selected_concurrency:
        raise RelayError(
            "managed worker control-query concurrency must be less than total concurrency"
        )
    core_source = definition.core_dir
    spool_source = definition.spool_dir
    jarvis_source = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_source = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_source = _configured_agent_bin(definition)
    spack_source = definition.spack_executable
    core_dir = render_systemd_remote_path(core_source, field="core_dir")
    spool_dir = render_systemd_remote_path(spool_source, field="spool_dir")
    jarvis_bin = render_systemd_remote_value(
        jarvis_source,
        field="jarvis_bin",
    )
    frpc_bin = render_systemd_remote_value(
        frpc_source,
        field="frpc_bin",
    )
    agent_bin = render_systemd_remote_value(agent_source, field="agent_bin")
    agent_args = " ".join(definition.agent_args)
    kind_limits = kind_concurrency_metadata(selected_kind_concurrency)
    jarvis_mcp_line = _optional_environment_line(
        JARVIS_MCP_COMMAND_ENV,
        os.environ.get(JARVIS_MCP_COMMAND_ENV),
    )
    jarvis_mcp_unset_line = "" if jarvis_mcp_line else f"UnsetEnvironment={JARVIS_MCP_COMMAND_ENV}"
    jarvis_mcp_spack_line = _optional_environment_line(
        JARVIS_MCP_SPACK_COMMAND_ENV,
        (
            render_systemd_remote_value(
                spack_source,
                field="spack_executable",
            )
            if spack_source is not None
            else None
        ),
        allow_home_specifier=(spack_source is not None and remote_value_expands_home(spack_source)),
    )
    jarvis_mcp_spack_unset_line = (
        "" if jarvis_mcp_spack_line else f"UnsetEnvironment={JARVIS_MCP_SPACK_COMMAND_ENV}"
    )
    exec_start_arguments = [
        relay_bin,
        "endpoint",
        "start",
        "--role",
        "worker",
        "--cluster",
        cluster,
        "--concurrency",
        str(selected_concurrency),
        "--control-query-concurrency",
        str(selected_control_concurrency),
    ]
    for kind, limit in kind_limits.items():
        exec_start_arguments.extend(["--kind-concurrency", f"{kind}={limit}"])
    exec_start_arguments.extend(["--scheduler-provider", definition.scheduler_provider])
    exec_start = " ".join(
        _systemd_exec_argument(argument, allow_home_specifier=index == 0)
        for index, argument in enumerate(exec_start_arguments)
    )
    exec_start_pre = " ".join(
        _systemd_exec_argument(argument, allow_home_specifier=index == 0)
        for index, argument in enumerate(
            [relay_bin, "queue", "migrate-indexes", "--all", "--batch-size", "500"]
        )
    )
    description_cluster = _systemd_exec_argument(cluster, allow_home_specifier=False)
    core_line = _environment_line(
        "CLIO_RELAY_CORE_DIR",
        core_dir,
        allow_home_specifier=remote_value_expands_home(core_source),
    )
    spool_line = _environment_line(
        "CLIO_RELAY_SPOOL_DIR",
        spool_dir,
        allow_home_specifier=remote_value_expands_home(spool_source),
    )
    jarvis_line = _environment_line(
        "CLIO_RELAY_JARVIS_BIN",
        jarvis_bin,
        allow_home_specifier=remote_value_expands_home(jarvis_source),
    )
    frpc_line = _environment_line(
        "CLIO_RELAY_FRPC_BIN",
        frpc_bin,
        allow_home_specifier=remote_value_expands_home(frpc_source),
    )
    agent_line = _environment_line(
        "CLIO_RELAY_AGENT_BIN",
        agent_bin,
        allow_home_specifier=remote_value_expands_home(agent_source),
    )
    return f"""[Unit]
Description=clio-relay worker endpoint for {description_cluster}
After=network-online.target
# Never strand an enabled endpoint after repeated unexpected exits. Each start
# remains bounded by TimeoutStartSec and retries are paced by RestartSec.
StartLimitIntervalSec=0

[Service]
Type=simple
Environment="PATH=%h/.local/share/clio-relay/current/bin:%h/.local/bin:/usr/local/bin:/usr/bin:/bin"
{core_line}
{spool_line}
{jarvis_line}
{frpc_line}
{agent_line}
{_environment_line("CLIO_RELAY_AGENT_ADAPTER", definition.agent_adapter)}
{_environment_line("CLIO_RELAY_AGENT_ARGS", agent_args)}
Environment="{INSTALL_RECEIPT_PATH_ENV}=%h/.local/share/clio-relay/install-receipt.json"
{jarvis_mcp_line}
{jarvis_mcp_spack_line}
{jarvis_mcp_unset_line}
{jarvis_mcp_spack_unset_line}
ExecStartPre={exec_start_pre}
ExecStart={exec_start}
# Queue-index migration runs in ExecStartPre. Give systemd a finite bound that
# is longer than normal migration, while keeping the external observer bounded
# slightly beyond this deadline for a definitive terminal state.
TimeoutStartSec={ENDPOINT_SERVICE_SYSTEMD_START_TIMEOUT_SECONDS}s
# Keep an enabled persistent endpoint available after clean or failed process
# exits. Explicit systemd stop operations are not restarted by this policy.
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
"""


def install_endpoint_user_service_over_ssh(
    *,
    cluster: str,
    ssh_host: str,
    service_text: str,
    start: bool,
    enable: bool,
    require_persistent: bool = True,
    timeout_seconds: float = ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS,
) -> list[str]:
    """Install a user-level systemd service on a remote cluster without sudo.

    Persistent installs require systemd user lingering so the worker remains
    available after the operator's final login session exits. The caller must
    explicitly opt into a login-scoped service when site policy forbids linger.
    """
    _validate_ssh_destination(ssh_host)
    if not isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("endpoint service SSH timeout must be finite and positive")
    service_name = endpoint_user_service_name(cluster)
    remote_script = _remote_install_script(
        service_name=service_name,
        service_text=service_text,
        start=start,
        enable=enable,
        require_persistent=require_persistent,
    )
    return _run_endpoint_service_script_over_ssh(
        ssh_host=ssh_host,
        remote_script=remote_script,
        timeout_seconds=timeout_seconds,
        operation="installation",
    )


def restart_endpoint_user_service_over_ssh(
    *,
    cluster: str,
    ssh_host: str,
    expected_capacity: WorkerCapacityPolicy,
    require_persistent: bool = True,
    timeout_seconds: float = ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS,
) -> list[str]:
    """Restart an installed endpoint unit after verifying its persisted policy."""
    _validate_ssh_destination(ssh_host)
    if not isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("endpoint service SSH timeout must be finite and positive")
    service_name = endpoint_user_service_name(cluster)
    remote_script = _remote_restart_script(
        service_name=service_name,
        expected_capacity=expected_capacity,
        require_persistent=require_persistent,
    )
    return _run_endpoint_service_script_over_ssh(
        ssh_host=ssh_host,
        remote_script=remote_script,
        timeout_seconds=timeout_seconds,
        operation="restart",
    )


def _run_endpoint_service_script_over_ssh(
    *,
    ssh_host: str,
    remote_script: str,
    timeout_seconds: float,
    operation: Literal["installation", "restart"],
) -> list[str]:
    """Run one bounded endpoint-service operation through SSH."""
    try:
        result = subprocess.run(
            ["ssh", ssh_host, "bash", "-s"],
            input=remote_script.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            f"endpoint service {operation} exceeded {timeout_seconds:g} seconds"
        ) from exc
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")
        stdout = result.stdout.decode("utf-8", errors="replace")
        detail = stderr.strip() or stdout.strip()
        verb = "install" if operation == "installation" else "restart"
        raise RelayError(f"failed to {verb} endpoint user service: {detail}")
    return result.stdout.decode("utf-8", errors="replace").splitlines()


def _remote_install_script(
    *,
    service_name: str,
    service_text: str,
    start: bool,
    enable: bool,
    require_persistent: bool,
) -> str:
    if _SYSTEMD_SERVICE_NAME.fullmatch(service_name) is None:
        raise RelayError(f"unsafe endpoint systemd service name: {service_name!r}")
    service_literal = shlex.quote(service_text)
    persistent_literal = "1" if require_persistent else "0"
    command = "systemctl --user daemon-reload\n"
    if enable:
        command += f"systemctl --user enable {shlex.quote(service_name)}\n"
    if start:
        command += (
            f"CLIO_RELAY_ENDPOINT_SERVICE_NAME={shlex.quote(service_name)}\n"
            "CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION=restart\n"
            + render_bounded_user_service_activation_helper()
            + "\nif ! clio_relay_endpoint_activate_bounded; then\n"
            + (
                '  echo "endpoint service did not become active: '
                f"{service_name} "
                "$CLIO_RELAY_ENDPOINT_ACTIVE_STATE/"
                "$CLIO_RELAY_ENDPOINT_SUB_STATE "
                'outcome=$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME" >&2\n'
            )
            + "  exit 1\n"
            + "fi\n"
        )
    command += (
        'service_enabled="$(systemctl --user is-enabled '
        f'{shlex.quote(service_name)} 2>/dev/null || true)"\n'
        'service_active="$(systemctl --user is-active '
        f'{shlex.quote(service_name)} 2>/dev/null || true)"\n'
    )
    if enable:
        command += (
            'if [ "$service_enabled" != "enabled" ]; then\n'
            f'  echo "endpoint service is not enabled: {service_name} '
            '$service_enabled" >&2\n'
            "  exit 1\n"
            "fi\n"
        )
    if start:
        command += (
            'if [ "$service_active" != "active" ]; then\n'
            f'  echo "endpoint service is not active: {service_name} '
            '$service_active" >&2\n'
            "  exit 1\n"
            "fi\n"
        )
    command += (
        "echo user_systemd=$(systemctl --user is-system-running || true)\n"
        'echo linger="$linger"\n'
        'echo endpoint_service.persistence="$persistence_mode"\n'
        'echo endpoint_service.enabled="${service_enabled:-unknown}"\n'
        'echo endpoint_service.active="${service_active:-unknown}"\n'
        "export SYSTEMD_COLORS=0 LANG=C LC_ALL=C\n"
        f"systemctl --user --no-pager --plain --full status "
        f"{shlex.quote(service_name)} || true\n"
    )
    script = f"""set -euo pipefail
require_persistent={persistent_literal}
relay_user="${{USER:-$(id -un)}}"
linger="$(loginctl show-user "$relay_user" -p Linger --value 2>/dev/null || true)"
if [ "$linger" = "yes" ]; then
  persistence_mode=systemd-user-linger
elif [ "$require_persistent" = "1" ]; then
  echo "persistent endpoint service requires systemd user lingering (Linger=yes)" >&2
  echo "run 'loginctl enable-linger $relay_user' once, or ask the site administrator" >&2
  echo "to enable lingering for this account" >&2
  echo "use --allow-login-scoped only when logout-time shutdown is explicitly acceptable" >&2
  exit 78
else
  persistence_mode=login-scoped
  echo "warning: endpoint service is login-scoped and may stop after the final login exits" >&2
fi
mkdir -p "$HOME/.config/systemd/user"
printf '%s' {service_literal} > "$HOME/.config/systemd/user/{service_name}"
{command}"""
    return script.replace("\r\n", "\n")


def _remote_restart_script(
    *,
    service_name: str,
    expected_capacity: WorkerCapacityPolicy,
    require_persistent: bool,
) -> str:
    """Render a restart-only script that verifies but cannot replace the unit."""
    if _SYSTEMD_SERVICE_NAME.fullmatch(service_name) is None:
        raise RelayError(f"unsafe endpoint systemd service name: {service_name!r}")
    service_literal = shlex.quote(service_name)
    persistent_literal = "1" if require_persistent else "0"
    expected_kind_concurrency = ",".join(
        f"{kind}={limit}"
        for kind, limit in kind_concurrency_metadata(expected_capacity.kind_concurrency).items()
    )
    script = f"""set -euo pipefail
require_persistent={persistent_literal}
expected_concurrency={shlex.quote(str(expected_capacity.concurrency))}
expected_control_query_concurrency={shlex.quote(str(expected_capacity.control_query_concurrency))}
expected_kind_concurrency={shlex.quote(expected_kind_concurrency)}
relay_user="${{USER:-$(id -un)}}"
linger="$(loginctl show-user "$relay_user" -p Linger --value 2>/dev/null || true)"
if [ "$linger" = "yes" ]; then
  persistence_mode=systemd-user-linger
elif [ "$require_persistent" = "1" ]; then
  echo "persistent endpoint service requires systemd user lingering (Linger=yes)" >&2
  echo "run 'loginctl enable-linger $relay_user' once, or ask the site administrator" >&2
  echo "to enable lingering for this account" >&2
  echo "use --allow-login-scoped only when logout-time shutdown is explicitly acceptable" >&2
  exit 78
else
  persistence_mode=login-scoped
  echo "warning: endpoint service is login-scoped and may stop after the final login exits" >&2
fi
export SYSTEMD_COLORS=0 LANG=C LC_ALL=C
service_enabled="$(systemctl --user is-enabled {service_literal} 2>/dev/null || true)"
if [ "$service_enabled" != "enabled" ]; then
  echo "endpoint service is not installed and enabled: {service_name} $service_enabled" >&2
  exit 1
fi
installed_exec_start="$(
  systemctl --user show {service_literal} \
    --property=ExecStart --value --no-pager 2>/dev/null || true
)"
set -f
argv_count=0
in_argv=0
expected_value=""
policy_parse_error=""
observed_concurrency=""
observed_control_query_concurrency=""
observed_kind_concurrency=""
for token in $installed_exec_start; do
  if [ "$in_argv" = "0" ]; then
    case "$token" in
      "argv[]="*)
        argv_count=$((argv_count + 1))
        in_argv=1
        ;;
    esac
    continue
  fi
  if [ "$token" = ";" ]; then
    if [ -n "$expected_value" ]; then
      policy_parse_error="missing value for $expected_value"
      expected_value=""
    fi
    in_argv=0
    continue
  fi
  if [ -n "$expected_value" ]; then
    case "$expected_value" in
      concurrency) observed_concurrency="$token" ;;
      control_query_concurrency) observed_control_query_concurrency="$token" ;;
      kind_concurrency)
        if [ -n "$observed_kind_concurrency" ]; then
          observed_kind_concurrency="$observed_kind_concurrency,$token"
        else
          observed_kind_concurrency="$token"
        fi
        ;;
    esac
    expected_value=""
    continue
  fi
  case "$token" in
    --concurrency)
      if [ -n "$observed_concurrency" ]; then
        policy_parse_error="duplicate --concurrency"
      fi
      expected_value="concurrency"
      ;;
    --control-query-concurrency)
      if [ -n "$observed_control_query_concurrency" ]; then
        policy_parse_error="duplicate --control-query-concurrency"
      fi
      expected_value="control_query_concurrency"
      ;;
    --kind-concurrency)
      expected_value="kind_concurrency"
      ;;
  esac
done
if [ -n "$expected_value" ]; then
  policy_parse_error="missing value for $expected_value"
fi
if [ "$argv_count" -ne 1 ] || [ -n "$policy_parse_error" ] || \
   [ "$observed_concurrency" != "$expected_concurrency" ] || \
   [ "$observed_control_query_concurrency" != "$expected_control_query_concurrency" ] || \
   [ "$observed_kind_concurrency" != "$expected_kind_concurrency" ]; then
  echo "endpoint service capacity policy does not match the persisted cluster policy" >&2
  printf 'expected concurrency=%s control_query_concurrency=%s kind_concurrency=%s\\n' \
    "$expected_concurrency" "$expected_control_query_concurrency" \
    "${{expected_kind_concurrency:-none}}" >&2
  printf 'observed concurrency=%s control_query_concurrency=%s kind_concurrency=%s\\n' \
    "${{observed_concurrency:-missing}}" \
    "${{observed_control_query_concurrency:-missing}}" \
    "${{observed_kind_concurrency:-none}}" >&2
  if [ -n "$policy_parse_error" ]; then
    printf 'policy parse error: %s\\n' "$policy_parse_error" >&2
  fi
  echo "run 'clio-relay cluster install-endpoint-service --cluster <configured-cluster>'" >&2
  echo "to reinstall the managed unit" >&2
  exit 79
fi
CLIO_RELAY_ENDPOINT_SERVICE_NAME={service_literal}
CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION=restart
{render_bounded_user_service_activation_helper()}
if ! clio_relay_endpoint_activate_bounded; then
  echo "endpoint service did not become active after restart: {service_name} \
$CLIO_RELAY_ENDPOINT_ACTIVE_STATE/$CLIO_RELAY_ENDPOINT_SUB_STATE \
outcome=$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME" >&2
  exit 1
fi
service_active="$(systemctl --user is-active {service_literal} 2>/dev/null || true)"
if [ "$service_active" != "active" ]; then
  echo "endpoint service is not active after restart: {service_name} $service_active" >&2
  exit 1
fi
echo user_systemd=$(systemctl --user is-system-running || true)
echo linger="$linger"
echo endpoint_service.persistence="$persistence_mode"
echo endpoint_service.enabled="$service_enabled"
echo endpoint_service.active="$service_active"
echo endpoint_service.unit_rewritten=false
echo endpoint_service.policy_source=installed-unit
echo endpoint_service.policy_validated=true
systemctl --user --no-pager --plain --full status {service_literal} || true
"""
    return script.replace("\r\n", "\n")


def write_endpoint_user_service(path: Path, service_text: str) -> Path:
    """Write a user-level systemd service to a local path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(service_text, encoding="utf-8")
    return path


def _optional_environment_line(
    name: str,
    value: str | None,
    *,
    allow_home_specifier: bool = False,
) -> str:
    if value is None or value == "":
        return ""
    return _environment_line(name, value, allow_home_specifier=allow_home_specifier)


def _environment_line(
    name: str,
    value: str,
    *,
    allow_home_specifier: bool = False,
) -> str:
    """Render one systemd environment assignment without directive injection."""
    if not name or any(
        not (character.isupper() or character.isdigit() or character == "_") for character in name
    ):
        raise RelayError(f"unsafe systemd environment name: {name!r}")
    escaped_value = _systemd_escape(value, allow_home_specifier=allow_home_specifier)
    assignment = f"{name}={escaped_value}"
    return f'Environment="{assignment}"'


def _systemd_exec_argument(value: str, *, allow_home_specifier: bool) -> str:
    """Render one exact systemd command argument."""
    escaped = _systemd_escape(value, allow_home_specifier=allow_home_specifier)
    if _SYSTEMD_UNQUOTED_ARGUMENT.fullmatch(escaped) is not None:
        return escaped
    return f'"{escaped}"'


def _systemd_escape(value: str, *, allow_home_specifier: bool) -> str:
    """Escape one value using systemd.syntax quoted-string rules."""
    if "\x00" in value:
        raise RelayError("systemd values cannot contain NUL")
    if allow_home_specifier and value.startswith("%h"):
        escaped_specifiers = "%h" + value.removeprefix("%h").replace("%", "%%")
    else:
        escaped_specifiers = value.replace("%", "%%")
    rendered: list[str] = []
    for character in escaped_specifiers:
        if character == "\\":
            rendered.append("\\\\")
        elif character == '"':
            rendered.append('\\"')
        elif character == "\n":
            rendered.append("\\n")
        elif character == "\r":
            rendered.append("\\r")
        elif character == "\t":
            rendered.append("\\t")
        elif ord(character) < 32 or ord(character) == 127:
            rendered.append(f"\\x{ord(character):02x}")
        else:
            rendered.append(character)
    return "".join(rendered)


def endpoint_user_service_name(cluster: str) -> str:
    """Map one logical cluster label to its portable deterministic worker unit name."""
    key = filesystem_key(cluster, domain="systemd-cluster")
    return f"clio-relay-worker-{key}.service"


def _validate_ssh_destination(value: str) -> None:
    """Reject destinations that SSH could interpret as options or multiple tokens."""
    if (
        not value
        or value != value.strip()
        or value.startswith("-")
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise RelayError(
            "ssh host must be one non-option destination without whitespace or controls"
        )


def _configured_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"
