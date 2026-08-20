"""Bounded systemd user-service activation: the bash observer template.

Renders the deterministic, bounded bash helper that a remote deploy script
sources to start/restart a user-level systemd endpoint unit and observe it
reach a terminal state (active, failed, or a bounded unverified timeout)
without ever silently double-enqueuing a systemd job. Split from
``deployment.py`` (clio-relay#231): this module owns the activation-timing
constants and the single template-rendering function together, since the
function's defaults are these constants and nothing else in the deployment
surface reads either.
"""

from __future__ import annotations

from clio_relay.errors import RelayError

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
