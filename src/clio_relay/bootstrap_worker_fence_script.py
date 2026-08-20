"""Managed worker fence/upgrade shell rendering for the Linux cluster bootstrap.

Split from bootstrap.py (clio-relay#255). `_worker_upgrade_fence_script` renders the
fence/recheck/restart shell fragments render_linux_user_bootstrap_script stitches around
the managed systemd worker unit during an upgrade; `_worker_writer_proof_shell` is its
bounded legacy-writer proof helper. Pure string assembly, not independently
monkeypatched.
"""

from __future__ import annotations

import shlex

from clio_relay.bootstrap_worker_proof_source import (
    _WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON,
    _WORKER_WRITER_PROOF_PYTHON,
)
from clio_relay.deployment import (
    endpoint_user_service_name,
    render_bounded_user_service_activation_helper,
)
from clio_relay.worker_lifetime_lock import WORKER_LIFETIME_LOCK_NAME


def _worker_writer_proof_shell(*, rendered_core_dir: str, success_variable: str) -> str:
    """Render one bounded legacy-writer proof against the configured core."""
    return "\n".join(
        [
            (
                'if ! python3 - "$WORKER_CLUSTER_NAME" '
                f"{rendered_core_dir} /proc <<'__CLIO_RELAY_WORKER_WRITER_PROOF__'"
            ),
            _WORKER_WRITER_PROOF_PYTHON.rstrip(),
            "__CLIO_RELAY_WORKER_WRITER_PROOF__",
            "then",
            ('  echo "cannot prove exclusive relay writer ownership for $WORKER_CLUSTER_NAME" >&2'),
            "  exit 1",
            "fi",
            f"{success_variable}=1",
        ]
    )


def _worker_upgrade_fence_script(
    cluster: str | None,
    *,
    rendered_core_dir: str,
    activation_observation_timeout_seconds: int | None = None,
    activation_poll_seconds: int | None = None,
    activation_progress_seconds: int | None = None,
) -> tuple[str, str, str, str]:
    """Render managed fencing, recheck, migration command, and restart step."""
    service_name = endpoint_user_service_name(cluster) if cluster is not None else ""
    declarations = "\n".join(
        [
            f"WORKER_SERVICE_NAME={shlex.quote(service_name)}",
            f"WORKER_CLUSTER_NAME={shlex.quote(cluster or '')}",
            "WORKER_WAS_ACTIVE=0",
            "WORKER_STOP_CONFIRMED=0",
            "WORKER_WRITER_PROOF=0",
            "WORKER_WRITER_RECHECK=0",
            "WORKER_LIFETIME_EXCLUSIVE=0",
            "WORKER_LIFETIME_GUARD_FD=",
            "WORKER_LIFETIME_LOCK_PATH=",
            "WORKER_RESTART_ATTEMPTED=0",
            "WORKER_RESTARTED=0",
            "WORKER_POST_START_STATE=unknown",
            "WORKER_POST_START_SUB_STATE=unknown",
            "WORKER_RESTART_OUTCOME=not-attempted",
        ]
    )
    if not service_name:
        no_service_fence = "\n".join(
            [
                declarations,
                "bootstrap_restore_fenced_worker_on_failure() { :; }",
                "bootstrap_release_worker_lifetime_guard() { :; }",
            ]
        )
        return no_service_fence, "", "clio-relay init", ""
    initial_proof = _worker_writer_proof_shell(
        rendered_core_dir=rendered_core_dir,
        success_variable="WORKER_WRITER_PROOF",
    )
    inherited_fd_check = "\n".join(
        [
            (
                f'python3 - {rendered_core_dir} "$WORKER_LIFETIME_GUARD_FD" '
                f"{shlex.quote(WORKER_LIFETIME_LOCK_NAME)} "
                "<<'__CLIO_RELAY_WORKER_LIFETIME_FD__'"
            ),
            _WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON.rstrip(),
            "__CLIO_RELAY_WORKER_LIFETIME_FD__",
        ]
    )
    recheck = "\n".join(
        [
            "bootstrap_require_worker_lifetime_guard",
            _worker_writer_proof_shell(
                rendered_core_dir=rendered_core_dir,
                success_variable="WORKER_WRITER_RECHECK",
            ),
        ]
    )
    fence = "\n".join(
        [
            declarations,
            f"CLIO_RELAY_ENDPOINT_SERVICE_NAME={shlex.quote(service_name)}",
            "CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION=start",
            render_bounded_user_service_activation_helper(
                observation_timeout_seconds=activation_observation_timeout_seconds,
                poll_seconds=activation_poll_seconds,
                progress_seconds=activation_progress_seconds,
            ),
            "bootstrap_release_worker_lifetime_guard() {",
            '  case "$WORKER_LIFETIME_GUARD_FD" in',
            "    '') return 0 ;;",
            "    8)",
            "      WORKER_LIFETIME_GUARD_FD=",
            "      exec 8>&-",
            "      ;;",
            "    *)",
            (
                '      echo "refusing to release unexpected worker lifetime guard fd: '
                '$WORKER_LIFETIME_GUARD_FD" >&2'
            ),
            "      return 1",
            "      ;;",
            "  esac",
            "}",
            "bootstrap_bounded_worker_restart() {",
            "  WORKER_RESTART_ATTEMPTED=1",
            "  bootstrap_release_worker_lifetime_guard || return 1",
            "  if ! clio_relay_endpoint_activate_bounded; then",
            '    WORKER_POST_START_STATE="$CLIO_RELAY_ENDPOINT_ACTIVE_STATE"',
            '    WORKER_POST_START_SUB_STATE="$CLIO_RELAY_ENDPOINT_SUB_STATE"',
            '    WORKER_RESTART_OUTCOME="$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"',
            "    return 1",
            "  fi",
            '  WORKER_POST_START_STATE="$CLIO_RELAY_ENDPOINT_ACTIVE_STATE"',
            '  WORKER_POST_START_SUB_STATE="$CLIO_RELAY_ENDPOINT_SUB_STATE"',
            '  WORKER_RESTART_OUTCOME="$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"',
            "  WORKER_RESTARTED=1",
            "}",
            "bootstrap_restore_fenced_worker_on_failure() {",
            '  local status="$1"',
            (
                '  if [ "$status" -ne 0 ] && [ "$WORKER_WAS_ACTIVE" = "1" ]'
                ' && [ "$WORKER_RESTARTED" != "1" ]; then'
            ),
            '    if [ "$WORKER_STOP_CONFIRMED" = "1" ]; then',
            '      if [ "$WORKER_RESTART_ATTEMPTED" = "1" ]; then',
            (
                '        echo "bootstrap failed after the worker start was already '
                'enqueued; observing $WORKER_SERVICE_NAME without a duplicate start" >&2'
            ),
            "      else",
            (
                '        echo "bootstrap failed; attempting bounded recovery of '
                '$WORKER_SERVICE_NAME" >&2'
            ),
            "      fi",
            "      if bootstrap_bounded_worker_restart; then",
            (
                '        echo "bootstrap worker_recovery=restored '
                'service=$WORKER_SERVICE_NAME state=active" >&2'
            ),
            "      else",
            '        case "$WORKER_RESTART_OUTCOME" in',
            "          in-progress)",
            (
                '            echo "bootstrap worker_recovery=in-progress '
                "service=$WORKER_SERVICE_NAME state=$WORKER_POST_START_STATE "
                "sub_state=$WORKER_POST_START_SUB_STATE; systemd start job retained "
                'without a duplicate request" >&2'
            ),
            "            ;;",
            "          failed)",
            (
                '            echo "bootstrap worker_recovery=failed '
                "service=$WORKER_SERVICE_NAME state=$WORKER_POST_START_STATE "
                "sub_state=$WORKER_POST_START_SUB_STATE; "
                'operator action is required" >&2'
            ),
            "            ;;",
            "          *)",
            (
                '            echo "bootstrap worker_recovery=unverified '
                "service=$WORKER_SERVICE_NAME state=$WORKER_POST_START_STATE "
                "sub_state=$WORKER_POST_START_SUB_STATE "
                "outcome=$WORKER_RESTART_OUTCOME; "
                'operator verification is required" >&2'
            ),
            "            ;;",
            "        esac",
            "      fi",
            "    else",
            (
                '      echo "bootstrap failed while fencing $WORKER_SERVICE_NAME; '
                'worker state is unknown and requires operator verification" >&2'
            ),
            "    fi",
            "  fi",
            "}",
            "bootstrap_worker_fence_exit() {",
            "  status=$?",
            "  trap - EXIT",
            '  bootstrap_restore_fenced_worker_on_failure "$status"',
            "  bootstrap_release_worker_lifetime_guard || true",
            "  if declare -F bootstrap_cleanup_preparing_root >/dev/null; then",
            "    bootstrap_cleanup_preparing_root || true",
            "  fi",
            '  exit "$status"',
            "}",
            "trap bootstrap_worker_fence_exit EXIT",
            "command -v systemctl >/dev/null 2>&1 || {",
            '  echo "systemctl is required to fence the configured relay worker" >&2',
            "  exit 1",
            "}",
            "command -v timeout >/dev/null 2>&1 || {",
            '  echo "timeout is required to bound relay worker recovery" >&2',
            "  exit 1",
            "}",
            (
                'if ! WORKER_LOAD_STATE="$(systemctl --user show "$WORKER_SERVICE_NAME" '
                '--property=LoadState --value)"; then'
            ),
            '  echo "cannot inspect relay worker unit: $WORKER_SERVICE_NAME" >&2',
            "  exit 1",
            "fi",
            (
                'if ! WORKER_ACTIVE_STATE="$(systemctl --user show "$WORKER_SERVICE_NAME" '
                '--property=ActiveState --value)"; then'
            ),
            '  echo "cannot inspect relay worker state: $WORKER_SERVICE_NAME" >&2',
            "  exit 1",
            "fi",
            'case "$WORKER_LOAD_STATE:$WORKER_ACTIVE_STATE" in',
            "  loaded:active|loaded:activating|loaded:reloading|loaded:deactivating)",
            "    WORKER_WAS_ACTIVE=1",
            '    systemctl --user stop "$WORKER_SERVICE_NAME"',
            (
                '    if ! WORKER_POST_STOP_STATE="$(systemctl --user show '
                '"$WORKER_SERVICE_NAME" --property=ActiveState --value)"; then'
            ),
            '      echo "cannot verify stopped relay worker: $WORKER_SERVICE_NAME" >&2',
            "      exit 1",
            "    fi",
            '    case "$WORKER_POST_STOP_STATE" in',
            "      inactive|failed) WORKER_STOP_CONFIRMED=1 ;;",
            "      *)",
            (
                '        echo "relay worker stop has unknown state '
                '$WORKER_POST_STOP_STATE: $WORKER_SERVICE_NAME" >&2'
            ),
            "        exit 1",
            "        ;;",
            "    esac",
            "    ;;",
            "  loaded:inactive|loaded:failed|masked:inactive|not-found:inactive) ;;",
            "  *)",
            (
                '    echo "refusing bootstrap with unknown relay worker state '
                '$WORKER_LOAD_STATE:$WORKER_ACTIVE_STATE: $WORKER_SERVICE_NAME" >&2'
            ),
            "    exit 1",
            "    ;;",
            "esac",
            initial_proof,
            f"mkdir -p -- {rendered_core_dir}",
            (
                f"WORKER_LIFETIME_LOCK_PATH={rendered_core_dir}/"
                f"{shlex.quote(WORKER_LIFETIME_LOCK_NAME)}"
            ),
            'exec 8<>"$WORKER_LIFETIME_LOCK_PATH"',
            "WORKER_LIFETIME_GUARD_FD=8",
            "bootstrap_require_worker_lifetime_guard() {",
            inherited_fd_check,
            "}",
            "bootstrap_require_worker_lifetime_guard",
            "WORKER_LIFETIME_EXCLUSIVE=1",
        ]
    )
    restart = "\n".join(
        [
            'if [ "$WORKER_WAS_ACTIVE" = "1" ]; then',
            "  bootstrap_require_worker_lifetime_guard",
            "  if ! bootstrap_bounded_worker_restart; then",
            (
                '    echo "relay worker did not become active '
                "state=${WORKER_POST_START_STATE:-unknown} "
                "sub_state=${WORKER_POST_START_SUB_STATE:-unknown} "
                "outcome=${WORKER_RESTART_OUTCOME:-unverified}: "
                '$WORKER_SERVICE_NAME" >&2'
            ),
            "    exit 1",
            "  fi",
            "fi",
        ]
    )
    return fence, recheck, "clio-relay init --migrate-legacy-output", restart
