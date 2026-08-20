"""Install and restart the worker endpoint's systemd unit over SSH.

Owns the sudo-less remote-deploy operations: the two public entry points
(``install_endpoint_user_service_over_ssh`` /
``restart_endpoint_user_service_over_ssh``), the bounded-subprocess runner
they share, the two remote bash scripts they build (install writes+enables+
starts the unit; restart verifies the installed unit's persisted capacity
policy before restarting it -- it can never rewrite the unit), and the SSH
destination validator that guards both. Split from ``deployment.py``
(clio-relay#231): this is the file's SSH/subprocess-boundary concern, layered
on top of ``deployment_activation`` (the bash observer both remote scripts
embed) and ``deployment_unit`` (the unit name both scripts validate against).
"""

from __future__ import annotations

import re
import shlex
import subprocess
from math import isfinite
from typing import Literal

from clio_relay.cluster_config import WorkerCapacityPolicy
from clio_relay.deployment_activation import (
    ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS,
    render_bounded_user_service_activation_helper,
)
from clio_relay.deployment_unit import endpoint_user_service_name
from clio_relay.errors import RelayError
from clio_relay.worker_concurrency import kind_concurrency_metadata

_SYSTEMD_SERVICE_NAME = re.compile(r"clio-relay-worker-[a-z0-9_-]+\.service\Z")


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
