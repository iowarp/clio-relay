"""Read-only readiness evidence for managed endpoint user services."""

from __future__ import annotations

import math
import shlex
import subprocess
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from clio_relay.deployment import endpoint_user_service_name
from clio_relay.errors import RelayError

ENDPOINT_SERVICE_STATUS_TIMEOUT_SECONDS = 30.0
_SYSTEMD_INSPECTION_TIMEOUT_SECONDS = 5
_TRANSITIONAL_ACTIVE_STATES = frozenset(
    {"activating", "deactivating", "maintenance", "refreshing", "reloading"}
)
_TRANSITIONAL_SUB_STATES = frozenset({"auto-restart", "start", "stop", "reload"})
_ENABLED_UNIT_STATES = frozenset({"enabled", "enabled-runtime", "linked", "linked-runtime"})

EndpointServiceRecoveryState = Literal[
    "ready",
    "recovering",
    "intentional-stop",
    "failed",
    "inactive-unexpected",
    "not-installed",
    "degraded",
]


class EndpointServiceReadiness(BaseModel):
    """Machine-readable state and recovery policy for one endpoint service."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.endpoint-service-readiness.v1"] = (
        "clio-relay.endpoint-service-readiness.v1"
    )
    cluster: str
    service_name: str
    observed_at: datetime
    load_state: str
    unit_file_state: str
    active_state: str
    sub_state: str
    result: str
    invocation_id: str | None
    restart_policy: str
    restart_delay: str
    start_limit_interval: str
    start_limit_burst: int | None
    automatic_restart_count: int | None
    activation_job_id: str | None
    activation_job_type: str | None
    activation_job_state: str | None
    linger: bool | None
    persistence: Literal["systemd-user-linger", "login-scoped", "unknown"]
    installed: bool
    enabled: bool
    active: bool
    activation_pending: bool
    self_healing_configured: bool
    persistent_across_logout: bool
    intentional_stop: bool
    recovery_state: EndpointServiceRecoveryState
    ready: bool
    diagnosis: str
    operator_action: str | None
    status_probe_mutated_queue: Literal[False] = False
    status_probe_mutated_scheduler_jobs: Literal[False] = False
    service_restart_preserves_durable_queue: Literal[True] = True
    service_restart_cancels_scheduler_jobs: Literal[False] = False


def endpoint_service_readiness_over_ssh(
    *,
    cluster: str,
    ssh_host: str,
    timeout_seconds: float = ENDPOINT_SERVICE_STATUS_TIMEOUT_SECONDS,
) -> EndpointServiceReadiness:
    """Inspect one managed endpoint service through a bounded, read-only SSH call."""
    if (
        not ssh_host
        or ssh_host != ssh_host.strip()
        or ssh_host.startswith("-")
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in ssh_host
        )
    ):
        raise RelayError(
            "ssh host must be one non-option destination without whitespace or controls"
        )
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("endpoint service status timeout must be finite and positive")
    service_name = endpoint_user_service_name(cluster)
    script = render_endpoint_service_readiness_script(service_name=service_name)
    try:
        result = subprocess.run(
            ["ssh", ssh_host, "bash", "-s"],
            input=script.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(f"endpoint service status exceeded {timeout_seconds:g} seconds") from exc
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    if result.returncode != 0:
        detail = stderr.strip() or stdout.strip()
        raise RelayError(f"failed to inspect endpoint user service: {detail}")
    return parse_endpoint_service_readiness(
        cluster=cluster,
        service_name=service_name,
        output=stdout,
    )


def render_endpoint_service_readiness_script(*, service_name: str) -> str:
    """Render a bounded systemd inspection with no service or queue mutations."""
    service_literal = shlex.quote(service_name)
    return f"""set -euo pipefail
export SYSTEMD_COLORS=0 LANG=C LC_ALL=C
service_name={service_literal}
if ! properties="$(
  timeout --signal=TERM --kill-after=2s {_SYSTEMD_INSPECTION_TIMEOUT_SECONDS}s \
    systemctl --user show "$service_name" --no-pager \
      --property=LoadState --property=UnitFileState --property=ActiveState \
      --property=SubState --property=Result --property=InvocationID \
      --property=Restart --property=RestartUSec --property=NRestarts \
      --property=StartLimitIntervalUSec --property=StartLimitBurst
)"; then
  echo "bounded systemd endpoint-service inspection failed: $service_name" >&2
  exit 74
fi
printf '%s\\n' "$properties"
linger="$(
  timeout --signal=TERM --kill-after=2s {_SYSTEMD_INSPECTION_TIMEOUT_SECONDS}s \
    loginctl show-user "${{USER:-$(id -un)}}" -p Linger --value 2>/dev/null || true
)"
printf 'Linger=%s\\n' "${{linger:-unknown}}"
job_id=
job_type=
job_state=
job_matches=0
if jobs="$(
  timeout --signal=TERM --kill-after=2s {_SYSTEMD_INSPECTION_TIMEOUT_SECONDS}s \
    systemctl --user list-jobs --no-legend --plain --no-pager "$service_name"
)"; then
  while read -r candidate_id candidate_unit candidate_type candidate_state _remaining; do
    [ -n "${{candidate_id:-}}" ] || continue
    [ "${{candidate_unit:-}}" = "$service_name" ] || continue
    job_matches=$((job_matches + 1))
    job_id="$candidate_id"
    job_type="${{candidate_type:-unknown}}"
    job_state="${{candidate_state:-unknown}}"
  done < <(printf '%s\\n' "$jobs")
else
  echo "bounded systemd endpoint-service job inspection failed: $service_name" >&2
  exit 74
fi
if [ "$job_matches" -gt 1 ]; then
  echo "ambiguous systemd endpoint-service activation jobs: $service_name" >&2
  exit 74
fi
printf 'ActivationJobId=%s\\n' "${{job_id:-none}}"
printf 'ActivationJobType=%s\\n' "${{job_type:-none}}"
printf 'ActivationJobState=%s\\n' "${{job_state:-none}}"
""".replace("\r\n", "\n")


def parse_endpoint_service_readiness(
    *,
    cluster: str,
    service_name: str,
    output: str,
) -> EndpointServiceReadiness:
    """Parse exact systemd properties into the public readiness contract."""
    properties: dict[str, str] = {}
    for raw_line in output.splitlines():
        key, separator, value = raw_line.partition("=")
        if not separator or not key or key in properties:
            raise RelayError("endpoint service status output is invalid")
        properties[key] = value
    required = {
        "LoadState",
        "UnitFileState",
        "ActiveState",
        "SubState",
        "Result",
        "InvocationID",
        "Restart",
        "RestartUSec",
        "NRestarts",
        "StartLimitIntervalUSec",
        "StartLimitBurst",
        "Linger",
        "ActivationJobId",
        "ActivationJobType",
        "ActivationJobState",
    }
    if properties.keys() != required:
        raise RelayError("endpoint service status output is incomplete")

    load_state = properties["LoadState"] or "unknown"
    unit_file_state = properties["UnitFileState"] or "unknown"
    active_state = properties["ActiveState"] or "unknown"
    sub_state = properties["SubState"] or "unknown"
    result = properties["Result"] or "unknown"
    restart_policy = properties["Restart"] or "unknown"
    restart_delay = properties["RestartUSec"] or "unknown"
    start_limit_interval = properties["StartLimitIntervalUSec"] or "unknown"
    restart_count = _optional_nonnegative_integer(properties["NRestarts"])
    start_limit_burst = _optional_nonnegative_integer(properties["StartLimitBurst"])
    activation_job_id = _optional_status_value(properties["ActivationJobId"])
    activation_job_type = _optional_status_value(properties["ActivationJobType"])
    activation_job_state = _optional_status_value(properties["ActivationJobState"])
    linger_value = properties["Linger"]
    linger = True if linger_value == "yes" else False if linger_value == "no" else None
    installed = load_state == "loaded"
    enabled = unit_file_state in _ENABLED_UNIT_STATES
    active = active_state == "active"
    activation_pending = bool(
        activation_job_id is not None
        or active_state in _TRANSITIONAL_ACTIVE_STATES
        or sub_state in _TRANSITIONAL_SUB_STATES
    )
    start_limit_disabled = start_limit_interval in {"0", "0s", "0us", "0ms"}
    self_healing_configured = restart_policy == "always" and start_limit_disabled
    persistent_across_logout = linger is True
    intentional_stop = bool(
        installed
        and enabled
        and self_healing_configured
        and active_state == "inactive"
        and sub_state in {"dead", "exited", "unknown"}
        and result in {"success", "unknown"}
        and not activation_pending
    )
    recovery_state, diagnosis, operator_action = _classify_endpoint_service(
        cluster=cluster,
        service_name=service_name,
        installed=installed,
        enabled=enabled,
        active=active,
        activation_pending=activation_pending,
        self_healing_configured=self_healing_configured,
        persistent_across_logout=persistent_across_logout,
        intentional_stop=intentional_stop,
        active_state=active_state,
        sub_state=sub_state,
        result=result,
    )
    ready = bool(
        installed and enabled and active and self_healing_configured and persistent_across_logout
    )
    return EndpointServiceReadiness(
        cluster=cluster,
        service_name=service_name,
        observed_at=datetime.now(UTC),
        load_state=load_state,
        unit_file_state=unit_file_state,
        active_state=active_state,
        sub_state=sub_state,
        result=result,
        invocation_id=_optional_status_value(properties["InvocationID"]),
        restart_policy=restart_policy,
        restart_delay=restart_delay,
        start_limit_interval=start_limit_interval,
        start_limit_burst=start_limit_burst,
        automatic_restart_count=restart_count,
        activation_job_id=activation_job_id,
        activation_job_type=activation_job_type,
        activation_job_state=activation_job_state,
        linger=linger,
        persistence=(
            "systemd-user-linger"
            if linger is True
            else "login-scoped"
            if linger is False
            else "unknown"
        ),
        installed=installed,
        enabled=enabled,
        active=active,
        activation_pending=activation_pending,
        self_healing_configured=self_healing_configured,
        persistent_across_logout=persistent_across_logout,
        intentional_stop=intentional_stop,
        recovery_state=recovery_state,
        ready=ready,
        diagnosis=diagnosis,
        operator_action=operator_action,
    )


def _classify_endpoint_service(
    *,
    cluster: str,
    service_name: str,
    installed: bool,
    enabled: bool,
    active: bool,
    activation_pending: bool,
    self_healing_configured: bool,
    persistent_across_logout: bool,
    intentional_stop: bool,
    active_state: str,
    sub_state: str,
    result: str,
) -> tuple[EndpointServiceRecoveryState, str, str | None]:
    install_action = f"clio-relay cluster install-endpoint-service --cluster {cluster}"
    restart_action = f"clio-relay cluster restart-endpoint-service --cluster {cluster}"
    if not installed:
        return "not-installed", "managed endpoint service is not installed", install_action
    if not enabled:
        return "degraded", "managed endpoint service is not enabled", install_action
    if intentional_stop:
        return (
            "intentional-stop",
            "managed endpoint service is inactive after an intentional systemd stop",
            restart_action,
        )
    if active_state == "failed" or result not in {"success", "unknown"} and not activation_pending:
        return (
            "failed",
            f"managed endpoint service failed: state={active_state}/{sub_state} result={result}",
            f"journalctl --user --unit={service_name} --lines=50 --no-pager",
        )
    if not self_healing_configured:
        return (
            "degraded",
            "managed endpoint service does not have persistent crash recovery",
            install_action,
        )
    if not persistent_across_logout:
        return (
            "degraded",
            "managed endpoint service is login-scoped and may stop after desktop disconnect",
            install_action,
        )
    if active:
        return "ready", "managed endpoint service is active and self-healing", None
    if activation_pending:
        return "recovering", "managed endpoint service activation is in progress", None
    return (
        "inactive-unexpected",
        f"managed endpoint service is unexpectedly inactive: state={active_state}/{sub_state}",
        restart_action,
    )


def _optional_status_value(value: str) -> str | None:
    return None if value in {"", "none", "unknown"} else value


def _optional_nonnegative_integer(value: str) -> int | None:
    if not value or value == "unknown":
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise RelayError("endpoint service status integer is invalid") from exc
    if parsed < 0:
        raise RelayError("endpoint service status integer is invalid")
    return parsed
