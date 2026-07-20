from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import cast

import pytest

import clio_relay.deployment as deployment
import clio_relay.doctor as doctor
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.deployment import (
    install_endpoint_user_service_over_ssh,
    render_endpoint_user_service,
)
from clio_relay.doctor import run_cluster_doctor, run_doctor
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpsConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
    render_frps_config,
)


def test_render_frps_config_has_no_application_state() -> None:
    rendered = render_frps_config(
        FrpsConfig(
            bind_port=7001,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
        )
    )

    assert "bindPort = 7001" in rendered
    assert 'auth.token = "secret"' in rendered
    assert "job" not in rendered.lower()
    assert "queue" not in rendered.lower()


def test_endpoint_service_transport_budget_outlives_activation_observer() -> None:
    """SSH setup and teardown cannot truncate the bounded systemd observer."""
    assert deployment.ENDPOINT_SERVICE_SSH_SETUP_MARGIN_SECONDS >= 60
    assert deployment.ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS == (
        deployment.ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS
        + deployment.ENDPOINT_SERVICE_SSH_SETUP_MARGIN_SECONDS
    )


def test_render_frpc_config_uses_configured_websocket_transport() -> None:
    rendered = render_frpc_config(
        FrpcConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            local_port=8848,
            secret_key="stcp-secret",
        )
    )

    assert 'serverAddr = "relay.example.test"' in rendered
    assert "serverPort = 443" in rendered
    assert 'transport.protocol = "wss"' in rendered
    assert 'type = "stcp"' in rendered


def test_render_frpc_visitor_config_uses_stcp_visitor() -> None:
    rendered = render_frpc_visitor_config(
        FrpcVisitorConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            server_name="cluster-relay",
            visitor_name="desktop-relay",
            bind_port=8765,
            secret_key="stcp-secret",
        )
    )

    assert 'serverAddr = "relay.example.test"' in rendered
    assert "serverPort = 443" in rendered
    assert 'transport.protocol = "wss"' in rendered
    assert "[[visitors]]" in rendered
    assert 'type = "stcp"' in rendered
    assert 'serverName = "cluster-relay"' in rendered
    assert 'bindAddr = "127.0.0.1"' in rendered
    assert "bindPort = 8765" in rendered


def test_render_frpc_config_supports_xtcp_proxy_and_visitor() -> None:
    proxy = render_frpc_config(
        FrpcConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            proxy_name="cluster-direct",
            proxy_type="xtcp",
            local_port=8848,
            secret_key="xtcp-secret",
        )
    )
    visitor = render_frpc_visitor_config(
        FrpcVisitorConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            visitor_name="desktop-direct",
            visitor_type="xtcp",
            server_name="cluster-direct",
            bind_port=8765,
            secret_key="xtcp-secret",
            keep_tunnel_open=True,
        )
    )

    assert 'type = "xtcp"' in proxy
    assert 'name = "cluster-direct"' in proxy
    assert 'type = "xtcp"' in visitor
    assert 'serverName = "cluster-direct"' in visitor
    assert "keepTunnelOpen = true" in visitor


def test_live_doctor_requires_frps_address(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_FRPS_ADDR"):
        run_doctor(settings, live=True)


def test_live_doctor_accepts_cluster_frps_address(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frp_token="secret",
        frpc_bin="python",
    )

    lines = run_doctor(settings, live=True, frps_addr="frps.example.test")

    assert "frps_addr: frps.example.test" in lines
    assert "frp_token: configured" in lines
    assert any(line.startswith("frpc:") for line in lines)


def test_live_doctor_reports_missing_frp_token(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="python",
    )

    lines = run_doctor(settings, live=True, frps_addr="frps.example.test")

    assert "frp_token: missing" in lines


def test_live_doctor_does_not_require_cluster_tools_locally(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frps_addr="frps.example.test",
        frp_token="secret",
        frpc_bin="python",
        jarvis_bin="definitely-not-local-jarvis",
        agent_bin="definitely-not-local-agent",
    )

    lines = run_doctor(settings, live=True)

    assert "frps_addr: frps.example.test" in lines
    assert "frp_token: configured" in lines
    assert any(line.startswith("frpc:") for line in lines)


def test_endpoint_user_service_is_sudo_less_and_configured() -> None:
    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            agent_adapter="exec",
            agent_npm_bin="current-agent",
            agent_args=["--prompt", "{prompt_path}"],
        ),
    )

    assert (
        "ExecStartPre=%h/.local/bin/clio-relay queue migrate-indexes --all --batch-size 500"
    ) in rendered
    assert (
        "ExecStart=%h/.local/bin/clio-relay endpoint start --role worker --cluster test-cluster"
    ) in rendered
    assert 'Environment="CLIO_RELAY_CORE_DIR=%h/.local/share/clio-relay/core"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_BIN=%h/.local/bin/current-agent"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_ADAPTER=exec"' in rendered
    assert (
        'Environment="CLIO_RELAY_INSTALL_RECEIPT=%h/.local/share/clio-relay/install-receipt.json"'
    ) in rendered
    assert "Restart=always" in rendered
    assert "Restart=on-failure" not in rendered
    assert "RestartSec=5" in rendered
    assert "TimeoutStartSec=300s" in rendered
    assert "sudo" not in rendered


def _run_activation_observer_fixture(
    *,
    scenario: str,
    observation_timeout_seconds: int,
    activation_action: str = "start",
    repeat_after_failure_count: int = 0,
) -> subprocess.CompletedProcess[bytes]:
    """Execute the rendered activation observer against a stateful fake systemd."""
    if scenario not in {
        "active-masked-no-job",
        "ambiguous-replacement",
        "confirmed-restart-first-start",
        "confirmed-no-job-inactive",
        "delayed",
        "delayed-job-after-failed",
        "delayed-job-after-inactive",
        "failed",
        "failed-same-result",
        "job-read-failure-active",
        "maintenance-after-enqueue",
        "preexisting-future-active-no-job",
        "preexisting-future-load-no-job",
        "preflight-ambiguous-survivor",
        "preexisting-maintenance-no-job",
        "preexisting-masked-active-no-job",
        "preexisting-masked-inactive-no-job",
        "preexisting-merged-active-no-job",
        "preexisting-reload",
        "preexisting-restart-different-id",
        "preexisting-restart-same-id",
        "preexisting-restart-stable",
        "preexisting-reloading-no-job",
        "preexisting-start-activating",
        "preexisting-start-stable",
        "preexisting-refreshing-no-job",
        "preexisting-stub-inactive-no-job",
        "refreshing-after-enqueue",
        "restart-active-unknown-invocation",
        "restart-same-invocation",
        "stuck",
        "timeout-restart-start-job",
        "timeout-restart-exact-job",
        "timeout-restart-transition-no-job",
        "timeout-restart-ambiguous-survivor",
        "timeout-tracked-job-completes-on-retry",
        "timeout-tracked-job-active-equal",
        "timeout-tracked-job-active-unknown",
        "timeout-tracked-job-failed",
        "timeout-tracked-job-inactive",
        "timeout-tracked-job-masked",
        "timeout-tracked-job-replaced",
        "timeout-tracked-job-transition-retry",
        "unknown-initial",
        "unknown-job-initial",
    }:
        raise ValueError(f"unsupported activation fixture scenario: {scenario}")
    if activation_action not in {"start", "restart"}:
        raise ValueError(f"unsupported activation fixture action: {activation_action}")
    if type(repeat_after_failure_count) is not int or repeat_after_failure_count < 0:
        raise ValueError("repeat_after_failure_count must be a non-negative integer")
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate bounded systemd activation")
    helper = deployment.render_bounded_user_service_activation_helper(
        observation_timeout_seconds=observation_timeout_seconds,
        poll_seconds=1,
        progress_seconds=1,
    )
    repeat = "clio_relay_endpoint_activate_bounded || true\n" * repeat_after_failure_count
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT
mkdir -p "$test_root/bin" "$test_root/home"
export HOME="$test_root/home"
export FAKE_SYSTEMD_ROOT="$test_root" FAKE_SYSTEMD_SCENARIO={scenario}
cat > "$test_root/bin/systemctl" <<'__FAKE_SYSTEMCTL__'
#!/usr/bin/env bash
set -u
printf '%s\n' "$*" >> "$FAKE_SYSTEMD_ROOT/calls"
action="${{2:-}}"
case "$action" in
  start|restart)
    attempts=0
    if [ -f "$FAKE_SYSTEMD_ROOT/attempts" ]; then
      attempts="$(cat "$FAKE_SYSTEMD_ROOT/attempts")"
    fi
    attempts=$((attempts + 1))
    printf '%s\n' "$attempts" > "$FAKE_SYSTEMD_ROOT/attempts"
    : > "$FAKE_SYSTEMD_ROOT/enqueued"
    case "$FAKE_SYSTEMD_SCENARIO" in
      timeout-restart-start-job|timeout-restart-exact-job|timeout-restart-transition-no-job|timeout-restart-ambiguous-survivor)
        exit 124
        ;;
    esac
    ;;
  show)
    if [ "$FAKE_SYSTEMD_SCENARIO" = unknown-initial ] && \
       [ ! -f "$FAKE_SYSTEMD_ROOT/enqueued" ]; then
      exit 1
    fi
    state=inactive
    sub_state=dead
    load_state=loaded
    result=success
    invocation_id=old-invocation
    control_pid=0
    post_count=0
    if [ "$FAKE_SYSTEMD_SCENARIO" = failed-same-result ]; then
      state=failed
      sub_state=failed
      result=exit-code
    elif [ "$FAKE_SYSTEMD_SCENARIO" = delayed-job-after-failed ]; then
      state=failed
      sub_state=failed
      result=exit-code
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-maintenance-no-job ]; then
      state=maintenance
      sub_state=maintenance
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-refreshing-no-job ]; then
      state=refreshing
      sub_state=refreshing
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-future-active-no-job ]; then
      state=future-active
      sub_state=future
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-masked-inactive-no-job ]; then
      load_state=masked
      state=inactive
      sub_state=dead
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-masked-active-no-job ]; then
      load_state=masked
      state=active
      sub_state=running
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-stub-inactive-no-job ]; then
      load_state=stub
      state=inactive
      sub_state=dead
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-merged-active-no-job ]; then
      load_state=merged
      state=active
      sub_state=running
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-future-load-no-job ]; then
      load_state=future-load
      state=inactive
      sub_state=dead
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preflight-ambiguous-survivor ]; then
      state=active
      sub_state=running
      if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
        invocation_id=new-invocation
      fi
    elif [[ "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-active-equal ||
            "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-active-unknown ||
            "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-failed ||
            "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-inactive ||
            "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-masked ||
            "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-transition-retry ]]; then
      state=active
      sub_state=running
      if [[ "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-active-unknown ||
            "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-transition-retry ]]; then
        invocation_id=
      fi
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-reload ]; then
      state=active
      sub_state=running
    elif [ "$FAKE_SYSTEMD_SCENARIO" = preexisting-reloading-no-job ]; then
      show_count=0
      if [ -f "$FAKE_SYSTEMD_ROOT/show-count" ]; then
        show_count="$(cat "$FAKE_SYSTEMD_ROOT/show-count")"
      fi
      show_count=$((show_count + 1))
      printf '%s\n' "$show_count" > "$FAKE_SYSTEMD_ROOT/show-count"
      if [ "$show_count" = 1 ]; then
        state=reloading
        sub_state=reload
      else
        state=active
        sub_state=running
      fi
    elif [[ "$FAKE_SYSTEMD_SCENARIO" = preexisting-start-activating ||
            "$FAKE_SYSTEMD_SCENARIO" = ambiguous-replacement ||
            "$FAKE_SYSTEMD_SCENARIO" = preexisting-restart-same-id ||
            "$FAKE_SYSTEMD_SCENARIO" = preexisting-restart-different-id ||
            "$FAKE_SYSTEMD_SCENARIO" = preexisting-start-stable ||
            "$FAKE_SYSTEMD_SCENARIO" = preexisting-restart-stable ||
            "$FAKE_SYSTEMD_SCENARIO" = restart-active-unknown-invocation ||
            "$FAKE_SYSTEMD_SCENARIO" = restart-same-invocation ]]; then
      show_count=0
      if [ -f "$FAKE_SYSTEMD_ROOT/show-count" ]; then
        show_count="$(cat "$FAKE_SYSTEMD_ROOT/show-count")"
      fi
      show_count=$((show_count + 1))
      printf '%s\n' "$show_count" > "$FAKE_SYSTEMD_ROOT/show-count"
      case "$FAKE_SYSTEMD_SCENARIO:$show_count" in
        restart-active-unknown-invocation:*)
          state=active; sub_state=running; invocation_id=
          ;;
        preexisting-start-activating:1)
          state=activating; sub_state=start
          ;;
        preexisting-start-stable:1)
          state=inactive; sub_state=dead
          ;;
        *:1)
          state=active; sub_state=running
          ;;
        *:2)
          state=activating; sub_state=start
          ;;
        restart-same-invocation:*)
          state=active; sub_state=running
          ;;
        *)
          state=active; sub_state=running; invocation_id=new-invocation
          ;;
      esac
    fi
    if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ]; then
      if [ -f "$FAKE_SYSTEMD_ROOT/post-count" ]; then
        post_count="$(cat "$FAKE_SYSTEMD_ROOT/post-count")"
      fi
      post_count=$((post_count + 1))
      printf '%s\n' "$post_count" > "$FAKE_SYSTEMD_ROOT/post-count"
      invocation_id=new-invocation
      case "$FAKE_SYSTEMD_SCENARIO:$post_count" in
        delayed:1) state=inactive; sub_state=dead ;;
        delayed:2) state=activating; sub_state=start-pre; control_pid=41 ;;
        delayed:*) state=active; sub_state=running; control_pid=42 ;;
        delayed-job-after-inactive:1)
          state=inactive; sub_state=dead; invocation_id=old-invocation
          ;;
        delayed-job-after-failed:1)
          state=failed; sub_state=failed; result=exit-code; invocation_id=old-invocation
          ;;
        delayed-job-after-inactive:2|delayed-job-after-failed:2)
          state=activating; sub_state=start-pre; control_pid=43
          ;;
        delayed-job-after-inactive:*|delayed-job-after-failed:*)
          state=active; sub_state=running; control_pid=44
          ;;
        failed:1) state=activating; sub_state=start-pre; control_pid=51 ;;
        failed:*) state=failed; sub_state=failed; result=exit-code ;;
        failed-same-result:*) state=failed; sub_state=failed; result=exit-code ;;
        maintenance-after-enqueue:*)
          state=maintenance; sub_state=maintenance; control_pid=65
          ;;
        refreshing-after-enqueue:*)
          state=refreshing; sub_state=refreshing; control_pid=66
          ;;
        confirmed-restart-first-start:1)
          state=activating; sub_state=start; control_pid=71
          ;;
        confirmed-restart-first-start:*)
          state=active; sub_state=running; control_pid=72
          ;;
        confirmed-no-job-inactive:*)
          state=inactive; sub_state=dead; invocation_id=old-invocation
          ;;
        active-masked-no-job:*)
          state=active; sub_state=running
          if [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            load_state=masked
          fi
          ;;
        job-read-failure-active:*) state=active; sub_state=running ;;
        stuck:*) state=activating; sub_state=start-pre; control_pid=61 ;;
        timeout-restart-start-job:1)
          state=activating; sub_state=start; control_pid=81
          ;;
        timeout-restart-start-job:*) state=active; sub_state=running ;;
        timeout-restart-exact-job:1|timeout-restart-exact-job:2)
          state=activating; sub_state=start; control_pid=83
          ;;
        timeout-restart-exact-job:*) state=active; sub_state=running ;;
        timeout-restart-ambiguous-survivor:*)
          if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            state=active; sub_state=running; invocation_id=new-invocation
          else
            state=activating; sub_state=start; control_pid=85
          fi
          ;;
        timeout-restart-transition-no-job:1)
          state=reloading; sub_state=reload; control_pid=91
          ;;
        timeout-restart-transition-no-job:*) state=active; sub_state=running ;;
        timeout-tracked-job-replaced:*)
          if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            state=active; sub_state=running; invocation_id=replacement-invocation
          else
            state=activating; sub_state=start; invocation_id=old-invocation
          fi
          ;;
        timeout-tracked-job-completes-on-retry:*)
          if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            state=active; sub_state=running; invocation_id=new-invocation
          else
            state=activating; sub_state=start; invocation_id=old-invocation
          fi
          ;;
        timeout-tracked-job-active-equal:*)
          state=active; sub_state=running; invocation_id=old-invocation
          ;;
        timeout-tracked-job-active-unknown:*)
          state=active; sub_state=running; invocation_id=
          ;;
        timeout-tracked-job-failed:*)
          if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            state=failed; sub_state=failed; result=exit-code
          else
            state=active; sub_state=running; invocation_id=old-invocation
          fi
          ;;
        timeout-tracked-job-inactive:*)
          if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            state=inactive; sub_state=dead
          else
            state=active; sub_state=running; invocation_id=old-invocation
          fi
          ;;
        timeout-tracked-job-masked:*)
          state=active; sub_state=running; invocation_id=old-invocation
          if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            load_state=masked
          fi
          ;;
        timeout-tracked-job-transition-retry:*)
          invocation_id=
          if [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            state=active; sub_state=running
          elif [ ! -f "$FAKE_SYSTEMD_ROOT/transition-served" ]; then
            state=activating; sub_state=start
          else
            state=active; sub_state=running
          fi
          ;;
      esac
    fi
    printf '%s\n' \
      "LoadState=$load_state" \
      "ActiveState=$state" \
      "SubState=$sub_state" \
      "Result=$result" \
      "ControlPID=$control_pid" \
      'ExecMainCode=0' \
      'ExecMainStatus=0' \
      'TimeoutStartUSec=5min' \
      "InvocationID=$invocation_id"
    ;;
  list-jobs)
    if [ "$FAKE_SYSTEMD_SCENARIO" = unknown-job-initial ]; then
      exit 1
    fi
    if [ "$FAKE_SYSTEMD_SCENARIO" = job-read-failure-active ] && \
       [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ]; then
      exit 1
    fi
    post_count=0
    if [ -f "$FAKE_SYSTEMD_ROOT/post-count" ]; then
      post_count="$(cat "$FAKE_SYSTEMD_ROOT/post-count")"
    fi
    show_count=0
    if [ -f "$FAKE_SYSTEMD_ROOT/show-count" ]; then
      show_count="$(cat "$FAKE_SYSTEMD_ROOT/show-count")"
    fi
    case "$FAKE_SYSTEMD_SCENARIO:$post_count" in
      delayed:1|delayed:2|failed:1)
        printf '101 clio-relay-worker-test.service start running\n'
        ;;
      delayed-job-after-inactive:2|delayed-job-after-failed:2)
        printf '102 clio-relay-worker-test.service start running\n'
        ;;
      stuck:*)
        if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ]; then
          printf '101 clio-relay-worker-test.service start running\n'
        fi
        ;;
      preexisting-reload:*)
        printf '202 clio-relay-worker-test.service reload waiting\n'
        ;;
      confirmed-restart-first-start:1)
        printf '401 clio-relay-worker-test.service start running\n'
        ;;
      timeout-restart-start-job:1)
        printf '501 clio-relay-worker-test.service start running\n'
        ;;
      timeout-restart-exact-job:1)
        printf '502 clio-relay-worker-test.service restart running\n'
        ;;
      timeout-restart-exact-job:2)
        printf '502 clio-relay-worker-test.service start running\n'
        ;;
      timeout-restart-ambiguous-survivor:*)
        if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ] && \
           [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
          printf '%s\n' \
            '801 clio-relay-worker-test.service restart running' \
            '802 clio-relay-worker-test.service start waiting'
        elif [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ] && \
             [ ! -f "$FAKE_SYSTEMD_ROOT/survivor-served" ]; then
          printf '801 clio-relay-worker-test.service restart running\n'
          : > "$FAKE_SYSTEMD_ROOT/survivor-served"
        fi
        ;;
      timeout-tracked-job-replaced:*)
        if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ] && \
           [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
          printf '601 clio-relay-worker-test.service restart running\n'
        elif [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ] && \
             [ ! -f "$FAKE_SYSTEMD_ROOT/replacement-served" ]; then
          printf '602 clio-relay-worker-test.service restart running\n'
          : > "$FAKE_SYSTEMD_ROOT/replacement-served"
        fi
        ;;
    esac
    case "$FAKE_SYSTEMD_SCENARIO:$show_count" in
      preexisting-start-activating:1)
        printf '203 clio-relay-worker-test.service start running\n'
        ;;
      ambiguous-replacement:1)
        printf '301 clio-relay-worker-test.service restart running\n'
        ;;
      ambiguous-replacement:2)
        printf '%s\n' \
          '301 clio-relay-worker-test.service restart running' \
          '302 clio-relay-worker-test.service start waiting'
        ;;
      preexisting-restart-same-id:1|restart-same-invocation:1)
        printf '301 clio-relay-worker-test.service restart running\n'
        ;;
      restart-active-unknown-invocation:1)
        printf '301 clio-relay-worker-test.service restart running\n'
        ;;
      preexisting-restart-same-id:2|restart-same-invocation:2)
        printf '301 clio-relay-worker-test.service start running\n'
        ;;
      preexisting-restart-different-id:1)
        printf '301 clio-relay-worker-test.service restart running\n'
        ;;
      preexisting-restart-different-id:2)
        printf '302 clio-relay-worker-test.service start running\n'
        ;;
      preexisting-start-stable:1|preexisting-start-stable:2)
        printf '310 clio-relay-worker-test.service start running\n'
        ;;
      preexisting-restart-stable:1|preexisting-restart-stable:2)
        printf '311 clio-relay-worker-test.service restart running\n'
        ;;
      preflight-ambiguous-survivor:*)
        if [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
          printf '%s\n' \
            '701 clio-relay-worker-test.service restart running' \
            '702 clio-relay-worker-test.service start waiting'
        elif [ ! -f "$FAKE_SYSTEMD_ROOT/survivor-served" ]; then
          printf '701 clio-relay-worker-test.service restart running\n'
          : > "$FAKE_SYSTEMD_ROOT/survivor-served"
        fi
        ;;
      timeout-tracked-job-active-equal:*|timeout-tracked-job-active-unknown:*|timeout-tracked-job-failed:*|timeout-tracked-job-inactive:*|timeout-tracked-job-masked:*)
        if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ] && \
           {{ [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ] || \
              [ "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-masked ]; }}; then
          printf '910 clio-relay-worker-test.service restart running\n'
        fi
        ;;
      timeout-tracked-job-transition-retry:*)
        if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ] && \
           {{ [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ] || \
              [ ! -f "$FAKE_SYSTEMD_ROOT/transition-served" ]; }}; then
          printf '920 clio-relay-worker-test.service restart running\n'
          if [ -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
            : > "$FAKE_SYSTEMD_ROOT/transition-served"
          fi
        fi
        ;;
      timeout-tracked-job-completes-on-retry:*)
        if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ] && \
           [ ! -f "$FAKE_SYSTEMD_ROOT/first-finished" ]; then
          printf '901 clio-relay-worker-test.service restart running\n'
        fi
        ;;
    esac
    ;;
  *) exit 2 ;;
esac
__FAKE_SYSTEMCTL__
cat > "$test_root/bin/flock" <<'__FAKE_FLOCK__'
#!/usr/bin/env bash
set -u
case "${{1:-}}" in
  --exclusive)
    while ! mkdir "$FAKE_SYSTEMD_ROOT/activation-flock" 2>/dev/null; do sleep 0.05; done
    ;;
  --unlock) rmdir "$FAKE_SYSTEMD_ROOT/activation-flock" ;;
  *) exit 2 ;;
esac
__FAKE_FLOCK__
chmod +x "$test_root/bin/systemctl" "$test_root/bin/flock"
export PATH="$test_root/bin:$PATH"
CLIO_RELAY_ENDPOINT_SERVICE_NAME=clio-relay-worker-test.service
CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION={activation_action}
{helper}
fixture_started=$SECONDS
first_status=0
clio_relay_endpoint_activate_bounded || first_status=$?
first_outcome="$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"
if [[ "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-replaced ||
      "$FAKE_SYSTEMD_SCENARIO" = active-masked-no-job ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-completes-on-retry ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-active-equal ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-active-unknown ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-failed ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-inactive ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-masked ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-tracked-job-transition-retry ||
      "$FAKE_SYSTEMD_SCENARIO" = preflight-ambiguous-survivor ||
      "$FAKE_SYSTEMD_SCENARIO" = timeout-restart-ambiguous-survivor ]]; then
  : > "$FAKE_SYSTEMD_ROOT/first-finished"
fi
{repeat}printf 'fixture.first_status=%s\n' "$first_status"
printf 'fixture.first_outcome=%s\n' "$first_outcome"
printf 'fixture.final_outcome=%s\n' "$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"
printf 'fixture.elapsed=%s\n' "$((SECONDS - fixture_started))"
attempts=0
if [ -f "$test_root/attempts" ]; then attempts="$(cat "$test_root/attempts")"; fi
printf 'fixture.attempts=%s\n' "$attempts"
exit "$first_status"
"""
    return subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=observation_timeout_seconds + 10,
    )


def test_endpoint_activation_waits_for_queued_delayed_dispatch() -> None:
    """An inactive unit with an exact queued job is not a startup failure."""
    result = _run_activation_observer_fixture(
        scenario="delayed",
        observation_timeout_seconds=5,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert "active_state=inactive" in stdout
    assert "job_id=101" in stdout
    assert "active_state=activating" in stdout
    assert "active_state=active" in stdout
    assert "outcome=not-attempted" not in stdout
    assert "fixture.first_outcome=active" in stdout
    assert "fixture.attempts=1" in stdout


@pytest.mark.parametrize(
    ("scenario", "active_state"),
    [
        ("delayed-job-after-inactive", "inactive"),
        ("delayed-job-after-failed", "failed"),
    ],
)
def test_endpoint_activation_enqueue_grace_precedes_late_exact_job(
    scenario: str,
    active_state: str,
) -> None:
    """A confirmed enqueue reports grace before its exact job becomes visible."""
    result = _run_activation_observer_fixture(
        scenario=scenario,
        observation_timeout_seconds=5,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    grace_records = [
        line
        for line in stdout.splitlines()
        if line.startswith("endpoint_service.activation ")
        and f"active_state={active_state}" in line
        and "job_id=none" in line
        and "outcome=in-progress" in line
    ]
    assert result.returncode == 0, stderr
    assert grace_records
    assert "job_id=102 job_type=start" in stdout
    assert "fixture.first_outcome=active" in stdout
    assert "fixture.attempts=1" in stdout


def test_endpoint_activation_reports_terminal_systemd_failure() -> None:
    """A vanished observed job plus failed state is terminal evidence."""
    result = _run_activation_observer_fixture(
        scenario="failed",
        observation_timeout_seconds=5,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=failed" in stdout
    assert "active_state=failed" in stderr
    assert "result=exit-code" in stderr
    assert "activation.operator_hint=journalctl --user" in stderr
    assert "fixture.attempts=1" in stdout


def test_endpoint_activation_rejects_active_masked_unit() -> None:
    """ActiveState cannot override a terminal non-loaded unit state."""
    result = _run_activation_observer_fixture(
        scenario="active-masked-no-job",
        observation_timeout_seconds=5,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=failed" in stdout
    assert "fixture.attempts=1" in stdout
    assert "load_state=masked" in stderr
    assert "active_state=active" in stderr
    assert "job_id=none" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_activation_masked_failure_requires_fresh_process_after_repair() -> None:
    """A repaired unit cannot reuse provenance rejected by a masked snapshot."""
    result = _run_activation_observer_fixture(
        scenario="active-masked-no-job",
        observation_timeout_seconds=5,
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=failed" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert "fixture.attempts=1" in stdout
    assert "load_state=masked" in stderr
    assert "load_state=loaded" in stderr
    assert "active_state=active" in stderr
    assert "outcome=active" not in stdout


@pytest.mark.parametrize(
    ("scenario", "active_state"),
    [
        ("preexisting-maintenance-no-job", "maintenance"),
        ("preexisting-refreshing-no-job", "refreshing"),
        ("preexisting-future-active-no-job", "future-active"),
    ],
)
def test_endpoint_activation_preflight_rejects_nonstable_active_state(
    scenario: str,
    active_state: str,
) -> None:
    """Only stable systemd ActiveState values may reach the enqueue boundary."""
    result = _run_activation_observer_fixture(
        scenario=scenario,
        observation_timeout_seconds=5,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.attempts=0" in stdout
    assert f"active_state={active_state}" in stderr
    assert "job_id=none" in stderr
    assert "outcome=active" not in stdout


@pytest.mark.parametrize(
    ("scenario", "active_state"),
    [
        ("maintenance-after-enqueue", "maintenance"),
        ("refreshing-after-enqueue", "refreshing"),
    ],
)
def test_endpoint_activation_observes_extended_transitional_state(
    scenario: str,
    active_state: str,
) -> None:
    """Maintenance and refreshing remain in-progress after a proven enqueue."""
    result = _run_activation_observer_fixture(
        scenario=scenario,
        observation_timeout_seconds=2,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.attempts=1" in stdout
    assert f"active_state={active_state}" in stderr
    assert "outcome=active" not in stdout


@pytest.mark.parametrize(
    ("scenario", "load_state", "active_state", "outcome"),
    [
        ("preexisting-masked-inactive-no-job", "masked", "inactive", "failed"),
        ("preexisting-masked-active-no-job", "masked", "active", "failed"),
        (
            "preexisting-stub-inactive-no-job",
            "stub",
            "inactive",
            "preflight-unverified",
        ),
        (
            "preexisting-merged-active-no-job",
            "merged",
            "active",
            "preflight-unverified",
        ),
        (
            "preexisting-future-load-no-job",
            "future-load",
            "inactive",
            "preflight-unverified",
        ),
    ],
)
def test_endpoint_activation_preflight_rejects_nonloaded_unit(
    scenario: str,
    load_state: str,
    active_state: str,
    outcome: str,
) -> None:
    """Every non-loaded systemd LoadState fails closed before mutation."""
    result = _run_activation_observer_fixture(
        scenario=scenario,
        observation_timeout_seconds=5,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert f"fixture.first_outcome={outcome}" in stdout
    assert "fixture.attempts=0" in stdout
    assert f"load_state={load_state}" in stderr
    assert f"active_state={active_state}" in stderr
    assert "job_id=none" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_activation_initial_state_failure_does_not_enqueue() -> None:
    """An unreadable initial state fails closed before systemd mutation."""
    result = _run_activation_observer_fixture(
        scenario="unknown-initial",
        observation_timeout_seconds=2,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=preflight-unverified" in stdout
    assert "fixture.attempts=0" in stdout
    assert "active_state=unknown" in stderr


def test_endpoint_activation_initial_job_read_failure_reports_unknown_inventory() -> None:
    """A failed list-jobs read cannot be represented as a proven empty inventory."""
    result = _run_activation_observer_fixture(
        scenario="unknown-job-initial",
        observation_timeout_seconds=2,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=preflight-unverified" in stdout
    assert "fixture.attempts=0" in stdout
    assert "job_id=unknown" in stderr
    assert "job_type=unknown" in stderr
    assert "job_state=unknown" in stderr
    assert "job_id=none" not in stderr


def test_endpoint_activation_active_with_unreadable_job_inventory_is_unverified() -> None:
    """Active state cannot pass while the exact job inventory is unreadable."""
    result = _run_activation_observer_fixture(
        scenario="job-read-failure-active",
        observation_timeout_seconds=2,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=unverified" in stdout
    assert "fixture.attempts=1" in stdout
    assert "active_state=active" in stderr
    assert "job_id=unknown" in stderr
    assert "job_type=unknown" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_activation_repeated_failure_uses_new_invocation_evidence() -> None:
    """A new failed invocation is terminal even when Result stays unchanged."""
    result = _run_activation_observer_fixture(
        scenario="failed-same-result",
        observation_timeout_seconds=5,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    elapsed = int(
        next(line for line in stdout.splitlines() if line.startswith("fixture.elapsed=")).split(
            "=", 1
        )[1]
    )

    assert result.returncode == 1
    assert "fixture.first_outcome=failed" in stdout
    assert elapsed < 5
    assert "fixture.attempts=1" in stdout
    assert "result=exit-code" in stderr
    assert "invocation_id=new-invocation" in stderr


def test_endpoint_activation_rejects_incompatible_preexisting_job() -> None:
    """A queued reload is reported without mutating a requested restart."""
    result = _run_activation_observer_fixture(
        scenario="preexisting-reload",
        observation_timeout_seconds=5,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    elapsed = int(
        next(line for line in stdout.splitlines() if line.startswith("fixture.elapsed=")).split(
            "=", 1
        )[1]
    )

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.attempts=0" in stdout
    assert elapsed < 5
    assert "active_state=active" in stderr
    assert "job_id=202" in stderr
    assert "job_type=reload" in stderr


def test_endpoint_activation_rejects_bare_transitional_preflight_state() -> None:
    """A jobless reload cannot be mistaken for evidence of our requested restart."""
    result = _run_activation_observer_fixture(
        scenario="preexisting-reloading-no-job",
        observation_timeout_seconds=5,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    elapsed = int(
        next(line for line in stdout.splitlines() if line.startswith("fixture.elapsed=")).split(
            "=", 1
        )[1]
    )

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.attempts=0" in stdout
    assert elapsed < 5
    assert "active_state=reloading" in stderr
    assert "job_id=none" in stderr
    assert "active_state=active" not in stdout


def test_endpoint_restart_rejects_preexisting_standalone_start() -> None:
    """A standalone start job cannot prove the requested restart is in progress."""
    result = _run_activation_observer_fixture(
        scenario="preexisting-start-activating",
        observation_timeout_seconds=5,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.attempts=0" in stdout
    assert "active_state=activating" in stderr
    assert "job_id=203" in stderr
    assert "job_type=start" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_adopts_same_job_restart_to_start_transition() -> None:
    """An adopted restart may become start only while retaining its job ID."""
    result = _run_activation_observer_fixture(
        scenario="preexisting-restart-same-id",
        observation_timeout_seconds=5,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert "job_id=301 job_type=restart" in stdout
    assert "job_id=301 job_type=start" in stdout
    assert "fixture.first_outcome=active" in stdout
    assert "fixture.attempts=0" in stdout


@pytest.mark.parametrize("activation_action", ["start", "restart"])
def test_endpoint_activation_rejects_changed_adopted_job_id(
    activation_action: str,
) -> None:
    """An unrelated job cannot replace the exact job first adopted by a request."""
    result = _run_activation_observer_fixture(
        scenario="preexisting-restart-different-id",
        observation_timeout_seconds=5,
        activation_action=activation_action,
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert "fixture.attempts=0" in stdout
    assert "job_id=302" in stderr
    assert "job_type=start" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_rejects_ambiguous_replacement_jobs() -> None:
    """Multiple replacement jobs invalidate an adopted restart's provenance."""
    result = _run_activation_observer_fixture(
        scenario="ambiguous-replacement",
        observation_timeout_seconds=5,
        activation_action="restart",
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert "fixture.attempts=0" in stdout
    assert "job_id=ambiguous" in stderr
    assert "job_type=ambiguous" in stderr
    assert "outcome=active" not in stdout


@pytest.mark.parametrize(
    ("scenario", "first_outcome", "attempts", "survivor_job_id"),
    [
        ("preflight-ambiguous-survivor", "preflight-unverified", 0, "701"),
        ("timeout-restart-ambiguous-survivor", "enqueue-unverified", 1, "801"),
    ],
)
def test_endpoint_restart_ambiguous_inventory_rejects_later_survivor(
    scenario: str,
    first_outcome: str,
    attempts: int,
    survivor_job_id: str,
) -> None:
    """Ambiguous inventory permanently rejects a later survivor and active state."""
    result = _run_activation_observer_fixture(
        scenario=scenario,
        observation_timeout_seconds=5,
        activation_action="restart",
        repeat_after_failure_count=2,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert f"fixture.first_outcome={first_outcome}" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert f"fixture.attempts={attempts}" in stdout
    assert "job_id=ambiguous" in stderr
    assert f"job_id={survivor_job_id}" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_confirmed_enqueue_can_bind_first_start_job() -> None:
    """A successful restart enqueue may first expose its systemd job as start."""
    result = _run_activation_observer_fixture(
        scenario="confirmed-restart-first-start",
        observation_timeout_seconds=5,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert "job_id=401 job_type=start" in stdout
    assert "outcome=not-attempted" not in stdout
    assert "fixture.first_outcome=active" in stdout
    assert "fixture.attempts=1" in stdout


def test_endpoint_restart_enqueue_grace_expires_before_retry() -> None:
    """Confirmed enqueue grace does not survive timeout into a same-shell retry."""
    result = _run_activation_observer_fixture(
        scenario="confirmed-no-job-inactive",
        observation_timeout_seconds=2,
        activation_action="restart",
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    progress_records = [
        line
        for line in stdout.splitlines()
        if line.startswith("endpoint_service.activation ") and "outcome=in-progress" in line
    ]
    assert result.returncode == 1
    assert progress_records
    assert "fixture.first_outcome=unverified" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert "fixture.attempts=1" in stdout
    assert "active_state=inactive" in stderr
    assert "job_id=none" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_timeout_does_not_adopt_start_job() -> None:
    """A timed-out restart has no provenance for a newly observed start job."""
    result = _run_activation_observer_fixture(
        scenario="timeout-restart-start-job",
        observation_timeout_seconds=5,
        activation_action="restart",
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=enqueue-unverified" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert "fixture.attempts=1" in stdout
    assert "active_state=activating" in stderr
    assert "job_id=501" in stderr
    assert "job_type=start" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_timeout_adopts_exact_restart_job() -> None:
    """A timed-out enqueue can adopt an exact restart and its same-ID start phase."""
    result = _run_activation_observer_fixture(
        scenario="timeout-restart-exact-job",
        observation_timeout_seconds=5,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert "job_id=502 job_type=start" in stdout
    assert "outcome=in-progress" in stdout
    assert "outcome=enqueue-unverified" not in stdout
    assert "outcome=failed" not in stdout
    assert "fixture.first_outcome=active" in stdout
    assert "fixture.attempts=1" in stdout


def test_endpoint_restart_timeout_does_not_adopt_bare_transition() -> None:
    """A timed-out restart cannot claim a jobless transition or later active state."""
    result = _run_activation_observer_fixture(
        scenario="timeout-restart-transition-no-job",
        observation_timeout_seconds=5,
        activation_action="restart",
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=enqueue-unverified" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert "fixture.attempts=1" in stdout
    assert "active_state=reloading" in stderr
    assert "job_id=none" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_retry_rejects_replacement_after_timeout() -> None:
    """A retry latches a replacement job before a later no-job active state."""
    result = _run_activation_observer_fixture(
        scenario="timeout-tracked-job-replaced",
        observation_timeout_seconds=2,
        activation_action="restart",
        repeat_after_failure_count=2,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.final_outcome=unverified" in stdout
    assert "fixture.attempts=1" in stdout
    assert "job_id=601" in stderr
    assert "job_id=602" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_retry_emits_terminal_active_observation() -> None:
    """A timed-out observer emits terminal evidence when a retry proves completion."""
    result = _run_activation_observer_fixture(
        scenario="timeout-tracked-job-completes-on-retry",
        observation_timeout_seconds=2,
        activation_action="restart",
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")

    terminal_records = [
        line
        for line in stdout.splitlines()
        if line.startswith("endpoint_service.activation ") and "outcome=active" in line
    ]
    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.final_outcome=active" in stdout
    assert "fixture.attempts=1" in stdout
    assert len(terminal_records) == 1
    assert "active_state=active" in terminal_records[0]
    assert "job_id=none" in terminal_records[0]


def test_endpoint_restart_retry_preserves_same_job_transition_evidence() -> None:
    """A same-job retry transition proves restart when InvocationID is unavailable."""
    result = _run_activation_observer_fixture(
        scenario="timeout-tracked-job-transition-retry",
        observation_timeout_seconds=2,
        activation_action="restart",
        repeat_after_failure_count=2,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")

    terminal_records = [
        line
        for line in stdout.splitlines()
        if line.startswith("endpoint_service.activation ") and "outcome=active" in line
    ]
    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "job_id=920 job_type=restart" in stdout
    assert "active_state=activating" in stdout
    assert "fixture.final_outcome=active" in stdout
    assert "fixture.attempts=1" in stdout
    assert len(terminal_records) == 1
    assert "invocation_id=" in terminal_records[0]
    assert "job_id=none" in terminal_records[0]


@pytest.mark.parametrize(
    ("scenario", "active_state", "final_outcome", "final_job_id"),
    [
        ("timeout-tracked-job-active-equal", "active", "unverified", "none"),
        ("timeout-tracked-job-active-unknown", "active", "unverified", "none"),
        ("timeout-tracked-job-failed", "failed", "failed", "none"),
        ("timeout-tracked-job-inactive", "inactive", "failed", "none"),
        ("timeout-tracked-job-masked", "active", "failed", "910"),
    ],
)
def test_endpoint_restart_retry_classifies_fresh_terminal_evidence(
    scenario: str,
    active_state: str,
    final_outcome: str,
    final_job_id: str,
) -> None:
    """A retry replaces the prior timeout outcome with its current exact evidence."""
    result = _run_activation_observer_fixture(
        scenario=scenario,
        observation_timeout_seconds=2,
        activation_action="restart",
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert f"fixture.final_outcome={final_outcome}" in stdout
    assert "fixture.attempts=1" in stdout
    assert f"active_state={active_state}" in stderr
    assert f"job_id={final_job_id}" in stderr
    assert "outcome=active" not in stdout


@pytest.mark.parametrize(
    ("scenario", "job_type"),
    [
        ("preexisting-start-stable", "start"),
        ("preexisting-restart-stable", "restart"),
    ],
)
def test_endpoint_start_adopts_stable_compatible_job(
    scenario: str,
    job_type: str,
) -> None:
    """A start request can observe an exact stable start or restart job."""
    result = _run_activation_observer_fixture(
        scenario=scenario,
        observation_timeout_seconds=5,
        activation_action="start",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert f"job_type={job_type}" in stdout
    assert "fixture.first_outcome=active" in stdout
    assert "fixture.attempts=0" in stdout


def test_endpoint_restart_known_equal_invocation_does_not_use_transition_fallback() -> None:
    """Known-equal invocation IDs disprove restart completion after a transition."""
    result = _run_activation_observer_fixture(
        scenario="restart-same-invocation",
        observation_timeout_seconds=2,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=unverified" in stdout
    assert "fixture.attempts=0" in stdout
    assert "active_state=active" in stderr
    assert "invocation_id=old-invocation" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_restart_without_invocation_or_transition_remains_unverified() -> None:
    """A vanished restart job alone cannot prove an always-active unit restarted."""
    result = _run_activation_observer_fixture(
        scenario="restart-active-unknown-invocation",
        observation_timeout_seconds=2,
        activation_action="restart",
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=unverified" in stdout
    assert "fixture.attempts=0" in stdout
    assert "active_state=active" in stderr
    assert "invocation_id=" in stderr
    assert "outcome=active" not in stdout


def test_endpoint_activation_timeout_preserves_job_without_duplicate_start() -> None:
    """Observer expiry leaves an activating job intact and retry only observes it."""
    result = _run_activation_observer_fixture(
        scenario="stuck",
        observation_timeout_seconds=2,
        repeat_after_failure_count=1,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 1
    assert "fixture.first_outcome=in-progress" in stdout
    assert "fixture.final_outcome=in-progress" in stdout
    assert "fixture.attempts=1" in stdout
    assert "active_state=activating" in stderr
    assert "job_id=101" in stderr


def test_endpoint_activation_concurrent_fresh_shell_does_not_duplicate_job() -> None:
    """The per-service lock closes preflight/enqueue TOCTOU across processes."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate cross-process activation locking")
    helper = deployment.render_bounded_user_service_activation_helper(
        observation_timeout_seconds=2,
        poll_seconds=1,
        progress_seconds=1,
    )
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT
mkdir -p "$test_root/bin" "$test_root/home"
export HOME="$test_root/home" FAKE_SYSTEMD_ROOT="$test_root"
cat > "$test_root/bin/systemctl" <<'__FAKE_SYSTEMCTL__'
#!/usr/bin/env bash
set -u
case "${{2:-}}" in
  start|restart)
    attempts=0
    if [ -f "$FAKE_SYSTEMD_ROOT/attempts" ]; then
      attempts="$(cat "$FAKE_SYSTEMD_ROOT/attempts")"
    fi
    printf '%s\n' "$((attempts + 1))" > "$FAKE_SYSTEMD_ROOT/attempts"
    : > "$FAKE_SYSTEMD_ROOT/start-entered"
    sleep 1
    : > "$FAKE_SYSTEMD_ROOT/enqueued"
    ;;
  show)
    state=inactive
    sub_state=dead
    invocation=old-invocation
    control_pid=0
    if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ]; then
      state=active
      sub_state=running
      invocation=new-invocation
      control_pid=61
    fi
    printf '%s\n' 'LoadState=loaded' "ActiveState=$state" \
      "SubState=$sub_state" 'Result=success' "ControlPID=$control_pid" \
      'ExecMainCode=0' 'ExecMainStatus=0' 'TimeoutStartUSec=5min' \
      "InvocationID=$invocation"
    ;;
  list-jobs)
    if [ -f "$FAKE_SYSTEMD_ROOT/enqueued" ]; then
      printf '101 clio-relay-worker-test.service start running\n'
    fi
    ;;
  *) exit 2 ;;
esac
__FAKE_SYSTEMCTL__
cat > "$test_root/bin/flock" <<'__FAKE_FLOCK__'
#!/usr/bin/env bash
set -u
case "${{1:-}}" in
  --exclusive)
    while ! mkdir "$FAKE_SYSTEMD_ROOT/activation-flock" 2>/dev/null; do sleep 0.05; done
    ;;
  --unlock) rmdir "$FAKE_SYSTEMD_ROOT/activation-flock" ;;
  *) exit 2 ;;
esac
__FAKE_FLOCK__
chmod +x "$test_root/bin/systemctl" "$test_root/bin/flock"
export PATH="$test_root/bin:$PATH"
cat > "$test_root/run-helper" <<'__RUN_HELPER__'
set -u
CLIO_RELAY_ENDPOINT_SERVICE_NAME=clio-relay-worker-test.service
CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION=start
{helper}
status=0
clio_relay_endpoint_activate_bounded || status=$?
printf 'fresh.outcome=%s\n' "$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME"
exit "$status"
__RUN_HELPER__
bash "$test_root/run-helper" > "$test_root/first.out" 2> "$test_root/first.err" &
first_pid=$!
while [ ! -f "$test_root/start-entered" ]; do sleep 0.05; done
bash "$test_root/run-helper" > "$test_root/second.out" 2> "$test_root/second.err" &
second_pid=$!
first_status=0
wait "$first_pid" || first_status=$?
second_status=0
wait "$second_pid" || second_status=$?
cat "$test_root/first.out" "$test_root/second.out"
cat "$test_root/first.err" "$test_root/second.err" >&2
printf 'fixture.first_status=%s\n' "$first_status"
printf 'fixture.second_status=%s\n' "$second_status"
printf 'fixture.attempts=%s\n' "$(cat "$test_root/attempts")"
"""
    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=15,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert "fixture.first_status=1" in stdout
    assert "fixture.second_status=1" in stdout
    assert stdout.count("fresh.outcome=in-progress") == 2
    assert "fixture.attempts=1" in stdout
    assert "active_state=active" in stderr
    assert "job_id=101" in stderr


def test_cluster_doctor_rejects_enabled_but_inactive_endpoint_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Doctor reports the persistent-worker stall before unrelated tool checks."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the cluster doctor service check")
    rendered = doctor._cluster_doctor_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            jarvis_bin="jarvis-test",
            frpc_bin="frpc-test",
        )
    )
    harness = f"""set -u
systemctl() {{
  case "${{2:-}}" in
    is-enabled) echo enabled ;;
    is-active) echo inactive ;;
  esac
}}
{rendered}
"""
    original_run = subprocess.run

    def run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        assert command == ["ssh", "test-host", "bash", "-s"]
        assert capture_output is True
        assert check is False
        return original_run(
            [bash, "-s"],
            input=harness.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=10,
        )

    monkeypatch.setattr(doctor.subprocess, "run", run)

    with pytest.raises(RelayError, match="endpoint service is enabled but not active"):
        run_cluster_doctor(
            ClusterDefinition(
                name="test-cluster",
                ssh_host="test-host",
                jarvis_bin="jarvis-test",
                frpc_bin="frpc-test",
            )
        )


def test_endpoint_user_service_uses_cluster_executable_overrides() -> None:
    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            jarvis_bin="/opt/jarvis/current",
            spack_executable="/opt/site/spack/bin/spack",
            frpc_bin="/opt/frp/frpc",
            agent_bin="/opt/agents/clio",
        ),
    )

    assert 'Environment="CLIO_RELAY_JARVIS_BIN=/opt/jarvis/current"' in rendered
    assert 'Environment="JARVIS_MCP_SPACK_COMMAND=/opt/site/spack/bin/spack"' in rendered
    assert 'Environment="CLIO_RELAY_FRPC_BIN=/opt/frp/frpc"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_BIN=/opt/agents/clio"' in rendered
    assert "UnsetEnvironment=JARVIS_MCP_SPACK_COMMAND" not in rendered


def test_endpoint_user_service_unsets_absent_optional_manager_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stale systemd-manager overrides cannot leak into a generated worker unit."""
    monkeypatch.delenv("CLIO_RELAY_JARVIS_MCP_COMMAND", raising=False)

    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
    )

    assert "UnsetEnvironment=CLIO_RELAY_JARVIS_MCP_COMMAND" in rendered
    assert "UnsetEnvironment=JARVIS_MCP_SPACK_COMMAND" in rendered


def test_endpoint_user_service_passes_optional_jarvis_mcp_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CLIO_RELAY_JARVIS_MCP_COMMAND",
        '["uvx","--from","git+https://github.com/iowarp/clio-kit.git@branch","clio-kit"]',
    )

    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(name="test-cluster", ssh_host="test-host"),
    )

    assert (
        'Environment="CLIO_RELAY_JARVIS_MCP_COMMAND=[\\"uvx\\",\\"--from\\",'
        '\\"git+https://github.com/iowarp/clio-kit.git@branch\\",\\"clio-kit\\"]"'
    ) in rendered
    assert "UnsetEnvironment=CLIO_RELAY_JARVIS_MCP_COMMAND" not in rendered


def test_endpoint_user_service_escapes_arbitrary_labels_and_values() -> None:
    """Systemd rendering cannot turn operator values into directives or unit paths."""
    rendered = render_endpoint_user_service(
        cluster='Target GPU %n "quoted"\nExecStart=/bin/false',
        definition=ClusterDefinition(
            name="Target GPU",
            ssh_host="target-gpu",
            agent_bin='/opt/agent "current" %n',
        ),
    )

    assert rendered.count("\nExecStart=") == 1
    assert rendered.count("\nEnvironment=") == 9
    assert "\\nExecStart=/bin/false" in rendered
    assert "%%n" in rendered
    assert 'CLIO_RELAY_AGENT_BIN=/opt/agent \\"current\\" %%n' in rendered


def test_endpoint_service_install_uses_safe_unit_name_and_bounded_ssh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        observed.update(command=command, input=input, timeout=timeout)
        assert capture_output is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, b"installed\n", b"")

    monkeypatch.setattr(deployment.subprocess, "run", run)

    lines = install_endpoint_user_service_over_ssh(
        cluster='Target GPU %n "quoted"',
        ssh_host="target-gpu",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=False,
        enable=False,
        timeout_seconds=15,
    )

    assert lines == ["installed"]
    assert observed["command"] == ["ssh", "target-gpu", "bash", "-s"]
    assert observed["timeout"] == 15
    script = cast(bytes, observed["input"]).decode("utf-8")
    assert "clio-relay-worker-k2-" in script
    assert "Target GPU" not in script


def test_endpoint_service_install_requires_linger_before_any_mutation() -> None:
    """A persistent install cannot write or control a unit under a login-scoped manager."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint installer")
    script = deployment._remote_install_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=True,
        enable=True,
        require_persistent=True,
    )
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
export HOME="$test_root/home" USER=test-user
loginctl() {{ echo no; }}
systemctl() {{ echo "unexpected-systemctl=$*" >&2; return 99; }}
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=10,
    )
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 78
    assert "persistent endpoint service requires systemd user lingering" in stderr
    assert "loginctl enable-linger test-user" in stderr
    assert "unexpected-systemctl" not in stderr
    assert script.index('if [ "$linger" = "yes" ]') < script.index('mkdir -p "$HOME')


@pytest.mark.parametrize(
    ("linger", "require_persistent", "expected_mode", "expected_warning"),
    [
        ("yes", True, "systemd-user-linger", None),
        (
            "no",
            False,
            "login-scoped",
            "endpoint service is login-scoped and may stop after the final login exits",
        ),
    ],
)
def test_endpoint_service_install_verifies_enabled_active_and_persistence_mode(
    linger: str,
    require_persistent: bool,
    expected_mode: str,
    expected_warning: str | None,
) -> None:
    """Successful installs report exact persistence, enabled, and active states."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint installer")
    script = deployment._remote_install_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=True,
        enable=True,
        require_persistent=require_persistent,
    )
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
export HOME="$test_root/home" USER=test-user FAKE_LINGER={linger}
loginctl() {{ echo "$FAKE_LINGER"; }}
mkdir -p "$test_root/bin"
export FAKE_SYSTEMD_ROOT="$test_root"
cat > "$test_root/bin/systemctl" <<'__FAKE_SYSTEMCTL__'
#!/usr/bin/env bash
set -u
echo "systemctl=$*" >&2
case "${{2:-}}" in
  is-enabled) echo enabled ;;
  is-active) echo active ;;
  is-system-running) echo running ;;
  restart) : > "$FAKE_SYSTEMD_ROOT/restarted" ;;
  list-jobs) ;;
  show)
    invocation=old-invocation
    if [ -f "$FAKE_SYSTEMD_ROOT/restarted" ]; then
      invocation=new-invocation
    fi
    printf '%s\n' 'LoadState=loaded' 'ActiveState=active' \
      'SubState=running' 'Result=success' 'ControlPID=42' \
      'ExecMainCode=0' 'ExecMainStatus=0' 'TimeoutStartUSec=5min' \
      "InvocationID=$invocation"
    ;;
esac
__FAKE_SYSTEMCTL__
cat > "$test_root/bin/flock" <<'__FAKE_FLOCK__'
#!/usr/bin/env bash
case "${{1:-}}" in --exclusive|--unlock) exit 0 ;; *) exit 2 ;; esac
__FAKE_FLOCK__
chmod +x "$test_root/bin/systemctl" "$test_root/bin/flock"
export PATH="$test_root/bin:$PATH"
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=10,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert f"linger={linger}" in stdout
    assert f"endpoint_service.persistence={expected_mode}" in stdout
    assert "endpoint_service.enabled=enabled" in stdout
    assert "endpoint_service.active=active" in stdout
    assert "systemctl=--user daemon-reload" in stderr
    assert "systemctl=--user enable clio-relay-worker-test.service" in stderr
    assert "systemctl=--user restart --no-block clio-relay-worker-test.service" in stderr
    if expected_warning is None:
        assert "login-scoped" not in stderr
    else:
        assert expected_warning in stderr


@pytest.mark.parametrize(
    ("enabled_state", "active_state", "expected_error"),
    [
        ("disabled", "active", "endpoint service is not enabled"),
        ("enabled", "inactive", "endpoint service is not active"),
    ],
)
def test_endpoint_service_install_rejects_unverified_service_state(
    enabled_state: str,
    active_state: str,
    expected_error: str,
) -> None:
    """A requested enabled/running deployment fails when systemd disproves either state."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint installer")
    script = deployment._remote_install_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        service_text="[Service]\nExecStart=/bin/true\n",
        start=True,
        enable=True,
        require_persistent=True,
    )
    harness = f"""set -u
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
export HOME="$test_root/home" USER=test-user
loginctl() {{ echo yes; }}
mkdir -p "$test_root/bin"
export FAKE_SYSTEMD_ROOT="$test_root"
cat > "$test_root/bin/systemctl" <<'__FAKE_SYSTEMCTL__'
#!/usr/bin/env bash
set -u
case "${{2:-}}" in
  is-enabled) echo {enabled_state} ;;
  is-active) echo {active_state} ;;
  is-system-running) echo running ;;
  restart) : > "$FAKE_SYSTEMD_ROOT/restarted" ;;
  list-jobs) ;;
  show)
    invocation=old-invocation
    if [ -f "$FAKE_SYSTEMD_ROOT/restarted" ]; then
      invocation=new-invocation
    fi
    printf '%s\n' 'LoadState=loaded' 'ActiveState=active' \
      'SubState=running' 'Result=success' 'ControlPID=42' \
      'ExecMainCode=0' 'ExecMainStatus=0' 'TimeoutStartUSec=5min' \
      "InvocationID=$invocation"
    ;;
esac
__FAKE_SYSTEMCTL__
cat > "$test_root/bin/flock" <<'__FAKE_FLOCK__'
#!/usr/bin/env bash
case "${{1:-}}" in --exclusive|--unlock) exit 0 ;; *) exit 2 ;; esac
__FAKE_FLOCK__
chmod +x "$test_root/bin/systemctl" "$test_root/bin/flock"
export PATH="$test_root/bin:$PATH"
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 1
    assert expected_error in result.stderr.decode("utf-8", errors="replace")


def test_endpoint_service_install_rejects_unsafe_or_unbounded_ssh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal called
        called = True
        raise AssertionError("unsafe destination must fail before SSH")

    monkeypatch.setattr(deployment.subprocess, "run", run)
    with pytest.raises(RelayError, match="non-option destination"):
        install_endpoint_user_service_over_ssh(
            cluster="target",
            ssh_host="-oProxyCommand=evil",
            service_text="[Service]\nExecStart=/bin/true\n",
            start=False,
            enable=False,
        )
    with pytest.raises(RelayError, match="finite and positive"):
        install_endpoint_user_service_over_ssh(
            cluster="target",
            ssh_host="target",
            service_text="[Service]\nExecStart=/bin/true\n",
            start=False,
            enable=False,
            timeout_seconds=float("nan"),
        )
    assert called is False


def test_endpoint_service_install_reports_ssh_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def timeout(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd=["ssh"], timeout=2)

    monkeypatch.setattr(deployment.subprocess, "run", timeout)

    with pytest.raises(RelayError, match="exceeded 2 seconds"):
        install_endpoint_user_service_over_ssh(
            cluster="target",
            ssh_host="target",
            service_text="[Service]\nExecStart=/bin/true\n",
            start=False,
            enable=False,
            timeout_seconds=2,
        )
