"""Composition tests for the frpc proxy bring-up/teardown/status bash scripts.

clio-relay#279, adversarial-review fix pass (D3/D5/D6/D7 + minors). Follows
``tests/test_bootstrap_preflight_transport.py``'s harness style: most tests
inspect RENDERED TEXT only (no process spawned); several drive a real
``bash`` (never ssh -- ``bash -n``/execution of a LOCAL script is not a
network dial) to prove the install script's file-writing, lingering gate,
partial-install cleanup, and active-after-install re-check run correctly
under a real shell, using a synthetic ``$HOME`` and a stub ``loginctl``/
``systemctl`` on ``PATH``.

Every scenario below was ALSO reproduced live against a real systemd user
instance (WSL, this development box) before being encoded as a portable
stub here: the lingering gate returned real exit 78 against a real
``loginctl`` reporting ``Linger=no``; and, running the install script for
real against a genuinely-missing ``frpc`` binary, the reused activation
observer itself sampled the unit mid-crash-loop and reported success
(``outcome=active``) -- and the SEPARATE, independent ``service_active``
re-check this fix adds then observed the unit had already crashed again
(``activating``/``auto-restart``), refused with exit 1, and the partial-
install trap removed the env file and printed
``FrpcProxyPartialInstall=``. That live run is the reason the stub below
reproduces the exact same two-phase shape (activation helper "succeeds",
independent re-check catches the flip) rather than a simpler one-shot
failure.
"""

from __future__ import annotations

import shutil
import subprocess
from uuid import uuid4

import pytest

from clio_relay.errors import RelayError
from clio_relay.frpc_proxy_receipt import BRINGUP_RECEIPT_SCHEMA, TEARDOWN_RECEIPT_SCHEMA
from clio_relay.frpc_proxy_scripts import (
    render_frpc_proxy_install_script,
    render_frpc_proxy_status_script,
    render_frpc_proxy_teardown_script,
)
from clio_relay.frpc_unit import frpc_proxy_paths

_PATHS = frpc_proxy_paths("ares")
_TOML_TEXT = 'serverAddr = "relay.example.org"\nauth.token = "{{ .Envs.CLIO_RELAY_FRP_TOKEN }}"\n'
_ENV_TEXT = 'CLIO_RELAY_FRP_TOKEN="TOKEN-VALUE-ABC"\nCLIO_RELAY_STCP_SECRET="SECRET-VALUE-XYZ"\n'
_UNIT_TEXT = "[Unit]\nDescription=clio-relay frpc proxy for ares\n\n[Service]\nType=simple\n"


def _install_script(*, require_persistent: bool = True) -> str:
    return render_frpc_proxy_install_script(
        cluster="ares",
        proxy_name="ares-owned-session",
        paths=_PATHS,
        toml_text=_TOML_TEXT,
        env_text=_ENV_TEXT,
        unit_text=_UNIT_TEXT,
        token_env="CLIO_RELAY_FRP_TOKEN",
        secret_env="CLIO_RELAY_STCP_SECRET",
        require_persistent=require_persistent,
    )


# --- install script composition --------------------------------------------


def test_install_script_writes_all_three_files_with_umask_077() -> None:
    script = _install_script()

    assert "umask 077" in script
    assert f'> "{_PATHS.toml_shell_path}"' in script
    assert f'> "{_PATHS.env_shell_path}"' in script
    assert f'> "{_PATHS.unit_shell_path}"' in script
    assert f'chmod 600 -- "{_PATHS.toml_shell_path}"' in script
    assert f'chmod 600 -- "{_PATHS.env_shell_path}"' in script


def test_install_script_env_file_content_is_the_real_secret_never_the_template() -> None:
    script = _install_script()

    # The rendered env text (real secret values) travels in the install
    # script -- it is what WRITES the secret-bearing file -- but the TOML's
    # own template placeholder must still be the only thing that lands in
    # the TOML file text embedded here.
    assert "TOKEN-VALUE-ABC" in script
    assert "SECRET-VALUE-XYZ" in script
    assert "{{ .Envs.CLIO_RELAY_FRP_TOKEN }}" in script


def test_install_script_enables_and_activates_via_the_shared_activation_helper() -> None:
    script = _install_script()

    assert "systemctl --user daemon-reload </dev/null" in script
    assert "systemctl --user enable clio-relay-frpc-proxy-ares.service </dev/null" in script
    assert "CLIO_RELAY_ENDPOINT_SERVICE_NAME=clio-relay-frpc-proxy-ares.service" in script
    assert "CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION=restart" in script
    assert "clio_relay_endpoint_activate_bounded" in script


def test_install_script_emits_the_typed_framed_receipt_with_config_digest() -> None:
    script = _install_script()

    assert f"printf 'FrpcProxyReceiptSchema=%s\\n' {BRINGUP_RECEIPT_SCHEMA}" in script
    assert "printf 'FrpcProxyCluster=%s\\n' ares" in script
    assert "printf 'FrpcProxyName=%s\\n' ares-owned-session" in script
    assert "printf 'FrpcProxyUnitName=%s\\n' clio-relay-frpc-proxy-ares.service" in script
    assert "printf 'FrpcProxyConfigSha256=%s\\n'" in script
    assert "printf 'FrpcProxyEnabled=%s\\n' \"$service_enabled_bool\"" in script
    assert "printf 'FrpcProxyInstalledAt=%s\\n'" in script


def test_install_script_rejects_a_cluster_name_with_an_embedded_newline() -> None:
    with pytest.raises(RelayError):
        render_frpc_proxy_install_script(
            cluster="ares\nmalicious",
            proxy_name="ares-owned-session",
            paths=_PATHS,
            toml_text=_TOML_TEXT,
            env_text=_ENV_TEXT,
            unit_text=_UNIT_TEXT,
            token_env="CLIO_RELAY_FRP_TOKEN",
            secret_env="CLIO_RELAY_STCP_SECRET",
        )


def test_install_script_never_uses_argv_sized_bash_c() -> None:
    """The whole script is sent over ssh's stdin, never as a `bash -c` argument."""
    assert "bash -c" not in _install_script()


def test_install_script_has_no_stray_carriage_returns() -> None:
    assert "\r" not in _install_script()


# --- D6 (script layer): active-after-install re-check ----------------------


def test_install_script_re_checks_active_state_after_the_activation_helper() -> None:
    """D6 script-layer guard: never trust the activation helper's own success alone."""
    script = _install_script()

    activation_index = script.index("clio_relay_endpoint_activate_bounded")
    recheck_index = script.index('if [ "$service_active" != "active" ]')
    receipt_index = script.index("printf 'FrpcProxyReceiptSchema=%s\\n'")

    assert activation_index < recheck_index < receipt_index
    assert "frpc proxy unit is not active after install" in script
    assert "exit 1" in script[recheck_index : receipt_index + 1]
    assert "printf 'FrpcProxyActive=%s\\n' true" in script


def test_install_script_active_recheck_reads_is_active_independently() -> None:
    script = _install_script()

    assert (
        'service_active="$(systemctl --user is-active clio-relay-frpc-proxy-ares.service '
        '2>/dev/null || true)"' in script
    )


# --- D7: lingering gate ------------------------------------------------------


def test_install_script_lingering_gate_precedes_every_file_write() -> None:
    script = _install_script(require_persistent=True)

    gate_index = script.index("exit 78")
    first_write_index = script.index("printf '%s'")
    assert gate_index < first_write_index
    assert "require_persistent=1" in script
    assert "loginctl show-user" in script
    assert "requires systemd user lingering" in script
    assert "loginctl enable-linger" in script
    assert "--allow-login-scoped" in script


def test_install_script_login_scoped_opt_out_skips_the_hard_gate() -> None:
    """``require_persistent=0`` at RUNTIME skips the ``elif`` branch that exits 78.

    The ``exit 78`` line itself stays present in the rendered TEXT either
    way (it is the body of a conditional branch, not conditionally
    rendered) -- what changes is the ``require_persistent`` value the
    script tests at runtime, proven separately by the real-shell
    end-to-end tests below.
    """
    script = _install_script(require_persistent=False)

    assert "require_persistent=0" in script
    assert "login-scoped and may stop after the final login exits" in script


# --- D5: partial-install cleanup trap ---------------------------------------


def test_install_script_arms_a_cleanup_trap_before_writing_the_secret_file() -> None:
    script = _install_script()

    trap_index = script.index("trap clio_relay_frpc_proxy_partial_cleanup EXIT")
    env_write_index = script.index(f'> "{_PATHS.env_shell_path}"')
    assert trap_index < env_write_index
    assert "ENV_FILE_WRITTEN=0" in script
    assert "ENV_FILE_WRITTEN=1" in script
    zero_index = script.index("ENV_FILE_WRITTEN=0")
    one_index = script.index("ENV_FILE_WRITTEN=1")
    assert zero_index < trap_index < env_write_index < one_index


def test_install_script_partial_cleanup_removes_only_the_env_file() -> None:
    script = _install_script()

    trap_body_start = script.index("clio_relay_frpc_proxy_partial_cleanup() {")
    trap_body_end = script.index("trap clio_relay_frpc_proxy_partial_cleanup EXIT")
    trap_body = script[trap_body_start:trap_body_end]

    assert f'rm -f -- "{_PATHS.env_shell_path}"' in trap_body
    assert _PATHS.toml_shell_path not in trap_body
    assert _PATHS.unit_shell_path not in trap_body
    assert "FrpcProxyPartialInstall=" in trap_body
    assert "teardown-proxy" in trap_body


# --- empty-secret guard ------------------------------------------------------


def test_install_script_refuses_to_enable_when_a_secret_reads_back_empty() -> None:
    script = _install_script()

    env_write_index = script.index(f'> "{_PATHS.env_shell_path}"')
    enable_index = script.index("systemctl --user enable")
    guard_region = script[env_write_index:enable_index]

    assert "grep -qE" in guard_region
    assert 'CLIO_RELAY_FRP_TOKEN="..*"' in guard_region
    assert 'CLIO_RELAY_STCP_SECRET="..*"' in guard_region
    assert "did not bind a non-empty value for CLIO_RELAY_FRP_TOKEN" in guard_region
    assert "did not bind a non-empty value for CLIO_RELAY_STCP_SECRET" in guard_region
    # Never sourced as shell: a secret containing `$(...)` must not execute.
    assert not any(line.strip().startswith(". ") for line in guard_region.splitlines())
    assert "\n. " not in guard_region


# --- teardown script composition --------------------------------------------


def test_teardown_script_disables_before_removing_files() -> None:
    script = render_frpc_proxy_teardown_script(cluster="ares", paths=_PATHS)

    disable_index = script.index("systemctl --user disable --now")
    rm_index = script.index("rm -f --")
    assert disable_index < rm_index
    assert "</dev/null" in script
    assert f'"{_PATHS.unit_shell_path}"' in script
    assert f'"{_PATHS.toml_shell_path}"' in script
    assert f'"{_PATHS.env_shell_path}"' in script


def test_teardown_script_tolerates_disable_failing_on_a_not_installed_unit() -> None:
    script = render_frpc_proxy_teardown_script(cluster="ares", paths=_PATHS)

    assert "systemctl --user disable --now clio-relay-frpc-proxy-ares.service </dev/null" in script
    assert "2>/dev/null || true" in script


def test_teardown_script_emits_the_typed_framed_receipt() -> None:
    script = render_frpc_proxy_teardown_script(cluster="ares", paths=_PATHS)

    assert f"printf 'FrpcProxyTeardownSchema=%s\\n' {TEARDOWN_RECEIPT_SCHEMA}" in script
    assert "printf 'FrpcProxyRemovedUnit=%s\\n' \"$unit_existed\"" in script
    assert "printf 'FrpcProxyRemovedToml=%s\\n' \"$toml_existed\"" in script
    assert "printf 'FrpcProxyRemovedEnv=%s\\n' \"$env_existed\"" in script


def test_teardown_script_computes_existed_flags_before_the_disable_call() -> None:
    script = render_frpc_proxy_teardown_script(cluster="ares", paths=_PATHS)

    existed_check_index = script.index("unit_existed=false")
    disable_index = script.index("systemctl --user disable")
    assert existed_check_index < disable_index


# --- status script composition -----------------------------------------------


def test_status_script_is_read_only() -> None:
    script = render_frpc_proxy_status_script(unit_name=_PATHS.unit_name)

    assert "systemctl --user show" in script
    assert "--property=LoadState" in script
    assert "--property=ActiveState" in script
    assert "--property=SubState" in script
    assert "--property=UnitFileState" in script
    for mutating in ("enable", "disable", "restart", "start", "stop", " rm "):
        assert mutating not in script


def test_status_script_base64_encodes_only_the_journal_tail() -> None:
    script = render_frpc_proxy_status_script(unit_name=_PATHS.unit_name)

    assert "journalctl --user --unit=clio-relay-frpc-proxy-ares.service" in script
    assert "base64" in script
    assert "JournalTailBase64=" in script


# --- real-shell execution proofs ---------------------------------------------


def _resolved_bash() -> str:
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the frpc proxy install script")
    return bash


def _bash_c(bash: str, command: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run([bash, "-c", command], capture_output=True, check=False, timeout=15)


def _bash_has_required_tools() -> bool:
    """Probe INSIDE bash's own PATH, not the host OS's.

    ``shutil.which`` searches the Windows ``PATH``, which cannot see
    ``flock``/``timeout`` when ``bash`` itself resolves to a WSL-hosted
    ``bash.exe`` -- those coreutils live only inside the WSL filesystem. This
    probes the actual interpreter the test will drive, exactly like it will
    be invoked.
    """
    bash = shutil.which("bash")
    if bash is None:
        return False
    probe = subprocess.run(
        [bash, "-c", "command -v flock >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1"],
        capture_output=True,
        check=False,
        timeout=15,
    )
    return probe.returncode == 0


_REQUIRES_BASH_TOOLS = pytest.mark.skipif(
    not _bash_has_required_tools(),
    reason="requires a POSIX bash + flock + timeout (present under WSL/Git-Bash+coreutils)",
)

# A minimal stub that makes the reused activation helper fail FAST by
# reporting the unit as never having existed. `LoadState=not-found` is
# systemd's real report for a unit that does not exist;
# `clio_relay_endpoint_activate_bounded` (reused unchanged from
# `deployment_activation.py`) returns 1 immediately on that state, without
# any polling wait.
_FAKE_SYSTEMCTL_NOT_FOUND = """#!/usr/bin/env bash
set -eu
if [ "$1" = "--user" ] && [ "$2" = "daemon-reload" ]; then exit 0; fi
if [ "$1" = "--user" ] && [ "$2" = "enable" ]; then exit 0; fi
if [ "$1" = "--user" ] && [ "$2" = "show" ]; then
  printf 'LoadState=not-found\\n'
  printf 'ActiveState=inactive\\n'
  printf 'SubState=dead\\n'
  printf 'Result=success\\n'
  printf 'ControlPID=0\\n'
  printf 'ExecMainCode=0\\n'
  printf 'ExecMainStatus=0\\n'
  printf 'TimeoutStartUSec=0\\n'
  printf 'InvocationID=inv-0\\n'
  exit 0
fi
if [ "$1" = "--user" ] && [ "$2" = "list-jobs" ]; then exit 0; fi
echo "unexpected systemctl invocation: $*" >&2
exit 99
"""

# A stub reproducing the EXACT live-proven D6 scenario: the activation
# helper's own preflight read sees the unit inactive, issues a restart, and
# the FIRST post-restart read already reports active with a fresh
# invocation id -- exactly the shape that makes
# `clio_relay_endpoint_activate_bounded` declare success (proven live: a
# real crash-looping unit's own restart cycle can land a `show` sample on
# the brief "active" moment between crashes). The SEPARATE `is-active`
# call this fix's D6 script-layer guard makes immediately afterward reports
# the unit already crashed again -- proven live to be a genuinely different
# answer than the sample the observer itself saw moments earlier.
_FAKE_SYSTEMCTL_SUCCESS_THEN_CRASHED = """#!/usr/bin/env bash
set -eu
if [ "$1" = "--user" ] && [ "$2" = "daemon-reload" ]; then exit 0; fi
if [ "$1" = "--user" ] && [ "$2" = "enable" ]; then exit 0; fi
if [ "$1" = "--user" ] && [ "$2" = "list-jobs" ]; then exit 0; fi
if [ "$1" = "--user" ] && [ "$2" = "restart" ]; then exit 0; fi
if [ "$1" = "--user" ] && [ "$2" = "is-enabled" ]; then echo enabled; exit 0; fi
if [ "$1" = "--user" ] && [ "$2" = "is-active" ]; then echo activating; exit 3; fi
if [ "$1" = "--user" ] && [ "$2" = "show" ]; then
  count_file="$CLIO_TEST_STATE_DIR/show_calls"
  count=0
  [ -f "$count_file" ] && count="$(cat "$count_file")"
  count=$((count + 1))
  echo "$count" > "$count_file"
  if [ "$count" -le 1 ]; then
    printf 'LoadState=loaded\\n'
    printf 'ActiveState=inactive\\n'
    printf 'SubState=dead\\n'
    printf 'InvocationID=inv-0\\n'
  else
    printf 'LoadState=loaded\\n'
    printf 'ActiveState=active\\n'
    printf 'SubState=running\\n'
    printf 'InvocationID=inv-1\\n'
  fi
  printf 'Result=success\\n'
  printf 'ControlPID=123\\n'
  printf 'ExecMainCode=0\\n'
  printf 'ExecMainStatus=0\\n'
  printf 'TimeoutStartUSec=0\\n'
  exit 0
fi
echo "unexpected systemctl invocation: $*" >&2
exit 99
"""


def _run_install_harness(
    *,
    fake_systemctl_body: str,
    require_persistent: bool = False,
    fake_loginctl_body: str | None = None,
) -> tuple[subprocess.CompletedProcess[bytes], str]:
    """Run ``_install_script()`` under a real bash with a stub systemctl on PATH.

    Returns the completed process and the ``$HOME``-rooted root path used, so
    callers can inspect (or assert the absence of) files afterward via a
    SEPARATE ``bash -c`` call -- the harness process itself may already have
    exited non-zero by the time the caller wants to look.
    """
    bash = _resolved_bash()
    root = f"/tmp/clio-relay-frpc-proxy-test_{uuid4().hex}"
    lines = [
        f'export HOME="{root}/home"',
        'mkdir -p "$HOME"',
        f'export CLIO_TEST_STATE_DIR="{root}/state"',
        'mkdir -p "$CLIO_TEST_STATE_DIR"',
        f'export CLIO_FAKE_BIN="{root}/bin"',
        'mkdir -p "$CLIO_FAKE_BIN"',
        "cat > \"$CLIO_FAKE_BIN/systemctl\" <<'__FAKE_SYSTEMCTL__'",
        fake_systemctl_body,
        "__FAKE_SYSTEMCTL__",
        'chmod +x "$CLIO_FAKE_BIN/systemctl"',
    ]
    if fake_loginctl_body is not None:
        lines += [
            "cat > \"$CLIO_FAKE_BIN/loginctl\" <<'__FAKE_LOGINCTL__'",
            fake_loginctl_body,
            "__FAKE_LOGINCTL__",
            'chmod +x "$CLIO_FAKE_BIN/loginctl"',
        ]
    lines.append('export PATH="$CLIO_FAKE_BIN:$PATH"')
    lines.append(_install_script(require_persistent=require_persistent))
    harness = "\n".join(lines)
    result = subprocess.run(
        [bash, "-s"], input=harness.encode("utf-8"), capture_output=True, check=False, timeout=30
    )
    return result, root


@_REQUIRES_BASH_TOOLS
def test_install_script_runs_under_a_real_shell_and_reports_a_clean_failure() -> None:
    """A real bash proves file-writing + activation-helper wiring + D5's trap.

    The stub `systemctl` reports the unit as never having been installed, so
    the reused activation helper fails immediately without waiting out a
    real timeout -- and because that failure lands AFTER the env file was
    written, D5's partial-install trap fires: the env file is removed and a
    `FrpcProxyPartialInstall=` line names what happened. A SECOND, separate
    ``bash -c`` call (matching this module's own established precedent) reads
    the surviving files back after the first process's ``exit 1`` has
    already ended that session.
    """
    result, root = _run_install_harness(fake_systemctl_body=_FAKE_SYSTEMCTL_NOT_FOUND)
    bash = _resolved_bash()

    try:
        assert result.returncode == 1
        stderr = result.stderr.decode("utf-8", errors="replace")
        assert "frpc proxy unit did not become active" in stderr
        assert "frpc proxy install failed partway" in stderr
        # Verification-pass fix: assert the marker's SHAPE, not substrings --
        # a malformed printf (two shell words recycling the format) previously
        # emitted the record twice with unit= carrying the remediation hint
        # and no trailing newline, while both substrings still matched.
        assert stderr.count("FrpcProxyPartialInstall=") == 1
        assert (
            "FrpcProxyPartialInstall=unit=clio-relay-frpc-proxy-ares.service "
            "toml_written=true env_removed=true next=teardown-proxy\n"
        ) in stderr
        stdout = result.stdout.decode("utf-8", errors="replace")
        assert "FrpcProxyReceiptSchema=" not in stdout

        toml_path = f"{root}/home/.config/clio-relay/frpc-proxy-ares.toml"
        env_path = f"{root}/home/.config/clio-relay/frpc-proxy-ares.env"
        unit_path = f"{root}/home/.config/systemd/user/clio-relay-frpc-proxy-ares.service"

        # D5: the secret-bearing env file was removed by the partial-install trap...
        env_exists = _bash_c(bash, f'[ -e "{env_path}" ]')
        assert env_exists.returncode != 0

        # ...but the TOML and unit -- neither secret-bearing -- were left for inspection.
        toml_read = _bash_c(bash, f'cat "{toml_path}"')
        assert toml_read.returncode == 0
        assert toml_read.stdout.decode("utf-8") == _TOML_TEXT

        unit_read = _bash_c(bash, f'cat "{unit_path}"')
        assert unit_read.returncode == 0
        assert unit_read.stdout.decode("utf-8") == _UNIT_TEXT

        toml_mode = _bash_c(bash, f'stat -c %a "{toml_path}"')
        assert toml_mode.returncode == 0
        assert toml_mode.stdout.decode("utf-8").strip() == "600"
    finally:
        _bash_c(bash, f'rm -rf -- "{root}"')


@_REQUIRES_BASH_TOOLS
def test_install_script_active_recheck_catches_a_flip_the_activation_helper_missed() -> None:
    """D6 end-to-end: reproduces the live-proven false-success scenario.

    The activation helper's own internal read reports the unit transitioning
    to active (a fresh invocation id after the restart) and declares
    success -- exactly what was proven live to happen when a real crash-
    looping unit's restart cycle lands a sample on its brief "active"
    window. The script's SEPARATE, independent `is-active` re-check (this
    fix's D6 layer) then observes the unit has already crashed again and
    refuses: exit 1, no receipt, and (since the env file was already
    written) D5's trap removes it.
    """
    result, root = _run_install_harness(fake_systemctl_body=_FAKE_SYSTEMCTL_SUCCESS_THEN_CRASHED)
    bash = _resolved_bash()

    try:
        assert result.returncode == 1
        stderr = result.stderr.decode("utf-8", errors="replace")
        assert "frpc proxy unit is not active after install" in stderr
        assert stderr.count("FrpcProxyPartialInstall=") == 1
        assert (
            "FrpcProxyPartialInstall=unit=clio-relay-frpc-proxy-ares.service "
            "toml_written=true env_removed=true next=teardown-proxy\n"
        ) in stderr
        stdout = result.stdout.decode("utf-8", errors="replace")
        assert "FrpcProxyReceiptSchema=" not in stdout
        assert "FrpcProxyActive=" not in stdout

        env_path = f"{root}/home/.config/clio-relay/frpc-proxy-ares.env"
        env_exists = _bash_c(bash, f'[ -e "{env_path}" ]')
        assert env_exists.returncode != 0
    finally:
        _bash_c(bash, f'rm -rf -- "{root}"')


_FAKE_LOGINCTL_NO_LINGER = """#!/usr/bin/env bash
if [ "$1" = "show-user" ]; then
  echo no
  exit 0
fi
echo "unexpected loginctl invocation: $*" >&2
exit 1
"""


@_REQUIRES_BASH_TOOLS
def test_install_script_lingering_gate_refuses_before_writing_anything() -> None:
    """D7 end-to-end: a real bash + stub loginctl reproduces the live-proven exit 78.

    Reproduces exactly what this fix's own live-systemd verification
    observed against a real ``loginctl`` reporting ``Linger=no``: exit 78,
    the operator-facing remediation lines on stderr, and -- unlike the
    worker precedent, which writes its unit file before this check in an
    older code path this module never copied -- NOTHING written to disk.
    """
    bash = _resolved_bash()
    root = f"/tmp/clio-relay-frpc-proxy-test_{uuid4().hex}"
    harness = (
        f'export HOME="{root}/home"\n'
        'mkdir -p "$HOME"\n'
        f'export CLIO_FAKE_BIN="{root}/bin"\n'
        'mkdir -p "$CLIO_FAKE_BIN"\n'
        "cat > \"$CLIO_FAKE_BIN/loginctl\" <<'__FAKE_LOGINCTL__'\n"
        f"{_FAKE_LOGINCTL_NO_LINGER}"
        "__FAKE_LOGINCTL__\n"
        'chmod +x "$CLIO_FAKE_BIN/loginctl"\n'
        'export PATH="$CLIO_FAKE_BIN:$PATH"\n'
        f"{_install_script(require_persistent=True)}"
    )

    try:
        result = subprocess.run(
            [bash, "-s"],
            input=harness.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=15,
        )

        assert result.returncode == 78
        stderr = result.stderr.decode("utf-8", errors="replace")
        assert "requires systemd user lingering" in stderr
        assert "loginctl enable-linger" in stderr
        assert "--allow-login-scoped" in stderr

        config_dir_exists = _bash_c(bash, f'[ -e "{root}/home/.config/clio-relay" ]')
        assert config_dir_exists.returncode != 0
    finally:
        _bash_c(bash, f'rm -rf -- "{root}"')
