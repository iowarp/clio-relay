"""Composition tests for the frpc proxy bring-up/teardown/status bash scripts.

clio-relay#279. Follows ``tests/test_bootstrap_preflight_transport.py``'s
harness style: most tests inspect RENDERED TEXT only (no process spawned);
one test drives a real ``bash`` (never ssh -- ``bash -n``/execution of a
LOCAL script is not a network dial) to prove the install script's file-
writing and failure-reporting logic runs correctly under a real shell, using
a synthetic ``$HOME`` and a stub ``systemctl`` on ``PATH`` -- the same
"real interpreter against a synthetic HOME" pattern that module already
established.
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
_ENV_TEXT = "CLIO_RELAY_FRP_TOKEN=TOKEN-VALUE-ABC\nCLIO_RELAY_STCP_SECRET=SECRET-VALUE-XYZ\n"
_UNIT_TEXT = "[Unit]\nDescription=clio-relay frpc proxy for ares\n\n[Service]\nType=simple\n"


def _install_script() -> str:
    return render_frpc_proxy_install_script(
        cluster="ares",
        proxy_name="ares-owned-session",
        paths=_PATHS,
        toml_text=_TOML_TEXT,
        env_text=_ENV_TEXT,
        unit_text=_UNIT_TEXT,
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
    assert "printf 'FrpcProxyActive=%s\\n' \"$service_active_bool\"" in script
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
        )


def test_install_script_never_uses_argv_sized_bash_c() -> None:
    """The whole script is sent over ssh's stdin, never as a `bash -c` argument."""
    assert "bash -c" not in _install_script()


def test_install_script_has_no_stray_carriage_returns() -> None:
    assert "\r" not in _install_script()


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
    assert f'"{_PATHS.receipt_shell_path}"' in script


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


# --- real-bash execution proof (install script, failure path) ---------------


_FAKE_SYSTEMCTL_DELIMITER = "__CLIO_RELAY_FAKE_SYSTEMCTL__"

# A minimal stub that makes the reused activation helper fail FAST.
# `LoadState=not-found` is systemd's real report for a unit that does not
# exist; `clio_relay_endpoint_activate_bounded` (reused unchanged from
# `deployment_activation.py`) returns 1 immediately on that state, without
# any polling wait -- exactly the property this test needs to stay fast and
# deterministic while still proving the WHOLE install script (file writes,
# `daemon-reload`, `enable`, activation-helper wiring, failure reporting)
# runs correctly under a real shell.
_FAKE_SYSTEMCTL_BODY = """#!/usr/bin/env bash
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


@pytest.mark.skipif(
    not _bash_has_required_tools(),
    reason="requires a POSIX bash + flock + timeout (present under WSL/Git-Bash+coreutils)",
)
def test_install_script_runs_under_a_real_shell_and_reports_a_clean_failure() -> None:
    """A real bash proves file-writing + activation-helper wiring + failure surfacing.

    Entirely local: no ssh anywhere, only a real interpreter driving the
    exact script text ``frpc_proxy_bringup.py`` would send over ssh's
    stdin, against a synthetic ``$HOME`` -- the same pattern
    ``test_bootstrap_preflight_transport.py`` already established
    (``export HOME="$(mktemp -d)"``, a plain ``/tmp`` root generated in
    PYTHON rather than pytest's Windows ``tmp_path``, since this may run
    under a WSL-hosted ``bash.exe`` that does not understand a
    ``C:\\...`` path). The stub `systemctl` reports the unit as never
    having been installed, so the reused activation helper fails
    immediately without waiting out a real timeout. A SECOND, separate
    ``bash -c`` call (matching this module's own
    ``test_one_pass_script_never_deletes_a_pre_existing_staging_directory``
    precedent) reads the files back after the first process's ``exit 1``
    has already ended that session.
    """
    bash = _resolved_bash()
    root = f"/tmp/clio-relay-frpc-proxy-test_{uuid4().hex}"
    harness = (
        f'export HOME="{root}/home"\n'
        'mkdir -p "$HOME"\n'
        f'export CLIO_FAKE_BIN="{root}/bin"\n'
        'mkdir -p "$CLIO_FAKE_BIN"\n'
        f"cat > \"$CLIO_FAKE_BIN/systemctl\" <<'{_FAKE_SYSTEMCTL_DELIMITER}'\n"
        f"{_FAKE_SYSTEMCTL_BODY}"
        f"{_FAKE_SYSTEMCTL_DELIMITER}\n"
        'chmod +x "$CLIO_FAKE_BIN/systemctl"\n'
        'export PATH="$CLIO_FAKE_BIN:$PATH"\n'
        f"{_install_script()}"
    )

    try:
        result = subprocess.run(
            [bash, "-s"],
            input=harness.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=30,
        )

        assert result.returncode == 1
        stderr = result.stderr.decode("utf-8", errors="replace")
        assert "frpc proxy unit did not become active" in stderr
        stdout = result.stdout.decode("utf-8", errors="replace")
        assert "FrpcProxyReceiptSchema=" not in stdout

        toml_path = f"{root}/home/.config/clio-relay/frpc-proxy-ares.toml"
        env_path = f"{root}/home/.config/clio-relay/frpc-proxy-ares.env"
        unit_path = f"{root}/home/.config/systemd/user/clio-relay-frpc-proxy-ares.service"

        toml_read = _bash_c(bash, f'cat "{toml_path}"')
        assert toml_read.returncode == 0
        assert toml_read.stdout.decode("utf-8") == _TOML_TEXT

        env_read = _bash_c(bash, f'cat "{env_path}"')
        assert env_read.returncode == 0
        assert env_read.stdout.decode("utf-8") == _ENV_TEXT

        unit_read = _bash_c(bash, f'cat "{unit_path}"')
        assert unit_read.returncode == 0
        assert unit_read.stdout.decode("utf-8") == _UNIT_TEXT

        toml_mode = _bash_c(bash, f'stat -c %a "{toml_path}"')
        assert toml_mode.returncode == 0
        assert toml_mode.stdout.decode("utf-8").strip() == "600"

        env_mode = _bash_c(bash, f'stat -c %a "{env_path}"')
        assert env_mode.returncode == 0
        assert env_mode.stdout.decode("utf-8").strip() == "600"
    finally:
        _bash_c(bash, f'rm -rf -- "{root}"')
