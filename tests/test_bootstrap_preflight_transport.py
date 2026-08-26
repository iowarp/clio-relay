"""The bootstrap preflight must not send its script as a command-line argument.

Found driving the ares self-install (clio-relay#158). The preflight embedded
its ~13 KB script in argv:

    _run(["ssh", ssh_host, remote_command])

Some ssh clients silently TRUNCATE a long command-line argument. The MSYS2
OpenSSH shipped with Git for Windows -- the default `ssh` on a Windows
developer box -- drops everything past roughly 8-10 KB. The remote shell then
receives a script cut off mid-token and reports

    bash: -c: line 11: unexpected EOF while looking for matching `''

which names neither the truncation nor the transport, so a client-side
transport limit is misread as a malformed script. Measured on the real host:
an 8117-byte command arrived intact, a 10117-byte command did not.

Size of the payload must not be a function of argv, so the script travels over
stdin -- the same shape ``session_remote_scripts._ssh_script`` already uses.
"""

from __future__ import annotations

import base64
import json
import shlex
import subprocess
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from pytest import MonkeyPatch

import clio_relay.bootstrap as bootstrap
import clio_relay.bootstrap_receipt_validation as bootstrap_receipt_validation
from clio_relay.bootstrap_constants import BOOTSTRAP_PERSISTENT_RECEIPT_PATH
from clio_relay.bootstrap_one_pass_script import (
    ONE_PASS_ARCHIVE_HEREDOC_DELIMITER,
    ONE_PASS_CLEANUP_TRAP_MARKER,
    ONE_PASS_CREATED_FLAG_NAME,
    ONE_PASS_PERSISTENT_RECEIPT_MARKER,
    ONE_PASS_SCRIPT_HEREDOC_DELIMITER,
    ONE_PASS_TARGET_IDENTITY_MARKER,
    extract_one_pass_payloads,
    parse_one_pass_persistent_receipt,
    parse_one_pass_target_identity,
    render_one_pass_cold_bootstrap_script,
)
from clio_relay.errors import RelayError


class _Captured:
    def __init__(self) -> None:
        self.command: list[str] = []
        self.input_bytes: bytes | None = None


def _capture(monkeypatch: MonkeyPatch) -> _Captured:
    captured = _Captured()

    def fake_run(
        command: list[str],
        *,
        input_bytes: bytes | None = None,
        **_kwargs: object,
    ) -> Any:
        captured.command = command
        captured.input_bytes = input_bytes

        class _Result:
            stdout = "bootstrap_preflight_unsupported=not_installed\n"
            stderr = ""
            returncode = 0

        return _Result()

    monkeypatch.setattr(bootstrap, "_run", fake_run)
    return captured


def _desired_state() -> Any:
    from clio_relay.bootstrap_reconcile import BootstrapDesiredState

    return cast(
        Any,
        BootstrapDesiredState.model_validate(
            {
                "schema_version": "clio-relay.bootstrap-desired-state.v1",
                "bootstrap_profile": "linux-user",
                "cluster": "ares-p5run2",
                "core_dir": "/mnt/common/u/relay-core",
                "spool_dir": "/mnt/common/u/relay-spool",
                "relay_install_spec": "clio-relay==1.6.6",
                "relay_artifact_sha256": "a" * 64,
                "relay_source_identity": "release:clio-relay==1.6.6:sha256:" + "a" * 64,
                "uv_version": "0.11.28",
                "uv_sha256": "b" * 64,
                "frp_version": "0.69.1",
                "frpc_sha256": "c" * 64,
                "frps_sha256": "d" * 64,
                "jarvis_cd_version": "1.8.0",
                "jarvis_cd_wheel_url": "https://example.test/jarvis_cd-1.8.0-py3-none-any.whl",
                "jarvis_cd_wheel_sha256": "e" * 64,
                "jarvis_util_commit": "f" * 40,
                "clio_kit_version": "2.7.2",
                "clio_kit_install_spec": "https://example.test/clio_kit-2.7.2-py3-none-any.whl",
                "clio_kit_artifact_sha256": "0" * 64,
                "jarvis_root": "~/.ppi-jarvis",
                "jarvis_config_dir": "~/.local/share/clio-relay/jarvis-config",
                "jarvis_private_dir": "~/.local/share/clio-relay/jarvis-private",
                "jarvis_shared_dir": "~/.local/share/clio-relay/jarvis-shared",
                "jarvis_resource_graph_profile": "ares",
                "allow_jarvis_resource_graph_build": False,
                "managed_jarvis_repo": "~/.local/share/clio-relay/clio_relay",
                "agent_adapter": "exec",
                "agent_args": [],
                "agent_npm_bin": None,
                "agent_npm_package": None,
                "worker_service": "clio-relay-worker-ares-p5run2.service",
            }
        ),
    )


def test_preflight_sends_its_script_over_stdin_not_argv(monkeypatch: MonkeyPatch) -> None:
    captured = _capture(monkeypatch)

    bootstrap._bootstrap_preflight_over_ssh(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ssh_host="ares",
        invocation_id="bootstrap_test",
        desired=_desired_state(),
        core_dir="/mnt/common/u/relay-core",
        spool_dir="/mnt/common/u/relay-spool",
        repair=False,
        timeout_seconds=30.0,
    )

    assert captured.command == ["ssh", "ares", "bash", "-s"]
    assert captured.input_bytes is not None
    # The script is real, and large enough that argv delivery was the hazard.
    assert b"bootstrap_preflight_unsupported=not_installed" in captured.input_bytes
    assert len(captured.input_bytes) > 8 * 1024


def test_no_preflight_argument_can_be_truncated_by_a_client_limit(
    monkeypatch: MonkeyPatch,
) -> None:
    """No single argv element may carry script-sized content.

    Guards the property rather than the current shape: whatever the command
    becomes, its arguments stay far below the ~8 KB where real ssh clients
    start dropping bytes.
    """
    captured = _capture(monkeypatch)

    bootstrap._bootstrap_preflight_over_ssh(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ssh_host="ares",
        invocation_id="bootstrap_test",
        desired=_desired_state(),
        core_dir="/mnt/common/u/relay-core",
        spool_dir="/mnt/common/u/relay-spool",
        repair=False,
        timeout_seconds=30.0,
    )

    assert max(len(argument) for argument in captured.command) < 1024


# --- clio-relay#209: one-pass cold bootstrap ------------------------------
#
# The tests below extend the stdin/argv transport-harness pattern above to
# the ONE combined cold-install pass: `bootstrap_one_pass_script.py` renders
# it, and `bootstrap_ssh_deploy.py` parses its framed stdout. Nothing here
# dials anywhere -- the composition tests inspect rendered script text
# directly, and the dial-count conformance tests monkeypatch `bootstrap._run`
# exactly like the harness above.


def test_one_pass_script_composes_inline_payloads_and_self_cleaning_trap() -> None:
    """The rendered script carries both payloads inline and self-cleans on any exit."""
    archive_bytes = b"\x00\x01clio-relay-archive-bytes\xff" * 50
    install_script = "echo installing\nprint('bootstrap_receipt_json={}')\n"

    script = render_one_pass_cold_bootstrap_script(
        remote_root="/tmp/clio-relay-bootstrap_composition",
        archive_bytes=archive_bytes,
        install_script=install_script,
    )

    # Payloads travel on stdin as base64 blocks -- never scp'd separately --
    # and round-trip exactly.
    recovered_archive, recovered_script = extract_one_pass_payloads(script)
    assert recovered_archive == archive_bytes
    assert recovered_script == install_script

    # Framing: both new marker-producing steps are present, after the real
    # install script runs.
    assert 'bash "$CLIO_RELAY_ONE_PASS_ROOT/clio-relay-bootstrap.sh"' in script
    assert ONE_PASS_PERSISTENT_RECEIPT_MARKER in script
    assert ONE_PASS_TARGET_IDENTITY_MARKER in script
    install_index = script.index('bash "$CLIO_RELAY_ONE_PASS_ROOT/clio-relay-bootstrap.sh"')
    assert install_index < script.index(ONE_PASS_PERSISTENT_RECEIPT_MARKER)
    assert install_index < script.index(ONE_PASS_TARGET_IDENTITY_MARKER)

    # The staging directory self-cleans on ANY exit -- success, a failed
    # install, or a dropped connection after the process starts -- via an
    # EXIT trap, never a second ssh dial.
    assert ONE_PASS_CLEANUP_TRAP_MARKER in script
    assert 'rm -rf -- "$CLIO_RELAY_ONE_PASS_ROOT"' in script
    trap_index = script.index(ONE_PASS_CLEANUP_TRAP_MARKER)
    mkdir_index = script.index('mkdir -- "$CLIO_RELAY_ONE_PASS_ROOT"')
    assert trap_index < mkdir_index, "the trap must be armed before anything it must clean up"

    # M3: the trap is armed before mkdir (so a failing mkdir is still
    # handled), but must never rm -rf a pre-existing directory it did not
    # create. The created-flag is declared 0 before the trap, set to 1 only
    # AFTER mkdir succeeds, and the trap's own rm -rf is gated on it.
    assert f"{ONE_PASS_CREATED_FLAG_NAME}=0" in script
    assert f"{ONE_PASS_CREATED_FLAG_NAME}=1" in script
    flag_zeroed_index = script.index(f"{ONE_PASS_CREATED_FLAG_NAME}=0")
    flag_set_index = script.index(f"{ONE_PASS_CREATED_FLAG_NAME}=1")
    assert flag_zeroed_index < trap_index < mkdir_index < flag_set_index
    guard_line = f'if [ "${{{ONE_PASS_CREATED_FLAG_NAME}}}" = "1" ]; then'
    assert guard_line in script
    # The guard lives INSIDE the cleanup function body, which is defined
    # (and so appears in the text) before the `trap ... EXIT` statement that
    # arms it -- the meaningful ordering is that the guard gates the rm -rf
    # that follows it in that same function body.
    guard_index = script.index(guard_line)
    assert guard_index < trap_index
    assert guard_index < script.index('rm -rf -- "$CLIO_RELAY_ONE_PASS_ROOT"')

    # L3: base64 availability is checked with the same explicit, named
    # failure as the python3 check below it -- never a bare "command not
    # found" from the decode step itself.
    assert "command -v base64" in script
    assert script.index("command -v base64") < script.index(ONE_PASS_ARCHIVE_HEREDOC_DELIMITER)

    # No argv element may be script-sized: only stdin heredoc bodies carry
    # the payload (the same truncation hazard the preflight script documents,
    # #158). The rendered script must never be handed to ssh as a `bash -c`
    # argument -- the caller sends it whole, over stdin, to `bash -s`.
    assert "bash -c" not in script

    # L1: the script is always joined with bare "\n" and ships to a POSIX
    # shell over stdin -- a stray "\r" (e.g. from a Windows-edited template)
    # would corrupt heredoc delimiters and shebang-adjacent lines silently.
    assert "\r" not in script


def test_one_pass_script_payload_size_is_not_argv_bound() -> None:
    """A multi-megabyte archive stays entirely inside heredoc bodies."""
    archive_bytes = b"x" * (4 * 1024 * 1024)

    script = render_one_pass_cold_bootstrap_script(
        remote_root="/tmp/clio-relay-bootstrap_large",
        archive_bytes=archive_bytes,
        install_script="true\n",
    )

    recovered_archive, _install_script = extract_one_pass_payloads(script)
    assert recovered_archive == archive_bytes
    # The delimiters bound each payload block; nothing about the archive's
    # own base64 text ever needs to be a single shell word.
    assert ONE_PASS_ARCHIVE_HEREDOC_DELIMITER in script
    assert ONE_PASS_SCRIPT_HEREDOC_DELIMITER in script


def _identity_which(executable: str) -> str:
    """Model ``shutil.which`` finding every tool already on PATH, unchanged."""
    return executable


def _resolved_bash() -> str:
    """Resolve a real bash, exactly matching the sibling embedded-script tests' check."""
    import shutil

    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the one-pass cold bootstrap script")
    return bash


def _staging_dir_exists(bash: str, remote_root: str) -> bool:
    """Ask bash itself whether a path exists.

    Both `remote_root` and bash's own synthetic $HOME below are plain
    POSIX paths bash creates and resolves itself (`/tmp/...`, `mktemp -d`) --
    never a Windows path handed to a WSL-hosted `bash.exe`, which does not
    understand `C:\\...` as an absolute path.
    """
    probe = subprocess.run(
        [bash, "-c", f'[ -e "{remote_root}" ]'],
        capture_output=True,
        check=False,
        timeout=10,
    )
    return probe.returncode == 0


def test_one_pass_script_runs_end_to_end_under_a_real_shell(tmp_path: Path) -> None:
    """A real bash proves mkdir/decode/install/receipt-reread/identity/cleanup.

    Entirely local: no ssh anywhere. Only exercises the SCRIPT the desktop
    would send over stdin, run by a real interpreter against a synthetic
    HOME, matching the embedded-script driver pattern already established by
    this module's siblings (test_bootstrap_cluster_paths.py).

    The persistent receipt the fake install script "writes" is captured from
    the REAL production writer (``write_bootstrap_receipt`` ->
    ``_atomic_json``, which pretty-prints multi-line ``indent=2`` JSON) --
    not a hand-written single-line literal. A hand-written fixture is exactly
    how the B1 regression (the post-install step framing the receipt file's
    raw multi-line bytes as one key=value line, corrupting every real cold
    bootstrap at desktop parse) passed every test while still being broken:
    every fixture agreed with itself, and none of them matched the real
    writer's actual output shape. Capturing real bytes here means any future
    drift in that writer's format is caught here too.
    """
    from clio_relay.bootstrap_reconcile_receipt import write_bootstrap_receipt

    bash = _resolved_bash()
    remote_root = f"/tmp/clio-relay-bootstrap_test_{uuid4().hex}"

    receipt: dict[str, object] = {"invocation_id": "bootstrap_e2e", "outcome": "installed"}
    captured_receipt_path = tmp_path / "captured-real-receipt.json"
    write_bootstrap_receipt(captured_receipt_path, receipt)
    real_receipt_bytes = captured_receipt_path.read_bytes()
    assert real_receipt_bytes.count(b"\n") > 1, (
        "the real writer must still be pretty-printing multi-line JSON -- "
        "if this fails, the writer changed shape and B1's regression guard "
        "is no longer proving anything"
    )
    encoded_receipt = base64.b64encode(real_receipt_bytes).decode("ascii")

    fake_install_script = (
        'mkdir -p "$HOME/.local/share/clio-relay"\n'
        'base64 -d > "$HOME/.local/share/clio-relay/bootstrap-receipt.json" '
        "<<'RECEIPT_B64'\n" + encoded_receipt + "\n"
        "RECEIPT_B64\n"
        "printf 'bootstrap_receipt=%s\\n' "
        '"$HOME/.local/share/clio-relay/bootstrap-receipt.json"\n'
        "printf 'bootstrap_receipt_json=%s\\n' "
        + shlex.quote(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
        + "\n"
    )
    script = render_one_pass_cold_bootstrap_script(
        remote_root=remote_root,
        archive_bytes=b"end-to-end archive bytes",
        install_script=fake_install_script,
    )
    # bash creates its own scratch HOME (mktemp -d is bash-side, never a
    # Windows path); remote_root above is a plain /tmp path for the same
    # reason.
    harness = f'export HOME="$(mktemp -d)"\n{script}'

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    stdout = result.stdout.decode("utf-8", errors="replace")
    assert "bootstrap_receipt_json=" in stdout
    persistent_line = next(
        line for line in stdout.splitlines() if line.startswith(ONE_PASS_PERSISTENT_RECEIPT_MARKER)
    )
    assert json.loads(persistent_line.partition("=")[2]) == {
        "invocation_id": "bootstrap_e2e",
        "outcome": "installed",
    }
    identity_line = next(
        line for line in stdout.splitlines() if line.startswith(ONE_PASS_TARGET_IDENTITY_MARKER)
    )
    identity = json.loads(identity_line.partition("=")[2])
    assert identity["hostnames"]
    # The remote EXIT trap removed its own staging directory -- no cleanup
    # dial exists to assert on.
    assert not _staging_dir_exists(bash, remote_root)


def test_one_pass_script_self_cleans_on_a_failed_install() -> None:
    """A failing install still self-cleans and preserves the failing exit code."""
    bash = _resolved_bash()
    remote_root = f"/tmp/clio-relay-bootstrap_test_{uuid4().hex}"

    script = render_one_pass_cold_bootstrap_script(
        remote_root=remote_root,
        archive_bytes=b"archive bytes",
        install_script='echo "simulated install failure" >&2\nexit 7\n',
    )
    harness = f'export HOME="$(mktemp -d)"\n{script}'

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 7
    assert "simulated install failure" in result.stderr.decode("utf-8", errors="replace")
    assert result.stdout.decode("utf-8", errors="replace") == ""
    assert not _staging_dir_exists(bash, remote_root)


def test_one_pass_script_never_deletes_a_pre_existing_staging_directory() -> None:
    """M3: a mkdir collision must not vanish a directory the script did not create.

    Before the created-flag guard, the trap is armed before `mkdir` (on
    purpose -- a failing mkdir must still be handled), which meant a
    pre-existing directory at the SAME path failed `mkdir` (no `-p`) and the
    trap then `rm -rf`'d that directory anyway: a mkdir collision reported to
    the operator as a transport failure while actually deleting unrelated
    state.
    """
    bash = _resolved_bash()
    remote_root = f"/tmp/clio-relay-bootstrap_test_{uuid4().hex}"

    script = render_one_pass_cold_bootstrap_script(
        remote_root=remote_root,
        archive_bytes=b"archive bytes",
        install_script="true\n",
    )
    sentinel_marker = "sentinel-not-created-by-this-run"
    harness = (
        f'export HOME="$(mktemp -d)"\n'
        f'mkdir -- "{remote_root}"\n'
        f'echo "{sentinel_marker}" > "{remote_root}/sentinel"\n'
        f"{script}"
    )

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert result.returncode != 0, "a pre-existing staging directory must fail, not proceed"
    # The directory -- and the caller's unrelated file inside it -- must
    # still exist: the created-flag guard means the trap never ran rm -rf
    # against a path this run did not create.
    still_present = subprocess.run(
        [bash, "-c", f'cat -- "{remote_root}/sentinel"'],
        capture_output=True,
        check=False,
        timeout=10,
    )
    assert still_present.returncode == 0
    assert sentinel_marker in still_present.stdout.decode("utf-8", errors="replace")

    # Cleanup: this test creates real /tmp state on purpose to prove the
    # trap did NOT remove it; remove it ourselves now that the proof is done.
    subprocess.run([bash, "-c", f'rm -rf -- "{remote_root}"'], check=False, timeout=10)


def _stdin_guard_receipt_script(body: str) -> str:
    """A fake install script that writes a receipt after ``body`` runs first.

    ``body`` is a stdin-consuming step (``read``, a bare ``cat``) that would
    eat the REST of the outer combined script if the install step were not
    isolated from it -- proving H2 requires the post-install steps to still
    run correctly afterward, not just that the install step itself survives.
    """
    return (
        f"{body}\n"
        'mkdir -p "$HOME/.local/share/clio-relay"\n'
        "cat > \"$HOME/.local/share/clio-relay/bootstrap-receipt.json\" <<'RECEIPT'\n"
        '{"invocation_id": "bootstrap_stdin_guard", "outcome": "installed"}\n'
        "RECEIPT\n"
        "printf 'bootstrap_receipt=%s\\n' "
        '"$HOME/.local/share/clio-relay/bootstrap-receipt.json"\n'
        "printf 'bootstrap_receipt_json=%s\\n' "
        '\'{"invocation_id": "bootstrap_stdin_guard", "outcome": "installed"}\'\n'
    )


def test_one_pass_script_install_step_reading_stdin_does_not_truncate_the_rest() -> None:
    """H2: an install step doing `read` must not eat the outer script's tail.

    Before the `</dev/null` fix, the install child shared fd 0 with the
    outer `bash -s` process still parsing its own remaining source from that
    SAME stream -- a `read` inside the install script silently consumed the
    post-install python step's own text, and the run failed downstream with
    a misleading "python3 is required" (the consumed bytes, not a missing
    interpreter).
    """
    bash = _resolved_bash()
    remote_root = f"/tmp/clio-relay-bootstrap_test_{uuid4().hex}"

    script = render_one_pass_cold_bootstrap_script(
        remote_root=remote_root,
        archive_bytes=b"archive bytes",
        install_script=_stdin_guard_receipt_script("read -r clio_relay_test_unused || true"),
    )
    harness = f'export HOME="$(mktemp -d)"\n{script}'

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    stdout = result.stdout.decode("utf-8", errors="replace")
    assert any(line.startswith(ONE_PASS_PERSISTENT_RECEIPT_MARKER) for line in stdout.splitlines())
    assert any(line.startswith(ONE_PASS_TARGET_IDENTITY_MARKER) for line in stdout.splitlines())
    assert not _staging_dir_exists(bash, remote_root)


def test_one_pass_script_install_step_draining_stdin_does_not_truncate_the_rest() -> None:
    """H2: an install step that drains ALL of stdin (a bare `cat`) is equally isolated.

    A worse case than a single `read`: `cat` with no arguments reads until
    EOF, which -- without `</dev/null` -- would consume every remaining byte
    of the outer script, silently skipping every post-install step (no
    receipt re-verification, no identity observation) with no error at all.
    """
    bash = _resolved_bash()
    remote_root = f"/tmp/clio-relay-bootstrap_test_{uuid4().hex}"

    script = render_one_pass_cold_bootstrap_script(
        remote_root=remote_root,
        archive_bytes=b"archive bytes",
        install_script=_stdin_guard_receipt_script("cat >/dev/null"),
    )
    harness = f'export HOME="$(mktemp -d)"\n{script}'

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    stdout = result.stdout.decode("utf-8", errors="replace")
    assert any(line.startswith(ONE_PASS_PERSISTENT_RECEIPT_MARKER) for line in stdout.splitlines())
    assert any(line.startswith(ONE_PASS_TARGET_IDENTITY_MARKER) for line in stdout.splitlines())
    assert not _staging_dir_exists(bash, remote_root)


def test_one_pass_persistent_receipt_parsing_accepts_matching_evidence() -> None:
    receipt: dict[str, object] = {"invocation_id": "bootstrap_ok", "outcome": "installed"}
    lines = [
        "some install diagnostic line",
        ONE_PASS_PERSISTENT_RECEIPT_MARKER + json.dumps(receipt, sort_keys=True),
    ]

    parse_one_pass_persistent_receipt(lines, receipt=receipt)


def test_one_pass_persistent_receipt_parsing_rejects_mismatched_evidence() -> None:
    receipt: dict[str, object] = {"invocation_id": "bootstrap_ok", "outcome": "installed"}
    persisted = {"invocation_id": "bootstrap_ok", "outcome": "different"}
    lines = [ONE_PASS_PERSISTENT_RECEIPT_MARKER + json.dumps(persisted, sort_keys=True)]

    with pytest.raises(RelayError, match="differs from current stdout evidence"):
        parse_one_pass_persistent_receipt(lines, receipt=receipt)


def test_one_pass_persistent_receipt_parsing_rejects_missing_marker() -> None:
    with pytest.raises(RelayError, match="exactly one persistent receipt"):
        parse_one_pass_persistent_receipt(["no marker here"], receipt={})


def test_one_pass_persistent_receipt_parsing_rejects_duplicate_marker() -> None:
    receipt: dict[str, object] = {"invocation_id": "bootstrap_ok"}
    line = ONE_PASS_PERSISTENT_RECEIPT_MARKER + json.dumps(receipt)
    with pytest.raises(RelayError, match="exactly one persistent receipt"):
        parse_one_pass_persistent_receipt([line, line], receipt=receipt)


def test_one_pass_persistent_receipt_parsing_rejects_malformed_json() -> None:
    lines = [ONE_PASS_PERSISTENT_RECEIPT_MARKER + "{not json"]

    with pytest.raises(RelayError, match="not valid JSON"):
        parse_one_pass_persistent_receipt(lines, receipt={})


def test_one_pass_target_identity_parsing_accepts_well_formed_evidence() -> None:
    identity = {
        "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
        "hostnames": ["ares-login-1", "ares-login-1.example.test"],
        "site_marker_sha256": "a" * 64,
    }
    lines = [ONE_PASS_TARGET_IDENTITY_MARKER + json.dumps(identity, sort_keys=True)]

    parsed = parse_one_pass_target_identity(lines)

    assert parsed == identity


def test_one_pass_target_identity_parsing_accepts_null_site_marker() -> None:
    identity = {
        "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
        "hostnames": ["ares-login-1"],
        "site_marker_sha256": None,
    }
    lines = [ONE_PASS_TARGET_IDENTITY_MARKER + json.dumps(identity, sort_keys=True)]

    parsed = parse_one_pass_target_identity(lines)

    assert parsed["site_marker_sha256"] is None


def test_one_pass_target_identity_parsing_rejects_missing_marker() -> None:
    with pytest.raises(RelayError, match="exactly one target identity"):
        parse_one_pass_target_identity(["nothing framed here"])


def test_one_pass_target_identity_parsing_rejects_wrong_schema() -> None:
    identity = {
        "schema_version": "clio-relay.cluster-target-info.v1",
        "hostnames": ["ares-login-1"],
    }
    lines = [ONE_PASS_TARGET_IDENTITY_MARKER + json.dumps(identity)]

    with pytest.raises(RelayError, match="schema did not match"):
        parse_one_pass_target_identity(lines)


def test_one_pass_target_identity_parsing_rejects_empty_hostnames() -> None:
    identity: dict[str, object] = {
        "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
        "hostnames": [],
        "site_marker_sha256": None,
    }
    lines = [ONE_PASS_TARGET_IDENTITY_MARKER + json.dumps(identity)]

    with pytest.raises(RelayError, match="omitted its observed hostnames"):
        parse_one_pass_target_identity(lines)


def test_one_pass_target_identity_parsing_rejects_malformed_json() -> None:
    lines = [ONE_PASS_TARGET_IDENTITY_MARKER + "not json at all"]

    with pytest.raises(RelayError, match="not valid JSON"):
        parse_one_pass_target_identity(lines)


def test_one_pass_target_identity_parsing_rejects_non_string_site_marker() -> None:
    identity = {
        "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
        "hostnames": ["ares-login-1"],
        "site_marker_sha256": 12345,
    }
    lines = [ONE_PASS_TARGET_IDENTITY_MARKER + json.dumps(identity)]

    with pytest.raises(RelayError, match="site marker was not a string"):
        parse_one_pass_target_identity(lines)


# --- Budget conformance: this is the slice's acceptance --------------------


def _cold_bootstrap_fake_run(
    *,
    receipt: dict[str, object],
    identity: dict[str, object],
    calls: list[list[str]],
    scripts: list[str],
) -> Any:
    def fake_run(
        command: list[str],
        *,
        input_bytes: bytes | None = None,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] != "ssh" or command[-2:] != ["bash", "-s"]:
            raise AssertionError(
                f"clio-relay#209 budget violation: unexpected remote dial {command}"
            )
        script = (input_bytes or b"").decode("utf-8")
        scripts.append(script)
        if "CLIO_RELAY_ONE_PASS_ROOT=" not in script:
            return subprocess.CompletedProcess(
                command, 0, "bootstrap_preflight_unsupported=not_installed\n", ""
            )
        stdout = (
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json\n"
            "bootstrap_receipt_json="
            + json.dumps(receipt, sort_keys=True, separators=(",", ":"))
            + "\n"
            + ONE_PASS_PERSISTENT_RECEIPT_MARKER
            + json.dumps(receipt, sort_keys=True, separators=(",", ":"))
            + "\n"
            + ONE_PASS_TARGET_IDENTITY_MARKER
            + json.dumps(identity, sort_keys=True, separators=(",", ":"))
            + "\n"
        )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    return fake_run


def test_cold_bootstrap_costs_exactly_two_ssh_dials(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#209 acceptance: a cold bootstrap issues EXACTLY the expected dials.

    Enumerated: (1) the preflight discovery pass (unavoidable -- it is what
    tells warm from cold) and (2) the ONE combined install pass this slice
    introduces. That is the total authenticated ssh session count for a cold
    bootstrap, <= the 2-dial budget docs/connection-model.md:141-157 sets
    (the held link is a later, separate phase and is not one of these two).
    This test must fail the moment any code path adds a third dial back.
    """
    receipt: dict[str, object] = {
        "schema_version": "clio-relay.bootstrap-receipt.v1",
        "invocation_id": "bootstrap_budget",
        "bootstrap_profile": "linux-user",
        "relay_install_spec": "clio-relay==1.0.0",
        "install_receipt_sha256": "a" * 64,
        "completed_at": "2026-08-26T00:00:00Z",
    }
    identity: dict[str, object] = {
        "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
        "hostnames": ["ares-login-1"],
        "site_marker_sha256": None,
    }
    calls: list[list[str]] = []
    scripts: list[str] = []

    def fake_create_bootstrap_archive(
        *, source_root: Path, archive: Path, relay_wheel: Path | None
    ) -> bootstrap.BootstrapArchive:
        del source_root, relay_wheel
        archive.write_bytes(b"budget-test-archive")
        return bootstrap.BootstrapArchive(
            archive=archive, install_spec=f"clio-relay=={bootstrap.__version__}"
        )

    def fake_render_linux_user_bootstrap_script(**_kwargs: object) -> str:
        return "print('bootstrap_receipt_json={}')\n"

    def validate_receipt(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", fake_create_bootstrap_archive)
    monkeypatch.setattr(
        bootstrap, "render_linux_user_bootstrap_script", fake_render_linux_user_bootstrap_script
    )
    monkeypatch.setattr(
        bootstrap_receipt_validation, "validate_bootstrap_receipt", validate_receipt
    )
    monkeypatch.setattr(
        bootstrap,
        "_run",
        _cold_bootstrap_fake_run(receipt=receipt, identity=identity, calls=calls, scripts=scripts),
    )
    monkeypatch.setattr(bootstrap, "uuid4", lambda: type("Uuid", (), {"hex": "budget"})())
    monkeypatch.setattr(bootstrap.shutil, "which", _identity_which)

    bootstrap.bootstrap_cluster_over_ssh(
        bootstrap_profile="linux-user",
        ssh_host="ares",
        source_root=tmp_path,
        relay_artifact_sha256="a" * 64,
        jarvis_resource_graph_profile="ares",
    )

    assert len(calls) == 2, f"clio-relay#209 budget violation: expected 2 dials, observed {calls}"
    assert all(command == ["ssh", "ares", "bash", "-s"] for command in calls)
    assert "CLIO_RELAY_ONE_PASS_ROOT=" not in scripts[0], "dial 1 must be the lightweight preflight"
    assert "CLIO_RELAY_ONE_PASS_ROOT=" in scripts[1], "dial 2 must be the combined install pass"
    recovered_archive, _install_script = extract_one_pass_payloads(scripts[1])
    assert recovered_archive == b"budget-test-archive"


def test_warm_bootstrap_still_costs_exactly_two_ssh_dials(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The warm no-op fast path is untouched by clio-relay#209 and stays at 2 dials.

    Preflight's own 1-dial-ness is proven directly elsewhere in this module
    (test_preflight_sends_its_script_over_stdin_not_argv); this test proves
    the SECOND dial -- persistent-receipt re-verification -- is the only one
    a warm bootstrap adds, and that the one-pass cold-install path is never
    reached.
    """
    receipt: dict[str, object] = {
        "schema_version": "clio-relay.bootstrap-receipt.v1",
        "invocation_id": "bootstrap_warm",
        "bootstrap_profile": "linux-user",
        "relay_install_spec": f"clio-relay=={bootstrap.__version__}",
        "install_receipt_sha256": "a" * 64,
        "completed_at": "2026-08-26T00:00:00Z",
    }

    def preflight(**kwargs: object) -> bootstrap.BootstrapPreflightResult:
        assert isinstance(kwargs["invocation_id"], str)
        return bootstrap.BootstrapPreflightResult(
            action="exact", receipt=receipt, lines=["bootstrap_preflight_json={}"]
        )

    def poison(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "clio-relay#209: the warm no-op path must never reach the one-pass cold install"
        )

    calls: list[list[str]] = []

    def fake_run(
        command: list[str], *, timeout_seconds: float | None = None, **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds
        calls.append(command)
        if command[-2:] == ["cat", BOOTSTRAP_PERSISTENT_RECEIPT_PATH]:
            return subprocess.CompletedProcess(command, 0, json.dumps(receipt), "")
        raise AssertionError(f"clio-relay#209 budget violation: unexpected remote dial {command}")

    def validate_receipt(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)
    monkeypatch.setattr(bootstrap, "_validate_relay_bootstrap_wheel", poison)
    monkeypatch.setattr(bootstrap, "render_linux_user_bootstrap_script", poison)
    monkeypatch.setattr(
        bootstrap_receipt_validation, "validate_bootstrap_receipt", validate_receipt
    )
    monkeypatch.setattr(bootstrap, "_run", fake_run)
    monkeypatch.setattr(bootstrap.shutil, "which", _identity_which)

    lines = bootstrap.bootstrap_cluster_over_ssh(
        bootstrap_profile="linux-user",
        ssh_host="ares",
        source_root=tmp_path,
        relay_artifact_sha256="a" * 64,
        jarvis_resource_graph_profile="ares",
    )

    # Only the persistent-receipt re-verification dial is issued here;
    # preflight itself (mocked at the function boundary) is proven to be
    # exactly one further dial by this module's other tests.
    assert calls == [["ssh", "ares", "cat", BOOTSTRAP_PERSISTENT_RECEIPT_PATH]]
    assert any(line.startswith("bootstrap_receipt_json=") for line in lines)
