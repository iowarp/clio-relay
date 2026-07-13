from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from clio_relay import process_containment
from clio_relay.errors import RelayError
from clio_relay.jarvis_provider import JarvisCdProvider


def test_embedded_containment_source_is_an_exact_isolated_runtime_mirror() -> None:
    root = Path(__file__).parents[1]
    source = root / "src" / "clio_relay" / "process_containment.py"
    embedded = root / "jarvis-packages" / "clio_relay" / "clio_relay" / "process_containment.py"

    assert embedded.read_bytes() == source.read_bytes()


def test_windows_recorded_cleanup_accepts_taskkill_failure_only_after_pid_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = cast(Any, process_containment)
    commands: list[list[str]] = []

    def failed_taskkill(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(
            command,
            1,
            "",
            "ERROR: There is no running instance of the task.",
        )

    def absent_after_taskkill(process_id: int) -> None:
        assert process_id == 4312
        return None

    monkeypatch.setattr(process_containment.subprocess, "run", failed_taskkill)
    monkeypatch.setattr(process_containment, "process_start_identity", absent_after_taskkill)

    private._terminate_recorded_windows_process_tree(4312, "windows-start:expected")

    assert commands == [["taskkill", "/PID", "4312", "/T", "/F"]]


@pytest.mark.parametrize(
    ("observed_identity", "error_match"),
    [
        ("windows-start:expected", "recorded process survived cleanup: 4312"),
        ("windows-start:replacement", "refused cleanup for reused process id 4312"),
    ],
)
def test_windows_recorded_cleanup_refuses_surviving_or_reused_pid(
    observed_identity: str,
    error_match: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = cast(Any, process_containment)

    def failed_taskkill(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, "", "taskkill failed")

    def observed_after_taskkill(process_id: int) -> str:
        assert process_id == 4312
        return observed_identity

    monkeypatch.setattr(process_containment.subprocess, "run", failed_taskkill)
    monkeypatch.setattr(process_containment, "process_start_identity", observed_after_taskkill)

    with pytest.raises(RuntimeError, match=error_match):
        private._terminate_recorded_windows_process_tree(4312, "windows-start:expected")


def test_secret_memory_gate_is_verified_or_explicitly_unsupported() -> None:
    if not sys.platform.startswith("linux"):
        with pytest.raises(RuntimeError, match="requires Linux PR_SET_DUMPABLE"):
            process_containment.enforce_linux_secret_memory_gate()
        return

    import ctypes

    process_containment.enforce_linux_secret_memory_gate()

    limits = Path("/proc/self/limits").read_text(encoding="utf-8")
    core_line = next(line for line in limits.splitlines() if line.startswith("Max core file size"))
    assert core_line.split()[-3:-1] == ["0", "0"]
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.restype = ctypes.c_int
    assert libc.prctl(3, 0, 0, 0, 0) == 0


def test_host_containment_capability_is_explicit() -> None:
    capability = process_containment.containment_capability()

    assert isinstance(capability.get("mode"), str)
    assert isinstance(capability.get("enforceable"), bool)
    assert isinstance(capability.get("reason"), str)
    if os.name == "nt":
        assert capability["mode"] == "windows_job_object"
        assert capability["enforceable"] is True


def test_broker_credential_never_enters_initial_environment_or_command_line() -> None:
    credential = json.dumps(
        {
            "schema_version": "test.credential.v1",
            "progress_file": "/private/progress.jsonl",
            "progress_token": "super-secret-progress-token",
            "runtime_file": "/private/runtime.jsonl",
            "runtime_token": "super-secret-token",
        },
        separators=(",", ":"),
    )
    script = r"""
import ctypes
import json
import os
import sys
from pathlib import Path

fd_text = os.environ.pop("CLIO_RELAY_BROKER_CREDENTIAL_FD", None)
ready_text = os.environ.pop("CLIO_RELAY_BROKER_READY_FD", None)
assert fd_text is not None and fd_text.isdecimal()
assert ready_text is not None and ready_text.isdecimal() and ready_text != fd_text
descriptor = int(fd_text)
ready_descriptor = int(ready_text)
os.set_inheritable(descriptor, False)
os.set_inheritable(ready_descriptor, False)
chunks = []
try:
    while chunk := os.read(descriptor, 4096):
        chunks.append(chunk)
finally:
    os.close(descriptor)
credential_bytes = b"".join(chunks)
credential_document = json.loads(credential_bytes)
runtime_file = credential_document["runtime_file"].encode()
runtime_token = credential_document["runtime_token"].encode()
progress_file = credential_document["progress_file"].encode()
progress_token = credential_document["progress_token"].encode()
assert os.write(ready_descriptor, b"1") == 1
os.close(ready_descriptor)
parent_environ = Path(f"/proc/{os.getppid()}/environ")
parent_environ_denied = False
if parent_environ.exists():
    try:
        parent_bytes = parent_environ.read_bytes()
    except PermissionError:
        parent_bytes = b""
        parent_environ_denied = True
else:
    parent_bytes = b""
parent_cmdline_path = Path(f"/proc/{os.getppid()}/cmdline")
parent_cmdline = parent_cmdline_path.read_bytes() if parent_cmdline_path.exists() else b""
parent_core_disabled = None
parent_mem_denied = None
parent_ptrace_denied = None
if sys.platform.startswith("linux"):
    parent_pid = os.getppid()
    limits = Path(f"/proc/{parent_pid}/limits").read_text(encoding="utf-8")
    core_line = next(line for line in limits.splitlines() if line.startswith("Max core file size"))
    core_fields = core_line.split()
    parent_core_disabled = core_fields[-3:-1] == ["0", "0"]
    try:
        parent_mem = open(f"/proc/{parent_pid}/mem", "rb", buffering=0)
    except OSError:
        parent_mem_denied = True
    else:
        parent_mem.close()
        parent_mem_denied = False
    libc = ctypes.CDLL(None, use_errno=True)
    libc.ptrace.restype = ctypes.c_long
    parent_ptrace_denied = libc.ptrace(16, parent_pid, None, None) == -1
print(json.dumps({
    "credential": credential_bytes.decode("utf-8"),
    "progress_file_env": os.environ.get("CLIO_RELAY_PROGRESS_FILE"),
    "progress_token_env": os.environ.get("CLIO_RELAY_PROGRESS_TOKEN"),
    "file_env": os.environ.get("CLIO_RELAY_RUNTIME_METADATA_FILE"),
    "token_env": os.environ.get("CLIO_RELAY_RUNTIME_METADATA_TOKEN"),
    "fd_env": os.environ.get("CLIO_RELAY_BROKER_CREDENTIAL_FD"),
    "ready_fd_env": os.environ.get("CLIO_RELAY_BROKER_READY_FD"),
    "parent_has_file": runtime_file in parent_bytes,
    "parent_has_token": runtime_token in parent_bytes,
    "parent_has_progress_file": progress_file in parent_bytes,
    "parent_has_progress_token": progress_token in parent_bytes,
    "parent_cmdline_has_progress_file": progress_file in parent_cmdline,
    "parent_cmdline_has_progress_token": progress_token in parent_cmdline,
    "parent_cmdline_has_runtime_file": runtime_file in parent_cmdline,
    "parent_cmdline_has_runtime_token": runtime_token in parent_cmdline,
    "parent_environ_denied": parent_environ_denied,
    "parent_core_disabled": parent_core_disabled,
    "parent_mem_denied": parent_mem_denied,
    "parent_ptrace_denied": parent_ptrace_denied,
}))
"""
    if os.name == "nt":
        with pytest.raises(RuntimeError, match="requires POSIX"):
            process_containment.spawn_owned_process(
                [sys.executable, "-I", "-S", "-c", script],
                credential_payload=credential,
                env=process_containment.owner_environment({}),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        return

    process = process_containment.spawn_owned_process(
        [sys.executable, "-I", "-S", "-c", script],
        credential_payload=credential,
        env=process_containment.owner_environment({"SAFE_VALUE": "present"}),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 0, stderr
        result = json.loads(stdout)
        expected = {
            "credential": credential,
            "progress_file_env": None,
            "progress_token_env": None,
            "file_env": None,
            "token_env": None,
            "fd_env": None,
            "ready_fd_env": None,
            "parent_has_file": False,
            "parent_has_token": False,
            "parent_has_progress_file": False,
            "parent_has_progress_token": False,
            "parent_cmdline_has_progress_file": False,
            "parent_cmdline_has_progress_token": False,
            "parent_cmdline_has_runtime_file": False,
            "parent_cmdline_has_runtime_token": False,
            "parent_environ_denied": False,
            "parent_core_disabled": None,
            "parent_mem_denied": None,
            "parent_ptrace_denied": None,
        }
        if sys.platform.startswith("linux"):
            expected.update(
                {
                    "parent_core_disabled": True,
                    "parent_environ_denied": True,
                    "parent_mem_denied": True,
                    "parent_ptrace_denied": True,
                }
            )
        assert result == expected
    finally:
        if process.poll() is None:
            process_containment.terminate_owned_process(process)
        process_containment.release_owned_process(process)


def test_broker_and_child_startup_ignore_python_startup_hooks(tmp_path: Path) -> None:
    malicious = tmp_path / "malicious"
    malicious.mkdir()
    site_marker = tmp_path / "sitecustomize-ran"
    pth_marker = tmp_path / "pth-ran"
    (malicious / "sitecustomize.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(site_marker)!r}).write_text('ran', encoding='utf-8')\n",
        encoding="utf-8",
    )
    (malicious / "hostile.pth").write_text(
        f"import pathlib; pathlib.Path({str(pth_marker)!r}).write_text('ran', encoding='utf-8')\n",
        encoding="utf-8",
    )
    process = process_containment.spawn_owned_process(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            "import json,os;print(json.dumps({'pythonpath': os.getenv('PYTHONPATH')}))",
        ],
        env=process_containment.owner_environment({"PYTHONPATH": str(malicious)}),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 0, stderr
        assert json.loads(stdout) == {"pythonpath": str(malicious)}
        assert not site_marker.exists()
        assert not pth_marker.exists()
    finally:
        if process.poll() is None:
            process_containment.terminate_owned_process(process)
        process_containment.release_owned_process(process)


def test_broker_readiness_timeout_kills_unacknowledged_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name == "nt":
        with pytest.raises(
            RuntimeError,
            match="secure broker credential transport requires POSIX",
        ):
            process_containment.spawn_owned_process(
                [sys.executable, "-I", "-S", "-c", "raise SystemExit(0)"],
                credential_payload='{"schema_version":"test.v1"}',
                env=process_containment.owner_environment({}),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        return

    pid_path = tmp_path / "unacknowledged.pid"
    shortened = cast(Any, process_containment)._BROKER_SCRIPT.replace(
        "HANDSHAKE_TIMEOUT_SECONDS = 5.0",
        "HANDSHAKE_TIMEOUT_SECONDS = 0.2",
    )
    monkeypatch.setattr(process_containment, "_BROKER_SCRIPT", shortened)
    script = r"""
import os
import sys
import time
from pathlib import Path

descriptor = int(os.environ.pop("CLIO_RELAY_BROKER_CREDENTIAL_FD"))
os.environ.pop("CLIO_RELAY_BROKER_READY_FD")
while os.read(descriptor, 4096):
    pass
os.close(descriptor)
Path(sys.argv[1]).write_text(str(os.getpid()), encoding="ascii")
time.sleep(60)
"""
    started = time.monotonic()

    with pytest.raises(RuntimeError, match="exited before child readiness"):
        process_containment.spawn_owned_process(
            [sys.executable, "-I", "-S", "-c", script, str(pid_path)],
            credential_payload='{"schema_version":"test.v1"}',
            env=process_containment.owner_environment({}),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    assert time.monotonic() - started < 3
    child_pid = int(pid_path.read_text(encoding="ascii"))
    assert _wait_until_pid_exits(child_pid, timeout_seconds=3)


@pytest.mark.parametrize(
    ("payload_kind", "error_match"),
    [
        ("forged", "child readiness timed out"),
        ("oversized", "payload exceeded its bound"),
    ],
)
def test_broker_readiness_rejects_forged_and_oversized_payloads_boundedly(
    payload_kind: str,
    error_match: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = cast(Any, process_containment)
    readiness = private._precreate_broker_readiness()
    descriptor = cast(int, readiness.descriptor)
    payload = b"1" if payload_kind == "forged" else readiness.token.encode("ascii") + b"x"
    os.lseek(descriptor, 0, os.SEEK_SET)
    assert os.write(descriptor, payload) == len(payload)
    os.fsync(descriptor)
    monkeypatch.setattr(process_containment, "BROKER_READY_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(process_containment, "POLL_SECONDS", 0.001)
    process = SimpleNamespace(poll=lambda: None, returncode=None)
    started = time.monotonic()

    with pytest.raises(RuntimeError, match=error_match):
        private._await_broker_readiness(process, readiness)

    assert time.monotonic() - started < 1
    assert readiness.descriptor is None
    assert not readiness.path.exists()


def test_broker_readiness_rejects_replaced_path_and_closes_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = cast(Any, process_containment)
    readiness = private._precreate_broker_readiness()
    path = readiness.path
    moved = path.with_name(f"{path.name}.moved")
    if os.name == "nt":
        real_stat = os.stat
        observed = real_stat(path, follow_symlinks=False)

        def replaced_stat(candidate: Path, *, follow_symlinks: bool = True) -> object:
            result = real_stat(candidate, follow_symlinks=follow_symlinks)
            if Path(candidate) == path:
                return SimpleNamespace(
                    st_dev=result.st_dev,
                    st_ino=result.st_ino + 1,
                    st_uid=result.st_uid,
                    st_mode=result.st_mode,
                )
            return result

        del observed
        monkeypatch.setattr(process_containment.os, "stat", replaced_stat)
    else:
        path.rename(moved)
        path.write_bytes(b"replacement")
    process = SimpleNamespace(poll=lambda: 1, returncode=1)
    try:
        with pytest.raises(RuntimeError, match="replaced broker readiness path"):
            private._await_broker_readiness(process, readiness)
    finally:
        monkeypatch.undo()
        path.unlink(missing_ok=True)
        moved.unlink(missing_ok=True)

    assert readiness.descriptor is None


def test_enforceable_provider_rejects_and_kills_background_escape(tmp_path: Path) -> None:
    capability = process_containment.containment_capability()
    if capability["enforceable"] is not True:
        pytest.fail(
            "release containment provider is unavailable: "
            f"{capability['mode']}: {capability['reason']}"
        )
    pid_path = tmp_path / "escaped-child.pid"
    script = """
import os
import subprocess
import sys
from pathlib import Path

child = subprocess.Popen(
    [sys.executable, "-c", "import time;time.sleep(60)"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=os.name != "nt",
    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
)
Path(sys.argv[1]).write_text(str(child.pid), encoding="ascii")
"""
    child_pid: int | None = None
    try:
        with pytest.raises(RelayError, match="left.*descendant|left.*Job Object|left.*scope"):
            JarvisCdProvider().run_command_streaming(
                [sys.executable, "-c", script, str(pid_path)],
                timeout_seconds=20,
            )
        child_pid = int(pid_path.read_text(encoding="ascii"))
        assert _wait_until_pid_exits(child_pid, timeout_seconds=5)
    finally:
        if child_pid is not None and _pid_exists(child_pid):
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(child_pid), "/T", "/F"],
                    check=False,
                    capture_output=True,
                    timeout=10,
                )
            else:
                os.kill(child_pid, signal.SIGKILL)


def _wait_until_pid_exits(process_id: int, *, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _pid_exists(process_id):
            return True
        time.sleep(0.05)
    return not _pid_exists(process_id)


def _pid_exists(process_id: int) -> bool:
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {process_id}", "/FO", "CSV", "/NH"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0 and f'"{process_id}"' in result.stdout
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True
