from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from clio_relay import process_containment
from clio_relay.errors import RelayError
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.process_containment import OwnedProcessSpawnError


def test_embedded_containment_source_is_an_exact_isolated_runtime_mirror() -> None:
    root = Path(__file__).parents[1]
    source = root / "src" / "clio_relay" / "process_containment.py"
    embedded = root / "jarvis-packages" / "clio_relay" / "clio_relay" / "process_containment.py"

    assert embedded.read_bytes() == source.read_bytes()


def test_owned_process_forwards_exact_bounded_stdin_payload() -> None:
    """The broker gives target stdin its own lossless channel after containment."""
    payload = b'line-one\n{"unicode":"\xe2\x80\xa8","nul":"\\u0000"}\n'
    process = process_containment.spawn_owned_process(
        [sys.executable, "-c", "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"],
        stdin_payload=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
        assert stdout == payload
        process_containment.ensure_owned_process_tree_empty(process)
    finally:
        if process.poll() is None:
            process_containment.terminate_owned_process(process)
        process_containment.release_owned_process(process)


def test_linux_containment_discovery_uses_one_absolute_startup_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First-call provider discovery and systemctl polling share the caller's budget."""
    private = cast(Any, process_containment)

    def is_file(_path: Path) -> bool:
        return True

    def find_executable(name: str) -> str:
        return f"/bin/{name}"

    monkeypatch.setattr(Path, "is_file", is_file)
    monkeypatch.setattr(process_containment.shutil, "which", find_executable)
    observed_timeouts: list[float] = []

    def timed_out_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, capture_output, text
        observed_timeouts.append(timeout)
        time.sleep(timeout)
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(process_containment.subprocess, "run", timed_out_run)
    started = time.monotonic()
    capability = private._probe_linux_systemd_scope_capability(startup_deadline=started + 0.02)
    assert time.monotonic() - started < 0.2
    assert capability["enforceable"] is False
    assert capability["transient"] is True
    assert observed_timeouts and max(observed_timeouts) <= 0.021

    systemctl_timeouts: list[float] = []

    def failed_systemctl(
        _arguments: list[str],
        *,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        systemctl_timeouts.append(timeout_seconds)
        time.sleep(timeout_seconds)
        return subprocess.CompletedProcess([], 1, "", "not ready")

    monkeypatch.setattr(process_containment, "_systemctl_user", failed_systemctl)
    monkeypatch.setattr(process_containment, "POLL_SECONDS", 0.001)
    process = SimpleNamespace(poll=lambda: None, stderr=None, returncode=None)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="scope setup timed out"):
        private._wait_for_systemd_control_group(
            "test.scope",
            process=process,
            startup_deadline=started + 0.02,
        )
    assert time.monotonic() - started < 0.2
    assert systemctl_timeouts and max(systemctl_timeouts) <= 0.021


def test_systemd_control_group_is_bound_to_exact_delegated_unit(tmp_path: Path) -> None:
    """Root, traversal, and sibling ControlGroup values cannot select a cleanup scope."""
    private = cast(Any, process_containment)
    root = tmp_path / "cgroup"
    unit = "clio-relay-012345.scope"
    expected = root / "user.slice" / unit
    expected.mkdir(parents=True)
    (expected / "cgroup.procs").write_text("", encoding="ascii")
    sibling = root / "user.slice" / "clio-relay-sibling.scope"
    sibling.mkdir()
    (sibling / "cgroup.procs").write_text("", encoding="ascii")

    assert (
        private._validated_systemd_cgroup_path(
            f"/user.slice/{unit}",
            unit=unit,
            cgroup_root=root,
        )
        == expected.resolve()
    )
    for malicious in (
        "/",
        "/../../tmp",
        "/user.slice/clio-relay-sibling.scope",
    ):
        with pytest.raises(RuntimeError):
            private._validated_systemd_cgroup_path(
                malicious,
                unit=unit,
                cgroup_root=root,
            )


def test_failed_spawn_attempts_termination_release_and_readiness_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One failing cleanup action cannot suppress the remaining ownership cleanup."""
    private = cast(Any, process_containment)
    actions: list[str] = []
    process = SimpleNamespace(pid=7411)

    def terminate(_process: object) -> None:
        actions.append("terminate")
        raise RuntimeError("termination failed")

    def release(_process: object) -> None:
        actions.append("release")
        raise RuntimeError("release failed")

    def remove(_readiness: object) -> None:
        actions.append("readiness")
        raise RuntimeError("readiness failed")

    monkeypatch.setattr(process_containment, "terminate_owned_process", terminate)
    monkeypatch.setattr(process_containment, "release_owned_process", release)
    monkeypatch.setattr(process_containment, "_remove_broker_readiness", remove)
    errors = private._cleanup_failed_owned_spawn(
        process,
        readiness=SimpleNamespace(),
        registered=True,
    )

    assert actions == ["terminate", "release", "readiness"]
    assert len(errors) == 3


def test_provider_release_failure_retains_registered_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed handle close remains registered so cleanup can be retried."""
    private = cast(Any, process_containment)
    process = SimpleNamespace(pid=7412)
    state = private._OwnedProcessState(
        mode="windows_job_object",
        enforceable=True,
        job_handle=99,
    )
    private._OWNED_PROCESSES[process.pid] = state

    def fail_close(_handle: int) -> None:
        raise RuntimeError("close failed")

    def ignore_tree(_process: subprocess.Popen[str]) -> None:
        return None

    monkeypatch.setattr(process_containment, "_close_windows_handle", fail_close)
    monkeypatch.setattr(process_containment, "ensure_owned_process_tree_empty", ignore_tree)
    typed_process = cast(subprocess.Popen[str], cast(object, process))
    try:
        with pytest.raises(RuntimeError, match="close failed"):
            process_containment.release_owned_process(typed_process)
        assert private._OWNED_PROCESSES[process.pid] is state
    finally:
        private._OWNED_PROCESSES.pop(process.pid, None)


def test_provider_release_does_not_hold_global_registry_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow provider close cannot block unrelated process registration."""
    private = cast(Any, process_containment)
    process = SimpleNamespace(pid=7413)
    state = private._OwnedProcessState(
        mode="linux_systemd_scope",
        enforceable=True,
        cgroup_path=Path("/test/cgroup/clio-relay-7413.scope"),
        systemd_unit="clio-relay-7413.scope",
    )
    other_state = private._OwnedProcessState(
        mode="cooperative_process_group",
        enforceable=False,
    )
    entered = threading.Event()
    unblock = threading.Event()

    def blocking_release(_unit: str) -> None:
        entered.set()
        assert unblock.wait(timeout=2)

    def ignore_tree(_process: subprocess.Popen[str]) -> None:
        return None

    monkeypatch.setattr(process_containment, "ensure_owned_process_tree_empty", ignore_tree)
    monkeypatch.setattr(process_containment, "_release_linux_systemd_scope", blocking_release)
    private._OWNED_PROCESSES[process.pid] = state
    typed_process = cast(subprocess.Popen[str], cast(object, process))
    release_thread = threading.Thread(
        target=process_containment.release_owned_process,
        args=(typed_process,),
    )
    release_thread.start()
    try:
        assert entered.wait(timeout=1)
        private._register_owned_process(7414, other_state)
        assert private._OWNED_PROCESSES[7414] is other_state
    finally:
        unblock.set()
        release_thread.join(timeout=2)
        private._OWNED_PROCESSES.pop(process.pid, None)
        private._OWNED_PROCESSES.pop(7414, None)
        private._OWNED_PROCESSES_RELEASING.discard(process.pid)
    assert not release_thread.is_alive()


def test_failed_systemd_spawn_attempts_every_cleanup_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wait or release failures cannot suppress later pre-registration cleanup."""
    private = cast(Any, process_containment)
    actions: list[str] = []

    def poll() -> None:
        return None

    def kill() -> None:
        actions.append("kill")

    def fail_wait(**_kwargs: object) -> int:
        raise RuntimeError("wait failed")

    process = SimpleNamespace(
        poll=poll,
        kill=kill,
        wait=fail_wait,
    )

    def release(_unit: str, **_kwargs: Any) -> None:
        actions.append("release")
        raise RuntimeError("release failed")

    def remove(_readiness: object) -> None:
        actions.append("readiness")

    monkeypatch.setattr(process_containment, "_release_linux_systemd_scope", release)
    monkeypatch.setattr(process_containment, "_remove_broker_readiness", remove)
    errors = private._cleanup_failed_linux_systemd_spawn(
        process,
        unit="clio-relay-test.scope",
        readiness=SimpleNamespace(),
        startup_deadline=time.monotonic() + 1,
    )

    assert actions == ["kill", "release", "readiness"]
    assert len(errors) == 2


@pytest.mark.parametrize("mode", ["linux_systemd_scope", "cooperative_process_group"])
def test_registration_failure_cleans_new_process_without_touching_stale_owner(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reused PID cannot redirect cleanup from a newly spawned process to stale state."""
    private = cast(Any, process_containment)
    process_id = 7415
    actions: list[str] = []
    alive = True

    def poll() -> int | None:
        return None if alive else -9

    def kill() -> None:
        nonlocal alive
        actions.append("kill-new")
        alive = False

    def wait(**_kwargs: object) -> int:
        return -9

    process = SimpleNamespace(pid=process_id, poll=poll, kill=kill, wait=wait)
    readiness = SimpleNamespace()
    scope = Path("/test/cgroup/clio-relay-new.scope")
    stale_state = private._OwnedProcessState(
        mode="windows_job_object",
        enforceable=True,
        job_handle=991,
    )
    private._OWNED_PROCESSES[process_id] = stale_state

    def capability(**_kwargs: object) -> dict[str, object]:
        return {
            "mode": mode,
            "enforceable": mode == "linux_systemd_scope",
            "reason": "test",
        }

    monkeypatch.setattr(process_containment, "containment_capability", capability)
    if mode == "linux_systemd_scope":

        def spawn_linux(
            _command: list[str],
            _popen_kwargs: dict[str, Any],
            *,
            startup_deadline: float,
        ) -> tuple[object, str, Path, object]:
            del startup_deadline
            return process, "clio-relay-new.scope", scope, readiness

        def terminate_linux(unit: str, observed_scope: Path) -> None:
            actions.append(f"terminate-new:{unit}:{observed_scope}")

        def release_linux(unit: str, **_kwargs: object) -> None:
            actions.append(f"release-new:{unit}")

        monkeypatch.setattr(process_containment, "_spawn_linux_systemd_scope", spawn_linux)
        monkeypatch.setattr(process_containment, "_terminate_linux_systemd_scope", terminate_linux)
        monkeypatch.setattr(process_containment, "_release_linux_systemd_scope", release_linux)
    else:

        def spawn_broker(
            _command: list[str],
            _popen_kwargs: dict[str, Any],
        ) -> tuple[object, object]:
            return process, readiness

        monkeypatch.setattr(process_containment, "_spawn_broker", spawn_broker)

    def remove_readiness(_readiness: object) -> None:
        actions.append("readiness")

    def fail_stale_termination(_process: subprocess.Popen[str]) -> None:
        pytest.fail("stale registered ownership was targeted")

    monkeypatch.setattr(process_containment, "_remove_broker_readiness", remove_readiness)
    monkeypatch.setattr(process_containment, "terminate_owned_process", fail_stale_termination)
    try:
        with pytest.raises(
            OwnedProcessSpawnError,
            match="process containment was already registered",
        ) as failure:
            process_containment.spawn_owned_process(["test-command"])
        assert failure.value.cleanup_verified is True
        assert isinstance(failure.value.__cause__, RuntimeError)
        assert str(failure.value.__cause__) == (
            f"process containment was already registered: {process_id}"
        )
        assert private._OWNED_PROCESSES[process_id] is stale_state
        assert "readiness" in actions
        if mode == "linux_systemd_scope":
            assert actions[:2] == [
                f"terminate-new:clio-relay-new.scope:{scope}",
                "kill-new",
            ]
            assert "release-new:clio-relay-new.scope" in actions
        else:
            assert actions[:2] == ["kill-new", "readiness"]
    finally:
        private._OWNED_PROCESSES.pop(process_id, None)


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


def test_windows_live_cleanup_accepts_taskkill_race_after_process_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = cast(Any, process_containment)
    poll_results = iter([None, 0])
    kills: list[bool] = []
    waits: list[float] = []

    def poll() -> int | None:
        return next(poll_results)

    def kill() -> None:
        kills.append(True)

    def wait(*, timeout: float) -> int:
        waits.append(timeout)
        return 0

    process = SimpleNamespace(
        pid=4312,
        poll=poll,
        kill=kill,
        wait=wait,
    )

    def failed_taskkill(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            1,
            "",
            "ERROR: There is no running instance of the task.",
        )

    monkeypatch.setattr(process_containment.subprocess, "run", failed_taskkill)

    private._terminate_windows_tree(process, timeout_seconds=3.0)

    assert kills == []
    assert waits == [3.0]


def test_windows_live_cleanup_fails_closed_after_taskkill_error_and_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = cast(Any, process_containment)
    kills: list[bool] = []

    def kill() -> None:
        kills.append(True)

    def wait(*, timeout: float) -> int:
        del timeout
        return 0

    process = SimpleNamespace(
        pid=4312,
        poll=lambda: None,
        kill=kill,
        wait=wait,
    )

    def failed_taskkill(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, "", "taskkill failed")

    monkeypatch.setattr(process_containment.subprocess, "run", failed_taskkill)

    with pytest.raises(RuntimeError, match="taskkill failed"):
        private._terminate_windows_tree(process, timeout_seconds=3.0)

    assert kills == [True]


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


def test_broker_readiness_replacement_is_rejected_without_truncation(tmp_path: Path) -> None:
    """The broker authenticates its descriptor before mutating a readiness file."""
    private = cast(Any, process_containment)
    readiness = private._precreate_broker_readiness()
    anchor = readiness.anchor()
    original = readiness.path.with_name(f"{readiness.path.name}.original")
    replacement = b"must-remain-unchanged"
    if readiness.descriptor is not None:
        os.close(readiness.descriptor)
        readiness.descriptor = None
    readiness.path.rename(original)
    readiness.path.write_bytes(replacement)
    setup = json.dumps(
        {
            "release": True,
            "credential": None,
            "readiness_token": readiness.token,
            "stdin_payload": None,
            "interactive_stdin": False,
            "target_environment": None,
        },
        separators=(",", ":"),
    )
    module_root = str(Path(process_containment.__file__).resolve().parent.parent)
    command = [sys.executable, "-I", "-S", "-c", "raise SystemExit(0)"]
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-u",
                "-c",
                private._BROKER_SCRIPT,
                json.dumps(command),
                str(readiness.path),
                json.dumps(anchor, separators=(",", ":")),
                module_root,
            ],
            input=setup + "\n",
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode != 0
        assert readiness.path.read_bytes() == replacement
    finally:
        readiness.path.unlink(missing_ok=True)
        original.unlink(missing_ok=True)


def test_enforceable_provider_rejects_and_kills_background_escape(tmp_path: Path) -> None:
    capability = process_containment.containment_capability()
    if capability["enforceable"] is not True:
        if os.name == "nt":
            pytest.fail(
                "Windows release containment provider is unavailable: "
                f"{capability['mode']}: {capability['reason']}"
            )
        assert capability["mode"] in {"linux_systemd_scope", "cooperative_process_group"}
        assert isinstance(capability["reason"], str) and capability["reason"]
        return
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
