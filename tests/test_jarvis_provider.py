from __future__ import annotations

import getpass
import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import cast

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from filelock import Timeout

from clio_relay import jarvis_provider
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_execution import sanitized_jarvis_environment
from clio_relay.jarvis_mcp import jarvis_cd_lock_binding_expectation
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import JarvisRunSpec, JobKind, McpCallSpec, RelayJob, RemoteAgentTaskSpec


def _private_sidecar_environment(*, scheduler_expected: bool | str) -> dict[str, str]:
    direct_proof = "test-direct-execution-proof"
    return {
        "CLIO_RELAY_PROGRESS_FILE": "/private/progress.jsonl",
        "CLIO_RELAY_PROGRESS_TOKEN": "private-progress-token",
        "CLIO_RELAY_RUNTIME_METADATA_FILE": "/private/runtime.jsonl",
        "CLIO_RELAY_RUNTIME_METADATA_TOKEN": "private-runtime-token",
        "CLIO_RELAY_RUNTIME_METADATA_ANCHOR": json.dumps(
            {"device": 1, "inode": 2, "owner": 3, "link_count": 1, "mode": 0o600}
        ),
        "CLIO_RELAY_RUNTIME_SUBMISSION_INTENT": json.dumps(
            {
                "schema_version": "clio-relay.scheduler-submission-intent.v1",
                "execution_id": "jarvis_test_execution",
                "marker": "clio-relay-0123456789abcdef",
                "created_at": "2026-07-11T00:00:00+00:00",
                "scheduler_user": getpass.getuser(),
                "scheduler_expected": scheduler_expected,
                "direct_proof_sha256": hashlib.sha256(direct_proof.encode("utf-8")).hexdigest(),
            }
        ),
        "CLIO_RELAY_RUNTIME_DIRECT_PROOF": direct_proof,
        "CLIO_RELAY_RUNTIME_SCHEDULER_PROVIDER": "slurm",
        "CLIO_RELAY_BROKER_CREDENTIAL_FD": "999",
        "CLIO_RELAY_BROKER_READY_FD": "998",
    }


def test_jarvis_environment_sanitization_removes_progress_and_runtime_secrets() -> None:
    private_names = _private_sidecar_environment(scheduler_expected=False)

    sanitized = sanitized_jarvis_environment({"SAFE_VALUE": "present", **private_names})

    assert sanitized == {"SAFE_VALUE": "present"}


def test_streaming_cancel_terminates_child_in_separate_process_group() -> None:
    provider = JarvisCdProvider()
    child_pids: list[int] = []
    script = """
import os
import subprocess
import sys
import time

child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(60)"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=os.name != "nt",
    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
)
print(child.pid, flush=True)
time.sleep(60)
"""

    def observe_stdout(value: str) -> None:
        stripped = value.strip()
        if stripped.isdigit():
            child_pids.append(int(stripped))

    try:
        result = provider.run_command_streaming(
            [sys.executable, "-u", "-c", script],
            on_stdout=observe_stdout,
            should_cancel=lambda: bool(child_pids),
            timeout_seconds=20,
        )

        assert result.returncode == -15
        assert len(child_pids) == 1
        assert _wait_until_pid_exits(child_pids[0], timeout_seconds=5)
    finally:
        for child_pid in child_pids:
            if not _pid_exists(child_pid):
                continue
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(child_pid), "/T", "/F"],
                    check=False,
                    capture_output=True,
                    timeout=10,
                )
            else:
                os.kill(child_pid, signal.SIGKILL)


def test_streaming_retains_only_bounded_tail_while_forwarding_complete_output() -> None:
    provider = JarvisCdProvider()
    output_size = jarvis_provider.STREAM_RESULT_TAIL_MAX_CHARACTERS + 250_000
    observed = 0

    def observe(value: str) -> None:
        nonlocal observed
        observed += len(value)

    result = provider.run_command_streaming(
        [sys.executable, "-c", f"import sys;sys.stdout.write('x'*{output_size})"],
        on_stdout=observe,
        timeout_seconds=20,
    )

    assert result.returncode == 0
    assert observed == output_size
    assert len(result.stdout) == jarvis_provider.STREAM_RESULT_TAIL_MAX_CHARACTERS
    assert result.stdout == "x" * jarvis_provider.STREAM_RESULT_TAIL_MAX_CHARACTERS


def test_streaming_callback_failure_terminates_process_and_propagates() -> None:
    provider = JarvisCdProvider()

    def fail_callback(_value: str) -> None:
        raise ValueError("spool callback failed")

    with pytest.raises(ValueError, match="spool callback failed"):
        provider.run_command_streaming(
            [
                sys.executable,
                "-u",
                "-c",
                "import time;print('ready',flush=True);time.sleep(60)",
            ],
            on_stdout=fail_callback,
            timeout_seconds=20,
        )


def test_streaming_on_start_failure_prevents_workload_release(tmp_path: Path) -> None:
    provider = JarvisCdProvider()
    process_ids: list[int] = []
    workload_marker = tmp_path / "workload-started.txt"

    def fail_on_start(process_id: int) -> None:
        process_ids.append(process_id)
        raise RuntimeError("execution-start persistence failed")

    with pytest.raises(RuntimeError, match="execution-start persistence failed"):
        provider.run_command_streaming(
            [
                sys.executable,
                "-c",
                "from pathlib import Path;import sys,time;"
                "Path(sys.argv[1]).write_text('started');time.sleep(60)",
                str(workload_marker),
            ],
            on_start=fail_on_start,
            timeout_seconds=20,
        )

    assert len(process_ids) == 1
    assert _wait_until_pid_exits(process_ids[0], timeout_seconds=5)
    assert workload_marker.exists() is False


def test_streaming_thread_start_failure_terminates_process(
    monkeypatch: MonkeyPatch,
) -> None:
    provider = JarvisCdProvider()
    process_ids: list[int] = []
    original_start = jarvis_provider.threading.Thread.start

    def fail_start(thread: threading.Thread) -> None:
        if thread.name.startswith("clio-relay-"):
            raise RuntimeError("stream reader could not start")
        original_start(thread)

    monkeypatch.setattr(jarvis_provider.threading.Thread, "start", fail_start)
    with pytest.raises(RuntimeError, match="stream reader could not start"):
        provider.run_command_streaming(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            on_start=process_ids.append,
            timeout_seconds=20,
        )

    assert len(process_ids) == 1
    monkeypatch.undo()
    assert _wait_until_pid_exits(process_ids[0], timeout_seconds=5)


def test_cross_process_core_lock_contention_is_bounded_and_cleans_sleeping_child(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core", lock_timeout_seconds=0.2)
    job = queue.submit_job(
        RelayJob(
            cluster="test",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "test"]),
            idempotency_key="lock-contention-watchdog",
        )
    )
    lock_path = queue.root / ".lock"
    holder = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            (
                "import sys,time;from filelock import FileLock;"
                "lock=FileLock(sys.argv[1]);"
                "lock.acquire();print('locked',flush=True);time.sleep(60)"
            ),
            str(lock_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    process_ids: list[int] = []
    callback_attempted = False

    def persist_output(text: str) -> None:
        nonlocal callback_attempted
        callback_attempted = True
        queue.append_event(job.job_id, "stdout.delta", text)

    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "locked"
        started = time.monotonic()
        with pytest.raises(Timeout):
            JarvisCdProvider().run_command_streaming(
                [
                    sys.executable,
                    "-u",
                    "-c",
                    "import os,time;print(os.getpid(),flush=True);time.sleep(60)",
                ],
                on_start=process_ids.append,
                on_stdout=persist_output,
                timeout_seconds=2,
            )
        elapsed = time.monotonic() - started
        assert elapsed < 5
        assert callback_attempted is True
        assert len(process_ids) == 1
        assert _wait_until_pid_exits(process_ids[0], timeout_seconds=5)
    finally:
        holder.kill()
        holder.wait(timeout=5)

    time.sleep(0.3)
    events, _ = queue.read_event_page(job.job_id, limit=20)
    assert [event.event_type for event in events] == ["job.queued"]


class _FakeRunningProcess:
    def __init__(self, pid: int = 100) -> None:
        self.pid = pid
        self.killed = False

    def poll(self) -> None:
        return None

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 0

    def kill(self) -> None:
        self.killed = True


def test_posix_termination_repeats_discovery_for_late_children(
    monkeypatch: MonkeyPatch,
) -> None:
    process = _FakeRunningProcess()
    discoveries = iter([[101], [101, 102], [101, 102, 103]])
    signaled: list[tuple[list[int], signal.Signals]] = []
    containment = jarvis_provider.process_containment
    monkeypatch.setattr(containment.os, "name", "posix")
    monkeypatch.setattr(containment, "_current_posix_group", lambda: 999)

    def discover(_pid: int) -> list[int]:
        return next(discoveries)

    def record_signal(
        process_ids: list[int],
        _groups: list[int],
        requested: signal.Signals,
    ) -> None:
        signaled.append((list(process_ids), requested))

    def no_residuals(
        *,
        process_ids: list[int],
        process_group: int | None,
        timeout_seconds: float,
    ) -> list[int]:
        del process_ids, process_group, timeout_seconds
        return []

    monkeypatch.setattr(containment, "_posix_descendant_process_ids", discover)
    monkeypatch.setattr(containment, "_signal_posix_tree", record_signal)
    monkeypatch.setattr(containment, "_wait_for_exit", no_residuals)

    jarvis_provider._terminate_process(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cast(subprocess.Popen[str], process)
    )

    assert signaled[0] == ([100, 101, 102, 103], signal.SIGTERM)


def test_posix_discovery_failure_is_reported_after_outer_cleanup(
    monkeypatch: MonkeyPatch,
) -> None:
    process = _FakeRunningProcess()
    signaled: list[list[int]] = []
    containment = jarvis_provider.process_containment
    monkeypatch.setattr(containment.os, "name", "posix")
    monkeypatch.setattr(containment, "_current_posix_group", lambda: 999)

    def fail_discovery(_pid: int) -> list[int]:
        raise RuntimeError("ps unavailable")

    monkeypatch.setattr(containment, "_posix_descendant_process_ids", fail_discovery)

    def record_signal(
        process_ids: list[int],
        _groups: list[int],
        _requested: signal.Signals,
    ) -> None:
        signaled.append(list(process_ids))

    def no_residuals(
        *,
        process_ids: list[int],
        process_group: int | None,
        timeout_seconds: float,
    ) -> list[int]:
        del process_ids, process_group, timeout_seconds
        return []

    monkeypatch.setattr(containment, "_signal_posix_tree", record_signal)
    monkeypatch.setattr(containment, "_wait_for_exit", no_residuals)

    with pytest.raises(RelayError, match="without complete descendant discovery"):
        jarvis_provider._terminate_process(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(subprocess.Popen[str], process)
        )

    assert signaled == [[100]]


def test_windows_taskkill_timeout_kills_outer_and_fails_closed(
    monkeypatch: MonkeyPatch,
) -> None:
    process = _FakeRunningProcess()
    containment = jarvis_provider.process_containment
    monkeypatch.setattr(containment.os, "name", "nt")

    def timeout(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(["taskkill"], 10)

    monkeypatch.setattr(containment.subprocess, "run", timeout)

    with pytest.raises(RelayError, match="timed out|could not prove"):
        jarvis_provider._terminate_process(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cast(subprocess.Popen[str], process)
        )

    assert process.killed is True


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


def test_bounded_command_yaml_generation() -> None:
    provider = JarvisCdProvider()
    rendered = provider.render_bounded_command_yaml(
        JarvisRunSpec(command=["python", "-V"], env={"A": "B"}, timeout_seconds=30)
    )
    document = yaml.safe_load(rendered)

    package = document["pkgs"][0]
    assert package["pkg_type"] == "clio_relay.bounded_command"
    assert package["command"] == ["python", "-V"]
    assert package["env"] == {"A": "B"}
    assert package["timeout_seconds"] == 30
    assert "progress" not in package


def test_remote_agent_task_yaml_generation(tmp_path: Path) -> None:
    provider = JarvisCdProvider(
        agent_bin="/opt/agent/bin/current-agent",
        agent_adapter="exec",
        agent_args=["--prompt", "{prompt_path}"],
    )
    rendered = provider.render_remote_agent_task_yaml(
        RemoteAgentTaskSpec(
            prompt_path=str(tmp_path / "prompt.md"),
            mcp_config_path=str(tmp_path / "mcp.json"),
            context={"source_event_seq": 7, "match_groups": {"step": "50"}},
        )
    )
    document = yaml.safe_load(rendered)

    package = document["pkgs"][0]
    assert package["pkg_type"] == "clio_relay.remote_agent"
    assert package["agent_bin"] == "/opt/agent/bin/current-agent"
    assert package["agent_adapter"] == "exec"
    assert package["agent_args"] == ["--prompt", "{prompt_path}"]
    assert package["prompt_path"].endswith("prompt.md")
    assert package["context"] == {"source_event_seq": 7, "match_groups": {"step": "50"}}


def test_mcp_call_yaml_generation() -> None:
    provider = JarvisCdProvider()
    rendered = provider.render_mcp_call_yaml(
        McpCallSpec(
            server="uvx",
            server_args=[
                "--from",
                "clio-kit==2.2.6",
                "clio-kit",
                "mcp-server",
                "jarvis",
            ],
            env_from={"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"},
            expected_jarvis_cd_lock_binding=jarvis_cd_lock_binding_expectation(),
            tool="inspect",
            arguments={"path": "x"},
        )
    )
    document = yaml.safe_load(rendered)

    package = document["pkgs"][0]
    assert package["pkg_type"] == "clio_relay.mcp_call"
    assert package["server"] == "uvx"
    assert package["server_args"] == [
        "--from",
        "clio-kit==2.2.6",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    assert package["env_from"] == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    assert package["expected_jarvis_cd_lock_binding"] == jarvis_cd_lock_binding_expectation()
    assert package["tool"] == "inspect"
    assert package["arguments"] == {"path": "x"}


def test_named_pipeline_command_uses_configured_jarvis_binary() -> None:
    provider = JarvisCdProvider(jarvis_bin="/opt/jarvis/bin/jarvis")

    command = provider.named_pipeline_command("my_pipeline")

    assert command[:4] == ["python", "-I", "-S", "-c"]
    assert command[5:] == ["named", "my_pipeline"]
    assert "Pipeline(sys.argv[2])" in command[4]
    assert "obj.load()" in command[4]


def test_unscheduled_pipeline_uses_in_process_private_wrapper(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        yaml.safe_dump({"name": "direct", "pkgs": []}),
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin="/opt/jarvis/bin/jarvis")

    command = provider.pipeline_command(pipeline)

    assert command[:4] == ["python", "-I", "-S", "-c"]
    assert command[5:] == ["yaml", str(pipeline)]
    assert "load_yaml_auto" in command[4]
    assert "enforce_linux_secret_memory_gate()" in command[4]


def test_scheduled_pipeline_uses_structured_submit_and_observe_runner(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "name": "scheduled",
                "scheduler": {"name": "slurm", "exclusive": True},
                "pkgs": [],
            }
        ),
        encoding="utf-8",
    )
    bin_dir = tmp_path / "opt" / "jarvis" / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "python").write_text("", encoding="utf-8")
    provider = JarvisCdProvider(jarvis_bin=str(bin_dir / "jarvis"))

    command = provider.pipeline_command(pipeline)

    assert command[0] == str(bin_dir / "python")
    assert command[1:4] == ["-I", "-S", "-c"]
    assert "from clio_relay.jarvis_execution import run_native_jarvis_broker" in command[4]
    assert "run_native_jarvis_broker(" in command[4]
    assert "runtime_intent=runtime_intent" in command[4]
    assert "runtime_direct_proof=runtime_direct_proof" in command[4]
    assert "append_runtime_record=append_runtime_record" in command[4]
    assert command[5:] == ["yaml", str(pipeline)]


@pytest.mark.parametrize("streaming", [False, True])
def test_unsupported_scheduled_pipeline_fails_before_credentials_or_process_spawn(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    streaming: bool,
) -> None:
    pipeline = tmp_path / "unsupported-scheduler.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "name": "unsupported-scheduler",
                "scheduler": {"name": "site-batch"},
                "pkgs": [],
            }
        ),
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin="jarvis")
    observation_lookups: list[str | None] = []
    credential_calls: list[object] = []
    process_calls: list[object] = []

    def record_observation_lookup(name: str | None) -> object:
        observation_lookups.append(name)
        return object()

    def reject_credential_extraction(environment: object) -> None:
        credential_calls.append(environment)
        raise AssertionError("credential extraction must not be reached")

    def reject_process_spawn(*args: object, **kwargs: object) -> None:
        process_calls.append((args, kwargs))
        raise AssertionError("process spawn must not be reached")

    monkeypatch.setattr(provider, "require_available", lambda: None)
    monkeypatch.setattr(
        jarvis_provider,
        "provider_for_scheduler",
        record_observation_lookup,
    )
    monkeypatch.setattr(
        jarvis_provider,
        "jarvis_private_credential_channel",
        reject_credential_extraction,
    )
    monkeypatch.setattr(
        provider,
        "run_command_streaming",
        reject_process_spawn,
    )

    with pytest.raises(ConfigurationError, match="only through slurm"):
        if streaming:
            provider.run_pipeline_streaming(pipeline, env={})
        else:
            provider.run_pipeline(pipeline)

    assert observation_lookups == ["site-batch"]
    assert credential_calls == []
    assert process_calls == []


def test_scheduled_launch_initial_argv_and_environment_hide_all_sidecar_secrets(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "scheduled-secure.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "name": "scheduled-secure",
                "scheduler": {"name": "slurm"},
                "pkgs": [],
            }
        ),
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin="jarvis")
    captured: dict[str, object] = {}

    def capture_launch(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(provider, "require_available", lambda: None)
    monkeypatch.setattr(provider, "run_command_streaming", capture_launch)
    result = provider.run_pipeline_streaming(
        pipeline,
        env={
            "SAFE_VALUE": "present",
            **_private_sidecar_environment(scheduler_expected=True),
        },
    )

    assert result.returncode == 0
    launch_env = cast(dict[str, str], captured["env"])
    assert launch_env == {"SAFE_VALUE": "present"}
    initial_argv = "\x00".join(cast(list[str], captured["command"]))
    for secret in (
        "/private/progress.jsonl",
        "private-progress-token",
        "/private/runtime.jsonl",
        "private-runtime-token",
        "test-direct-execution-proof",
    ):
        assert secret not in initial_argv
        assert secret not in launch_env.values()
    payload = json.loads(cast(str, captured["credential_payload"]))
    assert payload == {
        "schema_version": "clio-relay.jarvis-private-credential.v1",
        "progress_file": "/private/progress.jsonl",
        "progress_token": "private-progress-token",
        "runtime_file": "/private/runtime.jsonl",
        "runtime_token": "private-runtime-token",
        "runtime_anchor": {
            "device": 1,
            "inode": 2,
            "owner": 3,
            "link_count": 1,
            "mode": 0o600,
        },
        "runtime_intent": {
            "schema_version": "clio-relay.scheduler-submission-intent.v1",
            "execution_id": "jarvis_test_execution",
            "marker": "clio-relay-0123456789abcdef",
            "created_at": "2026-07-11T00:00:00+00:00",
            "scheduler_user": getpass.getuser(),
            "scheduler_expected": True,
            "direct_proof_sha256": hashlib.sha256(b"test-direct-execution-proof").hexdigest(),
        },
        "runtime_direct_proof": "test-direct-execution-proof",
        "scheduler_provider": "slurm",
    }


def test_direct_launch_initial_argv_and_environment_hide_all_sidecar_secrets(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "unscheduled-secure.yaml"
    pipeline.write_text("name: direct\npkgs: []\n", encoding="utf-8")
    provider = JarvisCdProvider(jarvis_bin="jarvis")
    captured: dict[str, object] = {}

    def capture_launch(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(provider, "require_available", lambda: None)
    monkeypatch.setattr(provider, "run_command_streaming", capture_launch)
    private_names = _private_sidecar_environment(scheduler_expected=False)

    result = provider.run_pipeline_streaming(
        pipeline,
        env={"SAFE_VALUE": "present", **private_names},
    )

    assert result.returncode == 0
    assert captured["env"] == {"SAFE_VALUE": "present"}
    initial_argv = "\x00".join(cast(list[str], captured["command"]))
    assert "/private/progress.jsonl" not in initial_argv
    assert "private-progress-token" not in initial_argv
    assert "/private/runtime.jsonl" not in initial_argv
    assert "private-runtime-token" not in initial_argv
    payload = json.loads(cast(str, captured["credential_payload"]))
    assert payload["schema_version"] == "clio-relay.jarvis-private-credential.v1"
    assert payload["progress_file"] == "/private/progress.jsonl"
    assert payload["progress_token"] == "private-progress-token"
    assert payload["runtime_intent"]["scheduler_expected"] is False


def test_default_unscheduled_run_uses_private_sidecar_channel(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    pipeline = tmp_path / "default-direct.yaml"
    pipeline.write_text("name: direct\npkgs: []\n", encoding="utf-8")
    provider = JarvisCdProvider(jarvis_bin="jarvis")
    captured: dict[str, object] = {}

    def capture_run(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(provider, "require_available", lambda: None)
    private_names = _private_sidecar_environment(scheduler_expected=False)
    for name, value in private_names.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SAFE_VALUE", "present")
    monkeypatch.setattr(provider, "run_command_streaming", capture_run)

    result = provider.run_pipeline(pipeline)

    assert result.returncode == 0
    launch_env = cast(dict[str, str], captured["env"])
    assert launch_env["SAFE_VALUE"] == "present"
    assert not any(name in launch_env for name in private_names)
    payload = json.loads(cast(str, captured["credential_payload"]))
    assert payload["progress_token"] == "private-progress-token"
    assert payload["runtime_intent"]["scheduler_expected"] is False


def test_scheduled_pipeline_test_config_uses_scheduler_runner(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline-test.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "config": {
                    "name": "scheduled-test",
                    "scheduler": {"name": "slurm", "nodes": 2},
                    "pkgs": [],
                },
                "vars": {"case": [1]},
            }
        ),
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin="jarvis")

    command = provider.pipeline_command(pipeline)

    assert command[1:4] == ["-I", "-S", "-c"]
    assert "load_yaml_auto" in command[4]
    assert command[5:] == ["yaml", str(pipeline)]


def test_scheduled_pipeline_uses_wrapper_shebang_when_sibling_python_is_missing(
    tmp_path: Path,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "name": "scheduled",
                "scheduler": {"name": "slurm"},
                "pkgs": [],
            }
        ),
        encoding="utf-8",
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    jarvis = bin_dir / "jarvis"
    jarvis.write_text(
        "#!/opt/clio-relay/jarvis-venv/bin/python\nprint('jarvis')\n",
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin=str(jarvis))

    command = provider.pipeline_command(pipeline)

    assert command[0] == "/opt/clio-relay/jarvis-venv/bin/python"


def test_receipt_bound_python_bypasses_unmanaged_sibling_discovery(tmp_path: Path) -> None:
    """A managed provider must never select an unrelated sibling interpreter."""
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text("name: direct\npkgs: []\n", encoding="utf-8")
    stable_bin = tmp_path / ".local/bin"
    stable_bin.mkdir(parents=True)
    jarvis = stable_bin / "jarvis"
    jarvis.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    unrelated_python = stable_bin / "python"
    unrelated_python.write_text("unrelated", encoding="utf-8")
    execution_python = tmp_path / "generation/jarvis-venv/bin/python"
    execution_python.parent.mkdir(parents=True)
    execution_python.write_text("managed", encoding="utf-8")
    provider = JarvisCdProvider(
        jarvis_bin=str(jarvis),
        execution_python=str(execution_python),
    )

    command = provider.pipeline_command(pipeline)

    assert command[0] == str(execution_python)
    assert command[0] != str(unrelated_python)
