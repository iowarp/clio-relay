"""JARVIS-CD provider boundary.

clio-relay translates durable relay intents into JARVIS-CD package/pipeline
inputs. JARVIS-CD remains responsible for scheduler submission, deployment,
environment capture, output collection, and provenance.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Any, Literal, cast

import yaml

from clio_relay import process_containment
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_execution import (
    jarvis_private_credential_channel,
    named_jarvis_command,
    scheduled_jarvis_command,
    yaml_jarvis_command,
)
from clio_relay.models import JarvisRunSpec, McpCallSpec, RemoteAgentTaskSpec
from clio_relay.scheduler_providers import provider_for_scheduler

STREAM_RESULT_TAIL_MAX_CHARACTERS = 1024 * 1024
STREAM_READ_CHARACTERS = 64 * 1024
STREAM_THREAD_JOIN_TIMEOUT_SECONDS = 15.0
STREAM_QUEUE_MAX_CHUNKS = 128
STREAM_QUEUE_PUT_SECONDS = 0.05


@dataclass
class _BoundedTextTail:
    """Retain a bounded result tail while callbacks receive the complete stream."""

    limit: int = STREAM_RESULT_TAIL_MAX_CHARACTERS
    chunks: deque[str] = field(default_factory=lambda: deque[str]())
    size: int = 0

    def append(self, value: str) -> None:
        """Append text and discard the oldest characters above the limit."""
        if not value or self.limit <= 0:
            return
        if len(value) >= self.limit:
            self.chunks.clear()
            self.chunks.append(value[-self.limit :])
            self.size = self.limit
            return
        self.chunks.append(value)
        self.size += len(value)
        while self.size > self.limit:
            excess = self.size - self.limit
            oldest = self.chunks[0]
            if len(oldest) <= excess:
                self.chunks.popleft()
                self.size -= len(oldest)
                continue
            self.chunks[0] = oldest[excess:]
            self.size -= excess

    def render(self) -> str:
        """Return the retained tail."""
        return "".join(self.chunks)


class JarvisCdProvider:
    """Materialize and invoke relay jobs through JARVIS-CD."""

    def __init__(
        self,
        *,
        jarvis_bin: str = "jarvis",
        execution_python: str | None = None,
        agent_bin: str = "agent",
        agent_adapter: str = "exec",
        agent_args: list[str] | None = None,
    ) -> None:
        self.jarvis_bin = jarvis_bin
        self.execution_python = execution_python
        self.agent_bin = agent_bin
        self.agent_adapter = agent_adapter
        self.agent_args = agent_args or []

    def require_available(self) -> None:
        """Raise if the configured JARVIS executable is unavailable."""
        if shutil.which(self.jarvis_bin) is None:
            raise ConfigurationError(f"JARVIS-CD executable not found: {self.jarvis_bin}")

    def render_bounded_command_yaml(self, spec: JarvisRunSpec) -> str:
        """Render a bounded-command JARVIS pipeline YAML document."""
        if spec.pipeline_yaml is not None:
            return spec.pipeline_yaml
        if spec.pipeline_path is not None:
            return spec.pipeline_path.read_text(encoding="utf-8")
        if spec.pipeline_name is not None:
            raise ConfigurationError(
                "pipeline_name jobs must be executed directly, not rendered to YAML"
            )
        if spec.command is None:
            raise ConfigurationError(
                "JarvisRunSpec requires pipeline_yaml, pipeline_path, or command"
            )
        document: dict[str, Any] = {
            "name": spec.package or "clio-relay-bounded-command",
            "pkgs": [
                {
                    "pkg_type": "clio_relay.bounded_command",
                    "pkg_name": "bounded_command",
                    "command": spec.command,
                    "workdir": str(spec.workdir) if spec.workdir is not None else None,
                    "env": spec.env,
                    "timeout_seconds": spec.timeout_seconds,
                    "progress": spec.progress or None,
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def render_remote_agent_task_yaml(self, spec: RemoteAgentTaskSpec) -> str:
        """Render a JARVIS pipeline for a remote agent task."""
        document: dict[str, Any] = {
            "name": "clio-relay-remote-agent",
            "pkgs": [
                {
                    "pkg_type": "clio_relay.remote_agent",
                    "pkg_name": "remote_agent",
                    "agent_bin": self.agent_bin,
                    "agent_adapter": self.agent_adapter,
                    "agent_args": self.agent_args,
                    "prompt_path": str(spec.prompt_path),
                    "mcp_config_path": (
                        str(spec.mcp_config_path) if spec.mcp_config_path is not None else None
                    ),
                    "model": spec.model,
                    "workdir": str(spec.workdir) if spec.workdir is not None else None,
                    "timeout_seconds": spec.timeout_seconds,
                    "context": spec.context or None,
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def render_mcp_call_yaml(self, spec: McpCallSpec) -> str:
        """Render a JARVIS pipeline for a remote MCP tool call."""
        document: dict[str, Any] = {
            "name": "clio-relay-mcp-call",
            "pkgs": [
                {
                    "pkg_type": "clio_relay.mcp_call",
                    "pkg_name": "mcp_call",
                    "server": spec.server,
                    "server_args": spec.server_args or None,
                    "env_from": spec.env_from or None,
                    "expected_server_artifact_digest": spec.expected_server_artifact_digest,
                    "expected_registered_contract": spec.expected_registered_contract,
                    "expected_jarvis_cd_lock_binding": spec.expected_jarvis_cd_lock_binding,
                    "operation": spec.operation.value,
                    "tool": spec.tool,
                    "arguments": spec.arguments or None,
                    "jarvis_input_manifest": (
                        spec.jarvis_input_manifest.model_dump(mode="json")
                        if spec.jarvis_input_manifest is not None
                        else None
                    ),
                    "timeout_seconds": spec.timeout_seconds,
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def write_pipeline(self, yaml_text: str, path: Path) -> Path:
        """Write a rendered JARVIS pipeline YAML file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml_text, encoding="utf-8")
        return path

    def run_pipeline(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Invoke JARVIS-CD for an already materialized pipeline."""
        self.require_available()
        command = self.pipeline_command(pipeline_path)
        launch_env, credential_payload = jarvis_private_credential_channel(os.environ)
        return self.run_command_streaming(
            command,
            cwd=cwd,
            env=launch_env,
            credential_payload=credential_payload,
        )

    def run_pipeline_streaming(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Invoke JARVIS-CD and stream output chunks while retaining final output."""
        self.require_available()
        command = self.pipeline_command(pipeline_path)
        launch_env, credential_payload = jarvis_private_credential_channel(env)
        return self.run_command_streaming(
            command,
            cwd=cwd,
            env=launch_env,
            credential_payload=credential_payload,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            on_start=on_start,
            should_cancel=should_cancel,
            on_poll=on_poll,
            timeout_seconds=timeout_seconds,
            on_timeout=on_timeout,
        )

    def run_named_pipeline_streaming(
        self,
        pipeline_name: str,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Invoke JARVIS-CD for an existing named pipeline and stream output."""
        self.require_available()
        launch_env, credential_payload = jarvis_private_credential_channel(env)
        return self.run_command_streaming(
            self.named_pipeline_command(pipeline_name),
            cwd=cwd,
            env=launch_env,
            credential_payload=credential_payload,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            on_start=on_start,
            should_cancel=should_cancel,
            on_poll=on_poll,
            timeout_seconds=timeout_seconds,
            on_timeout=on_timeout,
        )

    def run_command_streaming(
        self,
        command: list[str],
        *,
        process_label: str = "contained command",
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        credential_payload: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
        timeout_seconds: int | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a command and stream output chunks while retaining final output."""
        normalized_process_label = process_label.strip()
        if (
            not normalized_process_label
            or len(normalized_process_label) > 80
            or any(character in normalized_process_label for character in "\x00\r\n")
        ):
            raise ValueError("process_label must be a bounded single-line label")
        try:
            process = process_containment.spawn_owned_process(
                command,
                on_ready=(
                    None if on_start is None else lambda process_id, _metadata: on_start(process_id)
                ),
                cwd=cwd,
                env=process_containment.owner_environment(env),
                credential_payload=credential_payload,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
            )
        except (OSError, RuntimeError) as exc:
            raise RelayError(f"failed to execute {normalized_process_label}: {exc}") from exc
        stdout_tail = _BoundedTextTail()
        stderr_tail = _BoundedTextTail()
        stream_errors: list[BaseException] = []
        stream_error_lock = threading.Lock()
        stream_messages: Queue[tuple[Literal["stdout", "stderr"], str]] = Queue(
            maxsize=STREAM_QUEUE_MAX_CHUNKS
        )
        stop_streams = threading.Event()
        stdout_thread = threading.Thread(
            target=_drain_stream,
            args=(
                process.stdout,
                "stdout",
                stdout_tail,
                stream_messages,
                stop_streams,
                stream_errors,
                stream_error_lock,
            ),
            name=f"clio-relay-stdout-{process.pid}",
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_drain_stream,
            args=(
                process.stderr,
                "stderr",
                stderr_tail,
                stream_messages,
                stop_streams,
                stream_errors,
                stream_error_lock,
            ),
            name=f"clio-relay-stderr-{process.pid}",
            daemon=True,
        )
        stream_threads = [stdout_thread, stderr_thread]
        started_threads: list[threading.Thread] = []
        canceled = False
        timed_out = False
        return_code: int | None = None
        primary_error: BaseException | None = None
        deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
        try:
            for thread in stream_threads:
                thread.start()
                started_threads.append(thread)
            while True:
                _dispatch_stream_messages(
                    stream_messages,
                    on_stdout=on_stdout,
                    on_stderr=on_stderr,
                )
                _raise_stream_error(stream_errors, stream_error_lock)
                return_code = process.poll()
                if return_code is not None:
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    timed_out = True
                    if on_timeout is not None:
                        on_timeout()
                    _terminate_process(process)
                    return_code = process.wait()
                    break
                if should_cancel is not None and should_cancel():
                    canceled = True
                    _terminate_process(process)
                    return_code = process.wait()
                    break
                if on_poll is not None:
                    on_poll()
                time.sleep(0.25)
        except BaseException as exc:
            primary_error = exc
            stop_streams.set()
            if process.poll() is None:
                try:
                    _terminate_process(process)
                except BaseException as cleanup_exc:
                    primary_error = RelayError(f"{exc}; process cleanup also failed: {cleanup_exc}")
        finally:
            join_deadline = time.monotonic() + STREAM_THREAD_JOIN_TIMEOUT_SECONDS
            while any(thread.is_alive() for thread in started_threads):
                if primary_error is None:
                    try:
                        _dispatch_stream_messages(
                            stream_messages,
                            on_stdout=on_stdout,
                            on_stderr=on_stderr,
                        )
                        _raise_stream_error(stream_errors, stream_error_lock)
                    except BaseException as exc:
                        primary_error = exc
                        stop_streams.set()
                        if process.poll() is None:
                            try:
                                _terminate_process(process)
                            except BaseException as cleanup_exc:
                                primary_error = RelayError(
                                    f"{exc}; process cleanup also failed: {cleanup_exc}"
                                )
                if time.monotonic() >= join_deadline:
                    break
                for thread in started_threads:
                    thread.join(timeout=0.01)
            if primary_error is None:
                try:
                    _dispatch_stream_messages(
                        stream_messages,
                        on_stdout=on_stdout,
                        on_stderr=on_stderr,
                    )
                    _raise_stream_error(stream_errors, stream_error_lock)
                except BaseException as exc:
                    primary_error = exc
            alive_before_close = [thread for thread in started_threads if thread.is_alive()]
            if alive_before_close:
                stop_streams.set()
                if process.poll() is None:
                    try:
                        _terminate_process(process)
                    except BaseException as cleanup_exc:
                        if primary_error is None:
                            primary_error = cleanup_exc
                        else:
                            primary_error = RelayError(
                                f"{primary_error}; process cleanup also failed: {cleanup_exc}"
                            )
                for pipe in (process.stdout, process.stderr):
                    if pipe is not None:
                        with suppress(OSError):
                            pipe.close()
                for thread in alive_before_close:
                    thread.join(timeout=STREAM_THREAD_JOIN_TIMEOUT_SECONDS)
        alive_streams = [
            name
            for name, thread in (("stdout", stdout_thread), ("stderr", stderr_thread))
            if thread in started_threads and thread.is_alive()
        ]
        if alive_streams:
            join_error = RelayError(
                f"JARVIS stream readers did not stop within the bound: {alive_streams}"
            )
            if primary_error is None:
                primary_error = join_error
            else:
                primary_error = RelayError(f"{primary_error}; {join_error}")
        with stream_error_lock:
            if primary_error is None and stream_errors:
                primary_error = stream_errors[0]
        if primary_error is None:
            try:
                process_containment.ensure_owned_process_tree_empty(process)
            except RuntimeError as exc:
                primary_error = RelayError(str(exc))
        try:
            process_containment.release_owned_process(process)
        except RuntimeError as exc:
            if primary_error is None:
                primary_error = RelayError(f"could not release process containment: {exc}")
            else:
                primary_error = RelayError(
                    f"{primary_error}; process containment release also failed: {exc}"
                )
        if primary_error is not None:
            raise primary_error
        if return_code is None:
            raise RelayError("JARVIS process ended without a return code")
        return subprocess.CompletedProcess(
            command,
            124 if timed_out else return_code if not canceled else -15,
            stdout=stdout_tail.render(),
            stderr=stderr_tail.render(),
        )

    def named_pipeline_command(self, pipeline_name: str) -> list[str]:
        """Return the command used to execute a named JARVIS pipeline."""
        return named_jarvis_command(
            python_bin=self._execution_python(),
            pipeline_name=pipeline_name,
        )

    def pipeline_command(self, pipeline_path: Path) -> list[str]:
        """Return the command used to execute a materialized JARVIS pipeline."""
        scheduler_name = _scheduler_name(pipeline_path)
        if scheduler_name is not None:
            provider_for_scheduler(scheduler_name)
            return scheduled_jarvis_command(
                scheduler_name,
                python_bin=self._execution_python(),
                pipeline_path=pipeline_path,
            )
        return yaml_jarvis_command(
            python_bin=self._execution_python(),
            pipeline_path=pipeline_path,
        )

    def _execution_python(self) -> str:
        """Return the receipt-bound interpreter or use unmanaged discovery explicitly."""
        if self.execution_python is not None:
            return self.execution_python
        return _unmanaged_jarvis_python(self.jarvis_bin)


def _drop_none(value: Any) -> Any:
    if isinstance(value, dict):
        typed = cast(dict[str, Any], value)
        return {key: _drop_none(item) for key, item in typed.items() if item is not None}
    if isinstance(value, list):
        typed_list = cast(list[Any], value)
        return [_drop_none(item) for item in typed_list]
    return value


def _drain_stream(
    stream: Any,
    stream_name: Literal["stdout", "stderr"],
    tail: _BoundedTextTail,
    messages: Queue[tuple[Literal["stdout", "stderr"], str]],
    stop: threading.Event,
    errors: list[BaseException],
    error_lock: threading.Lock,
) -> None:
    if stream is None:
        return
    try:
        while chunk := stream.readline(STREAM_READ_CHARACTERS):
            tail.append(chunk)
            while not stop.is_set():
                try:
                    messages.put((stream_name, chunk), timeout=STREAM_QUEUE_PUT_SECONDS)
                    break
                except Full:
                    continue
    except BaseException as exc:
        with error_lock:
            errors.append(exc)


def _dispatch_stream_messages(
    messages: Queue[tuple[Literal["stdout", "stderr"], str]],
    *,
    on_stdout: Callable[[str], None] | None,
    on_stderr: Callable[[str], None] | None,
) -> None:
    """Invoke output callbacks synchronously on the provider control thread."""
    while True:
        try:
            stream_name, chunk = messages.get_nowait()
        except Empty:
            return
        callback = on_stdout if stream_name == "stdout" else on_stderr
        if callback is not None:
            callback(chunk)


def _raise_stream_error(
    errors: list[BaseException],
    error_lock: threading.Lock,
) -> None:
    """Propagate the first stream-reader failure on the provider control thread."""
    with error_lock:
        if errors:
            raise errors[0]


def _terminate_process(process: subprocess.Popen[str]) -> None:
    try:
        process_containment.terminate_owned_process(process)
    except RuntimeError as exc:
        raise RelayError(str(exc)) from exc


def _scheduler_name(pipeline_path: Path) -> str | None:
    try:
        document = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigurationError(f"failed to read JARVIS pipeline: {pipeline_path}") from exc
    return _document_scheduler_name(document)


def _document_scheduler_name(document: object) -> str | None:
    if not isinstance(document, dict):
        return None
    typed = cast(dict[str, object], document)
    scheduler = typed.get("scheduler")
    if isinstance(scheduler, dict) and scheduler:
        typed_scheduler = cast(dict[str, object], scheduler)
        name = typed_scheduler.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ConfigurationError("JARVIS scheduler objects require an explicit provider name")
        return name
    config = typed.get("config")
    if isinstance(config, dict):
        typed_config = cast(dict[str, object], config)
        config_scheduler = _document_scheduler_name(typed_config)
        if config_scheduler is not None:
            return config_scheduler
    experiments = typed.get("experiments")
    if isinstance(experiments, list):
        typed_experiments = cast(list[object], experiments)
        for experiment in typed_experiments:
            experiment_scheduler = _document_scheduler_name(experiment)
            if experiment_scheduler is not None:
                return experiment_scheduler
    return None


def _unmanaged_jarvis_python(jarvis_bin: str) -> str:
    """Best-effort interpreter discovery for unmanaged and development launchers."""
    jarvis_path = Path(jarvis_bin)
    if jarvis_path.parent.name == "bin":
        candidate = jarvis_path.parent / "python"
        if candidate.exists():
            return str(candidate)
        shebang_python = _python_from_shebang(jarvis_path)
        if shebang_python is not None:
            return shebang_python
    resolved = shutil.which(jarvis_bin)
    if resolved is not None:
        resolved_path = Path(resolved)
        candidate = resolved_path.parent / "python"
        if candidate.exists():
            return str(candidate)
        shebang_python = _python_from_shebang(resolved_path)
        if shebang_python is not None:
            return shebang_python
    return "python"


def _python_from_shebang(path: Path) -> str | None:
    try:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
    except (IndexError, OSError, UnicodeDecodeError):
        return None
    if not first_line.startswith("#!"):
        return None
    command = first_line[2:].strip()
    if not command:
        return None
    executable = command.split(maxsplit=1)[0]
    if Path(executable).name.startswith("python"):
        return executable
    return None
