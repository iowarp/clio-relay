"""The subprocess-backed ``CommandRunner`` implementation.

Extracted from ``service_runtime.py`` (#231 rework slice): the concrete
``SubprocessCommandRunner`` the supervisor uses by default (real
``subprocess.run``/``subprocess.Popen`` calls, platform-appropriate process
isolation, and durable process-identity capture after launch) plus its
private stdin-delivery helper, ``_deliver_process_input``.

Depends on ``service_runtime_types`` (the ``LocalConnectorIdentity`` return
type), ``service_runtime_connector_identity`` (durable identity capture), and
``service_runtime_primitives`` (the just-started-process-group rollback
helper) -- never on the supervisor class, which imports
``SubprocessCommandRunner`` back qualified through this module instead.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_types as _types
from clio_relay.errors import RelayError


class SubprocessCommandRunner:
    """Command runner backed by subprocess."""

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a local subprocess with text output."""
        input_bytes = input_text.encode("utf-8") if input_text is not None else None
        result = subprocess.run(
            list(command),
            input=input_bytes,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        return subprocess.CompletedProcess(
            args=result.args,
            returncode=result.returncode,
            stdout=result.stdout.decode("utf-8", errors="replace"),
            stderr=result.stderr.decode("utf-8", errors="replace"),
        )

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
        env: dict[str, str] | None = None,
        isolate_process_group: bool = False,
        input_bytes: bytes | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start a local subprocess with owned log files."""
        stdout_handle = stdout_path.open("ab")
        stderr_handle = stderr_path.open("ab")
        creationflags = 0
        start_new_session = False
        if isolate_process_group:
            if os.name == "nt":
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                start_new_session = True
        try:
            process = subprocess.Popen(
                list(command),
                stdin=subprocess.PIPE if input_bytes is not None else None,
                stdout=stdout_handle,
                stderr=stderr_handle,
                # The launched connector outlives this CLI process.  In particular, a relay
                # command may itself be invoked with captured stdout/stderr by an MCP surface.
                # Closing inherited descriptors on Windows prevents the connector grandchild
                # from retaining those capture pipes and blocking the short-lived CLI forever.
                close_fds=True,
                env=env,
                creationflags=creationflags,
                start_new_session=start_new_session,
            )
            if input_bytes is not None:
                _deliver_process_input(
                    process,
                    input_bytes=input_bytes,
                    isolate_process_group=isolate_process_group,
                )
            return process
        finally:
            stdout_handle.close()
            stderr_handle.close()

    def local_process_identity(
        self,
        *,
        pid: int,
        owner_token: str,
        expected_config: str,
    ) -> _types.LocalConnectorIdentity:
        """Capture and verify immutable process identity after launch."""
        return _connector_identity._capture_local_connector_identity(
            pid=pid,
            owner_token=owner_token,
            expected_config=expected_config,
        )


def _deliver_process_input(
    process: subprocess.Popen[bytes],
    *,
    input_bytes: bytes,
    isolate_process_group: bool,
) -> None:
    """Write one private bootstrap document and close its anonymous pipe promptly."""
    input_pipe = process.stdin
    delivery_error: Exception | None = None
    if input_pipe is None:
        delivery_error = RuntimeError("subprocess stdin pipe was not created")
    else:
        try:
            written = input_pipe.write(input_bytes)
            if written != len(input_bytes):
                raise OSError("subprocess stdin accepted only a partial bootstrap document")
            input_pipe.flush()
        except Exception as exc:
            delivery_error = exc
        finally:
            try:
                input_pipe.close()
            except Exception as exc:
                if delivery_error is None:
                    delivery_error = exc
    if delivery_error is None:
        return
    if isolate_process_group:
        _primitives._terminate_just_started_process_group(process.pid)
    else:
        with suppress(OSError):
            process.terminate()
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            with suppress(OSError):
                process.kill()
    raise RelayError("failed to deliver private process bootstrap over stdin") from delivery_error
