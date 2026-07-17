from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from pytest import MonkeyPatch

_OUTER_RUNNER_READINESS_TIMEOUT_SECONDS = 60.0


def test_mcp_call_refuses_artifact_drift_before_launch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    artifact: dict[str, Any] = {
        "requested_command": "science-mcp",
        "resolved_executable": "/opt/science-mcp",
        "executable": {"path": "/opt/science-mcp", "sha256": "a" * 64},
        "install_spec": "/opt/science.whl",
        "install_source": "wheel",
        "install_artifact_sha256": "b" * 64,
        "input_files": [],
        "launcher_artifact_verified": True,
        "nested_launcher": False,
        "server_process_artifact_verified": True,
        "identity_error": None,
        "verified": True,
    }
    launched = False

    def server_artifact_identity(_server: str, _server_args: list[str]) -> dict[str, Any]:
        return artifact

    monkeypatch.setattr(
        cast(Any, runner),
        "_server_artifact_identity",
        server_artifact_identity,
    )

    def fail_if_launched(*_args: object, **_kwargs: object) -> None:
        nonlocal launched
        launched = True
        raise AssertionError("drifted server was launched")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fail_if_launched)

    return_code = cast(Any, runner).run_mcp_call_from_params(
        {
            "server": "science-mcp",
            "tool": "inspect",
            "expected_server_artifact_digest": "f" * 64,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert launched is False
    assert "changed after discovery" in result["protocol_error"]
    assert result["expected_server_artifact_digest"] == "f" * 64
    assert result["observed_server_artifact_digest"] == cast(Any, runner)._server_artifact_digest(
        artifact
    )


def test_outer_runner_termination_kills_nested_mcp_process_tree(tmp_path: Path) -> None:
    runner_path = _runner_path()
    pid_path = tmp_path / "nested-pids.json"
    server_path = tmp_path / "nested_server.py"
    outer_path = tmp_path / "outer_runner.py"
    server_path.write_text(
        """from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

grandchild = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(120)"],
)
Path(sys.argv[1]).write_text(
    json.dumps({"server": os.getpid(), "grandchild": grandchild.pid}),
    encoding="utf-8",
)
for line in sys.stdin:
    message = json.loads(line)
    if message.get("method") == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "nested-cancel-test", "version": "1"},
        }
        print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result}), flush=True)
    elif message.get("method") == "tools/call":
        while True:
            time.sleep(1)
""",
        encoding="utf-8",
    )
    outer_path.write_text(
        f"""from __future__ import annotations

import importlib.util
import sys

spec = importlib.util.spec_from_file_location("outer_mcp_runner", {str(runner_path)!r})
if spec is None or spec.loader is None:
    raise RuntimeError("could not load runner")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
raise SystemExit(
    runner.run_mcp_call_from_params(
        {{
            "server": sys.executable,
            "server_args": [{str(server_path)!r}, {str(pid_path)!r}],
            "tool": "inspect",
            "arguments": {{}},
            "timeout_seconds": 120,
        }}
    )
)
""",
        encoding="utf-8",
    )
    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    diagnostics_path = tmp_path / "outer-runner.log"
    with diagnostics_path.open("wb", buffering=0) as diagnostics:
        outer = subprocess.Popen(
            [sys.executable, str(outer_path)],
            cwd=tmp_path,
            stdout=diagnostics,
            stderr=subprocess.STDOUT,
            start_new_session=os.name != "nt",
            creationflags=creationflags,
        )
        nested_pids: list[int] = []
        try:
            _wait_for_file_while_running(
                pid_path,
                process=outer,
                diagnostics_path=diagnostics_path,
                timeout=_OUTER_RUNNER_READINESS_TIMEOUT_SECONDS,
            )
            identities = cast(dict[str, int], json.loads(pid_path.read_text(encoding="utf-8")))
            nested_pids = [identities["server"], identities["grandchild"]]
            assert all(_process_is_running(pid) for pid in nested_pids)

            if os.name == "nt":
                outer.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(outer.pid, signal.SIGTERM)
            outer.wait(timeout=12)

            _wait_for_processes_to_exit(nested_pids, timeout=10)
            assert outer.returncode != 0
            assert all(not _process_is_running(pid) for pid in nested_pids)
        finally:
            _force_stop_process_tree(outer)
            for pid in nested_pids:
                _force_stop_pid(pid)


def _wait_for_file_while_running(
    path: Path,
    *,
    process: subprocess.Popen[bytes],
    diagnostics_path: Path,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        return_code = process.poll()
        if return_code is not None:
            raise AssertionError(
                f"process exited with code {return_code} while waiting for {path}; "
                f"diagnostics: {_read_diagnostics(diagnostics_path)}"
            )
        time.sleep(0.05)
    raise AssertionError(
        f"timed out after {timeout:.1f}s waiting for {path}; "
        f"process_state={'running' if process.poll() is None else process.returncode}; "
        f"diagnostics: {_read_diagnostics(diagnostics_path)}"
    )


def _read_diagnostics(path: Path) -> str:
    try:
        contents = path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError as error:
        return f"<unavailable: {error}>"
    return contents[-4_000:] if contents else "<no output>"


def _wait_for_processes_to_exit(pids: list[int], *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(not _process_is_running(pid) for pid in pids):
            return
        time.sleep(0.05)
    running = [pid for pid in pids if _process_is_running(pid)]
    raise AssertionError(f"nested MCP processes remained alive: {running}")


def _process_is_running(pid: int) -> bool:
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            check=False,
            text=True,
        )
        return f'"{pid}"' in result.stdout
    result = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)],
        capture_output=True,
        check=False,
        text=True,
    )
    state = result.stdout.strip()
    return bool(state) and not state.startswith("Z")


def _force_stop_process_tree(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            check=False,
        )
    else:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


def _force_stop_pid(pid: int) -> None:
    if not _process_is_running(pid):
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            check=False,
        )
    else:
        with suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def _load_runner() -> ModuleType:
    path = _runner_path()
    spec = importlib.util.spec_from_file_location("clio_relay_mcp_call_runner_hardening", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load MCP call runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _runner_path() -> Path:
    return (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "mcp_call"
        / "runner.py"
    )
