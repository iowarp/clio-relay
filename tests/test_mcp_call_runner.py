from __future__ import annotations

import base64
import hashlib
import hmac
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, cast

from pytest import MonkeyPatch, mark, raises

from clio_relay.bootstrap import (
    JARVIS_CD_VERSION,
    JARVIS_CD_WHEEL_SHA256,
    JARVIS_CD_WHEEL_URL,
)


class McpCallRunnerModule(Protocol):
    def run_mcp_call_from_params(self, params: dict[str, object]) -> int:
        """Run a remote MCP call from serialized parameters."""
        ...


def _jarvis_cd_uv_lock(
    *,
    version: str = JARVIS_CD_VERSION,
    source_url: str = JARVIS_CD_WHEEL_URL,
    wheel_url: str = JARVIS_CD_WHEEL_URL,
    sha256: str = JARVIS_CD_WHEEL_SHA256,
    package_entries: int = 1,
    metadata_entries: int = 1,
    metadata_url: str = JARVIS_CD_WHEEL_URL,
    metadata_marker: str | None = None,
    jarvis_mcp_entries: int = 1,
    resolved_dependency_entries: int = 1,
    resolved_dependency_marker: str | None = None,
) -> bytes:
    """Return a minimal uv lock with independently mutable JARVIS binding surfaces."""
    lines = ["version = 1", ""]
    for _index in range(jarvis_mcp_entries):
        lines.extend(
            [
                "[[package]]",
                'name = "jarvis-mcp"',
                'version = "3.2.1"',
                'source = { editable = "." }',
                "dependencies = [",
                *(
                    (
                        '    { name = "jarvis-cd"'
                        + (
                            f", marker = {json.dumps(resolved_dependency_marker)}"
                            if resolved_dependency_marker is not None
                            else ""
                        )
                        + " },"
                    )
                    for _dependency_index in range(resolved_dependency_entries)
                ),
                "]",
                "",
                "[package.metadata]",
                "requires-dist = [",
                *(
                    (
                        f'    {{ name = "jarvis-cd", url = {json.dumps(metadata_url)}'
                        + (
                            f", marker = {json.dumps(metadata_marker)}"
                            if metadata_marker is not None
                            else ""
                        )
                        + " },"
                    )
                    for _requirement_index in range(metadata_entries)
                ),
                "]",
                "",
            ]
        )
    if package_entries == 0:
        lines.extend(
            [
                "[[package]]",
                'name = "unrelated"',
                'version = "1.0.0"',
                'source = { registry = "https://pypi.org/simple" }',
                "",
            ]
        )
    for _index in range(package_entries):
        lines.extend(
            [
                "[[package]]",
                'name = "jarvis-cd"',
                f"version = {json.dumps(version)}",
                f"source = {{ url = {json.dumps(source_url)} }}",
                "wheels = [",
                (f'    {{ url = {json.dumps(wheel_url)}, hash = "sha256:{sha256}" }},'),
                "]",
                "",
            ]
        )
    return "\n".join(lines).encode("utf-8")


def _jarvis_cd_lock_expectation() -> dict[str, str]:
    return {
        "schema_version": "clio-relay.jarvis-cd-lock-expectation.v1",
        "version": JARVIS_CD_VERSION,
        "url": JARVIS_CD_WHEEL_URL,
        "sha256": JARVIS_CD_WHEEL_SHA256,
    }


def _verified_jarvis_server_artifact() -> dict[str, Any]:
    """Return the minimal verified built-in JARVIS artifact boundary."""
    expected = _jarvis_cd_lock_expectation()
    return {
        "verified": True,
        "nested_runtime": {
            "schema_version": "clio-kit.locked-server.v4",
            "server_name": "jarvis",
            "locked_runtime_verified": True,
            "jarvis_cd_lock_binding": {
                "schema_version": "clio-relay.jarvis-cd-lock-binding.v1",
                "dependency": "jarvis-cd",
                "verified": True,
                "error": None,
                "expected_version": expected["version"],
                "expected_url": expected["url"],
                "expected_sha256": expected["sha256"],
                "observed_version": expected["version"],
                "observed_source_url": expected["url"],
                "observed_wheel_url": expected["url"],
                "observed_wheel_sha256": expected["sha256"],
                "jarvis_mcp_package_entry_count": 1,
                "resolved_dependency_entry_count": 1,
                "observed_resolved_dependency_entries": [{"name": "jarvis-cd"}],
                "metadata_requirement_entry_count": 1,
                "observed_metadata_requirement_entries": [
                    {"name": "jarvis-cd", "url": expected["url"]}
                ],
                "observed_metadata_requirement_urls": [expected["url"]],
                "package_entry_count": 1,
                "wheel_entry_count": 1,
            },
        },
    }


def test_mcp_call_runner_invokes_tool_with_defaults(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    captured: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
        env_from: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del env_from
        captured["command"] = json.dumps(command)
        captured["tool"] = tool
        captured["arguments"] = json.dumps(arguments)
        captured["timeout"] = str(timeout)
        stdout = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-init", "result": {}}),
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "clio-relay-mcp-call",
                        "result": {"content": [{"type": "text", "text": "ok"}]},
                    }
                ),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect", "arguments": {"path": "x"}}
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert json.loads(captured["command"]) == ["science-mcp"]
    assert captured["tool"] == "inspect"
    assert json.loads(captured["arguments"]) == {"path": "x"}
    assert captured["timeout"] == "300"
    assert result["returncode"] == 0
    assert result["timed_out"] is False
    assert result["protocol_error"] is None
    assert result["protocol_result"] == {"content": [{"type": "text", "text": "ok"}]}


def test_mcp_call_runner_discovers_tools_and_records_server_provenance(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    captured: dict[str, object] = {}

    def fake_run(
        command: list[str],
        *,
        tool: str | None,
        arguments: dict[str, object],
        timeout: int | None,
        operation: str,
        env_from: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del env_from
        captured.update(
            command=command,
            tool=tool,
            arguments=arguments,
            timeout=timeout,
            operation=operation,
        )
        stdout = "\n".join(
            [
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "clio-relay-mcp-init",
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "serverInfo": {"name": "science", "version": "1.0"},
                        },
                    }
                ),
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "clio-relay-mcp-tools-list",
                        "result": {
                            "tools": [
                                {
                                    "name": "inspect",
                                    "description": "Inspect data.",
                                    "inputSchema": {"type": "object", "properties": {}},
                                }
                            ]
                        },
                    }
                ),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "operation": "tools/list", "timeout_seconds": 20}
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert captured == {
        "command": ["science-mcp"],
        "tool": None,
        "arguments": {},
        "timeout": 20,
        "operation": "tools/list",
    }
    assert result["operation"] == "tools/list"
    assert result["protocol_result"]["tools"][0]["name"] == "inspect"
    assert result["structured_result"] is None
    assert result["protocol_version"] == "2024-11-05"
    assert result["server_info"] == {"name": "science", "version": "1.0"}


def test_mcp_call_runner_discovers_tools_from_real_stdio_server(tmp_path: Path) -> None:
    runner = _load_runner()
    server_path = tmp_path / "stdio_server.py"
    server_path.write_text(
        """import json
import sys

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        response = {
            "jsonrpc": "2.0",
            "id": message["id"],
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "real-test-server", "version": "1.0"},
            },
        }
        print(json.dumps(response), flush=True)
    elif method == "tools/list":
        response = {
            "jsonrpc": "2.0",
            "id": message["id"],
            "result": {
                "tools": [
                    {
                        "name": "inspect",
                        "description": "Inspect a path.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"path": {"type": "string"}},
                            "required": ["path"],
                        },
                    }
                ]
            },
        }
        print(json.dumps(response), flush=True)
        break
""",
        encoding="utf-8",
    )
    original_cwd = Path.cwd()
    try:
        os.chdir(tmp_path)
        return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
            {
                "server": sys.executable,
                "server_args": [str(server_path)],
                "operation": "tools/list",
                "timeout_seconds": 10,
            }
        )
    finally:
        os.chdir(original_cwd)
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert result["protocol_error"] is None
    assert result["server_info"]["name"] == "real-test-server"
    assert result["protocol_result"]["tools"][0]["name"] == "inspect"
    assert result["server_artifact"]["verified"] is False
    assert result["server_artifact"]["server_process_artifact_verified"] is False
    assert result["server_artifact"]["executable"]["sha256"]
    assert result["server_artifact"]["input_files"][0]["path"] == str(server_path.resolve())
    assert result["server_artifact"]["input_files"][0]["sha256"]
    assert "distribution RECORD closure" in result["server_artifact"]["identity_error"]


def test_mcp_session_runs_inside_jarvis_worker_thread(tmp_path: Path) -> None:
    """A durable JARVIS package may execute the MCP session off the main thread."""
    runner = _load_runner()
    server_path = tmp_path / "threaded_stdio_server.py"
    server_path.write_text(
        """import json
import sys

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": message["id"],
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "threaded-test-server", "version": "1.0"},
            },
        }), flush=True)
    elif method == "tools/list":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": message["id"],
            "result": {"tools": []},
        }), flush=True)
        break
""",
        encoding="utf-8",
    )

    def run_in_worker() -> subprocess.CompletedProcess[str]:
        return cast(Any, runner)._run_mcp_session(
            [sys.executable, str(server_path)],
            tool=None,
            arguments={},
            timeout=10,
            operation="tools/list",
            env_from={},
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(run_in_worker).result(timeout=20)

    assert result.returncode == 0
    assert "threaded-test-server" in result.stdout
    assert result.stderr == ""


def test_direct_console_launcher_binds_real_distribution_record_and_detects_mutation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    uv = shutil.which("uv")
    assert uv is not None, "the production test suite requires uv"
    wheel = _minimal_console_wheel(tmp_path)
    environment = tmp_path / "server-environment"
    subprocess.run(
        [uv, "venv", "--python", sys.executable, str(environment)],
        check=True,
        capture_output=True,
        text=True,
    )
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run(
        [uv, "pip", "install", "--python", str(python), "--no-deps", str(wheel)],
        check=True,
        capture_output=True,
        text=True,
    )
    site_packages = Path(
        subprocess.run(
            [
                str(python),
                "-c",
                (
                    "from pathlib import Path; import science_mcp; "
                    "print(Path(science_mcp.__file__).parent.parent)"
                ),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    monkeypatch.setattr(sys, "path", [str(site_packages), *sys.path])
    launcher = environment / ("Scripts/science-mcp.exe" if os.name == "nt" else "bin/science-mcp")

    before = cast(Any, runner)._server_artifact_identity(str(launcher), [])
    module = site_packages / "science_mcp" / "__init__.py"
    module.write_bytes(b"VALUE = 2\n\ndef main():\n    return None\n")
    after = cast(Any, runner)._server_artifact_identity(str(launcher), [])

    assert before["verified"] is True
    assert before["python_distribution_runtime"]["runtime_closure_verified"] is True
    assert before["python_distribution_runtime"]["distribution"] == "science-mcp"
    assert before["python_distribution_runtime"]["entry_point"] == "science-mcp"
    assert after["verified"] is False
    assert after["python_distribution_runtime"]["runtime_closure_verified"] is False
    assert "RECORD hash mismatch" in after["python_distribution_runtime"]["error"]
    assert before["executable"]["sha256"] == after["executable"]["sha256"]
    assert cast(Any, runner)._server_artifact_digest(before) != cast(
        Any, runner
    )._server_artifact_digest(after)


def test_persistent_uv_tool_clio_kit_runtime_is_receipt_bindable(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    tool = tmp_path / ("clio-kit.exe" if os.name == "nt" else "clio-kit")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    wheel = tmp_path / "clio_kit-2.3.1-py3-none-any.whl"
    source = tmp_path / "clio_kit" / "__init__.py"
    lock = tmp_path / "share" / "clio-kit-mcp-servers" / "jarvis" / "uv.lock"
    source.parent.mkdir(parents=True)
    lock.parent.mkdir(parents=True)
    tool.write_bytes(b"persistent-clio-kit-tool")
    uv.write_bytes(b"pinned-uv")
    wheel.write_bytes(b"released-clio-kit-wheel")
    source.write_text(_locked_clio_kit_v4_launcher_source(), encoding="utf-8")
    lock.write_bytes(_jarvis_cd_uv_lock())

    def console_distribution_identity(_path: Path) -> dict[str, object]:
        return {
            "schema_version": "clio-relay.python-distribution-runtime.v1",
            "distribution": "clio-kit",
            "distribution_version": "2.3.1",
            "entry_point": "clio-kit",
            "entry_point_value": "clio_kit:main",
            "record_sha256": "b" * 64,
            "runtime_closure_sha256": "c" * 64,
            "runtime_file_count": 100,
            "runtime_bytes": 4096,
            "runtime_closure_verified": True,
            "direct_url": {"url": wheel.resolve().as_uri()},
            "provider_interpreter": str(tmp_path / "tool-python"),
            "contract_source_path": str(source),
            "server_lock_paths": {"jarvis": str(lock)},
            "error": None,
        }

    monkeypatch.setattr(
        runner,
        "_python_console_distribution_identity",
        console_distribution_identity,
    )

    identity = cast(Any, runner)._server_artifact_identity(
        str(tool),
        ["mcp-server", "jarvis"],
        verify_relay_jarvis_cd_lock=True,
    )

    assert identity["install_source"] == "uv-tool"
    assert identity["install_spec"] == str(wheel.resolve())
    assert (
        identity["install_artifact_sha256"]
        == hashlib.sha256(b"released-clio-kit-wheel").hexdigest()
    )
    assert identity["nested_launcher"] is True
    assert identity["nested_runtime"]["persistent_tool"] is True
    assert identity["nested_runtime"]["jarvis_cd_lock_binding"]["verified"] is True
    assert identity["nested_runtime"]["locked_runtime_verified"] is True
    assert identity["server_process_artifact_verified"] is True
    assert identity["verified"] is True


def test_external_console_probe_preserves_logical_provider_path(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A symlink-style tool interpreter path must retain its venv context."""
    runner = _load_runner()
    provider = tmp_path / "tool" / "bin" / "python"
    provider.parent.mkdir(parents=True)
    provider.write_bytes(b"provider")
    logical_provider = provider.parent / ".." / "bin" / provider.name
    launcher = tmp_path / "science-mcp"
    launcher.write_text(f"#!{logical_provider}\n", encoding="utf-8")
    captured: dict[str, list[str]] = {}

    def run_probe(
        command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"matches": []}),
            stderr="",
        )

    monkeypatch.setattr(cast(Any, runner).subprocess, "run", run_probe)

    evidence = cast(Any, runner)._external_python_console_distribution_identity(
        launcher,
        command_name="science-mcp",
    )

    assert captured["command"][0] == str(logical_provider)
    assert captured["command"][0] != str(logical_provider.resolve())
    assert (
        evidence["error"]
        == "persistent tool launcher has no unique installed console-script distribution"
    )


def test_mcp_call_runner_supports_server_arguments(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    captured: dict[str, list[str]] = {}

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
        env_from: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del tool, arguments, timeout, env_from
        captured["command"] = command
        stdout = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-init", "result": {}}),
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-call", "result": {}}),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": "uvx",
            "server_args": [
                "--from",
                "clio-kit==2.2.6",
                "clio-kit",
                "mcp-server",
                "jarvis",
            ],
            "tool": "jarvis_describe",
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert captured["command"][-5:] == [
        "--from",
        "clio-kit==2.2.6",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    assert result["server"] == "uvx"
    assert result["server_args"] == [
        "--from",
        "clio-kit==2.2.6",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    assert result["server_artifact"]["install_spec"] == "clio-kit==2.2.6"
    assert result["server_artifact"]["install_source"] == "pypi"
    assert result["server_artifact"]["nested_launcher"] is True
    assert result["server_artifact"]["server_process_artifact_verified"] is False
    assert result["server_artifact"]["verified"] is False
    assert "child source, lock, or uv runtime" in result["server_artifact"]["identity_error"]


def test_mcp_call_runner_verifies_locked_clio_kit_child_runtime(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.3.1-py3-none-any.whl"
    prefix = "clio_kit-2.3.1.data/data/clio-kit-mcp-servers/spack/"
    launcher_source = _locked_clio_kit_v4_launcher_source()
    project = {
        "README.md": b"Spack MCP\n",
        "pyproject.toml": b"[project]\nname='spack-mcp'\n",
        "server.json": b"{}\n",
        "src/spack_mcp/server.py": b"VALUE = 1\n",
        "uv.lock": b"version = 1\n",
    }
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("clio_kit/__init__.py", launcher_source)
        for relative, content in project.items():
            archive.writestr(prefix + relative, content)
        archive.writestr(prefix + "tests/test_server.py", "raise AssertionError\n")
        archive.writestr(prefix + "src/spack_mcp/__pycache__/ignored.pyc", b"ignored")

    identity = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        ["--from", str(wheel), "clio-kit", "mcp-server", "spack"],
    )

    assert identity["nested_launcher"] is True
    assert identity["nested_runtime"]["schema_version"] == ("clio-kit.locked-server.v4")
    assert identity["nested_runtime"]["server_name"] == "spack"
    assert identity["nested_runtime"]["runtime_policy"] == (
        "uv-run:materialized:frozen:no-editable:no-dev:v3"
    )
    assert identity["nested_runtime"]["project_sha256"] == _clio_kit_v4_project_sha256(project)
    assert (
        identity["nested_runtime"]["lock_sha256"] == hashlib.sha256(project["uv.lock"]).hexdigest()
    )
    assert identity["nested_runtime"]["runtime_file_count"] == len(project)
    assert identity["nested_runtime"]["runtime_bytes"] == sum(map(len, project.values()))
    assert identity["nested_runtime"]["contract_source_verified"] is True
    assert identity["nested_runtime"]["uv_executable"]["sha256"]
    assert identity["nested_runtime"]["locked_runtime_verified"] is True
    assert identity["server_process_artifact_verified"] is True
    assert identity["verified"] is True
    assert identity["identity_error"] is None


def test_mcp_call_runner_verifies_relay_jarvis_cd_pin_in_locked_child_runtime(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    assert cast(Any, runner).JARVIS_CD_VERSION == JARVIS_CD_VERSION
    assert cast(Any, runner).JARVIS_CD_WHEEL_URL == JARVIS_CD_WHEEL_URL
    assert cast(Any, runner).JARVIS_CD_WHEEL_SHA256 == JARVIS_CD_WHEEL_SHA256
    monkeypatch.chdir(tmp_path)
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.5.3-py3-none-any.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        server_name="jarvis",
        project={
            "pyproject.toml": b"[project]\nname='jarvis-mcp'\n",
            "uv.lock": _jarvis_cd_uv_lock(),
        },
    )
    launched = False

    def fake_run(
        command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal launched
        launched = True
        stdout = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-init", "result": {}}),
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-call", "result": {}}),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": str(uvx),
            "server_args": [
                "--from",
                str(wheel),
                "clio-kit",
                "mcp-server",
                "jarvis",
            ],
            "expected_jarvis_cd_lock_binding": _jarvis_cd_lock_expectation(),
            "tool": "jarvis_describe",
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert launched is True
    binding = result["server_artifact"]["nested_runtime"]["jarvis_cd_lock_binding"]
    assert binding == {
        "dependency": "jarvis-cd",
        "error": None,
        "expected_sha256": JARVIS_CD_WHEEL_SHA256,
        "expected_url": JARVIS_CD_WHEEL_URL,
        "expected_version": JARVIS_CD_VERSION,
        "jarvis_mcp_package_entry_count": 1,
        "observed_resolved_dependency_entries": [{"name": "jarvis-cd"}],
        "metadata_requirement_entry_count": 1,
        "observed_metadata_requirement_entries": [
            {"name": "jarvis-cd", "url": JARVIS_CD_WHEEL_URL}
        ],
        "observed_metadata_requirement_urls": [JARVIS_CD_WHEEL_URL],
        "observed_source_url": JARVIS_CD_WHEEL_URL,
        "observed_version": JARVIS_CD_VERSION,
        "observed_wheel_sha256": JARVIS_CD_WHEEL_SHA256,
        "observed_wheel_url": JARVIS_CD_WHEEL_URL,
        "package_entry_count": 1,
        "resolved_dependency_entry_count": 1,
        "schema_version": "clio-relay.jarvis-cd-lock-binding.v1",
        "verified": True,
        "wheel_entry_count": 1,
    }


@mark.parametrize(
    ("lock_content", "expected_error"),
    [
        (_jarvis_cd_uv_lock(version="0.0.0"), "version does not match relay pin"),
        (
            _jarvis_cd_uv_lock(
                source_url=("https://example.invalid/jarvis_cd-1.3.11-py3-none-any.whl"),
                wheel_url=("https://example.invalid/jarvis_cd-1.3.11-py3-none-any.whl"),
            ),
            "source URL does not match relay pin",
        ),
        (_jarvis_cd_uv_lock(sha256="0" * 64), "wheel SHA-256 does not match relay pin"),
        (_jarvis_cd_uv_lock(package_entries=0), "exactly one jarvis-cd package record"),
        (_jarvis_cd_uv_lock(package_entries=2), "exactly one jarvis-cd package record"),
        (
            _jarvis_cd_uv_lock(
                wheel_url="https://example.invalid/jarvis_cd-1.3.11-py3-none-any.whl"
            ),
            "source and wheel URLs do not match",
        ),
        (
            _jarvis_cd_uv_lock(
                metadata_url="https://example.invalid/jarvis_cd-1.3.11-py3-none-any.whl"
            ),
            "metadata jarvis-cd URL does not match relay pin",
        ),
        (
            _jarvis_cd_uv_lock(metadata_entries=0),
            "metadata must contain exactly one jarvis-cd requirement",
        ),
        (
            _jarvis_cd_uv_lock(metadata_entries=2),
            "metadata must contain exactly one jarvis-cd requirement",
        ),
        (
            _jarvis_cd_uv_lock(metadata_marker="sys_platform == 'never'"),
            "metadata jarvis-cd requirement must be an unconditional direct URL",
        ),
        (
            _jarvis_cd_uv_lock(jarvis_mcp_entries=2),
            "exactly one jarvis-mcp package record",
        ),
        (
            _jarvis_cd_uv_lock(resolved_dependency_entries=0),
            "must resolve exactly one direct jarvis-cd dependency",
        ),
        (
            _jarvis_cd_uv_lock(resolved_dependency_entries=2),
            "must resolve exactly one direct jarvis-cd dependency",
        ),
        (
            _jarvis_cd_uv_lock(resolved_dependency_marker="sys_platform == 'never'"),
            "resolved jarvis-cd dependency must be unconditional",
        ),
        (
            _jarvis_cd_uv_lock(resolved_dependency_marker="typed-marker").replace(
                b'marker = "typed-marker"',
                b"marker = 1979-05-27T07:32:00Z",
            ),
            "resolved jarvis-cd dependency must be unconditional",
        ),
        (
            _jarvis_cd_uv_lock(metadata_marker="typed-marker").replace(
                b'marker = "typed-marker"',
                b"marker = nan",
            ),
            "metadata jarvis-cd requirement must be an unconditional direct URL",
        ),
    ],
    ids=[
        "wrong-version",
        "wrong-url",
        "wrong-hash",
        "missing",
        "duplicate",
        "source-wheel-mismatch",
        "wrong-metadata-url",
        "missing-metadata-requirement",
        "duplicate-metadata-requirement",
        "conditional-metadata-requirement",
        "duplicate-jarvis-mcp-package",
        "missing-resolved-dependency",
        "duplicate-resolved-dependency",
        "conditional-resolved-dependency",
        "datetime-resolved-dependency-marker",
        "nonfinite-metadata-requirement-marker",
    ],
)
def test_mcp_call_runner_refuses_unbound_jarvis_cd_lock_before_process_launch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    lock_content: bytes,
    expected_error: str,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.5.3-py3-none-any.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        server_name="jarvis",
        project={
            "pyproject.toml": b"[project]\nname='jarvis-mcp'\n",
            "uv.lock": lock_content,
        },
    )
    opened = False

    def fail_if_opened(
        _command: list[str],
        *,
        env_from: dict[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        del env_from
        nonlocal opened
        opened = True
        raise AssertionError("_open_process must not run for an unbound JARVIS lock")

    monkeypatch.setattr(cast(Any, runner), "_open_process", fail_if_opened)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": str(uvx),
            "server_args": [
                "--from",
                str(wheel),
                "clio-kit",
                "mcp-server",
                "jarvis",
            ],
            "expected_jarvis_cd_lock_binding": _jarvis_cd_lock_expectation(),
            "tool": "jarvis_describe",
        }
    )
    result_text = (tmp_path / "mcp-result.json").read_text(encoding="utf-8")

    def reject_nonfinite_json(value: str) -> None:
        raise ValueError(f"non-finite JSON constant: {value}")

    result = json.loads(result_text, parse_constant=reject_nonfinite_json)

    assert return_code == 1
    assert opened is False
    assert expected_error in result["protocol_error"]
    binding = result["server_artifact"]["nested_runtime"]["jarvis_cd_lock_binding"]
    assert binding["verified"] is False
    assert expected_error in binding["error"]


def test_mcp_call_runner_refuses_unverified_builtin_jarvis_launcher_before_launch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A correct nested pin cannot authorize an unverified outer launcher."""
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uvx.write_bytes(b"unverified-launcher")
    artifact = _verified_jarvis_server_artifact()
    artifact["verified"] = False
    artifact["identity_error"] = "launcher digest did not verify"
    monkeypatch.setattr(
        runner,
        "_server_artifact_identity",
        lambda *_args, **_kwargs: artifact,
    )
    opened = False

    def fail_if_opened(
        _command: list[str],
        *,
        env_from: dict[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        del env_from
        nonlocal opened
        opened = True
        raise AssertionError("_open_process must not run for an unverified launcher")

    monkeypatch.setattr(runner, "_open_process", fail_if_opened)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": str(uvx),
            "expected_jarvis_cd_lock_binding": _jarvis_cd_lock_expectation(),
            "tool": "jarvis_describe",
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert opened is False
    assert "launcher digest did not verify" in result["protocol_error"]


def test_registered_jarvis_server_uses_artifact_binding_without_relay_dependency_pin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "operator_clio_kit-py3-none-any.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        server_name="jarvis",
        project={
            "pyproject.toml": b"[project]\nname='jarvis-mcp'\n",
            "uv.lock": _jarvis_cd_uv_lock(version="9.9.9"),
        },
    )
    launched = False

    def fake_run(
        command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal launched
        launched = True
        stdout = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-init", "result": {}}),
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-call", "result": {}}),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": str(uvx),
            "server_args": [
                "--from",
                str(wheel),
                "clio-kit",
                "mcp-server",
                "jarvis",
            ],
            "tool": "jarvis_describe",
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert launched is True
    assert "expected_jarvis_cd_lock_binding" not in result
    assert result["server_artifact"]["nested_runtime"]["locked_runtime_verified"] is True
    assert "jarvis_cd_lock_binding" not in result["server_artifact"]["nested_runtime"]


def test_mcp_call_runner_v4_identity_resists_structural_collision(tmp_path: Path) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    base = b"[project]\nname='collision-fixture'\n"
    payload = b"VALUE = 1\n"
    framed_source = len(b"src.py").to_bytes(8, "big") + b"src.py" + payload
    collapsed = {
        "pyproject.toml": base + framed_source,
        "uv.lock": b"version = 1\n",
    }
    separated = {
        "pyproject.toml": base,
        "src.py": payload,
        "uv.lock": b"version = 1\n",
    }
    collapsed_wheel = tmp_path / "clio_kit-2.3.1-collapsed.whl"
    separated_wheel = tmp_path / "clio_kit-2.3.1-separated.whl"
    _write_synthetic_clio_kit_wheel(collapsed_wheel, project=collapsed)
    _write_synthetic_clio_kit_wheel(separated_wheel, project=separated)

    collapsed_identity = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        ["--from", str(collapsed_wheel), "clio-kit", "mcp-server", "spack"],
    )["nested_runtime"]
    separated_identity = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        ["--from", str(separated_wheel), "clio-kit", "mcp-server", "spack"],
    )["nested_runtime"]

    assert _legacy_clio_kit_project_sha256(collapsed) == _legacy_clio_kit_project_sha256(separated)
    assert collapsed_identity["project_sha256"] != separated_identity["project_sha256"]
    assert collapsed_identity["locked_runtime_verified"] is True
    assert separated_identity["locked_runtime_verified"] is True


def test_mcp_call_runner_v4_identity_ignores_only_launcher_exclusions(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    project = {
        "README.md": b"Spack MCP\n",
        "pyproject.toml": b"[project]\nname='spack-mcp'\n",
        "src/spack_mcp/server.py": b"VALUE = 1\n",
        "uv.lock": b"version = 1\n",
    }
    baseline = tmp_path / "clio_kit-2.3.1-baseline.whl"
    excluded_mutation = tmp_path / "clio_kit-2.3.1-excluded.whl"
    included_mutation = tmp_path / "clio_kit-2.3.1-included.whl"
    lock_mutation = tmp_path / "clio_kit-2.3.1-lock.whl"
    _write_synthetic_clio_kit_wheel(baseline, project=project)
    _write_synthetic_clio_kit_wheel(
        excluded_mutation,
        project=project,
        excluded={"tests/test_server.py": b"assert False\n", "coverage.xml": b"changed"},
    )
    _write_synthetic_clio_kit_wheel(
        included_mutation,
        project={**project, "README.md": b"mutated\n"},
    )
    _write_synthetic_clio_kit_wheel(
        lock_mutation,
        project={**project, "uv.lock": b"version = 2\n"},
    )

    def nested_identity(wheel: Path) -> dict[str, object]:
        artifact = cast(Any, runner)._server_artifact_identity(
            str(uvx),
            ["--from", str(wheel), "clio-kit", "mcp-server", "spack"],
        )
        return cast(dict[str, object], artifact["nested_runtime"])

    baseline_identity = nested_identity(baseline)
    assert (
        nested_identity(excluded_mutation)["project_sha256"] == baseline_identity["project_sha256"]
    )
    assert (
        nested_identity(included_mutation)["project_sha256"] != baseline_identity["project_sha256"]
    )
    mutated_lock_identity = nested_identity(lock_mutation)
    assert mutated_lock_identity["project_sha256"] != baseline_identity["project_sha256"]
    assert mutated_lock_identity["lock_sha256"] != baseline_identity["lock_sha256"]


def test_mcp_call_runner_rejects_unsafe_clio_kit_wheel_paths(tmp_path: Path) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.3.1-unsafe.whl"
    project = {
        "pyproject.toml": b"[project]\nname='spack-mcp'\n",
        "uv.lock": b"version = 1\n",
    }
    _write_synthetic_clio_kit_wheel(
        wheel,
        project=project,
        outer_members={"../wheel-escape": b"unsafe"},
    )

    identity = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        ["--from", str(wheel), "clio-kit", "mcp-server", "spack"],
    )

    assert identity["nested_runtime"]["locked_runtime_verified"] is False
    assert "unsafe member path" in identity["nested_runtime"]["error"]
    assert identity["verified"] is False


def test_mcp_call_runner_rejects_linked_clio_kit_runtime_member(tmp_path: Path) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.3.1-linked.whl"
    project = {
        "pyproject.toml": b"[project]\nname='spack-mcp'\n",
        "uv.lock": b"version = 1\n",
    }
    _write_synthetic_clio_kit_wheel(wheel, project=project)
    link = zipfile.ZipInfo(
        "clio_kit-2.3.1.data/data/clio-kit-mcp-servers/spack/src/spack_mcp/link.py"
    )
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr(link, "server.py")

    identity = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        ["--from", str(wheel), "clio-kit", "mcp-server", "spack"],
    )

    assert identity["nested_runtime"]["locked_runtime_verified"] is False
    assert "non-regular file" in identity["nested_runtime"]["error"]
    assert identity["verified"] is False


def test_mcp_call_runner_rejects_clio_kit_wheel_replacement(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.3.1-replaced.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        project={
            "pyproject.toml": b"[project]\nname='spack-mcp'\n",
            "uv.lock": b"version = 1\n",
        },
    )
    original_file_identity = cast(Any, runner)._file_identity
    replaced = False

    def replace_after_identity(path: Path) -> dict[str, object] | None:
        nonlocal replaced
        result = cast(dict[str, object] | None, original_file_identity(path))
        if not replaced and result is not None and path.resolve() == wheel.resolve():
            replaced = True
            wheel.write_bytes(b"replacement")
        return result

    monkeypatch.setattr(cast(Any, runner), "_file_identity", replace_after_identity)

    identity = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        ["--from", str(wheel), "clio-kit", "mcp-server", "spack"],
    )

    assert replaced is True
    assert identity["nested_runtime"]["locked_runtime_verified"] is False
    assert "changed before runtime verification" in identity["nested_runtime"]["error"]
    assert identity["verified"] is False


def test_mcp_call_runner_executes_private_snapshot_during_swap_and_restore(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.3.1-swap.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        project={
            "pyproject.toml": b"[project]\nname='spack-mcp'\n",
            "uv.lock": b"version = 1\n",
        },
    )
    benign_bytes = wheel.read_bytes()
    malicious = tmp_path / "malicious.whl"
    malicious_bytes = b"malicious-wheel-consumed"
    malicious.write_bytes(malicious_bytes)
    backup = tmp_path / "original-wheel.backup"
    original_args = ["--from", str(wheel), "clio-kit", "mcp-server", "spack"]
    discovery = cast(Any, runner)._server_artifact_identity(str(uvx), original_args)
    expected_digest = cast(Any, runner)._server_artifact_digest(discovery)
    consumed: list[bytes] = []
    attack_outcomes: list[str] = []
    snapshot_path: Path | None = None

    def swap_source_during_session(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
        env_from: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        nonlocal snapshot_path
        del tool, arguments, timeout, env_from
        from_index = command.index("--from")
        snapshot_path = Path(command[from_index + 1])
        assert snapshot_path != wheel
        assert snapshot_path.is_file()
        consumed.append(snapshot_path.read_bytes())
        try:
            wheel.replace(backup)
        except OSError:
            assert os.name == "nt"
            attack_outcomes.append("source-replacement-blocked")
            consumed.append(snapshot_path.read_bytes())
        else:
            malicious.replace(wheel)
            try:
                attack_outcomes.append("source-swapped-and-restored")
                assert wheel.read_bytes() == malicious_bytes
                consumed.append(snapshot_path.read_bytes())
            finally:
                wheel.unlink()
                backup.replace(wheel)
        stdout = "\n".join(
            [
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "clio-relay-mcp-init",
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {"tools": {}},
                        },
                    }
                ),
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "clio-relay-mcp-call",
                        "result": {"content": [{"type": "text", "text": "safe"}]},
                    }
                ),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", swap_source_during_session)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": str(uvx),
            "server_args": original_args,
            "tool": "spack_find",
            "expected_server_artifact_digest": expected_digest,
        }
    )
    result_payload = (tmp_path / "mcp-result.json").read_text(encoding="utf-8")
    result = json.loads(result_payload)

    assert return_code == 0, json.dumps(result, indent=2)
    assert consumed == [benign_bytes, benign_bytes]
    assert attack_outcomes == [
        "source-replacement-blocked" if os.name == "nt" else "source-swapped-and-restored"
    ]
    assert malicious_bytes not in consumed
    assert wheel.read_bytes() == benign_bytes
    assert snapshot_path is not None and not snapshot_path.exists()
    assert str(snapshot_path) not in result_payload
    assert result["server_args"] == original_args
    assert result["server_artifact"]["install_spec"] == str(wheel)
    assert (
        result["server_artifact"]["install_artifact_sha256"]
        == hashlib.sha256(benign_bytes).hexdigest()
    )
    assert result["observed_server_artifact_digest"] == expected_digest
    execution = result["server_execution_artifact"]
    assert execution["source_sha256"] == hashlib.sha256(benign_bytes).hexdigest()
    assert execution["snapshot_sha256"] == execution["source_sha256"]
    assert execution["snapshot_verified_before_launch"] is True
    assert execution["snapshot_verified_after_launch"] is True
    assert execution["source_verified_after_launch"] is True
    assert execution["cleanup_verified"] is True


@mark.parametrize("mutation_target", ["snapshot", "source"])
def test_mcp_call_runner_private_launch_fails_closed_on_artifact_mutation(
    tmp_path: Path,
    mutation_target: str,
) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / f"clio_kit-2.3.1-{mutation_target}.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        project={
            "pyproject.toml": b"[project]\nname='spack-mcp'\n",
            "uv.lock": b"version = 1\n",
        },
    )
    benign_bytes = wheel.read_bytes()
    server_args = ["--from", str(wheel), "clio-kit", "mcp-server", "spack"]
    artifact = cast(Any, runner)._server_artifact_identity(str(uvx), server_args)
    evidence: dict[str, object] | None = None
    snapshot_path: Path | None = None
    mutation_blocked = False
    security_error: ValueError | None = None

    try:
        with cast(Any, runner)._prepared_mcp_launch(
            [str(uvx), *server_args],
            server_args=server_args,
            server_artifact=artifact,
        ) as prepared:
            launch_command, evidence = cast(
                tuple[list[str], dict[str, object]],
                prepared,
            )
            snapshot_path = Path(launch_command[launch_command.index("--from") + 1])
            target = snapshot_path if mutation_target == "snapshot" else wheel
            try:
                if mutation_target == "snapshot" and os.name != "nt":
                    os.chmod(target, 0o600)
                target.write_bytes(b"mutated-wheel")
            except OSError:
                mutation_blocked = True
    except ValueError as exc:
        security_error = exc
    finally:
        if wheel.exists() and wheel.read_bytes() != benign_bytes:
            wheel.write_bytes(benign_bytes)

    assert evidence is not None
    assert snapshot_path is not None and not snapshot_path.exists()
    assert evidence["cleanup_verified"] is True
    if mutation_blocked:
        assert security_error is None
        assert os.name == "nt"
    else:
        assert security_error is not None
        assert mutation_target in str(security_error)
        assert evidence[f"{mutation_target}_verified_after_launch"] is False


def test_mcp_call_runner_instance_bound_cleanup_preserves_substitute(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.3.1-cleanup-race.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        project={
            "pyproject.toml": b"[project]\nname='spack-mcp'\n",
            "uv.lock": b"version = 1\n",
        },
    )
    server_args = ["--from", str(wheel), "clio-kit", "mcp-server", "spack"]
    artifact = cast(Any, runner)._server_artifact_identity(str(uvx), server_args)
    substitute = tmp_path / "cleanup-substitute"
    substitute.mkdir()
    sentinel = substitute / "must-survive.txt"
    sentinel.write_text("substitute", encoding="utf-8")
    displaced = tmp_path / "displaced-original-snapshot"
    private_root: Path | None = None
    evidence: dict[str, object] | None = None
    cleanup_error: ValueError | None = None
    windows_replacement_blocked = False
    raced = False

    if os.name != "nt":
        original_rmdir = cast(Any, runner).os.rmdir

        def substitute_immediately_before_rmdir(
            name: str,
            *,
            dir_fd: int | None = None,
        ) -> None:
            nonlocal raced
            if not raced and private_root is not None and name == private_root.name:
                private_root.replace(displaced)
                substitute.replace(private_root)
                raced = True
            original_rmdir(name, dir_fd=dir_fd)

        monkeypatch.setattr(cast(Any, runner).os, "rmdir", substitute_immediately_before_rmdir)

    try:
        with cast(Any, runner)._prepared_mcp_launch(
            [str(uvx), *server_args],
            server_args=server_args,
            server_artifact=artifact,
        ) as prepared:
            launch_command, evidence = cast(
                tuple[list[str], dict[str, object]],
                prepared,
            )
            snapshot = Path(launch_command[launch_command.index("--from") + 1])
            private_root = snapshot.parent
            if os.name == "nt":
                try:
                    private_root.replace(displaced)
                except OSError:
                    windows_replacement_blocked = True
                else:
                    substitute.replace(private_root)
                    raced = True
    except ValueError as exc:
        cleanup_error = exc

    assert evidence is not None
    if os.name == "nt" and windows_replacement_blocked:
        assert cleanup_error is None
        assert evidence["cleanup_verified"] is True
        assert private_root is not None and not private_root.exists()
        assert sentinel.read_text(encoding="utf-8") == "substitute"
    else:
        assert raced is True
        assert cleanup_error is not None
        assert evidence["cleanup_verified"] is False
        assert private_root is not None
        surviving_sentinel = private_root / sentinel.name
        assert surviving_sentinel.read_text(encoding="utf-8") == "substitute"
        if displaced.exists():
            assert list(displaced.iterdir()) == []
    for residual in (private_root, displaced, substitute):
        if residual.exists():
            shutil.rmtree(residual)


def test_mcp_call_runner_held_file_proof_rejects_intercepted_unlink(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"pinned-uv-launcher")
    uv.write_bytes(b"pinned-uv-runtime")
    wheel = tmp_path / "clio_kit-2.3.1-unlink-race.whl"
    _write_synthetic_clio_kit_wheel(
        wheel,
        project={
            "pyproject.toml": b"[project]\nname='spack-mcp'\n",
            "uv.lock": b"version = 1\n",
        },
    )
    server_args = ["--from", str(wheel), "clio-kit", "mcp-server", "spack"]
    artifact = cast(Any, runner)._server_artifact_identity(str(uvx), server_args)
    displaced_snapshot = tmp_path / "verified-snapshot-survivor.whl"
    substitute = tmp_path / "unlink-substitute.whl"
    substitute_bytes = b"substitute-must-not-produce-cleanup-success"
    substitute.write_bytes(substitute_bytes)
    snapshot_path: Path | None = None
    private_root: Path | None = None
    evidence: dict[str, object] | None = None
    cleanup_error: ValueError | None = None
    replacement_blocked = False
    raced = False

    if os.name != "nt":
        original_unlink = cast(Any, runner).os.unlink

        def swap_immediately_before_unlink(
            name: object,
            *,
            dir_fd: int | None = None,
        ) -> None:
            nonlocal raced
            if (
                not raced
                and snapshot_path is not None
                and name == snapshot_path.name
                and dir_fd is not None
            ):
                snapshot_path.replace(displaced_snapshot)
                substitute.replace(snapshot_path)
                raced = True
            original_unlink(name, dir_fd=dir_fd)

        monkeypatch.setattr(cast(Any, runner).os, "unlink", swap_immediately_before_unlink)

    try:
        with cast(Any, runner)._prepared_mcp_launch(
            [str(uvx), *server_args],
            server_args=server_args,
            server_artifact=artifact,
        ) as prepared:
            launch_command, evidence = cast(
                tuple[list[str], dict[str, object]],
                prepared,
            )
            snapshot_path = Path(launch_command[launch_command.index("--from") + 1])
            private_root = snapshot_path.parent
            if os.name == "nt":
                try:
                    snapshot_path.replace(displaced_snapshot)
                except OSError:
                    replacement_blocked = True
    except ValueError as exc:
        cleanup_error = exc

    assert evidence is not None
    assert private_root is not None
    if os.name == "nt":
        assert replacement_blocked is True
        assert cleanup_error is None
        assert evidence["cleanup_verified"] is True
        assert not private_root.exists()
        assert substitute.read_bytes() == substitute_bytes
    else:
        assert raced is True
        assert cleanup_error is not None
        assert "held file remained linked" in str(cleanup_error)
        assert evidence["cleanup_verified"] is False
        assert displaced_snapshot.is_file()
        assert displaced_snapshot.read_bytes() == wheel.read_bytes()
        assert private_root.is_dir()
        assert list(private_root.iterdir()) == []
    for residual in (private_root, displaced_snapshot, substitute):
        if residual.exists():
            if residual.is_dir():
                shutil.rmtree(residual)
            else:
                residual.unlink()


def test_mcp_call_runner_streams_large_artifact_hashes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    artifact = tmp_path / "large-server-artifact.whl"
    chunk = b"artifact-bytes" * 8192
    digest = hashlib.sha256()
    with artifact.open("wb") as stream:
        for _ in range(96):
            stream.write(chunk)
            digest.update(chunk)

    def fail_full_file_read(_path: Path) -> bytes:
        raise AssertionError("artifact hashing must not use Path.read_bytes")

    monkeypatch.setattr(Path, "read_bytes", fail_full_file_read)

    identity = cast(Any, runner)._file_identity(artifact)

    assert identity is not None
    assert identity["size_bytes"] == artifact.stat().st_size
    assert identity["sha256"] == digest.hexdigest()
    assert cast(Any, runner).FILE_HASH_CHUNK_BYTES <= 1024 * 1024


def test_mcp_call_runner_records_protocol_errors(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
        env_from: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del tool, arguments, timeout, env_from
        stdout = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "clio-relay-mcp-call",
                "error": {"code": -32603, "message": "tool failed"},
            }
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect"}
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert result["returncode"] == 1
    assert "tool failed" in result["protocol_error"]


def test_mcp_call_runner_writes_result_on_timeout(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
        env_from: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del tool, arguments, env_from
        raise subprocess.TimeoutExpired(command, timeout=1, output="partial", stderr="late")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect", "timeout_seconds": 1}
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 124
    assert result["returncode"] == 124
    assert result["timed_out"] is True
    assert result["stdout"] == "partial"
    assert result["stderr"] == "late"


def test_mcp_call_runner_classifies_early_server_exit_as_protocol_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    server = tmp_path / "crashing_mcp_server.py"
    server.write_text(
        "import sys\n"
        "sys.stderr.write('startup exploded\\n')\n"
        "sys.stderr.flush()\n"
        "raise SystemExit(7)\n",
        encoding="utf-8",
    )

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "tool": "inspect",
            "timeout_seconds": 10,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert result["timed_out"] is False
    assert "stdout closed before response" in result["protocol_error"]
    assert "return code 7" in result["protocol_error"]
    assert "startup exploded" in result["stderr"]


def test_mcp_call_runner_scrubs_progress_env_from_server(
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_FILE", "forbidden")
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_TOKEN", "forbidden-token")
    monkeypatch.setenv("CLIO_RELAY_RUNTIME_METADATA_FILE", "forbidden-runtime")
    monkeypatch.setenv("CLIO_RELAY_RUNTIME_METADATA_TOKEN", "forbidden-runtime-token")
    monkeypatch.setenv("UNDECLARED_APPLICATION_SECRET", "not-forwarded")
    monkeypatch.setenv("REGISTERED_APPLICATION_SECRET", "forwarded-value")

    scrubbed = cast(Any, runner)._scrubbed_env()
    explicit = cast(Any, runner)._child_env({"SCIENCE_API_KEY": "REGISTERED_APPLICATION_SECRET"})

    assert "CLIO_RELAY_PROGRESS_FILE" not in scrubbed
    assert "CLIO_RELAY_PROGRESS_TOKEN" not in scrubbed
    assert "CLIO_RELAY_RUNTIME_METADATA_FILE" not in scrubbed
    assert "CLIO_RELAY_RUNTIME_METADATA_TOKEN" not in scrubbed
    assert "UNDECLARED_APPLICATION_SECRET" not in scrubbed
    assert explicit["SCIENCE_API_KEY"] == "forwarded-value"
    assert "REGISTERED_APPLICATION_SECRET" not in explicit


def test_mcp_call_runner_rejects_relay_credential_references() -> None:
    runner = _load_runner()

    with raises(ValueError, match="cannot expose relay credential"):
        cast(Any, runner)._environment_references({"REMOTE_TOKEN": "CLIO_RELAY_API_TOKEN"})


def test_mcp_call_result_persists_environment_references_not_values(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SITE_SCIENCE_TOKEN", "sensitive-value-not-for-artifact")
    captured: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
        env_from: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        del tool, arguments, timeout
        captured.update(env_from)
        stdout = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-init", "result": {}}),
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-call", "result": {}}),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": "science-mcp",
            "tool": "inspect",
            "env_from": {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"},
        }
    )
    rendered = (tmp_path / "mcp-result.json").read_text(encoding="utf-8")
    result = json.loads(rendered)

    assert return_code == 0
    assert captured == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    assert result["env_from"] == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    assert "sensitive-value-not-for-artifact" not in rendered


def test_mcp_call_runner_paginates_and_deduplicates_tools(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    server = _write_tools_list_server(
        tmp_path,
        """
cursor = message.get("params", {}).get("cursor")
if cursor is None:
    result = {"tools": [tool("alpha"), tool("shared")], "nextCursor": "page-2"}
elif cursor == "page-2":
    result = {"tools": [tool("shared"), tool("omega")]}
else:
    raise RuntimeError(f"unexpected cursor: {cursor}")
""",
    )

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "operation": "tools/list",
            "timeout_seconds": 10,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert [tool["name"] for tool in result["protocol_result"]["tools"]] == [
        "alpha",
        "shared",
        "omega",
    ]
    assert result["pagination"] == {
        "pages": 2,
        "tools": 3,
        "response_bytes": result["pagination"]["response_bytes"],
        "limits": {
            "max_pages": 64,
            "max_tools": 10_000,
            "max_response_bytes": 16 * 1024 * 1024,
        },
    }
    assert result["pagination"]["response_bytes"] > 0


def test_mcp_call_runner_rejects_repeated_pagination_cursor(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    server = _write_tools_list_server(
        tmp_path,
        'result = {"tools": [tool("alpha")], "nextCursor": "same"}',
    )

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "operation": "tools/list",
            "timeout_seconds": 10,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert "repeated nextCursor" in result["protocol_error"]


def test_mcp_call_runner_enforces_tools_list_page_limit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cast(Any, runner), "TOOLS_LIST_MAX_PAGES", 1)
    server = _write_tools_list_server(
        tmp_path,
        'result = {"tools": [tool("alpha")], "nextCursor": "more"}',
    )

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "operation": "tools/list",
            "timeout_seconds": 10,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert "maximum page count 1" in result["protocol_error"]


def test_mcp_call_runner_enforces_tools_list_tool_limit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cast(Any, runner), "TOOLS_LIST_MAX_TOOLS", 1)
    server = _write_tools_list_server(
        tmp_path,
        'result = {"tools": [tool("alpha"), tool("omega")]}',
    )

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "operation": "tools/list",
            "timeout_seconds": 10,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert "maximum tool count 1" in result["protocol_error"]


def test_mcp_call_runner_enforces_tools_list_byte_limit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cast(Any, runner), "TOOLS_LIST_MAX_RESPONSE_BYTES", 128)
    server = _write_tools_list_server(
        tmp_path,
        'result = {"tools": [tool("alpha")], "padding": "x" * 512}',
    )

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "operation": "tools/list",
            "timeout_seconds": 10,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert "maximum response size 128 bytes" in result["protocol_error"]


def test_mcp_call_runner_enforces_tools_call_byte_limit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cast(Any, runner), "MCP_CALL_MAX_RESPONSE_BYTES", 256)
    server = _write_tools_call_server(tmp_path, padding_bytes=1024)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "tool": "inspect",
            "timeout_seconds": 10,
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert "tools/call exceeded maximum response size 256 bytes" in result["protocol_error"]


def test_mcp_call_runner_kills_server_immediately_after_protocol_limit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cast(Any, runner), "MCP_CALL_MAX_RESPONSE_BYTES", 256)
    server = tmp_path / "oversized_then_sleeping_server.py"
    server.write_text(
        """import json
import sys
import time

for line in sys.stdin:
    message = json.loads(line)
    if message.get("method") == "initialize":
        result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}}
        print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result}), flush=True)
    elif message.get("method") == "tools/call":
        result = {"content": [{"type": "text", "text": "x" * 4096}]}
        print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result}), flush=True)
        time.sleep(30)
""",
        encoding="utf-8",
    )

    server_artifact = cast(Any, runner)._server_artifact_identity(
        sys.executable,
        [str(server)],
    )

    def preflighted_server_artifact(
        _server: str,
        _server_args: list[str],
    ) -> dict[str, Any]:
        return deepcopy(server_artifact)

    monkeypatch.setattr(
        cast(Any, runner),
        "_server_artifact_identity",
        preflighted_server_artifact,
    )
    started = time.monotonic()
    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "tool": "inspect",
            "timeout_seconds": 20,
        }
    )
    elapsed = time.monotonic() - started
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert elapsed < 10
    assert "tools/call exceeded maximum response size 256 bytes" in result["protocol_error"]


def test_mcp_call_runner_bridges_progress_before_call_returns(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()

    def fake_install(_process: subprocess.Popen[str]) -> dict[int, object]:
        return {}

    def fake_restore(_handlers: dict[int, object]) -> None:
        return None

    monkeypatch.setattr(cast(Any, runner), "_install_parent_termination_handlers", fake_install)
    monkeypatch.setattr(cast(Any, runner), "_restore_parent_termination_handlers", fake_restore)
    sidecar = tmp_path / "relay-progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer-relay-token",
        expected_server_artifact_digest="a" * 64,
        observed_server_artifact_digest="a" * 64,
        expected_pipeline_id="pipeline-a",
    )
    server = tmp_path / "live_progress_server.py"
    server.write_text(
        """import json
import sys
import time

def envelope(sequence, current, accepted):
    execution_id = "jarvis_execution_a"
    provider = {
        "entry_point": "lammps",
        "entry_point_value": "jarvis_cd.progress.lammps:adapter_from_package",
        "distribution": "jarvis_cd",
        "distribution_version": "1.2.2",
        "adapter": "lammps",
        "package_name": "builtin.lammps",
        "package_version": "1.2.2",
        "application_profile": "jarvis-cd.builtin.lammps",
    }
    record = {
        "label": "timestep",
        "current": current,
        "total": 10.0,
        "unit": "step",
        "message": f"step {current}",
        "metadata": {
            "adapter": "lammps",
            "package_name": "builtin.lammps",
            "package_version": "1.2.2",
            "run_id": execution_id,
            "execution_id": execution_id,
            "prediction_status": (
                "observed_lammps_timing" if accepted else "warming_up"
            ),
            "timing_source": "lammps_thermo_cpu" if accepted else None,
            "absolute_step": current,
            "eta_seconds": 1.0 if accepted else None,
        },
    }
    return {
        "schema_version": "clio-kit.jarvis-package-progress.v1",
        "execution_id": execution_id,
        "pipeline_id": "pipeline-a",
        "notification_sequence": sequence,
        "source_authority": "package_log",
        "provider": provider,
        "provider_acceptance_validated": accepted,
        "record": record,
    }

for line in sys.stdin:
    request = json.loads(line)
    if request.get("method") == "initialize":
        result = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}}
        print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
    elif request.get("method") == "tools/call":
        token = request["params"]["_meta"]["progressToken"]
        for sequence, current, accepted in ((1, 1.0, False), (2, 5.0, True)):
            payload = envelope(sequence, current, accepted)
            params = {
                "progressToken": token,
                "progress": current,
                "total": 10.0,
                "message": json.dumps(payload, separators=(",", ":"), sort_keys=True),
            }
            notification = {
                "jsonrpc": "2.0",
                "method": "notifications/progress",
                "params": params,
            }
            print(json.dumps(notification), flush=True)
            time.sleep(0.75)
        structured = {
            "pipeline_id": "pipeline-a",
            "runtime_metadata": {
                "schema_version": "jarvis.runtime.v1",
                "execution_id": "jarvis_execution_a",
                "pipeline_id": "pipeline-a",
                "package_provenance": [{"pkg_type": "builtin.lammps"}],
            },
        }
        result = {"structuredContent": structured, "content": []}
        print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
        break
""",
        encoding="utf-8",
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            cast(Any, runner)._run_mcp_session,
            [sys.executable, str(server)],
            tool="jarvis_run",
            arguments={"pipeline_id": "pipeline-a"},
            timeout=10,
            progress_bridge=bridge,
        )
        deadline = time.monotonic() + 5
        while sidecar.stat().st_size == 0 and not future.done() and time.monotonic() < deadline:
            time.sleep(0.02)
        if future.done():
            future.result()
        assert sidecar.stat().st_size > 0
        assert not future.done()
        early = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
        assert (
            early[0]["progress"]["metadata"]["mcp_progress_bridge"]["execution_validated"] is False
        )
        process = future.result(timeout=10)

    protocol_result = cast(Any, runner)._response_result(
        process.stdout,
        response_id="clio-relay-mcp-call",
    )
    structured_result = cast(Any, runner)._structured_result(
        protocol_result,
        operation="tools/call",
    )
    bridge.finalize(structured_result)
    records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]

    assert len(records) == 3
    assert "outer-relay-token" not in sidecar.read_text(encoding="utf-8")
    assert records[-1]["progress"]["metadata"]["mcp_progress_bridge"]["execution_validated"] is True
    for sequence, record in enumerate(records, start=1):
        assert record["sequence"] == sequence
        signed = {key: record[key] for key in ("schema_version", "sequence", "progress")}
        canonical = json.dumps(
            signed,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        assert hmac.compare_digest(
            record["progress_hmac"],
            hmac.new(b"outer-relay-token", canonical, hashlib.sha256).hexdigest(),
        )
    assert bridge.result_metadata()["execution_validated"] is True


def test_mcp_call_runner_rejects_unmatched_progress_token(tmp_path: Path) -> None:
    runner = _load_runner()
    bridge = cast(Any, runner)._McpProgressBridge(
        path=tmp_path / "progress.jsonl",
        relay_token="outer",
        expected_server_artifact_digest="a" * 64,
        observed_server_artifact_digest="a" * 64,
        expected_pipeline_id="pipeline-a",
    )

    with raises(RuntimeError, match="token did not match"):
        bridge.observe(
            {
                "jsonrpc": "2.0",
                "method": "notifications/progress",
                "params": {
                    "progressToken": "attacker-token",
                    "progress": 1,
                    "message": "{}",
                },
            }
        )
    assert not (tmp_path / "progress.jsonl").exists()


def test_mcp_call_runner_does_not_unlock_progress_for_unverified_server(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_FILE", str(tmp_path / "progress.jsonl"))
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_TOKEN", "outer")

    bridge = cast(Any, runner)._package_progress_bridge_from_invocation(
        operation="tools/call",
        tool="jarvis_run",
        arguments={"pipeline_id": "pipeline-a"},
        expected_server_artifact_digest="a" * 64,
        expected_jarvis_cd_lock_binding=_jarvis_cd_lock_expectation(),
        observed_server_artifact_digest="a" * 64,
        server_artifact={
            "verified": False,
            "nested_runtime": {
                "server_name": "jarvis",
                "locked_runtime_verified": False,
            },
        },
    )

    assert bridge is None


def test_registered_jarvis_run_does_not_enable_builtin_progress_bridge(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Keep built-in package progress semantics out of operator JARVIS servers."""
    runner = _load_runner()
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_FILE", str(tmp_path / "progress.jsonl"))
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_TOKEN", "outer")

    bridge = cast(Any, runner)._package_progress_bridge_from_invocation(
        operation="tools/call",
        tool="jarvis_run",
        arguments={"pipeline_id": "operator-pipeline"},
        expected_server_artifact_digest="a" * 64,
        expected_jarvis_cd_lock_binding=None,
        observed_server_artifact_digest="a" * 64,
        server_artifact=_verified_jarvis_server_artifact(),
    )

    assert bridge is None


def test_mcp_call_runner_rejects_final_execution_mismatch(tmp_path: Path) -> None:
    runner = _load_runner()
    _precreate_progress_sidecar(tmp_path / "progress.jsonl")
    bridge = cast(Any, runner)._McpProgressBridge(
        path=tmp_path / "progress.jsonl",
        relay_token="outer",
        expected_server_artifact_digest="a" * 64,
        observed_server_artifact_digest="a" * 64,
        expected_pipeline_id="pipeline-a",
    )
    envelope = _package_progress_envelope(sequence=1, accepted=True)
    bridge.observe(
        {
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 5.0,
                "total": 10.0,
                "message": json.dumps(envelope, separators=(",", ":"), sort_keys=True),
            },
        }
    )

    with raises(RuntimeError, match="execution id did not match"):
        bridge.finalize(
            {
                "runtime_metadata": {
                    "schema_version": "jarvis.runtime.v1",
                    "execution_id": "different-execution",
                    "pipeline_id": "pipeline-a",
                    "package_provenance": [{"pkg_type": "builtin.lammps"}],
                }
            }
        )
    records = (tmp_path / "progress.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(records) == 1
    assert bridge.result_metadata()["execution_validated"] is False


def test_mcp_call_runner_bridges_indeterminate_native_progress_by_transport_sequence(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="c" * 64,
        observed_server_artifact_digest="c" * 64,
        expected_pipeline_id="pipeline-a",
    )
    snapshot = _native_progress_snapshot(state="running", terminal=False)

    bridge.observe(
        {
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 1,
                "total": 999,
                "message": json.dumps(snapshot, separators=(",", ":"), sort_keys=True),
            }
        }
    )
    early = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]

    assert len(early) == 1
    assert "current" not in early[0]["progress"]
    assert "total" not in early[0]["progress"]
    bridge_metadata = early[0]["progress"]["metadata"]["mcp_native_progress_bridge"]
    assert bridge_metadata["transport_sequence"] == 1
    assert bridge_metadata["determinate"] is False
    assert bridge_metadata["execution_validated"] is False

    documents = _native_execution_documents(state="completed", terminal=True)
    bridge.finalize(documents)
    records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]

    assert len(records) == 2
    final_bridge = records[-1]["progress"]["metadata"]["mcp_native_progress_bridge"]
    assert final_bridge["execution_validated"] is True
    assert bridge.result_metadata()["schema_version"] == (
        "clio-relay.mcp-jarvis-progress-bridge.v1"
    )
    assert bridge.result_metadata()["execution_id"] == "native-execution"


def test_mcp_call_runner_does_not_compare_native_transport_sequence_to_workload_current(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="d" * 64,
        observed_server_artifact_digest="d" * 64,
        expected_pipeline_id="pipeline-a",
    )
    snapshot = _native_progress_snapshot(state="running", terminal=False)
    latest = cast(dict[str, Any], cast(list[dict[str, Any]], snapshot["packages"])[0]["latest"])
    latest.update({"current": 5.0, "total": 10.0, "determinate": True})

    bridge.observe(
        {
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 1,
                "message": json.dumps(snapshot, separators=(",", ":"), sort_keys=True),
            }
        }
    )
    record = json.loads(sidecar.read_text(encoding="utf-8"))

    assert record["progress"]["current"] == 5.0
    assert record["progress"]["total"] == 10.0
    assert record["progress"]["metadata"]["mcp_native_progress_bridge"]["transport_sequence"] == 1


def test_mcp_call_runner_accepts_explicitly_null_native_optional_progress_fields(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="d" * 64,
        observed_server_artifact_digest="d" * 64,
        expected_pipeline_id="pipeline-a",
    )
    snapshot = _native_progress_snapshot(state="running", terminal=False)
    latest = cast(dict[str, Any], cast(list[dict[str, Any]], snapshot["packages"])[0]["latest"])
    latest.update({"current": None, "total": None, "unit": None, "message": None})

    bridge.observe(
        {
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 1,
                "message": json.dumps(snapshot, separators=(",", ":"), sort_keys=True),
            }
        }
    )
    record = json.loads(sidecar.read_text(encoding="utf-8"))["progress"]

    assert record["current"] is None
    assert record["total"] is None
    assert record["unit"] is None
    assert record["metadata"]["mcp_native_progress_bridge"]["determinate"] is False


def test_mcp_call_runner_rejects_native_package_identity_drift(tmp_path: Path) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="d" * 64,
        observed_server_artifact_digest="d" * 64,
        expected_pipeline_id="pipeline-a",
    )
    first = _native_progress_snapshot(state="running", terminal=False)
    bridge.observe(
        {
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 1,
                "message": json.dumps(first),
            }
        }
    )
    second = _native_progress_snapshot(state="running", terminal=False)
    package = cast(list[dict[str, Any]], second["packages"])[0]
    latest = cast(dict[str, Any], package["latest"])
    package.update({"package_name": "different.package", "event_count": 2})
    latest.update({"package_name": "different.package", "sequence": 1})

    with raises(RuntimeError, match="package progress name changed"):
        bridge.observe(
            {
                "params": {
                    "progressToken": bridge.progress_token,
                    "progress": 2,
                    "message": json.dumps(second),
                }
            }
        )


def test_mcp_call_runner_persists_final_native_snapshot_without_notifications(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="e" * 64,
        observed_server_artifact_digest="e" * 64,
        expected_pipeline_id="pipeline-a",
    )

    bridge.finalize(_native_execution_documents(state="completed", terminal=True))
    records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]

    assert len(records) == 1
    assert (
        records[0]["progress"]["metadata"]["mcp_native_progress_bridge"]["execution_validated"]
        is True
    )
    assert bridge.result_metadata()["notification_count"] == 0


def test_mcp_call_runner_rejects_native_final_identity_mismatch(tmp_path: Path) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="f" * 64,
        observed_server_artifact_digest="f" * 64,
        expected_pipeline_id="pipeline-a",
    )
    snapshot = _native_progress_snapshot(state="running", terminal=False)
    bridge.observe(
        {
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 1,
                "message": json.dumps(snapshot),
            }
        }
    )
    documents = _native_execution_documents(state="completed", terminal=True)
    handle = cast(dict[str, Any], documents["execution_handle"])
    handle["execution_id"] = "different-execution"

    with raises(RuntimeError, match="handle and record identities did not match"):
        bridge.finalize(documents)


def test_mcp_call_runner_rejects_compatibility_progress_with_native_final_result(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="f" * 64,
        observed_server_artifact_digest="f" * 64,
        expected_pipeline_id="pipeline-a",
    )
    compatibility = _package_progress_envelope(sequence=1, accepted=True)
    bridge.observe(
        {
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 5,
                "total": 10,
                "message": json.dumps(compatibility),
            }
        }
    )

    with raises(RuntimeError, match="changed to native execution documents"):
        bridge.finalize(_native_execution_documents(state="completed", terminal=True))


def test_mcp_call_runner_rejects_rewritten_native_event_in_final_result(
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_progress_sidecar(sidecar)
    bridge = cast(Any, runner)._McpProgressBridge(
        path=sidecar,
        relay_token="outer",
        expected_server_artifact_digest="f" * 64,
        observed_server_artifact_digest="f" * 64,
        expected_pipeline_id="pipeline-a",
    )
    snapshot = _native_progress_snapshot(state="running", terminal=False)
    bridge.observe(
        {
            "params": {
                "progressToken": bridge.progress_token,
                "progress": 1,
                "message": json.dumps(snapshot),
            }
        }
    )
    documents = _native_execution_documents(state="completed", terminal=True)
    progress = cast(dict[str, Any], documents["progress"])
    package = cast(list[dict[str, Any]], progress["packages"])[0]
    cast(dict[str, Any], package["latest"])["message"] = "rewritten"

    with raises(RuntimeError, match="changed an existing package event"):
        bridge.finalize(documents)


def test_mcp_call_runner_validates_unified_execution_progress_and_artifact_page() -> None:
    runner = _load_runner()
    result = _jarvis_execution_query_result(include_progress=True, include_artifacts=True)
    arguments = {
        "pipeline_id": "pipeline-a",
        "execution_id": "native-execution",
        "include_progress": True,
        "artifacts": {
            "package_id": "gray-scott",
            "role": "output",
            "state": "finalized",
            "page_size": 50,
            "cursor": "opaque_cursor_1",
        },
    }

    validation = cast(Any, runner)._validated_jarvis_execution_query_result(
        result,
        arguments=arguments,
    )

    assert validation == {
        "schema_version": "clio-relay.jarvis-execution-query-validation.v1",
        "pipeline_id": "pipeline-a",
        "execution_id": "native-execution",
        "include_progress": True,
        "progress_included": True,
        "include_service_runtimes": False,
        "service_runtimes_included": False,
        "service_runtime_count": 0,
        "artifacts_requested": True,
        "artifact_filters": {
            "package_id": "gray-scott",
            "role": "output",
            "state": "finalized",
            "artifact_id": None,
            "page_size": 50,
            "cursor": "opaque_cursor_1",
        },
        "returned_artifact_count": 1,
        "next_cursor_present": False,
    }


def test_mcp_call_runner_persists_query_validation_in_durable_result(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    server = _write_structured_tools_call_server(
        tmp_path,
        _jarvis_execution_query_result(include_progress=True, include_artifacts=True),
    )
    artifact = _verified_jarvis_server_artifact()

    def server_identity(
        _server: str,
        _args: list[str],
        **_kwargs: object,
    ) -> dict[str, Any]:
        return artifact

    def server_digest(_artifact: dict[str, Any]) -> str:
        return "a" * 64

    monkeypatch.setattr(runner, "_server_artifact_identity", server_identity)
    monkeypatch.setattr(runner, "_server_artifact_digest", server_digest)

    returncode = cast(Any, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "operation": "tools/call",
            "tool": "jarvis_get_execution",
            "arguments": {
                "pipeline_id": "pipeline-a",
                "execution_id": "native-execution",
                "artifacts": {"role": "output"},
            },
            "expected_server_artifact_digest": "a" * 64,
            "expected_jarvis_cd_lock_binding": _jarvis_cd_lock_expectation(),
        }
    )

    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))
    assert returncode == 0
    assert result["returncode"] == 0
    assert result["protocol_error"] is None
    assert result["result_validation"]["schema_version"] == (
        "clio-relay.jarvis-execution-query-validation.v1"
    )
    assert result["result_validation"]["returned_artifact_count"] == 1


def test_registered_jarvis_execution_query_keeps_operator_result_schema(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Do not impose the relay's built-in result schema on registered JARVIS MCPs."""
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    server = _write_structured_tools_call_server(
        tmp_path,
        {"operator_contract": "custom", "status": "ready"},
    )
    artifact = _verified_jarvis_server_artifact()
    monkeypatch.setattr(runner, "_server_artifact_identity", lambda *_args: artifact)
    monkeypatch.setattr(runner, "_server_artifact_digest", lambda _artifact: "a" * 64)

    returncode = cast(Any, runner).run_mcp_call_from_params(
        {
            "server": sys.executable,
            "server_args": [str(server)],
            "operation": "tools/call",
            "tool": "jarvis_get_execution",
            "arguments": {"operator_argument": True},
            "expected_server_artifact_digest": "a" * 64,
        }
    )

    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))
    assert returncode == 0
    assert result["structured_result"] == {
        "operator_contract": "custom",
        "status": "ready",
    }
    assert result["result_validation"] is None


def test_mcp_call_runner_accepts_explicit_execution_query_opt_outs() -> None:
    runner = _load_runner()
    result = _jarvis_execution_query_result(include_progress=False, include_artifacts=False)

    validation = cast(Any, runner)._validated_jarvis_execution_query_result(
        result,
        arguments={
            "pipeline_id": "pipeline-a",
            "execution_id": "native-execution",
            "include_progress": False,
        },
    )

    assert validation["progress_included"] is False
    assert validation["artifacts_requested"] is False
    assert validation["returned_artifact_count"] == 0


def test_mcp_call_runner_validates_requested_service_runtime_envelope() -> None:
    runner = _load_runner()
    result = _jarvis_execution_query_result(
        include_progress=False,
        include_artifacts=False,
        include_services=True,
    )

    validation = cast(Any, runner)._validated_jarvis_execution_query_result(
        result,
        arguments={
            "pipeline_id": "pipeline-a",
            "execution_id": "native-execution",
            "include_progress": False,
            "include_service_runtimes": True,
        },
    )

    assert validation["include_service_runtimes"] is True
    assert validation["service_runtimes_included"] is True
    assert validation["service_runtime_count"] == 0


@mark.parametrize(
    ("mutation", "message"),
    [
        ("progress_lifecycle", "progress lifecycle did not match"),
        ("artifact_lifecycle", "artifact page lifecycle did not match"),
        ("artifact_execution", "artifact entry schema or identity was invalid"),
        ("artifact_filter", "did not satisfy the role filter"),
        ("artifact_count", "artifact page counts did not match"),
        ("artifact_cursor", "artifact next_cursor was invalid"),
    ],
)
def test_mcp_call_runner_rejects_incoherent_execution_query_results(
    mutation: str,
    message: str,
) -> None:
    runner = _load_runner()
    result = _jarvis_execution_query_result(include_progress=True, include_artifacts=True)
    progress = cast(dict[str, Any], result["progress"])
    page = cast(dict[str, Any], result["artifact_page"])
    artifact = cast(list[dict[str, Any]], page["artifacts"])[0]
    if mutation == "progress_lifecycle":
        progress["execution_state"] = "running"
        progress["terminal"] = False
    elif mutation == "artifact_lifecycle":
        page["terminal"] = False
    elif mutation == "artifact_execution":
        artifact["execution_id"] = "different-execution"
    elif mutation == "artifact_filter":
        artifact["role"] = "log"
    elif mutation == "artifact_count":
        page["returned_artifact_count"] = 2
    else:
        page["next_cursor"] = "not a valid cursor"

    with raises(RuntimeError, match=message):
        cast(Any, runner)._validated_jarvis_execution_query_result(
            result,
            arguments={
                "pipeline_id": "pipeline-a",
                "execution_id": "native-execution",
                "artifacts": {"role": "output"},
            },
        )


def test_mcp_call_runner_rejects_unrequested_execution_query_payloads() -> None:
    runner = _load_runner()
    result = _jarvis_execution_query_result(include_progress=False, include_artifacts=False)
    result["progress"] = _native_progress_snapshot(state="completed", terminal=True)

    with raises(RuntimeError, match="returned progress after it was omitted"):
        cast(Any, runner)._validated_jarvis_execution_query_result(
            result,
            arguments={
                "pipeline_id": "pipeline-a",
                "execution_id": "native-execution",
                "include_progress": False,
            },
        )

    result = _jarvis_execution_query_result(include_progress=False, include_artifacts=True)
    with raises(RuntimeError, match="returned artifacts without an artifact request"):
        cast(Any, runner)._validated_jarvis_execution_query_result(
            result,
            arguments={
                "pipeline_id": "pipeline-a",
                "execution_id": "native-execution",
                "include_progress": False,
            },
        )


def _precreate_progress_sidecar(path: Path) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
    finally:
        os.close(descriptor)


def _package_progress_envelope(*, sequence: int, accepted: bool) -> dict[str, object]:
    execution_id = "jarvis_execution_a"
    return {
        "schema_version": "clio-kit.jarvis-package-progress.v1",
        "execution_id": execution_id,
        "pipeline_id": "pipeline-a",
        "notification_sequence": sequence,
        "source_authority": "package_log",
        "provider": {
            "entry_point": "lammps",
            "entry_point_value": "jarvis_cd.progress.lammps:adapter_from_package",
            "distribution": "jarvis_cd",
            "distribution_version": "1.2.2",
            "adapter": "lammps",
            "package_name": "builtin.lammps",
            "package_version": "1.2.2",
            "application_profile": "jarvis-cd.builtin.lammps",
        },
        "provider_acceptance_validated": accepted,
        "record": {
            "label": "timestep",
            "current": 5.0,
            "total": 10.0,
            "unit": "step",
            "message": "step 5",
            "metadata": {
                "adapter": "lammps",
                "package_name": "builtin.lammps",
                "package_version": "1.2.2",
                "run_id": execution_id,
                "execution_id": execution_id,
                "prediction_status": "observed_lammps_timing",
                "timing_source": "lammps_thermo_cpu",
                "absolute_step": 5.0,
                "eta_seconds": 1.0,
            },
        },
    }


def _native_progress_snapshot(*, state: str, terminal: bool) -> dict[str, object]:
    return {
        "schema_version": "jarvis.execution.progress.v1",
        "execution_id": "native-execution",
        "pipeline_id": "pipeline-a",
        "execution_state": state,
        "terminal": terminal,
        "packages": [
            {
                "package_id": "render",
                "package_name": "builtin.paraview",
                "event_count": 1,
                "latest": {
                    "schema_version": "jarvis.progress.v1",
                    "package_name": "builtin.paraview",
                    "package_id": "render",
                    "execution_id": "native-execution",
                    "label": "server readiness",
                    "state": "ready",
                    "sequence": 0,
                    "observed_at_epoch": 1_789_000_000.0,
                    "determinate": False,
                    "metadata": {"mode": "server"},
                },
            }
        ],
    }


def _native_execution_documents(*, state: str, terminal: bool) -> dict[str, object]:
    handle: dict[str, object] = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": "native-execution",
        "pipeline_id": "pipeline-a",
        "mode": "direct",
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "cluster": None,
    }
    record: dict[str, object] = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": "native-execution",
        "pipeline_id": "pipeline-a",
        "pipeline_name": "pipeline-a",
        "mode": "direct",
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "cluster": None,
        "state": state,
        "submitted": False,
        "terminal": terminal,
        "created_at": "2026-07-12T10:00:00Z",
        "updated_at": "2026-07-12T10:00:01Z",
        "return_code": 0 if state == "completed" else None,
        "error": None,
        "metadata": {},
    }
    return {
        "execution_handle": handle,
        "execution_record": record,
        "progress": _native_progress_snapshot(state=state, terminal=terminal),
    }


def _jarvis_execution_query_result(
    *,
    include_progress: bool,
    include_artifacts: bool,
    include_services: bool = False,
) -> dict[str, Any]:
    documents = deepcopy(_native_execution_documents(state="completed", terminal=True))
    progress = documents.pop("progress") if include_progress else None
    artifact_page: dict[str, Any] | None = None
    if include_artifacts:
        artifact_page = {
            "producer_schema_version": "jarvis.execution.artifacts.v1",
            "pipeline_id": "pipeline-a",
            "execution_id": "native-execution",
            "execution_state": "completed",
            "terminal": True,
            "artifacts": [
                {
                    "schema_version": "jarvis.artifact.v1",
                    "package_name": "builtin.gray_scott",
                    "package_id": "gray-scott",
                    "execution_id": "native-execution",
                    "artifact_id": "art_0000000000000000000001",
                    "logical_name": "gray-scott-timesteps",
                    "kind": "timestep-collection",
                    "role": "output",
                    "structure": "collection",
                    "ownership": "execution",
                    "state": "finalized",
                    "revision": 1,
                    "sequence": 1,
                    "observed_at_epoch": 1_789_000_100.0,
                    "location": {
                        "kind": "execution_path",
                        "value": "outputs/gray-scott.bp",
                    },
                    "media_type": "application/octet-stream",
                    "format": "adios2",
                    "size_bytes": 4096,
                    "checksum": "sha256:0123456789abcdef",
                    "metadata": {"member_count": 10, "latest_timestep": 9},
                }
            ],
            "matching_artifact_count": 1,
            "returned_artifact_count": 1,
            "next_cursor": None,
        }
    return {
        "schema_version": "clio-kit.jarvis-execution.v2",
        "pipeline_id": "pipeline-a",
        "execution_id": "native-execution",
        **documents,
        "runtime_metadata": {"package_provenance": [{"pkg_id": "gray-scott"}]},
        "progress": progress,
        "artifact_page": artifact_page,
        "service_runtimes": (
            {
                "schema_version": "jarvis.execution.service-runtimes.v1",
                "execution_id": "native-execution",
                "pipeline_id": "pipeline-a",
                "execution_state": "completed",
                "terminal": True,
                "service_runtimes": [],
            }
            if include_services
            else None
        ),
    }


def _write_tools_call_server(tmp_path: Path, *, padding_bytes: int) -> Path:
    server_path = tmp_path / "bounded_call_stdio_server.py"
    server_path.write_text(
        f"""import json
import sys

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        result = {{"protocolVersion": "2024-11-05", "capabilities": {{"tools": {{}}}}}}
        print(json.dumps({{"jsonrpc": "2.0", "id": message["id"], "result": result}}), flush=True)
    elif method == "tools/call":
        result = {{"content": [{{"type": "text", "text": "x" * {padding_bytes}}}]}}
        print(json.dumps({{"jsonrpc": "2.0", "id": message["id"], "result": result}}), flush=True)
        break
""",
        encoding="utf-8",
    )
    return server_path


def _write_structured_tools_call_server(
    tmp_path: Path,
    structured: dict[str, Any],
) -> Path:
    server_path = tmp_path / "structured_stdio_server.py"
    encoded = json.dumps(structured, separators=(",", ":"), sort_keys=True)
    server_path.write_text(
        f"""import json
import sys

structured = json.loads({encoded!r})
for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        result = {{"protocolVersion": "2024-11-05", "capabilities": {{"tools": {{}}}}}}
        print(json.dumps({{"jsonrpc": "2.0", "id": message["id"], "result": result}}), flush=True)
    elif method == "tools/call":
        result = {{"structuredContent": structured, "content": []}}
        print(json.dumps({{"jsonrpc": "2.0", "id": message["id"], "result": result}}), flush=True)
        break
""",
        encoding="utf-8",
    )
    return server_path


def _write_tools_list_server(tmp_path: Path, tools_list_body: str) -> Path:
    server_path = tmp_path / "paginated_stdio_server.py"
    source = f"""import json
import sys

def tool(name):
    return {{"name": name, "inputSchema": {{"type": "object", "properties": {{}}}}}}

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        result = {{
            "protocolVersion": "2024-11-05",
            "capabilities": {{"tools": {{}}}},
            "serverInfo": {{"name": "pagination-test", "version": "1.0"}},
        }}
        print(json.dumps({{"jsonrpc": "2.0", "id": message["id"], "result": result}}), flush=True)
    elif method == "tools/list":
{_indent_python(tools_list_body, 8)}
        print(json.dumps({{"jsonrpc": "2.0", "id": message["id"], "result": result}}), flush=True)
"""
    server_path.write_text(source, encoding="utf-8")
    return server_path


def _indent_python(value: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line for line in value.strip().splitlines())


def _locked_clio_kit_v4_launcher_source() -> str:
    return "\n".join(
        [
            'LOCKED_SERVER_LAUNCH_SCHEMA = "clio-kit.locked-server.v4"',
            ('_LOCKED_SERVER_RUNTIME_POLICY = "uv-run:materialized:frozen:no-editable:no-dev:v3"'),
            'FLAGS = ["--no-dev", "--no-editable", "--frozen"]',
            "locked_server_project_identity = object()",
            "materialize_locked_server_project = object()",
            'UV_PROJECT_ENVIRONMENT = "UV_PROJECT_ENVIRONMENT"',
        ]
    )


def _write_synthetic_clio_kit_wheel(
    wheel: Path,
    *,
    project: dict[str, bytes],
    server_name: str = "spack",
    excluded: dict[str, bytes] | None = None,
    outer_members: dict[str, bytes] | None = None,
) -> None:
    prefix = f"clio_kit-2.3.1.data/data/clio-kit-mcp-servers/{server_name}/"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("clio_kit/__init__.py", _locked_clio_kit_v4_launcher_source())
        for relative, content in project.items():
            archive.writestr(prefix + relative, content)
        for relative, content in (excluded or {}).items():
            archive.writestr(prefix + relative, content)
        for name, content in (outer_members or {}).items():
            archive.writestr(name, content)


def _clio_kit_v4_project_sha256(project: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    policy = b"uv-run:materialized:frozen:no-editable:no-dev:v3"
    digest.update(len(policy).to_bytes(8, "big"))
    digest.update(policy)
    digest.update(len(project).to_bytes(8, "big"))
    for relative, content in sorted(project.items()):
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(hashlib.sha256(content).digest())
    return digest.hexdigest()


def _legacy_clio_kit_project_sha256(project: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for relative, content in sorted(project.items()):
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(content)
    return digest.hexdigest()


def _minimal_console_wheel(tmp_path: Path) -> Path:
    wheel = tmp_path / "science_mcp-1.0.0-py3-none-any.whl"
    dist_info = "science_mcp-1.0.0.dist-info"
    members = {
        "science_mcp/__init__.py": b"VALUE = 1\n\ndef main():\n    return None\n",
        f"{dist_info}/METADATA": (b"Metadata-Version: 2.4\nName: science-mcp\nVersion: 1.0.0\n"),
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: clio-relay-test\n"
            b"Root-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        f"{dist_info}/entry_points.txt": (b"[console_scripts]\nscience-mcp = science_mcp:main\n"),
    }
    record_lines: list[str] = []
    for name, content in sorted(members.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).decode().rstrip("=")
        record_lines.append(f"{name},sha256={digest},{len(content)}")
    record_name = f"{dist_info}/RECORD"
    record_lines.append(f"{record_name},,")
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
        archive.writestr(record_name, "\n".join(record_lines) + "\n")
    return wheel


def _load_runner() -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "mcp_call"
        / "runner.py"
    )
    spec = importlib.util.spec_from_file_location("clio_relay_mcp_call_runner", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load MCP call runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
