"""Cross-repository checks for clio-kit's shipped locked-server contracts."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, cast

import pytest

from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_user_contract,
)
from clio_relay.remote_mcp import (
    CLIO_KIT_SPACK_USER_CONTRACT_SHA256,
    CLIO_KIT_SPACK_USER_CONTRACT_VERSION,
    RemoteMcpToolSchema,
    remote_mcp_schema_digest,
)

JSON = dict[str, Any]
CONTRACT_INDEX_PATH = "clio_kit/_mcp_contracts/index.json"
CONTRACT_INDEX_SCHEMA = "clio-kit.mcp-user-contract-index.v1"
CONTRACT_SCHEMA = "clio-kit.mcp-user-contract.v1"
CONTRACT_CANONICALIZATION = "json-sort-keys-compact-utf8-v1"
CONTRACT_PROJECTION = "mcp-agent-tool-schema-v1"
MAX_CONTRACT_BYTES = 4 * 1024 * 1024
MAX_PROBE_OUTPUT_BYTES = 16 * 1024 * 1024
EXPECTED_CONTRACTS = {
    "clio-kit-jarvis-user-v2": {
        "server_name": "jarvis",
        "artifact": "jarvis-user-v2.json",
        "contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
        "tool_names": {
            "jarvis_add_step",
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_edit_step",
            "jarvis_run",
        },
    },
    "clio-kit-spack-user-v3": {
        "server_name": "spack",
        "artifact": "spack-user-v3.json",
        "contract_sha256": CLIO_KIT_SPACK_USER_CONTRACT_SHA256,
        "tool_names": {"spack_find", "spack_install", "spack_locate"},
    },
}


@pytest.fixture(scope="module")
def clio_kit_wheel() -> Path:
    """Return the exact external wheel used for cross-repository verification."""
    configured = os.getenv("CLIO_RELAY_CLIO_KIT_WHEEL")
    if configured is not None:
        wheel = Path(configured).expanduser().resolve(strict=True)
        if not wheel.is_file() or wheel.suffix != ".whl":
            raise AssertionError("CLIO_RELAY_CLIO_KIT_WHEEL must name one built wheel")
    else:
        sibling_dist = Path(__file__).resolve().parents[2] / "clio-kit" / "dist"
        wheels = sorted(sibling_dist.glob("clio_kit-*.whl"))
        if len(wheels) != 1:
            raise AssertionError(
                "set CLIO_RELAY_CLIO_KIT_WHEEL to the exact clio-kit release wheel; "
                f"found {len(wheels)} sibling build artifacts"
            )
        wheel = wheels[0].resolve(strict=True)
    expected_sha256 = os.getenv("CLIO_RELAY_CLIO_KIT_WHEEL_SHA256")
    if expected_sha256 is not None:
        assert hashlib.sha256(wheel.read_bytes()).hexdigest() == expected_sha256
    assert f"-{CLIO_KIT_SPACK_USER_CONTRACT_VERSION}-" in wheel.name
    return wheel


@pytest.fixture(scope="module")
def shipped_contracts(clio_kit_wheel: Path) -> dict[str, JSON]:
    """Load and cryptographically verify clio-kit's wheel contract artifacts."""
    return _load_shipped_contracts(clio_kit_wheel)


def test_relay_contract_pins_match_clio_kit_wheel_artifacts(
    shipped_contracts: dict[str, JSON],
) -> None:
    """Bind relay constants and local JARVIS definitions to canonical artifacts."""
    assert set(shipped_contracts) == set(EXPECTED_CONTRACTS)
    for contract_id, expected in EXPECTED_CONTRACTS.items():
        artifact = shipped_contracts[contract_id]
        assert artifact["contract_sha256"] == expected["contract_sha256"]
        assert set(cast(list[str], artifact["tool_names"])) == expected["tool_names"]

    jarvis_tools = _tools_by_name(shipped_contracts["clio-kit-jarvis-user-v2"])
    artifact_projection = {
        name: {
            "description": tool.get("description"),
            "inputSchema": tool["inputSchema"],
            "outputSchema": tool.get("outputSchema"),
            "annotations": tool.get("annotations"),
        }
        for name, tool in jarvis_tools.items()
    }
    assert jarvis_user_contract() == artifact_projection

    assert "jarvis_remove_step" not in jarvis_tools
    edit_input = cast(JSON, jarvis_tools["jarvis_edit_step"]["inputSchema"])
    edit_properties = cast(JSON, edit_input["properties"])
    assert cast(JSON, edit_properties["operation"])["enum"] == ["edit", "remove"]

    spack_tools = _tools_by_name(shipped_contracts["clio-kit-spack-user-v3"])
    assert "spack_load" not in spack_tools
    locate_output = cast(JSON, spack_tools["spack_locate"]["outputSchema"])
    locate_properties = cast(JSON, locate_output["properties"])
    assert locate_properties["load_spec"] == {"type": "string"}
    assert "load_spec" in cast(list[str], locate_output["required"])


@pytest.mark.parametrize("server_name", ["jarvis", "spack"])
def test_relay_runtime_identity_matches_exact_wheel_launcher(
    clio_kit_wheel: Path,
    tmp_path: Path,
    server_name: str,
) -> None:
    """Match relay evidence to the exact v4 identity computed by the wheel launcher."""
    project = _extract_wheel_server_project(clio_kit_wheel, server_name, tmp_path)
    expected = _wheel_launcher_project_identity(clio_kit_wheel, project)
    runner = _load_mcp_call_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"exact-uvx-launcher")
    uv.write_bytes(b"exact-uv-runtime")

    artifact = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        [
            "--refresh",
            "--no-config",
            "--from",
            str(clio_kit_wheel),
            "clio-kit",
            "mcp-server",
            server_name,
        ],
    )
    observed = cast(JSON, artifact["nested_runtime"])

    assert expected["schema_version"] == observed["schema_version"] == ("clio-kit.locked-server.v4")
    assert expected["server_name"] == observed["server_name"] == server_name
    assert expected["project_sha256"] == observed["project_sha256"]
    assert expected["lock_sha256"] == observed["lock_sha256"]
    assert observed["runtime_policy"] == ("uv-run:materialized:frozen:no-editable:no-dev:v3")
    assert observed["contract_source_verified"] is True
    assert observed["locked_runtime_verified"] is True
    assert artifact["server_process_artifact_verified"] is True
    assert artifact["verified"] is True


def test_runner_launches_exact_wheel_only_through_verified_snapshot(
    clio_kit_wheel: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the production runner and exact wheel through its private snapshot."""
    uvx = shutil.which("uvx")
    if uvx is None:
        raise AssertionError("uvx is required for the exact-wheel runner probe")
    runner = _load_mcp_call_runner()
    server_args = [
        "--refresh",
        "--no-config",
        "--from",
        str(clio_kit_wheel),
        "clio-kit",
        "mcp-server",
        "spack",
    ]
    discovery = cast(Any, runner)._server_artifact_identity(uvx, server_args)
    expected_digest = cast(Any, runner)._server_artifact_digest(discovery)
    monkeypatch.chdir(tmp_path)

    return_code = cast(Any, runner).run_mcp_call_from_params(
        {
            "server": uvx,
            "server_args": server_args,
            "operation": "tools/list",
            "timeout_seconds": 180,
            "expected_server_artifact_digest": expected_digest,
        }
    )
    result_payload = (tmp_path / "mcp-result.json").read_text(encoding="utf-8")
    result = _json_object(result_payload.encode("utf-8"), label="runner result")

    assert return_code == 0, result.get("protocol_error")
    assert result["server_args"] == server_args
    assert cast(JSON, result["server_artifact"])["install_spec"] == str(clio_kit_wheel)
    assert result["observed_server_artifact_digest"] == expected_digest
    execution = cast(JSON, result["server_execution_artifact"])
    assert execution["private_snapshot"] is True
    assert execution["source_sha256"] == hashlib.sha256(clio_kit_wheel.read_bytes()).hexdigest()
    assert execution["snapshot_sha256"] == execution["source_sha256"]
    assert execution["snapshot_verified_before_launch"] is True
    assert execution["snapshot_verified_after_launch"] is True
    assert execution["source_verified_after_launch"] is True
    assert execution["cleanup_verified"] is True
    assert "clio-relay-mcp-wheel-" not in result_payload


@pytest.mark.parametrize("contract_id", sorted(EXPECTED_CONTRACTS))
def test_live_locked_stdio_matches_shipped_contract_artifact(
    contract_id: str,
    clio_kit_wheel: Path,
    shipped_contracts: dict[str, JSON],
) -> None:
    """Compare each wheel artifact to its actual locked FastMCP stdio surface."""
    expected = EXPECTED_CONTRACTS[contract_id]
    server_name = cast(str, expected["server_name"])
    observed_tools = _probe_tools_list(clio_kit_wheel, server_name)
    artifact_tools = cast(list[JSON], shipped_contracts[contract_id]["tools"])
    observed_tools.sort(key=lambda tool: cast(str, tool["name"]))

    assert _canonical_json({"tools": observed_tools}) == _canonical_json({"tools": artifact_tools})
    parsed = [_relay_tool_schema(tool) for tool in observed_tools]
    assert remote_mcp_schema_digest(parsed) == expected["contract_sha256"]


def _load_shipped_contracts(wheel: Path) -> dict[str, JSON]:
    with zipfile.ZipFile(wheel) as archive:
        index_payload = _read_bounded_member(archive, CONTRACT_INDEX_PATH)
        index = _json_object(index_payload, label="contract index")
        assert index.get("schema_version") == CONTRACT_INDEX_SCHEMA
        entries = index.get("contracts")
        assert isinstance(entries, list)
        contracts: dict[str, JSON] = {}
        for raw_entry in cast(list[object], entries):
            assert isinstance(raw_entry, dict)
            entry = cast(JSON, raw_entry)
            contract_id = entry.get("contract_id")
            artifact_name = entry.get("artifact")
            assert isinstance(contract_id, str) and contract_id in EXPECTED_CONTRACTS
            assert isinstance(artifact_name, str)
            assert artifact_name == EXPECTED_CONTRACTS[contract_id]["artifact"]
            artifact_path = f"clio_kit/_mcp_contracts/{artifact_name}"
            artifact_payload = _read_bounded_member(archive, artifact_path)
            assert hashlib.sha256(artifact_payload).hexdigest() == entry["artifact_sha256"]
            artifact = _json_object(artifact_payload, label=contract_id)
            _verify_contract_artifact(entry, artifact)
            contracts[contract_id] = artifact
    return contracts


def _verify_contract_artifact(entry: JSON, artifact: JSON) -> None:
    assert artifact.get("schema_version") == CONTRACT_SCHEMA
    assert artifact.get("canonicalization") == CONTRACT_CANONICALIZATION
    assert artifact.get("projection") == CONTRACT_PROJECTION
    assert artifact.get("contract_id") == entry.get("contract_id")
    assert artifact.get("server_name") == entry.get("server_name")
    assert artifact.get("profile") == entry.get("profile") == "user"
    tools = artifact.get("tools")
    assert isinstance(tools, list)
    raw_tools = cast(list[object], tools)
    assert all(isinstance(tool, dict) for tool in raw_tools)
    typed_tools = [cast(JSON, tool) for tool in raw_tools]
    contract_digest = hashlib.sha256(_canonical_json(_contract_projection(typed_tools))).hexdigest()
    wire_digest = hashlib.sha256(_canonical_json({"tools": typed_tools})).hexdigest()
    assert artifact.get("contract_sha256") == entry.get("contract_sha256") == contract_digest
    assert artifact.get("wire_sha256") == entry.get("wire_sha256") == wire_digest
    assert artifact.get("tool_names") == [tool["name"] for tool in typed_tools]


def _contract_projection(tools: list[JSON]) -> JSON:
    projected = [
        {
            "annotations": tool.get("annotations"),
            "description": tool.get("description"),
            "input_schema": tool["inputSchema"],
            "name": tool["name"],
            "output_schema": tool.get("outputSchema"),
            "title": tool.get("title"),
        }
        for tool in tools
    ]
    projected.sort(key=lambda tool: cast(str, tool["name"]))
    return {"tools": projected}


def _probe_tools_list(wheel: Path, server_name: str) -> list[JSON]:
    uvx = shutil.which("uvx")
    if uvx is None:
        raise AssertionError("uvx is required for the clio-kit wheel contract probe")
    messages: tuple[JSON, ...] = (
        {
            "jsonrpc": "2.0",
            "id": "initialize",
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "clio-relay-contract-test", "version": "1.0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": "tools-list",
            "method": "tools/list",
            "params": {},
        },
    )
    command = [
        uvx,
        "--refresh",
        "--no-config",
        "--from",
        str(wheel),
        "clio-kit",
        "mcp-server",
        server_name,
    ]
    tools_list = _exchange_tools_list(command, messages, server_name=server_name)
    result = tools_list.get("result")
    if not isinstance(result, dict):
        raise AssertionError(f"clio-kit {server_name} tools/list response is malformed")
    tools = cast(JSON, result).get("tools")
    if not isinstance(tools, list):
        raise AssertionError(f"clio-kit {server_name} tools/list returned invalid tools")
    raw_tools = cast(list[object], tools)
    if not all(isinstance(tool, dict) for tool in raw_tools):
        raise AssertionError(f"clio-kit {server_name} tools/list returned invalid tools")
    return [cast(JSON, tool) for tool in raw_tools]


def _exchange_tools_list(
    command: list[str],
    requests: tuple[JSON, ...],
    *,
    server_name: str,
) -> JSON:
    """Complete the handshake before closing stdin, as a real MCP client does."""
    output_lines: queue.Queue[bytes | None] = queue.Queue(maxsize=1_024)
    deadline = time.monotonic() + 180
    with tempfile.TemporaryFile() as stderr:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
        )
        if process.stdin is None or process.stdout is None:
            raise AssertionError(f"clio-kit {server_name} stdio pipes are unavailable")
        stdin = process.stdin
        stdout = process.stdout

        def read_stdout() -> None:
            try:
                while line := stdout.readline():
                    output_lines.put(line)
            finally:
                output_lines.put(None)

        reader = threading.Thread(
            target=read_stdout,
            name=f"clio-kit-{server_name}-contract-stdout",
            daemon=True,
        )
        reader.start()
        try:
            stdin.write(_canonical_json(requests[0]) + b"\n")
            stdin.flush()
            initialize = _wait_for_response(
                output_lines,
                response_id="initialize",
                deadline=deadline,
                server_name=server_name,
            )
            if initialize.get("error") is not None:
                raise AssertionError(f"clio-kit {server_name} initialize failed")
            for request in requests[1:]:
                stdin.write(_canonical_json(request) + b"\n")
            stdin.flush()
            tools_list = _wait_for_response(
                output_lines,
                response_id="tools-list",
                deadline=deadline,
                server_name=server_name,
            )
            stdin.close()
            returncode = process.wait(timeout=max(0.1, deadline - time.monotonic()))
            reader.join(timeout=1)
            stderr.seek(0, os.SEEK_END)
            stderr_size = stderr.tell()
            if stderr_size > MAX_PROBE_OUTPUT_BYTES:
                raise AssertionError(f"clio-kit {server_name} exceeded bounded probe output")
            if returncode != 0:
                stderr.seek(max(0, stderr_size - 2_000))
                diagnostic = stderr.read().decode("utf-8", errors="replace")
                raise AssertionError(
                    f"clio-kit {server_name} exited with {returncode}: {diagnostic}"
                )
            return tools_list
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            stdout.close()
            if not stdin.closed:
                stdin.close()
            reader.join(timeout=1)


def _wait_for_response(
    output_lines: queue.Queue[bytes | None],
    *,
    response_id: str,
    deadline: float,
    server_name: str,
) -> JSON:
    total_bytes = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"timed out waiting for clio-kit {server_name} {response_id}")
        try:
            line = output_lines.get(timeout=remaining)
        except queue.Empty as exc:
            raise AssertionError(
                f"timed out waiting for clio-kit {server_name} {response_id}"
            ) from exc
        if line is None:
            raise AssertionError(f"clio-kit {server_name} closed stdout before {response_id}")
        total_bytes += len(line)
        if total_bytes > MAX_PROBE_OUTPUT_BYTES:
            raise AssertionError(f"clio-kit {server_name} exceeded bounded probe output")
        try:
            decoded = cast(object, json.loads(line.decode("utf-8")))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(decoded, dict):
            response = cast(JSON, decoded)
            if response.get("id") == response_id:
                return response


def _extract_wheel_server_project(wheel: Path, server_name: str, root: Path) -> Path:
    """Extract one trusted exact-wheel server project for launcher identity comparison."""
    project = root / "wheel-projects" / server_name
    project.mkdir(parents=True)
    suffix = f"/clio-kit-mcp-servers/{server_name}/uv.lock"
    with zipfile.ZipFile(wheel) as archive:
        lock_names = [
            info.filename
            for info in archive.infolist()
            if info.filename.endswith(suffix)
            or info.filename == f"clio-kit-mcp-servers/{server_name}/uv.lock"
        ]
        assert len(lock_names) == 1
        prefix = lock_names[0][: -len("uv.lock")]
        server_members = [
            info
            for info in archive.infolist()
            if info.filename.startswith(prefix) and info.filename != prefix
        ]
        assert len(server_members) <= 20_000
        assert sum(info.file_size for info in server_members) <= 512 * 1024 * 1024
        for info in server_members:
            relative_text = info.filename[len(prefix) :].rstrip("/")
            relative = PurePosixPath(relative_text)
            assert relative_text and relative.as_posix() == relative_text
            assert not relative.is_absolute() and ".." not in relative.parts
            target = project.joinpath(*relative.parts)
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination, length=1024 * 1024)
            assert target.stat().st_size == info.file_size
    return project


def _wheel_launcher_project_identity(wheel: Path, project: Path) -> JSON:
    """Ask clio-kit's exact wheel source to compute its own child identity."""
    script = "\n".join(
        [
            "import json",
            "import sys",
            "from pathlib import Path",
            "sys.path.insert(0, sys.argv[1])",
            "import clio_kit",
            "print(json.dumps(clio_kit.locked_server_project_identity(Path(sys.argv[2]))))",
        ]
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(wheel), str(project)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return _json_object(completed.stdout.encode("utf-8"), label="launcher identity")


def _load_mcp_call_runner() -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "mcp_call"
        / "runner.py"
    )
    spec = importlib.util.spec_from_file_location("clio_relay_mcp_call_runner_contract", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load MCP call runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _relay_tool_schema(tool: JSON) -> RemoteMcpToolSchema:
    return RemoteMcpToolSchema(
        name=cast(str, tool["name"]),
        title=cast(str | None, tool.get("title")),
        description=cast(str | None, tool.get("description")),
        input_schema=cast(JSON, tool["inputSchema"]),
        output_schema=cast(JSON | None, tool.get("outputSchema")),
        annotations=cast(JSON | None, tool.get("annotations")),
    )


def _tools_by_name(artifact: JSON) -> dict[str, JSON]:
    return {cast(str, tool["name"]): tool for tool in cast(list[JSON], artifact["tools"])}


def _read_bounded_member(archive: zipfile.ZipFile, name: str) -> bytes:
    info = archive.getinfo(name)
    assert not info.is_dir() and info.file_size <= MAX_CONTRACT_BYTES
    with archive.open(info) as stream:
        payload = stream.read(MAX_CONTRACT_BYTES + 1)
    assert len(payload) <= MAX_CONTRACT_BYTES and len(payload) == info.file_size
    return payload


def _json_object(payload: bytes, *, label: str) -> JSON:
    try:
        value = cast(object, json.loads(payload.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssertionError(f"clio-kit {label} is not UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise AssertionError(f"clio-kit {label} is not a JSON object")
    return cast(JSON, value)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
