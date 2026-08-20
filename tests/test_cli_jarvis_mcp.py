"""Tests for the ``jarvis-runtime-authority``/``mcp-call``/``jarvis-mcp-call``/
``jarvis-mcp-refresh``/``mcp-server`` top-level command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` alongside the five commands'
extraction into ``src/clio_relay/cli_jarvis_mcp.py``, per ground rule 3 (SS2
of ``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises. Classified by
whether the test actually drives ``CliRunner().invoke(app, [...])`` with one
of this group's command names -- tests that instead call a still cli.py-
resident JARVIS execution-query engine function directly (e.g. ``cli.
_run_post_run_jarvis_execution_query(...)``) as a plain unit test stayed in
``tests/test_cli.py``, since they exercise resident code, not this group's
thin command wrapper (``cli_jarvis_mcp.py``'s own docstring names that
engine and why it stays cli.py-resident for now). ``jarvis-mcp-refresh``
itself has no dedicated ``CliRunner``-level test anywhere in the suite;
``jarvis-runtime-authority``'s only one lives in
``tests/test_jarvis_service_runtime.py`` (its own docstring notes the patch-
target fix that test needed) and stayed there.

**Patch-target parity.** Every test below patches a collaborator on its
owner module directly, either via a bare module reference already common in
``tests/test_cli.py`` (``fastmcp_server``) or via monkeypatch's string form
(``"clio_relay.remote_cli.write_remote_file"``, ``"clio_relay.jarvis_mcp.
jarvis_mcp_server"``) -- never through ``cli.py`` -- so the move needed no
patch-target changes at all.

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on, plus a session-teardown collaborator half none
of these tests exercise). Reproduced here as the env-var half only, the same
precedent every prior command-group extraction in this campaign established.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from click import unstyle
from typer.testing import CliRunner

import clio_relay.fastmcp_server as fastmcp_server
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    RemoteMcpServerConfig,
    cluster_route_revision,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.jarvis_mcp import jarvis_cd_lock_binding_expectation
from clio_relay.models import JobKind, McpAdmissionClass, McpCallSpec, McpOperation
from clio_relay.remote_mcp import MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
from tests.test_cli import (
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only."""
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_cli_mcp_call_preserves_arguments(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "ares",
            "--server",
            "remote-server",
            "--server-arg",
            "--stdio",
            "--env-from",
            "SCIENCE_TOKEN=SITE_SCIENCE_TOKEN",
            "--tool",
            "simulate",
            "--arguments-json",
            '{"steps": 100, "case": "site-simulation"}',
            "--timeout-seconds",
            "90",
            "--idempotency-key",
            "cli-mcp-call-args",
        ],
    )

    assert result.exit_code == 0
    job_id = result.output.strip()
    job = ClioCoreQueue(core_dir).get_job(job_id)
    assert job.kind == JobKind.MCP_CALL
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "remote-server"
    assert job.spec.server_args == ["--stdio"]
    assert job.spec.env_from == {"SCIENCE_TOKEN": "SITE_SCIENCE_TOKEN"}
    assert job.spec.arguments == {"steps": 100, "case": "site-simulation"}
    assert job.spec.timeout_seconds == 90


@pytest.mark.parametrize(
    ("operation", "operation_arguments", "expected_tool_arguments"),
    [
        ("tools/list", [], None),
        (
            "tools/call",
            [
                "--tool",
                "inspect",
                "--arguments-json",
                '{"dataset":"asteroid-first-five"}',
            ],
            {"dataset": "asteroid-first-five"},
        ),
    ],
)
def test_cli_remote_mcp_call_stages_exact_route_authority_for_one_invocation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    operation: str,
    operation_arguments: list[str],
    expected_tool_arguments: dict[str, object] | None,
) -> None:
    """Remote discovery and calls receive the exact operator route, then remove it."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        allow_tools=["inspect"],
        profiles=["user"],
    )
    definition = ClusterDefinition(
        name="alpha",
        ssh_host="alpha-login",
        scheduler_provider="slurm",
        core_dir="$HOME/site/relay-core",
        spool_dir="$HOME/site/relay-spool",
        remote_mcp_servers={"science": registration},
    )
    ClusterRegistry(clusters={definition.name: definition}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    registry_writes: list[tuple[str, bytes]] = []
    argument_writes: list[tuple[str, bytes]] = []
    removals: list[tuple[str, str, bool]] = []
    shell_scripts: list[str] = []
    events: list[str] = []

    # `write_remote_file`/`remove_remote_file` now resolve through a single
    # extraction-stable seam (`remote_cli.write_remote_file`/
    # `remote_cli.remove_remote_file`) for BOTH the registry-staging call
    # inside `staged_remote_cluster_registry` and the arguments-staging call
    # inside cli_jarvis_mcp.py's own mcp-call handling -- they used to be
    # distinguishable by which namespace (`cli` vs `remote_cli`) intercepted
    # the call, an accident of the bare-import coupling docs/design/relay-
    # architecture-2026-08.md SS4.6 describes, not a real distinction.
    # Dispatch on the staged path itself instead, which is what actually
    # differs.
    def write_remote_file(
        selected_definition: ClusterDefinition,
        path: str,
        data: bytes,
    ) -> None:
        assert selected_definition == definition
        if path.endswith("/clusters.json"):
            events.append("write-registry")
            registry_writes.append((path, data))
        elif path.endswith("/arguments.json"):
            events.append("write-arguments")
            argument_writes.append((path, data))
        else:
            raise AssertionError(f"unexpected remote write path: {path}")

    def remove_remote_file(
        selected_definition: ClusterDefinition,
        path: str,
        *,
        remove_empty_parent: bool,
    ) -> None:
        assert selected_definition == definition
        if path.endswith("/clusters.json"):
            events.append("remove-registry")
            removals.append(("registry", path, remove_empty_parent))
        elif path.endswith("/arguments.json"):
            events.append("remove-arguments")
            removals.append(("arguments", path, remove_empty_parent))
        else:
            raise AssertionError(f"unexpected remote remove path: {path}")

    def run_remote_shell(selected_definition: ClusterDefinition, script: str) -> str:
        assert selected_definition == definition
        events.append("run")
        shell_scripts.append(script)
        return "job_remote_mcp\n"

    monkeypatch.setattr("clio_relay.remote_cli.write_remote_file", write_remote_file)
    monkeypatch.setattr("clio_relay.remote_cli.remove_remote_file", remove_remote_file)
    monkeypatch.setattr("clio_relay.remote_cli.run_remote_shell", run_remote_shell)

    result = CliRunner().invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            definition.name,
            "--server",
            registration.command,
            "--server-arg=--stdio",
            "--operation",
            operation,
            *operation_arguments,
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "job_remote_mcp"
    assert len(registry_writes) == 1
    registry_path, registry_payload = registry_writes[0]
    staged_registry = ClusterRegistry.model_validate_json(registry_payload)
    assert staged_registry.clusters == {definition.name: definition}
    staged_definition = staged_registry.require(definition.name)
    assert staged_definition.remote_mcp_servers == {"science": registration}
    assert cluster_route_revision(staged_definition) == cluster_route_revision(definition)
    assert registry_path.startswith(".local/share/clio-relay/desktop-submissions/mcp-registry-")
    assert registry_path.endswith("/clusters.json")
    assert len(shell_scripts) == 1
    assert f'export CLIO_RELAY_CLUSTER_REGISTRY="$HOME/{registry_path}";' in shell_scripts[0]
    assert "export CLIO_RELAY_CLI_MODE=local;" in shell_scripts[0]
    assert f'"$HOME/.local/bin/clio-relay" mcp-call --cluster {definition.name}' in shell_scripts[0]
    assert removals[-1] == ("registry", registry_path, True)
    if expected_tool_arguments is None:
        assert argument_writes == []
        assert events == ["write-registry", "run", "remove-registry"]
    else:
        assert len(argument_writes) == 1
        arguments_path, arguments_payload = argument_writes[0]
        assert json.loads(arguments_payload) == expected_tool_arguments
        assert removals == [
            ("arguments", arguments_path, True),
            ("registry", registry_path, True),
        ]
        assert events == [
            "write-registry",
            "write-arguments",
            "run",
            "remove-arguments",
            "remove-registry",
        ]


def test_cli_remote_mcp_call_cleans_arguments_and_route_authority_after_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A failed remote launch removes both invocation-scoped staging files."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        allow_tools=["inspect"],
    )
    definition = ClusterDefinition(
        name="alpha",
        ssh_host="alpha-login",
        remote_mcp_servers={"science": registration},
    )
    ClusterRegistry(clusters={definition.name: definition}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    staged_paths: list[tuple[str, str]] = []
    removed_paths: list[tuple[str, str, bool]] = []

    # See the sibling `_stages_exact_route_authority_for_one_invocation` test:
    # both the registry- and arguments-staging calls now resolve through the
    # single extraction-stable `remote_cli.write_remote_file`/
    # `remote_cli.remove_remote_file` seam, so dispatch on the staged path.
    def write_remote_file(
        _definition: ClusterDefinition,
        path: str,
        _data: bytes,
    ) -> None:
        if path.endswith("/clusters.json"):
            staged_paths.append(("registry", path))
        elif path.endswith("/arguments.json"):
            staged_paths.append(("arguments", path))
        else:
            raise AssertionError(f"unexpected remote write path: {path}")

    def remove_remote_file(
        _definition: ClusterDefinition,
        path: str,
        *,
        remove_empty_parent: bool,
    ) -> None:
        if path.endswith("/clusters.json"):
            removed_paths.append(("registry", path, remove_empty_parent))
        elif path.endswith("/arguments.json"):
            removed_paths.append(("arguments", path, remove_empty_parent))
        else:
            raise AssertionError(f"unexpected remote remove path: {path}")

    def fail_remote_shell(_definition: ClusterDefinition, _script: str) -> str:
        raise RelayError("remote launch failed")

    monkeypatch.setattr("clio_relay.remote_cli.write_remote_file", write_remote_file)
    monkeypatch.setattr("clio_relay.remote_cli.remove_remote_file", remove_remote_file)
    monkeypatch.setattr("clio_relay.remote_cli.run_remote_shell", fail_remote_shell)

    result = CliRunner().invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            definition.name,
            "--server",
            registration.command,
            "--server-arg=--stdio",
            "--tool",
            "inspect",
            "--arguments-json",
            '{"dataset":"asteroid-first-five"}',
        ],
    )

    assert result.exit_code == 1
    assert "remote launch failed" in result.output
    assert [kind for kind, _path in staged_paths] == ["registry", "arguments"]
    assert removed_paths == [
        ("arguments", staged_paths[1][1], True),
        ("registry", staged_paths[0][1], True),
    ]


@pytest.mark.parametrize("command", ["mcp-call", "jarvis-mcp-call"])
def test_cli_mcp_call_rejects_public_admission_class_option(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    command: str,
) -> None:
    """The CLI exposes no caller-selectable reserved worker lane."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            command,
            "--cluster",
            "ares",
            "--tool",
            "jarvis_describe" if command == "jarvis-mcp-call" else "inspect",
            *(["--server", "arbitrary-mcp"] if command == "mcp-call" else []),
            "--admission-class",
            "control_query",
        ],
    )

    assert result.exit_code == 2
    output = unstyle(result.output)
    assert "No such option" in output
    assert "--admission-class" in output


def test_cli_arbitrary_tools_list_remains_workload(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """An arbitrary MCP discovery command cannot occupy reserved capacity."""
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "ares",
            "--server",
            "arbitrary-mcp",
            "--server-arg=--hang",
            "--operation",
            "tools/list",
            "--timeout-seconds",
            "1",
            "--idempotency-key",
            "arbitrary-tools-list",
        ],
    )

    assert result.exit_code == 0, result.output
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.operation is McpOperation.TOOLS_LIST
    assert job.spec.admission_class is McpAdmissionClass.WORKLOAD


def test_cli_generic_default_key_tracks_timeout_and_derived_authority(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Default retries cannot alias workload, registered control, or timeout changes."""
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    def invoke(timeout: int) -> Any:
        return CliRunner().invoke(
            app,
            [
                "mcp-call",
                "--cluster",
                "ares",
                "--server",
                "science-mcp",
                "--server-arg=--stdio",
                "--operation",
                "tools/list",
                "--timeout-seconds",
                str(timeout),
            ],
        )

    workload_result = invoke(30)
    assert workload_result.exit_code == 0, workload_result.output
    registration = RemoteMcpServerConfig(
        command="science-mcp",
        args=["--stdio"],
        allow_tools=["inspect"],
        call_timeout_seconds=60,
    )
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="ares",
                remote_mcp_servers={"science": registration},
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    control_30_result = invoke(30)
    control_60_result = invoke(60)
    assert control_30_result.exit_code == 0, control_30_result.output
    assert control_60_result.exit_code == 0, control_60_result.output

    queue = ClioCoreQueue(core_dir)
    jobs = [
        queue.get_job(result.output.strip())
        for result in (workload_result, control_30_result, control_60_result)
    ]
    assert all(isinstance(job.spec, McpCallSpec) for job in jobs)
    specs = [cast(McpCallSpec, job.spec) for job in jobs]
    assert specs[0].admission_class is McpAdmissionClass.WORKLOAD
    assert specs[1].admission_class is McpAdmissionClass.CONTROL_QUERY
    assert specs[2].admission_class is McpAdmissionClass.CONTROL_QUERY
    assert len({job.idempotency_key for job in jobs}) == 3
    legacy_server_digest = hashlib.sha256(
        json.dumps(
            {
                "server": "science-mcp",
                "args": ["--stdio"],
                "env_from": {},
                "expected_server_artifact_digest": None,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    legacy_arguments_digest = hashlib.sha256(
        json.dumps(
            {"operation": "tools/list", "tool": None, "arguments": {}},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert jobs[0].idempotency_key == (
        f"mcp:ares:{legacy_server_digest}:tools/list:None:{legacy_arguments_digest}"
    )


def test_cli_pinned_jarvis_control_query_rejects_oversized_timeout(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Pinned control-query processes cannot outlive the reserved-lane bound."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_REMOTE_CLUSTER", "ares")

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--operation",
            "tools/list",
            "--timeout-seconds",
            str(MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS + 1),
        ],
    )

    assert result.exit_code == 2
    assert f"timeout exceeds {MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS} seconds" in result.output


def test_cli_jarvis_mcp_call_uses_builtin_cluster_command(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("JARVIS_MCP_SPACK_COMMAND", "/opt/site/spack/bin/spack")

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--tool",
            "jarvis_describe",
            "--arguments-json",
            '{"target":"packages"}',
            "--idempotency-key",
            "cli-jarvis-mcp",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "clio-kit"
    assert job.spec.server_args == ["mcp-server", "jarvis"]
    assert job.spec.env_from == {"JARVIS_MCP_SPACK_COMMAND": "JARVIS_MCP_SPACK_COMMAND"}
    assert job.spec.tool == "jarvis_describe"
    assert job.spec.expected_jarvis_cd_lock_binding == jarvis_cd_lock_binding_expectation()
    assert job.spec.arguments == {"target": "packages"}


def test_cli_remote_jarvis_call_defers_artifact_selection_to_target(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    writes: list[tuple[str, bytes]] = []
    removals: list[tuple[str, bool]] = []
    commands: list[list[str]] = []

    def write_remote(_definition: ClusterDefinition, path: str, data: bytes) -> None:
        writes.append((path, data))

    def fail_local_resolution() -> str:
        raise AssertionError("desktop resolved JARVIS artifact")

    monkeypatch.setattr(
        "clio_relay.remote_cli.write_remote_file",
        write_remote,
    )

    def remove_remote(
        _definition: ClusterDefinition,
        path: str,
        *,
        remove_empty_parent: bool,
    ) -> None:
        removals.append((path, remove_empty_parent))

    monkeypatch.setattr("clio_relay.remote_cli.remove_remote_file", remove_remote)

    def run_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        commands.append(args)
        return "job_remote_jarvis\n"

    monkeypatch.setattr("clio_relay.remote_cli.run_remote_clio", run_remote)
    monkeypatch.setattr(
        "clio_relay.jarvis_mcp.jarvis_mcp_server",
        fail_local_resolution,
    )

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--tool",
            "jarvis_describe",
            "--arguments-json",
            '{"target":"packages"}',
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_remote_jarvis"
    assert writes and json.loads(writes[0][1]) == {"target": "packages"}
    assert removals == [(writes[0][0], True)]
    assert commands[0][0] == "jarvis-mcp-call"
    assert "--server" not in commands[0]


def test_target_side_jarvis_discovery_uses_receipt_without_cluster_registry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_REMOTE_CLUSTER", "ares")

    result = CliRunner().invoke(
        app,
        [
            "jarvis-mcp-call",
            "--cluster",
            "ares",
            "--operation",
            "tools/list",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.operation.value == "tools/list"
    assert job.spec.tool is None
    assert job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY
    assert job.spec.timeout_seconds == MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
    assert job.spec.expected_jarvis_cd_lock_binding == jarvis_cd_lock_binding_expectation()


def test_cli_mcp_call_reads_arguments_json_file(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    core_dir = tmp_path / "core"
    arguments_path = tmp_path / "arguments.json"
    arguments_path.write_text('\ufeff{"steps": 150, "sample": "ares-live"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "mcp-call",
            "--cluster",
            "ares",
            "--server",
            "remote-server",
            "--tool",
            "echo",
            "--arguments-json-file",
            str(arguments_path),
            "--idempotency-key",
            "cli-mcp-call-file-args",
        ],
    )

    assert result.exit_code == 0
    job_id = result.output.strip()
    job = ClioCoreQueue(core_dir).get_job(job_id)
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.arguments == {"steps": 150, "sample": "ares-live"}


def test_mcp_server_cli_dispatches_native_fastmcp_stdio(
    monkeypatch: MonkeyPatch,
) -> None:
    observed: list[str] = []

    def run_stdio(*, profile: str) -> None:
        observed.append(profile)

    monkeypatch.setattr(fastmcp_server, "run_fastmcp_stdio", run_stdio)

    result = CliRunner().invoke(app, ["mcp-server", "--profile", "operator"])

    assert result.exit_code == 0
    assert observed == ["operator"]


def test_mcp_server_cli_dispatches_authenticated_fastmcp_http(
    monkeypatch: MonkeyPatch,
) -> None:
    observed: list[tuple[str, str, int, str]] = []

    def run_http(*, profile: str, host: str, port: int, path: str) -> None:
        observed.append((profile, host, port, path))

    monkeypatch.setattr(fastmcp_server, "run_fastmcp_http", run_http)

    result = CliRunner().invoke(
        app,
        [
            "mcp-server",
            "--profile",
            "all",
            "--transport",
            "http",
            "--host",
            "0.0.0.0",
            "--port",
            "9876",
            "--path",
            "/relay-mcp",
        ],
    )

    assert result.exit_code == 0
    assert observed == [("all", "0.0.0.0", 9876, "/relay-mcp")]
