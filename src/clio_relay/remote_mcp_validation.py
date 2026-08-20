"""The ``remote-mcp validate`` business-logic engine (iowarp/clio-relay#231
cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
calls out ``remote_mcp_validate`` -- the largest of ``remote_mcp_app``'s six
commands at 314 body lines -- among cli.py's "other giants": it composes
route resolution, one-or-three durable virtual MCP calls, and (for the fresh
Spack install transition) a bounded configuration-tree observation, ~980
lines of real business logic that ground rule 2 (cli.py parses and renders
only) says cli.py should not own. This module is that real owner -- the
route/call/spack-configuration engine, factored out from
``cli_remote_mcp.py``'s thin ``remote_mcp_validate`` command wrapper, which
calls back into this module's public API.

**Public API** (what ``cli_remote_mcp.py`` calls): ``RemoteMcpValidationRoute``
/``RemoteMcpValidationPreflight``/``RemoteMcpValidationCall`` (the three
result/state types), ``resolve_remote_mcp_validation_route``,
``execute_remote_mcp_validation_call``, ``require_passing_remote_mcp_call``,
``require_spack_preinstall_absent``, and ``collect_spack_configuration_
observation``. Everything else (the remote/local configuration-observation
split, manifest parsing and component-path safety, the embedded POSIX
observer script) is a private implementation detail of this module alone --
cli.py's own docstring-style leading-underscore convention for "not part of
the public seam," kept even though it moved out of cli.py, since none of it
needs to cross a module boundary a second time.

**Collaborators reached through cli.py's own name (not moved here).** Six
cli.py-private helpers this module's ``execute_remote_mcp_validation_call``
and ``_collect_remote_spack_configuration_observation`` call
(``_mcp_response_job_id``, ``_json_output``, ``_remote_artifact_records``,
``_read_remote_json_artifact_kind``, ``_complete_local_artifact_records``,
``_read_local_json_artifact_kind``) are confirmed shared with the jarvis-mcp
execution-query engine (cli_jarvis_mcp_validate.py's own docstring names
that engine) -- both remain cli.py-resident, unsequenced future work, and
this module reaches them the same way that sibling module does: through
cli.py's own name via the established function-local ``import clio_relay.cli
as cli`` discipline. ``mcp_stdio_validation``, ``remote_cli``, ``relay_ops``,
and ``remote_mcp`` are genuine owner modules, imported directly.

**Reassigned patch-seam callers.** ``remote_mcp.build_remote_mcp_acceptance_
report`` and ``remote_cli.run_remote_shell`` each had exactly one call site
in the whole of cli.py -- both now here, their only callers -- so this
slice reassigns their ``caller`` entry in ``AUDITED_COLLABORATORS`` from
``"cli"`` to ``"remote_mcp_validation"`` and registers this module in
``_GUARDED_CALLERS``. ``mcp_stdio_validation.run_packaged_mcp_stdio_
session``, ``remote_cli.run_remote_clio``/``should_execute_on_cluster``/
``remote_command_timeout``, and ``relay_ops.wait_for_terminal``/
``job_status`` are also audited but used pervasively elsewhere in cli.py
(session code, the jarvis-mcp engine), so they keep their ``"cli"`` caller
unchanged -- this module simply adds a second, module-attribute-style
caller for each, the shape every prior slice in this campaign established
for a still-shared collaborator.
"""

from __future__ import annotations

import hashlib
import os
import re
import shlex
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

import typer
from pydantic import ValidationError

import clio_relay.core_queue as core_queue
import clio_relay.mcp_stdio_validation as mcp_stdio_validation
import clio_relay.relay_ops as relay_ops
import clio_relay.remote_cli as remote_cli
import clio_relay.remote_mcp as remote_mcp
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.errors import RelayError
from clio_relay.mcp_stdio_validation import PackagedMcpStdioSession
from clio_relay.remote_mcp import (
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
    RemoteMcpAcceptanceReport,
    RemoteMcpSchemaCache,
    RemoteMcpSpackConfigurationObservation,
    RemoteMcpStructuredResultExpectation,
    VirtualRemoteMcpCatalog,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false

SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS = 60.0
MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES = 128 * 1024
MAX_SPACK_CONFIGURATION_TREE_ENTRIES = 1_024


@dataclass(frozen=True)
class RemoteMcpValidationRoute:
    """One preflight-resolved virtual alias and its argument wrapping mode."""

    alias: str
    arguments_wrapped: bool


@dataclass(frozen=True)
class RemoteMcpValidationPreflight:
    """Inputs and immutable routes resolved before any validation dispatch."""

    registry_path: Path
    registry: ClusterRegistry
    definition: ClusterDefinition
    remote_arguments: dict[str, Any]
    routes: dict[str, RemoteMcpValidationRoute]
    result_expectation: RemoteMcpStructuredResultExpectation | None

    @property
    def fresh_spack_transition(self) -> bool:
        """Return whether this run requests disposable-store install proof."""
        return (
            self.result_expectation is not None
            and self.result_expectation.fresh_install_store_root is not None
        )


@dataclass(frozen=True)
class RemoteMcpValidationCall:
    """One completed ordinary remote-MCP acceptance call and its protocol result."""

    report: RemoteMcpAcceptanceReport
    protocol_result: dict[str, Any] | None
    stdio_session: PackagedMcpStdioSession


def resolve_remote_mcp_validation_route(
    *,
    catalog: VirtualRemoteMcpCatalog,
    cluster: str,
    server_name: str,
    remote_tool_name: str,
) -> RemoteMcpValidationRoute:
    """Resolve exactly one fresh virtual alias before any MCP call is dispatched."""
    aliases = [
        alias
        for alias, virtual in catalog.tools.items()
        if virtual.remote_tool.name == remote_tool_name
        and cluster in virtual.routes
        and virtual.routes[cluster].server_name == server_name
    ]
    if len(aliases) != 1:
        raise typer.BadParameter(
            f"expected one fresh virtual alias for {cluster}/{server_name}/{remote_tool_name}, "
            f"found {len(aliases)}; run remote-mcp refresh and reload"
        )
    virtual = catalog.tools[aliases[0]]
    return RemoteMcpValidationRoute(
        alias=aliases[0],
        arguments_wrapped=virtual.arguments_wrapped,
    )


def execute_remote_mcp_validation_call(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    execute_remotely: bool,
    registry: ClusterRegistry,
    cache: RemoteMcpSchemaCache,
    cluster: str,
    server_name: str,
    profile: str,
    remote_tool_name: str,
    route: RemoteMcpValidationRoute,
    remote_arguments: dict[str, Any],
    result_expectation: RemoteMcpStructuredResultExpectation | None,
    wait_timeout_seconds: float,
    poll_seconds: float,
    reserved_names: set[str],
) -> RemoteMcpValidationCall:
    """Run one virtual alias and build its ordinary durable acceptance report."""
    import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
    import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination

    stdio_session = mcp_stdio_validation.run_packaged_mcp_stdio_session(
        profile=profile,
        tool=route.alias,
        arguments=(
            {"cluster": cluster, "arguments": remote_arguments}
            if route.arguments_wrapped
            else {"cluster": cluster, **remote_arguments}
        ),
    )
    job_id = cli_jarvis_artifact_io._mcp_response_job_id(stdio_session.tools_call_response)
    if execute_remotely:
        remote_cli.run_remote_clio(
            definition,
            [
                "job",
                "wait",
                job_id,
                "--timeout-seconds",
                str(wait_timeout_seconds),
                "--poll-seconds",
                str(poll_seconds),
            ],
        )
        call_status = cli_remote_collection_pagination._json_output(
            remote_cli.run_remote_clio(definition, ["job", "status", job_id]),
            "remote MCP validation job status",
        )
        artifacts = cli_jarvis_artifact_io._remote_artifact_records(definition, job_id)
        mcp_result = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
        )
        provenance = cli_jarvis_artifact_io._read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
        )
    else:
        relay_ops.wait_for_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        call_status = relay_ops.job_status(queue, job_id)
        artifacts = cli_remote_collection_pagination._complete_local_artifact_records(queue, job_id)
        mcp_result = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue,
            artifacts,
            kind="mcp_result",
        )
        provenance = cli_jarvis_artifact_io._read_local_json_artifact_kind(
            queue,
            artifacts,
            kind="provenance",
        )
    protocol_result = (
        cast(dict[str, Any], mcp_result["protocol_result"])
        if mcp_result is not None and isinstance(mcp_result.get("protocol_result"), dict)
        else None
    )
    report = remote_mcp.build_remote_mcp_acceptance_report(
        registry=registry,
        cache=cache,
        cluster=cluster,
        server_name=server_name,
        remote_tool_name=remote_tool_name,
        profile=profile,
        call_job_id=job_id,
        call_status=call_status,
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
        result_expectation=result_expectation,
        reserved_names=reserved_names,
        mcp_stdio_evidence=stdio_session.evidence(),
    )
    return RemoteMcpValidationCall(
        report=report,
        protocol_result=protocol_result,
        stdio_session=stdio_session,
    )


def require_passing_remote_mcp_call(
    call: RemoteMcpValidationCall,
    *,
    phase: str,
) -> None:
    """Stop a transition before its next mutation when an earlier call failed."""
    if not call.report.passed:
        failed = [check.name for check in call.report.checks if not check.passed]
        raise RelayError(f"{phase} acceptance failed before next dispatch: {failed}")


def require_spack_preinstall_absent(
    protocol_result: dict[str, Any] | None,
    *,
    requested_spec: str,
) -> None:
    """Require exact structured absence before dispatching the mutating install call."""
    structured = (
        cast(dict[str, Any], protocol_result.get("structuredContent"))
        if protocol_result is not None
        and isinstance(protocol_result.get("structuredContent"), dict)
        else None
    )
    if (
        protocol_result is None
        or protocol_result.get("isError") is True
        or structured is None
        or structured.get("schema_version") != "spack.mcp.result.v1"
        or structured.get("operation") != "find"
        or structured.get("query") != requested_spec
        or structured.get("count") != 0
        or isinstance(structured.get("count"), bool)
        or structured.get("packages") != []
    ):
        raise RelayError(
            "fresh Spack preinstall call did not prove count=0 and packages=[] "
            "for the exact requested spec"
        )


def collect_spack_configuration_observation(
    *,
    definition: ClusterDefinition,
    execute_remotely: bool,
    expectation: RemoteMcpStructuredResultExpectation,
    phase: Literal["preinstall", "postinstall"],
) -> RemoteMcpSpackConfigurationObservation:
    """Collect one real, bounded wrapper/configuration manifest observation."""
    manifest_path = expectation.fresh_install_configuration_manifest_path
    expected_sha256 = expectation.fresh_install_configuration_sha256
    if manifest_path is None or expected_sha256 is None:
        raise RelayError("fresh Spack configuration expectation is incomplete")
    if execute_remotely:
        observation = _collect_remote_spack_configuration_observation(
            definition=definition,
            phase=phase,
            manifest_path=manifest_path,
            expected_sha256=expected_sha256,
        )
    else:
        observation = _collect_local_spack_configuration_observation(
            phase=phase,
            manifest_path=manifest_path,
            expected_sha256=expected_sha256,
        )
    if (
        observation.phase != phase
        or observation.manifest_path != manifest_path
        or observation.manifest_sha256 != expected_sha256
    ):
        raise RelayError("fresh Spack configuration observation does not match expectation")
    return observation


def _collect_remote_spack_configuration_observation(
    *,
    definition: ClusterDefinition,
    phase: Literal["preinstall", "postinstall"],
    manifest_path: str,
    expected_sha256: str,
) -> RemoteMcpSpackConfigurationObservation:
    """Collect a configuration observation through one bounded Bash/SSH command."""
    import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination

    script = _remote_spack_configuration_observer_script()
    command = " ".join(
        (
            "python3",
            "-c",
            shlex.quote(script),
            shlex.quote(phase),
            shlex.quote(manifest_path),
            shlex.quote(expected_sha256),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES),
            str(MAX_SPACK_CONFIGURATION_TREE_ENTRIES),
        )
    )
    with remote_cli.remote_command_timeout(SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS):
        output = remote_cli.run_remote_shell(definition, command)
    if len(output.encode("utf-8")) > MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES:
        raise RelayError("remote Spack configuration observation output exceeded its bound")
    payload = cli_remote_collection_pagination._json_output(
        output, f"{phase} Spack configuration observation"
    )
    try:
        return RemoteMcpSpackConfigurationObservation.model_validate(payload)
    except ValidationError as exc:
        raise RelayError(
            f"remote Spack configuration observation is invalid: {exc.errors()[0]['msg']}"
        ) from exc


def _collect_local_spack_configuration_observation(
    *,
    phase: Literal["preinstall", "postinstall"],
    manifest_path: str,
    expected_sha256: str,
) -> RemoteMcpSpackConfigurationObservation:
    """Collect the same evidence locally using POSIX no-follow file operations."""
    if os.name == "nt":
        raise RelayError(
            "local fresh Spack configuration observation requires a POSIX host; "
            "use the configured SSH target from Windows"
        )
    manifest = Path(manifest_path)
    base = manifest.parent
    _require_regular_nonsymlink_directory(base, label="configuration manifest directory")
    manifest_bytes, manifest_size = _read_bounded_regular_nonsymlink_file(
        manifest,
        maximum_bytes=MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
        label="configuration manifest",
        require_nonempty=True,
    )
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha256 != expected_sha256:
        raise RelayError("configuration manifest SHA-256 does not match the expectation")
    declarations = _parse_spack_configuration_manifest(manifest_bytes)
    _require_exact_spack_configuration_component_set(base, declarations)
    components: list[dict[str, object]] = []
    for declared_sha256, relative_path in declarations:
        component_path = _safe_spack_configuration_component_path(base, relative_path)
        component_bytes, component_size = _read_bounded_regular_nonsymlink_file(
            component_path,
            maximum_bytes=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
            label=f"configuration component {relative_path}",
            require_nonempty=False,
        )
        observed_sha256 = hashlib.sha256(component_bytes).hexdigest()
        if observed_sha256 != declared_sha256:
            raise RelayError(f"configuration component SHA-256 changed: {relative_path}")
        components.append(
            {
                "relative_path": relative_path,
                "sha256": observed_sha256,
                "size_bytes": component_size,
                "regular_file": True,
            }
        )
    return RemoteMcpSpackConfigurationObservation.model_validate(
        {
            "phase": phase,
            "manifest_path": manifest_path,
            "manifest_sha256": manifest_sha256,
            "manifest_size_bytes": manifest_size,
            "manifest_regular_file": True,
            "components": components,
        }
    )


_SPACK_MANIFEST_LINE = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)$")


def _parse_spack_configuration_manifest(payload: bytes) -> list[tuple[str, str]]:
    """Parse one strict, sorted GNU sha256sum manifest within fixed limits."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RelayError("configuration manifest must be UTF-8") from exc
    if not text.endswith("\n") or "\x00" in text:
        raise RelayError("configuration manifest must be newline-terminated text")
    declarations: list[tuple[str, str]] = []
    for line in text.splitlines():
        match = _SPACK_MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise RelayError("configuration manifest contains an invalid sha256sum line")
        relative_path = match.group(2)
        if not _is_canonical_spack_component_relative_path(relative_path):
            raise RelayError("configuration manifest contains an unsafe component path")
        declarations.append((match.group(1), relative_path))
    paths = [relative_path for _digest, relative_path in declarations]
    if not 1 <= len(paths) <= MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS:
        raise RelayError("configuration manifest component count is outside its bound")
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise RelayError("configuration manifest component paths must be unique and sorted")
    return declarations


def _is_canonical_spack_component_relative_path(value: str) -> bool:
    """Return whether a manifest component is canonical and safely relative."""
    if (
        not value
        or len(value) > 1_024
        or value.startswith("/")
        or value == "."
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _safe_spack_configuration_component_path(base: Path, relative_path: str) -> Path:
    """Resolve a validated component while rejecting symlinked in-root parents."""
    if not _is_canonical_spack_component_relative_path(relative_path):
        raise RelayError("configuration component path is unsafe")
    current = base
    parts = PurePosixPath(relative_path).parts
    for part in parts[:-1]:
        current /= part
        _require_regular_nonsymlink_directory(
            current,
            label=f"configuration component parent {relative_path}",
        )
    return base.joinpath(*parts)


def _require_exact_spack_configuration_component_set(
    base: Path,
    declarations: list[tuple[str, str]],
) -> None:
    """Reject unmanifested files or symlinks in every covered configuration tree."""
    declared_paths = {relative_path for _digest, relative_path in declarations}
    covered_directories = sorted(
        {PurePosixPath(path).parts[0] for path in declared_paths if "/" in path}
    )
    observed_paths: set[str] = set()
    observed_entries = 0
    for relative_directory in covered_directories:
        directory = base / relative_directory
        _require_regular_nonsymlink_directory(
            directory,
            label=f"configuration tree {relative_directory}",
        )
        observed_entries += 1
        if observed_entries > MAX_SPACK_CONFIGURATION_TREE_ENTRIES:
            raise RelayError("configuration tree entry count exceeded its bound")
        pending = [directory]
        while pending:
            current = pending.pop()
            try:
                entries = os.scandir(current)
            except OSError as exc:
                raise RelayError(f"configuration tree is unavailable: {current}") from exc
            with entries:
                for entry in entries:
                    observed_entries += 1
                    if observed_entries > MAX_SPACK_CONFIGURATION_TREE_ENTRIES:
                        raise RelayError("configuration tree entry count exceeded its bound")
                    candidate = Path(entry.path)
                    relative_path = candidate.relative_to(base).as_posix()
                    if not _is_canonical_spack_component_relative_path(relative_path):
                        raise RelayError(
                            f"configuration tree entry has an unsafe path: {candidate}"
                        )
                    try:
                        metadata = candidate.lstat()
                    except OSError as exc:
                        raise RelayError(
                            f"configuration tree entry is unavailable: {candidate}"
                        ) from exc
                    if stat.S_ISLNK(metadata.st_mode):
                        raise RelayError(
                            f"configuration tree entry must not be a symbolic link: {candidate}"
                        )
                    if stat.S_ISDIR(metadata.st_mode):
                        pending.append(candidate)
                        continue
                    if not stat.S_ISREG(metadata.st_mode):
                        raise RelayError(
                            f"configuration tree entry must be a regular file: {candidate}"
                        )
                    observed_paths.add(relative_path)
    expected_covered_paths = {
        path for path in declared_paths if PurePosixPath(path).parts[0] in covered_directories
    }
    if observed_paths != expected_covered_paths:
        raise RelayError(
            "configuration tree files do not exactly match the bounded manifest: "
            f"missing={sorted(expected_covered_paths - observed_paths)} "
            f"unexpected={sorted(observed_paths - expected_covered_paths)}"
        )


def _require_regular_nonsymlink_directory(path: Path, *, label: str) -> None:
    """Require one existing directory without following its final path entry."""
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RelayError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RelayError(f"{label} must be a non-symlink directory: {path}")


def _read_bounded_regular_nonsymlink_file(
    path: Path,
    *,
    maximum_bytes: int,
    label: str,
    require_nonempty: bool,
) -> tuple[bytes, int]:
    """Read one stable regular file through a no-follow descriptor within a byte cap."""
    nofollow = cast(int | None, getattr(os, "O_NOFOLLOW", None))
    if nofollow is None:
        raise RelayError(f"{label} cannot be verified without O_NOFOLLOW support")
    flags = os.O_RDONLY | nofollow | cast(int, getattr(os, "O_CLOEXEC", 0))
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RelayError(f"{label} is unavailable or is a symbolic link: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RelayError(f"{label} must be a regular file: {path}")
        if before.st_size > maximum_bytes or (require_nonempty and before.st_size < 1):
            raise RelayError(f"{label} size is outside its bound: {path}")
        chunks: list[bytes] = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum_bytes:
                raise RelayError(f"{label} exceeded its byte bound while reading: {path}")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        stable_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) == (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if not stable_identity or observed != before.st_size:
            raise RelayError(f"{label} changed while it was observed: {path}")
        return b"".join(chunks), observed
    finally:
        os.close(descriptor)


def _remote_spack_configuration_observer_script() -> str:
    """Return the bounded POSIX observer executed by remote ``bash -lc``."""
    return r"""
import hashlib
import json
import os
import posixpath
import re
import stat
import sys

phase, manifest_path, expected_sha = sys.argv[1:4]
max_manifest, max_components, max_component, max_tree_entries = map(int, sys.argv[4:8])
line_pattern = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)$")

def safe_relative(value):
    return (
        bool(value)
        and len(value) <= 1024
        and not value.startswith("/")
        and value != "."
        and ".." not in value.split("/")
        and posixpath.normpath(value) == value
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )

def require_directory(path):
    metadata = os.lstat(path)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"not a non-symlink directory: {path}")

def read_regular(path, maximum, nonempty, retain_bytes=False):
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise RuntimeError("O_NOFOLLOW is unavailable")
    descriptor = os.open(path, os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0))
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError(f"not a regular file: {path}")
        if before.st_size > maximum or (nonempty and before.st_size < 1):
            raise RuntimeError(f"file size outside bound: {path}")
        digest = hashlib.sha256()
        chunks = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum:
                raise RuntimeError(f"file exceeded bound while reading: {path}")
            digest.update(chunk)
            if retain_bytes:
                chunks.append(chunk)
        after = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or observed != before.st_size
        ):
            raise RuntimeError(f"file changed while observed: {path}")
        return digest.hexdigest(), observed, b"".join(chunks)
    finally:
        os.close(descriptor)

base = posixpath.dirname(manifest_path)
require_directory(base)
manifest_sha, manifest_size, manifest_bytes = read_regular(
    manifest_path, max_manifest, True, retain_bytes=True
)
if manifest_sha != expected_sha:
    raise RuntimeError("configuration manifest SHA-256 mismatch")
text = manifest_bytes.decode("utf-8")
if not text.endswith("\n") or "\x00" in text:
    raise RuntimeError("configuration manifest is not newline-terminated text")
declarations = []
for line in text.splitlines():
    match = line_pattern.fullmatch(line)
    if match is None or not safe_relative(match.group(2)):
        raise RuntimeError("configuration manifest line is invalid")
    declarations.append((match.group(1), match.group(2)))
paths = [relative_path for _digest, relative_path in declarations]
if not 1 <= len(paths) <= max_components:
    raise RuntimeError("configuration component count is outside its bound")
if paths != sorted(paths) or len(paths) != len(set(paths)):
    raise RuntimeError("configuration component paths must be unique and sorted")
declared_paths = set(paths)
covered_directories = sorted({path.split("/", 1)[0] for path in paths if "/" in path})
observed_paths = set()
observed_entries = 0
for relative_directory in covered_directories:
    directory = posixpath.join(base, relative_directory)
    require_directory(directory)
    observed_entries += 1
    if observed_entries > max_tree_entries:
        raise RuntimeError("configuration tree entry count exceeded its bound")
    pending = [directory]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                observed_entries += 1
                if observed_entries > max_tree_entries:
                    raise RuntimeError("configuration tree entry count exceeded its bound")
                candidate = entry.path
                relative_path = posixpath.relpath(candidate, base)
                if not safe_relative(relative_path):
                    raise RuntimeError(f"configuration tree entry has an unsafe path: {candidate}")
                metadata = os.lstat(candidate)
                if stat.S_ISLNK(metadata.st_mode):
                    raise RuntimeError(
                        f"configuration tree entry must not be a symbolic link: {candidate}"
                    )
                if stat.S_ISDIR(metadata.st_mode):
                    pending.append(candidate)
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    raise RuntimeError(
                        f"configuration tree entry must be a regular file: {candidate}"
                    )
                observed_paths.add(relative_path)
expected_covered_paths = {
    path for path in declared_paths if path.split("/", 1)[0] in covered_directories
}
if observed_paths != expected_covered_paths:
    missing = sorted(expected_covered_paths - observed_paths)
    unexpected = sorted(observed_paths - expected_covered_paths)
    raise RuntimeError(
        "configuration tree files do not exactly match the bounded manifest: "
        f"missing={missing} unexpected={unexpected}"
    )
components = []
for declared_sha, relative_path in declarations:
    current = base
    parts = relative_path.split("/")
    for part in parts[:-1]:
        current = posixpath.join(current, part)
        require_directory(current)
    component_path = posixpath.join(base, *parts)
    observed_sha, observed_size, _unused = read_regular(component_path, max_component, False)
    if observed_sha != declared_sha:
        raise RuntimeError(f"configuration component SHA-256 mismatch: {relative_path}")
    components.append({
        "relative_path": relative_path,
        "sha256": observed_sha,
        "size_bytes": observed_size,
        "regular_file": True,
    })
print(json.dumps({
    "schema_version": "clio-relay.spack-configuration-observation.v1",
    "phase": phase,
    "manifest_path": manifest_path,
    "manifest_sha256": manifest_sha,
    "manifest_size_bytes": manifest_size,
    "manifest_regular_file": True,
    "components": components,
}, sort_keys=True, separators=(",", ":")))
""".strip()
