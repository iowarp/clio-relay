"""Remote shell/artifact IO primitives for the live acceptance runner.

Extracted from ``live_acceptance.py`` (#231 rework): the leaf helpers that
run one bounded remote command over the injected :class:`CommandRunner`,
decode its typed T2 delivery-refusal/artifact envelopes (doc §6.4), and
drain the exact clio-relay CLI job-family page chains acceptance evidence
depends on. Every function here is a pure IO/decoding primitive -- none of
them decide acceptance policy, they only surface what the remote side said.
"""

from __future__ import annotations

import json
import posixpath
import shlex
import subprocess
from base64 import b64decode
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import yaml

from clio_relay.bounded_payload import (
    describe_delivery_refusal,
    is_delivery_refusal,
    parse_delivery_refusal,
)
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.live_acceptance_models import (
    MAX_ACCEPTANCE_COLLECTION_RECORDS,
    CommandRunner,
)
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS
from clio_relay.remote_values import render_remote_shell_path, render_remote_shell_value


def _delivery_refusal_error(payload: Mapping[str, Any], *, label: str) -> RelayError:
    """Build the typed error for a T2 refusal (doc §6.4) an acceptance read hit.

    F5 (#231 R6 review): a refusal is not a malformed/missing-encoding
    envelope -- its own ``delivery.message``/``code`` is the accurate,
    typed reason a caller should report, not a generic "not base64
    encoded" that misdescribes why the artifact is unavailable.
    """
    # A2 (#231 R6 review): the message extraction itself now delegates to
    # bounded_payload.describe_delivery_refusal, the single owner.
    code = cast(dict[str, object], payload.get("delivery", {})).get("code")
    return RelayError(f"{label} delivery refused ({code}): {describe_delivery_refusal(payload)}")


def _decode_artifact_text(payload: dict[str, Any]) -> str:
    if is_delivery_refusal(payload):
        raise _delivery_refusal_error(payload, label="acceptance artifact payload")
    if payload.get("encoding") != "base64":
        raise RelayError("acceptance artifact payload was not base64 encoded")
    data = payload.get("data")
    if not isinstance(data, str):
        raise RelayError("acceptance artifact payload missing base64 data")
    return b64decode(data.encode("ascii")).decode("utf-8", errors="replace")


def _stage_acceptance_files(
    definition: ClusterDefinition,
    *,
    jarvis_yaml: Path,
    pipeline_yaml_text: str,
    run_id: str,
    runner: CommandRunner,
    write_remote: bool = True,
) -> str:
    loaded = cast(object, yaml.safe_load(pipeline_yaml_text))
    if not isinstance(loaded, dict):
        return pipeline_yaml_text
    document = cast(dict[str, object], loaded)
    relay_extension = document.pop("x_clio_relay", None)
    if relay_extension is None:
        return yaml.safe_dump(document, sort_keys=False)
    if not isinstance(relay_extension, dict):
        raise ConfigurationError("x_clio_relay must be an object")
    typed_extension = cast(dict[str, object], relay_extension)
    stage_files = typed_extension.get("stage_files", [])
    if not isinstance(stage_files, list):
        raise ConfigurationError("x_clio_relay.stage_files must be a list")
    for item in cast(list[object], stage_files):
        if not isinstance(item, dict):
            raise ConfigurationError("x_clio_relay.stage_files entries must be objects")
        typed_item = cast(dict[str, object], item)
        local_path_value = typed_item.get("local_path")
        remote_path_value = typed_item.get("remote_path")
        if not isinstance(local_path_value, str) or not isinstance(remote_path_value, str):
            raise ConfigurationError(
                "x_clio_relay.stage_files entries require local_path and remote_path strings"
            )
        local_path = Path(local_path_value)
        if not local_path.is_absolute():
            local_path = jarvis_yaml.parent / local_path
        if not local_path.exists():
            raise ConfigurationError(f"staged acceptance file does not exist: {local_path}")
        remote_path = remote_path_value.format(run_id=run_id)
        if write_remote:
            _remote_write_file(
                definition.ssh_host,
                remote_path,
                local_path.read_bytes(),
                runner=runner,
            )
    formatted_document = _format_run_id(document, run_id)
    return yaml.safe_dump(formatted_document, sort_keys=False)


def _format_run_id(value: object, run_id: str) -> object:
    if isinstance(value, str):
        return value.format(run_id=run_id)
    if isinstance(value, list):
        return [_format_run_id(item, run_id) for item in cast(list[object], value)]
    if isinstance(value, dict):
        typed = cast(dict[object, object], value)
        return {str(key): _format_run_id(item, run_id) for key, item in typed.items()}
    return value


def _remote_write_file(
    ssh_host: str,
    remote_path: str,
    data: bytes,
    *,
    runner: CommandRunner,
) -> None:
    mkdir_command = f"mkdir -p {shlex.quote(posixpath.dirname(remote_path))}"
    _remote_shell(ssh_host, mkdir_command, runner=runner)
    result = runner(["ssh", ssh_host, f"cat > {shlex.quote(remote_path)}"], input=data)
    if result.returncode != 0:
        raise RelayError(_command_error("remote file write failed", result))


def _remote_clio_json(
    definition: ClusterDefinition,
    args: list[str],
    *,
    runner: CommandRunner,
    raw_text: bool = False,
) -> Any:
    rendered_args = " ".join(shlex.quote(arg) for arg in args)
    output = _remote_shell(
        definition.ssh_host,
        f"{_remote_env(definition)} clio-relay {rendered_args}",
        runner=runner,
    )
    if raw_text:
        return output
    return json.loads(output)


def _remote_job_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    runner: CommandRunner,
) -> list[dict[str, Any]]:
    """Drain an exact job-family page chain or reject incomplete acceptance evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        raw_payload = _remote_clio_json(
            definition,
            [
                *command,
                "--cursor",
                str(cursor),
                "--limit",
                str(MAX_RESPONSE_PAGE_RECORDS),
            ],
            runner=runner,
        )
        if not isinstance(raw_payload, dict):
            raise RelayError(f"{label} did not return a JSON object")
        payload = cast(dict[str, Any], raw_payload)
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        page: list[dict[str, Any]] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            page.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if total > MAX_ACCEPTANCE_COLLECTION_RECORDS:
            raise RelayError(
                f"{label} exceeds the bounded completeness limit "
                f"{MAX_ACCEPTANCE_COLLECTION_RECORDS}"
            )
        if expected_total is not None and total != expected_total:
            raise RelayError(f"{label} changed during bounded discovery")
        expected_total = total
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if len(records) + len(page) > total:
            raise RelayError(f"{label} returned more records than its total")
        if next_cursor is not None and (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or not page
            or next_cursor != cursor + len(page)
            or next_cursor > total
        ):
            raise RelayError(f"{label} returned a non-contiguous next cursor")
        records.extend(page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"{label} ended before its declared total")
            return records
        cursor = next_cursor


def _remote_shell(ssh_host: str, script: str, *, runner: CommandRunner) -> str:
    result = runner(["ssh", ssh_host, f"bash -lc {shlex.quote(script)}"])
    if result.returncode != 0:
        raise _remote_command_failure(result)
    return result.stdout.decode("utf-8", errors="replace")


def _remote_command_failure(result: subprocess.CompletedProcess[bytes]) -> RelayError:
    """Build the typed error for a failed remote command.

    A1 (#231 R6 review): a remote CLI guard already exits non-zero *after*
    printing a T2 delivery-refusal document (doc §6.4) to stdout --
    recognized first, via ``bounded_payload.parse_delivery_refusal``, so
    its own typed code/message surfaces instead of the generic "remote
    command failed: <raw stdout+stderr blob>" that discards the structure.
    """
    refusal = parse_delivery_refusal(result.stdout)
    if refusal is not None:
        code = cast(dict[str, object], refusal.get("delivery", {})).get("code")
        return RelayError(
            f"remote command refused delivery ({code}): {describe_delivery_refusal(refusal)}"
        )
    return RelayError(_command_error("remote command failed", result))


def _remote_env(definition: ClusterDefinition) -> str:
    jarvis_bin = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_bin = _cluster_agent_bin(definition)
    rendered_core_dir = render_remote_shell_path(definition.core_dir, field="core_dir")
    rendered_spool_dir = render_remote_shell_path(definition.spool_dir, field="spool_dir")
    rendered_jarvis_bin = render_remote_shell_value(jarvis_bin, field="jarvis_bin")
    rendered_frpc_bin = render_remote_shell_value(frpc_bin, field="frpc_bin")
    rendered_agent_bin = render_remote_shell_value(agent_bin, field="agent_bin")
    return " ".join(
        [
            'export PATH="$HOME/.local/bin:$PATH";',
            f"export CLIO_RELAY_CORE_DIR={rendered_core_dir};",
            f"export CLIO_RELAY_SPOOL_DIR={rendered_spool_dir};",
            f"export CLIO_RELAY_JARVIS_BIN={rendered_jarvis_bin};",
            f"export CLIO_RELAY_FRPC_BIN={rendered_frpc_bin};",
            f"export CLIO_RELAY_AGENT_BIN={rendered_agent_bin};",
            f"export CLIO_RELAY_AGENT_ADAPTER={shlex.quote(definition.agent_adapter)};",
        ]
    )


def _cluster_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"


def _run_command(
    command: list[str],
    *,
    input: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, input=input, capture_output=True, check=False)


def _command_error(prefix: str, result: subprocess.CompletedProcess[bytes]) -> str:
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    detail = stderr or stdout
    return f"{prefix}: {detail}"
