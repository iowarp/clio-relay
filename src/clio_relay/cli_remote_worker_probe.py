"""Remote worker identity and SSH host-key probing (iowarp/clio-relay#231
continuation): resolves a remote worker's identity and fingerprints
its SSH host keys for the acceptance/validation command bodies."""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
import subprocess
import time
from pathlib import Path
from typing import Literal

import clio_relay.cli_remote_collection_pagination as cli_remote_collection_pagination
import clio_relay.remote_channel_dispatch as remote_channel_dispatch
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import ConfigurationError, ObservationTimeoutError, RelayError


def _remote_worker_info(
    definition: ClusterDefinition,
    *,
    timeout_seconds: float | None = None,
) -> dict[str, object]:
    """Read fresh process-bound worker identity over one optional total deadline.

    When the cluster registry pins a runtime (``relay_install_receipt``), that
    pin is threaded to the remote check so the worker is verified against its
    own cluster's declared identity rather than only this SSH session's
    ambient current installation (clio-relay#205).
    """
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
    args = ["endpoint", "worker-info", "--cluster", definition.name]
    if definition.relay_install_receipt is not None:
        args = [*args, "--pinned-install-receipt-path", definition.relay_install_receipt]
    if definition.dev_mode:
        args = [*args, "--dev-mode"]
    query: dict[str, object] = {"cluster": definition.name}
    if definition.relay_install_receipt is not None:
        query["pinned_install_receipt_path"] = definition.relay_install_receipt
    if definition.dev_mode:
        query["dev_mode"] = True
    info = cli_remote_collection_pagination._json_output(
        remote_channel_dispatch.dial_or_route_string_ambient(
            definition=definition,
            operation="remote_worker_info",
            method="GET",
            path="/worker-info",
            query=query,
            ssh_fallback=lambda: _run_remote_clio_before_deadline(
                definition,
                args,
                deadline=deadline,
            ),
        ),
        "remote clio-relay worker runtime info",
    )
    actual_provider = info.get("scheduler_provider")
    if actual_provider != definition.scheduler_provider:
        raise ConfigurationError(
            "remote worker scheduler provider does not match the cluster definition: "
            f"{actual_provider!r} != {definition.scheduler_provider!r}"
        )
    info["target_identity"] = _remote_target_identity(definition, deadline=deadline)
    return info


def _run_remote_clio_before_deadline(
    definition: ClusterDefinition,
    args: list[str],
    *,
    deadline: float | None,
) -> str:
    """Run one remote observation without exceeding a shared monotonic deadline."""
    if deadline is None:
        return remote_cli.run_remote_clio(definition, args)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ObservationTimeoutError("remote worker identity observation timed out")
    with remote_cli.remote_command_timeout(remaining):
        return remote_cli.run_remote_clio(definition, args)


def _remote_target_identity(
    definition: ClusterDefinition,
    *,
    deadline: float | None = None,
) -> dict[str, object]:
    """Verify and return one operator-pinned physical cluster identity."""
    target = definition.target_identity
    if target is None:
        raise ConfigurationError(
            f"cluster {definition.name} has no operator-pinned target_identity"
        )
    remote_target = cli_remote_collection_pagination._json_output(
        remote_channel_dispatch.dial_or_route_string_ambient(
            definition=definition,
            operation="remote_target_identity",
            method="GET",
            path="/target-info",
            query={"scheduler_provider": definition.scheduler_provider},
            ssh_fallback=lambda: _run_remote_clio_before_deadline(
                definition,
                [
                    "endpoint",
                    "target-info",
                    "--scheduler-provider",
                    definition.scheduler_provider,
                ],
                deadline=deadline,
            ),
        ),
        "remote physical cluster target info",
    )
    if remote_target.get("schema_version") != "clio-relay.cluster-target-info.v1":
        raise ConfigurationError("remote physical target identity schema does not match")
    if remote_target.get("scheduler_provider") != definition.scheduler_provider:
        raise ConfigurationError(
            "remote physical target scheduler provider does not match the cluster definition"
        )
    observed_hostnames = {
        value
        for key in ("hostname", "fqdn")
        if isinstance((value := remote_target.get(key)), str) and value
    }
    if not observed_hostnames.intersection(target.hostnames):
        raise ConfigurationError(
            "remote hostname does not match the operator-pinned cluster identity: "
            f"observed={sorted(observed_hostnames)!r} expected={target.hostnames!r}"
        )
    if (
        target.site_marker_sha256 is not None
        and remote_target.get("site_marker_sha256") != target.site_marker_sha256
    ):
        raise ConfigurationError("remote site marker does not match cluster target identity")
    if (
        target.scheduler_cluster_name is not None
        and remote_target.get("scheduler_cluster_name") != target.scheduler_cluster_name
    ):
        raise ConfigurationError("scheduler-native cluster name does not match target identity")
    fingerprints = (
        _ssh_host_key_fingerprints(definition.ssh_host)
        if deadline is None
        else _ssh_host_key_fingerprints(definition.ssh_host, deadline=deadline)
    )
    if not set(fingerprints).intersection(target.ssh_host_key_sha256):
        raise ConfigurationError(
            "live SSH host keys do not match the operator-pinned cluster target identity"
        )
    return {
        **remote_target,
        "ssh_host": definition.ssh_host,
        "ssh_host_key_sha256": fingerprints,
        "expected_hostnames": target.hostnames,
        "expected_ssh_host_key_sha256": target.ssh_host_key_sha256,
        "expected_scheduler_cluster_name": target.scheduler_cluster_name,
        "expected_site_marker_sha256": target.site_marker_sha256,
        "verified": True,
    }


FingerprintSource = Literal["known_hosts", "keyscan_fallback"]


def _ssh_host_key_fingerprints(
    ssh_host: str,
    *,
    deadline: float | None = None,
) -> list[str]:
    """Return trusted SHA-256 host-key fingerprints for a configured SSH target."""
    fingerprints, _source = _ssh_host_key_fingerprints_with_trust_source(
        ssh_host, deadline=deadline
    )
    return fingerprints


def _ssh_host_key_fingerprints_with_trust_source(
    ssh_host: str,
    *,
    deadline: float | None = None,
) -> tuple[list[str], FingerprintSource]:
    """Same as :func:`_ssh_host_key_fingerprints`, plus where they came from.

    clio-relay#209 H3(c): the two sources carry very different trust
    weight. ``"known_hosts"`` means the operator's own ssh client already
    accepted these keys at some prior connection (an entry in
    ``~/.ssh/known_hosts`` or an equivalent configured file) -- that
    acceptance already happened, outside this process, and this call is
    just reading it back. ``"keyscan_fallback"`` means no matching
    known_hosts entry existed and these were observed fresh via a bare
    ``ssh-keyscan``, which does NOT authenticate the host at all -- this is
    the only genuinely un-verified case, and callers pinning a fresh
    identity must say so out loud rather than reporting both sources as
    equally trustworthy "verified" evidence.
    """
    resolved_host = ssh_host
    resolved_port = "22"
    host_key_alias: str | None = None
    known_hosts_files: list[str] = []
    diagnostics: list[str] = []
    try:
        config = subprocess.run(
            ["ssh", "-G", ssh_host],
            capture_output=True,
            text=True,
            check=False,
            timeout=_remote_observation_subprocess_timeout(10, deadline=deadline),
        )
    except subprocess.TimeoutExpired:
        diagnostics.append("ssh -G timed out")
    except OSError as exc:
        diagnostics.append(f"ssh -G failed: {exc}")
    else:
        if config.returncode != 0:
            diagnostics.append(config.stderr.strip() or f"ssh -G exited {config.returncode}")
        else:
            for line in config.stdout.splitlines():
                fields = line.split(maxsplit=1)
                if len(fields) != 2:
                    continue
                key, value = fields[0].casefold(), fields[1].strip()
                if key == "hostname" and value:
                    resolved_host = value
                elif key == "port" and value:
                    resolved_port = value
                elif key == "hostkeyalias" and value:
                    host_key_alias = value
                elif key == "userknownhostsfile" and value:
                    known_hosts_files.extend(_split_ssh_config_values(value))

    lookup_host = host_key_alias or resolved_host
    if resolved_port != "22":
        lookup_host = f"[{lookup_host}]:{resolved_port}"
    fingerprints: set[str] = set()
    for configured_path in known_hosts_files:
        if configured_path.casefold() == "none":
            continue
        known_hosts_path = Path(os.path.expandvars(os.path.expanduser(configured_path)))
        try:
            found = subprocess.run(
                ["ssh-keygen", "-F", lookup_host, "-f", str(known_hosts_path)],
                capture_output=True,
                text=True,
                check=False,
                timeout=_remote_observation_subprocess_timeout(10, deadline=deadline),
            )
        except subprocess.TimeoutExpired:
            diagnostics.append(f"ssh-keygen timed out for {known_hosts_path}")
            continue
        except OSError as exc:
            diagnostics.append(f"ssh-keygen failed for {known_hosts_path}: {exc}")
            break
        fingerprints.update(_ssh_fingerprints_from_key_lines(found.stdout))
    if fingerprints:
        return sorted(fingerprints), "known_hosts"

    try:
        scanned = subprocess.run(
            ["ssh-keyscan", "-T", "10", "-p", resolved_port, resolved_host],
            capture_output=True,
            text=True,
            check=False,
            timeout=_remote_observation_subprocess_timeout(15, deadline=deadline),
        )
    except subprocess.TimeoutExpired:
        diagnostics.append("ssh-keyscan timed out")
        scanned = None
    except OSError as exc:
        diagnostics.append(f"ssh-keyscan failed: {exc}")
        scanned = None
    if scanned is not None:
        fingerprints.update(_ssh_fingerprints_from_key_lines(scanned.stdout))
        if scanned.returncode != 0:
            diagnostics.append(scanned.stderr.strip() or f"ssh-keyscan exited {scanned.returncode}")
    if not fingerprints:
        detail = "; ".join(item for item in diagnostics if item) or "no host keys returned"
        raise ConfigurationError(f"could not observe SSH host keys for {ssh_host}: {detail}")
    return sorted(fingerprints), "keyscan_fallback"


def _remote_observation_subprocess_timeout(
    default_seconds: float,
    *,
    deadline: float | None,
) -> float:
    """Return a positive subprocess timeout inside one shared observation budget."""
    if deadline is None:
        return default_seconds
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ConfigurationError("remote worker identity observation timed out")
    return min(default_seconds, remaining)


def _split_ssh_config_values(value: str) -> list[str]:
    """Split an ``ssh -G`` multi-value while preserving Windows path separators."""
    values: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(value):
        character = value[index]
        if quote is not None:
            if character == quote:
                quote = None
            elif character == "\\" and index + 1 < len(value) and value[index + 1] == quote:
                index += 1
                current.append(value[index])
            else:
                current.append(character)
        elif character in {'"', "'"}:
            quote = character
        elif (
            character == "\\"
            and index + 1 < len(value)
            and (value[index + 1].isspace() or value[index + 1] in {'"', "'"})
        ):
            index += 1
            current.append(value[index])
        elif character.isspace():
            if current:
                values.append("".join(current))
                current = []
        else:
            current.append(character)
        index += 1
    if current:
        values.append("".join(current))
    return values


def _ssh_fingerprints_from_key_lines(output: str) -> set[str]:
    """Decode public-key records emitted by ``ssh-keygen`` or ``ssh-keyscan``."""
    fingerprints: set[str] = set()
    for line in output.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        marker_offset = 1 if fields and fields[0].startswith("@") else 0
        if marker_offset and fields[0].casefold() == "@revoked":
            continue
        if len(fields) < marker_offset + 3:
            continue
        try:
            key_bytes = base64.b64decode(fields[marker_offset + 2], validate=True)
        except (binascii.Error, ValueError):
            continue
        digest = base64.b64encode(hashlib.sha256(key_bytes).digest()).decode().rstrip("=")
        fingerprints.add(f"SHA256:{digest}")
    return fingerprints


def _last_nonempty_line(value: str) -> str:
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    if not lines:
        raise RelayError("remote MCP discovery submission did not return a job id")
    return lines[-1]
