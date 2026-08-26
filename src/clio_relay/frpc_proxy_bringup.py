"""Run the cluster-side frpc proxy bring-up/teardown/status, one ssh pass each.

clio-relay#279. Composes ``frpc_unit.py``'s pure renderers +
``frpc_proxy_scripts.py``'s pure script text into ONE ``ssh <host> bash -s``
dial per operation -- never more, matching every other transport operation's
budget in ``docs/connection-model.md`` -- and parses the typed framed
receipt each script prints via ``frpc_proxy_receipt.py``. Mirrors
``deployment_ssh.py``/``endpoint_service_status.py``'s own split almost
exactly: this module owns the ssh/subprocess boundary, the two sibling
modules above own composition and the wire contract.

Nothing here is exercised by ssh in this repository's own tests (per
clio-relay#279's environment constraints: no ssh anywhere in CI). Every test
targets script composition directly, or these functions with
``subprocess.run`` replaced by a fake -- exactly like
``deployment_ssh.py``/``endpoint_service_status.py``'s own test siblings and
``test_bootstrap_preflight_transport.py``'s harness pattern.
"""

from __future__ import annotations

import math
import subprocess
from typing import Final

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.frp_proxy_naming import canonical_proxy_name
from clio_relay.frpc_proxy_receipt import (
    FrpcProxyBringupReceipt,
    FrpcProxyStatusDocument,
    FrpcProxyTeardownReceipt,
    build_frpc_proxy_status_document,
    parse_frpc_proxy_bringup_receipt,
    parse_frpc_proxy_status_properties,
    parse_frpc_proxy_teardown_receipt,
)
from clio_relay.frpc_proxy_scripts import (
    render_frpc_proxy_install_script,
    render_frpc_proxy_status_script,
    render_frpc_proxy_teardown_script,
)
from clio_relay.frpc_unit import (
    frpc_proxy_paths,
    render_frpc_proxy_env_file,
    render_frpc_proxy_toml,
    render_frpc_proxy_unit,
)

FRPC_PROXY_SSH_TIMEOUT_SECONDS: Final = 120.0
DEFAULT_FRPC_PROXY_REMOTE_PORT: Final = 8765


def install_frpc_proxy_over_ssh(
    *,
    cluster: str,
    definition: ClusterDefinition,
    ssh_host: str,
    remote_port: int = DEFAULT_FRPC_PROXY_REMOTE_PORT,
    local_ip: str = "127.0.0.1",
    frpc_bin: str = "%h/.local/bin/frpc",
    timeout_seconds: float = FRPC_PROXY_SSH_TIMEOUT_SECONDS,
) -> FrpcProxyBringupReceipt:
    """Render, install, enable, and start the cluster's frpc proxy in ONE ssh pass."""
    _validate_ssh_destination(ssh_host)
    paths = frpc_proxy_paths(cluster)
    proxy_name = canonical_proxy_name(definition, cluster=cluster)
    toml_text = render_frpc_proxy_toml(
        definition, cluster=cluster, local_port=remote_port, local_ip=local_ip
    )
    env_text = render_frpc_proxy_env_file(definition, cluster=cluster)
    unit_text = render_frpc_proxy_unit(cluster=cluster, paths=paths, frpc_bin=frpc_bin)
    script = render_frpc_proxy_install_script(
        cluster=cluster,
        proxy_name=proxy_name,
        paths=paths,
        toml_text=toml_text,
        env_text=env_text,
        unit_text=unit_text,
    )
    lines = _run_frpc_proxy_script(
        ssh_host, script, timeout_seconds=timeout_seconds, operation="install"
    )
    return parse_frpc_proxy_bringup_receipt(lines)


def teardown_frpc_proxy_over_ssh(
    *,
    cluster: str,
    ssh_host: str,
    timeout_seconds: float = FRPC_PROXY_SSH_TIMEOUT_SECONDS,
) -> FrpcProxyTeardownReceipt:
    """Disable, stop, and remove the cluster's frpc proxy unit in ONE ssh pass."""
    _validate_ssh_destination(ssh_host)
    paths = frpc_proxy_paths(cluster)
    script = render_frpc_proxy_teardown_script(cluster=cluster, paths=paths)
    lines = _run_frpc_proxy_script(
        ssh_host, script, timeout_seconds=timeout_seconds, operation="teardown"
    )
    return parse_frpc_proxy_teardown_receipt(lines)


def frpc_proxy_status_over_ssh(
    *,
    cluster: str,
    ssh_host: str,
    timeout_seconds: float = FRPC_PROXY_SSH_TIMEOUT_SECONDS,
) -> FrpcProxyStatusDocument:
    """Read the cluster's frpc proxy unit's diagnosable status in ONE ssh pass."""
    _validate_ssh_destination(ssh_host)
    paths = frpc_proxy_paths(cluster)
    script = render_frpc_proxy_status_script(unit_name=paths.unit_name)
    lines = _run_frpc_proxy_script(
        ssh_host, script, timeout_seconds=timeout_seconds, operation="status"
    )
    properties = parse_frpc_proxy_status_properties("\n".join(lines))
    return build_frpc_proxy_status_document(
        cluster=cluster, unit_name=paths.unit_name, properties=properties
    )


def _run_frpc_proxy_script(
    ssh_host: str,
    script: str,
    *,
    timeout_seconds: float,
    operation: str,
) -> list[str]:
    """Run one bounded ``ssh <host> bash -s`` pass and return its stdout lines."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("frpc proxy operation timeout must be finite and positive")
    try:
        result = subprocess.run(
            ["ssh", ssh_host, "bash", "-s"],
            input=script.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(f"frpc proxy {operation} exceeded {timeout_seconds:g} seconds") from exc
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")
        stdout = result.stdout.decode("utf-8", errors="replace")
        detail = stderr.strip() or stdout.strip()
        raise RelayError(f"failed to {operation} frpc proxy: {detail}")
    return result.stdout.decode("utf-8", errors="replace").splitlines()


def _validate_ssh_destination(value: str) -> None:
    """Reject destinations ssh could interpret as options or multiple tokens.

    Duplicated from ``deployment_ssh.py``/``endpoint_service_status.py``,
    which already each carry their own copy of this exact check rather than
    sharing one -- this module follows that same established precedent
    instead of introducing a new shared dependency for it.
    """
    if (
        not value
        or value != value.strip()
        or value.startswith("-")
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise RelayError(
            "ssh host must be one non-option destination without whitespace or controls"
        )
