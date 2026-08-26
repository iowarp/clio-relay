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

**Timeout derivation (adversarial review D3).** ``install_frpc_proxy_over_ssh``
runs the SAME ``clio_relay_endpoint_activate_bounded`` observer the worker
endpoint unit's install path does, whose default budget is
``deployment_activation.ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS``
(330s). A local ssh-level timeout SHORTER than that observer's own bound
kills the ssh session mid-script in exactly the slow case the observer
exists to ride out -- leaving unbounded partial remote state and no receipt
at all. ``FRPC_PROXY_INSTALL_SSH_TIMEOUT_SECONDS`` reuses
``deployment_activation.ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS`` (420s = the
observer's 330s plus the SAME 90s setup margin the worker precedent uses)
as ``install_frpc_proxy_over_ssh``'s default, rather than the shorter
120s default that remains correct for teardown/status (neither rides the
slow activation observer).

**Active-after-install re-check (adversarial review D6, Python layer).**
``install_frpc_proxy_over_ssh`` never trusts ``rc == 0`` plus a parsed
receipt alone: even though the install script's own
``if [ "$service_active" != "active" ]`` gate (``frpc_proxy_scripts.py``)
already refuses to print a success receipt for an inactive unit, this is a
SECOND, independent check on the exact same fact -- if a receipt somehow
carries ``active=false`` (an older script, a future edit that weakens the
shell-side gate), this raises a typed error rather than returning a
misleadingly "successful" receipt, with the receipt's own content folded
into the message so the journal pointer survives.
"""

from __future__ import annotations

import math
import subprocess
from typing import Final

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.deployment_activation import ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS
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
"""Bound for teardown/status: neither rides the slow activation observer."""

FRPC_PROXY_INSTALL_SSH_TIMEOUT_SECONDS: Final = ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS
"""Bound for install: MUST exceed the reused activation observer's own bound (D3)."""

DEFAULT_FRPC_PROXY_REMOTE_PORT: Final = 8765


def install_frpc_proxy_over_ssh(
    *,
    cluster: str,
    definition: ClusterDefinition,
    ssh_host: str,
    remote_port: int = DEFAULT_FRPC_PROXY_REMOTE_PORT,
    local_ip: str = "127.0.0.1",
    frpc_bin: str = "%h/.local/bin/frpc",
    require_persistent: bool = True,
    timeout_seconds: float = FRPC_PROXY_INSTALL_SSH_TIMEOUT_SECONDS,
) -> FrpcProxyBringupReceipt:
    """Render, install, enable, and start the cluster's frpc proxy in ONE ssh pass.

    Raises a typed :class:`RelayError` -- never returns a "successful"
    receipt -- when the parsed receipt reports the unit as not genuinely
    active (D6's Python-layer re-check; see module docstring).
    """
    _validate_ssh_destination(ssh_host)
    paths = frpc_proxy_paths(cluster)
    proxy_name = canonical_proxy_name(definition, cluster=cluster)
    transport = definition.frp_transport
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
        token_env=transport.token_env,
        secret_env=transport.stcp_secret_env,
        require_persistent=require_persistent,
    )
    lines = _run_frpc_proxy_script(
        ssh_host, script, timeout_seconds=timeout_seconds, operation="install"
    )
    receipt = parse_frpc_proxy_bringup_receipt(lines)
    if not receipt.active:
        raise RelayError(
            "frpc proxy install reported success but the receipt shows the unit is "
            f"not active for cluster {cluster!r}; diagnose with `clio-relay relay-host "
            f"proxy-status --cluster {cluster}` -- receipt: {receipt.model_dump_json()}"
        )
    return receipt


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
