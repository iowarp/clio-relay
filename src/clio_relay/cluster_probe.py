"""Read-only runtime probe for a configured cluster.

Every other cluster-targeted command reaches the host by dereferencing the
registry's ``relay_executable``. That makes a broken deployment unobservable:
when the pin points at a path that no longer exists, the operator cannot ask
what is wrong without tripping over the very thing that is wrong -- ``session
plan-start``, ``session status`` and the rest all die on the same dead pointer
(clio-relay#158).

This probe runs one plain shell script over SSH. It never invokes the relay,
so it keeps working precisely when the deployment is broken, and it reports a
typed state plus the command that repairs it.
"""

from __future__ import annotations

from typing import cast

from clio_relay.bootstrap_pin import (
    BOOTSTRAP_PRODUCED_INSTALL_RECEIPT,
    BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE,
)
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.remote_cli import remote_command_timeout, run_remote_shell
from clio_relay.remote_values import render_remote_shell_value

CLUSTER_PROBE_SCHEMA = "clio-relay.cluster-probe.v1"

PROBE_TIMEOUT_SECONDS = 30.0
"""Bounded: a probe is recon, so it must fail fast rather than hang."""


def _presence_script(definition: ClusterDefinition) -> str:
    """Render a relay-free presence check for the pinned and produced runtimes.

    Always exits 0 -- absence is an ANSWER, not a command failure, so it must
    not be reported through the non-zero-exit error path.
    """
    checks = {
        "pinned_executable": render_remote_shell_value(
            definition.relay_executable, field="relay_executable"
        ),
        "pinned_receipt": render_remote_shell_value(
            definition.relay_install_receipt or BOOTSTRAP_PRODUCED_INSTALL_RECEIPT,
            field="relay_install_receipt",
        ),
        "produced_executable": render_remote_shell_value(
            BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE, field="relay_executable"
        ),
        "produced_receipt": render_remote_shell_value(
            BOOTSTRAP_PRODUCED_INSTALL_RECEIPT, field="relay_install_receipt"
        ),
    }
    lines = [f"printf 'probe_schema=%s\\n' {CLUSTER_PROBE_SCHEMA}"]
    for name, rendered in checks.items():
        test_flag = "-x" if name.endswith("executable") else "-r"
        lines.append(
            f"if [ {test_flag} {rendered} ]; then printf '{name}_present=true\\n'; "
            f"else printf '{name}_present=false\\n'; fi"
        )
    lines.append("exit 0")
    return "\n".join(lines)


def _parse_presence(output: str) -> dict[str, bool]:
    presence: dict[str, bool] = {}
    for line in output.splitlines():
        key, separator, value = line.strip().partition("=")
        if separator and key.endswith("_present"):
            presence[key.removesuffix("_present")] = value == "true"
    return presence


def pinned_runtime_present(definition: ClusterDefinition) -> bool:
    """Report whether the cluster's CURRENTLY pinned executable exists on the host.

    Used to decide whether a pin may be repaired. Two deliberate shortcuts:

    * A pin that already names the produced runtime needs no round trip.
    * If the host cannot be observed at all, the answer is ``True``. An
      unobservable pin must never be treated as broken -- rewriting one on the
      strength of a failed probe would clobber a working custom deployment
      because the network blinked.
    """
    if definition.relay_executable == BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE:
        return True
    report = probe_cluster_runtime(definition)
    if not report.get("ssh_reachable"):
        return True
    pinned = cast(dict[str, object], report["relay_executable"])
    return bool(pinned["present"])


def probe_cluster_runtime(definition: ClusterDefinition) -> dict[str, object]:
    """Report one cluster's runtime health without invoking the relay.

    Returns a typed record. ``state`` is one of:

    ``ready``
        The pinned runtime exists on the host.
    ``relay_executable_missing``
        The pin is dangling but bootstrap's canonical launcher is present --
        the deployment is installed, the registry just points at the wrong
        place. ``cluster bootstrap`` re-points it.
    ``relay_not_installed``
        Neither the pin nor the canonical launcher exists; the host has no
        relay at all.
    ``ssh_unreachable``
        The host could not be reached, which is a transport fact and a
        distinct state from any install problem.

    Known scope bound: ``ssh_unreachable`` is deliberately coarse. A refused
    connection, a DNS failure, a transport timeout and an authentication
    failure all land here, because OpenSSH reports every one of them as exit
    255 and this probe does not parse its stderr to guess which occurred --
    guessing from prose is exactly the discrimination-by-message this work
    removed elsewhere. ``detail`` carries the underlying message for a human;
    splitting auth from network needs a structured signal ssh does not give us.
    """
    report: dict[str, object] = {
        "schema_version": CLUSTER_PROBE_SCHEMA,
        "cluster": definition.name,
        "ssh_host": definition.ssh_host,
    }
    try:
        with remote_command_timeout(PROBE_TIMEOUT_SECONDS):
            output = run_remote_shell(definition, _presence_script(definition))
    except RelayError as exc:
        report.update(
            {
                "ssh_reachable": False,
                "state": "ssh_unreachable",
                "detail": str(exc),
                "repair": None,
            }
        )
        return report

    presence = _parse_presence(output)
    pinned_executable = presence.get("pinned_executable", False)
    produced_executable = presence.get("produced_executable", False)
    if pinned_executable:
        state = "ready"
    elif produced_executable:
        state = "relay_executable_missing"
    else:
        state = "relay_not_installed"
    repair = (
        None if state == "ready" else f"clio-relay cluster bootstrap --cluster {definition.name}"
    )
    report.update(
        {
            "ssh_reachable": True,
            "state": state,
            "detail": None,
            "repair": repair,
            "relay_executable": {
                "path": definition.relay_executable,
                "present": pinned_executable,
            },
            "relay_install_receipt": {
                "path": definition.relay_install_receipt,
                "present": presence.get("pinned_receipt", False),
            },
            "produced_relay_executable": {
                "path": BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE,
                "present": produced_executable,
            },
            "produced_install_receipt": {
                "path": BOOTSTRAP_PRODUCED_INSTALL_RECEIPT,
                "present": presence.get("produced_receipt", False),
            },
        }
    )
    return report
