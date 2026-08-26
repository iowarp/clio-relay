"""Render the cluster-side frpc proxy's config, secrets file, and systemd unit.

clio-relay#279: the missing cluster-side half of #188's TCP transport modes.
``relay_host.py::render_frpc_config`` (reached here through
``frp_link.py::render_proxy_config``) already renders the frpc proxy TOML;
nothing installed it, ran it, or kept it alive on a cluster. This module
renders the three artifacts one persistent proxy needs, reusing
``frp_link.render_proxy_config`` UNCHANGED -- this module never forks the
TOML rendering:

- The frpc proxy TOML, via ``frp_link.render_proxy_config``, but with
  ``token``/``secret_key`` set to frp's own ``{{ .Envs.NAME }}`` template
  placeholders instead of real secret values. frpc resolves those from its
  OWN process environment at parse time (frp's "Referencing Environment
  Variables in Configuration Files" support), so the rendered TOML text is
  never secret-bearing even though the systemd unit's ``EnvironmentFile=``
  binds the real values. This is the only way to satisfy both "reuse
  ``render_proxy_config`` unchanged" and "secrets never inline in the TOML"
  (#188's acceptance items) at the same time.
- The ``EnvironmentFile=`` contents: one ``KEY=VALUE`` line per declared
  secret binding (``frp_transport.token_env``/``.stcp_secret_env``), written
  0600 by the bring-up script (``frpc_proxy_scripts.py``).
- The systemd **user** unit text: ``ExecStart=frpc -c <toml>``,
  ``EnvironmentFile=<env file>``, ``Restart=on-failure``,
  ``WantedBy=default.target``.

``identity_anchor="preshared_link_secret"`` is required before any of this
module's renderers will produce anything -- refused with the SAME typed
``TransportIdentityAnchorRequired`` ``control_channel.build_transport``
raises for the desktop side (relay-architecture-2026-08.md sec 8.3), so a
cluster that has not opted in cannot get a proxy unit rendered for it either.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.control_channel import TransportIdentityAnchorRequired
from clio_relay.errors import RelayError
from clio_relay.frp_link import (
    FrpLinkConfig,
    FrpVisitorType,
    render_proxy_config,
    require_frp_server_addr,
)
from clio_relay.frp_proxy_naming import canonical_proxy_name
from clio_relay.identifiers import filesystem_key
from clio_relay.relay_host import FrpTransportProtocol

DEFAULT_FRPC_PROXY_RESTART_SEC: Final = 5

_CONFIG_DIR_UNIT: Final = "%h/.config/clio-relay"
_CONFIG_DIR_SHELL: Final = "$HOME/.config/clio-relay"
_UNIT_DIR_UNIT: Final = "%h/.config/systemd/user"
_UNIT_DIR_SHELL: Final = "$HOME/.config/systemd/user"

FRPC_PROXY_SERVICE_NAME_PATTERN: Final = re.compile(r"clio-relay-frpc-proxy-[a-z0-9_-]+\.service\Z")


@dataclass(frozen=True)
class FrpcProxyPaths:
    """The deterministic name and file paths one cluster's frpc proxy unit occupies.

    Every path is carried in both its systemd-specifier form (``%h``, used
    inside the rendered unit's own directives) and its POSIX-shell form
    (``$HOME``, used by the bring-up/teardown/status bash scripts that
    actually write, read, and remove these files) -- the two refer to the
    same directory, but a raw shell script does not understand ``%h`` and a
    systemd unit file does not expand ``$HOME``.
    """

    unit_name: str
    toml_unit_path: str
    toml_shell_path: str
    env_unit_path: str
    env_shell_path: str
    unit_shell_path: str
    receipt_shell_path: str


def frpc_proxy_paths(cluster: str) -> FrpcProxyPaths:
    """Return the deterministic unit name and file paths for a cluster's proxy.

    Mirrors ``deployment_unit.endpoint_user_service_name``'s portable
    ``filesystem_key`` mapping; the distinct ``clio-relay-frpc-proxy-``
    prefix keeps this unit from ever colliding with the worker unit's
    ``clio-relay-worker-`` name for the same cluster.
    """
    key = filesystem_key(cluster, domain="systemd-cluster")
    stem = f"frpc-proxy-{key}"
    unit_name = f"clio-relay-{stem}.service"
    validate_frpc_proxy_service_name(unit_name)
    return FrpcProxyPaths(
        unit_name=unit_name,
        toml_unit_path=f"{_CONFIG_DIR_UNIT}/{stem}.toml",
        toml_shell_path=f"{_CONFIG_DIR_SHELL}/{stem}.toml",
        env_unit_path=f"{_CONFIG_DIR_UNIT}/{stem}.env",
        env_shell_path=f"{_CONFIG_DIR_SHELL}/{stem}.env",
        unit_shell_path=f"{_UNIT_DIR_SHELL}/{unit_name}",
        receipt_shell_path=f"{_CONFIG_DIR_SHELL}/{stem}-receipt.json",
    )


def validate_frpc_proxy_service_name(unit_name: str) -> None:
    """Defense-in-depth: refuse a unit name that does not match the generated shape.

    Mirrors ``deployment_ssh.py``'s own ``_SYSTEMD_SERVICE_NAME`` guard on the
    worker unit -- a belt-and-suspenders check ahead of embedding this name in
    a remote shell script, even though ``frpc_proxy_paths`` is the only
    producer and always emits a matching name.
    """
    if FRPC_PROXY_SERVICE_NAME_PATTERN.fullmatch(unit_name) is None:
        raise RelayError(f"unsafe frpc proxy systemd service name: {unit_name!r}")


def require_frp_identity_anchor(definition: ClusterDefinition, *, cluster: str) -> None:
    """Refuse (typed) unless the cluster opted into the preshared-link anchor.

    The SAME typed error and the SAME opt-in field
    ``control_channel.build_transport`` checks before dispatching to the
    desktop-side ``brokered_tcp``/``udp_rendezvous`` transports (sec 8.3): a
    cluster that has not explicitly accepted the weaker preshared-link anchor
    does not get a cluster-side proxy unit rendered for it either.
    """
    if definition.frp_transport.identity_anchor != "preshared_link_secret":
        raise TransportIdentityAnchorRequired(
            f"cluster {cluster!r} has not opted into an identity anchor; set "
            'frp_transport.identity_anchor to "preshared_link_secret" in this '
            "cluster's definition before a cluster-side frpc proxy unit can be "
            "installed for it (relay-architecture-2026-08.md sec 8.3)"
        )


def render_frpc_proxy_toml(
    definition: ClusterDefinition,
    *,
    cluster: str,
    local_port: int,
    local_ip: str = "127.0.0.1",
    proxy_type: FrpVisitorType = "stcp",
) -> str:
    """Render the cluster-side frpc proxy TOML with secrets env-templated, not inline.

    Delegates the actual TOML text to ``frp_link.render_proxy_config``
    unchanged -- only the ``token``/``secret_key`` fields differ from a
    desktop-transport call: here they carry frp's own env-template
    placeholder syntax rather than resolved secret values. ``proxy_type``
    defaults to ``"stcp"`` (mode (a), ``brokered_tcp`` -- #188's primary
    production TCP mode); pass ``"xtcp"`` for a cluster that dials out with
    ``udp_rendezvous`` instead. Unlike the desktop's own
    ``brokered_tcp``/``udp_rendezvous`` transports (``frp_transport.py``),
    there is no per-cluster config field selecting between the two --
    ``RelaySettings.remote_transport_mode`` is a desktop-side connection
    setting, not part of ``ClusterDefinition`` -- so the caller (the
    ``install-proxy`` CLI verb) is what decides, not this renderer.
    """
    require_frp_identity_anchor(definition, cluster=cluster)
    transport = definition.frp_transport
    server_addr = require_frp_server_addr(transport.server_addr, cluster)
    config = FrpLinkConfig(
        server_addr=server_addr,
        server_port=transport.server_port,
        protocol=FrpTransportProtocol(transport.protocol),
        token=_env_template(transport.token_env),
        secret_key=_env_template(transport.stcp_secret_env),
        proxy_name=canonical_proxy_name(definition, cluster=cluster),
    )
    return render_proxy_config(
        config, proxy_type=proxy_type, local_ip=local_ip, local_port=local_port
    )


def render_frpc_proxy_env_file(
    definition: ClusterDefinition,
    *,
    cluster: str,
    env: Mapping[str, str] | None = None,
) -> str:
    """Render the ``EnvironmentFile=`` contents binding the frpc proxy's real secrets.

    Resolves both secrets from the cluster's declared env bindings via
    ``FrpLinkConfig.from_cluster`` -- the SAME resolution the desktop
    transport uses -- so the systemd ``EnvironmentFile=`` carries the real
    values the TOML's ``{{ .Envs.NAME }}`` placeholders reference at frpc
    parse time. Never written by this process: the caller (the bring-up
    script) is what actually creates the 0600 file on the cluster.
    """
    require_frp_identity_anchor(definition, cluster=cluster)
    resolved = FrpLinkConfig.from_cluster(
        definition,
        cluster=cluster,
        proxy_name=canonical_proxy_name(definition, cluster=cluster),
        env=env,
    )
    transport = definition.frp_transport
    lines = [
        _env_file_line(transport.token_env, resolved.token),
        _env_file_line(transport.stcp_secret_env, resolved.secret_key),
    ]
    return "\n".join(lines) + "\n"


def render_frpc_proxy_unit(
    *,
    cluster: str,
    paths: FrpcProxyPaths,
    frpc_bin: str = "%h/.local/bin/frpc",
    restart_sec: int = DEFAULT_FRPC_PROXY_RESTART_SEC,
) -> str:
    """Render the persistent user-level systemd unit for a cluster's frpc proxy.

    ``Restart=on-failure`` paced by ``RestartSec`` (with
    ``StartLimitIntervalSec=0`` disabling the burst cap, mirroring
    ``deployment_unit.render_endpoint_user_service``'s own worker-unit
    reasoning) is the unit's backoff: systemd retries a crashing ``frpc``
    every ``restart_sec`` seconds indefinitely rather than giving up after a
    burst, and never restarts after an explicit ``systemctl --user stop``.
    """
    if restart_sec < 1:
        raise RelayError("frpc proxy restart delay must be a positive number of seconds")
    description_cluster = _escape_unit_value(cluster)
    return f"""[Unit]
Description=clio-relay frpc proxy for {description_cluster}
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
EnvironmentFile={paths.env_unit_path}
ExecStart={frpc_bin} -c {paths.toml_unit_path}
Restart=on-failure
RestartSec={restart_sec}

[Install]
WantedBy=default.target
"""


def frpc_proxy_config_digest(toml_text: str) -> str:
    """Return the sha256 digest identifying one rendered proxy TOML's content.

    A stable fingerprint for the receipt (clio-relay#279 design point 3):
    since the TOML is deterministic given (cluster, proxy_name, canonical
    composition), this digest changing between two bring-up receipts is
    itself evidence of a configuration drift worth investigating.
    """
    return hashlib.sha256(toml_text.encode("utf-8")).hexdigest()


def _env_template(name: str) -> str:
    """Return frp's own ``{{ .Envs.NAME }}`` template placeholder for one env binding."""
    _validate_env_name(name)
    return "{{ .Envs." + name + " }}"


def _validate_env_name(name: str) -> None:
    if (
        not name
        or name[0].isdigit()
        or any(not (c.isupper() or c.isdigit() or c == "_") for c in name)
    ):
        raise RelayError(f"unsafe frp environment binding name: {name!r}")


def _env_file_line(name: str, value: str) -> str:
    _validate_env_name(name)
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise RelayError(f"unsafe value for env-file binding {name!r}")
    return f"{name}={value}"


def _escape_unit_value(value: str) -> str:
    """Escape one value for embedding in this unit's ``Description=`` line.

    A minimal local mirror of ``deployment_unit._systemd_escape``'s rules for
    the one field this module embeds operator-controlled text into: cluster
    names are free-form (``identifiers.filesystem_key`` hashes anything
    non-portable rather than validating it up front), so ``%``/newlines must
    not be able to inject a new unit directive or malform the file. Kept as a
    small local copy rather than a cross-module private import: this module's
    own escaping need (one Description= value) is narrower than
    ``deployment_unit``'s (arbitrary exec arguments and Environment=
    assignments), and duplicating the two-line rule here is cheaper than a
    new cross-module private coupling for it.
    """
    if "\x00" in value or "\n" in value or "\r" in value:
        raise RelayError("unit description value must not contain NUL or newlines")
    return value.replace("%", "%%")
