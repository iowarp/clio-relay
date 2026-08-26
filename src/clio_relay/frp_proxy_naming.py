"""The ONE canonical stcp/xtcp proxy name a cluster's link resolves to (clio-relay#279).

A dedicated single-function module rather than adding this to ``frp_link.py``
(the more obvious home, already the shared frp-link substrate): that module
sits right at the file-size ratchet's 800-line cap
(``scripts/check_file_size.py``), and this codebase's own convention when a
capped file has no room is a new, tightly-scoped owner module -- not a new
baseline entry (baselines only ratchet down, per the cleanup program's
ground rules) and not bolting one more concern onto an already-full file.

Both ``frp_transport.py`` (the desktop-side ``brokered_tcp``/
``udp_rendezvous`` transports) and ``frpc_unit.py`` (the cluster-side proxy
TOML/unit renderer, clio-relay#279) import :func:`canonical_proxy_name` from
here -- neither computes it inline any more -- so the two ends of one
stcp/xtcp pairing can never silently diverge. This closes the trap
``cli_relay_host.py``'s ``render-frpc-config`` command used to fall into:
its ``--proxy-name`` option defaulted to the unrelated literal
``"relay-stcp"`` instead of this canonical form, so an operator who rendered
that config without an explicit override got a proxy name that never
matched what the desktop transport (or a proxy unit installed via
``relay-host install-proxy``) actually dials.
"""

from __future__ import annotations

from clio_relay.cluster_config import ClusterDefinition


def canonical_proxy_name(definition: ClusterDefinition, *, cluster: str) -> str:
    """Return the canonical stcp/xtcp proxy name for one cluster.

    ``definition.frp_transport.proxy_name`` wins when the cluster explicitly
    registered a different name at the relay point; otherwise this resolves
    to the ``<cluster>-owned-session`` default.
    """
    return definition.frp_transport.proxy_name or f"{cluster}-owned-session"
