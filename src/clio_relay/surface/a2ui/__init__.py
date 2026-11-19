"""Relay's Compute page for CLIO, as A2UI surfaces (RL31-RL33, HS1-HS5)."""

from clio_relay.surface.a2ui.protocol import BASIC_CATALOG_ID, EXTENSION_ID, RELAY_CATALOG_ID
from clio_relay.surface.a2ui.surfaces import ACTIONS
from clio_relay.surface.a2ui.tab import TAB_URI, ComputeTab, install

__all__ = [
    "ACTIONS",
    "BASIC_CATALOG_ID",
    "EXTENSION_ID",
    "RELAY_CATALOG_ID",
    "TAB_URI",
    "ComputeTab",
    "install",
]
