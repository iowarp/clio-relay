"""How each Compute page action's tool is titled and annotated."""

from __future__ import annotations

import mcp_types

from clio_relay.surface.a2ui import surfaces

TITLES = {
    surfaces.OPEN_ADD: "Open the add-endpoint form",
    surfaces.ADD_SITE: "Add an endpoint",
    surfaces.ADD_FROM_CATALOG: "Add an endpoint from the catalog",
    surfaces.EDIT_SITE: "Edit an endpoint",
    surfaces.SAVE_SITE: "Save an endpoint",
    surfaces.CANCEL: "Cancel a form or a removal",
    surfaces.REMOVE_SITE: "Ask to remove an endpoint",
    surfaces.CONFIRM_REMOVE: "Remove an endpoint",
    surfaces.PROBE_SITE: "Probe an endpoint",
    surfaces.SWITCH_ADAPTER: "Change an endpoint's protocol",
    surfaces.SET_VALUE: "Change an endpoint's protocol, security or spending, or a default",
    surfaces.CONNECT_SITE: "Sign in to an endpoint",
    surfaces.OPEN_DIALOG: "Open the add-endpoint or defaults dialog",
    surfaces.PICK: "Pick a site, a computer or a protocol in the add-endpoint dialog",
    surfaces.TEST_ENDPOINT: "Test an endpoint before adding it",
    surfaces.SAVE_ENDPOINT: "Add a tested endpoint",
    surfaces.BACK: "Go back from a test in the add-endpoint dialog",
    surfaces.CLOSE_DIALOG: "Close a dialog",
    surfaces.SAVE_DEFAULTS: "Save the defaults for new endpoints",
    surfaces.UNDO: "Undo the last addition or removal",
}


# What each action does to relay's endpoints. Opening a form or a dialog, picking
# in it, going back, cancelling and asking to remove change only the page;
# probing and testing write relay's probe cache or an adapter's state; adding
# and signing in add; saving, switching, setting, undoing and confirming a
# removal replace or delete what was there.
PAGE_ONLY = mcp_types.ToolAnnotations(
    read_only_hint=True, destructive_hint=False, open_world_hint=False
)


ADDS = mcp_types.ToolAnnotations(
    read_only_hint=False, destructive_hint=False, open_world_hint=False
)


REPLACES = mcp_types.ToolAnnotations(
    read_only_hint=False, destructive_hint=True, open_world_hint=False
)


ANNOTATIONS = {
    surfaces.OPEN_ADD: PAGE_ONLY,
    surfaces.EDIT_SITE: PAGE_ONLY,
    surfaces.CANCEL: PAGE_ONLY,
    surfaces.REMOVE_SITE: PAGE_ONLY,
    surfaces.PROBE_SITE: ADDS,
    surfaces.ADD_SITE: ADDS,
    surfaces.ADD_FROM_CATALOG: ADDS,
    surfaces.CONNECT_SITE: ADDS,
    surfaces.SAVE_SITE: REPLACES,
    surfaces.SWITCH_ADAPTER: REPLACES,
    surfaces.SET_VALUE: REPLACES,
    surfaces.CONFIRM_REMOVE: REPLACES,
    surfaces.OPEN_DIALOG: PAGE_ONLY,
    surfaces.PICK: PAGE_ONLY,
    surfaces.BACK: PAGE_ONLY,
    surfaces.CLOSE_DIALOG: PAGE_ONLY,
    surfaces.TEST_ENDPOINT: ADDS,
    surfaces.SAVE_ENDPOINT: ADDS,
    surfaces.SAVE_DEFAULTS: REPLACES,
    surfaces.UNDO: REPLACES,
}
