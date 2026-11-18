# Relay's Compute tab

This catalog is the part of the Basic catalog that relay's Compute tab draws
with: `Text`, `Row`, `Column`, `Card`, `Tabs`, `Divider`, `Button`,
`TextField`, `CheckBox` and `ChoicePicker`, each unmodified. It adds no
component, so everything on the tab is something CLIO already renders.

Relay's MCP server writes these surfaces itself. It sends the tab when CLIO
reads `a2ui://clio-relay/compute`, and it redraws the tab in the result of
each button. An agent does not author surfaces with this catalog. To show a
person their sites, run relay's `describe` tool or point them to the Compute
tab.

Each button calls one of relay's app-only `compute_*` tools with a
single-use token the surface carries. Relay refuses a call without a fresh
token, so an agent cannot press the tab's buttons for the person. The tab
changes the same site store as the `clio-relay site` commands.
