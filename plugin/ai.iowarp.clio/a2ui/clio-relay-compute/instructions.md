# Relay's Compute page

This catalog holds the components relay's Compute page draws with: the part
of the Basic catalog it uses (`Text`, `Row`, `Column`, `Card`, `Tabs`,
`Divider`, `Button`, `TextField`, `CheckBox` and `ChoicePicker`, each
unmodified) and CLIO's choice table, `clio.choice-table.v1`. The table lays
the endpoints out like CLIO's Tools settings: a row of defaults for new
endpoints, then one row per endpoint with its Protocol and Security pickers,
its Status and a menu of Probe, Edit and Remove. A client that does not list
this catalog gets the same page as Basic cards.

Relay's MCP server writes these surfaces itself. It sends the page when CLIO
reads `a2ui://clio-relay/compute`, and it redraws the page in the result of
each action. An agent does not author surfaces with this catalog. To show a
person their endpoints, run relay's `describe` tool or point them to the
Compute page.

Each control calls one of relay's app-only `compute_*` tools with a
single-use token the surface carries. Relay refuses a call without a fresh
token, so an agent cannot act on the page for the person. The page changes
the same site store as the `clio-relay site` commands.
