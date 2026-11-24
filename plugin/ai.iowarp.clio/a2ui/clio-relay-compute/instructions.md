# Relay's Compute page

This catalog holds the components relay's Compute page draws with. It has the
part of the Basic catalog it uses (`Text`, `Row`, `Column`, `Card`, `Tabs`,
`Divider`, `Button`, `Icon`, `TextField`, `CheckBox` and `ChoicePicker`, each
unmodified) and three of CLIO's own components:

- `clio.choice-table.v1`, the endpoint list. It lays the person's endpoints
  out like CLIO's Tools settings: column headers (Endpoint, Protocol, Security,
  Spending, Status), then one row per endpoint with its pickers, its status and
  a menu of Connect, Test, Edit and Remove. The headers show even with no rows.
  A row's `options` limits a column to the protocols that endpoint has, and
  `inherited` marks values that follow the defaults.
- `clio.dialog.v1`, a dialog frame. Its trigger sits in the page and opens it.
  The Add endpoint dialog has three tabs (Catalog, Saved, Your own), a step rail
  (Site, Protocol, Test, Save), a status line for the test and a footer with
  Test, Cancel or Back, and Save endpoint. Save stays off until a test of the
  same choices passes. The gear opens the defaults for new endpoints in a
  narrow dialog, and Edit and Sign in open in dialogs too.
- `clio.picker.v1`, a searchable list or compact select of items, each with
  a title, a subtitle and chips. The dialog picks a site, a protocol or a saved
  computer with it.

Spending decides when relay asks the person before it submits a job there:
every time, above a node-hour threshold, or never (Always allow). A client that
does not list this catalog gets the same page as Basic cards.

Relay's MCP server writes these surfaces itself. It sends the page when CLIO
reads `a2ui://clio-relay/compute`, and it redraws the page in the result of
each action. An action's result can carry a toast in `_meta` under
`io.iowarp/toast` (`message`, `tone` and an optional `undo` action), which the
client shows briefly with an Undo button. An agent does not author surfaces with
this catalog. To show a person their endpoints, run relay's `describe` tool or
point them to the Compute page.

The page speaks the language the host sends in a request's `_meta` under
`io.iowarp/locale` (English, Spanish, Japanese or Greek), and English otherwise.

Each control calls one of relay's app-only `compute_*` tools with a
single-use token the surface carries. Relay refuses a call without a fresh
token, so an agent cannot act on the page for the person. The page changes
the same site store as the `clio-relay site` commands.
