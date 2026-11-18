# Interface context

A dense map of relay's interfaces for coding agents. The README is the short
version for people. The published interfaces and every decision behind them are
in `docs/design/relay-rework-ledger.md` ("W1. Relay core"); this page points
there rather than repeating it.

## The model

Relay is a standard MCP server. Any MCP client uses all of it: there is no
client-specific path. It keeps a list of sites in `sites.yaml`. Each site names
an adapter (how relay reaches it), optional alternatives that routing falls
back to, and a policy (`open` or `guarded`). Every site answers the same five
verbs. Operations are durable records under the state directory, so a run
survives a relay restart and is reconciled with the adapter's `find`.

## Command line

`clio-relay` and `python -m clio_relay` run the same Typer app
(`src/clio_relay/cli.py`). Every command prints JSON; a refusal prints
`<reason>: <message>` on stderr and exits 2.

- `serve`: the MCP server. Stdio by default. `--http` serves Streamable HTTP on
  `127.0.0.1:8766/mcp` (`--host`, `--port`, `--path`) with a bearer token from
  `CLIO_RELAY_TOKEN`, `--token-file`, or a token written once to
  `<state>/http-token`. `--grant-file PROVIDER=PATH` (repeatable) names a grant
  file the host keeps current. `--tls-cert` and `--tls-key`, given together,
  serve HTTPS.
- `adapters`: the installed adapters and the verbs each implements.
- `site list|show|add|catalog|add-from-catalog|edit|remove|switch`: manage
  `sites.yaml`. `add`, `add-from-catalog`, `edit` and `switch` print what the
  adapter set up under `set_up`.
- `asks list|show|answer|decline|page|identities|sign-in|sign-out|probe`: answer
  what a site asks (one-time codes, sign-ins, spend approvals and package
  reviews) and manage stored identities.
- `connect <handle>`: forward a running service's ports to this machine and hold
  them open. The handle comes from the `serve` result's `connect_command`.
- `rendezvous show|set KEY=VALUE...`: the rendezvous host where a service and
  this machine both dial out.
- `worker ...`: the `worker` adapter's own tree (`cluster`, `endpoint`, `api`,
  `session`, `queue`, `scheduler`, `relay-host`). A worker on the cluster runs
  `clio-relay worker api start`. See `docs/adapters/worker.md` and
  `docs/operations.md`.

## MCP surface

Five tools, on both protocol eras (`2026-07-28` and `2025-11-25`):

| Tool | What it does |
| --- | --- |
| `describe` | Lists sites, describes one site with its routes and skill, or observes one operation (`job=`) |
| `run` | Submits a PSI/J job spec, or a reviewed package on a guarded site, and follows it to the end |
| `exec` | Runs one command and returns its output. No time limit unless `timeout_s` is given; quiet commands report progress |
| `files` | Lists, reads, writes and stages files. Staged inputs are content-addressed by SHA-256 |
| `serve` | Starts a long-running service and returns its endpoint and a connect command |

- **Tasks.** On `2026-07-28`, relay advertises `io.modelcontextprotocol/tasks`.
  A new `run` or `serve` becomes a task when the client asks for one: progress
  goes on the task, asks arrive as `input_required` and are answered with
  `tasks/update`, and `tasks/cancel` cancels the job. A client without tasks gets
  an operation id and follows it with `describe(job=...)`. See
  `docs/mcp-tasks.md`.
- **Asks.** A sign-in, one-time code, spend approval or package review is an
  elicitation (form or URL) when the client supports it, `input_required` under
  a task, or an ask the person answers with `clio-relay asks`.
- **Refusals** are typed: the result carries one `RefusalReason` value (the list
  is in the ledger) and a message that says what to do.
- **Resources.** `relay://skills/{name}` and `relay://sites/{site}/skill` serve
  site skills as `text/markdown`. Changes are announced on
  `subscriptions/listen` (`2026-07-28`) and as plain list-changed and updated
  notifications (`2025-11-25`).
- **Service UI data.** A `serve` result carries the service's URLs only in
  `_meta["ai.iowarp.relay/service"]`, for the host's UI. The record, `describe`
  and the task status keep ports without URLs.
- **Outputs.** `OutputRef.metadata["view"]` is a view descriptor
  (`src/clio_relay/serve/schemas/view-descriptor.v1.json`) and
  `metadata["provenance"]` a `relay.provenance.v1` block. A running job's latest
  step is in the operation's `result["progress"]`.

## Extension points

Entry points, so an adapter or hook ships in its own package:

- `clio_relay.adapters`: `local`, `fixture`, `ssh`, `worker`, `tapis`, `globus`,
  `iri`, `sky`. An adapter subclasses `BaseAdapter` and passes the conformance
  kit, `clio_relay.testing.conformance.AdapterConformance`.
- `clio_relay.hooks`: `asks` (sign-ins and approvals), `guard` (guarded-site
  policy, pricing and package preparation) and `serve` (the service runtime).
  A hook whose module is not installed falls back to a stub.

## Environment and files

- `CLIO_RELAY_CONFIG_DIR`: `sites.yaml`, `skills/`, and `credentials.json` when
  the OS keyring is unavailable. `CLIO_RELAY_STATE_DIR`: operations, asks,
  per-site adapter state and the content store. Both default to the platform's
  usual places.
- `CLIO_RELAY_GRANT_FILE_<PROVIDER>`: a grant file, the same as `--grant-file`.
- `CLIO_RELAY_TOKEN`: the HTTP bearer token.
- Adapter settings live in the site's entry, not the environment. Adapter pages
  under `docs/adapters/` list the few variables an adapter reads, such as the
  Tapis OAuth client.

## Examples are not product defaults

Catalog sites, skills and the live acceptance runs name real targets (Ares,
TACC systems, a rendezvous host). They are tested configurations. Core code
names no cluster, agent or transport.