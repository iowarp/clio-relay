# Relay rework: execution plan

This document turns `clio-relay-plan.md` (the contract: RL1 to RL50, HS1 to HS5, the removals, clio-jarvis, Tapis, LQ1 to LQ40 and LQ-L1 to LQ-L17) into workstreams that child sessions can run in parallel. It says who owns which files, what has to land before what, which qualification rows each workstream proves, what runs on Ares, and what only the owner can unblock.

It does not restate the contract. Where this plan amends the contract, the amendment is marked **Amendment** and the first change of workstream W1 writes it into the contract and the ledger.

Target path in clio-relay: `docs/design/relay-rework-execution.md`. The planning session could not write into the worktree, so W1 commits this file there in its first change, together with `relay-rework-workstreams.md` (the overview diagram).

## Status

| Item | State |
| --- | --- |
| Contract | `clio-relay-plan.md`, plus the MCP Apps plan it builds on |
| Execution plan | This file. Reviewed by a big model (see "Review record") |
| Ares | Ready. See "Ares" |
| Implementation | Wave 0 under way. Progress and evidence are in `relay-rework-ledger.md` |
| Missing inputs | `plans/remote-visualization-scope.md` and `plans/remote-connectors-plan.md` were not available. See "Gaps" |

## Principle zero: relay is a plain MCP server

This is a user-directed requirement and it overrides anything below that conflicts with it.

> The old implementation was built on the assumption that CLIO would have a special path for clio-relay. Relay must be generally accessible to any other agentic framework, and have a tight but native integration with CLIO.

**Amendment RL0.** clio-relay is a standard MCP server. Claude Code, Copilot CLI, Codex, or a generic MCP client gets the whole surface. Relay imports nothing from clio-agent, has no code path that only runs under CLIO, and never tests for "am I under CLIO". It reads only what the client advertises in `initialize`. CLIO's tight integration comes from the five host semantics HS1 to HS5, which never name relay, plus relay's own CLIO plugin and Compute tab, which are data that any host is free to ignore.

### Protocol baseline: MCP v2 with tasks

Relay is designed task-first. The primary path for long work is the MCP tasks extension: task-augmented `run`, `serve` and large `files` calls, with progress on the task, and asks raised as `input_required` with their `inputRequests` on `tasks/get`, answered through `tasks/update`. The handle path in RL2a is the fallback for clients that do not declare the extension.

| Item | Pin |
| --- | --- |
| Protocol revision | MCP `2026-07-28`, the first revision with tasks as an extension. Earlier revisions (`2025-11-25`) are served without tasks, on the RL2a fallback |
| Tasks extension | `io.modelcontextprotocol/tasks`, SEP-2663, specification revision `2026-07-28`. Methods `tasks/get`, `tasks/update`, `tasks/cancel`. Declared per request in the client's capabilities `extensions` map |
| SDK and types | Python `mcp` 2.3.0 (on Ares), with its `2026-07-28` types |
| Task runtime on the server | FastMCP 4 with `fastmcp-tasks` 4.0.0b1, which relay develop already depends on and which implements SEP-2663 over a durable store |

Two facts that shape W1, checked at tag v2.3.0 of the Python SDK:
- **`mcp` 2.3.0 does not implement the tasks extension.** Its conformance baseline lists all nine server tasks scenarios as expected failures, and its migration guide says so; the work is tracked in python-sdk#2806. The extension's types are present, but no client or server runtime is. So the server runtime is `fastmcp-tasks`, and the qualification client for the task path (LQ43) is a thin client on `mcp` 2.3.0's transport and `2026-07-28` types that sends the three extension methods itself. It does not use fastmcp, so relay is not judged by its own library. When the SDK ships the extension, W1 swaps the thin client for the SDK's and records it in the ledger.
- **FastMCP 4's known gaps on `2026-07-28` connections.** `ctx.session_id` is per request, and `ctx.set_state` does not round-trip. Relay keys every operation, task and ask by its own durable record id and never relies on session state.

### What any MCP client gets

| Capability | Client advertises it | Client does not |
| --- | --- | --- |
| The five verbs | `describe`, `run`, `exec`, `files`, `serve` as ordinary tools with JSON Schema input and text plus structured output | Same. Tools are core MCP |
| Long work (RL2) | **Primary path.** `run`, `serve` and large `files` calls are task-augmented (`io.modelcontextprotocol/tasks`, revision `2026-07-28`). Progress rides the task. A question moves the task to `input_required` with its outstanding input requests, answered through `tasks/update`, as the extension specifies. Cancel is `tasks/cancel` | **Fallback, Amendment RL2a.** The call is an ordinary tool call and returns at once with a relay handle in text and structured content. It is never dressed up as an MCP task result. The five verbs' own schemas carry the follow-up, so a tools-only client on a fresh connection can finish the work. `describe` with `site` lists that site's jobs, services and transfers, and with `job` gives state and output locations. `run` with `cancel` cancels. `files` with `job` reads outputs, and `files` with `transfer` reports a transfer's progress, completion or failure. `serve` with `service` reports readiness, the endpoint and, once stopped, the final artifacts, and `serve` with `stop` stops it. When relay runs remotely over HTTP, a service's endpoint on the relay machine is not reachable from the client's desktop. The `serve` result then gives the host-neutral command to run on the desktop (`clio-relay connect <handle>`, which runs the frp visitor or `ssh -L` there), and the service is never exposed publicly. One durable record per operation serves both paths. No call blocks for the life of a job |
| Choices and approvals (form elicitation) | Asked in band with form elicitation: SSH profile, account, queue, package review, spend gate | **Amendment RL25a.** Nothing is submitted, and the ask stays pending until a person answers it outside the agent's reach. The call ends with the typed reason `needs_approval`, an ask id, and exactly what is being asked: the operation, the package hash, the charge. Over stdio, relay also gives the address of a loopback approval page. Over Streamable HTTP, the page is on relay's own HTTPS origin and needs the operator's credential, which is separate from the MCP caller's. Opening the page (GET) changes nothing. Approving is a CSRF-protected POST bound to that operation, hash and charge, so a retry with changed arguments needs a new approval. `clio-relay asks answer <id>` on the relay machine does the same. The site entry can pre-approve (`spend.ask`, a reviewed package). Declining or not answering submits nothing. An approved ask submits at most once: relay records its intent and the scheduler job name before submitting, and on restart it reconciles by that name instead of submitting again. Limitation, written in the docs: on a local stdio relay, an agent with a shell on the same machine could reach the loopback page, so guarded sites should be used from hosts that support elicitation |
| Sign-ins and secrets (URL elicitation) | Relay sends its page or the provider's `https` page | The call ends with the typed reason `needs_sign_in`, the address to open, and "retry when done". The address is relay's single-use page (loopback over stdio, relay's own origin over HTTP) or the provider's page, so the model seeing it reveals no secret |
| A2UI surfaces | **Amendment HS1a.** Sent only on a request whose client capabilities, as declared for that request under revision 2026-07-28, include the A2UI v0.9 or v0.9.1 extension. Never inferred from the client's name or cached for the server process. Always in addition to the full text result | Not sent. The text result carries the same facts |
| The Compute tab (HS3) | CLIO only, through relay's plugin. Tab actions go to relay as `tools/call` (HS2), on tools that are never in `tools/list`, so every client, including one that ignores MCP Apps visibility, sees exactly the five verbs plus mounted tools (LQ1). Relay accepts such a call only with a single-use action token bound to the surface and action it rendered. The token travels only in A2UI payloads, which go only to clients that declared A2UI. Each action calls the same site-store code as the command line | `describe` with no site lists the sites as text. Adding, editing, removing and switching a site's adapter is an operator task by design: `clio-relay site add`, `edit`, `remove`, or editing `sites.yaml`. Connecting an account goes through sign-in on first use |
| Account grants (HS4) | **Amendment HS4a.** Relay takes a grant file per provider through its own documented setting (`--grant-file <provider>=<path>` or `CLIO_RELAY_GRANT_FILE_<PROVIDER>`), re-read on change, so a refresh or revocation needs no restart. The mapping from `CLIO_ACCOUNT_<PROVIDER>_FILE` lives only in the launch configuration in relay's `plugin.json`. Relay's identity code never reads a CLIO variable name. Covers Globus and private GitHub package access (RL30) | Relay signs in itself through URL elicitation (Globus native app, TACC device code, GitHub device flow) and keeps tokens in the OS keyring. It works with no grant setting and no CLIO |
| Mounted services (RL21) | Tools appear under `<site>_<name>_`, with `tools/list_changed`. MCP Apps metadata passes through | Same tools. A host without MCP Apps shows them as plain tools |
| Site skills (RL34) | MCP resources | Same |

### Coupling that has to go

Audited on the develop heads listed under "Base and drift". Each item is removed or made generic by the workstream named.

| Where | What | Action | Owner |
| --- | --- | --- | --- |
| clio-relay `mcp_tool_catalog*.py`, `mcp_server.py`, `fastmcp_server.py`, `mcp_dispatch.py`, `mcp_gateway_tools.py`, `mcp_queue_tools.py` | The `relay_*` surface. On develop the user names are `relay_remote_mcp_context`, `relay_submit_agent`, `relay_status`, `relay_cancel`, `relay_observe`, `relay_wait`, `relay_queue_list`, `relay_queue_diagnose`, `relay_queue_stale`, `relay_storage_status`, `relay_bind_jarvis_runtime`, `relay_artifact_lineage`, `relay_list_artifacts`, `relay_read_artifacts` (`USER_MCP_TOOL_NAMES`), plus the virtual JARVIS, administrator and gateway tools, 45 in all | Delete. The ledger lists all 45 and every caller | W8 |
| clio-relay `mcp_submission_mcp_call.py`, `_mcp_call_runner_facade.py`, `mcp_remote_catalog.py`, `mcp_remote_transport.py`, `cli_remote_mcp*.py`, `jarvis-packages/clio_relay/clio_relay/mcp_call/`, `docs/remote-mcp-federation.md` | Queued `mcp_call` federation, built so a CLIO desktop agent sees remote tools with a `cluster` argument | Delete. RL21 replaces it with a held session | W8 |
| clio-relay `browser_gateway.py`, `browser_gateway_errors.py`, `live_acceptance_browser_*.py` | Capability-URL proxy for CLIO's sandboxed viewer | Delete (RL22) | W8 |
| clio-relay `http_api_routes_*.py` | Routes that only the `relay_*` tools and CLIO's viewer called | Delete. The worker's own link routes stay behind `worker` | W8 |
| clio-relay `scheduler_providers/` (8 files on develop) | Relay's own Slurm provider | Delete (RL18). The worker submits through `psij-python` | W8 |
| clio-relay `models_gateway.py` `ArtifactRef` | Lifts `metadata["clio.provenance.v1"]` | Replace with a neutral provenance block (RL40 provenance) | W9 |
| clio-relay `pyproject.toml` | `clio-schemas==0.2.1`, used for CLIO payload projections and the cross-repo schema-equality fixtures `tests/fixtures/clio_schemas_v0_2_0_projections.json`, `relay_schema_equality_v1.json`, `relay_state_map_v1.json` | Drop from the server path. Kept only if the worker link still needs it, and the ledger says why | W8 |
| clio-relay `docs/ai/interface-context.md`, `docs/remote-mcp-federation.md`, `docs/mcp-tasks.md` | Describe CLIO calling special routes | Rewrite for any MCP client | W8 (federation), W1 (interface, tasks) |
| clio-agent `gact/ssh_tools.py` and its supporting files | `ssh_execute`, a native tool that exists to get around the fence | Delete (RL36), replaced by RL37 | W4 |
| clio-agent `gact/infrastructure/drivers.py` | `RELAY_VERSION`, the `relay` managed service and `_relay_plan`: the Infrastructure tab installs relay | Delete (RL38) | W4 |
| clio-agent `tools/gateway.py`, stale comments, `relay` pytest marker | Relay-era leftovers | Delete (RL38) | W4 |
| gact-tui `infrastructure-topology.tsx`, `tool-operation-contract.ts`, `tool-operation-outcome.ts`, `tool-kind-icons.ts` | Relay engine icon, `ssh_execute` | Delete | W12 |
| gact-tui `scripts/check_infrastructure_ownership.mjs`, `web/tests/review/service-methods-catalog.json`, `desktop/src-tauri/src/ssh_transport_command.rs` (test reference), `docs/design/provider-runtime-campaign-2026-09.md` | Relay references not listed in RL38 | Remove the managed-service and `clio-relay==1.6.8` entries. Historical design docs keep their text with a note | W12 |

**Amendment LQ41.** A generic MCP client drives relay end to end. A script on the official `mcp` Python SDK (2.3.0 on Ares, not fastmcp, so relay is not tested by its own library) connects over stdio and then over Streamable HTTP. It lists tools, runs `describe`, submits a job to the Slurm fixture, answers a form elicitation (spend gate) and a URL elicitation (loopback page) through the SDK's own handlers, and reads the result. The task path is LQ43. Passes when every step works with no CLIO process on the machine.

**Amendment LQ42.** The same client with tasks and elicitation not advertised, over stdio and over Streamable HTTP, the HTTP run from a second Ares node. `run` returns a handle. The client disconnects, reconnects on a fresh connection, finds the job through `describe`, sees it finish and reads its outputs through `files`. A second job is cancelled through `run` with `cancel`. A large `files` transfer is finished through `files` with `transfer`, and a service through `serve` with `service` and `stop`, using only those schemas. The spend gate ends with `needs_approval` and an ask id. Opening the approval page changes nothing. Over HTTP the page refuses without the operator credential. A retry with changed arguments is refused after approval. A decline submits nothing, no answer submits nothing, and an approval followed by two retries and a relay restart submits exactly once, including a crash injected after the scheduler accepted the job and before relay recorded its id. No A2UI item appears in any result. One relay process serving a capable and an incapable client in turn sends A2UI only to the capable one. A direct `tools/call` to a tab action without its token is refused.

**Amendment LQ43. The task path end to end.** The thin task client from "Protocol baseline" declares `io.modelcontextprotocol/tasks` on protocol `2026-07-28`, over stdio and over Streamable HTTP, against the Slurm fixture on Ares. `run` returns a task. `tasks/get` shows progress records advancing. The spend gate moves the task to `input_required` with one input request; the client answers it through `tasks/update`, and the task returns to working and submits exactly once. A second task is cancelled through `tasks/cancel` and its Slurm job is gone. The first task's result carries the job's outputs. The client then reconnects on a fresh connection and reads the finished task by id. The same job is visible through `describe`, so the task and RL2a paths share one record.

**Amendment LQ-L18 (person).** Add relay to one third-party agent host (Claude Code or Copilot CLI) with its documented `mcp add` command and run "submit hostname to Ares and tell me the node". Record the host, its version and the transcript.

**Amendment LQ31a.** A boundary check on relay, in both directions. In a clean venv on Ares with no clio-agent, no marketplace plugin and no `CLIO_*` variable set, install relay from its built wheel and complete LQ41. Audit the installed dependency tree for clio-agent and `clio-schemas`. Check by use, not by spelling: relay's canonical provenance is the neutral block, and nothing in `src/clio_relay` imports a host runtime or reads a `CLIO_*` name outside relay's own `CLIO_RELAY_*` settings. `plugin/` and the CLIO user-guide pages are allowed to name CLIO. The host-side check is LQ31 itself.

## Base and drift

Work targets `develop` in every repo except clio-kit, and clio-jarvis, which is new.

| Repo | Plan says | Use | Drift that matters |
| --- | --- | --- | --- |
| clio-relay | `main` 3b051d3 (v1.6.8) | `develop` 034117be5 | 85 commits ahead. jarvis-cd pin 1.8.1 (main has 1.8.0 in `bootstrap_constants.py` and `jarvis_cd_lock_binding.py`), clio-kit 2.10.6 pin. About 483 modules under `src/clio_relay` against about 384 on main, mostly more queue, service runtime, bootstrap reconcile and validation seams. Every line count in the contract's "Current state" is low. `scheduler_providers/` has 8 files (`command`, `constants`, `external`, `protocols`, `slurm_connector`, `slurm_provider`, `slurm_status`, `__init__`). The planning worktree is on `main`, so every implementation session starts from `develop` |
| clio-agent | `develop` 0ed2241a0 | `develop` f08dbde3a | `offline_access` is assembled in `gact/storage/auth.py`, not `globus_consent.py`. The plugin manifest parser is `plugins/manifest.py` and `plugins/loader.py`, not `gact/a2ui_catalogs/plugin.py`, which only loads catalogs. URL trust lives in `gact/elicitation_config.py` (`url_trusted_origins`) and is global, with no per-server allowance. No CONNECT helper exists. The A2UI action dispatcher is `gact/a2ui_actions/dispatcher.py` and already resolves a surface's owner. Plugin server environments are built in the plugin server launcher (`plugins/servers.py` and the MCP runtime child-environment builder; W3 records the exact function) |
| gact-tui | `develop` def3ae0ab | `develop` 21163590c | `WorkbenchTab` (`workbench-shared.ts`) is a closed union rendered by `workbench-tab-content.tsx`, with no plugin tab registry. A2UI runs through `web/src/lib/a2ui/processor-store.ts` on `@a2ui/web_core/v0_9` and `@a2ui/react/v0_9`. No secret field type exists in questions or A2UI. Four relay references are not in RL38 (see coupling table) |
| clio-kit | `main` 490269f | `main` 490269fb1 | A `develop` branch exists (8375600bb) but is stale: old `clio-kit-mcp-servers/` layout, ParaView 2.2.3, docs dated 2026-08-18. Do not use it. On `main` the ParaView server already has the MCP Apps trame app (`node/app.py`, `node/engine.py`, `open_view` with a `scene` argument). It has no view descriptor and does not follow a growing source |
| clio-agent-marketplace | submodule pin | `develop` dbc82afa8 | Plugins are `plugins/<name>/plugin.json` on the agent-plugins.org 1.0.0 schema, with `extensions.ai.iowarp.clio.follows` and `connections`. The ParaView plugin follows a connector entry. No relay plugin exists |
| jarvis-cd | v1.8.1 9c917c2 | same, read-only | Not on PyPI, installed from the git tag. Packages are `Application`, `Service`, `Interceptor` with `_configure_menu`, `start`, `stop`, `status`, `clean`. No output descriptor, progress record or self-test API. `builtin/packages/xcompact3d` (2DECOMP&FFT 2.0.4, ADIOS2, `adios2-bp`), `gray_scott`, `gray_scott_paraview`, `paraview` (`service.py` `ParaViewBackend`, `scene_manifest.py`) exist |
| psij-python | 0.9.11 | same | `ResourceSpecV1` is required. The bare `ResourceSpec` is abstract (found on Ares) |
| frp | relay pins 0.69.1 | same | Ares has `frpc` and `frps` 0.69.1 in `~/.local/bin`, off the non-interactive `PATH` |

## Branches and the ledger

- **Branch.** `jaimecernuda-relay-rework` in every repo is the integration branch, and the draft PR goes from it to `develop` (to `main` in clio-kit). A repo with one workstream (gact-tui, clio-kit, clio-jarvis, clio-agent-marketplace) works on it directly. A repo with several (clio-relay, clio-agent) gives each session `jaimecernuda-relay-rework-<ws>` (for example `jaimecernuda-relay-rework-w7`), and the session merges into the integration branch with a merge commit when its gate passes. Nobody rewrites the integration branch.
- **Ledger.** `docs/design/relay-rework-ledger.md` in clio-relay. W1 creates it in its first commit with the branch name in the header and one section per workstream. A session writes only in its own section: its decisions with the reason, the facts the contract asks for, and its qualification evidence (commands, handles, screenshots read). Separate sections keep merges clean. W16 writes the summary table at the top.
- **The contract in the repo.** W1 commits `clio-relay-plan.md` as `docs/design/relay-rework-plan.md` with the amendments in this file, so later corrections are versioned in the same change as the code.
- **One checkout per session.** Every session works in its own clone or worktree, locally and on Ares (`~/clio-relay-rework/<repo>-<ws>`). No two sessions share a git index. Only the repo's integrator pushes the integration branch: W1 for clio-relay, W3 for clio-agent, and the single session for every other repo.

## Shared files have one writer

Ownership below is by explicit file or by a directory the owner creates, and globs are checked for overlap before any session starts. A file that several workstreams need to change has one writer, and everyone else sends that writer a request.

| Shared file | Writer | How others change it |
| --- | --- | --- |
| clio-relay `pyproject.toml`, `uv.lock` | W1, for the whole rework | W1 declares every adapter entry point and extra up front (`local`, `ssh`, `worker`, `globus`, `iri`, `sky`, `tapis`). A new dependency is a request to W1, which lands it the same day |
| clio-relay `cli.py` (the command root) | W1 | Each workstream adds commands in its own module (`cli_asks.py`, `cli_site.py`, `adapters/worker/cli.py`) and W1 registers the group once |
| clio-relay `catalog/__init__.py`, the catalog schema, `catalog/sites/` and `catalog/skills/` index files | W1 | Site YAML and skill files are per file: `ares.yaml` W7, TACC and Chameleon entries W10, facility entries W11 |
| `docs/design/relay-rework-ledger.md` | Its header and summary: W1, then W16. Sections: their workstream | A session edits only its own section |
| clio-relay `docs/` index and `mkdocs` or README links | W1 | Pages are per owner. W1 adds the links |
| clio-relay `.github/workflows/*` | W8 | Rewritten to run ruff, pyright and pytest on the new tree, with the release flow reduced to build and publish |
| clio-relay `src/clio_relay/__init__.py`, `surface/__init__.py` | W1. W1 does not create `surface/a2ui/` | W14 creates and owns `surface/a2ui/` |
| clio-relay `identity/__init__.py` and the store | W6 | W10 owns only `identity/tacc.py` and registers through W6's provider table |
| clio-agent `pyproject.toml`, `uv.lock` | W4 | W3 sends requests |
| gact-tui, clio-kit, clio-jarvis, marketplace | The single session in that repo | |

## Old and new surfaces during the rework

The old server stays the default entry point until the new one can replace it. W1 ships the new surface as a separate console script, `clio-relay-next`, with its own tests. It is never registered alongside the old tools in one server. W8 runs a caller and import inventory and writes it into the ledger, then turns it into an enumerated deletion list rather than globs. The gate for the switch is the worker conformance tests, plus LQ12 to LQ15 run by W9 against `clio-relay-next` on Ares. W16 repeats LQ15 later, but the switch does not wait for wave 3. When the gate passes, W8 asks W1, which owns the packaging, to point `clio-relay` (the console script and `python -m clio_relay`) at the new surface. W8 then deletes the old exposure and runs LQ1 and LQ32 against the installed wheel. Until W13 lands, the new surface refuses any site marked guarded with the typed reason `guard_unavailable`, so the switch never runs a guarded site unguarded. The installable 1.6.8 release stays on PyPI for anyone who needs the old surface. The new server has no compatibility path for it.

RL35 keeps bootstrap, the queue, leases, frp, the service runtime, and the `remote_agent` and `bounded_command` packages. They are moved, not deleted, and W8 tests the retained packages from the installed wheel. W9 alone owns the service runtime, `models_gateway.py` and `frp_*`, and migrates their tests. W8 never deletes a module W9 owns, and W8's deletion list is checked against W9's ownership before it runs.

## Workstreams

Sixteen sessions. Each owns its files outright. "Reads" means it may import or call a module, never edit it. A change needed in a file someone else owns goes to that owner as a request through the coordinator.

### W1. Relay core (clio-relay)

**Scope.** RL0, RL1, RL2 and RL2a, RL3 (the server half), RL4, RL5, RL6, RL7, RL8, RL9 (`local`), RL15, RL16 (the digest model and store layout), RL17 (the record interface), RL34 (serving skills as resources). The typed refusal reasons. The ledger skeleton and the in-repo contract.

**Owns.** New packages `src/clio_relay/surface/` (the MCP server, the five verbs, task plumbing on `fastmcp-tasks`, capability negotiation for A2UI and its absence), `src/clio_relay/sites/` (models, `sites.yaml`, catalog loader, verb routing), `src/clio_relay/adapters/base.py`, `adapters/registry.py`, `adapters/local.py`, `src/clio_relay/jobs/` (job spec on PSI/J names, service spec, inputs and digests, record interface, refusal reasons), `src/clio_relay/testing/conformance/`, `src/clio_relay/catalog/__init__.py` and the catalog schema. `pyproject.toml` and `uv.lock` (see "Shared files have one writer"), `cli.py` as the command root, `src/clio_relay/sites/store.py` (the site store the command line and the tab both call), `cli_site.py`. `docs/design/relay-rework-plan.md`, this file, the overview diagram, the ledger skeleton, `docs/ai/interface-context.md`, `docs/mcp-tasks.md`, the page on writing an adapter, the docs index.

**Interfaces.** W1's first milestone, M1, is a runnable slice merged before wave 1 starts. On Ares it serves `describe`, runs one ordinary `run` and one negotiated task on the `local` adapter and on a fixture adapter, answers one form and one URL elicitation through stubs, and ships the conformance kit as executable contract tests. It publishes `Adapter` and its `Config`, `SiteInfo`, `JobSpec` (PSI/J field names, built into `ResourceSpecV1`), `ServiceSpec`, `JobHandle`, `Endpoint`, `ExecResult`, `FileAccess`, the refusal reasons, the ask interface W6 implements (`ask.choose`, `ask.approve`, `ask.url`), the guard hook W13 implements (`guard.check_run`, `guard.check_exec`, `guard.price`), the serve hook W9 implements, the durable operation record, and the content-store layout. These are owned rather than frozen. A change goes through W1, which reruns every consumer's contract tests before merging it. After M1, W1 stays on as the clio-relay integrator: it merges the session branches, owns the shared files, and finishes RL34, RL15 and the docs.

**Proves.** LQ2, LQ4 for `local`, LQ41, LQ31a. LQ1 and LQ3 are accepted by W16, because LQ1's mounted-tool clause needs W9 and LQ3's form needs W14. W1 owns their relay-side tests.

### W2. Fixtures and the Ares harness (clio-relay)

**Scope.** Every fixture in the contract's Qualification section except the Tapis stub (W10) and the CLIO fixture plugin (W3): an SSH host in a container, the same with Slurm, the same with PBS, an SSH host whose sign-in asks for a one-time code, an frp rendezvous host, a small HTTP and WebSocket service that checks a secret, a small MCP server that keeps a counter. A script that brings them up and down on Ares under one compose project and label.

**Owns.** `tests/fixtures/sites/` (Dockerfiles, compose file, fixture services), `scripts/ares/` (bring-up, tear-down, port map, the env file the tests read), `tests/test_fixtures_smoke.py`.

**Rule.** Every container, network and volume carries the label `clio-relay-rework=1` and the compose project `clio-relay-rework`. Tear-down removes only that label. Nothing else on Ares is touched.

**Proves.** No LQ row. Its gate is a smoke test per fixture on Ares, recorded in the ledger.

### W3. Host semantics backend (clio-agent)

**Scope.** HS1 and HS1a, HS2, HS3 (manifest and resource reading), HS4 (grant files, scope merging, the `data_source/login` fallback), HS5. A fixture plugin and fixture MCP server that exercise all five and name nothing relay-specific.

**Owns.** `src/clio_agent/plugins/manifest.py`, `plugins/loader.py`, `plugins/servers.py` and the MCP child-environment builder, the tool-result path that will read `application/a2ui+json` (new module under `gact/a2ui_producer/` for server-owned surfaces), `gact/a2ui_actions/dispatcher.py` (route to the owning server as `tools/call`), `gact/elicitation_config.py`, `gact/elicitation_schema.py`, the URL check in `tools/mcp_handlers.py`, `gact/storage/auth.py` and `gact/storage/globus_consent.py` for added scopes, new `tests/fixtures/plugins/host_semantics/`, the developer-guide page on the five host semantics.

**First deliverables** (before W12 and W14 start):
- The wire contract in the ledger: the A2UI client capability key and versions, how a tab names its resource, the grant file format, how a surface's owner is stored, and the HTTP or event shape gact-tui reads.
- O9 settled by experiment. The fixture plugin declares its own MCP server command in `plugin.json`, with no entry in `catalogs/connectors/`, and is installed and started. If the manifest cannot carry a server command today, W3 adds that as a generic manifest capability, available to every plugin, and records the scope in the ledger. Relay never gets a connector entry or any other exception.

**Proves.** LQ22, LQ23, LQ24 (backend), LQ25, LQ26.

### W4. CLIO removals and SSH through the fence (clio-agent)

**Scope.** RL36, RL37, RL38, in that dependency order: RL37 lands and is proven before `ssh_execute` is removed. On Linux inside the real Codex fence on Ares, W4 checks `ssh`, `scp`, `sftp` and Git over SSH through the CONNECT helper, the `net.egress` evidence, and that a host outside the policy is refused. Only then does it delete the tool, check that recorded `ssh_execute` rows still render generically, and check that the Infrastructure tab's SSH still works. The Windows fence is LQ-L7, run by the person.

**Owns.** `gact/ssh_tools.py`, `gact/infrastructure/ssh_execution.py`, `gact/infrastructure/ssh_result.py`, `gact/ssh_recorded_presentation.py` and its two importers (`tool_result_presentation.py`, `session_export_projection.py`) for the import only, `gact/agents/native_tool_registry.py` for the one registration, the SSH test files, `runtime/sandbox.py` (the `ssh` launcher inside the fence), a new CONNECT helper module under `runtime/`, `gact/infrastructure/drivers.py`, `tools/gateway.py`, the stale comments in `agent.py`, `gact/provenance_wiring.py`, `tools/mcp_connection_era.py`, `tools/mcp_discovery.py`, `errors.py`, `tools/fs_write.py`, `conf.py`, the `relay` marker in `pyproject.toml` and the tests that carry it.

**Proves.** LQ29, LQ30 and LQ31 for clio-agent.

### W5. clio-jarvis (new repository, jarvis-cd read-only)

**Scope.** RL39, RL40, RL41, the release RL42 installs. The contract parts of RL40 are a thin layer on top of jarvis-cd's `Application` and `Service` in clio-jarvis, plus a validator that refuses a package missing any part and names it. jarvis-cd is not changed. If a part cannot be expressed without a jarvis-cd change, the session stops and asks.

**Owns.** The whole clio-jarvis repository: the contract layer, the validator, `schemas/view-descriptor.v1.json` (the descriptor relay reports and clio-kit's `open_view` takes; W9 and W15 vendor it), `pvserver`, `gray_scott`, `xcompact3d`, the self-tests, a tagged release. The ledger fact "which `builtin` packages relay 1.6.8 calls" (read from clio-relay develop).

**Until the repository exists** (owner blocker O1): work in a local git repository in `~/clio-relay-rework/clio-jarvis` on Ares and push when it is created.

**Proves.** LQ33, LQ34.

### W6. Asks and identities (clio-relay)

**Scope.** RL25a's out-of-band answer and approval page, RL27, RL28, RL29, RL30 except TACC, HS4a on the relay side. Form and URL elicitation helpers, and the `input_required` path inside a task. Relay's page (single-use, expiring, nothing logged), on loopback over stdio and under relay's own origin over Streamable HTTP. The `SSH_ASKPASS` helper with `SSH_ASKPASS_REQUIRE=force`. The keyring store. Grant-file reading, re-read on change. The Globus native-app sign-in and GitHub device flow for hosts that hand relay no grant. Probing catalog sites after a grant changes. Exactly-once submission of an approved ask, across restarts. `clio-relay asks` commands.

**Owns.** `src/clio_relay/asks/`, `src/clio_relay/identity/` (except `identity/tacc.py`), `src/clio_relay/cli_asks.py`.

**Ledger facts.** Which OpenSSH versions honour `SSH_ASKPASS_REQUIRE`, Windows included.

**Proves.** LQ42. Carries the page and askpass halves of LQ10, LQ11, LQ19, LQ20 and LQ39, which their owners run.

### W7. The `ssh` adapter and scheduler jobs (clio-relay)

**Scope.** RL10, RL16 staging over `scp`, RL17 job directory, RL18 on the `ssh` side, the detached no-scheduler path, `interactive_auth_required`. The Ares catalog entry and its skill.

**Owns.** `src/clio_relay/adapters/ssh/`, `src/clio_relay/catalog/sites/ares.yaml`, `catalog/skills/ares.md`, the `ssh` adapter doc page.

**Proves.** LQ4 for `ssh`, LQ5, LQ6, LQ7, LQ8, LQ9, LQ-L1 (job and file transfer).

### W8. The `worker` adapter and the old surface's removal (clio-relay)

**Scope.** RL3 (operator commands), RL11, RL18 on the worker side, RL35. The existing bootstrap, queue, leases and one-link connection go behind `worker`. Everything in the coupling table marked W8.

**Owns.** `src/clio_relay/adapters/worker/` (new), and the enumerated deletion list built from its inventory, covering the old `mcp_*` modules, `fastmcp_server.py`, `http_api_*`, the old `cli_*` modules (not `cli.py` or the new `cli_asks.py` and `cli_site.py`), `endpoint_*`, `scheduler_providers/`, `browser_gateway*`, `live_acceptance*`, `live_validation*`, `jarvis-packages/clio_relay/clio_relay/mcp_call/` and only what it alone uses (`remote_agent` and `bounded_command` stay), the old tests for all of these, `.github/workflows/*`, `docs/remote-mcp-federation.md`, `docs/connection-model.md`. It moves, rather than deletes, `bootstrap_*`, `queue_*`, `core_queue.py`, leases and the `jarvis_*` modules the worker uses, into `adapters/worker/`. Never anything W9 owns. The cutover in "Old and new surfaces during the rework". The `worker` adapter doc page.

**Proves.** LQ4 for `worker`, LQ10, LQ11, LQ32, the six existing workflows' replacement.

### W9. `serve` (clio-relay)

**Scope.** RL19 to RL23. Today's `ServiceRuntimeSpec` and gateway sessions become `ServiceSpec`. The frp client beside a service, the hop through the site's worker or an HTTP proxy, XTCP, the `ssh -L` path for an `ssh` site with no rendezvous host, durable re-binding, mounting a served MCP server under `<site>_<name>_` with one held session, the port-protection refusal, stop with a grace period and artifacts. The rendezvous host page.

**Owns.** `src/clio_relay/serve/` (new), `service_runtime*.py`, `jarvis_service_runtime*.py`, `models_gateway.py`, `frp_*.py`, `docs/rendezvous-host.md`. Adapters call `serve.start_for(adapter, spec)`. They never reach into `serve/`.

**Proves.** LQ12, LQ13, LQ14, LQ15, LQ-L1 (service), LQ-L11.

### W10. Tapis (clio-relay)

**Scope.** RL46 to RL50, with the TACC identity (device code, keyring, refresh), the launcher tar, app and system registration, the key pair for a person's own host, `TMS_KEYS` for TACC hosts, and the Tapis stub.

**Owns.** `src/clio_relay/adapters/tapis/`, `src/clio_relay/identity/tacc.py`, `catalog/sites/{chameleon-tapis,ares-tapis,vista,stampede3}.yaml` and their skills, `tests/fixtures/tapis_stub/`, the `tapis` adapter doc page.

**Ledger facts.** The `tapipy` 25.4.0 operation behind each call, the TACC pages behind the catalog entries, the registration results for Ares and Chameleon, the refresh-token lifetime.

**Proves.** LQ35 to LQ40, LQ-L14, LQ-L15, LQ-L17.

### W11. `globus`, `iri` and `sky` (clio-relay)

**Scope.** RL12, RL13, RL14 against recorded fixtures and mocks, the Polaris, Aurora and DeltaAI catalog entries and skills from the facilities' own pages, the spend price from SkyPilot. hpc-bridge is a reference, not a dependency.

**Owns.** `src/clio_relay/adapters/{globus,iri,sky}/`, `catalog/sites/{polaris,aurora,deltaai}.yaml` and skills, their doc pages.

**Ledger facts.** Facility pages, Globus Compute user-configuration keys, IRI API version and token type.

**Proves.** Unit and mock tests only. LQ-L2 to LQ-L6 and LQ-L8 are the person's, and this session writes their run sheets.

### W12. gact-tui host semantics and removals (gact-tui)

**Scope.** The UI half of HS1 to HS3 and HS5: render a server-owned surface in the conversation, a generic plugin tab kind in `WorkbenchTab` filled by a resource and refreshed on `resources/updated`, sending actions to the owning server, showing loopback and plugin-named origins in a URL question. The gact-tui removals from the coupling table.

**Owns.** `web/src/components/clio/workbench-shared.ts`, `workbench-tab-content.tsx`, `use-workbench-tab-state.ts`, the A2UI surface and action files under `web/src/lib/a2ui/`, `question-interaction-forms.tsx`, `infrastructure-topology.tsx`, `tool-operation-contract.ts`, `tool-operation-outcome.ts`, `web/src/lib/tool-kind-icons.ts`, `scripts/check_infrastructure_ownership.mjs`, `web/tests/review/service-methods-catalog.json`, the Rust test reference.

**Starts** when W3's wire contract is in the ledger.

**Proves.** LQ22, LQ23 and LQ24 in the UI, LQ30 and LQ31 for gact-tui.

### W13. Guardrails (clio-relay)

**Scope.** RL24 to RL26, RL42 on the relay side (adding clio-jarvis at a tagged release on a site's JARVIS), package review records pinned by hash, the read-only `exec` list, the spend gate on top of W6's asks, the warm allocation with a ten-minute idle release. The guarded-sites doc page.

**Owns.** `src/clio_relay/guard/`, `src/clio_relay/jarvis_site.py` (new).

**Proves.** LQ16, LQ17, LQ18, LQ19, LQ20, LQ21.

### W14. Relay's CLIO plugin and the Compute tab (clio-relay and clio-agent-marketplace)

**Scope.** RL31 to RL33, RL50's row. The plugin manifest with its server command (as settled by W3 under O9), A2UI catalog, `tabs`, `accounts`, loopback and `https://tacc.tapis.io` origins, and the grant-file mapping of HS4a in the launch configuration. The tab resource and its surfaces (site rows, the add-site form generated from each adapter's `Config` schema, the adapter switch, Connect, Probe, Edit, Remove), using only components CLIO already renders. Every action is an unlisted tool that relay accepts only with the single-use action token from the surface it rendered, and it calls W1's site store, so the tab adds no behaviour the command line lacks. Publishing to the marketplace. The CLIO user-guide page on the Compute tab.

**Owns.** `plugin/` in clio-relay (manifest, catalog, skills index), `src/clio_relay/surface/a2ui/` (surface builders; W1 leaves the directory empty), `plugins/clio-relay/` in clio-agent-marketplace and its pin.

**Proves.** LQ27, LQ28. Writes the run sheet for LQ-L7 and LQ-L18.

### W15. ParaView follows a growing source (clio-kit)

**Scope.** RL44 on the ParaView server that is already on `main`: `open_view` takes the view descriptor (vendored from clio-jarvis), the server re-reads the reader's time list when the source grows, a view following the latest step moves, a parked view stays and shows newer steps exist, each step goes out through `ui/update-model-context` and as task progress.

**Owns.** `mcp-servers/paraview/` in clio-kit.

**Proves.** LQ-L12.

### W16. Integration and qualification (all repos)

**Scope.** Freshly restarted stack, every row of the first table plus LQ41 and LQ42, the Ares rows LQ-L1, LQ-L9, LQ-L11, LQ-L12, and LQ-L14, LQ-L15 and LQ-L17 once the TACC sign-in exists. Acceptance of the joined rows no single owner can sign: LQ1 (exactly five tools plus a mounted server), LQ3 (an outside adapter and its generated form), LQ15 (termination output and the host's artifact registration), LQ33 and LQ34 (descriptors and progress, and a deliberately broken package refused), LQ39 and LQ40 (no secret kept, and keys and credentials cleaned up), LQ-L12 (following and parked views). Done items 1 to 7. Screenshots at 1440 by 900 and 390 by 844. Submodule pins. Draft PRs with evidence. The ledger summary. A mock, a text search or an owner-pending row is never counted as a live pass. LQ-L17 keeps its contract exception: if Tapis's hold on an Ares key is refused, the rejection is recorded and LQ-L14 qualifies. That exception does not extend to any other Ares row.

**Owns.** The ledger summary table, `docs/design/relay-rework-qualification.md` (run log), PR descriptions. It fixes nothing in place. A failure goes back to the owning workstream.

**Proves.** LQ1, LQ3, LQ-L9 and the Done list.

## Order

```
wave 0  W1 core   W2 fixtures   W3 host semantics   W4 CLIO removals   W5 clio-jarvis   W15 ParaView follow
          |  interfaces frozen and merged      |  wire contract in ledger
wave 1  W6 asks   W7 ssh   W8 worker and removal   W9 serve   W10 tapis   W11 globus, iri, sky   W12 gact-tui
wave 2  W13 guardrails (needs W6, W7)     W14 plugin and Compute tab (needs W1, W3, W6, W12)
wave 3  W16 integration and qualification
```

| Edge | Why |
| --- | --- |
| W1 before W6 to W11, W13, W14 | The adapter protocol, specs, refusal reasons and hooks are frozen in W1 |
| W2 before the fixture-backed gates of W7, W8, W9, W13 | They qualify against the fixtures. Their unit work starts without them |
| W3's contract before W12 and W14 | The UI and the plugin are written against it |
| W6 before W8's LQ10 and LQ11, W13's LQ19 and LQ20, W10's LQ39 | They need the page, askpass and elicitation |
| W7 before W13's LQ21 | The warm allocation needs a scheduler path |
| W9 and W15 before LQ-L11 and LQ-L12 | Service and following view |
| W5's release before W13's RL42 and LQ33 | A tagged clio-jarvis to add |
| Everything before W16 | |

## Qualification ownership

| Rows | Owner | Where |
| --- | --- | --- |
| LQ2, LQ4 (`local`), LQ31a, LQ41, LQ43 | W1 | Local unit plus Ares |
| LQ1, LQ3 | W16 accepts, W1 owns the relay-side tests | Ares plus the owner's stack |
| LQ4 (`ssh`), LQ5 to LQ9 | W7 | Ares fixtures |
| LQ4 (`worker`), LQ10, LQ11, LQ32 | W8 | Ares fixtures |
| LQ12 to LQ15 | W9 | Ares fixtures |
| LQ16 to LQ21 | W13 | Ares fixtures |
| LQ22 to LQ26 | W3 (backend), W12 (UI) | Ares for backend tests, UI checked on the owner's stack by W16 |
| LQ27, LQ28 | W14 | W16's stack |
| LQ29, LQ30, LQ31 | W4 (clio-agent), W12 (gact-tui). W16 signs off across the repos | Ares (LQ29 needs Linux) |
| LQ33, LQ34 | W5 | Ares |
| LQ35 to LQ40 | W10 | Ares, Tapis stub |
| LQ42 | W6 | Ares |
| LQ-L1 | W7 leads and runs it, W9 supplies the service half | Ares |
| LQ-L9 | W16 | Ares |
| LQ-L11 | W9 | Ares |
| LQ-L12 | W15 | Ares |
| LQ-L14, LQ-L15, LQ-L17 | W10 runs them once the person has signed in. The person supplies credentials and is not the runner | Chameleon, Ares, after O3 to O6 |
| LQ-L2 to LQ-L8, LQ-L10, LQ-L13, LQ-L16, LQ-L18 | Person, run sheets by W11, W14 | Facilities, Windows, clouds |

### Requirement to owner

Every contract item has one implementing owner. W16 accepts the joined rows above and the Done list.

| Items | Owner |
| --- | --- |
| RL0, RL1, RL2, RL2a, RL3 (server half), RL4 to RL8, RL9 (`local`), RL15, RL16 (store), RL17 (record), RL34 | W1 |
| RL3 (operator commands), RL11, RL18 (worker side), RL35 | W8 |
| RL10, RL16 (staging), RL17 (job directory), RL18 (`ssh` side) | W7 |
| RL12, RL13, RL14 | W11 |
| RL19 to RL23 | W9 |
| RL24 to RL26, RL42 (relay side) | W13 |
| RL25a (asks), RL27 to RL30 (TACC excepted) | W6 |
| RL31 to RL33 | W14 |
| RL36 to RL38 | W4 (clio-agent), W12 (gact-tui) |
| RL39 to RL41, RL42 release | W5 |
| RL43 (simulation to tab chain), RL45 (guarded offline visualization) | Built from W5 (packages, descriptors), W9 (serve bound to the job), W13 (guarded `pvserver`) and W15 (following view). The open-site chain is accepted by W16 through LQ-L9 and LQ-L11 on Ares. The guarded Xcompact3d chain is LQ-L10, and RL45's "the dataset never leaves the site" is LQ-L13 on Polaris. Both are person rows, and W13 writes their run sheets |
| RL44 | W15 |
| RL46 to RL50 | W10 (RL50's plugin row W14) |
| HS1 to HS5, HS1a | W3 (backend), W12 (UI) |
| HS4a | W6 (relay side), W14 (launch mapping), W3 (host side) |
| Ledger facts the contract asks for | The owner of the matching RL row, in its own section |
| Adapter, rendezvous and guarded-site pages | W1 (writing an adapter), W7 to W11 (each adapter), W9 (rendezvous), W13 (guarded sites) |
| CLIO developer guide (host semantics), CLIO user guide (Compute tab) | W3, W14 |
| The six existing workflows | W8 |
| Pushed child commits, submodule pins, draft PRs and their evidence | W16 |


## Ares

Set up by the coordinator. Do not redo it.

- **Work area.** `~/clio-relay-rework` (under `/mnt/common/jcernudagarcia`). Nothing else in the home directory was touched.
- **Python.** `.venv` with Python 3.12.0 from `uv` 0.13.9 (`~/.local/bin/uv`): `psij-python` 0.9.11, `jarvis-cd` 1.8.1 (from the git tag), `tapipy` 25.4.0, `globus-sdk` 4.10.0, `globus-compute-sdk` 4.21.0, `mcp` 2.3.0, `pydantic` 2.14.3, `pyyaml`.
- **Clone.** `clio-relay` on `develop` 034117b.
- **frp.** `frpc` and `frps` 0.69.1 in `~/.local/bin`. Use full paths, since they are not on the non-interactive `PATH`.
- **Slurm.** slurm-wlm 21.08.5, partitions `compute` (default) and `debug`. A psij `JobExecutor("slurm")` probe ran `/bin/hostname` on `debug` as job 27670, `COMPLETED`, exit 0, on ares-comp-04.
- **Ports.** Binding loopback ports works.
- **Containers.** docker and podman, the user is in the docker group. Existing containers are not touched (W2's label rule).
- **Not present.** lmod (`module`), SkyPilot, ParaView or `pvserver`, Xcompact3d.

### Still to install, and who does it

| Item | Needed by | How |
| --- | --- | --- |
| Per-session venvs from each repo's lock (`uv sync`) | Every session | The session itself, under `~/clio-relay-rework/<repo>-<ws>` |
| A conda-forge environment from micromamba in `~/clio-relay-rework/micromamba` with `paraview>=6` (headless, gives `pvserver`, `pvpython`, `pvbatch`), `adios2` built with MPI, `openmpi`, `mpi4py`, `cmake`, `cxx-compiler`, `fortran-compiler`, `fftw` | W5 (gray_scott, xcompact3d, pvserver self-tests), W9 (LQ-L11), W15 (LQ-L12), W16 (LQ-L9) | W5 at the start of its work, so it is ready before wave 1. First check whether Ares has a system MPI that Slurm's `srun` expects, and if it does, build ADIOS2 against it instead |
| Xcompact3d with 2DECOMP&FFT 2.0.4 and ADIOS2, from source in `~/clio-relay-rework/build` | W5 (`xcompact3d` self-test), LQ-L15 second half | W5, after O2 hands over the working configuration |
| ADIOS2 Gray-Scott example built against that ADIOS2 | W5, W16 (LQ-L9) | W5 |
| `keyrings.alt` in test venvs | W6, W10 (Ares is headless with no Secret Service) | The session, tests only. The production default stays the OS keyring |
| SkyPilot in its own venv | W11 import and mock tests only | W11. No cloud credentials go on Ares |
| Fixture images | W2 | W2, labelled |

## Owner blockers

Session provisioning (the micromamba stack, builds, venvs, fixture images, jarvis-cd from its git tag) is done by the sessions themselves under "Still to install". The items below need the person.

| Id | Item | Gates |
| --- | --- | --- |
| O0 | Child sessions on this machine need shell, `ssh` and worktree writes approved. This planning session was refused every shell command and every file write inside its worktree under unattended autopilot, sub-agents included | Every implementation session. Without it, nothing can be built, tested on Ares or committed |
| O1 | Create the empty `iowarp/clio-jarvis` repository, and give the session push and tag rights | W5's push and tag, W13's RL42, W10's guarded staging, LQ33 in W16. W5 can start in a local repository |
| O2 | Hand over the working Xcompact3d configuration: the ADIOS2 XML and the ParaView reader settings | W5's `xcompact3d`, LQ-L10, the second half of LQ-L15 |
| O3 | A TACC account, and the TACC sign-in when relay first asks | LQ-L14, LQ-L15, LQ-L16, LQ-L17 |
| O4 | One OAuth client for relay in the `tacc` tenant, id and key handed over | Live device-code sign-in with a refresh token (RL48). The stub rows do not need it |
| O5 | A leased Chameleon node with a floating IP, and an SSH profile for it | LQ-L14, LQ-L15 |
| O6 | Confirm Ares accepts SSH from outside IIT with a key and no second factor, and that a key held by TACC's Tapis is acceptable | LQ-L17 |
| O7 | Ask TACC what "full access" covers, the demo date, whether the NAIRR Vista system can be opened meanwhile, and which Tapis systems to use for Vista and Stampede3 | LQ-L16 and its `system_id` |
| O8 | Run the person rows: Polaris, Aurora, an IRI facility, AWS, DeltaAI, Windows, a third-party agent host | LQ-L2 to LQ-L8, LQ-L10, LQ-L13, LQ-L16, LQ-L18 |
| O9 | Decide whether relay's plugin may carry its own server command in `plugin.json`, or needs a connector entry in clio-agent's `catalogs/connectors/` the way ParaView does. A connector entry named relay in clio-agent would break the contract's rule 1. W3 settles it by experiment first. The person decides only if a generic manifest capability is needed and its scope is in question | W14's install path |
| O10 | Register a Globus native-app client for relay's own sign-in (the path for hosts that hand relay no grant), and hand over its id | Standalone Globus sign-in in W6, LQ42's sign-in step against live Globus. Stubs do not need it |
| O11 | Facility allocations: Vista, Polaris, Aurora, DeltaAI. Consent to spend on a paid cloud account for LQ-L8 | The matching person rows |

## Gaps

- `plans/remote-visualization-scope.md`. The seven questions are answered inline in RL19 to RL23, so W9 works from those. The ledger records that the source document was not read.
- `plans/remote-connectors-plan.md`. The contract says it is not changed. The open point is O9.
- The MCP specification revision `2026-07-28`. Settled in "Protocol baseline": `fastmcp-tasks` implements SEP-2663, and `mcp` 2.3.0 does not yet. W1's M1 runs the extension's conformance scenarios (the nine `tasks-*` server scenarios) against relay and records the result in the ledger before publishing the interfaces.
- `ResourceSpecV1`. The job spec keeps PSI/J's field names. W1 builds a `ResourceSpecV1` from them.

## Review record

### Round 1, gpt-6.2-astra

The reviewer read the contract but could not open this file, so it reviewed against the contract and a summary of this plan. Eleven findings.

| # | Finding | Outcome |
| --- | --- | --- |
| 1 | `clio-relay asks answer` cannot be the only approval path: an HTTP client may have no shell on the relay machine, and an agent that can run it defeats the gate. With tasks, use `input_required` and `tasks/update` | Taken. RL25a now uses a single-use approval page served by relay, with the CLI as an optional operator path. Tasks use `input_required`. Decline, no answer, duplicates and restart are tested in LQ42. The limit (a host without elicitation cannot prove a person approved) is written down |
| 2 | A non-task handle must be usable from a fresh connection, for `files` too, and must not pose as a task result | Taken. RL2a spells out finding, polling, cancelling, reading outputs and stopping through the five verbs' own schemas. LQ42 reconnects |
| 3 | W8 deleting by glob could delete what W9 and RL35 keep | Taken. W9 solely owns the service runtime, `models_gateway.py` and frp. W8 works from an inventory-based list, moves rather than deletes RL35's parts, and switches the entry point only after worker conformance and LQ12 to LQ15 pass. New section "Old and new surfaces during the rework" |
| 4 | Directory ownership collides (`pyproject.toml`, `cli.py`, `identity/`, `catalog/`, `surface/a2ui/`, ledger, docs index, workflows), and sessions must not share a git index | Taken. New section "Shared files have one writer", one checkout per session, one integrator pushes each integration branch |
| 5 | W1 should ship a runnable slice, not a paper freeze. LQ1's mounted tools and LQ3's form depend on later work | Taken. M1 is a runnable slice with executable contract tests. Interfaces are owned by W1 and change with consumer tests rerun. LQ1 and LQ3 move to W16 for acceptance. The reviewer's idea of a separate first session is not taken: M1 inside W1 does the same with one fewer hand-off |
| 6 | Tab actions must not become a capability only CLIO has, and relay must not get a connector exception | Taken. Tab actions are app-only tools over W1's site store, which the command line also uses. Site management is an operator task for other hosts by design. W3 settles O9 with a fixture plugin before W14 starts |
| 7 | Capabilities are per request in 2026-07-28. Grant mapping belongs in launch configuration. Cover GitHub too. Loopback is wrong for a remote HTTP relay | Taken in HS1a, HS4a, RL25a and W6 |
| 8 | RL37 has to be proven inside the real fence before RL36 removes `ssh_execute` | Taken in W4's scope |
| 9 | LQ31a by word search is too weak, and too strict for docs | Taken. LQ31a is now a clean-environment install plus a dependency and use audit |
| 10 | Joined rows and the Done list need an acceptance owner, and every RL and HS row an owner | Taken. W16 accepts the joined rows. New "Requirement to owner" table |
| 11 | Separate session provisioning from what only the person can give | Taken. Owner blockers now lead with that split. Added O10 (Globus native-app client) and O11 (allocations and paid-cloud consent). The LQ-L17 exception is limited to LQ-L17 |

### Round 2, gpt-6.2-astra

The revised sections were given to the reviewer inline. It confirmed that every RL and HS row has an owner. Six findings.

| # | Finding | Outcome |
| --- | --- | --- |
| 1 | The approval page still lets the requesting agent approve itself. It needs an operator credential, a GET that changes nothing, a POST bound to the exact operation, and exactly-once submission through a crash after the scheduler accepted the job | Taken. Over HTTP the page needs the operator's credential, separate from the MCP caller's. GET changes nothing. Approval is a CSRF-protected POST bound to operation, hash and charge. Relay records intent and the job name before submitting and reconciles by name. All of this is in LQ42. Not taken: a mandatory credential for a local stdio relay. The person and the agent share that machine, so no credential separates them. That case stays a documented limitation, with elicitation-capable hosts recommended for guarded sites |
| 2 | App-only visibility is the host's job, so a host that ignores it would show the tab's tools to the model | Taken, done differently from the suggestion. Tab actions are never in `tools/list` for any client. They are accepted only with a single-use action token that rides in A2UI payloads, and only clients that declared A2UI get those. LQ1 holds for every client, and LQ42 checks that a call without the token is refused |
| 3 | W8 would delete `jarvis-packages/`, which RL35 keeps. The packaging switch belongs to W1. Waiting for W16's LQ15 is a backwards gate. Guarded sites could run unguarded before W13 | Taken. Only `mcp_call` is deleted, and `remote_agent` and `bounded_command` are tested from the wheel. W1 makes the switch when W8 asks. W9 runs LQ12 to LQ15 against `clio-relay-next` before cutover. Guarded sites are refused with `guard_unavailable` until W13 lands |
| 4 | Transfer and service handles had no named follow-up, and a remote relay's loopback endpoint is not the desktop's | Taken. `files` with `transfer`, `serve` with `service`, and `clio-relay connect <handle>` on the desktop. The HTTP half of LQ42 runs from a second Ares node |
| 5 | `CLIO_RELAY_GRANT_FILE_*` conflicts with LQ31a's "no `CLIO_*` name" | Taken by narrowing LQ31a. `CLIO_RELAY_*` is relay's own settings prefix, already used across the codebase, and is neither a host name nor a host variable. Renaming it would be churn with no gain in portability. What LQ31a forbids is reading a host's variables, such as `CLIO_ACCOUNT_*` |
| 6 | RL45 cannot be proven on Ares. LQ30 and LQ31 need a cross-repo signer. LQ-L1, LQ-L14, LQ-L15 and LQ-L17 need a named runner | Taken. RL45 maps to LQ-L13, and the guarded Xcompact3d chain to LQ-L10, both person rows with run sheets from W13. W16 signs LQ30 and LQ31 across repos. W7 leads LQ-L1, and W10 runs the TACC rows after sign-in. LQ42 was already W6's |

Review closed after two rounds.
