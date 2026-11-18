# Relay rework ledger

Branch: `jaimecernuda-relay-rework` (integration, in every repository). Session branches in clio-relay are `jaimecernuda-relay-rework-<ws>`.

Contract: [relay-rework-plan.md](relay-rework-plan.md). Execution plan: [relay-rework-execution.md](relay-rework-execution.md). Overview: [relay-rework-workstreams.md](relay-rework-workstreams.md).

## How to write here

- Each session writes only in its own section. The header and the summary belong to W1, then W16.
- Record decisions with their reason, the facts the contract asks for with their source, and qualification evidence: the commands, handles and screenshots read.
- A mock, a text search or an owner-pending row is never recorded as a live pass.

## Summary

Written by W16 at qualification.

| Row | Owner | State | Evidence |
| --- | --- | --- | --- |

## Open items

Kept by W1 as the ledger's header owner. The coordinator assigns owners.

| Item | Raised by | Owner | Notes |
| --- | --- | --- | --- |
| Linux fence egress bypass | W4 | TBD (coordinator) | The Linux Codex profile grants network "enabled" (host netns), so egress that does not go through the CONNECT proxy is neither recorded nor gated. Tracked outside W4 |
| frpc over wss does not verify the frps certificate | W2 | W9 (owns `frp_*`) | frpc 0.69.1 logs in over wss without the CA, so it does not verify by default. Relay must set TLS verification explicitly. The fixture CA is `FX_WSS_CA_FILE` in the fixture env file |
| W12's 8 client-side contract assumptions | W12 | W3 | Closed. W3 answered all 8 in clio-agent `jaimecernuda-relay-rework-w3` at 25da48a8b (`docs/design/host-semantics-contract.md`): 1 to 6 confirmed; 7 and 8 corrected (re-read the open tabs on an SSE reconnect or `stream.gap`; tabs are per session). W12 applies them in iowarp/gact-tui#1320 |
| gact-tui relay references remain | W12 | W8, W14 | `managed-service-identity.tsx:28`, `infrastructure-service-labels.ts:14`, `test-fixtures/managed-services.ts`, `relay-settings.tsx`, the e2e fixtures, `domain.ts` `relay_job`, and `scripts/check_brand_literals` |
| Marketplace plugins/ssh still declares native_tool("ssh_execute") | W4 | W4 | Done. W4 deleted `plugins/ssh` in iowarp/clio-agent-marketplace, branch `jaimecernuda-relay-rework-w4` at 1dc6415 (from develop dbc82af), with a CHANGELOG entry and one reworded graspa skill line. The clio-agent submodule pointer is bumped at the final merge |
| Push clio-jarvis branch and tag v0.1.0 | W5 | W5 | Blocked on the user creating iowarp/clio-jarvis (O1) |
| Relay bootstrap still registers the jarvis-cd builtin repo | W5 | W1 (or whoever has RL39) | `bootstrap_reconcile_builtin_repos.py` and `managed_builtin_repo_registered`. Under RL39 it and the release-gate references move to `clio_jarvis.*` |
| O2: the Xcompact3d case | W5 | W5 | Resolved: TGV (Taylor-Green vortex) at Re 1600 |
| clio-kit develop is stale vs main; forward main into develop (precedent #386) before the final merge | W15 | Coordinator | develop 8375600b is 38 ahead and 175 behind main |
| clio-agent: `test_mcp_first_start_direct` times out on `server/discover` | W3 | TBD (coordinator) | Also times out on develop f08dbde3a. Not written off as flaky |
| clio-agent: `test_startup_imports` subprocess timeouts at load 45 to 60 | W3 | TBD (coordinator) | Not written off as flaky |
| clio-agent: two HS-suite timeouts under load | W3 | TBD (coordinator) | Not written off as flaky |
| clio-agent: develop fails the era gateway test | W3 | TBD (coordinator) | Pre-existing on develop |
| clio-agent: `clio-agent serve` takes about 17 s to exit on SIGTERM | W4 | Unassigned (clio-core) | Pre-existing at f08dbde3a. clio_run stop does not reach the private clio-core daemon (stop_undelivered), then it waits out the 12 s ceiling and hard-kills |
| clio-agent: 13 full-suite failures reproduce identically at develop f08dbde3a | W4 | TBD (coordinator) | test_runner (7, ImportError), test_copilot_auth_discovery (2), test_clio_core_restart_durability, the landlock cross-dir rename (EXDEV on NFS), release_install_policy desktop SDK roots (no gact-tui on Ares), and live_suite_runtime temp. Pre-existing or Ares environment; triage before the final merge |
| clio-agent: the file-size ratchet fails on 5 files from W3's HS work | W4 | W3 | Closed. W3 fixed it in 4878fcc40, with no baselines changed |
| clio-agent: `test_sync_tool_executor_setup_waits_while_its_server_starts` (a 1 s no-progress timing test) failed once under load on Ares and passed on rerun | W3 | TBD (coordinator) | Not written off as flaky. If it fails again under load, remove it or replace it with a deterministic check before the final merge |
| clio-relay: the old process-containment tests depend on a systemd user manager | W1 | W8 | On Ares compute nodes uid 1008 has no user manager unless a session happens to exist, so `systemd-run --user` fails with "Failed to connect to bus" and the failing set changes between runs (`test_process_containment`, `test_storage_process_guard`). The code is unchanged from develop. On the login node (linger, stable bus) both modules pass 43 of 43, 3 runs out of 3. Not written off as flaky: run them where a user manager exists, or make the capability probe and the spawn agree, before the final merge |
| clio-relay: develop's full suite has 139 pre-existing failures and 3 errors on Ares | W1 | TBD (coordinator) | origin/develop 034117b, same node type. Mostly the old surface (bootstrap receipts, JARVIS receipt binding, `/run/user` absent on compute nodes). Triage before the final merge |
| clio-relay: `tests/relay_ssh/test_ssh_fixtures.py:223` skips `test_lq9_a_code_site_refuses_and_names_worker[True]` when W6's askpass is installed | W1 | W7 | It skips on the integration branch, where askpass is always installed. A skip counts as a failure. Make the case hold with askpass installed, or remove the parameter |

## Decisions across workstreams

Kept by W1 as the ledger's header owner. Each is the coordinator's.

- **LQ14 reads as one server instance.** Raised by W2. Protocol `2026-07-28` has no MCP session: `ctx.session_id` changes on every call, over Streamable HTTP and over stdio. "One session" in LQ14 therefore means continuity of one server instance (and one client connection). W2's counter reports `server_instance` for this. W9 and W16 test against it.
- **clio-kit relay rework work targets main, per the execution plan's base and drift row.** Raised by W15. clio-kit's develop (8375600b) is the old layout, without mcp-servers/ and with ParaView 2.2.3, and has diverged from main (38 ahead, 175 behind, 80 conflicts). W15 stays on main; its branch line is updated when it sends its final commit with the paraview-mcp 3.2.0 bump.

## W1. Relay core

Branch `jaimecernuda-relay-rework-w1`, merged into `jaimecernuda-relay-rework` at M1.

### Decisions taken from W3's contract

Accepted from the coordinator on W3's two open points, with what they mean for relay:

- O9 is settled. Relay's plugin ships its server command in `mcp.json` beside `plugin.json`, with no connector entry.
- HS1a. Relay sends A2UI only when that request's `_meta["io.modelcontextprotocol/clientCapabilities"].extensions` carries `org.a2ui/a2ui`, and always beside the full text result.
- HS4a. Relay reads the grant file named by its own setting (`--grant-file globus=<path>` or `CLIO_RELAY_GRANT_FILE_GLOBUS`). The plugin maps it from `${CLIO_ACCOUNT_GLOBUS_FILE}` in `mcp.json`. Relay's code never reads the CLIO name.
- Tab actions arrive as plain `tools/call` with `arguments` set to the action context, so the single-use action token travels in the context.

### Protocol and pins

- MCP `2026-07-28` with the `io.modelcontextprotocol/tasks` extension is the primary path. `2025-11-25` clients are served too and use the RL2a handle path. Source: `src/clio_relay/surface/server.py` (`PROTOCOL_VERSION`).
- Python `mcp==2.3.0`, `fastmcp==4.2.1`, `fastmcp-slim[client,server]==4.2.1`, `fastmcp-tasks==4.2.1`, `psij-python` with `ResourceSpecV1`. Pinned in `pyproject.toml` and locked in `uv.lock`.
- fastmcp moved from 4.0.0b1 to 4.2.1 because pip-audit flagged GHSA-vqqp-x5qm-5qp4 (M2M OAuth issuer binding, fixed in 4.1.2). anyio 4.16.0, dnspython 2.9.2, filelock 4.2.12 and pyjwt 2.15.1 were upgraded in the lock for advisories that develop's lock also carries.

### Decisions

- **Relay is a plain MCP server.** Five tools: `describe`, `run`, `exec`, `files`, `serve`. There is no client-specific path. The proof clients are the plain `mcp` 2.3.0 `Client` and the fastmcp `Client`, over stdio and Streamable HTTP. Why: user rule 1.
- **Tasks first, handles as the fallback.** `run` is task-optional on `2026-07-28` (`fastmcp-tasks`, poll interval 2 s, TTL 7 days). The task id is the operation id. On `2025-11-25` the `run` listing says `taskSupport: forbidden`, because tasks there are only the extension, so that client gets an ordinary result or a handle. A call without a task waits up to `wait_s` (default 20, max 300) and then returns the handle. Every result carries `_meta["ai.iowarp.relay/operation"]`. Why: user rule 2.
- **Asks reach a person in four ways.** In a task: `input_required`, answered with `tasks/update`. Inline: form or URL elicitation when the client declares that mode. Neither available: a typed refusal (`needs_approval` or `needs_sign_in`) that carries a single-use loopback page address. And `clio-relay-next asks` on the command line. An answer is recorded once. An approval binds the site, the spec digest and the charge, so a changed spec is refused with `approval_mismatch`.
- **Policy.** Sites are `open` or `guarded`. A guarded site with no guard hook installed refuses with `guard_unavailable`, rather than running unguarded. Spend asks are `never`, `always` or a threshold.
- **Restart safety.** Operation records are written before submit. A submit interrupted by a restart is reconciled by the adapter's `find(operation_id)` (for `local` on a scheduler, the job name `relay-<operation_id>`) after a 2-minute grace, so a job is submitted once.
- **Extension points are entry points.** Adapters: `clio_relay.adapters` (`local`, `fixture`). Hooks: `clio_relay.hooks` (`asks` for W6, `guard` for W13, `serve` for W9), with stub defaults that keep M1 runnable.
- **Grant files.** Relay reads `CLIO_RELAY_GRANT_FILE_<PROVIDER>` or `--grant-file <provider>=<path>`. It never reads a CLIO name (HS4a above).
- **Change events at `2026-07-28`.** FastMCP 4.2.1 does not serve `subscriptions/listen`, so relay registers the SDK's `ListenHandler` over an `InMemorySubscriptionBus` itself (`src/clio_relay/surface/changes.py`). `RelayServer.changes.resource_updated(uri)`, `resources_list_changed()` and `tools_list_changed()` publish on the bus. They also send the bare notification to every `2025-11-25` connection relay has seen. Relay keeps the connection, because mcp 2.3's `ServerSession` lasts only one request. Why: W3's HS3 server obligation. W14's Compute tab resource calls `resource_updated`.
- **The old surface stays.** The new command is `clio-relay-next` beside the old `clio-relay` until W8 removes the old one ("Old and new surfaces during the rework").
- **Adapter extras and the universal lock (coordinator, wave 1).** uv's lock is universal, so an extra's caps apply to every install.
  - `tapis = ["tapipy==25.4.0"]`. Its openapi-core 0.16 lowers jsonschema-path to 0.3.4, pathable to 0.4.4 and referencing to 0.36.2. Accepted.
  - `globus` pins `globus-compute-sdk>=4.21,<4.22`. Without the cap, relay's `prerelease = "allow"` picks the 4.22.0a0 alpha. Every globus-compute-sdk caps `rich<15`, so rich is 14.3.4. Accepted.
  - There is no `sky` extra. skypilot 0.14.3 caps `click<8.2` and `uvicorn<0.36`. W11 runs skypilot from its own environment as a subprocess.
  - keyring is in core (W6), and `keyrings.alt` is in the `testing` extra.
- **Hook entry points fall back when their module is missing.** `hook_factory` uses the stub when a hook's own module is not installed. Any other import error is raised. Why: an entry point can be registered before its workstream merges, and the guard stub refuses guarded sites, so the fallback is safe.
- **The serve flow follows run (W9's spec).** `RelayService.start_serve` runs the guard check and price on `spec.job`, creates a `kind="serve"` record with the spend and sign-in asks, and calls `serve_hook.start`. `refresh` calls `serve_hook.status` and `cancel` calls `serve_hook.stop`. `record.result["endpoint"]` holds the endpoint, `record.result["message"]` the latest `JobStatus.message`, and `record.outputs` the stop artifacts. `serve` is task-optional like `run`. A running serve task is `working`, and its status message names each port as `name: host:port`. `wait` also returns once the service is ready.
- **Secret URLs reach the UI only.** The record, `describe` and the task status keep an endpoint's ports without their `url`. The serve result carries `_meta["ai.iowarp.relay/service"]` from `ServeHook.ui_data(record)`. Why: RL22. A test caught that the endpoint first reached the record with its URLs.
- **The stub serve hook refuses up front.** With no serve runtime installed, `serve` refuses with `verb_not_implemented` and writes no record. `ServeHook.attach(server, changes)` is called in `RelayServer.build`, and the stub's `attach` and `ui_data` do nothing. `local` lists `serve` and imports `clio_relay.serve` lazily; without it, `local.serve` refuses the same way.
- **Under tasks, `run(cancel=)` and `run(resume=)` return a plain result.** Only a new run or serve becomes a task. The same holds for `serve(service=)` and `serve(stop=)`.
- **Ask pages mount on the HTTP server (W6).** `MountsPages` is an optional Protocol (`mount(server)`). `create_server` calls it after `build()` for HTTP when the ask provider implements it. W9's `attach` follows the same pattern. `HookContext.settings["public_base_url"]` holds the server's public origin.
- **`serve` over HTTP.** The command line takes `--grant-file PROVIDER=PATH` (repeatable). It did not exist at M1, though the published interfaces listed it. It also takes `--tls-cert` and `--tls-key`, given together, which make the origin https for RL25a. Uvicorn's access log is off, because page addresses carry single-use tokens.
- **The job name is recorded before submit.** A run's SUBMITTING move stores `result["job_name"]` with the note "job name X" (RL25a).
- **The asks command group is W6's.** `clio-relay-next asks` mounts `clio_relay.cli_asks.app`. The built-in `list` and `answer` were removed when W6 merged.
- **Bounded commands leave nothing behind.** `run_bounded` reads both pipes to their end after a timeout kill. When its caller is cancelled, it kills the process group before it re-raises. This fixed a leaked subprocess transport ("Event loop is closed") from the local conformance tests, which was already present at M1. The relay_next suite now runs with `PytestUnraisableExceptionWarning` as an error.
- **W11 merged.** `jaimecernuda-relay-rework-w11` at b41c756 is merged as aa9b7b1. Its adapter pages are in the docs index in `README.md`.
- **Routing skips an adapter whose settings do not build (RL5, coordinator decision).** An alternative that fails at build time with `invalid_config`, such as Aurora with no endpoint or iri with no `resource_id`, is skipped, and routing moves on to the next one. The skip is part of the route, as `skipped: [{site, adapter, reason, message}]`. It shows in `describe`'s routes, in an operation's `result["skipped"]` and in an `exec` result, so the person sees why it was not used. When every candidate fails, the call refuses with `invalid_config`, and the refusal lists each adapter's reason. Any other refusal at build time is still returned as it is. Routing builds each candidate through the service's adapter cache (`RelayService.route` and `site_routes`).
- **`exec` has no hard time limit (coordinator decision, the person's rule).** `timeout_s` is optional and has no default or ceiling. Without it the command runs until it ends. After 30 s with no output on either stream, relay reports it as quiet, then reports again after waits that double (30, 60, 120 s and so on, capped at 600 s) while it stays quiet. Each report is a progress notification on the call (not a log message, since logging is deprecated at `2026-07-28`): "no output for N s; the command is still running; relay checks again in M s; cancel the call to stop it". Cancelling the call kills the command's process group. A caller may still pass `timeout_s`, and then the command is killed at that limit as before. Every adapter's `exec` must accept `timeout_s=None` and an optional `on_stall`.
- **The conformance kit no longer skips.** For a verb the adapter does not implement, each kit test asserts the typed `verb_not_implemented` refusal and passes, instead of skipping. A new kit test runs `exec` with no time limit.
- **Wave 1 merges.** Each was merged into `jaimecernuda-relay-rework-w1`, gated on Ares and then pushed to the integration branch:
  - W11 at 785dcba, then at b8c9cf4 (merge f8a02d2), which moves globus and sky onto W6's identity API. The `credentials.service` setting is gone, and a config that still sets it is refused with `invalid_config`.
  - W7 at c30e348 (merge 82a2302), then its follow-up at cbf2e39 (merge 9490a62). Its exec replaces my compat stopgap c5ea18b.
  - W6 at 4bad7cb, then at 03c2819 (merge a27a8b2), with the device-flow fix.
  - W10 at c5b6c3c, then at 09d1cf8 (merge 303524b).
  - Fixes at merge: `conftest.answer_page(..., auth=)`, and `test_transports` passes the operator token to the ask page over HTTP.
- **Relay never imposes a walltime or a duration (coordinator decision, the person's rule).** `JobAttributes.duration` is `timedelta | None` with no default, for run and serve specs alike. When it is unset, the site's `defaults.duration` applies if the site sets one. Otherwise no walltime is sent, and the scheduler's own default applies. `to_psij` passes `duration=None` explicitly, because psij's own default is 10 minutes. `JobSpec.node_hours()` returns `None` when the duration is unknown, and `Charge.walltime_s` and `Charge.node_hours` may be `None`, meaning unknown. The globus, iri, tapis and sky adapters send no walltime when the spec has none. These are compat edits in W10's and W11's files, for their owners to review. Sky prices such a job with no amount.
- **An unknown cost always asks (coordinator decision).** `SpendPolicy.needs_approval(None)` is true unless the site's `ask` is `never`. The approval message says plainly: "sets no walltime, so its duration and cost are unknown: the scheduler's default limit applies".
- **No new job state for a job the scheduler forgot (coordinator decision).** A job the scheduler no longer lists, with no exit record of its own, stays open as running with `native_state: "unknown"` and a message that says cancel ends it. It is never recorded as failed. W7's ssh adapter and relay's `local` adapter both follow this.
- **Local batch jobs are named and leave a record (W6's crash finding).** psij 0.9.11's templates read `job.name`, which psij's `Job` lacks, so they never name a job. Slurm then names it `submit.sh`, and `find()` cannot reconcile a job submitted just before a crash. `LocalAdapter.name_submit` puts the name on the submit line: Slurm `--job-name=`, PBS `-N`, LSF `-J`. `mark_script` has the job script write its scheduler id to `scheduler_id` in the job directory, and `find()` reads that when the scheduler has forgotten the job. The job's exit code comes from psij's `<native_id>.ec` file. Ares has `sacct` off and `MinJobAge` 300 s, so the scheduler forgets a finished job after 5 minutes.
- **psij 0.9.11 bugs that relay works around.** `Job` has no `name`, as above. The pbspro template writes `-N="x"`, which qsub refuses. Paths are made absolute against the local machine, not the site. W7's ssh adapter renders its own scripts for the last two.
- **A resumed run must match its approval.** `run(resume=op, site=, spec=)` with a changed site or spec refuses with `approval_mismatch`, as a task's resume round does. `RelayService.same_request(record, site, spec)` holds the check.
- **A sign-in finished elsewhere closes its ask.** `advance()` resolves a pending URL ask as accepted when the adapter's `sign_in_needed()` returns `None`. This covers a device-flow sign-in finished at the provider when no one answered relay's page.
- **`describe` with no site gives each site's status (LQ2).** Each listed site has `status`: `{"state": "ready"}`, `{"state": "needs_sign_in", "provider", "detail"}` or `{"state": "unavailable", "reason", "detail"}`. It asks the adapter that serves `describe` whether a sign-in is needed. That reads local state and runs nothing remote. `src/clio_relay/surface/describing.py` holds both reports.
- **Adapters may set a site up when it is added (W10's request).** The optional protocol is `clio_relay.adapters.base.SiteAdded`, with `async on_site_added(site: SiteEntry) -> dict | None`. `RelayService.site_added(site_id)` calls it on the adapter in use and on every alternative that implements it. The command line calls it after `site add`, `add-from-catalog`, `edit` and `switch`, and prints the results under `set_up`. A typed refusal, such as no sign-in yet, is reported, and the site stays saved, so the method must be safe to run again. An adapter whose settings do not build is left out here; `describe` shows why.
- **Hooks see the config directory.** `HookContext.settings["config_dir"]` (W6's request).
- **Relay keeps watching a service (W9's requests).** While a service is not ready, its task status message is "the service is <state>: <the hook's latest message>". A TLS refusal or a hung start is therefore visible on `tasks/get`. When the server starts, it observes each open service once, in the background, so a service that ended while relay was down is finished and its forwards close. Any error from `ServeHook.status` is a failed look, not the service's end: the service stays open, and its message says "relay could not observe the service: <error>". The hook must keep secrets out of its exception text, as it does out of `JobStatus.message`.

### Published interfaces (M1)

Owned by W1, not frozen. A change goes through W1, which reruns every consumer's contract tests first.

- Adapters (`clio_relay.adapters.base`): the `Adapter` Protocol (`name`, `Config`, `describe`, `run(spec, operation_id)`, `status`, `cancel`, `find(operation_id)`, `exec(command, *, timeout_s=None, max_output_bytes, on_stall=None)`, `files`, `serve`), `BaseAdapter[ConfigT]`, `SiteInfo`, `SignInNeed`, `AdapterContext`, and the optional `SiteAdded` (`on_site_added(site)`).
- Jobs (`clio_relay.jobs`): `JobSpec` (PSI/J names, `JobResources.to_psij()` builds `ResourceSpecV1`), `JobResources`, `JobAttributes`, `InputRef`, `PackageRef`, `ServiceSpec`, `ServicePort`, `ServiceLifetime`, `canonical_digest`, `JobHandle`, `JobStatus`, `JobState`, `ExecResult`, `StallReport` and `STALL_S`, `Endpoint`, `EndpointPort`, `FileEntry`, `FileAccess`, `OutputRef`.
- Records: `OperationRecord`, `OperationState`, `RecordStore`. Layout: `<state>/operations/`, `<state>/asks/`, `<state>/sites/<site>/<adapter>/`, and `sites.yaml` in the config directory.
- Content store: `ContentStore` at `<state>/content/cas/sha256/<first two hex>/<digest>`.
- `RefusalReason`, 24 typed reasons: `verb_not_implemented`, `site_not_found`, `site_exists`, `unknown_adapter`, `adapter_unavailable`, `invalid_config`, `invalid_spec`, `invalid_arguments`, `guard_unavailable`, `policy_refused`, `needs_approval`, `needs_sign_in`, `declined`, `interactive_auth_required`, `operation_not_found`, `ask_not_found`, `approval_mismatch`, `input_missing`, `path_not_found`, `path_exists`, `not_cancellable`, `submit_failed`, `unreachable`, `timeout`.
- Hooks (`clio_relay.adapters.hooks`): `Ask`, `AskProvider` (`choose`, `approve`, `url`) for W6; `GuardHook` (`check_run`, `check_exec`, `price`), `Charge` and `GuardDecision` for W13; `ServeHook` (`start`, `status`, `stop`, `attach`, `ui_data`) for W9; the optional `MountsPages` (`mount`) for an ask provider's pages. `HookContext.settings` holds `grant_files`, `config_dir` and, over HTTP, `public_base_url`.
- Serve runtime, provided by W9 and imported lazily: `clio_relay.serve.factory`, `start_for(adapter, spec, *, operation_id)`, `connect(handle, *, paths)` and `settings(paths, changes)`, where `changes` is None to show. `SiteEntry.serve` holds a site's serve settings.
- Change events: `ServerChanges` (above).
- Routing (`clio_relay.sites.routing`): `route(entry, verb, check) -> Routed(adapter, config, skipped)`, `routes(entry, check) -> list[Route]`, and `Skipped(site, adapter, reason, message)` in `Route.skipped`.
- Command line `clio-relay-next`: `serve` (stdio, or HTTP with a bearer token, `--grant-file`, `--tls-cert` and `--tls-key`), `adapters`, `site`, `asks`, `connect <handle>`, `rendezvous set|show`.

### Conformance kit

`clio_relay.testing.conformance.AdapterConformance`, parameterised by a `ConformanceTarget(adapter, config, state_dir, ...)`. It holds 11 executable contract tests per adapter: config schema, describe shape, typed refusal of unimplemented verbs, bounded exec, exec with no time limit, files round trip, job lifecycle, job failure, job cancel, the same job observed after a restart, and inputs by digest. An adapter package subclasses it in its own tests (see the module docstring). W7 and W8 run it for `ssh` and `worker` (LQ4).

### M1 evidence (Ares)

All runs are in `~/clio-relay-rework/clio-relay-w1`, branch `jaimecernuda-relay-rework-w1`.

- **relay_next suite.** `uv run pytest tests/relay_next` gives 68 passed and 0 skipped. It covers:
  - `describe`, plus an ordinary `run` and a negotiated task on the `fixture` adapter (`test_server.py`) and on `local` (`test_server_local.py`);
  - form and URL elicitation through the stub ask provider, on both eras;
  - `input_required` answered with `tasks/update`;
  - decline and cancel;
  - a task outliving the server;
  - the typed refusal with a loopback page when the client has no elicitation;
  - the conformance kit on `fixture` and on `local` with no scheduler (20 tests);
  - listen-stream and legacy change events.
- **LQ41.** `test_transports.py` starts the real `clio-relay-next serve` as a subprocess. The plain `mcp` 2.3.0 `Client` and the fastmcp `Client` each run a job and answer a form and a URL question, over stdio and over Streamable HTTP, on `2025-11-25` and `2026-07-28`. A wrong bearer token is refused.
- **LQ4 for `local`.** The conformance kit passes for `local` without a scheduler (above). It also passes for `local` on Ares Slurm: `RELAY_TEST_SLURM_DIR` set, `tests/relay_next/test_conformance_slurm.py`, 10 passed. The Slurm suite is collected only when that directory is set, so it is never a skip.
- **LQ2 (relay half).** `describe` with no site lists the sites and adapters with status. With a site it reports the route for each verb (`test_describe`). The SSH-fixture half waits for W7's adapter.
- **LQ31a.** `uv build` gives `clio_relay-1.6.8-py3-none-any.whl`. A clean venv install runs `clio-relay-next --help` and `adapters` (lists `fixture`, `local`), and a stdio `describe` lists the five tools. `pip-audit` on the exported lock reports no known vulnerabilities.
- **Lint and types.** ruff check and ruff format are clean. pyright on the new packages has 0 errors. Repository-wide pyright reports 7514 errors, identical to origin/develop 034117b: 0 new and 0 gone.
- **W2 fixtures.** With `CLIO_RELAY_FIXTURES_ENV` set to W2's env file, `tests/test_fixtures_smoke.py` gives 13 passed.
- **Full suite.** It runs in Slurm on a compute node (`pytest -n 24`). The pre-existing failures are compared by node id with origin/develop 034117b on the same node type: develop gives 139 failed, 6482 passed, 7 skipped and 3 errors.
  - Job 27716 gives 143 failed, 6546 passed, 7 skipped and 3 errors. Job 27717 gives 138 failed, 6551 passed, 7 skipped and 3 errors. The 7 skips are the same as develop's (6 need systemd `--user`, 1 is Windows only).
  - Two tests that fail on develop pass on w1: `test_wheel_record_closure::test_native_jarvis_record_closure_verifies_standard_wheel_data`, and the bootstrap repair test, which no longer reads the real `~/.local/share/clio-relay`.
  - Some failures appear only on w1, and they differ between runs: 6 in `test_process_containment` (27716) and 1 in `test_storage_process_guard` (27717). Each is `systemd-run --user` failing with "Failed to connect to bus". Ares compute nodes have no user manager for this uid unless a session happens to exist there. w1 changes no containment code. On the login node, which has linger and a stable user bus, both modules pass 43 of 43 in 3 runs out of 3. This is recorded as an open item, not written off.
  - The dependency bumps needed these test changes:
    - The embedded bootstrap verifier now requires every console script in the wheel's `entry_points.txt`, so `clio-relay-next` verifies. The empty `catalog/{sites,skills}/.gitkeep` files became a non-empty `README.txt`.
    - FastMCP 4.2 always derives a tool title.
    - The mcp 2.3 client stamps `Mcp-Name` on each request, so the forged-name test rewrites it in an httpx request hook.
    - filelock 4 applies the explicit mode only to a lock file it creates, so the late-start test makes `transition.lock` owner-private.

## W2. Fixtures and the Ares harness

Branch `jaimecernuda-relay-rework-w2`. Files: `tests/fixtures/sites/` (one Dockerfile and entrypoint per site, the two services, a README), `scripts/ares/fixtures.sh`, `tests/test_fixtures_smoke.py`.

### How to use them

```bash
scripts/ares/fixtures.sh up        # builds images the first time, then starts all seven sites
scripts/ares/fixtures.sh status
scripts/ares/fixtures.sh down      # --images also removes the images
```

`up` writes `~/clio-relay-rework/state/w2-fixtures/fixtures.env` (override with `FX_STATE_DIR`). It names every port, the client key, `ssh_config` (hosts `fx-ssh`, `fx-slurm`, `fx-pbs`, `fx-otp`), `known_hosts`, the `frpc` binary, and each secret by file path, never inline. Tests read it from `$CLIO_RELAY_FIXTURES_ENV` or that default path. Containers on the fixture network reach each other as `fx-<site>`; the env file also carries the inner addresses (`FX_INNER_*`).

| Site | Container port | Ares port (127.0.0.1) | What it is |
| --- | --- | --- | --- |
| `fx-ssh` | 22 | 47100 | SSH host, user `relay`, key only, scp and sftp |
| `fx-slurm` | 22 | 47101 | SSH plus Slurm 21.08.5, one node, partitions `compute` (default) and `debug`, `sacct` through slurmdbd |
| `fx-pbs` | 22 | 47102 | SSH plus OpenPBS 23.06.06, one node, queues `workq` and `debug`, job history on |
| `fx-otp` | 22 | 47103 | SSH that asks for the key and then `Verification code:` (TOTP) |
| `fx-frps` | 7000 | 47104 | frps 0.69.1, tcp and websocket, token auth, `tcpMux` as `render_frps_config` |
| `fx-frps` | 7443 | 47105 | wss: a TLS front (socat) handing the connection to frps |
| `fx-secret` | 8080 | 47106 | HTTP and WebSocket service that checks a secret |
| `fx-counter` | 8000 | 47107 | Counter MCP server at `/mcp`, `count_slowly` is a task tool |

Ports are chosen from 47100 up, skipping any port in use, and kept in `ports.env` so later runs reuse them. Every SSH site also carries `clio-fixture-secret-service` and `clio-fixture-counter-mcp` on the PATH, for LQ12 to LQ15 to start on a site.

### Decisions

- Plain `docker`, no compose file. Ares has docker 29.1.3 with no compose plugin, no `docker-compose` and no `podman-compose`. Every container, network and image carries `clio-relay-rework=1`, `com.docker.compose.project=clio-relay-rework` and `clio-relay-rework.owner=w2-fixtures`. Containers the script runs also carry `clio-relay-rework.fx-run=1`, because image labels reach a build's intermediate containers; `down` selects on all of them, so it never stops a running build or anything another session owns.
- All SSH sites build on `ubuntu:22.04` with OpenSSH 8.9, as on the clusters relay targets. The fixture Python (3.12, fastmcp 4.0.0b1 and fastmcp-tasks) lives in `/opt/clio-fixtures/venv`, off the user's PATH, so the site looks like a plain host to relay's bootstrap. The system Python is 3.10.
- PBS is OpenPBS built from source. psij's PBS executor reads `qstat -f -F json -x`, which Torque does not answer. Only `/opt/pbs` is copied into the runtime image; copying the whole install root replaced Ubuntu's `/lib` symlink.
- Slurm includes slurmdbd and MariaDB because relay's Slurm provider calls `sacct`. The database password is fixture-local and in the repo on purpose.
- The one-time-code site uses `pam_google_authenticator` with `AuthenticationMethods publickey,keyboard-interactive:pam`. A BatchMode client is refused after the key; a client that answers the prompt with the current TOTP gets in.
- frps 0.69.1 does not serve wss itself (`transport.protocol = "wss"` against the bind port fails with EOF). The fixture fronts it with TLS on a second port, as Cloudflare does for a public frps. frpc logs in over wss without being given the CA, so frpc 0.69.1 does not verify the wss certificate by default. The self-signed certificate is in the env file as `FX_WSS_CA_FILE` for anyone who wants to pin it.
- Secrets are made at `up` and never baked into an image. Each container gets only its role's files, copied in rather than bind-mounted, since the state directory may sit on NFS. The client's private key never enters a container.
- `authorized_keys` on each site is copied once and stays writable by `relay`, so a test can add and remove its own key (LQ40).

### Findings for other workstreams

- Protocol `2026-07-28` has no MCP session. Against the counter, three calls on one fastmcp 4.0.0b1 `Client` gave three different `ctx.session_id` values over Streamable HTTP (no `mcp-session-id` header is sent) and also over stdio. The counter therefore reports `server_instance` (one per process) beside `session_id`, and the smoke test proves continuity with that. LQ14's "one session served all three" needs reading as one server instance and one client connection; W9 and W16 should confirm that reading with the coordinator.
- `scripts/check_file_size.py` reports that the tests distribution ratchet can rise to 63.84%. That file is W1's.
- The smoke module skips as a whole when the env file is missing, so a run anywhere but a fixture host shows one module skip.

### Smoke evidence on Ares

Run at `a774081` (fixture code identical to `36219c0`) after a fresh `down`, `build`, `up`, from `~/clio-relay-rework/clio-relay-w2` with the shared toolchain (`uv run`).

- `scripts/ares/fixtures.sh up`: all seven sites ready in 80 s with images built.
- `uv run pytest tests/test_fixtures_smoke.py`: `13 passed in 58.31s`. The tests:
  - `fx-ssh`: hostname, user and Python; a 64 KiB file round-trips through scp and sftp with a matching SHA-256; a key not in `authorized_keys` is refused.
  - `fx-slurm`: `sinfo` shows both partitions; psij's `squeue --me -o %i -h -r -t all` runs; an `sbatch --wrap` job reaches `COMPLETED` in `sacct` and its output names `fx-slurm`.
  - `fx-pbs`: a `qsub` job reaches `job_state F` with `Exit_status 0` in `qstat -f -F json -x`, and its output is staged back.
  - `fx-otp`: BatchMode is refused with `Permission denied`; askpass answering `Verification code:` with the computed TOTP gets in.
  - Secret service: `/health` is open; `/` gives 401 without the secret and with a wrong one, and 200 with it; `/ws` gives 401 without the secret and echoes with it. Started on `fx-ssh` over SSH, it writes its final artifact on SIGTERM.
  - frp: an stcp proxy and visitor through frps reach the secret service over `tcp`, `websocket` and `wss`; a wrong token is refused at login.
  - Counter: three increments read `start+1..start+3` from one server instance; `count_slowly` through `fastmcp_tasks.call_tool_task` returns a task id and a result with `background_task: true`. Over SSH stdio on `fx-ssh` the counter reads 1, 2, 3.
- `scripts/ares/fixtures.sh down`: removed 7 containers and 1 network; the 12 containers of other owners on Ares were still running afterwards.
- `ruff check`, `ruff format --check`, strict `pyright` (0 errors) and `shellcheck -x` are clean on the touched files; `scripts/check_file_size.py` passes.
- Image sizes: ssh 429 MB, slurm 814 MB, pbs 789 MB, otp 431 MB, frps 52 MB.

## W3: host semantics wire contract (clio-agent)
**Merged** into the clio-agent integration branch `jaimecernuda-relay-rework` as 6cb4e6656; W3's gate has passed.

Full text: clio-agent `docs/design/host-semantics-contract.md` (branch jaimecernuda-relay-rework-w3). The excerpt below is what other workstreams build against.

**O9.** No connector entry and no new manifest capability. A plugin declares its own server in `mcp.json` beside `plugin.json` (Agent Plugins 1.0.0 layout). Proven on Ares with the generic fixture `tests/fixtures/plugins/own_server/`: `plugin install` plus `plugin trust` were enough, and the production `discover_declared_mcp_servers` and `transport_for` spawned the server with spec `source='plugin:own-server'` and `connector=''`. W14 ships relay's server in its plugin's `mcp.json`.

**Manifest.** `plugin.json` `extensions["ai.iowarp.clio"]` gains three fields:
- `tabs: [{id, title, icon (Lucide name), server (mcp.json name, optional if only one), resource: "a2ui://..."}]`
- `accounts: [{provider: github|google_drive|globus|ndp, scopes: [..]}]`
- `elicitation: {loopback: bool, origins: ["https://host[:port]"]}`

`catalogs` stays as it is, as the server's producible set.

**HS1a.** The client advertises MCP extension `org.a2ui/a2ui` with settings `{versions: ["v0.9","v0.9.1"], clientCapabilities: {"v0.9": {supportedCatalogIds: [<the server's plugin catalogs>]}}}`.
- Under 2026-07-28 the SDK stamps it on every request in `_meta["io.modelcontextprotocol/clientCapabilities"].extensions`.
- Send A2UI only when that request carries it, and always send the full text result too.
- A server with no plugin catalogs gets no entry.

**HS1.**
- A2UI is a tool-result content item `{"type":"resource","resource":{"mimeType":"application/a2ui+json","text":...}}`, or a `resources/read` item with that mimeType. `text` is one A2UI message or an array of them, applied as one batch. `blob` and `resource_link` are not used.
- Versions: only `v0.9` or `v0.9.1`.
- Typed refusals: `a2ui_version_unsupported`, `a2ui_payload_invalid`, `a2ui_catalog_unknown`, `a2ui_catalog_not_producible`, `a2ui_surface_not_owned`, `a2ui_surface_owner_conflict`, `a2ui_validation_failed`.
- The model never sees the A2UI JSON, only a placeholder plus the text result.
- The surface gets `owner: {kind:"server", plugin, server, namespace}`.
- For a task-augmented call, A2UI is applied from the terminal result.

**HS2.**
- Actions on a server-owned surface go to `POST /v1/sessions/{sid}/a2ui/actions`, with the usual envelope.
- `action.name` is the server's tool name. CLIO sends `tools/call` with `arguments` = `action.context`, verbatim, and no model turn.
- The tool must have `_meta.ui.visibility` absent or including `"app"`. Otherwise CLIO returns 403 `a2ui_server_tool_not_app_visible`.
- A2UI in the result is applied only to surfaces that server owns.
- A task's `input_required` becomes a session question.
- Response: `{status:"delivered", destination:"server", action_id, tool, outcome: applied|no_update|task_pending|tool_error, surface_ids}`. Errors: 503 `a2ui_server_unavailable`, 422 for HS1 reasons.
- Exception: `data_source/login/<provider>`, sent with an empty context, is a host action. CLIO's own sign-in runs.
- The context is opaque to CLIO, so action tokens belong in it.

**HS3.**
- Routes:
  - `GET /v1/sessions/{sid}/plugin-tabs` returns `{tabs:[{id:"<plugin>/<tab>", plugin, title, icon, server, resource, available, reason}]}`.
  - `POST .../plugin-tabs/{plugin}/{tab}/open` returns `{tab, surfaces, revision}`.
  - `POST .../close`.
  - `POST .../actions` behaves as in HS2.
- `open` sends `resources/read`, plus `resources/subscribe` if the server supports it. The read result replaces all of the tab's surfaces, and its first batch must start with `createSurface`.
- `notifications/resources/updated` for that URI triggers a re-read and replace. CLIO does not poll.
- Tab surfaces live in memory only. They are not in the transcript and not seen by the model.
- Unavailable reasons: `plugin_tab_server_unavailable`, `plugin_tab_resource_failed`.

**HS4.**
- Declared scopes are added to that provider's consent. For Globus they go beside transfer and `offline_access`.
- For each declared provider and each launched (stdio) server, CLIO sets `CLIO_ACCOUNT_<PROVIDER>_FILE` (for example `CLIO_ACCOUNT_GLOBUS_FILE`). It also expands `${CLIO_ACCOUNT_<PROVIDER>_FILE}` in `mcp.json` args and env, so a plugin can map it to the server's own setting name (HS4a).
- The variable is always set. The file exists only while a grant does.
- File protection: private dir 0700, file 0600, atomic replace. CLIO rewrites the file at least 5 minutes before expiry and deletes it on disconnect.
- File format: `{version:1, provider, written_at, tokens:[{resource_server, access_token, token_type, expires_at, scopes:[..]}]}`. One entry per Globus resource server, and no refresh token.
- No token ever goes in env or args.

**HS5.** URL elicitation is accepted from:
- Any server: the configured `tools.mcp.elicitation.url_trusted_origins`.
- That server only: the plugin's `elicitation.origins` (https).
- A launched server whose plugin has `loopback: true`: `http://127.0.0.1:<any port>` and `http://localhost:<any port>`.

Everything else is refused with the existing reasons: `elicitation_url_insecure_scheme`, `_untrusted_origin` or `_not_declared`. Question metadata gains `origin`, `origin_trust: configured|plugin_origin|plugin_loopback`, `plugin` and `server`.

**SSE on /v1/sessions/{sid}/events:**
- `a2ui.surface.upserted`, now with `owner`
- `a2ui.server_surface.refused` `{plugin, server, tool, uri, reason, detail}`
- `plugin_tab.surface.upserted` `{tab_id, revision, surface}`
- `plugin_tab.surface.deleted`
- `plugin_tab.refreshed` `{tab_id, revision, surface_ids}`
- `plugin_tab.unavailable` `{tab_id, reason, detail}`

**Decisions (coordinator):**
- `org.a2ui/a2ui` accepted; advertise an official ID too if A2UI publishes one.
- Tabs are per session. The relay plugin goes in the default agent (W14).

## W4. CLIO removals and SSH through the fence

RL37 done in clio-agent (W4, `jaimecernuda-relay-rework-w4`). Fenced-shell ssh, scp, sftp and Git over SSH run through `runtime/net_connect.py`, a CONNECT helper used as ProxyCommand. The shims come from `runtime/fence_ssh.py` and are on PATH only inside an enforced fence. LQ29 passed on Ares inside codex-linux-sandbox 0.167.0: `ssh <host> hostname` succeeded and net.egress showed host and port 22. scp, sftp, `git ls-remote` and `git clone` over SSH passed. A host the policy denies is refused with an explicit message. Open: the Linux Codex profile grants network "enabled" (host netns), so egress that does not go through the proxy is neither recorded nor gated.

RL36: "ssh_execute removed from clio-agent. Deleted: gact/ssh_tools.py, gact/ssh_recorded_presentation.py, gact/infrastructure/ssh_execution.py, gact/infrastructure/ssh_result.py, their tests and docs/verification/ssh-agent-execution-2026-10-19.md. The native_tool_registry entry and the legacy presentation calls in tool_result_presentation.py and session_export_projection.py are gone. Recorded ssh_execute rows render through the generic recorded-presentation path; new test test_retired_tool_presentation.py. Agents reach SSH hosts from the fenced shell (RL37); docs/PERMISSIONS.md says so. Kept: the Infrastructure SSH transport (paramiko SshRoute), storage SFTP and NDP SSH routes."

RL38: "Relay leftovers removed from clio-agent. The managed relay service is gone (RELAY_VERSION, the relay ManagedServiceDefinition, _relay_plan, _relay_reused, _ssh_destination) from gact/infrastructure/drivers.py. Its only readers were drivers.py and test_infrastructure_reuse.py; the service_definitions consumers only iterate. A persisted relay service record is no longer listed, and acting on it returns 'Unknown managed service'. Also removed: the relay pytest marker (pyproject.toml), the CLIO_RELAY_* env strip in tests/conftest.py, relay in the network guard's EXEMPT_MARKERS, CLIO_RELAY_API_TOKEN, FRP_TOKEN and STCP_SECRET in conf.py and clio-paths.yaml (no code reads them), the relay row in the infrastructure docs and site page, and stale relay comments in agent.py, tools/gateway.py, gact/provenance_wiring.py, tools/mcp_connection_era.py, tools/mcp_discovery.py, errors.py, tools/fs_write.py and gact/infrastructure/reuse.py. Kept on purpose: gact/task_migration.py reading placement 'relay' from historical task records (a legacy-data reader); historical text in docs/design, docs/knob_audit, benchmark/ and scripts/check_file_size.py and check_task_replacement_removal.py comments; generic test fixtures that use 'relay' as an arbitrary namespace name."

RL38 follow-up 48c966883 + eba4b0697: removed relay_fetch_artifact provenance edges, the dead Capabilities.relay field and consume_dual_emission_twin; codex comments neutral; gateway.py ratchet baseline retired.

Branch: clio-agent `jaimecernuda-relay-rework-w4` at 6801be185. The clio-agent integration branch is at 4878fcc40.

## W5. clio-jarvis

Repo: clio-jarvis (local on Ares at `~/clio-relay-rework/clio-jarvis`, branch `jaimecernuda-relay-rework`, tag `v0.1.0` at `2d12dbb`; schema first published at `a6aba08`). It is not pushed yet: iowarp/clio-jarvis does not exist, and the user will create it. jarvis-cd 1.8.1 is unchanged, and nothing in the contract needed a jarvis-cd change.

### Decisions

- **The contract is code, not prose (RL40).** `clio_jarvis/_contract/spec.py` defines the seven parts: parameters, resources, outputs with view descriptor, progress, service ports, provenance and self-test. Every package declares them in `contract.yaml`, and the package's `artifacts.py` and `progress.py` implement them. `load_contract` refuses a package that is missing any part and names the missing part (`ContractError.part`). `python -m clio_jarvis validate` checks all packages, and pytest removes each part in turn to prove the refusal (LQ34). Reason: "a package that misses any part does not ship" has to be a check that fails, not a review note.
- **The view descriptor schema is `schemas/view-descriptor.v1.json`.** Its fields are `path`, `path_kind`, `format`, `reader`, `reader_properties`, `growing`, `follow.signal` and `follow.expected_steps`. W9 and W15 vendor it. Reason: one schema shared by the package, relay and clio-kit's `open_view`.
- **Output is ADIOS2 BP5 with the VTX schema.** Each run writes a `vtk.xml` attribute, one global array per field with shape [NZ, NY, NX], and a `time` double each step. It uses one BeginStep/EndStep per output step and keeps the engine open, so `ADIOS2VTXReader` follows the growing file (W15 verified this). Reason: one reader for both solvers, and no restart of the reader between steps. The ADIOS2 XML each run uses ships with its package (`gray_scott/config/adios2.xml`, `xcompact3d/config/adios2_config.xml`), as RL41 asks. The fallback, Xcompact3d's XDMF snapshots, was not needed.
- **Output paths:** `{shared_dir}/run/gs.bp` (Gray-Scott) and `{shared_dir}/run/data.bp5` (Xcompact3d). Inside a jarvis execution, `shared_dir` is `<jarvis shared>/<pipeline>/executions/<id>/shared/<pkg>`.
- **The Xcompact3d case is TGV (Taylor-Green vortex) at Re 1600.** This is a documented Incompact3d example (BSD-3). The vortices roll up and transition to turbulence by about t = 9, so each output step looks visibly different, which suits the headline demo. The user said "just pick a good one", so this is no longer owner blocker O2.
- **Headline cadence.** Gray-Scott runs at L 128, 15000 steps, plotgap 150, giving 100 output steps at about 2.7 s each. Xcompact3d runs at n 65, ilast 2400, ioutput 20, dt 0.005, giving 120 output steps to t = 12 at about 3.2 s each, about 6.4 minutes. Each run's descriptor sets `follow.expected_steps` so the view shows "step k of N".
- **The VTX patches are provisional.**
  - Gray-Scott: VTX attribute and `time` variable added to ADIOS2 2.12.1 `examples/simulations/gray-scott`.
  - Xcompact3d: Incompact3d v5.0 writes ux, uy, uz and pp through 2DECOMP&FFT 2.0.4, with a BP5 backport in 2decomp.
  - Multi-rank works: the self-test runs on 4 ranks. One caveat: the Fortran column order means the [NZ, NY, NX] mapping relies on 2decomp's x-pencil block order, and that is checked only for the self-test decomposition.
- **ParaView for visualisation is Kitware's 6.2.1 build with Mesa EGL** (`~/clio-relay-rework/paraview/current`, source `pv-env.sh`). The conda-forge ParaView 6.2.1 has no ADIOS2 reader, so the conda env `clio` is used only to build the solvers (adios2 with MPI, openmpi, mpi4py, cmake, compilers, fftw).
- **pvserver persists across clients.** ParaView 6.2.1's `pvserver` serves one session and exits with rc 0 when its client disconnects, and `--multi-clients` is deprecated. The supervisor (menu param `persist`, default true) restarts it on the same port after a clean exit that followed a client connection. It prints `clio-pvserver restarted port=P restarts=N` and updates `restarts`, `server_pid` and `restarted_at` in the ready record (`clio.service-ready.v1`). The stop record (`clio.service-stop.v1`) includes `restarts`. Reason: the desktop tab can reconnect without relay re-serving the package.
- **pvserver has one `tcp` port, named `paraview`, bound to 127.0.0.1.**
  - Health is `tcp-listen`, read from `/proc/net/tcp` and never checked by connecting, because pvserver serves the first client that connects. jarvis-cd's `service_runtime` health probe is HTTP-only, so the supervisor does this check itself.
  - Protection: the port binds to loopback and is reached only through relay's SSH tunnel (RL19). An optional `connect_id` must match.
  - Stop: SIGTERM with 30 s grace, which writes `{state_dir}/pvserver-stop.json`.
- **Open MPI on Ares uses `OMPI_MCA_pml=ob1` and `OMPI_MCA_btl=self,sm`.** `pv-env.sh` is sourced on the compute node, not the login node.
- **Commits on Ares use `-c commit.gpgsign=false` and `tag.gpgsign=false`.** Ares has no signing key.
- **The packages are named `clio_jarvis.pvserver`, `clio_jarvis.gray_scott` and `clio_jarvis.xcompact3d`,** and they need `jarvis repo add <checkout>/clio_jarvis` (RL42). The schemas directory is found from a checkout or an editable install.

### Facts

- **Which builtin packages relay 1.6.8 calls** (clio-relay `develop` at `034117b`, `pyproject` version 1.6.8): relay's `src/` launches no `builtin.*` package.
  - Its own package types are `clio_relay.bounded_command`, `clio_relay.remote_agent` and `clio_relay.mcp_call` (`src/clio_relay/jarvis_provider.py:118,141,166`).
  - `builtin.paraview` appears in `src/` only as example text in the `_contracts/jarvis-user-v3.2` to `v3.5` schema descriptions.
  - The LAMMPS adapter was deleted (`93095eb`).
  - `builtin.paraview` (100 hits), `builtin.lammps` (31) and `builtin.gray_scott` (7) appear in tests, examples and release-gate docs only.
  - relay's bootstrap still registers and reconciles the jarvis-cd `builtin` repo (`bootstrap_reconcile_builtin_repos.py`, `managed_builtin_repo_registered`). Under RL39, that check and the release-gate references move to `clio_jarvis.*`.
- **Self-tests: all three passed on Ares** (Slurm job 27700 at `2d12dbb`, debug partition, 1 node, 4 ranks). Each self-test runs through `jarvis` with an isolated `JARVIS_ROOT` and checks the outputs against their descriptors.

  | Package | Checks | Duration | Seconds per step | Final state |
  | --- | --- | --- | --- | --- |
  | gray_scott | 27 | 15.2 s | 0.05 (self-test size L 64) | completed |
  | xcompact3d | 23 | 47.4 s | 3.12 | completed |
  | pvserver | 23 | 39.0 s | n/a | completed |

- **Contract tests:** `python -m clio_jarvis validate` reports 3 of 3 ok. pytest reports 37 passed and 2 skipped (the skips are the service-ports part for applications, which have no ports).

### Evidence

- Commands:
  - `sbatch ares/selftest.sbatch`, which runs `python -m clio_jarvis selftest <pkg> --work <dir> --pv-env <pv-env.sh>` for each package;
  - `python -m clio_jarvis validate`;
  - `python -m pytest tests`.
- Handles: Slurm jobs 27697 (pvserver failed before persist), 27699 (pvserver passed) and 27700 (all passed). Commits `a6aba08` and `2d12dbb`, tag `v0.1.0`.
- Reports and log: `~/clio-relay-rework/w5-scratch/evidence/`, holding three `selftest-report.json` files (`clio.selftest-report.v1`) and `selftest-27700.log`.
- Screenshots read:
  - `gs-last.png`: the last Gray-Scott step, through ADIOS2VTXReader.
  - `x3-last.png`: the last TGV step, through ADIOS2VTXReader.
  - `pvserver-sphere-2.png`: the second client's render through the restarted pvserver.

  All three render correctly.

### Open

- Push the branch and tag `v0.1.0` once the user creates iowarp/clio-jarvis (owner blocker O1).
- The VTX patches to Gray-Scott and Xcompact3d stay provisional until upstream or the user accepts them.

## W6. Asks and identities

Branch `jaimecernuda-relay-rework-w6` (from the integration branch, with 4b72bf2 merged in). Files: `src/clio_relay/asks/`, `src/clio_relay/identity/` (not `tacc.py`), `src/clio_relay/cli_asks.py`, `tests/asks/`.

### Published interfaces

- **The `asks` hook.** `clio_relay.asks:create_provider(context) -> RelayAskProvider`, registered as `asks` in `clio_relay.hooks` (a60bc01). It implements W1's `AskProvider`:
  - `choose(...)` and `approve(...)` store a form ask. An approval is bound to the operation id, the spec digest and the charge, and the page shows queue, account, nodes, walltime and the most it may use.
  - `url(site=, operation_id=, need=)` stores a URL ask.
  - It also implements `MountsPages.mount(server)`, `page_address(ask)` and `close()`.
  - The sign-in mode comes from the need: an `https` URL is an external page; `keyring:<name>` asks for a secret; a name in the identity table is a sign-in through that provider; a site with a registered askpass starter runs ssh; anything else is a confirm.
- **The page.** Each page is single-use: the token in its URL is good for one POST, and a second POST gets 404. The link expires after `CLIO_RELAY_PAGE_TTL_S` (default 24 hours); the ask itself does not, and `asks page <ask>` mints a fresh link. GET only shows. A POST needs a CSRF token that is an HMAC of the ask and page token. Nothing is logged: the askpass answers stay in memory, the records hold no secret, and `serve` runs uvicorn with its access log off.
  - Over stdio the page is on a loopback listener, and `<state>/pages/base.json` holds its address.
  - Over Streamable HTTP it is under relay's own origin at `/relay/asks/<ask_id>` and `/relay/asks/callback`. It needs the operator credential: HTTP Basic, any user name, with the operator token as the password. The token is `CLIO_RELAY_OPERATOR_TOKEN` or `<state>/operator-token` (0600, written when the routes mount). This is separate from the MCP bearer token, so a client holding only the bearer cannot approve anything.
- **Askpass (RL28), as given to W7.** An adapter calls `clio_relay.asks.askpass.environment(site=, operation_id=None, title=None, state_dir=None)`. That returns `SSH_ASKPASS` (`clio-relay-askpass`, the `[project.scripts]` entry; `askpass.program()` finds it), `SSH_ASKPASS_REQUIRE=force`, `DISPLAY`, `CLIO_RELAY_ASKPASS_ADDR`, `CLIO_RELAY_ASKPASS_TOKEN`, `CLIO_RELAY_ASKPASS_SITE` and, when given, `CLIO_RELAY_ASKPASS_OPERATION`.
  - The adapter sets `BatchMode=no` and closes stdin.
  - ssh runs the helper once per prompt, with the prompt as `argv[1]`. The helper prints the answer, or exits 1 on a decline.
  - `SSH_ASKPASS_PROMPT=confirm` prints `yes` on agreement. `none` shows a notice until ssh ends the helper.
  - `askpass.register_sign_in(site_id, async starter(env))` lets a site's sign-in run from the page.
  - W7 found that `-M` together with `-o ControlMaster=yes` puts OpenSSH in ask mode, which raises "Allow shared connection?" through askpass every session. Use `ControlMaster=yes` alone.
- **The identity table (RL30), as given to W10.** `clio_relay.identity`:
  - `register(name, factory | None)`, `get(name)` (returns the factory), `names()`, and the entry point group `clio_relay.identities`.
  - Relay ships `globus` and `github`. `tacc` points at `clio_relay.identity.tacc:TaccIdentity` and is skipped while that module is absent.
  - A factory takes `IdentityContext(store, grants, settings)`.
  - A provider meets the `IdentityProvider` Protocol in `identity/base.py`: `name` and `title`, plus async `status()`, `token(resource_server=None)`, `begin(ctx)`, `poll(ctx, step)`, `callback(ctx, params)` and `sign_out()`.
  - A redirect provider keeps its OAuth state in `step.data["state"]`. `step.data` is never shown or written down.
  - **A device flow** sets `poll_interval_s` on its step. Relay polls at that interval whether `relay_page` is true or false, and adds 5 s when the provider answers `slow_down`. When the code expires (`poll` returns `expired`, or the step's optional `expires_at` passes), relay calls `begin` again and shows the new link and code on the same ask's page. The ask stays pending, and nothing limits how long the person takes. `clio_relay.identity.device.follow` is the shared loop, used by both the page and `asks sign-in`.
- **Shared identities for adapters.** `clio_relay.identity.shared()` returns the `Identities` the serving relay uses, so it sees `--grant-file`. Outside a serving relay it builds one from `CLIO_RELAY_GRANT_FILE_<PROVIDER>` and the keyring. `shared().get("globus")` returns a built provider.
- **Grant files.** `GrantFiles` reads `CLIO_RELAY_GRANT_FILE_<PROVIDER>`, overridden by `--grant-file PROVIDER=PATH`. It re-reads a file when its mtime, size or inode changes. A grant token comes before a keyring token. On a change, `GrantWatcher` probes the sites that name that identity (`identity/probe.py`; results in `<state>/identity/probes.json`).
- **The keyring.** `TokenStore` uses keyring service `clio-relay` with JSON entries. Secrets typed on the page are kept as `secret:<name>`, read with `TokenStore().secret(name)`, and written with `set_secret` and `delete_secret`. `CLIO_RELAY_KEYRING=memory` keeps entries in the process, for tests.
- **`clio-relay-next asks`** (mounted by `cli_next`):
  - `list [--all]`, `show`, `page`;
  - `answer <ask> [--decline] [--value]`, `decline`;
  - `identities`, `sign-in <provider>`, `sign-out <provider>`, `probe <provider>`.
  - `answer` refuses with exit 2 (`approval_mismatch`) when the operation's spec digest no longer matches the approval.

### Answers for W11

- `Token` is a pydantic model: `resource_server: str | None`, `access_token: SecretStr` (read it with `.get_secret_value()`), `token_type = "Bearer"`, `expires_at: datetime | None`, `scopes: list[str]`, `source: "grant" | "keyring"`, and `live()`. It is not a tuple.
- `status()` and `token()` are async, as are `begin`, `poll`, `callback` and `sign_out`.
- Use `clio_relay.identity.shared().get("globus")`, not `identity.get("globus")`: `get` returns the factory, not a provider.
- Cloud keys: the service `clio-relay` is W6's, so use `TokenStore().secret("aws-access-key-id")` rather than raw keyring entries. The names under `secret:` are W11's to choose. To have the person type one on relay's page, raise `SignInNeed(provider="keyring:aws-access-key-id")`.

### Decisions

- **The page needs the operator credential over HTTP, and loopback stays credential-free.** Over stdio the page is on 127.0.0.1 with a single-use token, as the plan says. Over HTTP the origin is reachable from other machines, so it takes HTTP Basic on top.
- **The origin rule.** A POST whose `Origin` or `Referer` is foreign is refused. A POST with neither passes, because some browsers drop both on a form POST to the same origin; the HMAC CSRF token is the defence.
- **`accept` is taken as `done`** on a URL ask's page, so an MCP client that answers `accept` works.
- **No hard limit on the person.** No ask, askpass prompt or sign-in has a time limit. A prompt whose ssh has gone away is dropped, and a device sign-in polls at the provider's interval until the person finishes or declines.
- **A standalone askpass prompt** (one not tied to an operation) is seen only through `clio-relay-next asks list` and `page`. No MCP request carries it.
- **Globus sign-in** is a native-app PKCE flow: the person pastes the code on relay's page. GitHub uses the device flow, with its code shown on relay's page and not in the transcript. The client ids come from `CLIO_RELAY_GLOBUS_CLIENT_ID` and `CLIO_RELAY_GITHUB_CLIENT_ID`, or from `settings`.
- **Production keeps the OS keyring.** `keyrings.alt` is only in the `testing` extra, for headless Ares.

### Facts

- **`SSH_ASKPASS_REQUIRE` is honoured from OpenSSH 8.4** ([release notes](https://www.openssh.com/txt/release-8.4): "a new $SSH_ASKPASS_REQUIRE environment variable, including forcibly enabling and disabling its use", bz#69).
  - Ares and every W2 fixture run 8.9p1, so `force` works there; the fx-otp tests prove it.
  - On Windows, Win32-OpenSSH 8.1 (the Windows 10 and Server 2019 built-in) ignores it ([PowerShell/Win32-OpenSSH#1726](https://github.com/PowerShell/Win32-OpenSSH/issues/1726)). 8.6p1 (the Windows 11 built-in) honours it ([PowerShell/Win32-OpenSSH#2115](https://github.com/PowerShell/Win32-OpenSSH/issues/2115)).
  - This machine has `OpenSSH_for_Windows_9.5p2`; it was not exercised with askpass, since tests run on Ares only.
  - Before 8.4, askpass runs only with `DISPLAY` set and no terminal. Relay closes stdin and sets `DISPLAY`, which is enough for a detached relay, but `force` is the dependable route.
- **Ares has Slurm accounting off** (`AccountingStorageType = accounting_storage/none`), so `sacct` returns nothing. `squeue -t all` keeps a finished job for `MinJobAge = 300 s`. Reconciling by name on Ares therefore works only within 5 minutes of the job ending.
- **mcp 2.3.0 and fastmcp 4.2.1** in the W6 venv. `mcp.Client.call_tool` has no `raise_on_error`.

### Evidence

`tests/asks` on Ares at 8b13e42 (`CLIO_RELAY_KEYRING=memory`): **68 passed, 0 skipped**. After the device-flow fix: **71 passed, 0 skipped, 0 failed**. That includes `test_askpass_otp`: real ssh to fx-otp (127.0.0.1:47103), with the key and then a TOTP typed on relay's page, answered and declined. ruff, `ruff format --check` and strict pyright are clean on the W6 files.

`tests/relay_next` with W6 installed: at 4b72bf2, `test_transports.py::test_plain_mcp_client_over_http[legacy]` and `[auto]` failed because W1's helper opened the HTTP page without the operator credential. W1 fixed this with `answer_page(auth=)` at 87da62d. At 87da62d, `tests/asks` plus those two tests give **73 passed, 0 skipped, 0 failed** on Ares.

**LQ42, live on Ares, 2026-11-18.** The client is `tests/asks/lq42_live.py`. It uses the official `mcp` SDK `Client` with no elicitation callback and no tasks, and opens a new connection for each step. The site is `slurm` (adapter `local`, `{"scheduler": "slurm"}`, `--policy open --spend always`), the job `/bin/sh -c "hostname; echo lq42-<tag>"` on partition `debug`, PT5M.

- **Over stdio, mode `legacy`** (`python tests/asks/lq42_live.py --site slurm <step>`):
  - The gate: `run` refused with `needs_approval`, operation `op_8bd623bec5c50aace0da1f47`, ask `ask_63381fe64a9f82c3cb6138df`, and a page on `http://127.0.0.1:10165`. The ask's message was "Run /bin/sh on slurm? It may use up to 0.0833 node-hours (1 node(s) for 5 min)."
  - Two GETs of the page returned 200, after which `describe` still showed `awaiting_input`, the ask `pending` and no native id.
  - Approving on the page: POST 200; the same POST again, 404.
  - Retry 1 and retry 2 (`run resume`) both returned Slurm job **27724**.
  - The same run with a changed spec got a new operation (`op_1e477f453cdd09a4b161496d`) and a new approval. The first approval did not carry over.
  - Resuming the approved operation with a changed spec continued it at 4b72bf2. At 87da62d, rerun on 2026-11-18 at 07:38, it is refused with `approval_mismatch` (`op_222ad58111a41d0967834392`, Slurm job 27744).
  - Reconnecting: `describe` showed `completed`, job 27724. `files read jobs/<op>/stdout` gave `ares-comp-03` and `lq42-gate`.
  - Decline: `op_13632a885db31975f126bff2` went to `cancelled` (`declined`), with no handle and no job directory.
  - No answer: `op_b474063849e345932b3a93dd` stayed `awaiting_input`, with no handle and no job directory; a retry was again `needs_approval`.
  - Cancel through `run`: `op_7ec137cc87b2d3de98a12a17` was approved and ran as job 27725, then `run cancel` gave `cancelled` and Slurm showed 27725 `CANCELLED`.
- **Over Streamable HTTP with TLS, from compute nodes.**
  - Relay ran on the login node: `clio-relay-next serve --http --host ares.ares.local --port 47160 --tls-cert … --tls-key …`, with a self-signed certificate.
  - The client ran under `srun -p debug -N1` on ares-comp-03 (gate, decline, no answer) and ares-comp-04 (reconnect).
  - MCP without the bearer: 401.
  - The gate: `op_53a0f721139608c923a83519`, ask `ask_690532b42e5aa0e5da6fdff4`, page `https://ares.ares.local:47160/relay/asks/…`.
    - The page without the operator credential: **401**.
    - With it: GET 200 twice, still `pending`, no job.
    - Approve: POST 200, then 404.
    - Two retries: one job, **27731**.
  - Decline (mode `legacy`) and no answer (mode `auto`) submitted nothing.
  - Reconnect: `completed`, with output read through `files`.
  - The serve log has no request lines and no page URLs.
- **Exactly once across a crash.** A fake `sbatch` first on `PATH` runs the real one and then `kill -9`s relay, after Slurm has accepted the job and before relay writes its id. Relay is then restarted with the normal `PATH`.
  - **At 4b72bf2, it failed to reconcile.** `op_841635bf1ab636c3eb03c79a` was accepted by Slurm as job 27726, which ran to completion. After the restart relay could not find it and, after `SUBMIT_GRACE`, recorded `failed` ("no job was found"). The cause: psij's Slurm template takes `--job-name` from `job.name`, which is empty, so Slurm named the job `submit.sh`, not `relay-<op>`. Nothing was submitted twice, but relay lost the job.
  - **At 87da62d, with W1's job naming and `scheduler_id` record, it passes.**
    - Restart while Slurm still lists the job: `op_7752f84c897ba04b72395965` was killed after Slurm accepted job **27745** (`relay-op_7752…`, PENDING, no native id in the record). The restart reconciled by name. History: `submitting` ("job name relay-op_…"), `queued` ("reconciled after an interrupted submit"), `running`, `completed`. Slurm had exactly one job for it, and stdout was `ares-comp-03` and `lq42-crash`.
    - Restart after Slurm forgot the job: `op_07e8c69c4cc2c58895a47f45` was killed after acceptance of job **27751**. Relay was restarted only once squeue no longer listed the job (MinJobAge passed, 07:39 to 07:44). The restart found it through `<job_dir>/scheduler_id` and recorded `queued` ("reconciled after an interrupted submit") and then `completed`, with output read back. One job.
- **Not covered here.** `files` transfer, `serve` service and stop, A2UI, and the tab token belong to W1, W9 and W14, and are left to their owners' LQ42 runs.

### Halves of other rows

- **LQ10 and LQ11 (W8).** Ready. A worker's sign-in raises a `url` ask with a loopback page; the code is typed on the page (askpass, or a `keyring:` secret) and is never in an ask record, the transcript or a log. W8 registers its starter with `askpass.register_sign_in`.
- **LQ19 and LQ20 (W13).** Ready, proven above through W1's stub guard: the page shows queue, account, nodes, walltime and the most it may use; a no submits nothing; nobody answering leaves the operation waiting with no job. W13 runs them with its own guard.
- **LQ39 (W10).** Ready on W6's side. TACC registers in the identity table; a device-code sign-in shows its code on relay's page (`SignInStep(relay_page=True, display=…)`), never in `ask.url`.
- **Globus (O10).** Tested against a stub only. **The live Globus sign-in is pending O10** (no native-app client is registered yet). It is not a pass.

### Open

- **The four W1 requests landed at 87da62d:** job naming (with `scheduler_id`), refusing a changed resume, `answer_page(auth=)`, and `settings["config_dir"]`, which the ask provider now reads. Closed.
- **Live Globus** waits for O10.

## W7. The ssh adapter and scheduler jobs

Branch `jaimecernuda-relay-rework-w7`. Code in `src/clio_relay/adapters/ssh/`, tests in `tests/relay_ssh/`, user doc in `docs/adapters/ssh.md`, Ares entry in `catalog/sites/ares.yaml` and `catalog/skills/ares.md`.

### Decisions

- RL10 governs, per the coordinator: relay renders the submit script on this machine with psij-python, then sends the script, its PSI/J launcher and any missing inputs with one `scp`, and submits with one `ssh` command. The site needs a POSIX shell and its scheduler, not Python or psij. Status and cancel commands, and the parsing of their output, are PSI/J's executors, run over the same `ssh`.
- The remote job directory (RL17) is `<remote_dir>/jobs/<operation_id>`. It holds `submit.sh`, `spec.json`, `inputs.json`, `scheduler`, `states`, `final`, `stdout` and `stderr`. Its name is the operation id, so `find(operation_id)` reconciles by looking at that directory, and a second `run` of the same operation returns the first job. A lock directory makes the submit happen once even if two relays race. `final` is written by the first poll that sees a final state, so later reads need no scheduler.
- Staging (RL16) is content addressed on the site under `<remote_dir>/content/cas/sha256`. Each `run` first asks which digests the site already has, sends only the missing ones, checks every digest on arrival and refuses with `input_missing` on a mismatch. Inputs are hard links into the store (copies when the filesystem refuses links). An input the job did not replace is not listed as an output.
- With no scheduler (`scheduler: none` or none detected), a small `sh` supervisor is started detached with `setsid` (`nohup` where there is no `setsid`). It records the pid, the child's pid and the exit code in the job directory, so a later poll from a new relay reads the result (LQ8). The native id is `pid:<n>`.
- Every `ssh` and `scp` uses `BatchMode=yes`. A site that asks for a code refuses with `interactive_auth_required`, naming `worker` as the adapter to use, unless `interactive_sign_in` is set. Then `sign_in_needed()` returns `SignInNeed(provider="ssh")` and registers a starter with W6's `askpass.register_sign_in`; the starter brings up a ControlMaster through askpass, and every later call reuses it with `BatchMode=yes`. Until W6 merges, the adapter falls back to an `askpass:` program from the config.
- serve: the adapter declares it and calls `clio_relay.serve.start_for` lazily, and exposes `ssh_argv()` for W9's `ssh -L`.
- No time limits on jobs or transfers. Only the connection setup has one (`connect_timeout_s`, default 30 s), which refuses as `unreachable`.
- No walltime of relay's own (coordinator decision after the gate). When the spec gives no `duration`, the adapter passes `None` to PSI/J, which then writes no `--time` or `walltime` line, and the scheduler's partition or queue default applies. A site default walltime is the existing `defaults.duration` in the site entry (and so in the catalog schema, since `CatalogEntry` extends `SiteEntry`); the service fills it in before pricing and before the record, so the charge and the script agree. The adapter takes a duration as given only when it is in the spec's `model_fields_set`, because `JobAttributes.duration` still defaults to 10 minutes.
- exec follows W1's interface (integration `14e8edd`): `timeout_s` is optional, `on_stall` gets quiet-time reports on W1's schedule (30, 60, 120, 240 s, then at most every 600 s). The command runs on the site in its own session under a small watchdog that holds the ssh session's standard input. When relay's `ssh` ends, times out, is cancelled or drops, standard input closes and the watchdog stops the command's process group, so exec needs no remote guard time.
- A job the scheduler has forgotten (Slurm drops finished jobs after `MinJobAge`, 300 s on fx-slurm and on Ares) is read from its own record: PSI/J's `<native id>.ec` in the job directory. With no exit code, Slurm accounting (`sacct -X -n -P -j <id> -o State,ExitCode`) is asked, only in that case. With neither, `status` reports the job open (`running`, or `queued` if that was the last record) with `native_state: unknown` and a message, never `failed`. `JobState` has no unknown value; if W1 adds one, the adapter will use it. PBS keeps finished jobs through `qstat -x` for `job_history_duration` (24 h on fx-pbs); after that it falls to the same record. `find` never consults the queue, so a purged job is never submitted again.

### Facts

- psij-python 0.9.11 (the shared venv's pin):
  - `Job` has no `name`, which its templates read; the adapter sets it from the operation.
  - The pbspro template writes `#PBS -N="name"`, which `qsub` refuses; the adapter rewrites the line to `#PBS -N name`.
  - `JobSpec` paths and the launch command are made absolute against the local machine; the adapter sets the site's paths after building the spec.
  - With no `duration`, W1's `JobAttributes` fills in 10 minutes (`jobs/spec.py`), and the record round trip (`model_dump`, then `JobSpec.model_validate` at submit) marks it as given. So through the service every job without a duration is still sent with 10 minutes and charged `walltime_s: 600`, until W1 makes the default `None` (requested). Directly through the adapter, and once W1's change lands, no walltime is sent.
- OpenSSH: `-M` together with `-o ControlMaster=yes` is "ask" mode, which confirms every shared session through askpass ("Master refused session request" under BatchMode). Use `ControlMaster=yes` alone. This matters to W6's sign-in page.
- Ares, from the [GRC Ares user guide](https://grc.iit.edu/docs/ares/ares-user-guide/) and live `sinfo`: partitions `compute` (default, 22 nodes, 2 days), `debug` (6 nodes, 4 days) and `datacrumbs`; 40 threads and about 47 GB per node; Ubuntu 22.04.5; `module` (Lmod 8.7.25) works only in a login shell, so the skill says to wrap commands in `bash -lc`.

### Evidence

All runs on Ares from `~/clio-relay-rework/clio-relay-w7` with its own venv, against W2's fixtures at 127.0.0.1:47100 to 47107 (`fixtures.env` sourced with `set -a`).

- `uv run pytest tests/relay_ssh` at `3f31cf1`: `53 passed in 150.49s`. Again after merging the integration branch at `4b72bf2`, at `164a2f6` with `tests/relay_next/test_serve.py`: `61 passed in 153.34s`. `ruff check`, `ruff format --check` and strict `pyright` are clean on the touched files.
- LQ4 (`ssh`): `test_ssh_conformance.py` subclasses W1's `AdapterConformance` three times, on fx-ssh (detached), fx-slurm and fx-pbs: 30 of 30 passed.
- LQ5: `test_lq5_a_slurm_job_completes_after_relay_restarts`. A 15 s Slurm job is submitted, the adapter is dropped with no ssh process left behind, and a new adapter with a fresh state directory finds the job by operation id, waits for `completed`, reads `lq5-done`, and a second `run` of the operation returns the same Slurm id.
- LQ6: `test_lq6_the_pbs_script_and_status_come_from_psij`. On OpenPBS the job's `submit.sh` carries `#PBS -N relay-` and PSI/J's `single_launch.sh`, the job completes from PSI/J's status parsing, and stdout is `lq6 <PBS job id>`.
- LQ7: `test_lq7_a_rerun_stages_nothing`. A 200 MiB input is staged once (its blob is in the first upload) and a second job with it uploads no blob; every later upload is under 1 MiB. `sha256sum` on the site matches the digest both times.
- LQ8: `test_lq8_a_detached_job_is_read_by_a_later_poll`. On fx-ssh with no scheduler, a job is started as `pid:<n>`, relay drops it, and a new adapter finds the same handle, reads `failed` with exit code 7 and separate stdout and stderr.
- LQ9: `test_lq9_a_code_site_refuses_and_names_worker` (with and without `interactive_sign_in`; the second is skipped once W6's askpass is installed) refuses describe and run on fx-otp with `interactive_auth_required` and `adapter: worker`. `test_lq9_the_code_site_gets_in_once_askpass_answers` brings up the master through an askpass program that answers `Verification code:` with the TOTP; describe, run and files then work, and the person is prompted once.
- LQ2 (SSH-fixture half): a plain `mcp.Client` over stdio to `clio-relay-next serve` negotiates `2026-07-28`; `describe()` lists the `fx-ssh` and `ares` sites with their adapter and verbs; `describe(site="fx-ssh")` gives facts (host fx-ssh, 40 cpus, scheduler none), a route for each of the five verbs and `sign_in: null`; `exec` returns `fx-ssh`. `describe()` with no site carries no per-site status; that is W1's service.
- LQ-L1 (job and file half), real Ares Slurm, through MCP with the catalog's `ares` site and `remote_dir: clio-relay-rework/w7-scratch/relay-remote`, at `3f31cf1`:
  - `files stage` of 530288 bytes, then `files write`.
  - Handle path, `run` with `wait_s: 0`: `op_762577707ebfa2cb494c0e8c`, Slurm job 27722 on `debug`, `ares-comp-03`. The first poll, 3.00 s after submit, read `running`; `completed` with exit 0 at 31.43 s. stdout carries the staged input's sha256; outputs are exactly `result.txt`, `stderr` and `stdout`; `files read` and `files list` on the job directory work.
  - Tasks path (`io.modelcontextprotocol/tasks`): `op_af3050dd70a6b3d5db8a5b81`, Slurm job 27723, task created in 1.53 s, completed with exit 0.
  - The service half is W9's.
- No walltime of relay's own (code at `f768fc9`, retested at `14b83ae`):
  - `test_no_duration_renders_no_walltime[slurm|pbs]`: the rendered script has no `--time` or `walltime` line.
  - `test_a_job_without_a_duration_gets_the_schedulers_limit[fx-slurm|fx-pbs]`: `scontrol show job` gives `TimeLimit=UNLIMITED` and `qstat -f -F json` gives no `Resource_List.walltime` for a job with no duration; with 15 minutes, both show `00:15:00` and the script has the directive.
  - Live, through the adapter: `sh -c "sleep 660; echo slept 660"` with no duration, on fx-slurm (`op_fd5e6c4933b6e32c552ececa`, Slurm job 53) and fx-pbs (`op_d38a82aafd767420b31f9417`, `32.fx-pbs`) together. No time directives; both `completed` with exit 0 after 674.7 s, stdout `slept 660`.
- exec (W1's interface): `test_exec_without_time_limit` passes on all three targets; `test_a_quiet_process_gets_stall_reports`; `test_the_exec_watchdog_stops_the_command_when_stdin_closes`; `test_cancelling_exec_stops_the_remote_command` on fx-ssh cancels an exec of `sleep 3600` and the remote pid goes away.
- Forgotten jobs: `test_a_job_the_scheduler_forgot_is_read_from_its_own_record` on fx-slurm submits a job that prints and one `sleep 3600`, checks both scripts carry `#SBATCH --job-name="relay-..."`, drops the adapter, `scancel`s the second outside relay, and waits until `squeue -t all` lists neither (MinJobAge 300 s). A new adapter then finds both: the first is `completed`, exit 0, `native_state: gone`, from its `.ec`; the second is `cancelled` from `sacct`. Units cover `sacct` parsing and the unknown branch.
- After merging integration `14e8edd`, at `14b83ae`: `pytest tests/relay_ssh`, `78 passed in 491.72s`. `ruff check`, `ruff format` and `pyright` clean.

## W8. The worker adapter and the old surface's removal

## W9. serve

## W10. Tapis

Branch `jaimecernuda-relay-rework-w10`; the commit sent to W1 is named in the report to the coordinator. Rows: RL46 to RL49 (RL50's plugin row is W14's), LQ35 to LQ40, and the live rows LQ-L14, LQ-L15 and LQ-L17. Page: [docs/adapters/tapis.md](../adapters/tapis.md).

Files: `src/clio_relay/adapters/tapis/` (adapter, client, files, launcher, mapping, register, the `python -m clio_relay.adapters.tapis` driver and `tapisjob_app.sh`), `src/clio_relay/identity/tacc.py`, `catalog/sites/{chameleon-tapis,ares-tapis,vista,stampede3}.yaml` and their skills, `tests/fixtures/tapis_stub/` and `tests/relay_tapis/`.

### Decisions

- **Every Tapis call goes through tapipy 25.4.0, in a worker thread.** `client.TapisClient.tapis_call(service, operation, **kwargs)` builds one `Tapis` object per call from the keyring token, with no spec download (`resource_set="local"`). An HTTP 401 becomes `needs_sign_in`; other errors become typed refusals carrying Tapis's own message. Reason: tapipy is synchronous, and a stored client would hold a token past its refresh.
- **The launcher is one shell script in a gzip tar, run by Tapis's `ZIP` runtime (RL47).** The tar is byte-stable (mtime 0, USTAR, fixed mode). The app id is the system id for a relay-made system, otherwise `clio-relay.<username>.<system id>`, and the version is `1.0-<first 10 hex of the script's SHA-256>`, so a changed launcher is a new app version and an old job keeps its own. The archive goes to `<work dir>/launcher/` through Tapis Files.
- **The job spec travels as one base64 argument** of NUL-terminated fields. Reason: nothing is quoted through a shell, and Tapis's argument handling cannot split it.
- **Inputs are staged by content into `<work dir>/content/<sha256>` and passed as `tapislocal` file inputs.** Tapis does not copy them; the launcher links them into the job's directory. The launcher writes `relay.exitcode`, which `status` reads once the job ends.
- **`find` searches Tapis jobs for the description `clio-relay op <operation id>`** (`getJobSearchList` with `description.eq`), so a retried `run` after a restart finds the job it already submitted.
- **Job type:** `BATCH` on a Slurm system, `FORK` on a system with no scheduler and `canExec`. Any other scheduler is refused with `verb_not_implemented`.
- **`exec` and `serve` are not implemented** and route to the site's `ssh` or `worker` alternative. The catalog's `ares-tapis` and `chameleon-tapis` name `ssh`; `vista` and `stampede3` name `worker`, because those hosts ask for a second factor.
- **Guarded sites refuse a free-form `run` in the adapter, before any Tapis call,** with `policy_refused` and `next_step: package_review`. The service's own guard (W13) refuses first when it is installed.
- **Own host (`authn: pki`).** Relay reads the login, home and Slurm partitions through the site's `ssh` alternative, creates `clio-relay.<username>.<site id>` (job working directory `~/.clio-relay/tapis`, one logical queue per partition), makes a new RSA 4096 pair in a private temp file, appends the public key to `authorized_keys` with the comment `clio-relay-tapis:<system id>`, gives the pair to Tapis with `createUserCredential`, and deletes the private file. If Tapis's credential check fails, the key line is taken back out. `unregister-host` removes the credential, the system only when relay made it, and the key line.
- **TACC host (`authn: tms`).** Relay calls `createUserCredential` with `createTmsKeys=true` and makes no key. It never creates or deletes a TACC system.
- **The TACC identity is the device code grant, kept in the keyring (RL48).** `TaccIdentity` implements W6's `IdentityProvider` with `relay_page=False`: the ask carries TACC's own `https` page and the short user code. The device code, the access token and the refresh token stay in the keyring (service `clio-relay`) and are never logged, written to relay's files or returned. Refresh uses the `refresh_token` grant and needs the client key; a refused refresh signs out and the next call asks again. The types are mirrored from W6's published interface until W6 merges, and become imports then; the swap is ready and checked against W6 at 03c2819.
- **No hard limits.** Jobs, sign-ins and transfers have none. The device code's own 600 s expiry is Tapis's. The step carries it as `expires_at` with `poll_interval_s` 5, and `poll` answers `expired` for a dead code, whether the time passed or Tapis says so. Relay's device loop (W6's `device.follow`) then calls `begin` for a new code on the same ask.
- **A driver for live runs:** `python -m clio_relay.adapters.tapis {status,sign-in,sign-out,lifetimes,refresh,describe,register-host,unregister-host} --site <id>`. It prints the page, the code and token expiry times, never a token. It is the run sheets' tool and the TACC demo's.

### Facts

- **tapipy 25.4.0 operations behind each call** (read from the 25.4.0 wheel's bundled specs):

  | Call | Operations |
  | --- | --- |
  | sign-in | `authenticator.generate_device_code`, `authenticator.create_token` (`grant_type=device_code`) |
  | refresh | `authenticator.create_token` (`grant_type=refresh_token`, client id and key) |
  | sign-out | `authenticator.revoke_token` (access and refresh) |
  | describe | `systems.getSystem`, `systems.checkUserCredential` |
  | run | `apps.getApp`, `apps.createAppVersion`, `files.insert`, `files.mkdir`, `files.getStatInfo`, `jobs.submitJob` |
  | status, cancel | `jobs.getJob`, `jobs.getJobStatus`, `jobs.cancelJob` |
  | find | `jobs.getJobSearchList` |
  | files | `files.listFiles`, `files.getContents`, `files.insert`, `files.mkdir`, `files.moveCopy`, `files.delete`, `files.getStatInfo` |
  | register-host | `systems.createSystem` (`skipCredentialCheck`), `systems.createUserCredential` (PKI pair or `createTmsKeys`) |
  | unregister-host | `systems.removeUserCredential`, `systems.deleteSystem` |

- **Token lifetimes.** The Tapis authenticator issues device-code access tokens for 30 days, against the plan's assumed 4 h. A device code lives 600 s, and the authenticator sends no `interval` and no `slow_down`, so relay polls every 5 s (RFC 8628's default). The device code response's `expires_in` is a date string, not seconds. **The refresh-token lifetime is not yet known**; it is recorded at LQ-L14.
- **TACC pages behind the catalog entries** (each fetched with HTTP 200):
  - `vista`: https://docs.tacc.utexas.edu/hpc/vista/
  - `stampede3`: https://docs.tacc.utexas.edu/hpc/stampede3/
  - TACC's second factor: https://docs.tacc.utexas.edu/basics/mfa/
  - `ares-tapis`: https://tapis.readthedocs.io/en/latest/technical/systems.html
  - the sign-in: https://tapis.readthedocs.io/en/latest/technical/authentication.html
  - `chameleon-tapis`: https://chameleoncloud.readthedocs.io/en/latest/getting-started/index.html
- **Registration results for Ares and Chameleon: pending.** They need O5, O6 and the person's sign-in. Recorded at LQ-L14 and LQ-L17.
- **Not yet confirmed against the real Tapis** (the stub follows the specs; each is checked in the live rows):
  - `description.eq` in a job search;
  - `getContents`'s `range` semantics, and `listFiles`'s size unit;
  - whether `containerImage` for a `ZIP` app may be a path on the system;
  - that `tapislocal` inputs are not copied;
  - whether `batchLogicalQueues` must be set on a Slurm system;
  - `${apiUserId}` in `effectiveUserId` together with a `loginUser` credential;
  - that Tapis accepts the RSA PEM relay writes.

### Evidence

All on Ares, in `~/clio-relay-rework/clio-relay-w10` with its own venv (`uv sync --extra tapis`, plus `keyrings.alt`), against the stub in `tests/fixtures/tapis_stub` and W2's `fx-ssh` and `fx-slurm`. Ruff, format and pyright are clean on the W10 paths. Tests ran at f9d61c8 (integration ee1a845, with W7, merged): conformance 11, identity 10, register 3 and service 5, all 29 passing. In a scratch worktree with W6 (03c2819) merged and the mirrored types swapped for W6's imports, all 30 pass, the extra one driving W6's `device.follow` through an expired code to a sign-in; pyright is clean there too.

| Row | State | Command | What was read |
| --- | --- | --- | --- |
| LQ35 | Passed | `pytest tests/relay_tapis/test_tapis_conformance.py` | 11 of 11 with W1's kit at 14e8edd, including `test_exec_bounded`, `test_exec_without_time_limit` and `test_unimplemented_verbs_refuse` asserting `verb_not_implemented` for `exec` and `serve` |
| LQ36 | Passed | `test_tapis_service.py::test_run_survives_a_restart`, `::test_find_reconciles_through_tapis_search` | The service is closed and reopened mid-job; the task completes. Operation states `awaiting_input, submitting, queued, running, completed`. Stub job history `PENDING` to `FINISHED` through Tapis's states. The task result carries the Tapis UUID. The request named `appId clio-relay.alice.tp`, `jobType FORK`, and a `tapislocal://exec.tapis/.../content/<sha256>` input with target `input.txt`. `find` returns the same job |
| LQ37 | Passed | `test_tapis_service.py::test_describe_routes_exec_and_serve_to_ssh` | With W7 merged into integration: `exec` and `serve` route `{adapter: ssh, via: alternative}`; `describe`, `run` and `files` stay on `tapis` |
| LQ38 | Passed | `test_tapis_service.py::test_guarded_site_refuses_a_free_form_run` | The service refuses with `guard_unavailable` until W13's guard is installed; the adapter's own `run` refuses with `policy_refused`, `next_step: package_review`. The stub log has no `/v3/jobs` request |
| LQ39 | Passed | `pytest tests/relay_tapis/test_tapis_identity.py` | 10 of 10. One URL elicitation: `https://tapis-stub.invalid/v3/oauth2/device?client_id=clio-relay-test`, "Sign in to TACC. Open the TACC page, enter the code XXXX and sign in with your TACC account." No password asked. The access token, refresh token, device code and client key are absent from the ask records, relay's log, relay's state files and the MCP transcript. A client without elicitation gets `needs_sign_in` with the same `https` page and resumes once the ask is answered. Throttled polling, an expired code (reported as `expired`, by time or by Tapis, with a new code from the next `begin`), refresh, no client key, a refused refresh, sign-out, and the driver's `lifetimes` and `refresh` (no token in their output) each pass |
| LQ40 | Passed | `pytest tests/relay_tapis/test_tapis_register.py` | 3 of 3. `Registration(system_id='clio-relay.alice.lq40', created_system=True, authn='pki', login_user='relay', key_installed=True, private_key_deleted=True)`. The stub's credential check logs in to `fx-ssh` with the registered key, and the public key is in `authorized_keys`. An audit hook on `open` during registration saw 5 files, none under `~/.ssh` or the fixture's ssh directory. Removal `{credential: True, system: True, key_line: True}`. A refused credential gives `submit_failed` "Credential check failed: no login" and the key line is gone. On `fx-slurm` the queues are `[compute, debug]` and the job type is `BATCH` |

Also on Ares:
- **The TMS path** (`test_a_tacc_host_gets_a_tms_credential_and_no_key`): `Registration(system_id='stampede3-tapis-stub', created_system=False, authn='tms', key_installed=False)`. Unregistering removes the credential only.
- **The live driver against the standalone stub** (`python -m tests.fixtures.tapis_stub --port 47180`, file keyring in scratch, `fx-ssh` as the host): `status` signed_out; `sign-in` prints the page and the code, then "Signed in." with source keyring, account alice, expiry 30 days out; `register-host` and `describe` report credential ok, job type `FORK` and app `clio-relay.alice.fx-tapis` version `1.0-540fce7250`; `unregister-host` returns all true; `sign-out` returns to signed_out. No token appears in the output.

### Run sheets for the live rows

All three are **pending**, never passed. They wait on the owner blockers below. The person signs in and supplies credentials; W10 runs the rows. Before each run: `ssh ares`, `cd ~/clio-relay-rework/clio-relay-w10`, `git pull`, `~/.local/bin/uv sync --extra tapis`. The person sets the OAuth client in the shell themselves (`export CLIO_RELAY_TACC_CLIENT_ID=...` and `CLIO_RELAY_TACC_CLIENT_KEY=...`); neither is pasted into a chat. `T=python -m clio_relay.adapters.tapis`.

**Sign-in (shared by all three).**
1. `uv run $T sign-in --site chameleon-tapis`. It prints TACC's page and a code.
2. The person opens the page, enters the code, and signs in with their TACC account and second factor. The driver prints "Signed in."
3. `uv run $T status --site chameleon-tapis`. Record `account` and `expires_at`, never a token.
4. `uv run $T lifetimes --site chameleon-tapis`. It prints, from the tokens' claims, when each token expires and the seconds left, and no token text. Record the access lifetime (expected 30 days) and **the refresh-token lifetime**, the ledger fact still open.
5. `uv run $T refresh --site chameleon-tapis`. Expect "A new access token was issued." This confirms the refresh grant with relay's client key.

**LQ-L14, `chameleon-tapis`.** Needs O3, O4, O5.
1. Add `chameleon-tapis` from the catalog, with `alternatives.ssh.ssh_profile` set to the person's profile for the node. Check `ssh <profile> hostname` first.
2. `uv run $T register-host --site chameleon-tapis`. Record the whole `Registration` (system id, login user, key installed, private key deleted, Tapis's message).
3. `uv run $T describe --site chameleon-tapis`. Expect credential ok and job type `FORK`.
4. Start relay's MCP server on that site list and, from an MCP client, call `run` with `/bin/sh -c "hostname; date; echo relay-l14 > out.txt"`. Poll the task to completion.
5. Record the Tapis job UUID from the task result, and the time from `submitting` to `running` from the operation's history.
6. Read `out.txt` back with the `files` tool and check it says `relay-l14`.
7. Keep the system for LQ-L15. After LQ-L15: `uv run $T unregister-host --site chameleon-tapis`, then check `grep clio-relay-tapis ~/.ssh/authorized_keys` on the node is empty.

**LQ-L15, the TACC demo on `chameleon-tapis`.** Needs LQ-L14, and on the node: `jarvis` on PATH with the `clio_jarvis` repo added (W5, O1), ADIOS2 and MPI for the solvers. Needs W14's ParaView tab (and W15's follow) on the desktop side.
1. From the agent, ask: "Run Gray-Scott on Chameleon through Tapis and show me the field."
2. Expect a package `run` (`clio_jarvis.gray_scott`, version and SHA-256), a Tapis job UUID, the outputs read back through Tapis Files, and the BP5 output opening in the ParaView tab.
3. Repeat with Xcompact3d (`clio_jarvis.xcompact3d`, TGV).
4. Record both UUIDs, both submit-to-running times, and a screenshot of each field, in the science log.

**LQ-L17, `ares-tapis`.** Needs O3, O4, O6.
1. Add `ares-tapis` from the catalog (`alternatives.ssh.ssh_profile: ares`).
2. `uv run $T register-host --site ares-tapis`. Record the `Registration`, including the queues (Ares's Slurm partitions).
3. **The contract exception.** If Tapis's credential check cannot log in to Ares from TACC, the driver prints `submit_failed` with Tapis's own message, and the key line is removed. Record that message verbatim as the row's result; LQ-L14 then carries the adapter. Stop here.
4. Otherwise `describe` (job type `BATCH`), then `run` from an MCP client with `/bin/sh -c "hostname; echo relay-l17 > out.txt"` on queue `debug`.
5. Record the Tapis job UUID, the Slurm job id Tapis reports (`remoteJobId` in `getJob`), and the submit-to-running time.
6. Read `out.txt` back with `files`. Then `unregister-host` and check the key line is gone from `~/.ssh/authorized_keys` on Ares.

### Owner blockers

| Blocker | What the person supplies | Rows |
| --- | --- | --- |
| O3 | A TACC account, signed in once through the driver | LQ-L14, LQ-L15, LQ-L17 |
| O4 | An OAuth client in the `tacc` tenant: its id and key, set as `CLIO_RELAY_TACC_CLIENT_ID` and `CLIO_RELAY_TACC_CLIENT_KEY` | All three. Without the key, sign-in works but refresh does not |
| O5 | A leased Chameleon node with a floating IP, and an SSH profile for it | LQ-L14, LQ-L15 |
| O6 | Confirmation that Ares accepts SSH from outside IIT with a key and no second factor, and that a key held by Tapis is acceptable | LQ-L17 |
| O7 | TACC's answers on Vista and Stampede3: whether relay's TMS credential and a `ZIP` app are allowed there, and the allocation to use | Catalog entries `vista` and `stampede3`; no live row yet |

### Open

- **The service does not resume a no-elicitation client after a device-flow sign-in.** `RelayService.advance` treats a pending URL ask as waiting and never asks the adapter whether sign-in is still needed. With `relay_page=False` nothing answers the ask, so the client stays at `needs_sign_in` until someone answers it. Suggested: for a pending URL ask, resolve it as accepted when `sign_in_needed()` returns None. Owner of `surface/service.py`.
- **When W6 merges,** tacc.py swaps its mirrored types for imports from `clio_relay.identity.base`, and the identity tests gain the `device.follow` test. Both are ready and pass against W6 at 03c2819.
- **Adding a catalog site does not register the host yet.** Until an add-site hook calls `register_host`, the run sheets call `register-host` by hand.
- **W13:** confirm the command the launcher's package mode runs (`jarvis` with the unpacked `clio_jarvis` package).
- **W1:** link `docs/adapters/tapis.md` from the docs index; note `tests/relay_tapis/` as a test directory.
- **Shared fixture:** LQ40 edits `fx-ssh`'s `authorized_keys` only on lines with the comment `clio-relay-tapis:<system id>`, and removes them in `finally`.

## W11. globus, iri and sky

Branch `jaimecernuda-relay-rework-w11`, from `6b32300`. Code: `src/clio_relay/adapters/{globus,iri,sky}/`. Catalog: `catalog/sites/{polaris,aurora,deltaai}.yaml` and the skills `alcf-polaris`, `alcf-aurora` and `ncsa-deltaai`. Pages: `docs/adapters/{globus,iri,sky}.md`. hpc-bridge was read as a reference only; nothing is imported from it.

All W11 evidence below comes from unit and mock tests. None of it is a live pass. The live rows LQ-L2 to LQ-L6 and LQ-L8 belong to the person and have the run sheets at the end of this section.

### Decisions

- **The `sky` adapter never imports SkyPilot (coordinator decision, recorded at `a60bc01`).** SkyPilot 0.14.3 caps `click` below 8.2 and `uvicorn` below 0.36, and the universal lock would force those downgrades on every relay install. So relay has no `sky` extra. The adapter runs `adapters/sky/bridge.py` as a subprocess in SkyPilot's own Python. The bridge uses only the standard library and `sky`. It writes one JSON answer on a line starting `CLIO_RELAY_SKY`, and it moves SkyPilot's own stdout to stderr.
  - The Python comes from the site's `python` setting. When that is empty, relay tries `~/.clio-relay/sky/bin/python`, then the Python next to a `sky` command on `PATH` or the one its first line names.
  - With none found, `build()` refuses `adapter_unavailable` with the install steps. A Python that cannot import `sky` gives the same refusal on the first call.
  - The spend price goes through the same subprocess.
  - The install step is in `docs/adapters/sky.md`.
- **globus-compute-sdk is pinned to 4.21.0 (W1, `6b32300`).** 4.22 is an alpha. Stable 4.21.0 is what the adapter and its tests were written against.
- **Credentials come from W6's identity layer.** `tokens.provider("globus")` calls `clio_relay.identity.shared().get("globus")`, the provider relay is serving with, so it sees `--grant-file`. `identity.get` is not used: it returns the factory. W6's `Token.access_token` is a `SecretStr` and is revealed only to build the `Authorization` header; `status()` is async and returns an `IdentityStatus` whose `state` the adapters report. Where `clio_relay.identity` is absent, `GrantFileProvider` stands in, reading the grant file named by `CLIO_RELAY_GRANT_FILE_GLOBUS`. The adapters never start a sign-in. With no token, or an expired one, they refuse `needs_sign_in`, and `sign_in_needed()` names the provider.
- **globus: one Compute task per job, cancelled with a marker.** The task is `relay_task`, a self-contained function that uses only the standard library, because Compute ships functions by source.
  - The task writes the inputs and `relay-state.json` into `<job_root>/<operation id>/`. It runs the command in its own process group and polls for a `relay-cancel` marker.
  - `cancel` sends a second task that writes the marker, because the Compute service cannot cancel a submitted task.
  - A task the endpoint retries finds the final record and does not run the job again.
  - Submission uses `create_batch` and `batch_run`, and polling uses `get_task`. The SDK `Executor` is never opened, since it holds a long-lived connection.
- **globus: `user_config_keys` maps the job onto the endpoint's template.** The defaults, `account`, `queue`, `walltime` and `nodes_per_block`, are ALCF's keys. Each facility names them differently.
- **globus `files` is Globus Transfer between two collections.** One is the site's; the other is on relay's own machine, for example Globus Connect Personal. A transfer that keeps faulting is cancelled and refused. A transfer that is making progress has no time limit.
- **globus: the person's own endpoint starts through another adapter.** `start` names `worker` or `ssh` and the start command, for Aurora. It goes through W1's `exec` with no time limit (since `14e8edd`); quiet stretches are logged, and `start.timeout_s` sets a limit only if the site asks for one. A failed start refuses `unreachable` before anything is submitted. The IRI `token_command` runs the same way, with no limit.
- **iri: v2 is the default, and v1 is supported.**
  - In v2, `run` sends an `Idempotency-Key`, uuid5 of `clio-relay:<site>:<operation id>`, so a retried submit returns the same job.
  - v1 has no key. Relay's own record under `state_dir/iri-jobs/` covers `find`.
  - The two versions differ in where file-call parameters go. The adapter handles both, and the fake facility checks both.
- **iri tokens.** With `identity: globus`, the token comes from the Globus identity, for the facility's resource server and scope. With `token_command`, it is the stdout of a command, for facilities that issue their own tokens.
- **iri `files`.** File calls are facility tasks, polled with backoff and stall detection, and have no time limit. Reads and writes are capped at 5 MiB, the API's limit. Anything larger is refused with a pointer to the `globus` adapter.
- **sky runs managed jobs, with `describe` and `run` only.** `serve`, and autostop as the idle release, belong to W9.
  - The job is named `relay-<operation id>`.
  - Inputs are materialized into a work directory that SkyPilot syncs. A site `workdir` together with inputs is refused `invalid_spec`.
  - When the job ends, its log is downloaded as the output.
  - SkyPilot reports no exit code for a managed job. A failure is recorded with exit code 1, unless the failure reason names a code, and the message says so.
  - There is no hard limit on a SkyPilot call. Relay stops waiting only when SkyPilot prints nothing for `stall_s` (1800 s).
- **sky cloud keys are secrets in W6's token store.** `credentials.env` maps variables to secret names, such as `AWS_ACCESS_KEY_ID: aws-access-key-id`, read with `TokenStore().secret(name)` (stored by W6 as `secret:<name>` under its keyring service; W11 writes nothing there). A missing secret refuses `needs_sign_in`, and `sign_in_needed()` returns `SignInNeed(provider="keyring:<name>")`, so relay's page asks the person to type it. Without `credentials`, SkyPilot uses the cloud profiles already on the machine.
- **The spend price is `SkyAdapter.price(spec) -> Charge`, in US dollars.**
  - `amount` is SkyPilot's `Resources.get_cost(walltime)` times the node count.
  - `unit` is `"USD"`, and `nodes`, `walltime_s` and `node_hours` are filled in. `Charge.unit` is free text, so W13 needs no new field.
  - The price lookup is offline, from SkyPilot's catalog, and needs no cloud account.
- **The catalog entries are `guarded` with `spend: {ask: always}`.**
  - Aurora has no `endpoint`: the person sets their own, and building the site refuses `invalid_config` until they do.
  - DeltaAI sends the queue as `partition`. NCSA does not publish the template, so LQ-L8 checks that first.
- **The fakes ship in the package for reuse (W2, W16).**
  - `adapters.globus.fakes` provides `LocalComputeService` and `LocalTransferService`.
  - `adapters.iri.fakes.FakeIriFacility` is an `httpx.MockTransport` handler. It validates job specs against the published field sets.
  - `adapters/sky/fake_sky/sky/` is a stand-in `sky` package with SkyPilot 0.14.3's call shapes. It goes on the bridge's `PYTHONPATH`.

### Facts

| Fact | Value | Source |
| --- | --- | --- |
| Polaris address and scheduler | `polaris.alcf.anl.gov`, PBS Pro. MFA with MobilePASS+ or a CRYPTOCard | https://docs.alcf.anl.gov/polaris/ |
| Polaris hardware | 560 nodes. Each has 1 AMD EPYC 7543P, 512 GB and 4 NVIDIA A100 | https://docs.alcf.anl.gov/polaris/ |
| Polaris queues | debug (1 to 2 nodes, 1 h), debug-scaling (1 to 10, 1 h), prod (10 to 496, 24 h), preemptable (1 to 10, 72 h), demand (1 to 56), capacity (1 to 4, 168 h). `-A` and `-l filesystems=` are required | https://docs.alcf.anl.gov/polaris/running-jobs/ |
| Polaris Globus Compute endpoint | multi-user endpoint `9a947ba5-f537-4681-acf3-cc66485aadec`, running Python 3.13 | https://docs.alcf.anl.gov/services/globus-compute/ |
| ALCF endpoint user-configuration keys | `account` and `queue` are required. Optional: `walltime`, `nodes_per_block`, `worker_init`, `scheduler_options` (default `#PBS -l filesystems=home`) and `max_idletime` (240) | https://docs.alcf.anl.gov/services/globus-compute/ |
| ALCF Globus collections | `alcf#dtn_home` `9032dd3a-e841-4687-a163-2720da731b5b`, `alcf#dtn_eagle` `05d2c76a-e867-4f67-aa57-76edeb0beda0`, `alcf#dtn_flare` `f39a7a0f-5bfc-46ce-9615-ba9f8592814f` | https://docs.alcf.anl.gov/data-management/data-transfer/using-globus/ |
| Aurora address and scheduler | `aurora.alcf.anl.gov`, PBS Pro. 10,624 nodes, each with 2 Xeon CPU Max and 6 GPU Max 1550. File system `/flare` | https://docs.alcf.anl.gov/aurora/ |
| Aurora queues | debug (1 to 2, 1 h), debug-scaling (2 to 256, 1 h), prod (256 to 10,624, 24 h), capacity (1 to 16, 168 h), visualization (1 to 32, 8 h) | https://docs.alcf.anl.gov/aurora/running-jobs-aurora/ |
| Aurora endpoint | no facility endpoint. Users run their own on a login node, with ALCF's template `configs/aurora` | https://docs.alcf.anl.gov/services/globus-compute/, https://github.com/argonne-lcf/alcf-globus-compute |
| DeltaAI address and scheduler | `dtai-login.delta.ncsa.illinois.edu`, Duo MFA, Slurm | https://docs.ncsa.illinois.edu/systems/deltaai/en/latest/ |
| DeltaAI partitions and accounts | ghx4 (48 h), ghx4-interactive (4 nodes, 2 h). 4 NVIDIA GH200 per node. Accounts end in `-dtai-gh`. File systems `/u`, `/projects`, `/work/hdd`, `/work/nvme` | https://docs.ncsa.illinois.edu/systems/deltaai/en/latest/ |
| DeltaAI Globus Compute endpoint | multi-user endpoint `eb28e239-288a-4974-8341-27c49cacb37a`. Its template keys are not published | https://docs.ncsa.illinois.edu/en/latest/software/portals/diamond.html |
| IRI API versions | 1.0.0 and 2.0.0, OpenAPI 3.1.0. NERSC and ALCF serve both | https://api.iri.nersc.gov/openapi.json, https://api.iri.nersc.gov/api/v2/openapi.json, https://api.alcf.anl.gov/api/v1/openapi.json, https://api.alcf.anl.gov/api/v2/openapi.json |
| IRI token type | `HTTPBearer` (`type: http`, `scheme: bearer`). At NERSC the bearer is a Globus Auth access token for scope `https://auth.globus.org/scopes/ed3e577d-f7f3-4639-b96e-ff5a8445d699/iri_api` | the same OpenAPI documents (`components.securitySchemes`). NERSC's scope is from https://api.iri.nersc.gov |
| IRI base URLs | NERSC `https://api.iri.nersc.gov`, ALCF `https://api.alcf.anl.gov`, ESnet (development) `https://iri-dev.ppg.es.net`. NCSA serves none | https://docs.alcf.anl.gov/services/iri-api/ and the OpenAPI `servers` entries |
| SkyPilot version | 0.14.3. Calls used: `jobs.launch`, `jobs.queue_v2`, `jobs.cancel`, `jobs.download_logs`, `Resources.get_cost` | probed in `~/clio-relay-rework/w11-sky-venv` on Ares, https://docs.skypilot.co/ |
| SkyPilot price examples | AWS `m6i.large` at 0.096 USD/h on demand. 2 nodes for 1 h is 0.192 USD on demand and 0.0202 spot. 2 × `p4d.24xlarge` (A100:8) for 1 h is 43.91528 USD | the adapter's `price` through real SkyPilot 0.14.3, Ares, no credentials (Evidence) |

### Evidence

All of this is unit and mock evidence.

- **Gate on Ares** at `08c2880` (integration `14e8edd` merged, W6's identity API adopted), in the clone `~/clio-relay-rework/clio-relay-w11` with its own venv (`uv sync --extra globus --extra testing`):
  - `uv run pytest -q -rs tests/relay_next/test_globus_adapter.py tests/relay_next/test_iri_adapter.py tests/relay_next/test_sky_adapter.py tests/relay_next/test_w11_catalog.py`: exit 0, 99 passed, none skipped. The kit asserts the `verb_not_implemented` refusal for `exec` on all four targets and `files` on sky, and runs `exec` with no limit. The globus endpoint-start tests cover no default limit, a set limit passed through, stall reporting wired, and a failed start refusing `unreachable` before submit. The four W6 tests cover the `SecretStr` token and async `IdentityStatus`, `shared().get` over `identity.get`, cloud keys read from `TokenStore`, and the old `credentials.service` setting refused.
  - The same modules, ruff and pyright on a scratch merge with `-w6` at `4bad7cb` (local, not pushed): exit 0, 99 passed, clean, 0 errors. A smoke script on that merge with `CLIO_RELAY_KEYRING=memory` showed `tokens.provider("globus")` returning W6's `GlobusIdentity`, status `signed_out`, the bearer refusing `needs_sign_in`, the sky adapter asking `keyring:aws-access-key-id`, and after `TokenStore().set_secret` the key handed to SkyPilot's environment.
  - Earlier gates: `83f81dc` gave 95 passed; `3015e81` gave 82 passed and 5 skipped, before the kit asserted refusals.
  - `uv run ruff check` and `ruff format --check` on the W11 paths: clean.
  - `pyright` 1.1.411 on the W11 paths: 0 errors.
- **What the mocks cover.**
  - Conformance runs against the globus fakes, the IRI fake facility (v2 and v1), and the sky bridge with the stand-in `sky`. The sky case runs the real `bridge.py` in a separate Python process.
  - Unit tests cover:
    - globus: refusals, user-config mapping, walltime formats, marker cancel, retry safety, transfer faults and endpoint start;
    - iri: idempotency, v1 query parameters, problem documents, token sources, stall, the 5 MiB refusal and download forms;
    - sky: price, spot and overrides, `adapter_unavailable` for a missing Python and for a missing `sky`, keyring credentials, launch request fields, command rendering and exit code 1;
    - catalog: each entry validates against `GlobusConfig`.
- **Real SkyPilot smoke on Ares** (no credentials, `HOME=~/clio-relay-rework/w11-sky-home`):
  - Setup: `SkyAdapter` in relay's venv, with `python` pointing at the SkyPilot 0.14.3 venv.
  - `describe` reported `skypilot_version: 0.14.3`.
  - `price` for 2 nodes and 1 h on `infra: aws, cpus: 2+` gave `{"amount": 0.192, "unit": "USD", "node_hours": 2.0}`. Spot gave 0.0202, and `accelerators: A100:8` gave 43.91528.
  - This shows the subprocess path and the offline price work with SkyPilot itself. It launched nothing.

### Run sheets for the person rows

These need the integrated relay: W1's verbs, W6's sign-in page, and W13's spend gate. Each sheet first checks the site with `describe` and then runs one job through relay's `run`, from any MCP client. For a check before integration, the adapter can be built directly:

```
uv run --extra globus python -c "import asyncio; from pathlib import Path; from clio_relay import catalog; from clio_relay.adapters import registry; from clio_relay.adapters.base import AdapterContext; from clio_relay.jobs.content import ContentStore; e = catalog.site('polaris'); d = Path.home() / '.clio-relay' / 'preflight'; a = registry.build(e.adapter, e.config, AdapterContext(site_id=e.id, state_dir=d, content=ContentStore(d / 'content'))); print(asyncio.run(a.describe()))"
```

Record for each row: the date, relay's commit, the site entry as used, the handle (`native_id`), the states seen, the exit code, the output names, and a screenshot of the spend gate and the tab. Write the result in W16's summary, never in this section.

**LQ-L2. Polaris through `globus`.**

Needs an ALCF account with a Polaris allocation, and a Globus identity linked to it.

1. Add the catalog site `polaris`. Set `defaults.account` to the project.
2. On relay's page, sign in to Globus. The consent must include Globus Compute (`https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all`) and Transfer (`urn:globus:auth:scope:transfer.api.globus.org:all`).
3. The row turns ready with no SSH. `describe` shows the endpoint `9a947ba5-…` online and the PBS queues. Record it.
4. `run` a reviewed package with `queue: debug`, 1 node and 10 minutes. The spend gate asks first, because the site is `spend: always`; approve it.
5. Watch the states go from queued to running to completed. The first job may wait in the PBS queue while the endpoint's block starts. Record the time to running.
6. Check that `stdout`, `stderr` and the exit code are listed and readable through `files`.
7. Run a second job of `sleep 600` and `cancel` it. Record how long the state takes to become cancelled; the cancel task may queue behind the job.
8. Check `~/.clio-relay/jobs/<operation id>/` on Polaris (through Globus or SSH): `relay-state.json`, `relay-cancel` and the outputs.

**LQ-L3. Polaris through `worker`.**

Needs SSH to Polaris with MobilePASS+, and W8's `worker` adapter.

1. Switch the Polaris row to the `worker` alternative, which uses the `ssh_profile: polaris` SSH profile.
2. On relay's page, start the sign-in and type the one-time code once.
3. Run `describe`, then a 1-node debug job, as in LQ-L2 steps 4 to 6.
4. Leave relay idle and check `describe` at intervals. Record how long the worker stays linked before it needs the code again.

**LQ-L4. Aurora through your own endpoint.**

Needs an ALCF account with an Aurora allocation, and SSH with MobilePASS+.

1. On an Aurora login node, make a Python environment and run `pip install globus-compute-endpoint`.
2. Run `globus-compute-endpoint configure clio-relay`. Replace its config with ALCF's `configs/aurora` template from https://github.com/argonne-lcf/alcf-globus-compute. Keep the keys `account`, `queue`, `walltime` and `nodes_per_block`, or note the template's names.
3. Run `globus-compute-endpoint start clio-relay`. Sign in to Globus when it asks; this happens once. Copy the endpoint id it prints.
4. Add the catalog site `aurora`. Set `config.endpoint` to that id and `defaults.account` to the project. If the template uses other key names, set `config.user_config_keys`.
5. Run `globus-compute-endpoint stop clio-relay`, then `describe` in relay. The endpoint shows offline.
6. `run` a reviewed package in `debug`. Relay starts the endpoint through `worker` with one sign-in (the MobilePASS+ code), then submits. Record that the start happened and how long it took.
7. Check that the job completes and its outputs are listed, as in LQ-L2 steps 5 and 6.

**LQ-L5. An IRI facility (NERSC first, or ALCF).**

Needs a NERSC account with an allocation and IRI access, or the ALCF equivalent.

1. Add a site with `adapter: iri`, `base_url: https://api.iri.nersc.gov`, `api_version: v2`, `identity: globus` and NERSC's scope from the Facts table. For ALCF, use `base_url: https://api.alcf.anl.gov` and a `token_command` that prints a token from ALCF's own tool, since relay has not found ALCF's Globus scope.
2. Sign in, then run `describe`. It lists `/compute/resources`. Set `resource_id` to the compute resource, such as Perlmutter's, and `filesystem_resource_id` if the files resource differs.
3. Record the API version, from `GET /api/v2/` or the OpenAPI document, and the token type: what the facility accepted, a Globus access token or the facility's own.
4. Run `files`: write a 1 KiB file, read it back, list it and stat it. Repeat with a binary file. Record the download form the facility returned.
5. `run` `hostname` with 1 node in the debug queue and the account set. Check that it completes with exit code 0 and `stdout` and `stderr` listed.
6. `run` `sleep 600`, then `cancel` it. Check that the state becomes cancelled.
7. Repeat steps 4 to 6 with `api_version: v1`, if the facility still serves it.

**LQ-L6. AWS through `sky`.**

Needs an AWS account the person agrees to spend on (O11), and an IAM key pair with EC2 rights.

1. On relay's machine, run:
   ```
   uv venv ~/.clio-relay/sky
   uv pip install --python ~/.clio-relay/sky 'skypilot[aws]==0.14.3'
   ```
2. Add a site with `adapter: sky`, `infra: aws`, `cpus: 2+` and `credentials: {provider: aws, env: {AWS_ACCESS_KEY_ID: aws-access-key-id, AWS_SECRET_ACCESS_KEY: aws-secret-access-key}}`.
3. Without the keys, `describe` reports `skypilot_version` and the site shows as needing sign-in. Type the two keys on relay's page.
4. Check that they are in the OS keyring, under W6's service `clio-relay` as `secret:aws-access-key-id` and `secret:aws-secret-access-key`, and not in any file.
5. `run` `echo hello` with 1 node and 15 minutes. The spend gate must show SkyPilot's price in USD, about 0.024 for `m6i.large` on demand. Screenshot it, then approve.
6. Check that the job completes with exit code 0, and that `run.log` is listed and contains `hello`.
7. In the AWS console or `sky status`, check that the job's instance is terminated once the managed job ends. With W9, check that a `serve` cluster autostops when idle.
8. `run` `sleep 1800`, then `cancel` it. Check that the state becomes cancelled and that no instance is left running.

**LQ-L8. DeltaAI through `globus`.**

Needs an ACCESS or NCSA allocation on DeltaAI, and a Globus identity mapped to the NCSA account.

1. Open https://app.globus.org/compute/endpoints/eb28e239-288a-4974-8341-27c49cacb37a and read the endpoint's user configuration schema. If the queue key is not `partition`, or the account key is not `account`, set `config.user_config_keys` to match.
2. Add the catalog site `deltaai`. Set `defaults.account` to the allocation ending in `-dtai-gh`.
3. Sign in to Globus as in LQ-L2 step 2, then run `describe`. Check that the endpoint is online.
4. `run` `nvidia-smi -L` with 1 node, `queue: ghx4` and 10 minutes. Approve the spend gate. Check that it completes with 4 GH200 listed in `stdout`.

**What the person provides**

- O11: allocations and project names on Polaris, Aurora and DeltaAI. NERSC, or ALCF, IRI access. Consent to spend on a paid AWS account.
- Accounts: ALCF accounts with MobilePASS+, and an NCSA account with Duo. A Globus identity linked to each.
- Consent in Globus to the Compute and Transfer scopes and, for LQ-L5, the facility's IRI scope.
- O10: the Globus native-app client id, for hosts that hand relay no grant (W6's sign-in).
- AWS: an IAM access key pair, typed on relay's page.
- On relay's machine: the SkyPilot venv (LQ-L6 step 1).
- On Aurora: the one-time endpoint setup (LQ-L4 steps 1 to 3).
- O8: running the rows themselves.

### Open

- **Compute cancel is a marker.** The Compute service cannot cancel a submitted task. On a busy endpoint, the cancel task may wait behind the job it cancels. LQ-L2 step 7 measures this.
- **A crash between submit and the record write loses the globus task id.** `find` returns nothing, and a retried `run` submits again. Compute has no idempotency key. The window is one local write.
- **Globus Compute status strings** beyond those in SDK 4.21.0's documentation are not verified live. Unknown strings map to queued. LQ-L2 records the strings it sees.
- **ALCF's IRI Globus scope was not found** in ALCF's pages. Until it is, ALCF IRI uses `token_command`.
- **The IRI download format varies by facility.** Binary round trips are checked in LQ-L5 step 4.
- **The DeltaAI template keys are unpublished.** LQ-L8 step 1 checks them.
- **Aurora's endpoint setup is a one-time person step.** Relay starts the endpoint afterwards; it does not create it.
- **Requests to W1:**
  - Add `docs/adapters/{globus,iri,sky}.md` to the docs index.
  - O11's text says "paid cloud account for LQ-L8". The cloud row is LQ-L6, and LQ-L8 is DeltaAI.
- **Settled by W1 at `14e8edd`:** routing now skips an alternative whose build refuses `invalid_config` (Aurora with no `endpoint`, `iri` with no `resource_id`) and records the skip; and `exec` takes no time limit, so the globus endpoint start and the IRI `token_command` have none.
- **Answered by W6 (4bad7cb on `-w6`), and adopted:** the `Token` and `IdentityStatus` shapes, `shared().get`, and cloud keys as `TokenStore` secrets raised as `keyring:<name>`. Until W6 merges, the W11 tests check those shapes against stand-ins.
- **Request to W13:** call `SkyAdapter.price(spec)` from `GuardHook.price` for sky sites. It returns a `Charge` with `unit: "USD"`, and `node_hours` is still filled in for the node-hour threshold.
- **For W9:** SkyPilot autostop is the idle release for `serve`. The `sky` adapter does not implement `serve`.

## W12. gact-tui host semantics and removals

W12, gact-tui host semantics and removals: done (draft PR iowarp/gact-tui#1320).
- HS1/HS2: `A2UISurface.owner` decoded. Server-owned surfaces send actions to their server through `/a2ui/actions`.
  - The outcome (applied, no_update, task_pending, tool_error) is worded under the surface.
  - Attribution reads "From {server} in the {plugin} plugin".
  - The surface is read-only when its server is gone.
  - `a2ui.server_surface.refused` raises a worded toast.
- HS3: a generic `plugin` WorkbenchTab kind.
  - Launchers come from `GET plugin-tabs`; open, close and actions use their routes.
  - `plugin_tab.*` SSE keeps open tabs current; a revision gap reopens the tab.
  - States: loading, empty, unavailable and error with Retry, plus Reload.
  - The pocket width never changes when tabs open.
- HS5: URL questions show Origin and "Trusted because" for configured, plugin_origin and plugin_loopback.
- Removed: the relay engine icon, `ssh_execute`, and the `clio-relay==1.6.8` managed-service entries.
- Proof: live against a stub, at 1440x900 and 390x844, in en and el.

## W13. Guardrails

## W14. Relay's CLIO plugin and the Compute tab

## W15. ParaView follows a growing source (RL44, LQ-L12)

Branch: iowarp/clio-kit `jaimecernuda-relay-rework`, commit 202ce174 (paraview-mcp 3.1.0 to 3.2.0 bump, docs/mcps/paraview.md regenerated) on top of 538703d0 (RL44), on origin/main 490269fb. Scope: mcp-servers/paraview/ only.

### Decisions
- **`open_view` takes a view descriptor.** The new `view` argument is a clio.view-descriptor.v1, vendored from clio-jarvis a6aba08 (`schemas/view-descriptor.v1.json`, sha256 8773b9ed...c1b076, provenance in descriptor.py). The descriptor sets the reader, reader properties, follow and display. Bad display values give a `display_note` and never fail the open. Why: one contract from producer to viewer, and the server stays a generic MCP server, because any host can send the descriptor.
- **Reader: ADIOS2VTXReader on BP5,** refreshed with `ReloadFiles` and a re-read of `TimestepValues`. Why: it follows a growing BP5 on the Kitware 6.2.1 build. The XDMF fallback wasn't needed. W5 writes the `vtk.xml` attribute (ImageData, WholeExtent, Spacing, TIME) that I specified.
- **Follow and park.** A view following the latest step moves to each new step with no reload of the pipeline. A parked view (set_timestep to an earlier step) stays put and reports "N newer step(s) exist"; setting it to the latest step resumes following. Why: the user can inspect a step while the run continues.
- **Progress through MCP v2 tasks as the primary path.** `watch_steps` is a task tool (fastmcp[tasks]); each new step is reported with `report_progress(k, expected_steps, "Step k of N at time T is written; the view follows it.")`, and the task completes when expected_steps is reached or the source stops growing. The panel also posts each step through `ui/update-model-context`. Why: user-confirmed; hosts without tasks still get the panel path.
- **Analysis after the run.** `measure_over_time(array)` returns per-step min, max, mean and range for the field across all steps (structured_content has every step). Why: the agent analyses the run after it ends (headline demo).
- **Colour maps.** ParaView 6.2 preset names resolve without the " (matplotlib)" qualifier. A hand-set range (descriptor `display.range`) survives later representation changes. A free map grows with the data while following; a "Never" lock is respected. Why: the X3 vorticity grew from 1.1 to 12.9 and saturated a first-step map; GS needs a fixed 0 to 0.6.

### Facts
- Runtime: `source ~/clio-relay-rework/paraview/pv-env.sh` on the compute node, then `~/clio-relay-rework/paraview/current/bin/pvpython --force-offscreen-rendering`.
- Attach plus first view takes 17 to 23 s; with `follow.poll_seconds` 2, steps show 2 to 4 s after they are written.
- The last step's progress message can be overtaken by task completion; the result always reports N of N.
- measure_over_time over 60 GS steps (L=128) takes 13.4 s.

### Evidence
- Tests on Ares, in sbatch 27701 (debug): `pytest -q tests` gives 93 passed and 1 skipped (pre-existing, no ffmpeg on the compute node). ruff check, ruff format --check and mypy src are clean. Root `tests/test_generate_server_json_contract.py` and `tests/test_release_workflow.py`: 57 passed. `scripts/check_file_size.py` OK. New tests: tests/test_follow.py; test_node_half growing ADIOS2 BP followed then parked; descriptor display; fixed range kept; free colour maps grow.
- LQ-L12, Gray-Scott, job 27695: L=128, plotgap=200, steps=12000, 4 ranks, 60 steps about 4 s apart. W5's descriptor (V, CELLS, Slice, range [0, 0.6], Viridis). The view followed steps 1 to 11, parked at 11 while step 20 was written ("stays parked at step 11, N newer step(s) exist", up to 29), resumed at 40, and completed 60 of 60. measure_over_time V: 60 steps. Solver exit 0. Screenshots read: open, step_012, step_030, parked_while_40_written, step_050, last; Viridis with a fixed 0 to 0.6 legend.
- LQ-L12, Xcompact3d TGV 65^3, job 27698: ilast=800, ioutput=20, 4 ranks, 40 steps. Descriptor: vort, POINTS, Slice, free range. The view parked at step 7, resumed at 24, and completed 40 of 40. The legend grew to 0 to 7.1 while following and held at 1.2 while parked. Screenshots read: open, step_010, step_020, parked_while_24_written, step_032, last. Job 27696 showed the saturated-map bug that led to the colour-growth fix.
- Not exercised live: the browser `ui/update-model-context` delivery (there is no browser host); front tests cover it.

## W16. Integration and qualification
