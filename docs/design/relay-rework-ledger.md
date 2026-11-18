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
- **Tab action tools are listed only to A2UI clients, as app-only (amends LQ1 and review round 2 #2).** Raised by W14: clio-agent's dispatcher (`server_actions.resolve_server_tool`) and its executor permission gate call only tools that appear in `tools/list`, so tab actions kept out of every listing would leave each Compute tab button broken. Relay lists its tab action tools, marked `_meta.ui.visibility: ["app"]`, only in a `tools/list` request whose `_meta` declares `org.a2ui/a2ui`. That is the standard MCP Apps app-only pattern and needs no host change. A client without A2UI sees exactly the five verbs plus mounted tools. Every action call still needs its single-use action token, so a host that ignores visibility still cannot act. W14 builds it in `surface/a2ui/`, and asks W1 if it needs a hook in the `tools/list` path.
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
  - W11 at 785dcba, then at b8c9cf4 (merge f8a02d2), which moves globus and sky onto W6's identity API. Then at 745308c (merge 3f2d571), which adds regression tests for my duration compat edits in globus, iri and sky. The `credentials.service` setting is gone, and a config that still sets it is refused with `invalid_config`.
  - W7 at c30e348 (merge 82a2302), then its follow-up at cbf2e39 (merge 9490a62). Its exec replaces my compat stopgap c5ea18b. Then at 337ca66 (merge b6b97f1), b6535ae (merge 0ebdaff) and 31f4aca (merge d641426): askpass fallback dropped, a remote file check lists its directory first, Slurm runs in its own process group so cancel traps run, `walltime()` is the duration, and `SshAdapter.price` charges node-hours (no amount without a walltime), kept in `adapters/ssh/pricing.py` so `adapter.py` stays under the size cap. Then at ae221c7 (merge f85e85a), which drops a job's deleted inputs from the site's content store at its final state, and at f8ff1a4 (merge 1739438), which drops transient inputs on the site and has the batch shell wait for the command's trap after passing on a TERM or INT. Then at 60697f7 (merge faf00f8): transient inputs on the ssh adapter, the multi-line `_command_end` fix (`signals.py`), a stop record written once before the group wait, and ssh master hygiene (starts serialized, a running master reused, a stale socket unlinked, the master ended when relay exits). relay_ssh is 88 passed, 0 skipped at 31f4aca.
  - W6 at 4bad7cb, then at 03c2819 (merge a27a8b2), with the device-flow fix. Then at 0945b1a, which adds the crash rerun's evidence and Package, Version and Source rows on the approval page for package reviews. Then at 723c9a2 (merge 6975131): one token store for every identity, the OS keyring when it works, otherwise a 0600 `credentials.json` in the config directory, and `describe` says which.
  - W10 at c5b6c3c, then at 09d1cf8 (merge 303524b), then at 0388291, with `on_site_added` for Tapis and W6's types, then at 5681806 (merge f770403), with Tapis service packages held until cancel (RL45). W10 replaced my compat `_max_minutes`. When Tapis gets no `maxMinutes`, it falls back to the app's limit, then the system's, then 10 minutes, and kills the job. So the mapping always sends one: the duration, else the queue's `maxMinutes` for BATCH jobs, else 525600. At the cutover, the W10 run sheets that name `clio-relay-next` get renamed.
  - W8 at a4dc2cf (merge f8cb43c), which removes the old surface and brings W9 in at 5a01766 (W8's 91bcc3c). The `worker` adapter's CLI is the old `clio-relay` app for now. The old `remote_mcp*`, `scheduler_providers` and `browser_gateway` modules are gone. The gate at f8cb43c: core with tests/serve 453, asks 72, relay_tapis 50, relay_ssh 88, all 0 skipped. The pyright ratchet fell to 5050.
  - W9 at 48bfc40 (merge 0bc5baf), on top of W8: provenance on a service's outputs (7618c7c) and the scancel trap. Then at 36c5bd7 (merge bb2a668): `native_id` on the record, the STUN setting and a visible XTCP fallback, `secrets.env` marked transient, and W9's LQ32 items.
  - W13 at 4b38e74 (merge cd02007): guardrails (LQ16 to LQ21), the ssh price in the guard, service mode for `package_job` and `prepare`, and RL42's helper. R9: the two tests that check a guarded site with no guard installed (`test_refusals_are_typed` and `test_guarded_site_refuses_without_a_guard`) now use a `stub_guard` fixture that sets relay's `DefaultGuard` for the test, because W13's `guard` entry point is installed and answers `policy_refused` otherwise.
  - W8 at 126f8cd (merge 527716a): the seven `browser_*` refusal reasons and `as_browser_gateway_error` are gone (REASONS 75 to 68), and the user docs link `docs/adapters/worker.md` instead of deleted pages. W8's asks to me: `clio-relay-next worker` now mounts the worker adapter's CLI, and `README.md` and `docs/mcp-tasks.md` are rewritten for the five-verb surface. They use the `clio-relay` command name the cutover gives the new surface.
  - W8 at e83091b (merge ba6ecfb): the 1.0 release gate is deleted with the modules only it reached; `validation_report` and its kin become `run_report*`, `install_identity` and `file_digest` with the same wire shape. The pyright ratchet fell to 4929.
  - W9 at 36c5bd7 and W7 at 60697f7 are listed with their workstreams above. Gated together at 115f439 on Ares: core 468, asks 79, guard 86, relay_tapis 50 and relay_ssh 108 passed, 0 skipped; the worker kit passes; pyright 4929; size floors src 81.80 and tests 77.13.
  - Test isolation (W13's wheel-closure finding, and the sky and test_cli failures it also caused): three leaks ran into later tests in the same process. The sky bridge `call()` left its subprocess running with open pipes when it was cancelled, so its transport was collected after its loop closed; it now kills the bridge and reads both pipes to their end on the same loop. psij keeps a daemon queue poller per batch executor that calls `time.sleep` and the scheduler's status command; `tests/relay_next/conftest.py` stops them after each test. `test_cli`'s fake `time.sleep` now acts only on the test's own thread (W8's file).
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
- **Site skills are resources (RL34).** `relay://skills/{name}` serves each skill: the catalog's `catalog/skills/<name>.md`, or the person's own `<config_dir>/skills/<name>.md`, which replaces a shipped one of the same name. `relay://sites/{site}/skill` serves the skill a site links with its `skill` field, which is a skill name or the path of a Markdown file (a relative path is read from the config directory). Both are `text/markdown` for the assistant, listed by a FastMCP provider (`surface/skills.py`, `SkillResources`) that reads the disk on each request, so a skill linked from the command line shows without a restart. A site with no skill, or with a link relay cannot read, has no site skill resource. `describe` gives each site `skill`: `None`, `{"linked", "uri"}`, or `{"linked", "uri": None, "detail"}` when the link is broken. The server looks at the sites file and skills every 5 s and announces a changed list or an edited skill on the change bus. The server instructions name the URI, and `site add --skill` links one.
- **Package reviews are approvals, and the guard prepares what runs (W13's requests R1 to R3, RL24 and RL45).**
  - `check_run`'s decision may carry `data["asks"]`, a list of reviews. Each has a `message` and a `binding` (`kind: "package_review"`, `package`, `version`, `package_hash`, `source`). Relay turns each into an approve ask bound to the site and the spec digest, placed after the sign-in ask and before the spend ask. `advance` checks it like any approval (`surface/guarding.py`, `review_asks`).
  - `GuardHook.prepare(site, spec, *, adapter, service=False) -> JobSpec` returns the spec the adapter submits. Relay calls it in `submit` just before `adapter.run`, and with `service=True` on a service's job just before `ServeHook.start`. It never changes `record.spec`, so the approved digest stays the one the person saw. A refusal from it fails the operation. `DefaultGuard.prepare` returns the spec unchanged.
  - **A service package starts and stays up (RL45, coordinator decision R6 and R7).** W9's live run showed a service package must run `jarvis ppl start` and stop on TERM, not `ppl run`. So `prepare` takes `service: bool = False`, and `_start_service` passes `True`. `PackageRef.service: bool = False` (`jobs/spec.py`) carries it to the adapter. For a `runs_packages` adapter, W13's `prepare` sets it on the submitted copy only; `record.spec` and its digest are unchanged. W13 implements the runner side. Because a spec's digest is its full JSON dump, the new field changes the digest of every package spec. An operation opened before this change keeps its stored digest, so answering its ask still works. Only a resume that passes the package spec again recomputes the digest and refuses with `approval_mismatch`.
  - `BaseAdapter.runs_packages` (default `False`) says the adapter runs a reviewed package rather than the agent's command. Tapis sets it.
  - The entry point `guard = "clio_relay.guard:create_guard"` is registered. It falls back to `DefaultGuard` while `clio_relay.guard` is not installed.
  - The docs index links `docs/guarded-sites.md`, which W13 writes.
- **Outputs carry metadata, and a running job reports its latest step (W9's requests, RL40 and RL43).**
  - `OutputRef.metadata: dict[str, Any]` (default empty) is JSON the adapter or package attaches; relay passes it on unchanged. Two keys have a set meaning, named by constants in `jobs/handles.py`: `view` (`VIEW_METADATA`) holds the output's view descriptor (path or path pattern, format, ParaView reader and its configuration, whether it is still being written), and `provenance` (`PROVENANCE_METADATA`) holds the package version and hash and its input digests. W9's `serve.provenance` goes there.
  - `JobStatus.progress: JobProgress | None` is the latest step an adapter observed: `step`, `total`, `message`, `outputs` (what the step wrote, each with its descriptor) and `data` (adapter-owned JSON). On each observation relay stores it in the operation's `result["progress"]`, so `describe`, the operation's structured result and the final task result carry it in full.
  - The task's `statusMessage` names it: "relay operation is running: step 3 of 10; wrote snapshot 3; views: snapshots" (`surface/progress.py`). The full descriptor is not put in `tasks/get` while the task is working, because the tasks extension's `GetTaskResult` has no `_meta` (its schema forbids extra properties). A client reads the step from the operation instead. This keeps relay a plain MCP server: no client needs a relay-only field to follow a run.
- **Transient inputs never outlive their job (coordinator decision, closing W7's and W9's secret gaps, RL16).** `InputRef.transient: bool = False`. At the final state, whatever the state, each copy of a transient input goes: the adapter drops the site's blob and the job directory's copy, and relay drops its own content-store copy (`surface/transient.py`, `drop_transient`, called wherever a record becomes final). Relay keeps a blob that another open operation still lists in its `input_digests`, so two runs sharing one secret do not break each other. The `local` adapter shares relay's store and unlinks the job copy before it lists outputs, so a transient input never appears as an output. A later run that needs the same bytes stages them again, since `stage` is idempotent. A run opened between the check and the drop can find the blob gone and refuse with `input_missing`; restaging fixes it. Like `PackageRef.service`, the new field enters every spec's digest: stored operations keep their digest, and only a resume that re-sends an old spec refuses with `approval_mismatch`. W9 marks `secrets.env` transient; W7 does the ssh side.
- **Every stored output carries provenance, and view descriptors are checked (W9's RL43 request).** `RelayService._apply` passes each status through `surface/outputs.py`'s `marked_status` before it stores it. That applies to run and serve alike, and to the final outputs and the progress step's outputs. Each output gets W9's `relay.provenance.v1` block under `metadata["provenance"]` (`serve.provenance.with_provenance`), unless it brought its own block. A `metadata["view"]` descriptor is checked with `serve.descriptor.problems`. One that fails the schema is taken off the output, so no client opens a broken view, and its reasons go under `metadata["view_problems"]`.
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
- Skills (`clio_relay.surface.skills`): `skill_uri(name)`, `site_skill_uri(site)`, `skills(config_dir)`, `site_skill`, `skill_link`, and the `SkillResources` provider.
- Routing (`clio_relay.sites.routing`): `route(entry, verb, check) -> Routed(adapter, config, skipped)`, `routes(entry, check) -> list[Route]`, and `Skipped(site, adapter, reason, message)` in `Route.skipped`.
- Command line `clio-relay` (`clio-relay-next` before the cutover): `serve` (stdio, or HTTP with a bearer token, `--grant-file`, `--tls-cert` and `--tls-key`), `adapters`, `site`, `asks`, `connect <handle>`, `rendezvous set|show`, `worker`.

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
- **The token store.** `TokenStore` (`identity/store.py`) is relay's one credential store, and tacc uses it too. It keeps JSON entries under the service `clio-relay`. Secrets typed on the page are kept as `secret:<name>`, read with `TokenStore().secret(name)`, and written with `set_secret` and `delete_secret`.
  - Where entries live: the OS keyring when one is usable. Otherwise they go to `<config_dir>/credentials.json`, mode 0600, written whole through a 0600 temporary file and a rename. A keyring is unusable when it is the `fail` or `null` backend or a read-only probe fails.
  - `CLIO_RELAY_KEYRING` picks one: `keyring`, `file`, or `memory` (in the process, for tests). Unset means the keyring, falling back to the file.
  - A failed read is signed out. A failed write refuses `adapter_unavailable`. A file readable by others is set back to 0600 with a warning.
  - `IdentityStatus.stored_in` (`keyring`, `file` or `memory`) and `store_path` say where, and `Token.source` is `file` for a token from the file. Tapis `describe` shows them as `facts.credentials`.
- **`clio-relay-next asks`** (mounted by `cli_next`):
  - `list [--all]`, `show`, `page`;
  - `answer <ask> [--decline] [--value]`, `decline`;
  - `identities`, `sign-in <provider>`, `sign-out <provider>`, `probe <provider>`.
  - `answer` refuses with exit 2 (`approval_mismatch`) when the operation's spec digest no longer matches the approval.

### Answers for W11

- `Token` is a pydantic model: `resource_server: str | None`, `access_token: SecretStr` (read it with `.get_secret_value()`), `token_type = "Bearer"`, `expires_at: datetime | None`, `scopes: list[str]`, `source: "grant" | "keyring" | "file"`, and `live()`. It is not a tuple.
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
- **With no usable keyring, relay keeps sign-ins in a 0600 file in its own config dir** (W14's bug, decided by the coordinator). Under the CLIO fence there is no Secret Service, and `NoKeyringError` used to escape tacc's own keyring store and break `describe`, the Compute tab and `site add-from-catalog`. tacc now uses the one `TokenStore`. This is generic relay behaviour, with no host-specific setting; `describe` names the place so the person can see it. Grant files work as before.

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

**No keyring backend, live on Ares, 2026-11-18** (`PYTHON_KEYRING_BACKEND=keyring.backends.fail.Keyring`, `CLIO_RELAY_KEYRING` unset, config and state in scratch):
- At integration 9f00ef6, `clio-relay-next site add-from-catalog stampede3` exits 1 with `NoKeyringError`, and MCP `describe site=stampede3` (official `fastmcp` client over stdio) is a tool error with the same message.
- With the fix, `add-from-catalog` exits 0. MCP `describe` returns the site report, with Tapis's own refusal (`invalid_config` with no TACC client id, `unreachable` with the stub's) and no keyring error.
- Against the Tapis stub (site `tacc-stub`, port 47181): `status` is signed_out with `stored_in: file`. `sign-in` showed code `GTYURQJE`, which was approved on the stub; it exited 0.
- From fresh processes afterwards: `status` is signed_in, account `w6tester`, `source: file`. The driver's `describe` and MCP `describe` through a new `clio-relay-next serve` both give `signed_in: true` and `credentials: {stored_in: file, path: <config>/credentials.json}`. `asks identities` lists every provider with `stored_in: file`.
- `credentials.json` and its lock are 0600. The access and refresh tokens appear in none of the seven outputs or the stub log.
- Tests at 0f4b5b5 (`CLIO_RELAY_KEYRING=memory`): `tests/asks` plus `tests/relay_tapis`, **129 passed, 0 skipped, 0 failed**. W11's globus, iri and sky modules, with the globus extra, **95 passed, 0 skipped**. ruff, format and pyright clean on the touched paths.

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
  - An input the job deletes from its directory is dropped from the store when the job's final state is recorded, for a cancelled job too, unless another job still links the blob (link count 1, by `find -links 1`). Reason: W9's `secrets.env` is read and deleted by its launcher, and the store's copy outlived it. The rule is general, with no file named in the adapter. The poll reads `inputs.json` to know the names. A job cancelled before it started still holds its copy, and so does the store, unless the input is transient.
  - An input marked `transient` (W1's `InputRef.transient`) is dropped at the job's final state no matter what: the job's copy (`rm -f`) and the store's blob, even if the job kept the file, never started, or another job links the same blob (that job's hard link keeps its own data). `inputs.json` carries the flag, so a restarted relay drops it too. Reason: coordinator decision, so serve's secrets never outlive a job cancelled before `launch.sh` runs.
- With no scheduler (`scheduler: none` or none detected), a small `sh` supervisor is started detached with `setsid` (`nohup` where there is no `setsid`). It records the pid, the child's pid and the exit code in the job directory, so a later poll from a new relay reads the result (LQ8). The native id is `pid:<n>`.
- Every `ssh` and `scp` uses `BatchMode=yes`. A site that asks for a code refuses with `interactive_auth_required`, naming `worker` as the adapter to use, unless `interactive_sign_in` is set. Then `sign_in_needed()` returns `SignInNeed(provider="ssh")` and registers a starter with W6's `askpass.register_sign_in`; the starter brings up a ControlMaster (`ControlMaster=yes`, no `-M`) with `askpass.environment(site=..., operation_id=..., title="Sign in to <site>")` merged into its environment, and every later call reuses it with `BatchMode=yes`. The fallback from before W6 merged is gone (integration `87da62d`). An `askpass:` program in the config replaces `SSH_ASKPASS` and keeps W6's other variables; with one, `sign_in_needed()` first tries the starter itself and asks the person only if that fails.
  - No master is left behind (W8's stale masters on fx-otp). `ssh -N -f` puts the master in its own session, so it used to outlive relay. After it starts, a detached `sh` watcher checks every 5 s that the relay process that started it and the master are both alive, and sends the master TERM once relay has gone (ssh removes its socket on TERM). A restarted relay therefore asks for a sign-in again. The starter also returns at once when a master is already up, under a lock, and removes a dead master's socket first: before, a second start, or one over a stale socket, printed "ControlSocket ... already exists, disabling multiplexing" and left a plain `-N` connection open with nothing to close it.
- Cancel signals: `cancel` runs PSI/J's command (`scancel -Q`, `qdel`). On Ares only the batch shell gets `SIGTERM`, and the job's other processes get `SIGKILL` after `KillWait`, so a trap in the command never ran (W9's finding in LQ-L11). The rendered Slurm script now starts PSI/J's launcher in its own process group (`set -m`), traps TERM, INT, USR1 and USR2, and passes them on to that group, then waits and keeps PSI/J's `.ec` line. A signal that arrives before the launcher starts is held and sent once it has. Where Slurm signals every process itself (fx-slurm), the launcher has already died a second later, so the batch shell does not send it again. PBS scripts are unchanged: `qdel` signals every process of the job.
  - After a TERM or INT, the batch shell waits until the command's whole process group has gone (`kill -0 -- -<group>`), not just PSI/J's launcher (W9's patch, from job 27808). PSI/J's `single_launch.sh` dies on the forwarded TERM, so the batch shell used to exit at once and Slurm then killed the cgroup with the command's trap half done. The wait follows any TERM or INT the batch shell gets, not only one it passed on: where Slurm signals every process (fx-slurm), the launcher is already dead, nothing is passed on, and W9's version skipped the wait (fx-slurm 456 and 462). USR1 and USR2 are passed on without the wait, since no kill follows them and the job would otherwise stay held until its walltime.
  - The launch is wrapped whole even when it spans several lines (W13's R10 finding, Slurm job 27812). A multi-line `bash -c` script used to get ` &` and the wait loop inside its quotes, so it ran in the foreground with no forwarding. `_command_end` (W13's patch) extends the launch line by line until `shlex.split` sees every quote closed and no trailing backslash continues it. The forwarding lives in `adapters/ssh/signals.py`, with its tests in `test_ssh_signals.py`, so `scripts.py` and `test_ssh_units.py` stay under 500 lines.
  - Because the batch shell now outlives the signal, PSI/J's `.ec` line runs and records 143 for a stopped job. So the trap also writes TERM or INT into the job's `signalled` file. For a job the queue has forgotten, a `signalled` record sends the adapter to accounting as if there were no exit code: `sacct` says `CANCELLED`, `TIMEOUT` or `PREEMPTED`. With no accounting, a nonzero exit code is `failed` with a message naming the signal and the three possible causes. A cancel through relay is still read from relay's own `cancel` record first.
- NFS: every check of a file another host may have written lists its directory first (`fresh`, an `ls -f`), because an NFS client keeps a negative lookup and stale sizes for its attribute cache's lifetime. The job directory and its parent, the upload directory, CAS shard directories, the submit wait loop, the status and cancel scripts, and the files verb's `read`, `stat`, `list`, `stage` and `has` all do it; `stat` also opens a regular file so its size is current. exec needs none (its watchdog uses the ssh session's standard input), and the detached supervisor runs on one host.
- The PSI/J launcher install no longer leaves a nested copy behind when two first submits race (`mv` into an existing directory).
- serve: the adapter declares it and calls `clio_relay.serve.start_for` lazily, and exposes `ssh_argv()` for W9's `ssh -L`.
- No time limits on jobs or transfers. Only the connection setup has one (`connect_timeout_s`, default 30 s), which refuses as `unreachable`.
- No walltime of relay's own (coordinator decision after the gate). When the spec gives no `duration`, the adapter passes `None` to PSI/J, which then writes no `--time` or `walltime` line, and the scheduler's partition or queue default applies. A site default walltime is the existing `defaults.duration` in the site entry (and so in the catalog schema, since `CatalogEntry` extends `SiteEntry`); the service fills it in before pricing and before the record, so the charge and the script agree. W1 made `JobAttributes.duration` optional with no default (integration `fff2a35`), so the adapter takes the spec's duration as it is.
- The spend price is `SshAdapter.price(spec) -> Charge`, in node-hours (RL25), the same shape as W11's `SkyAdapter.price`. With a walltime, `walltime_s`, `node_hours` and `amount` are nodes times walltime, with queue and account from the spec. With none, the charge has no amount and no node-hours (coordinator decision), so the spend gate asks unless the site's `spend.ask` is `never`. Request to W13: call it from `GuardHook.price` for `ssh` sites.
- exec follows W1's interface (integration `14e8edd`): `timeout_s` is optional, `on_stall` gets quiet-time reports on W1's schedule (30, 60, 120, 240 s, then at most every 600 s). The command runs on the site in its own session under a small watchdog that holds the ssh session's standard input. When relay's `ssh` ends, times out, is cancelled or drops, standard input closes and the watchdog stops the command's process group, so exec needs no remote guard time.
- A job the scheduler has forgotten (Slurm drops finished jobs after `MinJobAge`, 300 s on fx-slurm and on Ares) is read from its own record: PSI/J's `<native id>.ec` in the job directory. With no exit code, Slurm accounting (`sacct -X -n -P -j <id> -o State,ExitCode`) is asked, only in that case. With neither, `status` reports the job open (`running`, or `queued` if that was the last record) with `native_state: unknown` and a message, never `failed`. `JobState` gets no unknown value (coordinator decision). PBS keeps finished jobs through `qstat -x` for `job_history_duration` (24 h on fx-pbs); after that it falls to the same record. `find` never consults the queue, so a purged job is never submitted again.

### Facts

- psij-python 0.9.11 (the shared venv's pin):
  - `Job` has no `name`, which its templates read; the adapter sets it from the operation.
  - The pbspro template writes `#PBS -N="name"`, which `qsub` refuses; the adapter rewrites the line to `#PBS -N name`.
  - `JobSpec` paths and the launch command are made absolute against the local machine; the adapter sets the site's paths after building the spec.
  - With no `duration`, W1's `JobAttributes` used to fill in 10 minutes, which the record round trip then marked as given. W1's `fff2a35` made it optional with no default, so a job without one is sent with no walltime through the service too.
- OpenSSH: `-M` together with `-o ControlMaster=yes` is "ask" mode, which confirms every shared session through askpass ("Master refused session request" under BatchMode). Use `ControlMaster=yes` alone. This matters to W6's sign-in page.
- Ares, from the [GRC Ares user guide](https://grc.iit.edu/docs/ares/ares-user-guide/) and live `sinfo`: partitions `compute` (default, 22 nodes, 2 days), `debug` (6 nodes, 4 days) and `datacrumbs`; 40 threads and about 47 GB per node; Ubuntu 22.04.5; `module` (Lmod 8.7.25) works only in a login shell, so the skill says to wrap commands in `bash -lc`.
- Ares storage: on the login node `/mnt/common` is local xfs (it is the NFS server); compute nodes mount `172.25.1.1:/mnt/common` over NFSv3 with `acregmin`, `acregmax`, `acdirmin` and `acdirmax` all 120. The adapter's own scripts run on the login node, so on Ares only the job side sees the cache; a site whose login node is an NFS client, or with several login nodes behind one name, would see it in the adapter too.
- Ares Slurm: `KillWait=30`, `proctrack/cgroup`. A plain `sbatch` with a trap in the batch script itself gets TERM from `scancel`; a trap in a child of the batch shell does not, with `scancel`, `--full --signal=TERM` or `--batch --signal=TERM`. fx-slurm (`KillWait=10`, `proctrack/linuxproc`) and fx-pbs signal every process.

### Evidence

All runs on Ares from `~/clio-relay-rework/clio-relay-w7` with its own venv, against W2's fixtures at 127.0.0.1:47100 to 47107 (`fixtures.env` sourced with `set -a`).

- `uv run pytest tests/relay_ssh` at `3f31cf1`: `53 passed in 150.49s`. Again after merging the integration branch at `4b72bf2`, at `164a2f6` with `tests/relay_next/test_serve.py`: `61 passed in 153.34s`. `ruff check`, `ruff format --check` and strict `pyright` are clean on the touched files.
- LQ4 (`ssh`): `test_ssh_conformance.py` subclasses W1's `AdapterConformance` three times, on fx-ssh (detached), fx-slurm and fx-pbs: 30 of 30 passed.
- LQ5: `test_lq5_a_slurm_job_completes_after_relay_restarts`. A 15 s Slurm job is submitted, the adapter is dropped with no ssh process left behind, and a new adapter with a fresh state directory finds the job by operation id, waits for `completed`, reads `lq5-done`, and a second `run` of the operation returns the same Slurm id.
- LQ6: `test_lq6_the_pbs_script_and_status_come_from_psij`. On OpenPBS the job's `submit.sh` carries `#PBS -N relay-` and PSI/J's `single_launch.sh`, the job completes from PSI/J's status parsing, and stdout is `lq6 <PBS job id>`.
- LQ7: `test_lq7_a_rerun_stages_nothing`. A 200 MiB input is staged once (its blob is in the first upload) and a second job with it uploads no blob; every later upload is under 1 MiB. `sha256sum` on the site matches the digest both times.
- LQ8: `test_lq8_a_detached_job_is_read_by_a_later_poll`. On fx-ssh with no scheduler, a job is started as `pid:<n>`, relay drops it, and a new adapter finds the same handle, reads `failed` with exit code 7 and separate stdout and stderr.
- LQ9, re-proved with W6's askpass at integration `87da62d` (W7 at `829d348`), `pytest tests/relay_ssh/test_ssh_fixtures.py -k lq9`: 4 passed, none skipped.
  - `test_lq9_a_code_site_refuses_and_names_worker`: without `interactive_sign_in`, describe and run on fx-otp refuse with `interactive_auth_required` and `adapter: worker`. It no longer has a `[True]` case that skipped whenever askpass was installed (W1's open item).
  - `test_lq9_with_interactive_sign_in_the_code_site_asks_for_a_sign_in`: `sign_in_needed()` gives `SignInNeed(provider="ssh")`, a starter is registered with `askpass`, and describe shows the site as needing sign-in.
  - `test_lq9_the_code_site_gets_in_through_relays_askpass`: a listener on W6's broker plays the person on relay's page and answers `Verification code:` with the TOTP. The registered starter runs with `askpass.environment(site=...)`, as the page's "Start sign-in" does, so the real `clio-relay-askpass` helper carries the prompt. Describe, run and files then work over the master, and the person is asked once.
  - `test_lq9_an_askpass_program_signs_in_without_a_person`: with `askpass:` set to a TOTP program, `sign_in_needed()` signs in by itself and returns nothing to ask; the program saw one prompt.
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
- NFS, on Ares compute node `ares-comp-04` with the adapter's own scripts run there while the login node wrote the files: a plain `[ -f ]` on the job's `.ec` and on an output stayed false after the write, while `files.read` returned `b'result\n'`, `files.stat` gave size 7 and the status script read the exit code `0`. A plain `[ -f ]` on the `.ec` was still false afterwards. `test_checks_of_remote_files_list_their_directory_first` keeps the order.
- Cancel signals, live with the adapter's `run` and `cancel`, a job whose command traps TERM and INT, in two shapes (a foreground `sleep` and `sleep & wait`):
  - Before the change, Ares jobs 27757 and 27758: the trap never ran; the jobs were final 4 to 8 s after the cancel.
  - After it, at `829d348`: Ares 27771 and 27772 on `ares-comp-10` log `stopping: signal TERM` 2 s after the cancel; fx-slurm 211 and 212 and fx-pbs `123.fx-pbs` and `124.fx-pbs` log it once, at 0 s. All four end `cancelled`.
    - Again at `6023709`, after the `signalled` record: Ares 27784 and 27785 (2 s), fx-slurm 273 and 274 and fx-pbs `165.fx-pbs` and `166.fx-pbs` (0 s); each trap ran once and every job ended `cancelled`.
    - Units: `test_a_signal_to_the_batch_shell_reaches_the_command` (only the batch shell is signalled; the grandchild's trap runs and `.ec` reads 143) and `test_the_batch_shell_keeps_the_commands_exit_code` (7).
  - A trap that takes time, at `c549780`: the command is `sleep 3600 & wait $!` with a TERM trap that logs `stopping`, sleeps 3 s, logs `stopped` and exits 143. Ares 27815 logs `stopping` 2 s after the cancel and `stopped` at 5 s; fx-slurm 413 at 0 s and 3 s; fx-pbs `246.fx-pbs` at 1 s and 4 s. All three end `cancelled`. Before the change, W9's Ares job 27808 left no `stopped.json`. Unit: `test_the_batch_shell_waits_for_a_trap_the_launcher_does_not` (the launcher exits on TERM while the command's trap is still sleeping; the trap's log line is written and the batch shell goes on), which fails on `c624d82`.
  - A forgotten job whose batch shell wrote `.ec` 143 after a signal still asks `sacct`, so the forgotten-job test above keeps reading `cancelled`. Units: `test_a_forgotten_job_the_scheduler_stopped_asks_accounting[CANCELLED|TIMEOUT|none]`.
- At `6023709` (on integration `87da62d`): `pytest tests/relay_ssh`, `87 passed in 479.31s`, none skipped. `ruff check`, `ruff format` and `pyright` clean.
- Deleted inputs leave the store (W9's `secrets.env`), at `540decd`:
  - Units in `test_ssh_store.py`: a final record drops the blob of an input the job deleted, keeps one it kept, one it replaced, and one another job still links; `input_entries` gives nothing for unreadable `inputs.json`.
  - `test_an_input_the_job_deletes_leaves_no_copy_in_the_store[fx-slurm|fx-pbs|fx-ssh]`: a job sources `serve/secrets.env`, deletes it and ends; its blob is gone, `kept.txt`'s blob and the job's link stay. A second job deletes its copy and sleeps: the blob stays while it runs and is gone once it is `cancelled`.
  - Live on Ares Slurm with `remote_dir: clio-relay-rework/w7-scratch/blob-remote`: job 27803 `completed`, stdout the token, secret blob gone, kept blob present; job 27804 deleted its copy, the blob stayed while it ran, then `cancelled` and the blob was gone, the kept one present.
  - The store code is `adapters/ssh/store.py` and its tests have their own modules, so `scripts.py`, `test_ssh_units.py` and `test_ssh_fixtures.py` stay under 500 lines. `check_file_size.py`: OK, with src 79.44% and tests 70.07% under 500 lines.
  - `pytest tests/relay_ssh`, `93 passed in 550.07s`, none skipped. `ruff check`, `ruff format` and `pyright` clean.
- Transient inputs, run on a scratch merge of W1's branch at `fe25112` (which has `InputRef.transient`):
  - `test_a_transient_input_is_dropped_however_the_job_ends[fx-slurm|fx-pbs|fx-ssh]`: a job that sources and keeps its transient `serve/secrets.env` and completes, and a job cancelled at once; in both the job's copy and the blob are gone, and `kept.txt`'s blob stays. Unit: a transient input another job links is still dropped.
  - Live on Ares Slurm, a serve-shaped job (`sh serve/launch.sh`, transient `serve/secrets.env`) held with `slurm.begin: now+1hour`: `op_2f161be78f21ebeee739b972`, job 27817, `squeue` `PENDING BeginTime`. Cancelled, it ended `cancelled` 2 s later; `serve/launch.log` was never written, the job's `secrets.env` and its blob were gone, and `launch.sh`'s blob stayed.
  - At `960a455` on integration `bf45eb9`, `store.entry` reads `InputRef.transient` itself. Unit: `test_inputs_json_carries_the_transient_mark`.
- A launch over several lines (W13's R10 finding, Slurm job 27812), and the wait where Slurm signals every process, at `ae1a6ee`:
  - Units in `test_ssh_signals.py`: `test_a_launch_over_several_lines_is_wrapped_whole` renders a multi-line `bash -c`, a backslash continuation and a quoted newline through `_render`, and checks the whole launch sits between `set -m` and ` &`; `test_a_multi_line_command_gets_the_signal` runs one and signals only the batch shell; `test_forward_signals_needs_the_launch_to_end`. With `_command_end` taken out, the render tests fail and the run test hangs.
  - `test_the_batch_shell_waits_when_the_scheduler_signals_every_process`: the launcher's group gets TERM first, then the batch shell; the trap's line, the shell's last line and `signalled` are all written. It and `test_the_batch_shell_waits_for_a_trap_the_launcher_does_not` fail with the group wait taken out.
  - Live, a 6-line runner (`set -eu`, a TERM trap that logs `stopping`, sleeps 3 s, logs `stopped` and exits 143, then `sleep 3600 & wait $!`), cancelled through the adapter. With the multi-line wrap only: Ares 27821 logs `stopping` at 2 s and `stopped` at 5 s, `signalled` TERM, `cancelled` 143; fx-slurm 456 and 462 log only `stopping`. With the wait after any TERM or INT: Ares 27822 at 1 s and 4 s, fx-slurm 469 at 0 s and 3 s, both `signalled` TERM and `cancelled` 143; fx-pbs `277.fx-pbs` at 0 s and 3 s, `cancelled` (PBS scripts are not wrapped). Each Slurm script holds all 6 lines of the launch.
- Stale masters on fx-otp (W8's report): at the report, Ares had no ssh master of mine left and fx-otp had no session besides its listener, so there was nothing to clean. Shown by hand on fx-otp: a second `ssh -N -f -o ControlMaster=yes` on a live socket printed "disabling multiplexing" and left 2 processes; a TERM to the master removed its socket. `test_ssh_master.py`, with the change: `test_signing_in_again_reuses_the_master` (two starts at once keep the same master pid, one process on the control path, one prompt), `test_a_stale_socket_does_not_block_a_sign_in` (after a SIGKILL leaves the socket, a sign-in starts one new master), and `test_the_master_ends_with_the_relay_that_started_it` (a relay subprocess signs in and exits; its master goes, with its socket). All 3 pass, with LQ9's 4, and no fx master or watcher is left afterwards. On the old transport the first two fail and one master is left behind.

## W8. The worker adapter and the old surface's removal

Branch `jaimecernuda-relay-rework-w8`, from `jaimecernuda-relay-rework` at 719104c (M1).

### Inventory method

- Source: the integration branch at 719104c. The import graph covers every `clio_relay.*` module in `src/`, from `import` statements and from module names in strings (monkeypatch targets, `python -m`, lazy imports).
- What stays is what the roots reach without passing through a deleted family. The roots are:
  - the new tree (`adapters`, `catalog`, `jobs`, `sites`, `surface`, `testing`, `cli_next`);
  - W9's files (`service_runtime*`, `jarvis_service_runtime*`, `models_gateway`, `frp_*`);
  - the worker's own roots (endpoint, HTTP API, owned session, bootstrap, queue, one-link connection, remote CLI) and the old `cli_*` modules that become the worker's operator commands.
- The deleted families are `mcp_*`, `fastmcp_server`, `browser_gateway*`, `live_acceptance*`, `live_validation*`, `scheduler_providers/`, `remote_mcp*`, `cli_remote_mcp*`, `_mcp_call_runner_facade` and `session_runtime`, plus the old `cli` root, which W1 owns and replaces at the cutover.
- Callers outside this repository were searched in clio-agent (develop f08dbde3a, then rechecked at its integration branch 4878fcc40), gact-tui (21163590c), clio-kit (main 490269fb1) and clio-agent-marketplace (develop).

### The old tools

There are 49 tools, not 45: 43 static `relay_*` tools in four catalog modules, and 6 virtual JARVIS tools. The admin, operator and all profiles list every tool. The user profile lists 14 (`USER_MCP_TOOL_NAMES`, `mcp_tool_catalog.py:57`). `relay_action` is a payload field, not a tool.

The in-repo columns count files that name the tool. Every one of those `src/` files is in the deletion list, except `scripts/check_file_size.py`, which lists file names.

| Tool | Catalog module | src files | test files | Docs and scripts that name it |
| --- | --- | --- | --- | --- |
| `relay_remote_mcp_context` | job_lifecycle | 3 | 2 | operations.md, ai/release-validation-0.9.22.md |
| `relay_submit_agent` | job_lifecycle | 5 | 5 | operations.md, mcp-tasks.md, ai/release-validation-0.9.22.md |
| `relay_status` | job_lifecycle | 5 | 5 | operations.md, ai/release-validation-0.9.22.md, check_file_size.py |
| `relay_cancel` | job_lifecycle | 6 | 3 | operations.md, ai/release-validation-0.9.22.md, check_file_size.py |
| `relay_observe` | job_lifecycle | 7 | 3 | operations.md, remote-mcp-federation.md, ai/release-validation-0.9.22.md, check_file_size.py |
| `relay_wait` | job_lifecycle | 11 | 5 | operations.md, remote-mcp-federation.md, design/relay-architecture-2026-08.md, ai/release-validation-0.9.22.md, check_file_size.py |
| `relay_submit_jarvis_pipeline` | job_lifecycle | 3 | 2 | none |
| `relay_submit_jarvis_job` | job_lifecycle | 2 | 1 | none |
| `relay_submit_remote_agent` | job_lifecycle | 4 | 2 | mcp-tasks.md |
| `relay_submit_mcp_call` | job_lifecycle | 3 | 2 | remote-mcp-federation.md |
| `relay_call_jarvis_mcp` | job_lifecycle | 4 | 1 | none |
| `relay_get_job` | monitoring | 2 | 3 | operations.md |
| `relay_get_job_status` | monitoring | 2 | 2 | operations.md |
| `relay_monitor_job` | monitoring | 2 | 2 | none |
| `relay_watch_job_events` | monitoring | 2 | 1 | none |
| `relay_list_tasks` | monitoring | 2 | 2 | none |
| `relay_record_task_event` | monitoring | 2 | 2 | README.md |
| `relay_watch_task_events` | monitoring | 2 | 2 | README.md |
| `relay_read_job_log` | monitoring | 2 | 1 | none |
| `relay_list_artifacts` | monitoring | 6 | 6 | check_file_size.py |
| `relay_artifact_lineage` | monitoring | 4 | 5 | README.md, operations.md, ai/interface-context.md, check_file_size.py |
| `relay_record_progress` | monitoring | 2 | 1 | none |
| `relay_list_progress` | monitoring | 2 | 2 | none |
| `relay_read_artifact` | monitoring | 5 | 5 | design/relay-architecture-2026-08.md, check_file_size.py |
| `relay_cancel_job` | monitoring | 2 | 1 | operations.md |
| `relay_queue_list` | queue_retention | 3 | 2 | operations.md |
| `relay_queue_diagnose` | queue_retention | 3 | 1 | operations.md |
| `relay_queue_stale` | queue_retention | 3 | 1 | operations.md |
| `relay_queue_cleanup_stale` | queue_retention | 2 | 1 | operations.md |
| `relay_retention_plan` | queue_retention | 2 | 0 | none |
| `relay_retention_status` | queue_retention | 2 | 0 | none |
| `relay_retention_collect` | queue_retention | 2 | 1 | none |
| `relay_worker_status` | queue_retention | 2 | 1 | none |
| `relay_create_monitor_rule` | queue_retention | 2 | 2 | none |
| `relay_list_monitor_rules` | queue_retention | 2 | 1 | none |
| `relay_evaluate_monitor_rules` | queue_retention | 2 | 1 | none |
| `relay_create_gateway_session` | gateway_session | 2 | 1 | none |
| `relay_list_gateway_sessions` | gateway_session | 2 | 1 | none |
| `relay_bind_jarvis_runtime` | gateway_session | 9 | 3 | README.md, operations.md, connection-model.md, one-link-control-plane.md, ai/interface-context.md, ai/system-context.md |
| `relay_storage_status` | gateway_session | 3 | 2 | operations.md |
| `relay_get_gateway_session` | gateway_session | 2 | 2 | none |
| `relay_update_gateway_session` | gateway_session | 2 | 3 | none |
| `relay_close_gateway_session` | gateway_session | 2 | 3 | none |
| `jarvis_describe`, `jarvis_create_pipeline`, `jarvis_add_step`, `jarvis_edit_step`, `jarvis_run`, `jarvis_get_execution` | virtual, from `_contracts/jarvis-user-v3.7.2.json` (`jarvis_mcp.py:80`) | 0 by name | 0 by name | none |

Callers in other repositories:

- **clio-agent (no production tool calls).** Read at the integration branch `jaimecernuda-relay-rework` (4878fcc40). `scripts/check_task_replacement_removal.py:62` names the module `clio_agent.gact.relay_status`, not the tool. `tests/test_tools/test_mcp_dual_emission_consume.py` and `tools/gateway.py` no longer name a relay tool or the relay_ops namespace (they did on develop f08dbde3a). The other `relay_*` names appear only in CHANGELOG and design docs.
- **clio-agent no longer calls the old command line.** On develop f08dbde3a, `src/clio_agent/gact/infrastructure/drivers.py:356-445` ran `clio-relay cluster ...` with `--scheduler-provider`. W4's RL38 commit 6801be185 removed that code, and it is on clio-agent's integration branch `jaimecernuda-relay-rework` (4878fcc40). W3 checked: nothing there calls `clio-relay cluster` or `--scheduler-provider`. Later clio-agent and marketplace checks read that integration branch, not develop.
- **gact-tui:** `desktop/src-tauri/src/ssh_transport_command.rs:456` names `clio-relay` in a test only. The relay references in gact-tui's UI are W12's open item.
- **clio-kit:** `jarvis_handler.py:2940` keeps compatibility aliases for clio-relay 0.x and calls no tool.
- **clio-agent-marketplace:** no caller.

### The deletion list

Checked against W9's ownership before running: no `service_runtime*`, `jarvis_service_runtime*`, `models_gateway` or `frp_*` module is in the deletion list or the move list. The script asserts this before it deletes anything.

**`src/clio_relay/`: 161 modules.** `scheduler_providers/` is deleted as a package. `cli.py` is listed because nothing kept imports it. W1 owns it and replaces it at the cutover; W8 does not edit it.

- `_mcp_call_runner_facade`, `mcp_agent_input_guard`, `mcp_arguments`, `mcp_call_result_error`, `mcp_dispatch`, `mcp_gateway_tools`, `mcp_job_lifecycle`, `mcp_job_status`, `mcp_queue_tools`, `mcp_remote_catalog`, `mcp_remote_transport`, `mcp_result_verification`, `mcp_server`, `mcp_session_state_codec`, `mcp_stdio_validation`, `mcp_stdio_validation_contract`, `mcp_stdio_validation_executable`, `mcp_stdio_validation_jarvis_contract`, `mcp_stdio_validation_process`, `mcp_stdio_validation_process_io`, `mcp_stdio_validation_support`, `mcp_submission_agent`, `mcp_submission_mcp_call`, `mcp_submission_result`, `mcp_task_projection`, `mcp_task_runtime`, `mcp_tasks_extension`, `mcp_tool_catalog`, `mcp_tool_catalog_gateway_session`, `mcp_tool_catalog_job_lifecycle`, `mcp_tool_catalog_monitoring`, `mcp_tool_catalog_queue_retention`, `mcp_tool_provider`.
- `artifact_routing`.
- `bounded_file_io`.
- `browser_gateway`, `browser_gateway_errors`.
- `cli`, `cli_agent`, `cli_diagnostics`, `cli_gateway`, `cli_gateway_runtime`, `cli_jarvis_artifact_io`, `cli_jarvis_dispatch`, `cli_jarvis_execution_run`, `cli_jarvis_execution_types`, `cli_jarvis_intent_checkpoint`, `cli_jarvis_mcp`, `cli_jarvis_mcp_validate`, `cli_jarvis_package_search`, `cli_jarvis_pending_report`, `cli_jarvis_query_observation`, `cli_jarvis_remote_contract`, `cli_jarvis_resume_checkpoint`, `cli_job`, `cli_job_records`, `cli_monitor`, `cli_release`, `cli_remote_mcp`, `cli_remote_mcp_validate`, `cli_storage`.
- `clio_kit_runtime_identity`, `clio_kit_wheel_archive`.
- `constants`.
- `doctor`.
- `fastmcp_server`.
- `input_staging`.
- `jarvis_artifact_documents`, `jarvis_cd_lock_binding`, `jarvis_execution_query`, `jarvis_input_plane`, `jarvis_mcp_validation`, `jarvis_mcp_validation_contract`, `jarvis_mcp_validation_core`, `jarvis_mcp_validation_execution_query`, `jarvis_mcp_validation_lifecycle_progress`, `jarvis_mcp_validation_live_progress`, `jarvis_mcp_validation_package_search`, `jarvis_mcp_validation_progress_semantics`, `jarvis_mcp_validation_report`, `jarvis_mcp_validation_report_support`, `jarvis_native_execution_documents`.
- `live_acceptance`, `live_acceptance_agent_prompt`, `live_acceptance_browser_evidence`, `live_acceptance_browser_http`, `live_acceptance_checkpoint`, `live_acceptance_handoff`, `live_acceptance_job_verification`, `live_acceptance_models`, `live_acceptance_packaged_mcp`, `live_acceptance_progress`, `live_acceptance_remote_io`, `live_acceptance_secret_redaction`, `live_acceptance_secure_runtime`, `live_acceptance_transport`, `live_acceptance_wait`, `live_validation_capacity`, `live_validation_cleanup`, `live_validation_constants`, `live_validation_jobs`, `live_validation_orchestrator`, `live_validation_process`, `live_validation_support`.
- `observation`.
- `params_and_manifest`.
- `process_environment`.
- `progress_bridge`.
- `promotion_record`.
- `protocol_messages`.
- `pytest_platform_partition`, `pytest_release_gate`.
- `python_console_distribution`, `python_external_distribution`.
- `release_check_runtime`, `release_command_stream`, `release_pin_sites`, `release_pin_sites_jarvis_tests`, `release_pin_sites_kit`, `release_pin_validation`, `release_pins`, `release_validation`.
- `remote_mcp`, `remote_mcp_acceptance_evidence`, `remote_mcp_acceptance_models`, `remote_mcp_acceptance_report`, `remote_mcp_admission`, `remote_mcp_aliasing`, `remote_mcp_cache`, `remote_mcp_catalog_build`, `remote_mcp_catalog_models`, `remote_mcp_contract_checks`, `remote_mcp_schema_validation`, `remote_mcp_schema_wrapping`, `remote_mcp_scientific_catalog_result`, `remote_mcp_spack_result_validation`, `remote_mcp_spack_transition_checks`, `remote_mcp_spack_transition_report`, `remote_mcp_stdio_evidence`, `remote_mcp_structured_result`, `remote_mcp_tool_schema`, `remote_mcp_validation`, `remote_mcp_wire_schemas`.
- `result_document`.
- `safe_archive`.
- `scheduler_providers`.
- `server_artifact_identity`.
- `session_runtime`.
- `stdio_io`.
- `wheel_private_launch`, `wheel_snapshot_identity`.

**`jarvis-packages/clio_relay/clio_relay/`:** `mcp_call/` (3 files), and the 22 modules that only it uses: `_mcp_call_runner_facade`, `bounded_file_io`, `bounded_payload`, `clio_kit_runtime_identity`, `clio_kit_wheel_archive`, `constants`, `jarvis_artifact_documents`, `jarvis_cd_lock_binding`, `jarvis_execution_query`, `jarvis_native_execution_documents`, `params_and_manifest`, `process_environment`, `progress_bridge`, `protocol_messages`, `python_console_distribution`, `python_external_distribution`, `result_document`, `server_artifact_identity`, `session_runtime`, `stdio_io`, `wheel_private_launch` and `wheel_snapshot_identity`. Kept: `__init__`, `_jarvis_api`, `bounded_command/`, `remote_agent/` and the 15 `process_containment*` modules, which are all `bounded_command` and `remote_agent` import.

**Tests: 40 files.**

`test_browser_gateway.py`, `test_builtin_jarvis_input_staging.py`, `test_cli_agent.py`, `test_cli_diagnostics.py`, `test_cli_gateway.py`, `test_cli_gateway_runtime.py`, `test_cli_jarvis_mcp.py`, `test_cli_jarvis_mcp_validate.py`, `test_cli_job.py`, `test_cli_job_records.py`, `test_cli_monitor.py`, `test_cli_release.py`, `test_cli_remote_mcp.py`, `test_clio_schemas_convergence.py`, `test_fastmcp_server.py`, `test_input_staging.py`, `test_jarvis_mcp_validation.py`, `test_live_acceptance.py`, `test_mcp_call_owner_modules.py`, `test_mcp_call_result_error.py`, `test_mcp_call_runner.py`, `test_mcp_call_runner_lifecycle.py`, `test_mcp_input_staging.py`, `test_mcp_server.py`, `test_mcp_stdio_validation.py`, `test_promotion_record.py`, `test_release_acceptance_runbook.py`, `test_release_command_stream.py`, `test_release_pins.py`, `test_release_validation.py`, `test_remote_mcp.py`, `test_remote_mcp_acceptance_models.py`, `test_remote_mcp_aliasing.py`, `test_remote_mcp_cache.py`, `test_remote_mcp_schema_validation.py`, `test_remote_mcp_schema_wrapping.py`, `test_remote_mcp_tool_schema.py`, `test_remote_mcp_wire_schemas.py`, `test_safe_archive.py`, `test_schema_equality.py`.

**Docs:** `docs/remote-mcp-federation.md` and `docs/connection-model.md`. Links to them in the user docs are updated.

**clio-schemas fixtures:** `tests/fixtures/clio_schemas_v0_2_0_projections.json`, `relay_schema_equality_v1.json`, `relay_state_map_v1.json` and `tests/generate_schema_equality_goldens.py`, with the two tests above that read them.

**Kept tests that need edits: 57 files.** Each edit removes the cases for a deleted module and points the rest at the moved paths.

| File | Deleted modules it names |
| --- | --- |
| `test_acceptance_report_defaults.py` | cli, cli_jarvis_artifact_io, cli_jarvis_dispatch, cli_jarvis_execution_run, cli_jarvis_package_search, cli_jarvis_remote_contract, jarvis_mcp_validation, live_acceptance, mcp_server, mcp_stdio_validation, release_validation, remote_mcp, scheduler_providers |
| `test_artifact_cluster_routing.py` | artifact_routing, mcp_server |
| `test_artifact_lineage.py` | cli, mcp_server |
| `test_artifact_listing_by_execution_id.py` | cli, mcp_server |
| `test_bootstrap_cluster_paths.py` | cli |
| `test_bootstrap_fast_path.py` | cli, safe_archive |
| `test_bounded_command_package.py` | input_staging |
| `test_browser_attachment_queue.py` | browser_gateway |
| `test_ci_clio_kit_wheel.py` | remote_mcp |
| `test_cli.py` | cli, cli_jarvis_artifact_io, cli_jarvis_execution_run, cli_jarvis_execution_types, cli_jarvis_intent_checkpoint, cli_jarvis_package_search, cli_jarvis_pending_report, cli_jarvis_query_observation, cli_jarvis_remote_contract, cli_jarvis_resume_checkpoint, mcp_stdio_validation, scheduler_providers |
| `test_cli_api.py` | cli |
| `test_cli_cluster.py` | cli |
| `test_cli_cluster_deploy.py` | cli |
| `test_cli_endpoint.py` | cli, scheduler_providers |
| `test_cli_init.py` | cli |
| `test_cli_installation_receipt.py` | cli |
| `test_cli_patch_seam.py` | cli |
| `test_cli_queue.py` | cli |
| `test_cli_queue_maintenance.py` | cli, scheduler_providers |
| `test_cli_relay_host.py` | cli |
| `test_cli_scheduler.py` | cli, scheduler_providers |
| `test_cli_session.py` | cli |
| `test_cli_session_owned.py` | cli |
| `test_cli_worker.py` | cli |
| `test_clio_kit_mcp_contracts.py` | remote_mcp |
| `test_cluster_config.py` | remote_mcp |
| `test_contract_gate.py` | remote_mcp |
| `test_core_queue_split_architecture.py` | browser_gateway |
| `test_dial_burndown_channel_routing.py` | cli_jarvis_artifact_io, cli_remote_mcp |
| `test_doctor_and_relay_host.py` | doctor |
| `test_door_errors.py` | browser_gateway, fastmcp_server, mcp_server |
| `test_endpoint.py` | remote_mcp |
| `test_endpoint_service_recovery.py` | cli |
| `test_endpoint_worker_lanes.py` | cli |
| `test_execution_watch.py` | remote_mcp |
| `test_gateway_public_projection.py` | cli, mcp_server |
| `test_http_api.py` | remote_mcp |
| `test_identifiers.py` | mcp_server |
| `test_jarvis_execution_artifacts.py` | mcp_server |
| `test_jarvis_execution_recovery_guards.py` | remote_mcp |
| `test_jarvis_lost_response_recovery.py` | remote_mcp |
| `test_jarvis_pipeline_precheck.py` | remote_mcp |
| `test_jarvis_run_failure_reporting.py` | remote_mcp |
| `test_jarvis_service_runtime.py` | browser_gateway, cli, cli_jarvis_mcp, mcp_server, remote_mcp, scheduler_providers |
| `test_production_admin_surfaces.py` | cli, mcp_server |
| `test_remote_value_contract.py` | doctor, live_acceptance |
| `test_scheduler_cancel_attempt_claims.py` | scheduler_providers |
| `test_scheduler_cancel_refusal_idempotency.py` | scheduler_providers |
| `test_scheduler_status.py` | scheduler_providers |
| `test_service_runtime.py` | browser_gateway, cli |
| `test_surface_pagination.py` | cli, mcp_server |
| `test_transform_provenance.py` | cli, input_staging, mcp_server |
| `test_user_profile_content_path.py` | mcp_server |
| `test_validation_report.py` | remote_mcp |
| `test_worker_concurrency.py` | cli |
| `test_worker_lifetime_lock.py` | mcp_server |
| `test_worker_service_policy.py` | cli |

`test_service_runtime.py` and `test_jarvis_service_runtime.py` are W9's to migrate (below).

### What moves into `adapters/worker/`

263 modules move, unchanged apart from imports, to `src/clio_relay/adapters/worker/<same name>.py`. References to `clio_relay.<name>` are rewritten in `src`, `tests` and `scripts` (imports, monkeypatch targets, `python -m` strings). Remote command strings change from `clio-relay <group>` to `clio-relay worker <group>`.

- `application_profiles`.
- `bootstrap`, `bootstrap_acceptance`, `bootstrap_candidate_package_sources`, `bootstrap_candidate_uv_install_source`, `bootstrap_constants`, `bootstrap_frp_local_install`, `bootstrap_full_activation_staging`, `bootstrap_jarvis_staging`, `bootstrap_journal`, `bootstrap_one_pass_script`, `bootstrap_pin`, `bootstrap_pinned_copy_sources`, `bootstrap_preparing_root_source`, `bootstrap_provider_build_info`, `bootstrap_receipt_classifier_source`, `bootstrap_receipt_validation`, `bootstrap_reconcile`, `bootstrap_reconcile_activation_paths`, `bootstrap_reconcile_builtin_repos`, `bootstrap_reconcile_constants`, `bootstrap_reconcile_execution_identity`, `bootstrap_reconcile_generation_staging`, `bootstrap_reconcile_inspection`, `bootstrap_reconcile_jarvis_wrapper_binding`, `bootstrap_reconcile_locks`, `bootstrap_reconcile_models`, `bootstrap_reconcile_planning`, `bootstrap_reconcile_planning_support`, `bootstrap_reconcile_primitives`, `bootstrap_reconcile_readiness`, `bootstrap_reconcile_receipt`, `bootstrap_reconcile_replacement_provider`, `bootstrap_reconcile_repository`, `bootstrap_reconcile_script_activation`, `bootstrap_reconcile_script_commit`, `bootstrap_reconcile_script_generation_prepare`, `bootstrap_reconcile_script_recovery`, `bootstrap_reconcile_transaction`, `bootstrap_recovery`, `bootstrap_script_commit`, `bootstrap_script_jarvis_repo_setup`, `bootstrap_script_jarvis_state`, `bootstrap_script_preamble`, `bootstrap_script_provider_install`, `bootstrap_ssh_deploy`, `bootstrap_staged_provider_source`, `bootstrap_worker_fence_script`, `bootstrap_worker_proof_source`.
- `cli_api`, `cli_cleanup_evidence`, `cli_cleanup_report`, `cli_cluster`, `cli_cluster_deploy`, `cli_endpoint`, `cli_init`, `cli_installation_receipt`, `cli_owned_relay_jobs`, `cli_owned_relay_jobs_remote_listing`, `cli_owned_report_artifact`, `cli_owned_runtime_cleanup`, `cli_owned_scheduler_cancel`, `cli_owned_session_recovery`, `cli_owner_session_teardown_verify`, `cli_queue`, `cli_queue_maintenance`, `cli_relay_host`, `cli_remote_collection_pagination`, `cli_remote_worker_attach`, `cli_remote_worker_probe`, `cli_scheduler`, `cli_session`, `cli_session_attach`, `cli_session_owned`, `cli_session_start`, `cli_session_teardown`, `cli_session_teardown_action`, `cli_session_teardown_finalize`, `cli_session_teardown_jobs`, `cli_session_teardown_recovery`, `cli_session_teardown_report`, `cli_session_teardown_state`, `cli_support`, `cli_transport_validation`, `cli_worker`.
- `cluster_probe`.
- `core_queue`.
- `deployment`, `deployment_activation`, `deployment_ssh`, `deployment_unit`.
- `door_error_adapters`, `door_error_messages`, `door_errors`.
- `endpoint`, `endpoint_execution_cleanup_actions`, `endpoint_execution_lifecycle`, `endpoint_execution_output`, `endpoint_execution_sidecar_cleanup`, `endpoint_jarvis_dispatch`, `endpoint_jarvis_recovery`, `endpoint_jarvis_recovery_bookkeeping`, `endpoint_jarvis_recovery_query`, `endpoint_job_execution`, `endpoint_owner_session_sweep`, `endpoint_progress_ingest`, `endpoint_progress_log_io`, `endpoint_progress_trust`, `endpoint_recovery_directory`, `endpoint_result_finalization`, `endpoint_runtime_metadata_ingest`, `endpoint_runtime_metadata_persist`, `endpoint_runtime_sidecar_anchor`, `endpoint_runtime_sidecar_failure`, `endpoint_scheduler_cancel`, `endpoint_scheduler_cancel_actions`, `endpoint_scheduler_metadata`, `endpoint_scheduler_submission`, `endpoint_scheduler_submission_reconcile`, `endpoint_serve_loop`, `endpoint_service_status`, `endpoint_sidecar_types`, `endpoint_windows_sidecar_handles`, `endpoint_worker_environment`, `endpoint_worker_lanes`.
- `frpc_proxy_bringup`, `frpc_proxy_receipt`, `frpc_proxy_scripts`, `frpc_unit`.
- `http_api`, `http_api_auth`, `http_api_context`, `http_api_error_handlers`, `http_api_middleware`, `http_api_models`, `http_api_owner_session_lease_renewal`, `http_api_queue_paging`, `http_api_redaction`, `http_api_routes_artifacts`, `http_api_routes_events`, `http_api_routes_gateway`, `http_api_routes_jobs`, `http_api_routes_owner_session_admin`, `http_api_routes_queue`, `http_api_routes_scheduler`, `http_api_routes_session`, `http_api_routes_worker_probe`, `http_api_streaming`.
- `jarvis_dispatch_failure`, `jarvis_execution`, `jarvis_execution_artifacts`, `jarvis_pipeline_precheck`, `jarvis_run_environment`.
- `job_terminal_verdict`.
- `owned_session_record`.
- `progress_adapters`.
- `queue_admission_simulation`, `queue_admission_snapshot`, `queue_artifact_lineage`, `queue_artifacts`, `queue_browser_attachments`, `queue_context`, `queue_diagnosis`, `queue_diagnosis_constants`, `queue_endpoints`, `queue_events`, `queue_execution_cleanup`, `queue_execution_cleanup_markers`, `queue_gateway_indexes`, `queue_gateways`, `queue_gc_storage`, `queue_idempotency`, `queue_index_discovery`, `queue_index_migration`, `queue_index_state`, `queue_input_ingest`, `queue_jarvis_inputs`, `queue_job_gc`, `queue_job_gc_protections`, `queue_jobs`, `queue_layout`, `queue_lease_admission`, `queue_lease_capacity_audit`, `queue_lease_capacity_state`, `queue_lease_indexes`, `queue_lease_records`, `queue_lease_recovery`, `queue_leases`, `queue_legacy_audit`, `queue_legacy_output_audit`, `queue_legacy_output_codec`, `queue_legacy_output_migration`, `queue_listing`, `queue_management`, `queue_monitor_rules`, `queue_order_index`, `queue_owner_session_lease`, `queue_owner_session_lifecycle`, `queue_owner_session_records`, `queue_progress`, `queue_scheduler_cancel_claims`, `queue_scheduler_cancel_records`, `queue_scheduler_cancel_state`, `queue_stale_recovery`, `queue_startup`, `queue_store_lock`, `queue_store_read`, `queue_store_write`, `queue_tasks`, `queue_transitions`, `queue_validation`, `queue_worker_capacity`, `queue_worker_status`.
- `remote_channel_dispatch`, `remote_connection`, `remote_connection_lease`, `remote_connection_registry`, `remote_connection_stream_io`.
- `retention`.
- `scheduler_status`, `scheduler_validation`.
- `session_api_readiness`, `session_attach`, `session_cleanup_execution`, `session_cleanup_reporting`, `session_cleanup_targets`, `session_install_identity`, `session_lifecycle`, `session_lifecycle_report`, `session_process_scope`, `session_recovery_attempt_status`, `session_recovery_cleaned_receipt`, `session_recovery_cleanup_receipt`, `session_recovery_inspection`, `session_recovery_lease_projection`, `session_remote_command`, `session_remote_scripts`, `session_start_attempt_validation`, `session_start_execution`, `session_start_execution_core`, `session_start_identity_challenge`, `session_start_promotion`, `session_start_query`, `session_start_wait`, `session_startup_receipt`, `session_transaction`, `session_validation`.
- `storage_managed_queue`, `storage_managed_queue_admission`, `storage_managed_queue_leasing`, `storage_runtime`, `storage_runtime_core`, `storage_runtime_errors`.
- `transport_probe`, `transport_probe_evidence`, `transport_probe_primitives`, `transport_probe_remote_cleanup`, `transport_probe_remote_cleanup_models`, `transport_probe_remote_script`, `transport_probe_session_lifecycle`.
- `worker_concurrency`, `worker_lifetime_lock`, `worker_runtime_verification`.

**RL39 files, moved as they are.** `bootstrap_reconcile_builtin_repos.py` and every module that holds the `managed_builtin_repo_registered` receipt validation move with no change in behaviour. Their new paths are:

- `src/clio_relay/adapters/worker/bootstrap_reconcile_builtin_repos.py`;
- `src/clio_relay/adapters/worker/bootstrap_receipt_validation.py`;
- `src/clio_relay/adapters/worker/bootstrap_reconcile_execution_identity.py`;
- `src/clio_relay/adapters/worker/bootstrap_reconcile_inspection.py`;
- `src/clio_relay/adapters/worker/bootstrap_reconcile_models.py`.

W1 changes them after this branch merges.

**`core_queue` moves** because the execution plan names it. W9 imports it in two places, which need an import-only change (below).

### What stays at the top level

120 modules stay where they are. They are either imported directly by a W9 file, so W9 needs no edit, or reached by both W9 and the worker:

- ``.
- `acceptance_facts`.
- `actions_artifact`.
- `application_runtime_prediction`, `application_verdict`.
- `artifact_identity_verification`.
- `bounded_payload`, `bounded_process`.
- `branch_protection`.
- `candidate_provenance`.
- `ci_run_status`, `ci_validation`.
- `cluster_config`, `cluster_config_io`, `cluster_config_models`, `cluster_config_registry`, `cluster_config_windows_acl`, `cluster_config_windows_guard`, `cluster_config_windows_paths`, `cluster_config_windows_primitives`, `cluster_config_windows_read_verify`.
- `command_evidence`.
- `component_runtime_identity`, `component_verification_remote`.
- `config`.
- `console_stream`.
- `contract_gate`.
- `control_channel`.
- `dev_mode`.
- `distribution_archive`, `distribution_source_identity`.
- `durable_validation_write`.
- `errors`.
- `execution_watch`, `execution_watch_prediction`.
- `filesystem_paths`.
- `identifiers`.
- `installation`, `installation_receipt_models`.
- `jarvis_mcp`, `jarvis_provider`.
- `job_identity`.
- `lock_holder_sidecar`.
- `models`, `models_artifact_provenance`, `models_enums`, `models_jarvis_package`, `models_jarvis_pipeline`, `models_job`, `models_job_specs`, `models_job_telemetry`, `models_mcp_admission`, `models_owner_session_lease`, `models_scheduling`, `models_shared`.
- `native_jarvis_contract`.
- `owner_session_admission`.
- `pagination`.
- `payload_policy`.
- `persistent_uv_tool_probe`.
- `process_ancestry`, `process_containment`, `process_containment_broker`, `process_containment_broker_script`, `process_containment_environment`, `process_containment_popen`, `process_containment_posix`, `process_containment_recorded`, `process_containment_registry`, `process_containment_spawn`, `process_containment_systemd_core`, `process_containment_systemd_query`, `process_containment_systemd_scope`, `process_containment_termination`, `process_containment_types`, `process_containment_windows`.
- `progress_provenance`.
- `provenance_primitives`.
- `public_records`.
- `python_distribution_probe`.
- `redaction`.
- `regular_file_identity`.
- `relay_host`, `relay_ops`.
- `release_assets`, `release_gate_evaluation`, `release_gate_resources`, `release_gate_targets`, `release_identity`.
- `remote_cli`, `remote_values`.
- `runtime_metadata`, `runtime_metadata_coercion`, `runtime_metadata_core_model`, `runtime_metadata_mcp_normalize`, `runtime_metadata_merge`, `runtime_metadata_native_documents`, `runtime_metadata_native_normalize`, `runtime_metadata_native_validators`, `runtime_metadata_sidecar`, `runtime_metadata_types`.
- `session_api`, `session_wire_models`.
- `spack_transition_checks`.
- `spool`.
- `storage_file_io`, `storage_ledger_codec`, `storage_policy`, `storage_policy_types`, `storage_reservation_ledger`, `storage_snapshot`.
- `uv_tool_receipt`.
- `validation_directory_windows`, `validation_limits`, `validation_recorder`, `validation_report`, `validation_report_assets`, `validation_schema`, `validation_writer_lock`.
- `wheel_record_closure`.

### Cuts in kept code

Kept modules that import a deleted one. Each import is removed, or replaced with the new path:

- `endpoint_job_execution` and `job_terminal_verdict` use `mcp_call_result_error`. The `mcp_call` job kind goes, together with `McpCallSpec` and its fields in `models`.
- `endpoint_jarvis_recovery`, `http_api_routes_jobs`, `jarvis_mcp`, `queue_tasks` and `session_api` use `remote_mcp`. The pinned control-query admission they need stays in the worker. The federation parts go.
- `queue_browser_attachments` uses `browser_gateway`, for one record type. The type moves into the queue module.
- `queue_validation` uses seven `live_validation_*` modules. That validation entry point goes.
- 13 modules use `scheduler_providers`: `cli_endpoint`, `cli_owned_scheduler_cancel`, `cli_queue_maintenance`, `cli_scheduler`, `endpoint`, `endpoint_scheduler_cancel_actions`, `endpoint_scheduler_submission`, `endpoint_worker_environment`, `http_api_routes_scheduler`, `http_api_routes_worker_probe`, `jarvis_provider`, `scheduler_status` and `scheduler_validation`. RL18 replaces it with `psij-python`, which is already pinned. The worker submits, polls and cancels through psij's batch executors, with the job name `relay-<operation_id>` so `find` works after a restart.
- 20 old `cli_*` modules import helpers from the old `cli` root. The helpers move into the worker's command line.

The routes that only the old tools used go with them. These are gateway sessions, monitor rules, retention, queue diagnose, stale and cleanup, `/workers`, task events, progress recording and artifact lineage. The worker link keeps health, session identity and status, storage status, quiesce, admission status, worker and target info, job submit, status, get and cancel, logs, artifacts and input ingest.

### LQ32 scope

LQ32 searches clio-relay for `relay_submit`, `mcp_call`, `browser_gateway` and `scheduler_providers`. Today they appear in about 112 files outside the deletion list, mostly tests and the `mcp_call` job kind in the endpoint, queue and models. W8 removes them all from `src`, `tests`, `jarvis-packages`, `scripts`, `.github` and the user docs. The design docs under `docs/design/` and the dated validation notes under `docs/ai/` keep the names, because they record the history. Assumption: LQ32 reads the shipped tree and the user docs, not the design record.

Exception found during the move: `browser_gateway`, `scheduler_providers` and `remote_mcp*` were held, because W9's runtime still imported them. W9 dropped those imports at 5a01766. W8 merged W9's branch at that commit (91bcc3c), so the deletion could go ahead, and deleted the three groups with their tests (ba5ad4b). Nothing in `src`, `tests`, `jarvis-packages` or `scripts` imports them now. The worker's scheduler module is imported as `worker_scheduler`.

Done in the follow-up (353ef89):
- The seven `browser_*` door reasons and `as_browser_gateway_error` are removed. Nothing produced them once the gateway was gone. REASONS goes from 75 to 68, and the HTTP statuses from 61 to 54.
- In the user docs, the sections for the deleted commands are removed: `operations.md` (agent, `mcp-server`, remote JARVIS MCP, remote MCP federation, `live-test`) and `architecture.md` (remote MCP catalogs, the `mcp_call` row).
- Links to the deleted `connection-model.md` and `remote-mcp-federation.md` now point at `docs/adapters/worker.md`, or are dropped.

Left, with the reason:
- The `mcp_call` job kind (`JobKind.MCP_CALL`, `McpCallSpec`) has no producer and no runner on the worker. W9 removed its last use (`jarvis_service_runtime_validation.py:55`) at 5035e8c. W8 removes the kind once that reaches integration, with its test hits.
- The `virtual_*_mcp_call` roles are gone, with the 1.0 release gate (see "The 1.0 release gate" under "Changes made during the move").
- Some history comments in W1's scripts name deleted modules. W8 leaves W1's files alone beyond the approved edits.
- `README.md` (lines 42, 62, 130, 253) and `docs/mcp-tasks.md:127` belong to W1.
- `operations.md` still documents the old operator command paths (`clio-relay cluster ...` and gateway sessions). W8 rewrites it for the `clio-relay worker cluster ...` paths after the cutover (coordinator decision).

### clio-schemas

Kept. This corrects the inventory, which said dropped. `jarvis-packages/clio_relay/clio_relay/remote_agent/runner.py:16` imports `ArtifactKind`, `ArtifactVersion`, `Custody`, `IdentityEvidence` and `Mechanism` from it, and `remote_agent` stays on the worker. Nothing in `src/` imports it. The schema-equality fixtures and their tests (`test_clio_schemas_convergence.py`, `test_schema_equality.py`) are deleted, because they compared relay's records with CLIO's schema package. So `clio-schemas==0.2.1` stays in `pyproject.toml`, and the earlier request to W1 to remove it is withdrawn. Dropping it needs `remote_agent` to stop using those five types.

### The process-containment tests

They do not go with the old surface. `process_containment*` is kept, because `bounded_command` and `remote_agent` use it on the worker, so `test_process_containment.py` and `test_storage_process_guard.py` stay.

The cause is not random. The capability probe (`process_containment_systemd_scope.py:52-112`) runs `systemd-run --user --scope` once. On a compute node it succeeds only while some session of the same user keeps a user manager there, and the later spawn can then fail with "Failed to connect to bus". The develop baseline shows the same cause in the endpoint activation tests: `mkdir: cannot create directory '/run/user/1008': Permission denied` (9 failures).

Decision: run them where a user manager exists. On Ares that is the login node, which has linger and a stable user bus, and where W1 measured 43 of 43 in 3 runs out of 3. The worker's endpoint service runs there too. W8's gate runs these modules on the login node, and runs the rest of the full suite in Slurm, so nothing is skipped and nothing is rerun to pass.

### Develop's pre-existing failures

develop 034117b on a compute node gives 139 failed and 3 errors, which is 142 node ids (`~/clio-relay-rework/w1-base.log`). By what W8 does to their file:

| Their file | Node ids | Files |
| --- | --- | --- |
| Kept, edited | 116 | `test_doctor_and_relay_host.py` (50), `test_endpoint.py` (20), `test_execution_watch.py` (8), `test_scheduler_cancel_attempt_claims.py` (7), `test_bootstrap_cluster_paths.py` (5), `test_scheduler_cancel_refusal_idempotency.py` (5), `test_http_api.py` (4), `test_endpoint_worker_lanes.py` (3), `test_service_runtime.py` (3), `test_validation_report.py` (3), `test_worker_service_policy.py` (2), `test_bootstrap_fast_path.py` (1), `test_cli_queue_maintenance.py` (1), `test_jarvis_execution_recovery_guards.py` (1), `test_scheduler_status.py` (1), `test_worker_concurrency.py` (1), `test_worker_lifetime_lock.py` (1) |
| Moved | 11 | `test_jarvis_recovery_scheduling.py` (3), `test_endpoint_bounded_reads.py` (2), `test_jarvis_handle_first_admission.py` (2), `test_control_query_admission.py` (1), `test_frpc_proxy_scripts.py` (1), `test_owner_session_lease.py` (1), `test_queue_validation.py` (1) |
| Deleted | 14 | `test_mcp_server.py` (7), `test_release_acceptance_runbook.py` (3), `test_release_command_stream.py` (2), `test_mcp_stdio_validation.py` (1), `test_remote_mcp.py` (1) |
| Kept as is | 1 | `test_wheel_record_closure.py` (1) |

- The 14 in deleted files are retired by the deletion.
- The 116 in edited files, the 11 in moved files and the 1 in `test_wheel_record_closure.py` stay. Their causes are the missing user manager on compute nodes (`/run/user/1008`, 9), old bootstrap receipt binding to clio-kit and jarvis artifacts, the uv version pin (`0.13.9` against `0.11.28`, 3), and the endpoint activation fixture outcomes (`preflight-unverified`, about 35).
- W8 reruns them after the move. A failure that remains in a test of kept code is fixed or reported, not skipped.

### The worker adapter

- **Shape.** `clio_relay.adapters.worker.WorkerAdapter` is registered under the `worker` entry point. It is a `BaseAdapter[WorkerConfig]` (`adapters/worker/adapter.py`). The config keys are:
  - `ssh_profile`, the Host in `~/.ssh/config` used for sign-in and placement (the key W11's catalog entries use), and `ssh_config_file`;
  - `scheduler`, `auto` or a psij executor name;
  - `work_dir` and `remote_dir`;
  - `rendezvous`: `address`, `port`, `protocol` (`tcp`, `websocket` or `wss`), `site_address`, `site_port`, `token_env` and `token_file`;
  - `relay_wheel` or `relay_requirement`, `site_frpc`, `python` and `submit_timeout_s`.
- **Placement.** In the one ssh pass relay installs itself in a private directory on the site, writes the worker's identity and starts `clio-relay worker site-serve` detached. That process serves the owned-session API on loopback and holds an frpc `stcp` proxy to the rendezvous. Relay reaches it over the one held link (`brokered_tcp`), and nothing uses SSH after that. `worker-link.json` (0600) in the adapter's state directory lets a restarted relay reconnect over frp without SSH.
- **Verbs.** Each verb is one POST to the worker's `/adapter` routes. On the site the verbs run through the `local` adapter, so scheduler coverage is psij's (RL18), and a refusal on the site comes back as the same typed refusal.
  - `describe`, `run`, `status`, `cancel`, `find` and `files` (`WorkerFileAccess`, `adapters/worker/files.py`).
  - `exec(command, *, timeout_s=None, max_output_bytes, on_stall=None)` follows W1's exec contract: with no `timeout_s` it runs until the command ends, and stall progress comes from the site's polls.
  - `serve` is `return await clio_relay.serve.start_for(self, spec, operation_id=operation_id)`, with a lazy import. Without the serve runtime it refuses `verb_not_implemented`.
- **Sign-in (LQ10, LQ11).**
  - `sign_in_needed()` returns `SignInNeed(provider="ssh", url=None, ...)` while the link is down.
  - The adapter calls `askpass.register_sign_in(site_id, starter)`. The starter runs placement and the connect over ssh with `clio_relay.asks.askpass.environment(site=, operation_id=, title=)` and `BatchMode=no`.
  - Deviation from W6's "close stdin": the connect pipes the placement script to the remote shell over ssh's stdin. Under `SSH_ASKPASS_REQUIRE=force` ssh never reads a prompt answer from stdin, so the prompt still goes to askpass. LQ10 and LQ11 passed this way.
  - When W6 is not installed, a local stub with the same signatures stands in.
- **The hop for W9.** `WorkerAdapter.hop_exec(command, *, timeout_s=None, max_output_bytes, on_stall=None) -> ExecResult` runs one command on the worker's host (the login side) over the held link, with the exec contract above. There is no ssh. Long-running service processes go through `run(spec)`. Service bytes stay on W9's frp connector and visitor.
- **Operator commands (RL3).** `clio_relay.adapters.worker.cli:worker_app`, a Typer app mounted as `clio-relay worker`. Its groups are `endpoint`, `relay-host`, `cluster`, `api`, `session`, `queue` and `scheduler`. Its commands are `status`, `init`, `install-frp` and `installation-*`, and the hidden `bootstrap-inspect`, `jarvis-runtime-authority` and `site-serve`. The old `agent`, `job`, `monitor`, `gateway`, `jarvis`, `remote-mcp`, `storage`, `release`, `live-test` and `diagnostics` groups go. Their verbs are the new surface's `run`, `exec`, `files` and `describe`.
- **Not covered by LQ4: wss.** The frpc config writer has no custom-CA option, so the fixture's self-signed wss endpoint cannot be trusted. LQ4 ran over tcp. A wss rendezvous with a public certificate needs no change.

### Changes made during the move

Fixes and edits beyond the mechanical move, in commit order:

- **safe_archive.** The deletion list included `safe_archive.py`, but the bootstrap remote script imports it from a string. It is restored byte for byte as `adapters/worker/safe_archive.py`, with `tests/test_safe_archive.py` (1d1a900). A sweep for string references to deleted modules found no other real caller.
- **The bootstrap candidate overlay.** It was flat and keyed by file name. Its keys are now paths under the candidate `clio_relay` package (`adapters/worker/X.py`), with `extend_path` inits for `adapters/` and `adapters/worker/`. The writer refuses absolute paths, `..` and symlinked parents. The overlay was never self-contained, even at base: it relies on `extend_path` falling back to the installed package (1d1a900).
- **`package_source_root`** in `bootstrap.py` used `parents[2]`, the repo root before the move. It is `parents[4]` now (9a88f8c).
- **The worker command prefix.** The bootstrap preamble and the readiness probe call `clio-relay worker endpoint worker-info`, and fall back to the unprefixed command for a relay placed before the move (2e223de). `test_bootstrap_fast_path` counts both.
- **`jarvis_mcp.py`** no longer imports `remote_mcp`. It keeps the contract, the artifact checks and its own `server_artifact_digest`. The virtual JARVIS tools and the MCP server builders went with the old surface (8dd5260).
- **Scheduler seams.** `http_api_routes_scheduler`, `test_cli`, `test_cli_endpoint`, `test_cli_scheduler` and the two scheduler-cancel tests use `clio_relay.adapters.worker.scheduler`, not the held `scheduler_providers` (80ebdaa).
- **GC purge** kept one open descriptor per directory level, so trash deeper than `ulimit -n` could never be purged (EMFILE at 1024, the soft limit on Ares login and compute nodes). It keeps only the current parent open now. Each level is still opened with `O_NOFOLLOW` relative to its parent. This defect predates the move; `test_gc_purge_is_iterative_and_detects_directory_swap_races` fails the same way on base ee1a845 (140b02f).
- **Tests.**
  - The patch-seam guard (`test_cli_patch_seam.py`) knows both packages. 23 audited pairs went with their callers or owners, so its counts go from 61/32 to 38/18 (531053b).
  - 15 test files import `worker_app as app`.
  - Tests of removed commands are deleted: `job`, `gateway`, `storage`, `live-test` and `scheduler validate-lifecycle`.
  - `test_ci_clio_kit_wheel.py` drops its three checks of the old ci and release steps. Its pin checks stay.
  - `test_core_queue_split_architecture` accepts `clio_relay.adapters.worker` as an owner package. `queue_validation` leaves its non-owner lists. The `queue_startup` import budget goes from 550 to 552, for the two import lines the move wraps.
  - `test_door_errors` expects 61 HTTP statuses, not 64. `jarvis_dispatch_refused`, `execution_not_found`, `mcp_submission_conflict` and `mcp_admission_refused` went with their routes.
  - `test_bootstrap` disables commit signing for its scratch commits (46751cd). Ares sets `commit.gpgsign=true`, and gpg fails in Slurm.
- **The bootstrap build-info line.** The payload template wrapped the `BOOTSTRAP_CANDIDATE_PROVIDER_BUILD_INFO` assignment across a swallowed line break, so bash ran the path as a command. Found by a live bootstrap on fx-slurm. It is assigned through `BOOTSTRAP_CANDIDATE_WORKER` now, with a regression test that every shell variable is assigned on its own line (dd1352c).
- **The route-revision warning** in `cli_cluster` told the person to refresh remote MCP, which is gone. It is removed with its 3 tests (dd1352c, af85a62).
- **The held modules.** `remote_mcp*`, `scheduler_providers/` and `browser_gateway*` are deleted after merging W9 at 5a01766 (91bcc3c, ba5ad4b). Test edits:
  - `test_scheduler_status`: 26 old provider, connector and registry tests deleted; the selection and JARVIS isolation checks run against `adapters.worker.scheduler`.
  - `test_door_errors`: the 3 browser gateway tests deleted.
  - `test_validation_report`: the 3 remote MCP tests deleted.
  - `test_ci_clio_kit_wheel`: the certification pin test deleted.
  - `test_clio_kit_mcp_contracts`: 4 contract tests deleted; the 3 install receipt tests stay.
  - `test_contract_gate`: imports `adapters.worker.clio_kit_constants`.
  - `test_cluster_config`: the two cache-path tests check only the registry path now.
- **Edits to W1's files (approved).** `scripts/check_file_size.py`: removal comments for deleted files, and lower baselines for `bootstrap.py` (924), `session_recovery_inspection.py` (837) and the dropped `jarvis_mcp.py` and `browser_gateway.py` entries. `scripts/check_no_class_in_function.py`: the `wheel_snapshot_identity` entry at its worker path. `scripts/check_pyright_ratchet.py`: the baseline goes 7498 to 5583 (89fd6c8), then to 5076.
- **W9's tests (import-only edits, and prunes that let them collect).** `test_service_runtime`, `test_jarvis_service_runtime` and `test_frp_transport_dials` import the moved modules from `adapters.worker`. 15 tests in `test_jarvis_service_runtime` are deleted, with the helper `_owner_token_values`. The first drove `cli_jarvis_mcp` and the rest drove `mcp_server`:
    - `test_hidden_relay_authority_command_emits_one_private_json_document`
    - `test_model_facing_mcp_projection_redacts_runtime_bearer_token`
    - `test_verified_jarvis_wait_preserves_public_bearer_fingerprint`
    - `test_jarvis_authorization_restore_rejects_non_public_descriptors`
    - `test_arbitrary_mcp_cannot_expose_jarvis_shaped_authorization`
    - `test_waited_execution_exposes_compact_verified_binding_before_large_result`
    - `test_generic_mcp_wait_serializes_verified_result_before_bulk_evidence`
    - `test_async_relay_wait_exposes_same_compact_verified_service_binding`
    - `test_ssh_remote_async_relay_wait_exposes_verified_service_binding`
    - `test_owned_remote_async_relay_wait_exposes_verified_service_binding`
    - `test_multiple_ready_services_have_exact_unchanged_bindings`
    - `test_bind_rejects_mixed_or_invalid_handoff_forms`
    - `test_agent_bind_persists_urls_and_rejects_runtime_commands`
    - `test_agent_bind_pending_replays_same_gateway_and_connector_intents`
    - `test_owned_agent_bind_uses_shared_cluster_scoped_gateway_admission`
- **The 1.0 release gate (coordinator decision, 67f6488).** The LQ and RL rows replace it.
  - Deleted: `release_gate_evaluation`, `release_gate_resources` and `release_gate_targets`. Also the modules only the gate reached: `spack_transition_checks`, `ci_validation`, `validation_report_assets`, `actions_artifact`, `branch_protection`, `candidate_provenance`, `ci_run_status`, `payload_policy`, `release_assets`, `release_identity`, `distribution_archive` and `provenance_primitives`. Their tests went too (`test_ci_validation`, `test_distribution_archive`, `test_provenance_primitives`, `test_release_external_runtime`), as did `docs/release-gate-1.0.yaml` with its `virtual_*_mcp_call` roles, `docs/release-acceptance-1.0.md` and `examples/release-gate/`. The set is the import closure of the gate modules minus everything that kept code still imports.
  - The report code that bootstrap and session commands use moves out of `validation_report`, `validation_schema` and `validation_recorder`, which are deleted, into plain names:
    - `run_report_models` (the report wire models);
    - `run_report` (default path, write, load, markdown);
    - `run_report_recorder` (`ValidationRecorder`, `new_live_validation_report`);
    - `install_identity` (`detect_software_identity`, `detect_install_source`);
    - `file_digest` (`sha256_file`).
    Callers import from the owner directly; there is no re-export facade. The wire shape is unchanged: persisted reports load with `extra="forbid"`, and W9's runtime builds them.
  - `tests/test_validation_report.py` becomes `tests/test_run_report.py`. Its 35 gate tests and gate helpers are deleted, and 48 stay. `test_gateway_start_and_stop_reports_record_their_checks` keeps the report half of the old combined test.
  - `test_bootstrap_uv_pin_matches_release_policy` read the YAML. It is now `test_bootstrap_uv_pin_is_digest_bound`, with the digest asserts only.
  - The four bootstrap remote scripts import `sha256_file` from `clio_relay.file_digest`.
  - Also changed: `.gitattributes` (the `examples/release-gate` lines), `docs/ai/testing-context.md` (the release CI section and the command list), and `docs/ai/interface-context.md` (`release validate-local` and `release gate`). The `--allow-login-scoped` help no longer says "release-gate eligible".
  - W9's `service_runtime_results.py` gets import lines and one docstring line only. W1's scripts are not edited: their comments still name the deleted files, and `check_file_size.py` reports ratchet-down room only.

### Gate evidence

**The 1.0 release gate removal (67f6488).** Pyright on Ares counts 4929 errors, under the 5050 baseline (W1's script; W1 lowers it). The targeted modules ran on the login node, with the fixtures env and uv on PATH: `test_run_report`, `test_cli`, `test_installation`, `test_service_runtime`, `test_session_wire_models`, `test_transport_probe`, `test_bootstrap`, `test_bootstrap_reconcile`, `test_acceptance_report_defaults`, `test_cli_installation_receipt`, `test_cli_relay_host`, `test_dev_mode`, `test_identifiers`, `test_session_lifecycle` and `tests/serve`. 798 passed and none skipped. Four did not pass:
- 3 errors in `test_run_report` (`test_real_uv_tool_scopes_launcher_to_isolated_bin`, `test_real_uvx_0_11_28_produces_verified_os_bound_launcher_receipt`, `test_real_uvx_no_cache_temporary_environment_is_not_a_verified_receipt`). They are develop's 3 `test_validation_report` failures, under the new file name: `~/.local/bin/uv` is 0.13.9, and the tests pin 0.11.28.
- `test_service_runtime::test_local_connector_does_not_retain_captured_cli_pipes` (W9's). It gives a child CLI 2 s to import `service_runtime_command_runner`, spawn and exit. That import alone takes 3.8 s on the Ares login node, and 7.4 s at load 20. It takes the same time at 126f8cd, before this change, so the removal did not cause it. It is a timing test that fails under load; reported to W9 through the coordinator, not rerun.

On Ares, at ba5ad4b unless noted. The merge of integration at dae2774 (8b34a44) was clean and touched no worker file.

- **LQ4 (`worker`).** The conformance kit in `tests/worker` passed 11 of 11 with no skips. It ran on the login node against fx-slurm over a tcp rendezvous, with a fresh remote directory and the wheel built at that commit (sha256 `b81d0726206f…`). The kit includes the new exec-without-limit test and the `verb_not_implemented` checks.
- **LQ10 and LQ11.** Passed on fx-otp through W6's askpass interface: the sign-in ran as asks, and the link came up with no terminal (`op_6774ddf180e7bcf14383b593`).
- **Bootstrap.** A live `clio-relay worker cluster bootstrap` on fx-slurm gets past the overlay. It then fails exactly as base ee1a845 does: `systemctl: command not found`, and then "bootstrap existing executable digest changed" for `~/.local/bin/uv`. Another workstream put uv 0.13.9 there, and the bootstrap pins 0.11.28 and refuses to adopt it. No fixture has a systemd user manager, so a full bootstrap cannot finish on one. What this proves is that the moved bootstrap behaves like base.
- **The process-containment modules** (the six that need a user manager) passed 170 of 170 on the login node at af85a62.
- **Targeted modules** (17 files: the edited tests plus the cli, scheduler, http api, service runtime and patch-seam modules) ran in Slurm (job 27799). 13 tests failed, all of them in develop's 142, so none are new: 12 in the two scheduler-cancel files ("relay-managed JARVIS installation receipt does not match this worker"), and `test_isolated_named_wrapper_loads_plain_module_roots_without_python_hooks`, which fails because the compute node has no user manager.
- **pyright.** 5076 errors, under the 5583 baseline, measured with `uv run` at 8b34a44. The baseline is lowered to match. Run without the venv active, the count is 8987, because imports do not resolve then. That is a measuring mistake, not a regression.
- **The full suite** ran once, in Slurm job 27809 at a4dc2cf, without the six user-manager modules (which run on the login node). The run counted 3913 dots (passing tests) and 8 skips, plus 126 failed node ids:
  - **46 are in develop's list.** All are in kept or moved files: `test_endpoint` (18), the two scheduler-cancel files (12, "relay-managed JARVIS installation receipt does not match this worker"), `test_bootstrap_cluster_paths` (5), `test_jarvis_recovery_scheduling` (3), `test_endpoint_bounded_reads` (2), and one each in `test_endpoint_worker_lanes`, `test_frpc_proxy_scripts`, `test_owner_session_lease`, `test_scheduler_status`, `test_worker_concurrency` and `test_worker_lifetime_lock`. Their causes are the ones listed under "Develop's pre-existing failures".
  - **78 failed because of where they ran.** W2's fixtures listen on the login node's loopback (`127.0.0.1:47100-47107`), which a compute node cannot reach (`relay_ssh`, `test_fixtures_smoke`, `asks/test_askpass_otp`). `relay_tapis` failed because W8's venv lacked the `tapis` extra.
  - **2 more were not in develop's list.** `test_remote_cli` expected the remote command without the `worker` group; the test is fixed (24c6d6d). `test_remote_agent_runner` runs `python` from PATH, which Ares does not have outside the venv.
  - **Rerun on the login node at 24c6d6d**, with the fixtures env, the `tapis` extra and the venv on PATH: `relay_ssh`, `relay_tapis`, `test_fixtures_smoke`, `asks/test_askpass_otp`, `test_remote_cli` and `test_remote_agent_runner` passed 178 of 178, none skipped. The six user-manager modules passed 170 of 170 there.
- **Develop's failures retired.** W8's base list has 173 node ids. 127 no longer fail:
  - 59 were in deleted files;
  - 52 are in the user-manager modules, which pass on the login node;
  - 16 in kept files now pass.
  The 46 above remain. None of them is skipped or rerun to pass.

### Workflows

There are 8 files in `.github/workflows/`, not 6: `ci`, `finalize-release`, `live-validation-attest`, `main-provenance`, `release-gate`, `release`, `released-validation-attest` and `stage-candidate`. `ci` is rewritten to run `uv run ruff check`, `uv run ruff format --check`, `uv run pyright` and `uv run pytest` on the new tree. `release` is reduced to building and publishing the wheel. The other six only served the old release gate, which reads the deleted `release_*`, `live_validation*` and `promotion_record` modules, so they are deleted. Neither workflow is enabled or waited on.

### Requests to other owners

- **W9.**
  - Done: W9 dropped its imports of the held modules (5a01766), and W8 deleted them (ba5ad4b). `BrowserAttachmentRecord` now has one definition, `adapters/worker/browser_attachment_record.py`, and nothing calls the `connector-step-*` commands. W8's branch contains W9 at 5a01766, so merging W8 brings it in.
  - `frp_link.py:174` tells the person to run `clio-relay cluster add`. The command is now `clio-relay worker cluster add`.
  - `jarvis_service_runtime_validation.py:55` used the `mcp_call` job kind. W9 removed it at 5035e8c; W8 removes the kind once that is in integration.
  - Test failures in W9's files on this branch, none of them rerun:
    - 12 gateway tests in `test_service_runtime.py` and 2 in `test_jarvis_service_runtime.py` call the deleted `gateway` commands or `scheduler connector-step-start`.
    - `test_local_connector_does_not_retain_captured_cli_pipes` fails with "captured relay CLI waited for its long-lived connector grandchild". It is not in develop's failure list. Cause found at 67f6488: the test gives the child CLI a fixed 2 s, and importing `service_runtime_command_runner` alone takes 3.8 s on the Ares login node (7.4 s at load 20), the same before and after W8's change. It is a timing test that fails under load; W9 should drop the fixed budget.
- **W1.**
  - Lower the pyright baseline in `scripts/check_pyright_ratchet.py` to 4929 (measured at 67f6488).
  - Keep `clio-schemas` (see above). The `mcp_call` force-include in `pyproject.toml` is removed on this branch (approved edit).
  - Mount `clio_relay.adapters.worker.cli:worker_app` as `clio-relay worker` in `cli_next.py`, and at the cutover point `clio-relay` and `python -m clio_relay` at the new surface.
- **W7.** Four ssh masters to fx-otp (`/tmp/clio-relay-ssh-1008/*.sock` and `/tmp/w7dbg.*/cm.sock`) were left running from an earlier W7 run. W8 does not touch them.
- **Residual.** W8's scratch from LQ10 and LQ11, `~/w8-live` on fx-otp, is still there. Removing it needs a keyboard-interactive sign-in to fx-otp. W8's other Ares and fx-slurm scratch is removed, except `state/w8-live/worker-gate.json`, which the post-cutover LQ1 and LQ32 runs use.
- **W3 and W4 (clio-agent).** None. The old command-line calls in `drivers.py` went with RL38 (6801be185).

## W9. serve

Branch `jaimecernuda-relay-rework-w9`; the commit sent to W1 is named in the report to the coordinator. Rows: RL19 to RL23, LQ12 to LQ15, the service half of LQ-L1, and LQ-L11. It feeds RL43 and RL45. Host page: [docs/rendezvous-host.md](../rendezvous-host.md).

Files: `src/clio_relay/serve/` (config, plan, frp, forward, desktop, mount, runtime, store, descriptor, provenance, the vendored `schemas/view-descriptor.v1.json`, and `_site/launch.sh` and `_site/hop_proxy.py`, which run on the site), and `tests/serve/`. The old runtime files (`service_runtime*`, `jarvis_service_runtime*`, `models_gateway.py`, `frp_*`) and their tests stay W9's until the old surface goes.

### Published interfaces

- `clio_relay.serve.start_for(adapter, spec: ServiceSpec, *, operation_id) -> Endpoint`. An adapter that declares `serve` returns this from its `serve()` and never reaches into `serve/`. The runtime submits the wrapped job through `adapter.run` and reads it through `adapter.status`. It also uses three optional adapter methods:
  - `ssh_argv()` and `transport.environment()` (W7) for the `ssh -L` path;
  - `hop_exec(site, argv)` (W8), or `exec` where there is no `hop_exec`, to start the login hop.
- The `ServeHook` comes from the `serve` entry point in `clio_relay.hooks` (`clio_relay.serve:factory`):
  - `start`, `status` and `stop`;
  - `attach(server, changes)` mounts served MCP servers and announces them with `changes.tools_list_changed()`;
  - `ui_data(record)` gives `{"ports": {<name>: {"address", "url"?, "secret"?, "secret_env"?}}}`. W1 puts it in the serve result's `_meta["ai.iowarp.relay/service"]`. It is the only place a URL or a secret appears (RL22).
- The CLI functions: `connect(handle, *, paths)` and `settings(paths, changes)`, behind `clio-relay connect` and `clio-relay rendezvous set|show`.
- Settings. The file is `rendezvous.json`, found in this order: the `rendezvous_file` hook setting, then `CLIO_RELAY_RENDEZVOUS_FILE`, then `<state>/serve/rendezvous.json`.
  - `rendezvous`: `address`, `port`, `protocol` (tcp, kcp, quic, websocket, wss), `token_file` or `token_env`, `ca_file`, `server_name`, `frpc`, and `direct` (XTCP first, STCP as the fallback).
  - Per site, a `serve:` block in the site entry, or the file's `sites`:
    - `path`: auto, frp or ssh;
    - `address` and `port`: the rendezvous as the site sees it;
    - `hop`: none, login or http_proxy, with `proxy_url`, `hop_bind` and `hop_advertise`;
    - `frpc` or `frpc_binary`, and `python`;
    - `stop_grace_s`, default 30.
- In the job, `${CLIO_RELAY_PORT_<NAME>}` in args and env becomes the service's port on the node.
- Mounted tools are named `<site>_<name>_<tool>`. Their MCP Apps metadata (`_meta.ui`) passes through unchanged.
- `serve.descriptor`: the view descriptor schema, vendored byte for byte from clio-jarvis a6aba08 (identical at v0.1.0, 2d12dbb), sha256 `8773b9ed…1b076`, with `problems(descriptor)`.
- `serve.provenance`: the neutral block `relay.provenance.v1` under an artifact's `metadata["provenance"]`, and `for_output(record, output)`. It names nothing CLIO-specific. `models_gateway.ArtifactRef` reads it first and the legacy `clio.provenance.v1` second.

### Decisions

- A service is never exposed publicly. Its frpc runs beside it inside the job and registers an STCP proxy (XTCP too when `direct` is set) with a per-service secret key. The desktop runs the visitor on a loopback port. With no rendezvous host and an adapter that gives `ssh_argv()`, relay forwards with `ssh -N -L` instead, adding `-J` when the job is on a compute node.
- wss always verifies the certificate: frpc gets `trustedCaFile` (when `ca_file` is set) and `serverName` explicitly. This closes the open item on frpc not verifying the frps certificate.
- Port protection (RL22): an `http` port must say how it is protected, or validation refuses it. If the port is already in use on the node, launch.sh refuses with exit 98 and names the port. The service that holds it is left alone.
- Stop (RL23): relay writes a stop file, and launch.sh sends TERM to the service's process group. After `stop_grace_s` it sends KILL. Then it writes `final-artifact.json` and the artifacts list. The outputs are the service's own files plus `serve/{frpc,launch,service}.log`, stdout and stderr. The wrapper's own files, `.relay-package` and the package runner's JARVIS tree are left out, except `.relay-jarvis/shared/`.
- Durable rebinding (LQ13): frpc logs in again on its own. The visitor rebinds the same local port. The plan, including the secret, is kept in `<state>/serve/plans/`, so a new relay rebinds the same port at startup (from the mount provider's lifespan). The ports of a service whose job has ended close when that operation is next observed.
- One session (LQ14): the mount holds one client connection to one server instance for the life of the service, checked with W2's `server_instance`.
- No time limits. Relay sets no duration on a service. The `ssh -L` forward is retried with visible restart lines, and readiness has no deadline.
- The STUN server for XTCP is a setting: `rendezvous set stun_server=host:port`, overridden by `site.<name>.stun_server`. Both frpc ends get `natHoleStunServer` only when `direct` is on. The default is stun.nextcloud.com:3478. With `frpc nathole discover`, 5 tries each from the Ares host and from the fx-pbs container: voipgate, sipgate and nextcloud 10/10, frp's default stun.easyvoip.com 7/10 (the rest timed out), miwifi 8/10, voipbuster 4/10, stunprotocol.org and freeswitch 0/10. Cloudflare and Google answer only from one address, which frp refuses ("need 2 addresses"). Nextcloud was chosen from the three at 10/10 because it is run by a known project and supports RFC 5780.
- The direct path waits longer and says so. The visitor has `keepTunnelOpen = true`, so it punches at start, and `fallbackTimeoutMs` comes from `direct_wait_s` (default 10, at most 19, because frpc's own punch gives up at 20 s). A connection that starts while no tunnel is open waits that long, then goes through the rendezvous host. Relay reads frpc's log and shows the path as a line in the serve status and on `connect`'s stderr: "trying the direct path (XTCP)", "direct path (XTCP) open", "the direct path (XTCP) failed (<frp's reason>); a connection waits N s for it, then goes through the rendezvous host (STCP)" and "a connection waited N s for the direct path (XTCP) (<reason>) and went through the rendezvous host (STCP)". The trade-off: where XTCP never works, every new connection waits `direct_wait_s`, so `direct` should be off there.
- serve writes the adapter's handle into the operation record when `run` returns, when `_handle()` recovers it, and before the final outputs. A service's provenance then names the scheduler's `native_id`, and the ssh adapter hides `<native_id>.out`, `.ec` and `.nodefile`.
- The service secrets and the frp token reach the job as the input `serve/secrets.env`, marked `transient` (W1's `InputRef.transient`). launch.sh deletes the job's copy once read, and the adapter drops the staged blob from the site's content store at the final state. serve stages through `adapter.files().stage`, so relay keeps no copy of its own.
- Placement is the caller's choice. The headline driver puts pvserver on the run's node with the custom attribute `slurm.nodelist` and binds it with `lifetime.bound_to`. serve does not choose nodes.
- Three cuts in the old runtime, approved by the coordinator. The new server has no compatibility path, and 1.6.8 stays on PyPI for the old surface.
  - The allocation-scoped connector is retired. Starting one refuses and points to serve. Stopping or detaching an old session that has one reports a residual naming the Slurm job and step to `scancel`. The remote `connector-step-*` commands and every caller of them are gone.
  - Browser attach is gone. Revoke on teardown stays for records an older relay left, through `adapters.worker.browser_attachment_record`.
  - JARVIS MCP results that came through a registered `remote_mcp` route are refused, because that route came only from the deleted registry.
- The old runtime no longer imports `browser_gateway`, `scheduler_providers`, `remote_mcp` or the old `cli`. `core_queue` comes from `clio_relay.adapters.worker.core_queue`. Tests of the retired connector, browser attach and the deleted gateway CLI commands were removed, and the rest were migrated.

### Facts

- At protocol 2026-07-28, `notifications/tools/list_changed` reaches a client only on a `subscriptions/listen` stream with `tools_list_changed: true`. A client that never opens one does not see mounts.
- Ares `/mnt/common` is NFSv3 with a 120 s attribute cache. A file written on one node can stay invisible to `[ -f ]` on another node for up to two minutes; a readdir forces it. launch.sh checks the stop file with `ls`. Reading a growing output from another node can lag, so pvserver goes on the run's node.
- On Ares, a compute node's sshd sometimes accepts the key and then closes every session ("Connection closed by UNKNOWN port 65535"), even for a plain `ssh ares-comp-03` from the login node. It happened while other short jobs started and ended on the same node under the shared account. The `ssh -L` forward recovered by retrying.
- `jarvis ppl run` is `start()` then `stop()`, so a package runner that ends in `ppl run` stops a service as soon as it is ready.
- An Ares venv must be synced again (`uv sync --all-extras`) when the entry points in `pyproject.toml` change. Otherwise a hook falls back to its stub with no message.
- Strict pyright fails across the whole repository at every point. The count fell from 1940 errors at M1 (719104c) to 1729 on this branch, and no new error is in W9's files.

### Evidence

All runs on Ares from `~/clio-relay-rework/clio-relay-w9` with its own venv. Each is a plain MCP client over stdio to `clio-relay-next serve` at 2026-07-28 on the tasks path, unless marked otherwise. The fixture runs use site fx-slurm (W7's ssh adapter) and W2's frps at 47104. The site and the rendezvous were set with the CLI (`site add`, `rendezvous set`).

- LQ12, `op_7aab27479b5e65d1952ed548`: the serve task reached `working` with "the service is running: web 127.0.0.1:25687". `serve(service=)` returned an endpoint with no URL. The secret and URL were only in `_meta["ai.iowarp.relay/service"]`, and describe carried no secret. `/health` gave 200, `/` without the secret gave 401 and with it 200 (hostname fx-slurm), and a WebSocket echo worked.
- LQ13, through the surface, `op_cecb10b501c9c394694eb4db`: a scratch frps inside fx-slurm went down for 60 s and came back. The task stayed `working`, and `/health` came back on the same port 127.0.0.1:57819 about 20 s later. The echo worked before and after. An earlier hook-direct run, `op_4563a495…`, gave the same result.
- LQ14, `op_3b4d3ed48567ff3ed011fbcd`: W2's counter MCP was mounted as `fx-slurm_counter_{count_slowly,increment,read,reset}`. The client heard `tools/list_changed` on its listen stream at mount and again at unmount. Three increments gave 1, 2 and 3, and read gave 3. `server_instance` stayed `34954cbd950d47b28d592c9f5c12cea1` (pid 42974) throughout, while `session_id` changed on every request. After stop, no tools were left under the prefix.
- LQ15, `op_7aab27479b5e65d1952ed548`: `serve(stop=)` completed with "stopped". The task result listed `final-artifact.json`, `serve/{frpc,launch,service}.log`, stdout and stderr, and the desktop port closed. Whether CLIO shows these as artifacts is the client half.
- Refusals: an unprotected http port is refused at validation ("port 'web' is http: say how it is protected (RL22)"). `op_e0642d468a4e401aa3d5edd3` failed with exit 98 and "refused: port 33867 (web) was already in use on fx-slurm", and the service holding the port kept running.
- wss with the fixture CA (`FX_WSS_CA_FILE`, serverName fx-frps), `op_205e6c36b36e94547e5285be`: health 200 and echo. With a wrong CA, `op_c510b58cf681ce1b9ecf5044`: frpc refused ("x509: certificate signed by unknown authority"), the service never reported ready, and the status message named frpc's warning.
- The login hop through relay's CONNECT proxy, `op_c5aacee8c9db63ba48fd3808`: health and echo worked, and the proxy closed afterwards.
- `ssh -L` with no rendezvous, `op_8222d02b39b5abe644f900b4`: health and echo worked, and the forward closed afterwards.
- `clio-relay connect <handle>` against a relay over HTTP printed "web: http://127.0.0.1:64319". The handle's port was already held by relay on the same machine, so connect picked a free one. Health, the secret and the echo passed, and connect's frpc exited on SIGTERM.
- Relay restart, `op_37114594d3e2ddeb52e47464`: a new relay rebound the same port and secret at startup. `/health` gave 200 before any call, then the echo worked and stop completed.
- No duration, `op_f03945ba4f85360941a309b9`, Slurm job 27750 on ares-comp-03 (site `ares`, pvserver): relay sent `{"queue_name": "debug"}` and nothing else, so squeue showed the partition's limit, 4-00:00:00. The service read `running` from 2.4 to 12.8 minutes. At 12.8 minutes pvpython rendered through the desktop port (screenshot read: "after 13 min: rendered on ares-comp-03"). Stop gave "completed, stopped", and the port closed afterwards.
- LQ-L11 and the service half of LQ-L1, on site `ares` over `ssh -L` (Ares has no rendezvous host):
  - pvserver alone, `op_9c61aa86f5da1a5712ec8801`: clio-jarvis's supervisor ran 2 ranks in persist mode on ares-comp-03 and was ready in about 7 s. Two separate pvpython clients rendered through the loopback port; both screenshots, read, show the wavelet contour captioned "rendered on ares-comp-03". Stop took about 12 s ("stopping: stopped", exit 0). The artifacts included `pv/pvserver-stop.json`, and the port closed afterwards.
  - The headline chain, run `op_9c7046029b42d2e46b3f0fc8` and serve `op_5cd896ca3eb45c9ca0b74768`: Gray-Scott ran with 4 ranks, L128 and 15000 steps, writing BP5 on ares-comp-03. pvserver was served bound to the run and placed on its node. pvpython followed the growing output with ADIOS2VTXReader reloads at 8, 23, 39 and 54 steps; the screenshots, read, show 4 spots at step 8 and a full pattern at step 54. When the run completed, the bound service ended on its own ("completed, stopped") and the port closed. The agent then listed and read the run directory with `files`.
- RL45, pvserver as a clio-jarvis package (`clio_jarvis.pvserver` v0.1.0, archive sha256 `43d9c882…f20419`), lowered by W13's `guard.prepare` (w13 6f6ad40 merged into a throwaway tree) on the ssh adapter. serve needed no change.
  - Slurm 27780: clio-jarvis's contract refused `max_lifetime=0`.
  - Slurm 27782, with `max_lifetime` 172800: pvserver became ready, then the runner's `jarvis ppl run` stopped it.
  - The next runs used a runner tail of `jarvis ppl start`, then a wait, with `jarvis ppl stop` on TERM:
    - Slurm 27783 held about 11 minutes. `serve(stop=)` sent TERM, `ppl stop` ran and pvserver exited 0.
    - Slurm 27787: two pvpython clients rendered through 127.0.0.1:46691 on ares-comp-03, and the port was closed after stop.
    - Slurm 27791: the same run, with clean outputs.
  - The headline chain as a package: run `op_2a7c5f13547423ea0abafd29` (Slurm 27792, ares-comp-03) and serve `op_40c9386b921a742d4823146e` (Slurm 27793), bound to it. The view followed 4 rounds, from step 21 (t 3150) to step 67 (t 10050). The screenshots `follow-op_40c9386b…-1` and `-4`, read, show the pattern evolving. The bound service ended "completed, stopped", and the port was closed after.
- XTCP direct (`rendezvous set direct=true`):
  - Through the surface, `op_7dad65f0c715f5df5adebfac`: both frpc logged "nathole prepare success, nat type: EasyNAT" and got the other's candidates. The punch then failed ("make hole error: wait detect message error: read udp4 ... i/o timeout"), and the visitor fell back to STCP. Health, 401, 200 with the secret, the echo and stop with artifacts all passed over the fallback.
  - The punch cannot succeed between the Ares host and a fixture container. Both sit behind one public NAT (216.47.152.168) with no hairpin. The host drops every UDP datagram from the docker bridges, and a direct test to 172.27.0.1 and 172.17.0.1 reached nothing. frp also lists only 10 local addresses, and 172.27.0.1 was not among them.
  - Peer run, `op_9af32f2014986c74d635aab6`: relay served the secret service with `direct` on, through the surface. Relay's own `visitor.toml` for that operation ran in the fx-pbs container, with only `serverAddr` and `serverPort` changed to fx-frps:7000. A second visitor ran relay's XTCP block alone, without `fallbackTo` and `fallbackTimeoutMs`. It logged "establishing nat hole connection successful, remoteAddr [172.27.0.7:41533]" (fx-slurm). Through it, `/` without the secret gave 401 and with it 200, with hostname fx-slurm, so those bytes crossed the P2P tunnel. Its first request was reset while two STUN lookups timed out, which is expected with no fallback.
  - With the STUN setting and the visible fallback (50ce56b), `op_e831a442cb8e1f5540fa4727`: relay's `visitor.toml` had `natHoleStunServer = "stun.nextcloud.com:3478"`, `keepTunnelOpen = true` and `fallbackTimeoutMs = 10000`, and the site's `frpc.toml` had the same STUN server. Before a connection the status read "serving: trying the direct path (XTCP)". A GET from the Ares host gave 200 after 10.1 s, and the status then read "a connection waited 10 s for the direct path (XTCP) (make hole error: wait detect message error: read udp4 ... i/o timeout) and went through the rendezvous host (STCP)". Relay's visitor config, run in fx-pbs, logged "nathole prepare success, nat type: EasyNAT" 0.2 s after start and "establishing nat hole connection successful, remoteAddr [172.27.0.7:55550]" 1.4 s after start. Its first health check, 401 without the secret and 200 with it all passed at once, and again 2 s later.
  - `clio-relay connect`, `op_a9b10815dd02685d934a9e8d` (relay over HTTP, Ares host): connect printed "trying the direct path (XTCP)", then the failed line with frp's reason, then one "a connection waited 10 s ... went through the rendezvous host (STCP)" line for each of health, the secret request and the echo, all of which passed.
- Native id, `op_ea37477274c4e1030aad7c00` (site `ares`, Slurm 27818, ended by an external `scancel`): the record's `handle.native_id` was 27818 while running and at the end, every output's provenance had `native_id` 27818, and `27818.out`, `.ec` and `.nodefile` were in the job directory but not in the outputs. The state was cancelled with "the job is cancelled".
- Transient secrets and W7's trap, after merging integration bf45eb9: `op_790da0420d19688989ffe3f0` (site `ares`, Slurm 27824, a port protected with `secret_env:SVC_SECRET`, ended by an external `scancel`). While it ran, the site's content store held one new blob starting `export `, `0cf200d2…`; at the final state it was gone, and the job's `serve/` had no `secrets.env`. The trap finished: `serve/stopped.json` read `{"reason": "signal", "exit_code": 143}`, `serve/artifacts.txt` listed `result.txt`, and the outputs were `result.txt`, `serve/{launch,service}.log`, stdout and stderr, with no `27824.out`. Before W7's fix, Slurm 27808 had been killed partway through the trap.
- `tests/test_service_runtime.py::test_local_connector_does_not_retain_captured_cli_pipes` failed on Ares at load average 21, where importing the runner alone took about 6 s against the test's 2 s budget. It now checks order instead of time: the grandchild runs until the test writes its stop file, so the CLI's pipes closing first proves the CLI did not wait.
- The `http_proxy` hop, `op_40d03c469267e5de573c7805`: proxy.py 2.4.10 (from PyPI, in a scratch venv in fx-pbs on port 3128) served as the site's HTTP proxy, with `site.fx-slurm.hop=http_proxy` and `proxy_url=http://fx-pbs:3128`. The job's `serve/frpc.toml` carried `transport.proxyURL = "http://fx-pbs:3128"`. The proxy logged "172.27.0.7:34690 - CONNECT fx-frps:7000 - 5297 bytes - 8262.41ms", the job's frpc tunnel for the service's whole life. Health, 401, 200 with the secret and the echo passed through the surface. Stop completed with the artifacts, and the port closed afterwards.
- Artifacts and provenance on a stop, `op_545c46fb15cae3f495f4a32f` (site `ares`, Slurm debug): the program wrote `result.txt` and served its directory, and `serve(stop=)` completed with "stopped". `serve/artifacts.txt` listed `result.txt` once, with no Slurm `<job id>.out`; before 3c209ee it listed each file twice, because the job directory is `/mnt/common/...` and the service directory `/home/...`. Every output in the task result carried `metadata["provenance"]` with schema `relay.provenance.v1` and the operation id.
- Regression tests on Ares are in the gate report: `tests/serve`, `tests/relay_next`, `tests/test_service_runtime.py`, `tests/test_jarvis_service_runtime.py` and the `tests/test_frp*` modules, in a venv with all extras.

### Requests to other owners

- **W13:** the package runner must be service-aware. A service package needs `ppl start`, a wait, then `ppl stop` on TERM; an application package keeps `ppl run`. The tail above is proven. It needs a service flag from `_start_service` through `guard.prepare`; W1's side is in (e96bea0, `service=True`), and W13's runner has to act on it.
- **W5:** the pvserver contract caps `max_lifetime` and `walltime` at 60 to 172800 s, and `max_lifetime` is required (default 14400). That conflicts with "no hard time limits".
- **W1:** done at 9f00ef6 and 0ff0150. `OutputRef.metadata` carries relay's block under `provenance`, and `JobStatus.progress` carries a step's outputs with their view descriptors. `RelayService._apply` now runs every status through `surface/outputs.py:marked_status`, which applies `serve.provenance.with_provenance` to run and serve outputs alike (final and `progress.outputs`) and checks view descriptors with `serve.descriptor.problems`.
- **The owners of five non-W9 tests:** `remote_mcp*` is imported only by `test_ci_clio_kit_wheel`, `test_clio_kit_mcp_contracts`, `test_cluster_config`, `test_contract_gate` and `test_validation_report`. Deleting it belongs with them.
- `scheduler_providers` stays, because W8's `adapters/worker/http_api_routes_scheduler.py` uses it.

### Open

- XTCP is proven between two hosts that can exchange UDP. A full surface run on Ares only ever shows the fallback, because of the shared NAT and the host's UDP filter (see Evidence). A desktop outside Ares would show the direct path.
- A site with no outbound UDP never gets the direct path, whatever the STUN server. With `direct` on there, each new connection waits `direct_wait_s` before the fallback (see Decisions).
- The `connect` handle carries no frps token. `connect` reads it from the person's own rendezvous setting.
- `plans/remote-visualization-scope.md` was not available. Its seven questions are answered by RL19 to RL23.

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
- **Package mode runs the `jarvis` on PATH with a `JARVIS_ROOT` of the job's own (W13's sequence, jarvis-cd 1.8.1).** The package is `<repo>.<package>`; the archive holds `<repo>/<repo>/<package>/...` at its root, plus whatever the repo reads beside it (clio-jarvis reads `schemas/` there, `_contract/spec.py:42`). The launcher refuses a name not of that form (exit 64) and an archive with no `<repo>/<repo>` (exit 66) before calling jarvis. Then, with `JARVIS_ROOT=<job dir>/.relay-jarvis`: `jarvis init "$JARVIS_ROOT/config" "$JARVIS_ROOT/private" "$JARVIS_ROOT/shared"`, `jarvis repo add <unpacked>/<repo>` (jarvis names a repo after its directory, so not the digest directory), `jarvis ppl create relay_<job uuid>`, `jarvis ppl append <name> pkg key=value...` (the alias `pkg` is explicit, or jarvis takes the first `key=value` as the alias), `jarvis ppl run`. Reason: the site's own `~/.ppi-jarvis` is never read or changed, and jobs share nothing. `TapisAdapter.runs_packages` is `True` (W1's `BaseAdapter` flag, integration 1a8c20b).
- **A service package holds until cancelled, then stops (RL45, W13's request, W1's `PackageRef.service` at integration dae2774).** With `spec.package.service`, the launcher record's mode is `package-service`, and the launcher runs `jarvis ppl start` instead of `ppl run`, then waits in a `sleep 60 & wait` loop. A `TERM`, `INT` or `HUP` trap, or the loop's wait failing, runs `jarvis ppl stop` and ends with the stop's exit code, so a clean cancel leaves `relay.exitcode` 0. A failed `ppl start` also runs `jarvis ppl stop`, then ends with the start's code. Three changes from W13's tail, so W13 can match them: (1) the loop's `wait` failing also stops the service, because a Tapis `FORK` cancel is `pkill -9 -P <launcher pid>` (Jobs `JobExecutionUtils`), which kills only the sleep and sends the launcher nothing; (2) the failed-start stop; (3) the sleep's pid is kept in a variable, because under `set -u` a `$!` read in the trap is unbound if `TERM` arrives before the first sleep.
- **`BATCH` jobs start the launcher with `exec` where the system allows it (RL45).** Tapis's batch script runs `<cmdPrefix>./tapisjob_app.sh <args> > tapisjob.out 2>&1` in the foreground. On Ares (Slurm 21.08.5, proctrack/cgroup, KillWait 30 s) a `scancel` of such a script kills every process at once and no trap runs (jobs 27795 and 27796); with `exec` before the launcher, the launcher gets `TERM` and finishes its stop within KillWait (job 27798). So for `BATCH` on a system with `enableCmdPrefix`, `submitJob` carries `cmdPrefix: exec`, and the systems relay creates set `enableCmdPrefix: true`. Tapis refuses a `cmdPrefix` on any other system (`JOBS_CMD_PREFIX_NOT_ENABLED_FOR_SYSTEM`), so none is sent there. `FORK` never gets one (Tapis runs it under `nohup`). On TACC's own systems, which relay does not create, a `BATCH` cancel has no stop step, and Slurm's cgroup still ends every process. This matters for W7 and W13 too: a Slurm runner that wants a stop on `scancel` must `exec` the process holding the trap.- **Own host (`authn: pki`).** Relay reads the login, home and Slurm partitions through the site's `ssh` alternative, creates `clio-relay.<username>.<site id>` (job working directory `~/.clio-relay/tapis`, one logical queue per partition, carrying the partition's time limit from `sinfo` as `maxMinutes`), makes a new RSA 4096 pair in a private temp file, appends the public key to `authorized_keys` with the comment `clio-relay-tapis:<system id>`, gives the pair to Tapis with `createUserCredential`, and deletes the private file. If Tapis's credential check fails, the key line is taken back out. `unregister-host` removes the credential, the system only when relay made it, and the key line.
- **TACC host (`authn: tms`).** Relay calls `createUserCredential` with `createTmsKeys=true` and makes no key. It never creates or deletes a TACC system.
- **The TACC identity is the device code grant, kept in the keyring (RL48).** `TaccIdentity` implements W6's `IdentityProvider` with `relay_page=False`: the ask carries TACC's own `https` page and the short user code. The device code, the access token and the refresh token stay in the keyring (service `clio-relay`) and are never logged, written to relay's files or returned. Refresh uses the `refresh_token` grant and needs the client key; a refused refresh signs out and the next call asks again. The types are W6's, imported from `clio_relay.identity.base`, and `tacc` is in W6's provider table.
- **Registration runs when a site is saved.** The adapter implements W1's `SiteAdded`: after `site add`, `add-from-catalog`, `edit` and `switch`, `on_site_added` calls `checkUserCredential` and, only if that fails, `register_host` through the site's `ssh` alternative (none for TMS). So running it again changes nothing. Signed out, it refuses `needs_sign_in` and the site stays saved. `register-host` in the driver stays, for the run sheets and for a site whose hook was refused.
- **`maxMinutes` is always sent (reviewing W1's compat edit at 87da62d).** W1's `_max_minutes` sent none for a job with no duration. That is wrong for Tapis: an unset `maxMinutes` falls back to the app's, then the system's, then 10 minutes, and Tapis fails a job past it with `JOB_EXECUTION_MONITORING_TIMEOUT` (Tapis jobs docs, "maxMinutes"). Relay's apps and systems set none, so every job with no duration would be killed at 10 minutes, a hard limit the person's rule forbids. `mapping.max_minutes` replaces it: a duration is sent rounded up to whole minutes; with no duration, a `BATCH` job gets its logical queue's `maxMinutes` (so Slurm's `--time` fits the partition), and otherwise relay sends 525600 (a year), since Tapis has no "no limit" value.
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
- **Tapis job time limit:** `maxMinutes` defaults to 10 and inherits request, then app, then system; a job past it fails with `JOB_EXECUTION_MONITORING_TIMEOUT`. Source: https://tapis.readthedocs.io/en/latest/technical/jobs.html. For Slurm, Tapis turns it into `--time`.
- **Not yet confirmed against the real Tapis** (the stub follows the specs; each is checked in the live rows):
  - `description.eq` in a job search;
  - `getContents`'s `range` semantics, and `listFiles`'s size unit;
  - whether `containerImage` for a `ZIP` app may be a path on the system;
  - that `tapislocal` inputs are not copied;
  - whether `batchLogicalQueues` must be set on a Slurm system;
  - `${apiUserId}` in `effectiveUserId` together with a `loginUser` credential;
  - that Tapis accepts the RSA PEM relay writes.

### Evidence

All on Ares, in `~/clio-relay-rework/clio-relay-w10` with its own venv (`uv sync --extra tapis`, plus `keyrings.alt`), against the stub in `tests/fixtures/tapis_stub` and W2's `fx-ssh` and `fx-slurm`. Ruff, format and pyright are clean on the W10 paths. Tests ran at 350e132 (integration 87da62d, with W6 and W7, merged): conformance 11, identity 11, register 4 and service 17, all 43 passing; the file size and class-in-function ratchets pass (16 and 10).

| Row | State | Command | What was read |
| --- | --- | --- | --- |
| LQ35 | Passed | `pytest tests/relay_tapis/test_tapis_conformance.py` | 11 of 11 with W1's kit at 14e8edd, including `test_exec_bounded`, `test_exec_without_time_limit` and `test_unimplemented_verbs_refuse` asserting `verb_not_implemented` for `exec` and `serve` |
| LQ36 | Passed | `test_tapis_service.py::test_run_survives_a_restart`, `::test_find_reconciles_through_tapis_search` | The service is closed and reopened mid-job; the task completes. Operation states `awaiting_input, submitting, queued, running, completed`. Stub job history `PENDING` to `FINISHED` through Tapis's states. The task result carries the Tapis UUID. The request named `appId clio-relay.alice.tp`, `jobType FORK`, and a `tapislocal://exec.tapis/.../content/<sha256>` input with target `input.txt`, and `maxMinutes` 525600 for a job with no duration. `find` returns the same job |
| LQ37 | Passed | `test_tapis_service.py::test_describe_routes_exec_and_serve_to_ssh` | With W7 merged into integration: `exec` and `serve` route `{adapter: ssh, via: alternative}`; `describe`, `run` and `files` stay on `tapis` |
| LQ38 | Passed | `test_tapis_service.py::test_guarded_site_refuses_a_free_form_run` | The service refuses with `guard_unavailable` until W13's guard is installed; the adapter's own `run` refuses with `policy_refused`, `next_step: package_review`. The stub log has no `/v3/jobs` request |
| LQ39 | Passed | `pytest tests/relay_tapis/test_tapis_identity.py` | 11 of 11. One URL elicitation: `https://tapis-stub.invalid/v3/oauth2/device?client_id=clio-relay-test`, "Sign in to TACC. Open the TACC page, enter the code XXXX and sign in with your TACC account." No password asked. The access token, refresh token, device code and client key are absent from the ask records, relay's log, relay's state files and the MCP transcript. A client without elicitation gets `needs_sign_in` with the same `https` page and resumes once the ask is answered. Throttled polling, an expired code (reported as `expired`, by time or by Tapis, with a new code from the next `begin`), refresh, no client key, a refused refresh, sign-out, and the driver's `lifetimes` and `refresh` (no token in their output) each pass. W6's `device.follow`, given a code Tapis calls expired, asks again with a new code and signs in |
| LQ40 | Passed | `pytest tests/relay_tapis/test_tapis_register.py` | 4 of 4. `Registration(system_id='clio-relay.alice.lq40', created_system=True, authn='pki', login_user='relay', key_installed=True, private_key_deleted=True)`. The stub's credential check logs in to `fx-ssh` with the registered key, and the public key is in `authorized_keys`. An audit hook on `open` during registration saw 5 files, none under `~/.ssh` or the fixture's ssh directory. Removal `{credential: True, system: True, key_line: True}`. A refused credential gives `submit_failed` "Credential check failed: no login" and the key line is gone. On `fx-slurm` the queues are `[compute, debug]`, with `maxMinutes` from `sinfo` (`compute` none, `debug` 30), and the job type is `BATCH`. Through `RelayService.site_added` (the CLI's hook), site `lq40hook` registers once: `{registered: True, system_id: 'clio-relay.alice.lq40hook', authn: 'pki', key_installed: True, private_key_deleted: True, tapis_message: 'credential registered'}`; the second call returns `{system_id, registered: False, credential: 'ok'}` and leaves the key alone |

Also on Ares:
- **The TMS path** (`test_a_tacc_host_gets_a_tms_credential_and_no_key`): `Registration(system_id='stampede3-tapis-stub', created_system=False, authn='tms', key_installed=False)`. Unregistering removes the credential only. The site hook on a TACC host (`vista-tapis-stub`) registers a TMS credential once and changes nothing the second time. Signed out, the hook refuses `needs_sign_in` with TACC's page and makes no Systems call.
- **`maxMinutes`** (`test_max_minutes_never_falls_to_the_tapis_default`, `test_slurm_time_limits_become_minutes`): FORK with no duration 525600; BATCH on a 2880-minute default queue 2880; a queue with no limit 525600; 90.5 s gives 2. Slurm limits `2-00:00:00`, `1:00:00`, `30:00`, `30:01`, `45` read as 2880, 60, 30, 31, 45; `infinite`, `UNLIMITED`, empty and zero as no limit.
- **The CLI hook, signed out** (`clio-relay-next site add-from-catalog ares-tapis` in a scratch config, no OAuth client): the site is saved and `set_up.tapis.refusal` is `invalid_config`, "relay has no OAuth client for the TACC tenant yet; set CLIO_RELAY_TACC_CLIENT_ID (and CLIO_RELAY_TACC_CLIENT_KEY for refresh)". `site switch ares-tapis tapis` runs the hook again with the same answer.
- **The live driver against the standalone stub** (`python -m tests.fixtures.tapis_stub --port 47180`, file keyring in scratch, `fx-ssh` as the host): `status` signed_out; `sign-in` prints the page and the code, then "Signed in." with source keyring, account alice, expiry 30 days out; `register-host` and `describe` report credential ok, job type `FORK` and app `clio-relay.alice.fx-tapis` version `1.0-540fce7250`; `unregister-host` returns all true; `sign-out` returns to signed_out. No token appears in the output.
- **Package mode with the real jarvis-cd 1.8.1** (W13's request; Slurm job 27774 on `ares-comp-03`, debug partition, 1 node, 4 ranks; `sbatch` running `python -m tests.relay_tapis.live_package --root ~/clio-relay-rework/clio-jarvis --include clio_jarvis --include schemas --package clio_jarvis.gray_scott --set nprocs=4 --set ppn=4 --set L=64 --set steps=500 --set plotgap=50`, with the shared venv's `jarvis` and W5's Gray-Scott and MPI on PATH and no `PYTHONPATH`). The adapter's `run` on a guarded site staged the archive (40764 bytes, sha256 `de90efea...a2d555e`, clio-jarvis 2d12dbb) through Tapis Files, and the stub ran `tapisjob_app.sh`. Tapis history `PENDING` to `FINISHED`; status `completed`, exit 0, `relay.exitcode` 0, `stderr` empty. jarvis's `stdout`: "Jarvis initialized at <job dir>/.relay-jarvis", "Added repository: <work dir>/packages/<sha256>/clio_jarvis", "Created pipeline: relay_ca55ce15_..._007", "Added package clio_jarvis.gray_scott as pkg to pipeline", process layout 2x2x1, "Simulation at step 500 writing output step 10". `bpls` on `<job dir>/.relay-jarvis/shared/<pipeline>/executions/<id>/shared/pkg/run/gs.bp` (41 MB): `U` and `V` `10*{64, 64, 64}`, `step` and `time` `10*scalar`. `find ~/.ppi-jarvis -newer <mark made before the job>` printed nothing. The run before it (job 27773), with an archive of `clio_jarvis/` alone, resolved the package but failed in clio-jarvis's contract loader, which reads `schemas/` beside the repo directory; hence the layout note in Decisions.
- **Package mode against a recording `jarvis`** (`pytest tests/relay_tapis/test_tapis_package.py`, 7 of 7 at dae2774 plus the RL45 change): the exact calls, each with `JARVIS_ROOT=<job dir>/.relay-jarvis`, are `init <root>/config <root>/private <root>/shared`, `repo add <work dir>/packages/<sha256>/w10demo`, `ppl create relay_<uuid>`, `ppl append w10demo.hello pkg "greeting=hi there" count=3` and `ppl run`, with no `~/.ppi-jarvis` made in the job's home. A name with no dot fails with exit 64, and an archive with no `w10demo/w10demo` with exit 66, each with its reason in `relay.launcher.log` and no jarvis call. A service package, cancelled through Tapis on `FORK` and on `BATCH`: mode `package-service`, the last calls `ppl start` then `ppl stop`, `CANCELLED`, `relay.exitcode` 0; the `BATCH` request carries `cmdPrefix: exec`. A service whose start fails with 3: `ppl start` then `ppl stop`, `FAILED`, exit 3. A `BATCH` job on a system without `enableCmdPrefix` carries no `cmdPrefix`. The stub now models the three cancels: `FORK` as `pkill -9 -P`, local `BATCH` as `TERM` with a 30 s kill, and real Slurm (`StubState.slurm`) as `sbatch` of a Tapis-like `tapisjob.sh` and `scancel`. With the ratchets, relay_tapis is 50 passing and the ratchets 26, no skips.
- **A real service package, held and cancelled (RL45; clio-jarvis 2d12dbb `clio_jarvis.pvserver`, ParaView 6.2.1, jarvis-cd 1.8.1, `--set env_script=~/clio-relay-rework/paraview/pv-env.sh --set port=0 --set hold=false`, driver `python -m tests.relay_tapis.live_package ... --service --ready '**/pvserver-ready.json'`).** The driver waits for pvserver's ready record, calls the adapter's `cancel` (Tapis `cancelJob`), and waits for the launcher.
  - `FORK`, the driver inside `srun -p debug` on `ares-comp-03` (Slurm job 27802): pvserver ready on port 33279; Tapis history `PENDING` to `RUNNING`, then `CANCELLED`; jarvis's `stdout` ends "Stopping pipeline: relay_24429f2c_..._007", "[STOP] BEGIN", "[STOP] END"; `stderr` is only bash's "Killed sleep 60" (the `pkill -9 -P`); `relay.exitcode` 0; `pvserver-stop.json` has `reason: signal`, `signal: SIGTERM`, `server_exit_code: -15`.
  - `BATCH`, real Slurm (Slurm job 27806 on `ares-comp-03`, debug partition, `--time=30`): `tapisjob.sh`'s last line is `exec ./tapisjob_app.sh <base64 spec>`; pvserver ready on port 43729; `scancel`'s `TERM` reached the launcher's trap, and jarvis's `stdout` ends with the same stop lines; `stderr` empty; status `CANCELLED`; `relay.exitcode` 0; the stop record has `reason: signal`, `signal: SIGTERM`.
  - Afterwards, `pgrep` on `ares-comp-03` found no launcher, pvserver or supervisor of either job.
  - For W13: with `state_dir` left empty, pvserver's records went to the job directory (jarvis's working directory), not `{shared_dir}/pvserver` as the menu says.

### Run sheets for the live rows

All three are **pending**, never passed. They wait on the owner blockers below. The person signs in and supplies credentials; W10 runs the rows. Before each run: `ssh ares`, `cd ~/clio-relay-rework/clio-relay-w10`, `git pull`, `~/.local/bin/uv sync --extra tapis`. The person sets the OAuth client in the shell themselves (`export CLIO_RELAY_TACC_CLIENT_ID=...` and `CLIO_RELAY_TACC_CLIENT_KEY=...`); neither is pasted into a chat. `T=python -m clio_relay.adapters.tapis`; `R=clio-relay` (the new CLI; it was `clio-relay-next` before the cutover).

**Sign-in (shared by all three).** `S` is the row's site: `chameleon-tapis` or `ares-tapis`.
1. `uv run $R site add-from-catalog $S`. For `chameleon-tapis`, then set the person's SSH profile for the node: `uv run $R site edit $S '{alternatives: {ssh: {ssh_profile: <profile>}}}'` (`ares-tapis` already names `ares`). Check `ssh <profile> hostname` first. Signed out, each prints `set_up` with a `needs_sign_in` refusal, and the site is saved; with no OAuth client in the shell, the refusal is `invalid_config` naming the two variables instead.
2. `uv run $T sign-in --site $S`. It prints TACC's page and a code.
3. The person opens the page, enters the code, and signs in with their TACC account and second factor. The driver prints "Signed in."
4. `uv run $T status --site $S`. Record `account` and `expires_at`, never a token.
5. `uv run $T lifetimes --site $S`. It prints, from the tokens' claims, when each token expires and the seconds left, and no token text. Record the access lifetime (expected 30 days) and **the refresh-token lifetime**, the ledger fact still open.
6. `uv run $T refresh --site $S`. Expect "A new access token was issued." This confirms the refresh grant with relay's client key.

**LQ-L14, `chameleon-tapis`.** Needs O3, O4, O5.
1. Sign in as above with `S=chameleon-tapis`.
2. `uv run $R site switch chameleon-tapis tapis` (tapis is already in use, so this only runs the hook again). Its `set_up.tapis` is the registration. Record it whole (system id, login user, key installed, private key deleted, Tapis's message). Run it a second time and record `registered: false, credential: ok`. If it shows a refusal, `uv run $T register-host --site chameleon-tapis` gives the same record with the full error.
3. `uv run $T describe --site chameleon-tapis`. Expect credential ok and job type `FORK`.
4. Start relay's MCP server on that site list and, from an MCP client, call `run` with `/bin/sh -c "hostname; date; echo relay-l14 > out.txt"`. Poll the task to completion.
5. Record the Tapis job UUID from the task result, and the time from `submitting` to `running` from the operation's history.
6. Read `out.txt` back with the `files` tool and check it says `relay-l14`.
7. Keep the system for LQ-L15. After LQ-L15: `uv run $T unregister-host --site chameleon-tapis`, then check `grep clio-relay-tapis ~/.ssh/authorized_keys` on the node is empty.

**LQ-L15, the TACC demo on `chameleon-tapis`.** Needs LQ-L14, and on the node: `jarvis` on PATH (jarvis-cd 1.8.1; the launcher makes a `JARVIS_ROOT` of the job's own, so nothing needs adding to the node's `~/.ppi-jarvis`), ADIOS2 and MPI for the solvers. The package archive is W13's, holding `clio_jarvis/` and `schemas/` (W5, O1). Needs W14's ParaView tab (and W15's follow) on the desktop side.
1. From the agent, ask: "Run Gray-Scott on Chameleon through Tapis and show me the field."
2. Expect a package `run` (`clio_jarvis.gray_scott`, version and SHA-256), a Tapis job UUID, the outputs read back through Tapis Files, and the BP5 output opening in the ParaView tab.
3. Repeat with Xcompact3d (`clio_jarvis.xcompact3d`, TGV).
4. Record both UUIDs, both submit-to-running times, and a screenshot of each field, in the science log.

**LQ-L17, `ares-tapis`.** Needs O3, O4, O6.
1. Sign in as above with `S=ares-tapis`.
2. `uv run $R site switch ares-tapis tapis` (tapis is already in use, so this only runs the hook again). Record `set_up.tapis`, the registration, including the queues (Ares's Slurm partitions) and each queue's `maxMinutes`. If it shows a refusal, run `uv run $T register-host --site ares-tapis` for the full record.
3. **The contract exception.** If Tapis's credential check cannot log in to Ares from TACC, the registration refuses `submit_failed` with Tapis's own message, and the key line is removed. Record that message verbatim as the row's result; LQ-L14 then carries the adapter. Stop here.
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

- **W13:** the package archive must hold the repo's `schemas/` beside `clio_jarvis/`, or clio-jarvis's contract loader fails at `ppl append` (job 27773).
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

- **Gate on Ares** at `702356e` (integration `87da62d` merged, with W6 and optional duration), in the clone `~/clio-relay-rework/clio-relay-w11` with its own venv (`uv sync --extra globus --extra testing`): the four W11 modules gave exit 0, 102 passed and none skipped; ruff is clean and pyright reports 0 errors. The three new tests cover a job with no duration: globus sends no walltime, iri sends no `duration`, and sky prices it with no amount, walltime or node-hours, without calling SkyPilot.
- **Gate on Ares** at `08c2880` (integration `14e8edd` merged, W6's identity API adopted):
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
- **Answered by W6 (4bad7cb on `-w6`), and adopted:** the `Token` and `IdentityStatus` shapes, `shared().get`, and cloud keys as `TokenStore` secrets raised as `keyring:<name>`. W6 is merged at integration `87da62d`, and the gate there ran against its code.
- **Settled by W1 at `87da62d`:** a job's duration is optional. W11 reviewed and kept W1's edits: globus sends no walltime (the endpoint's default applies), iri omits `duration`, and sky returns a `Charge` with no amount, so the spend gate asks.
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

Branch `jaimecernuda-relay-rework-w13`. Rows: RL24 to RL26, RL42 (relay side), the guard half of RL45, LQ16 to LQ21, and the run sheets for LQ-L10 and LQ-L13. Page: [docs/guarded-sites.md](../guarded-sites.md).

Files: `src/clio_relay/guard/` (`policy.py`, `reviews.py`, `execlist.py`, `warm.py`, `config.py`), `src/clio_relay/jarvis_site.py` and `tests/guard/`. The guard is installed through the `clio_relay.hooks` entry point `guard`, so W1's `guard_unavailable` stand-in no longer answers once it is installed.

### Decisions

- **A guarded site runs only reviewed JARVIS packages (RL24).** `check_run` refuses an `executable` with `policy_refused`, `next_step: package_review` and the way forward: build an archive with `python -m clio_relay.jarvis_site archive <repo dir> <repo.package> --stage`, then call `run` with `package: {name, version, sha256, parameters}`. A package job carrying `environment` or `launcher` is also refused, because its settings travel as package parameters. An open site is never checked.
- **A review is pinned by the archive's SHA-256, per site.** An unreviewed hash asks once (`kind: approve`). The binding is `{kind: package_review, package, version, package_hash, source, files}`. The question shows the package and version, the hash, the git source (tag, commit, path), the archive's path on the relay host, and each file with its own hash and size. A yes is harvested from the answered asks into `<state>/guard/reviews/<site>/`. A changed byte is a new hash and a new review. There is no withdrawal command yet; deleting the review file withdraws it.
- **The archive is byte-stable and carries what the repo reads beside it.** It is a gzip tar with mtime 0 and fixed modes, holding `<repo>/<repo>/<package>/…`, the repo's `__init__.py` files, `_contract/` and the repository's `schemas/` (clio-jarvis's `_contract/spec.py:42` reads `<repo dir>/../schemas`; W10's job 27773 failed without it). f54f171.
- **`prepare` lowers a reviewed package to a `/bin/bash` runner job.** The archive is an input named `.relay-package/<sha256>.tar.gz`. The runner checks the name is `<repo>.<package>` (exit 64), the archive's hash against the reviewed one (65), the `<repo>/<repo>` layout (66) and that `jarvis` is on PATH (69), the same codes as W10's tapis launcher. Then, with `JARVIS_ROOT=<job dir>/.relay-jarvis`, it runs `jarvis init`, `repo add`, `hostfile set` (from Slurm or PBS), `ppl create relay_<digest12>` and `ppl append <name> pkg k=v…`. The site's own `~/.ppi-jarvis` is never read or changed. An adapter with `runs_packages` (Tapis) gets the package spec unchanged and runs it itself.
- **A service package holds until stopped (RL45, W9's finding).** `prepare(…, service=True)` (W1's `_start_service`) picks the service tail: `jarvis ppl start`, then a `sleep 60 & wait` loop; a `TERM`, `INT` or `HUP` trap, or a failed wait (a `pkill -9 -P` of the sleep), runs `jarvis ppl stop`. A failed `ppl start` runs `ppl stop` and exits with start's code. The sleep pid is kept in a variable, so `set -u` never meets a bare `$!` (W10 5681806). An application package keeps `exec jarvis ppl run`. For `runs_packages` adapters, `prepare` returns a copy with `package.service = True` (W1's `PackageRef.service`), so W10's launcher picks its `package-service` mode with the same tail. beefba3, 790824e, 02e1481.
- **`exec` on a guarded site runs only a read-only argument list.** `argv[0]`, named bare, must be on the list: `cat df du file find grep head hostname id jarvis lfs ls md5sum module nproc pbsnodes pwd qstat quota readlink realpath sacct sacctmgr scontrol sha256sum sinfo sprio squeue srun sshare stat tail uname wc whoami`, plus the site's `read_only` in `guard.yaml`. Some are checked by argument: `find` without `-exec`, `-delete` or `-fprint*`; `file` without `--compile`; `scontrol show`; `sacctmgr show|list`; `lfs quota|df|getstripe`; `pbsnodes` without the options that change a node; module queries; JARVIS reads (`ppl list|status|print|path`, `repo list`, `pkg readme|help|path`, `execution get|list|progress|artifacts`). `srun` runs only as `srun --jobid=<a warm allocation relay holds on this site> --overlap <a read-only command>`. A path such as `/tmp/x/ls` is refused, and since the list is matched against argv with no shell, a shell, a redirect or a pipe never runs.
- **The spend gate prices every job and asks unless the site's `spend.ask` lets it go (RL25).** `price` builds a `Charge` from the spec (queue, account, nodes, walltime, node-hours), then asks the routed adapter's own `price(spec)` when it has one, which is `SkyAdapter` (USD) and `SshAdapter`. An unknown walltime gives `walltime_s`, `node_hours` and `amount` null, and an unknown cost always asks unless `spend.ask` is `never`. Relay never sets a walltime. `hooks.Charge` stays without a currency field: `unit` already says `USD` or `node-hours`.
- **A warm allocation is a holder job (RL26).** `run` with `executable: clio-relay-warm` and no arguments goes through the spend gate like any job and is submitted as a `/bin/sh` holder. Every 30 s it takes the latest use from a running step (`squeue -s`), the end of the latest finished step (`sacct`, so a short `srun` between two looks counts) and the job directory's `.relay-warm-active` marker. Once nothing has used it for `warm_idle_s` (600 by default, at least 60) and no step runs, it exits and the scheduler frees the nodes. It never stops a running step, and a scheduler that does not answer counts as busy. Without Slurm only the marker counts. The finished-step end time was found missing live (Slurm 284) and added in f1ccc62.
- **RL42 is `add_clio_jarvis(adapter, url=, tag=, commit=, base=, jarvis=)`.** Through the adapter's `exec`, it clones the tag shallowly into `<base>/<tag>` (default `~/.clio-relay/clio-jarvis`), checks the tag's commit when one is given (exit 65 otherwise), and runs `jarvis repo add <dir>/clio_jarvis` unless `jarvis repo list` already shows it. It returns `{tag, commit, repo, added}`. W1 calls it from the bootstrap after W8's move; W13 edits no bootstrap file.
- **Settings per site** live in `<config>/guard.yaml` under `sites.<id>`: `jarvis` (the command on the site), `read_only` and `warm_idle_s`. The file is read on every check.

### Facts

- **Slurm cancel on Ares** (Slurm 21.08.5, `proctrack/cgroup`, KillWait 30 s; job 27814): a plain `scancel` sends `TERM` to the batch shell only. A child in another process group got nothing, then `SIGKILL` at KillWait. So on the `ssh` path the runner's trap is reached only through W7's process-group forwarding; it cannot `exec` into place, because psij's `single_launch.sh` runs the command as `"$@"`.
- **W7's forwarding had two gaps, found with RL45 (R10, routed to W7).** (1) It wrapped only the first line of the launch, so a multi-line argument (the runner, or any multi-line `bash -c`) got the wrapper inside its quotes: no forwarding, and the script's first line ran as a background no-op (job 27812: no `signalled` marker, no `.ec`, no `ppl stop`). (2) After passing `TERM` on, the batch shell exited as soon as `single_launch.sh` died, and Slurm killed the runner mid-stop (job 27820, fix 1 only: `signalled` TERM, no stop record, the supervisor pid file left). Both are fixed in W7's 60697f7 (integration faf00f8) and proven live in Slurm 27830.
- **NFS on Ares** (`/mnt/common`, NFSv3): a file written on a compute node can stay invisible to `[ -f ]` on the login node for up to 120 s; the drivers list the directory first.
- **clio-jarvis v0.1.2** (W5, 8f69e37, local on Ares, not pushed): pvserver has no `max_lifetime` (absent means until stopped), `hold` defaults to false, `ppl start` returns once pvserver is ready, and its records land in `{shared_dir}/pvserver`.

### Evidence

All on Ares: relay in `~/clio-relay-rework/clio-relay-w13` with its own venv, W2's `fx-slurm` (Slurm, ports 47100 to 47107) for LQ16 to LQ21 and RL42, and the real Ares Slurm for RL45. The client is `tests/guard/guard_live.py`, a plain MCP client over stdio that drives `clio-relay-next mcp` and answers asks only through W6's page (GET then the CSRF-bound POST). Sites: `fxg` (guarded, no spend setting, so unknown cost asks), `fxk` (guarded, `spend.ask: always`, `defaults.duration` 5 min, account `fixture`) and `ares13` (guarded, `spend.ask: never`, queue `debug`). The proof ran on integration code (1a8c20b and later) with no scratch patch; the scratch patch the coordinator allowed was never applied. The test package is `w13demo.hello` in a local git repo with tags v0.1.0 (`5b2beb81…`) and v0.1.1 (`da0a07af…`).

| Row | State | Handles | What was read |
| --- | --- | --- | --- |
| LQ16 | Passed | `run` `/bin/sh` on `fxg` | `policy_refused`, no operation, no ask, `next_step: package_review`, and the way forward (build the archive, call `run` with `package`). `squeue` empty |
| LQ17 | Passed | op_e66e66821a1acb03db338269, Slurm 283; op_5e2b56723498563f5e6087ed | The review question showed `w13demo.hello 0.1.0`, the hash `5b2beb81…`, the source (tag v0.1.0, commit c004b4e, path w13demo), the archive path and three files with their hashes. After the yes and the spend yes, 283 completed with jarvis's `[START]`/`[STOP]` lines and `wrote hello.txt`. A second run of the same hash (op_bb33198445283954505eb516) asked only the spend question. The edited package (v0.1.1, `da0a07af…`) asked for a new review |
| LQ18 | Passed | `exec` on `fxg` | `rm …` and `sh -c …` refused with `policy_refused`, naming the program and listing the read-only programs; `squeue` returned its listing |
| LQ19 | Passed (page half by W6; R8 open) | op_a394bab1ae615b712863f109 on `fxk` | After the review, the spend question: "It may use up to 0.0833 node-hours (1 node(s) for 5 min)", with queue `debug`, account `fixture`, 1 node and 300 s in the charge. A no gave `cancelled`, reason `declined`, `native_id` null and an empty `squeue`. The unknown-cost question (op_bb33…, `fxg`): "sets no walltime, so its duration and cost are unknown"; a no submitted nothing |
| LQ20 | Passed | op_5e2b56723498563f5e6087ed | With nobody answering, `run(resume=)` returned `needs_approval` with the same ask, the operation stayed `awaiting_input`, and `squeue` showed no job |
| LQ21 | Passed | op_bcc2a45ccc21c02f34a2aeba, Slurm 305 | The warm request asked the spend question; after the yes, 305 ran. `exec srun --jobid=305 --overlap hostname` returned `fx-slurm`, and `squeue` run the same way listed step 305.1 inside it. `srun … rm …` and `srun --jobid=1 …` were refused. The last step, 305.2, ended at 15:05:45. The holder logged "idle for 617 s; released at 2026-11-18T15:16:02Z", and `sacct` showed 305 COMPLETED 0:0, elapsed 00:14:34. The first try (Slurm 284) found the gap fixed in f1ccc62 |
| RL42 | Passed (local tag; O1 open) | `add_clio_jarvis` on `fxg`, url a bare clone of W5's local repo, tag v0.1.0 | First call `{commit: 2d12dbb…, repo: /home/relay/.clio-relay/clio-jarvis/v0.1.0/clio_jarvis, added: true}`; the second `added: false`; a wrong commit `submit_failed` exit 65 "tag v0.1.0 is 2d12dbb…, not 0000…". `jarvis repo list` showed `clio_jarvis` once. Against `iowarp/clio-jarvis` it waits on O1 |
| RL45 (guard half) | Passed on integration 36eb92c, no scratch patch | op_11b4bae3067c1f80c20a1d5c, Slurm 27830 on ares-comp-03 (earlier op_a0a368ea466173130a7d917a, Slurm 27819, with the scratch patch) | Re-proof on 36eb92c (W7's 60697f7): `submit.sh` puts ` &` after the whole multi-line runner and drains the group. Ready at port 51383 with `max_lifetime: null`, held 2 min with the port open, then an adapter cancel: `signalled` TERM, `[STOP] BEGIN/END`, `pvserver-stop.json` reason `signal` (SIGTERM), no supervisor pid file, `.ec` 143, final `cancelled`. Driver `rl45c.py ares13 <digest> v0.1.2 120 <remote>`; `jarvis` was `~/.local/bin/jarvis` (jarvis_cd 1.8.1), since a `uv sync` of the shared venv dropped its `jarvis`. First run, 27819: | clio-jarvis v0.1.2 `clio_jarvis.pvserver`, archive `7095bef8…` (16 files with `schemas/`). Unreviewed `prepare` refused `policy_refused`; reviewed, it lowered to `/bin/bash`, mode `service`. Ready with `max_lifetime: null`; the port was open from the node through each poll. (The earlier 27812 held for more than 10 minutes.) An adapter cancel gave `signalled` TERM, stdout `Stopping pipeline … [STOP] BEGIN/END`, `pvserver-stop.json` reason `signal`, the supervisor pid file gone, `.ec` 143, final `cancelled`, with W7's two fixes as a scratch patch |

Suite on Ares at 02e1481: ruff, format and pyright are clean on the W13 paths. `tests/guard` passes 80 of 80. Against base 1a8c20b, the only new failures in the full suite are W1's two guard-absent tests (R9) and two `test_wheel_record_closure.py` tests that pass alone and together with `tests/guard`.

### Run sheets for the person rows

Record for each: the date, relay's commit, the site entry as used, the review and spend screenshots, the handle (`native_id`), the states seen, and the exit code. Write the result in W16's summary.

**LQ-L10. Xcompact3d into the tab, on Polaris (guarded).**

Needs: an ALCF account with a Polaris allocation, LQ-L2's Globus sign-in, the clio-jarvis release on Polaris (RL42; the real remote waits on O1), a `jarvis` on Polaris with Xcompact3d built, and the ParaView tab (W14, W15).

1. Add the catalog site `polaris` (guarded, `spend.ask: always`). Set `defaults.account` to the project. In `guard.yaml`, set `sites.polaris.jarvis` to the jarvis on Polaris.
2. From a clio-jarvis checkout at the release tag, run `python -m clio_relay.jarvis_site archive <checkout>/clio_jarvis clio_jarvis.xcompact3d --stage`. Record the `sha256` and the file count.
3. Ask the agent to run Xcompact3d on Polaris and show the field. Check that it calls `run` with `package: {name: clio_jarvis.xcompact3d, version, sha256, parameters}`, not an executable.
4. Review question: check the hash, the tag, the commit and the file list against step 2, then approve it.
5. Spend question: check queue `debug`, the account, the nodes, walltime 1 h (the catalog's `defaults.duration`) and the node-hours, then approve it.
6. Watch queued, running. Open the tab as the run writes. Record the time from a step's file being written (its mtime, through `files`) to the tab showing it.
7. When it completes, read `stdout` for jarvis's `[START]`/`[STOP]` lines. Run it again with one parameter changed: no review question, only the spend question. Edit one file in the package, rebuild: a new hash and a new review question.

**LQ-L13. Offline pvserver on Polaris (guarded).**

Needs: LQ-L10's setup, a dataset already on Polaris, the `pvserver` package (clio-jarvis v0.1.2 or later) with ParaView on Polaris, W9's `serve` (its module is not yet in integration) and the tab.

1. Build and stage `clio_jarvis.pvserver` as in LQ-L10 step 2. Record the hash, and check that `schemas/` is listed.
2. Ask the agent to open `<dataset path on Polaris>` in ParaView. Check that it uses `serve` with the package; on a guarded site a free-form `pvserver` is refused.
3. Approve the review question, then the spend question (debug, 1 node, 1 h).
4. The service holds: no `max_lifetime`, until it is stopped. The tab connects through the port relay returns and shows the dataset.
5. Check that the dataset is not copied off the site: relay's content store and any `files` transfers in relay's log hold no part of it.
6. Stop the service. Record that the port closes, `pvserver-stop.json` exists with reason `signal`, and PBS shows the job gone. On PBS, check that `qdel` reaches the runner: the stop record is the proof.

### Requests and open items

- R6, R7 (W1): `prepare(…, service=)` and `PackageRef.service`. Done at dae2774.
- R8 (W6): the approval page's rows when the walltime and the cost are unknown (the ask's message carries them). Open.
- R9 (W1): `tests/relay_next/test_server.py::test_refusals_are_typed` and `test_service.py::test_guarded_site_refuses_without_a_guard` expect `guard_unavailable`, but with the guard installed it answers `policy_refused`. Force the stand-in in those tests. Closed: W1's `stub_guard` fixture, b4c215f.
- R10 (W7): the multi-line launch in `forward_signals` and the group drain. Closed: W7's 60697f7 (merge faf00f8), proven live in Slurm 27830.
- O1: `iowarp/clio-jarvis` on GitHub, for RL42 against the real remote.
- W9's `clio_relay.serve` module, for RL45 through `serve` and LQ-L13.
- A review withdrawal command (today: delete the review file).
- The full suite on Ares hung at exit in pytest's temp cleanup after writing its report (job 27790).

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
