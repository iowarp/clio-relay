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
- **The asks command group comes from W6 when installed.** `clio-relay-next asks` mounts `clio_relay.cli_asks.app`. Without it, the built-in `list` and `answer` remain.
- **Bounded commands leave nothing behind.** `run_bounded` reads both pipes to their end after a timeout kill. When its caller is cancelled, it kills the process group before it re-raises. This fixed a leaked subprocess transport ("Event loop is closed") from the local conformance tests, which was already present at M1. The relay_next suite now runs with `PytestUnraisableExceptionWarning` as an error.
- **W11 merged.** `jaimecernuda-relay-rework-w11` at b41c756 is merged as aa9b7b1. Its adapter pages are in the docs index in `README.md`.
- **Routing skips an adapter whose settings do not build (RL5, coordinator decision).** An alternative that fails at build time with `invalid_config`, such as Aurora with no endpoint or iri with no `resource_id`, is skipped, and routing moves on to the next one. The skip is part of the route, as `skipped: [{site, adapter, reason, message}]`. It shows in `describe`'s routes, in an operation's `result["skipped"]` and in an `exec` result, so the person sees why it was not used. When every candidate fails, the call refuses with `invalid_config`, and the refusal lists each adapter's reason. Any other refusal at build time is still returned as it is. Routing builds each candidate through the service's adapter cache (`RelayService.route` and `site_routes`).
- **`exec` has no hard time limit (coordinator decision, the person's rule).** `timeout_s` is optional and has no default or ceiling. Without it the command runs until it ends. After 30 s with no output on either stream, relay reports it as quiet, then reports again after waits that double (30, 60, 120 s and so on, capped at 600 s) while it stays quiet. Each report is a progress notification on the call (not a log message, since logging is deprecated at `2026-07-28`): "no output for N s; the command is still running; relay checks again in M s; cancel the call to stop it". Cancelling the call kills the command's process group. A caller may still pass `timeout_s`, and then the command is killed at that limit as before. Every adapter's `exec` must accept `timeout_s=None` and an optional `on_stall`.
- **The conformance kit no longer skips.** For a verb the adapter does not implement, each kit test asserts the typed `verb_not_implemented` refusal and passes, instead of skipping. A new kit test runs `exec` with no time limit.

### Published interfaces (M1)

Owned by W1, not frozen. A change goes through W1, which reruns every consumer's contract tests first.

- Adapters (`clio_relay.adapters.base`): the `Adapter` Protocol (`name`, `Config`, `describe`, `run(spec, operation_id)`, `status`, `cancel`, `find(operation_id)`, `exec(command, *, timeout_s=None, max_output_bytes, on_stall=None)`, `files`, `serve`), `BaseAdapter[ConfigT]`, `SiteInfo`, `SignInNeed`, `AdapterContext`.
- Jobs (`clio_relay.jobs`): `JobSpec` (PSI/J names, `JobResources.to_psij()` builds `ResourceSpecV1`), `JobResources`, `JobAttributes`, `InputRef`, `PackageRef`, `ServiceSpec`, `ServicePort`, `ServiceLifetime`, `canonical_digest`, `JobHandle`, `JobStatus`, `JobState`, `ExecResult`, `StallReport` and `STALL_S`, `Endpoint`, `EndpointPort`, `FileEntry`, `FileAccess`, `OutputRef`.
- Records: `OperationRecord`, `OperationState`, `RecordStore`. Layout: `<state>/operations/`, `<state>/asks/`, `<state>/sites/<site>/<adapter>/`, and `sites.yaml` in the config directory.
- Content store: `ContentStore` at `<state>/content/cas/sha256/<first two hex>/<digest>`.
- `RefusalReason`, 24 typed reasons: `verb_not_implemented`, `site_not_found`, `site_exists`, `unknown_adapter`, `adapter_unavailable`, `invalid_config`, `invalid_spec`, `invalid_arguments`, `guard_unavailable`, `policy_refused`, `needs_approval`, `needs_sign_in`, `declined`, `interactive_auth_required`, `operation_not_found`, `ask_not_found`, `approval_mismatch`, `input_missing`, `path_not_found`, `path_exists`, `not_cancellable`, `submit_failed`, `unreachable`, `timeout`.
- Hooks (`clio_relay.adapters.hooks`): `Ask`, `AskProvider` (`choose`, `approve`, `url`) for W6; `GuardHook` (`check_run`, `check_exec`, `price`), `Charge` and `GuardDecision` for W13; `ServeHook` (`start`, `status`, `stop`, `attach`, `ui_data`) for W9; the optional `MountsPages` (`mount`) for an ask provider's pages.
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

## W7. The ssh adapter and scheduler jobs

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
- **The TACC identity is the device code grant, kept in the keyring (RL48).** `TaccIdentity` implements W6's `IdentityProvider` with `relay_page=False`: the ask carries TACC's own `https` page and the short user code. The device code, the access token and the refresh token stay in the keyring (service `clio-relay`) and are never logged, written to relay's files or returned. Refresh uses the `refresh_token` grant and needs the client key; a refused refresh signs out and the next call asks again. The types are mirrored from W6's published interface until W6 merges, and become imports then.
- **No hard limits.** Jobs, sign-ins and transfers have none. The device code's own 600 s expiry is Tapis's, and an expired code fails the step so a new one is issued.
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

All on Ares, in `~/clio-relay-rework/clio-relay-w10` with its own venv (`uv sync --extra tapis`, plus `keyrings.alt`), against the stub in `tests/fixtures/tapis_stub` and W2's `fx-ssh` and `fx-slurm`. Ruff, format and pyright are clean on the W10 paths. Tests ran at c7cdaf3 (integration 14e8edd merged): conformance 11, identity 9, register 3 and service 4 of 5, the fifth being LQ37, which needs W7. In a scratch worktree with W7 (b44a2a8) merged in, all 28 pass.

| Row | State | Command | What was read |
| --- | --- | --- | --- |
| LQ35 | Passed | `pytest tests/relay_tapis/test_tapis_conformance.py` | 11 of 11 with W1's kit at 14e8edd, including `test_exec_bounded`, `test_exec_without_time_limit` and `test_unimplemented_verbs_refuse` asserting `verb_not_implemented` for `exec` and `serve` |
| LQ36 | Passed | `test_tapis_service.py::test_run_survives_a_restart`, `::test_find_reconciles_through_tapis_search` | The service is closed and reopened mid-job; the task completes. Operation states `awaiting_input, submitting, queued, running, completed`. Stub job history `PENDING` to `FINISHED` through Tapis's states. The task result carries the Tapis UUID. The request named `appId clio-relay.alice.tp`, `jobType FORK`, and a `tapislocal://exec.tapis/.../content/<sha256>` input with target `input.txt`. `find` returns the same job |
| LQ37 | Passed with W7 | `test_tapis_service.py::test_describe_routes_exec_and_serve_to_ssh` | In a scratch worktree with `jaimecernuda-relay-rework-w7` (b44a2a8) merged: `exec` and `serve` route `{adapter: ssh, via: alternative}`; `describe`, `run` and `files` stay on `tapis`. On the integration branch alone it fails with `unavailable: {ssh: adapter_unavailable}`, because the ssh adapter is not merged |
| LQ38 | Passed | `test_tapis_service.py::test_guarded_site_refuses_a_free_form_run` | The service refuses with `guard_unavailable` until W13's guard is installed; the adapter's own `run` refuses with `policy_refused`, `next_step: package_review`. The stub log has no `/v3/jobs` request |
| LQ39 | Passed | `pytest tests/relay_tapis/test_tapis_identity.py` | 9 of 9. One URL elicitation: `https://tapis-stub.invalid/v3/oauth2/device?client_id=clio-relay-test`, "Sign in to TACC. Open the TACC page, enter the code XXXX and sign in with your TACC account." No password asked. The access token, refresh token, device code and client key are absent from the ask records, relay's log, relay's state files and the MCP transcript. A client without elicitation gets `needs_sign_in` with the same `https` page and resumes once the ask is answered. Throttled polling, an expired code, refresh, no client key, a refused refresh, sign-out, and the driver's `lifetimes` and `refresh` (no token in their output) each pass |
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
- **W6:** with `relay_page=False`, the identity driver must call `poll` at `poll_interval_s` until done, and re-ask with a new code when `poll` reports the code expired.
- **Adding a catalog site does not register the host yet.** Until an add-site hook calls `register_host`, the run sheets call `register-host` by hand.
- **LQ37 passes only once W7 merges.**
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
- **Credentials come from W6's identity layer.** `tokens.provider("globus")` calls `clio_relay.identity.get("globus")` when that module exists. Until W6 merges, it uses `GrantFileProvider`, a stub with the same shape (`async token(resource_server=...)`, `status()`). The stub reads the grant file named by `CLIO_RELAY_GRANT_FILE_GLOBUS`. The adapters never start a sign-in. With no token, or an expired one, they refuse `needs_sign_in`, and `sign_in_needed()` names the provider.
- **globus: one Compute task per job, cancelled with a marker.** The task is `relay_task`, a self-contained function that uses only the standard library, because Compute ships functions by source.
  - The task writes the inputs and `relay-state.json` into `<job_root>/<operation id>/`. It runs the command in its own process group and polls for a `relay-cancel` marker.
  - `cancel` sends a second task that writes the marker, because the Compute service cannot cancel a submitted task.
  - A task the endpoint retries finds the final record and does not run the job again.
  - Submission uses `create_batch` and `batch_run`, and polling uses `get_task`. The SDK `Executor` is never opened, since it holds a long-lived connection.
- **globus: `user_config_keys` maps the job onto the endpoint's template.** The defaults, `account`, `queue`, `walltime` and `nodes_per_block`, are ALCF's keys. Each facility names them differently.
- **globus `files` is Globus Transfer between two collections.** One is the site's; the other is on relay's own machine, for example Globus Connect Personal. A transfer that keeps faulting is cancelled and refused. A transfer that is making progress has no time limit.
- **globus: the person's own endpoint starts through another adapter.** `start` names `worker` or `ssh` and the start command, for Aurora. It goes through W1's `exec`, which requires `timeout_s` (default 900 s here). The start command daemonizes and returns. See Open.
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
- **sky cloud keys come from the keyring.** `credentials.env` maps variables to keyring entries under the service `clio-relay`, such as `AWS_ACCESS_KEY_ID: aws-access-key-id`. A missing entry refuses `needs_sign_in` with the provider. Without `credentials`, SkyPilot uses the cloud profiles already on the machine.
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

- **Gate on Ares** at `3015e81`, in the clone `~/clio-relay-rework/clio-relay-w11` with its own venv (`uv sync --extra globus --extra testing`):
  - `uv run pytest -q -rs tests/relay_next/test_globus_adapter.py tests/relay_next/test_iri_adapter.py tests/relay_next/test_sky_adapter.py tests/relay_next/test_w11_catalog.py`: 82 passed, 5 skipped. The skips are the conformance kit's own: `exec` is not implemented on the four targets (globus, iri v2, iri v1, sky), and `files` is not implemented on sky.
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
2. Add a site with `adapter: sky`, `infra: aws`, `cpus: 2+` and the keyring entries `AWS_ACCESS_KEY_ID: aws-access-key-id` and `AWS_SECRET_ACCESS_KEY: aws-secret-access-key`.
3. Without the keys, `describe` reports `skypilot_version` and the site shows as needing sign-in. Type the two keys on relay's page.
4. Check that they are in the OS keyring under the service `clio-relay`, and not in any file.
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
- **Endpoint start uses `exec`, whose Protocol requires `timeout_s`.** `StartConfig.timeout_s` defaults to 900 s and is settable. `globus-compute-endpoint start` daemonizes and returns, so the bound does not limit jobs. If the coordinator reads it as a hard limit on something the person waits on, W1's `exec` needs a stall-only form.
- **Globus Compute status strings** beyond those in SDK 4.21.0's documentation are not verified live. Unknown strings map to queued. LQ-L2 records the strings it sees.
- **ALCF's IRI Globus scope was not found** in ALCF's pages. Until it is, ALCF IRI uses `token_command`.
- **The IRI download format varies by facility.** Binary round trips are checked in LQ-L5 step 4.
- **The DeltaAI template keys are unpublished.** LQ-L8 step 1 checks them.
- **Aurora's endpoint setup is a one-time person step.** Relay starts the endpoint afterwards; it does not create it.
- **Requests to W1:**
  - Add `docs/adapters/{globus,iri,sky}.md` to the docs index.
  - RL5 routing cannot fall back to an alternative on a config-level refusal: Aurora with no `endpoint`, or `iri` with no `resource_id`, refuses `invalid_config` at build. Decide whether routing skips such an alternative or reports it.
  - O11's text says "paid cloud account for LQ-L8". The cloud row is LQ-L6, and LQ-L8 is DeltaAI.
- **Requests to W6:**
  - Confirm the `Token` shape (`access_token`, `expires_at`) and whether `status()` is sync or async. `tokens.py` accepts either.
  - Confirm the keyring naming for cloud keys: service `clio-relay`, entries such as `aws-access-key-id`. Relay's page should write those entries.
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
