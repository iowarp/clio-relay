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

### Published interfaces (M1)

Owned by W1, not frozen. A change goes through W1, which reruns every consumer's contract tests first.

- Adapters (`clio_relay.adapters.base`): the `Adapter` Protocol (`name`, `Config`, `describe`, `run(spec, operation_id)`, `status`, `cancel`, `find(operation_id)`, `exec`, `files`, `serve`), `BaseAdapter[ConfigT]`, `SiteInfo`, `SignInNeed`, `AdapterContext`.
- Jobs (`clio_relay.jobs`): `JobSpec` (PSI/J names, `JobResources.to_psij()` builds `ResourceSpecV1`), `JobResources`, `JobAttributes`, `InputRef`, `PackageRef`, `ServiceSpec`, `ServicePort`, `ServiceLifetime`, `canonical_digest`, `JobHandle`, `JobStatus`, `JobState`, `ExecResult`, `Endpoint`, `EndpointPort`, `FileEntry`, `FileAccess`, `OutputRef`.
- Records: `OperationRecord`, `OperationState`, `RecordStore`. Layout: `<state>/operations/`, `<state>/asks/`, `<state>/sites/<site>/<adapter>/`, and `sites.yaml` in the config directory.
- Content store: `ContentStore` at `<state>/content/cas/sha256/<first two hex>/<digest>`.
- `RefusalReason`, 24 typed reasons: `verb_not_implemented`, `site_not_found`, `site_exists`, `unknown_adapter`, `adapter_unavailable`, `invalid_config`, `invalid_spec`, `invalid_arguments`, `guard_unavailable`, `policy_refused`, `needs_approval`, `needs_sign_in`, `declined`, `interactive_auth_required`, `operation_not_found`, `ask_not_found`, `approval_mismatch`, `input_missing`, `path_not_found`, `path_exists`, `not_cancellable`, `submit_failed`, `unreachable`, `timeout`.
- Hooks (`clio_relay.adapters.hooks`): `Ask`, `AskProvider` (`choose`, `approve`, `url`) for W6; `GuardHook` (`check_run`, `check_exec`, `price`), `Charge` and `GuardDecision` for W13; `ServeHook` for W9.
- Change events: `ServerChanges` (above).
- Command line `clio-relay-next`: `serve` (stdio, or HTTP with a bearer token), `adapters`, `site`, `asks`.

### Conformance kit

`clio_relay.testing.conformance.AdapterConformance`, parameterised by a `ConformanceTarget(adapter, config, state_dir, ...)`. It holds 10 executable contract tests per adapter: config schema, describe shape, typed refusal of unimplemented verbs, bounded exec, files round trip, job lifecycle, job failure, job cancel, the same job observed after a restart, and inputs by digest. An adapter package subclasses it in its own tests (see the module docstring). W7 and W8 run it for `ssh` and `worker` (LQ4).

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

Branch `jaimecernuda-relay-rework-w8`, from `jaimecernuda-relay-rework` at 719104c (M1).

### Inventory method

- Source: the integration branch at 719104c. The import graph covers every `clio_relay.*` module in `src/`, from `import` statements and from module names in strings (monkeypatch targets, `python -m`, lazy imports).
- What stays is what the roots reach without passing through a deleted family. The roots are:
  - the new tree (`adapters`, `catalog`, `jobs`, `sites`, `surface`, `testing`, `cli_next`);
  - W9's files (`service_runtime*`, `jarvis_service_runtime*`, `models_gateway`, `frp_*`);
  - the worker's own roots (endpoint, HTTP API, owned session, bootstrap, queue, one-link connection, remote CLI) and the old `cli_*` modules that become the worker's operator commands.
- The deleted families are `mcp_*`, `fastmcp_server`, `browser_gateway*`, `live_acceptance*`, `live_validation*`, `scheduler_providers/`, `remote_mcp*`, `cli_remote_mcp*`, `_mcp_call_runner_facade` and `session_runtime`, plus the old `cli` root, which W1 owns and replaces at the cutover.
- Callers outside this repository were searched in clio-agent (develop f08dbde3a), gact-tui (21163590c), clio-kit (main 490269fb1) and clio-agent-marketplace (develop).

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

- **clio-agent (no production tool calls).** `relay_wait` is in `tests/test_tools/test_mcp_dual_emission_consume.py:27,152,162,172`. `relay_status` is in `scripts/check_task_replacement_removal.py:62`. `relay_submit_agent`, `relay_observe`, `relay_list_artifacts`, `relay_read_artifact` and `relay_artifact_lineage` appear only in CHANGELOG and design docs.
- **clio-agent calls the old command line.** `src/clio_agent/gact/infrastructure/drivers.py:356-445` runs:
  - `clio-relay cluster endpoint-service-status`;
  - `uv tool install clio-relay==<version>`;
  - `clio-relay cluster add --scheduler-provider slurm --agent-adapter exec`;
  - `clio-relay cluster bootstrap --relay-artifact-sha256`;
  - `clio-relay cluster install-endpoint-service --start --enable`;
  - `uv tool uninstall clio-relay`.
  These commands move under `clio-relay worker`, and `--scheduler-provider` goes with RL18. `tools/gateway.py:244` names the relay_ops namespace. The clio-agent owners (W3 and W4) are told through the coordinator.
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

### clio-schemas

Dropped. Nothing in `src/` imports it, and the worker link does not use it. It is used only by the schema-equality fixtures, their generator, `test_clio_schemas_convergence.py` and `test_schema_equality.py`. Those tests check relay's records against CLIO's schema package, which is CLIO-only coupling (user rule 1). They are deleted, and W1 removes `clio-schemas==0.2.1` from `pyproject.toml`.

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

- **Shape.** `clio_relay.adapters.worker.WorkerAdapter` is registered under the `worker` entry point. It is a `BaseAdapter[WorkerConfig]`, with this config:
  - `host`, the ssh destination used only for placement and sign-in;
  - `scheduler`, a psij executor name or `none`;
  - `rendezvous`, the frps address;
  - `state_dir`;
  - `install`, the wheel or version to place.
- **Verbs.**
  - `describe` and `status` read the owned session and worker info.
  - `run`, `cancel` and `find` go through the job routes; `find` uses the job name `relay-<operation_id>`.
  - `exec` runs a `bounded_command` job.
  - `files` uses artifacts and input ingest.
  - `serve` is `return await clio_relay.serve.start_for(self, spec, operation_id=operation_id)`, with a lazy import, as W9 asked.
- **Sign-in (LQ10, LQ11).**
  - `sign_in_needed()` returns `SignInNeed(provider="ssh", url=None, ...)` while the worker link is down.
  - The adapter calls `askpass.register_sign_in(site_id, starter)`. The starter runs bootstrap and the one-link connect over ssh. Its environment is `clio_relay.asks.askpass.environment(site=, operation_id=, title=)`, with `BatchMode=no` and stdin closed.
  - After the worker dials the rendezvous, the ssh process exits and nothing uses SSH again.
  - Until W6 merges, a local stub with the same signatures stands in.
- **The hop for W9.** `WorkerAdapter.hop_exec(site: str, argv: list[str], *, env: Mapping[str, str] | None = None, stdin: bytes = b"") -> ExecResult` (async).
  - It runs one bounded command on the login side through the held worker link, as a `bounded_command` job on the worker HTTP API. There is no ssh.
  - It replaces the `remote_cli.remote_env` and ssh calls in `frp_remote_scripts.py`, `service_runtime_connector_step_scripts.py`, `service_runtime_submission_scripts.py` and `jarvis_service_runtime.py`.
  - Long-running service processes go through `run(spec)`. Service bytes stay on W9's frp connector and visitor.
- **Operator commands (RL3).** `clio_relay.adapters.worker.cli:worker_app`, a Typer app that W1 mounts as `clio-relay worker`. Its groups are `cluster`, `endpoint`, `api`, `session`, `relay-host`, `queue`, `scheduler` and `runtime` (the old `worker` group). Its commands are `init`, `install-frp`, `installation-*` and `bootstrap-inspect`. The old `agent`, `job`, `monitor`, `gateway`, `jarvis`, `remote-mcp`, `storage`, `release` and `diagnostics` groups go. Their verbs are the new surface's `run`, `exec`, `files` and `describe`.

### Workflows

There are 8 files in `.github/workflows/`, not 6: `ci`, `finalize-release`, `live-validation-attest`, `main-provenance`, `release-gate`, `release`, `released-validation-attest` and `stage-candidate`. `ci` is rewritten to run `uv run ruff check`, `uv run ruff format --check`, `uv run pyright` and `uv run pytest` on the new tree. `release` is reduced to building and publishing the wheel. The other six only served the old release gate, which reads the deleted `release_*`, `live_validation*` and `promotion_record` modules, so they are deleted. Neither workflow is enabled or waited on.

### Requests to other owners

- **W9.**
  - In `jarvis_service_runtime.py:35` and `service_runtime_core.py:45`, change the import `clio_relay.core_queue` to `clio_relay.adapters.worker.core_queue`.
  - Drop the `browser_gateway` imports in `service_runtime_browser`, `service_runtime_local_start` and `service_runtime_readiness`.
  - Drop the `scheduler_providers` imports in `service_runtime_observation`, `service_runtime_remote_connector` and `service_runtime_start`. Use psij or `hop_exec` instead.
  - Drop the `remote_mcp` import in `jarvis_service_runtime_validation`.
  - Change the remote command strings `clio-relay scheduler connector-step-*` in `frp_remote_scripts.py:156,197` to `clio-relay worker scheduler connector-step-*`.
  - Migrate `test_service_runtime.py` and `test_jarvis_service_runtime.py` off the old `cli`, `browser_gateway` and `scheduler_providers`.
- **W1.**
  - Remove `clio-schemas` and the `mcp_call` force-include (`pyproject.toml` lines 9 and 48 to 53).
  - Add the `worker` entry point in `clio_relay.adapters`.
  - Mount `clio_relay.adapters.worker.cli:worker_app` as `clio-relay worker`.
  - At the cutover, replace the old `cli.py` root.
- **W3 and W4 (clio-agent).** `drivers.py:356-445` uses `clio-relay cluster ...` and `--scheduler-provider`, and both change at the cutover.

## W9. serve

## W10. Tapis

## W11. globus, iri and sky

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
