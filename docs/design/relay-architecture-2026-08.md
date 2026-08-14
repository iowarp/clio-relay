# clio-relay architecture — 2026-08 decomposition design

**Status:** R1–R2 complete (2026-08-14); R3–R8+ open · **Origin:** owner
correction 2026-08-13 — clio-relay drifted back to monolithic god files ·
**Tracking:** [iowarp/clio-relay#231](https://github.com/iowarp/clio-relay/issues/231)
(item 1: this doc). Mirrors the shape of clio-agent's
`docs/design/system-cleanup-2026-07.md`.

---

## 0. Execution status (living)

Kept current as slices land.

| Slice | Scope | State | Commits | Branch | Ratchet delta |
|---|---|---|---|---|---|
| **R1** | Port the file-size + class-in-function ratchet CI from clio-agent, wired into `local.file-size-ratchet`/`local.no-class-in-function` release-gate checks; delete dead `app_profiles/`, `package_adapters/`, stale `TASK.md` | **DONE** | `2713692` (ratchet CI), `429d3cb` (deletions) | `feat/231-owner-modules` | Establishes baselines (39 files file-size, 5 files class-in-function; §3). Deletion commit removed 62 unbaselined lines; zero baselined files touched. |
| **R2** | This design doc; two pre-existing hygiene fixes (`remote_connection.py` pyright, `runner.py` ruff E501) reddening the ratchet-gated release path | **DONE** (this document) | `ee3120f` (hygiene fix), plus the commit landing this file | `feat/231-owner-modules` | Net zero — see §10.1 for the worked example (a naive fix would have regressed `runner.py`'s baseline 5758→5766). |
| **R3** | `door_errors.py` — the one error-translation owner (`classify(exc) -> RelayFault`, `as_mcp_error`/`as_http_problem`) | PLANNED | — | — | — |
| **R4** | `frp_link.py` — transport substrate shared by modes (a)/(b) | PLANNED | — | — | — |
| **R5** | `frp_transport.py` — sibling `RelayTransport` implementations for modes (a)/(b) | PLANNED | — | — | — |
| **R6** | `bounded_payload.py` — the T1/T2/T3 byte-budget enforcement + `clio-relay.truncation.v1` | PLANNED | — | — | — |
| **R7** | `release_identity.py` — one `PinSite` registry + bump command + preflight (#198) | PLANNED | — | — | — |
| **R8+** | `test_cli.py` monkeypatch-seam rework → `relay-host` command-module extraction → `session_lifecycle.py` wire-model extraction | PLANNED | — | — | — |

---

## 1. Why this document exists

[#231](https://github.com/iowarp/clio-relay/issues/231) was filed after an
owner correction on 2026-08-13: clio-relay's source tree has drifted back into
monolithic god files, and the concrete cost was lived twice in one night.

**The six-sites-per-concern dev-mode example.** Changing ONE concern —
whether identity/artifact verification honors `CLIO_RELAY_DEV_MODE` — required
patching six call sites across five files in a single session on 2026-08-13,
because the concern's logic was duplicated at every call site instead of
living in one owner:

| Commit | File | What it fixed |
|---|---|---|
| `c0b679d` | `src/clio_relay/session_lifecycle.py` | placeholder artifact identity for shaless checkout installs |
| `c1a46f9` | `src/clio_relay/remote_cli.py` | `remote_env` exports `CLIO_RELAY_DEV_MODE` for dev-mode clusters |
| `11ce490` | `src/clio_relay/cli.py` | placeholder identity in the api-start release check |
| `0ff0c0d` | `src/clio_relay/endpoint.py` | worker-side mcp-result artifact digest pins honor dev mode |
| `c0f4ea5` | `src/clio_relay/remote_mcp.py` | virtual catalog server-artifact identity check honors dev mode |
| `d79f24e` | `src/clio_relay/remote_mcp.py` | control-query admission server-artifact checks honor dev mode |

Six commits, five distinct files (`remote_mcp.py` needed two separate
patches), all between 2026-08-13 02:12 and 04:20 local time, all for one
concern that has no single owner today.

**The 13-copy v3.7 contract bump.** The same night, bumping the JARVIS MCP
contract literal from v3.6 to v3.7 required editing scattered copies of the
same pinned string. Four commits on `develop` name their own copy count
directly (`relay#231 evidence` in each message):

- `603157e` — *"bump the runner's `REGISTERED_JARVIS_EXECUTION_QUERY_CONTRACT`
  to v3.7 (the 7th scattered contract pin)"*
- `80ba276` — *"bump the input-route/package-contract Literal pins to v3.7
  (copies 9-10)"* (`src/clio_relay/models.py`)
- `9d9bf9d` — *"admit `content_max_bytes` in the runner's artifact-query
  allowlist (copy 12)"*
- `2119832` — *"admit v3.7 `content`/`content_error` artifact-entry fields
  (copy 13)"*

Thirteen sites carrying the same version literal, patched incrementally as
each one was discovered failing — not because the contract changed thirteen
times, but because nothing enumerates the sites that must move together (see
§7, and [#198](https://github.com/iowarp/clio-relay/issues/198), filed after
cutting release 1.6.5 required touching 11+ files by hand across two prior
release attempts).

**What this document is.** Not one extraction — a design pass over the whole
system: which concerns exist, which module owns each, what the seams are,
what gets deleted, and the order slices land in. Item 1 of #231. Item 2 (the
ratchet CI) landed as R1. Items 3-4 (incremental migration, the `cli.py`
parsing-only rule) are what R3+ execute against the map this document draws.

## 2. Ground rules

1. **One concern, one owner.** A concern (identity verification, error
   translation, transport lifecycle, byte-budget enforcement, release-identity
   pins) lives in exactly one module. Every caller delegates to it; nothing
   re-implements it locally. The six-sites and 13-copy examples above are
   what happens when this rule is absent.
2. **`cli.py` parses and renders ONLY.** Every piece of domain logic
   currently inlined in a `@app.command` function (registry mutation, session
   orchestration, transport validation, verification — see §4.1) moves to its
   owner module. What stays in `cli.py` after R8+: argument parsing, calling
   the owner, and rendering the result.
3. **Tests move with logic.** A test that exercises business logic currently
   reachable only through a CLI command moves to the owner module's test file
   in the same slice that extracts the logic. A test that exercises argument
   parsing or rendering stays with `cli.py`.
4. **Deletions are first-class.** Every slice states what it deletes, not
   only what it adds. R1 already deleted `app_profiles/`, `package_adapters/`,
   and `TASK.md` (§10). A slice that only moves code without deleting the
   duplicate call sites it replaces is incomplete.
5. **The ratchet only ratchets down.** `RATCHET_BASELINE` in
   `scripts/check_file_size.py` (and its class-in-function counterpart) may
   only decrease. A slice that shrinks a baselined file lowers its entry in
   the same commit; a slice must never grow one (§10.1 is the worked example
   of resolving that constraint honestly instead of bumping a baseline up).
6. **No new file over 800 lines.** `DEFAULT_MAX_LINES` in
   `scripts/check_file_size.py` is 800; a brand-new file over that cap fails
   CI immediately (no baseline grandfathering for new files). This is why the
   transport work (§8) is planned as two files rather than one:
   `src/clio_relay/control_channel.py` is 750 lines today (the existing
   `RelayTransport` seam), already close to the cap, so the new sibling
   transports land in their own `frp_transport.py` (the `RelayTransport`
   implementations) with the frp-specific wire/process substrate factored
   into a separate `frp_link.py` (R4) rather than accreting either into
   `control_channel.py` or into one combined file that would immediately
   need to split again.

## 3. Measurable exit criteria

| Metric | Today (2026-08-14) | Target |
|---|---|---|
| Files over 800 lines (`scripts/check_file_size.py` baseline entries) | 39 | 0 baselined entries remaining (each either deleted, absorbed under 800, or its ceiling lowered slice by slice) |
| Files with a class defined inside a function (`scripts/check_no_class_in_function.py` baseline entries) | 5 | 0 |
| Release-identity pin sites that must move together on a version bump (#198) | 11+ (measured at 1.6.5; §7 recounts the current tree) | 1 (`release_identity.py`'s `PinSite` table) + 1 bump command |
| Bare/untyped error surfaces reaching a client (§6) | `http_api.py`: 107 hand-rolled `HTTPException` sites (`grep -c "raise HTTPException("`, 2026-08-14 — corrected from an earlier ~40 estimate; the file has grown), 0 global exception handlers; 1 deliberately-bare re-raise in `fastmcp_server.py:1106-1115` | 0 unclassified exceptions reach the wire; every surface routes through `door_errors.classify()` |
| `cli.py` inlined domain concerns (§4.1) | 4+ identified (identity/verification, registry mutation, session orchestration, transport validation) | 0 — all delegate to an owner module |
| `service_runtime.py` frp lifecycle copies (§4.3) | 3rd copy of frp deploy/start/stop logic, duplicating `relay_host.py`/`transport_probe.py` | 1 (absorbed into `frp_link.py`, R4) |

## 4. Concern inventory of the monoliths

### 4.1 `cli.py` (19,315 lines; 16 Typer sub-apps, 125 commands)

Sixteen `typer.Typer()` instances (`app.py:846-861`; the top-level `app` plus
15 registered via `app.add_typer(...)` at `cli.py:863-877`): `endpoint`,
`relay-host`, `job`, `cluster`, `agent`, `monitor`, `api`, `session`,
`gateway`, `queue`, `worker`, `scheduler`, `remote-mcp`, `release`, `storage`.
125 `@<app>.command(...)` decorators total, concentrated in `session_app`
(20), `job_app` (17), `queue_app` (15), the top-level `app` (13),
`gateway_app` (12), `scheduler_app` (11), `cluster_app` (9).

**Four domain concerns inlined rather than delegated:**

- **Identity/verification** — `_verify_owner_session_teardown`
  (`cli.py:14786-15056`, 271 lines): rejects closure unless session id and
  generation match, checks for residual resources, validates cleanup-policy
  keys/types, and enforces an `allowed_resource_kinds` allowlist. This is
  verification business logic, not CLI glue.
- **Registry mutation** — `cluster_add` (`cli.py:1940-2160`, 221 lines):
  builds a `ClusterDefinition` from ~25 parameters, computes
  `_route_revision_before_edit(name)`, and calls
  `ClusterRegistry.mutate(default_registry_path(), ...)` directly — an
  in-place on-disk registry mutation performed inline in the command
  function.
- **Session orchestration** — `session_teardown`
  (`cli.py:6342-7706`, ~1365 body lines, the largest function in the file):
  resolves `should_execute_on_cluster`, drives a managed queue,
  `_observe_worker_before_cleanup`, and a nested
  `checkpoint_finalized_cleanup_artifact` closure that verifies
  cleanup-evidence locks before authoritative closure.
- **Transport validation** — `_run_transport_validation`
  (`cli.py:18478-18630`, 153 body lines), called from `test-http-transport`,
  `test-direct-transport`, and `test-ssh-transport`: builds a
  `ValidationResource`/`ValidationReport`, runs an injected probe, and on
  failure mutates connector state and appends structured cleanup actions —
  validation-report business logic, not parsing or rendering.

**Shared-plumbing fan-out** (call-site counts, def excluded):
`_run_or_exit` (`cli.py:19307`) — 74 call sites;
`_require_cluster` (`cli.py:19132`) — 56;
`_write_failed_acceptance_report` (`cli.py:18908`) — 19;
`_resolve_env_secret` (`cli.py:19212`) — 19;
`_acceptance_report_command` (`cli.py:838`, applied as a bare decorator) — 17
applications; `default_report_path` (imported from
`src/clio_relay/validation_report.py:2006`, not defined locally) — 18 call
sites. These are exactly the kind of cross-cutting helper that an owner
module (not `cli.py`) should host once `cli.py` is parsing/rendering only.

**Other giants** (by line span): `_persist_local_cleanup_report_artifact`
(`cli.py:4515-5324`, 810 lines) and, past the top five, `jarvis_mcp_validate`
(`cli.py:11587-12037`, 451 lines) and `live_test`
(`cli.py:12399-12725`, 327 lines).

### 4.2 `core_queue.py` (16,137 lines)

Five concerns in one class (`ClioCoreQueue`, `core_queue.py:599+`), each with real
logic, not just plumbing:

- **Storage** — `_write_bytes` (atomic staged write + `os.replace`,
  `:14057-14099`), `_write`/`_write_json` (`:13943-13954`).
- **Idempotency** — `IdempotentSubmissionResolution` dataclass (`:468-474`),
  `resolve_idempotent_submission` (`:3684-3777`),
  `_write_committed_idempotency_record` (`:11789-11795`),
  `_job_idempotency_digest` (`:15906+`).
- **Leases** — `_lease_job_unlocked` (acquire, `:6842-6906`), `renew_lease`
  (`:7014-7118`), `_recover_expired_leases_unlocked` (expire,
  `:7118-7219`), `release_lease`/`_delete_lease_unlocked` (`:7363`, `:7376`).
- **Task projection (SEP-2663/MCP)** — `put_mcp_task` (`:7477-7508`),
  `update_mcp_task_projection` (`:7509-7545`), `get_mcp_task` (`:7546-7556`).
- **Store schemas** — a `record_families` registry (family → pydantic model →
  identity field, 8 families) built inline inside
  `_audit_legacy_state_before_initialization` (`:1113`, tuple at
  `:1155-1164`), plus native internal record dataclasses
  (`_LeaseIndexIdentity` `:510-518`, `_LeaseCapacityAggregate` `:522-530`,
  `_LeaseCapacityCheckpoint` `:534-541`, `_LegacyOutputAudit` `:553-561`) —
  even though the canonical wire records (`RelayJob`, `Lease`, `RelayTask`,
  ...) are imported from `models.py` (`:41-91`), the family-registry mapping
  and several internal schema types are defined natively here, so the
  "schemas" concern has already leaked into the queue module.

### 4.3 `service_runtime.py` (10,163 lines) incl. the THIRD copy of the frp lifecycle

frp process lifecycle (render config, spawn `frpc`, track, kill) appears at
more than the four hinted ranges — it is threaded through the file:
`_local_connector_intent` (`:4488-4501`, desktop-frpc path identity),
`_start_remote_connector` (`:5900-6107`, renders `render_frpc_config` at
`:5978`, launches remotely via SSH script calls `:5999-6011`/`:6056-6064`),
`_stop_allocation_connector` (`:6219-6296`), `_start_local_visitor`
(`:6335-6420`, renders `render_frpc_visitor_config` at `:6355-6368` then
directly `Popen`s the local frpc binary at `:6381`), `_start_browser_proxy`
(`:6421-6508`), `_remote_allocation_frpc_start_script`
(`:7661-7918`, generates the remote shell script that writes `frpc.toml` and
launches `frpc` as a scheduler-durable step), `_remote_frpc_start_script`
(`:7951-8188`, sibling generator for the non-allocation remote connector,
launches `frpc` in the background at `:8126` and records
`remote_frpc_pid`/`remote_frpc_pgid` at `:8162-8165`), `_remote_stop_script`
(`:8421-8467`), `signal_owned_processes` (`:8565-8652`, local kill matching
the `"frpc"` command marker at `:8607-8608`), plus discovery/status scripts
(`:8189-8420`, `:8653-8749`) and local process-group verification
(`:9732-9866`).

This is a genuine third copy, not merely config rendering. `relay_host.py`
(132 lines) is the shared, config-only source — `render_frps_config`
(`:29-40`), `render_frpc_config` (`:75-96`), `render_frpc_visitor_config`
(`:99-123`) — imported by both `service_runtime.py` and `transport_probe.py`.
But two *other* files independently spawn and manage the `frpc` process
themselves: `transport_probe.py`'s `run_frp_http_probe`
(`:211-337`, spawns `[frpc_bin, "-c", str(visitor_config_path)]` at `:288`,
tracks/kills it at `:299-315`, and embeds its own remote start/stop script at
`:1213-1286`) and the 48-line `frp_check.py`'s
`run_frpc_connection_check` (`:13-40`, spawns
`subprocess.run([frpc_bin, "-c", str(config_path)], ...)` at `:25`). Three
independent "write frpc TOML, then spawn `frpc -c <config>`, then track it"
implementations for one substrate concern (§8.2 plans their absorption into
one `frp_link.py`).

### 4.4 `session_lifecycle.py` (8,328 lines): state machine + ~19-30 wire models + helpers

The wire-model cluster sits at `:890-1433` (not `:891-1435` — the class
statement for the first model, `RemoteSession`, is at `:891` but the
preceding `@dataclass(frozen=True)` decorator is `:890`; the cluster's last
model, `OwnedSessionStartPlan`, closes at `:1432`, and `:1434` begins an
unrelated process-tracking dataclass). Seventeen classes in that range: the
one frozen dataclass (`RemoteSession`, a lightweight `session_id`/
`remote_api_port`/`api_token` record) plus sixteen `pydantic.BaseModel`
wire types — `SessionApiReleaseIdentity` (`:899`),
`OwnedSessionInputPolicy` (`:924`), `OwnedSessionStartRequest` (`:960`),
`OwnedSessionStartRejection` (`:978`), `OwnedSessionStartStatusSelector`
(`:994`), `OwnedSessionStartRetrySelector` (`:1014`),
`OwnedSessionTeardownRequest` (`:1034`),
`OwnedSessionIdentityChallengeRequest` (`:1048`),
`OwnedSessionCleanupTarget` (`:1059`), `CleanupResource` (`:1082`),
`RemoteSessionStateEvidence` (`:1142`),
`OwnedSessionCleanupReportReference` (`:1156`),
`OwnedSessionRecoveryStatus` (`:1173`), `OwnedSessionStartResult`
(`:1263`), `OwnedSessionStartReceipt` (`:1352`), `OwnedSessionStartPlan`
(`:1385`).

For contrast, the state-machine logic lives 1,600-4,300+ lines downstream of
that cluster, not interleaved with it: `inspect_owned_session_recovery_status`
(`:2417-3098`, explicitly documented as read-only — inspects durable
metadata, process identity, and core-queue admission to classify
recoverable/dead/running/mismatched), `execute_owned_session_start`
(`:5315-6220`, ~900 lines), `execute_owned_session_teardown`
(`:6751-7092`).

### 4.5 `bootstrap.py` / `endpoint.py` / `mcp_server.py` / `remote_mcp.py` / `mcp_call/runner.py`

- **`bootstrap.py` (8,733 lines)** — "Autonomous installation helpers for
  desktop and cluster targets." Three concerns collide: archive packaging
  (`create_bootstrap_archive` `:8529-8552`), SSH remote-orchestration +
  cryptographic receipt validation (`bootstrap_cluster_over_ssh`
  `:2815-3075`, `_validate_bootstrap_receipt` `:3119-3652`, 533 lines), and
  two enormous embedded shell-script-template renderers —
  `_relay_only_reconcile_script` (`:4061-5880`, 1,819 lines) and
  `render_linux_user_bootstrap_script` (`:5883-8441`, 2,558 lines) — which
  alone are 4,377 of the file's 8,733 lines (~50%). Lines `:85-1809` are
  mostly module-level *string constants* holding embedded Python source
  shipped to run on other machines, not real top-level functions.
- **`endpoint.py` (8,710 lines)** — "Long-running desktop and cluster
  endpoint behavior." One class, `EndpointWorker` (`:258-5813`, 5,555 lines
  = ~64% of the file, 81 methods), covers job execution
  (`_run_job` `:653`, `_run_execution_streaming` `:1450`), scheduler
  cancellation with retry/backoff (`_confirm_scheduler_cancellation`
  `:5518`), and lease renewal (`_renew_lease_if_needed` `:5771`). ~90
  module-level functions after the class (`:5816-8710`) cover unrelated
  concerns: Jarvis execution-recovery bookkeeping
  (`_durable_jarvis_execution_recovery` `:6005`) and Windows-specific
  sidecar-file handle cleanup (`_quarantine_windows_sidecar_by_handle`
  `:7728`) — the same handle-cleanup shape duplicated in `runner.py` below.
- **`mcp_server.py` (5,920 lines)** — "Stdio MCP server for relay job
  submission tools." `serve_stdio` (`:421-459`) is the JSON-RPC read loop;
  `_all_tool_definitions` (`:664-1762`, 1,098 lines) is one function
  returning the inline JSON-schema catalog for every relay MCP tool;
  `_call_tool` (`:1778-2215`, 437 lines) is a string-match dispatcher
  routing ~40 tool names to private business functions
  (`_submit_jarvis_pipeline` `:4160`, `_wait_job` `:3885`).
- **`remote_mcp.py` (5,309 lines)** — deliberately separates remote schema
  discovery from local `tools/list` (docstring `:1-7`). 20 top-level
  classes, mostly pydantic models: `RemoteMcpSchemaCache` (`:664-789`, a
  `FileLock`-backed on-disk cache) and `RemoteMcpAcceptanceReport`
  (`:1136-1346`, itself carrying a business method,
  `to_live_validation_report` `:1157`). Catalog assembly
  (`build_virtual_remote_mcp_catalog` `:2126`) sits beside two large
  domain-specific validator families — Spack (`_spack_fresh_install_check`
  `:3379`, `_spack_user_contract_check` `:4354`) and scientific-catalog
  (`_scientific_catalog_structured_result_check` `:3904`) — inside a module
  whose own docstring frames it as generic virtualization.
- **`mcp_call/runner.py` (5,758 lines)** — "Minimal stdio MCP client used by
  relay endpoint containment and legacy JARVIS adapters." A hand-rolled
  subprocess JSON-RPC client (`_open_process` `:5311`, `_write_message`
  `:5165`, a reader thread + `Queue` at `:5222`/`:5266`) sits beside the same
  Windows private-snapshot handle-cleanup pattern duplicated from
  `endpoint.py` (`_open_windows_snapshot_cleanup_handle` `:2691`) and Python
  wheel/distribution identity verification for the clio-kit runtime
  (`_verified_wheel_archive` `:4620`, `_installed_clio_kit_runtime_identity`
  `:4030`).

### 4.6 `tests/test_cli.py` (14,548 lines) as the coupling that makes 4.1 expensive

236 call sites monkeypatch `cli`'s own module namespace directly — 232 of the
form `monkeypatch.setattr(cli, "<name>", ...)` and 4 patching attributes of
modules `cli` itself imports (`cli.uvicorn` at `:4925`, `cli.subprocess` at
`:9471`/`:9535`/`:9573`). Examples: `monkeypatch.setattr(cli, "EndpointWorker",
make_worker)` (`:663`), `monkeypatch.setattr(cli, "_require_cluster",
fail_registry_lookup)` (`:664`), `monkeypatch.setattr(cli,
"worker_runtime_info", worker_info)` (`:1022`).
`tests/test_acceptance_report_defaults.py:963` is the exact worked example:
inside `_install_success_fakes` (`:934-1160`, 227 lines), the
`case.name == "relay-host-test-http-transport"` branch does
`monkeypatch.setattr(cli, "run_frp_http_probe", _successful_http_probe)` — one
of 28 such `case.name`-dispatched fakes in that one file, one branch per
acceptance-report CLI command.

This is why extracting logic out of `cli.py` is expensive today: `monkeypatch`
patches names *where they are looked up*, not where they are implemented.
`cli.py` currently imports or defines `EndpointWorker`, `_require_cluster`,
`run_frp_http_probe`, and similar symbols directly into its own namespace,
and every one of the 236 (+28) call sites binds a fake onto that exact
namespace slot. Moving the real implementation into an owner module without
also either re-exporting the symbol from `cli.py` under the identical name or
rewriting every patch site to target the new module breaks the test silently
(the fake is never invoked) or loudly (`AttributeError` once the name leaves
`cli`'s namespace). §9 sequences the monkeypatch-seam rework (R8+) as its own
slice for exactly this reason — the coupling has to be paid down before
`cli.py` extractions can proceed cheaply.

## 5. Target owner-module map

| Concern | Owner module | Current home(s) | Slice |
|---|---|---|---|
| Error classification/translation | `door_errors.py` | scattered: `fastmcp_server.py` typed conversions (with one deliberately-bare exception, §6.1), `http_api.py` (~40 hand-rolled `HTTPException` sites), `mcp_server.py`, `runner.py` | R3 |
| frp process substrate (render TOML → spawn `frpc` → track/kill) | `frp_link.py` | `service_runtime.py` (3rd copy, §4.3), `transport_probe.py`, `frp_check.py`; config-only rendering already centralized in `relay_host.py` | R4 |
| `RelayTransport` implementations for modes (a)/(b) | `frp_transport.py` | `control_channel.py`'s `build_transport` refuses both (`TransportModeUnavailable`, §8.2); `transport_probe.py` has probe-only, non-production logic | R5 |
| Byte-budget enforcement / truncation (T1/T2/T3, §6.4) | `bounded_payload.py` | constants scattered across `control_channel.py`, `remote_connection.py`, `mcp_server.py`, `runner.py` | R6 |
| Release-identity + contract pins (§7) | `release_identity.py` | `pyproject.toml`, `__init__.py`, `models.py` (×3), `jarvis_mcp.py` (×3, incl. `CLIO_KIT_JARVIS_MCP_VERSION`), `cluster_config.py`, `installation.py`, `remote_mcp.py`, `runner.py`, `bootstrap.py`, `.github/workflows/ci.yml` (×2 jobs), `docs/release-gate-1.0.yaml`, `examples/release-gate/*.json`, 4+ test files | R7 |
| `cli.py`↔test monkeypatch seam (§4.6) | rework the injection seam itself (no new module — a DI seam `cli.py` exposes so extractions don't break 236+28 patch sites) | `tests/test_cli.py`, `tests/test_acceptance_report_defaults.py` | R8+ |
| `relay-host` command-module extraction (parsing/rendering only, ground rule 2) | new `cli_commands/relay_host.py`-shaped module (exact name TBD at R8+; owns `relay_host_app`'s 7 commands) | `cli.py` (`relay_host_app`) | R8+, sequenced after R5 (§9 overlap) |
| `session_lifecycle.py` wire models (§4.4) | a dedicated wire-model module (exact name TBD at R8+) | `session_lifecycle.py:890-1433` | R8+ |
| Identity/verification (the six-sites example, §1) | not yet sequenced — needs its own slice number beyond R8+ | `cli.py:14786` (`_verify_owner_session_teardown`), `remote_mcp.py` (×2), `endpoint.py`, `session_lifecycle.py`, `remote_cli.py` | unsequenced (§9 flags this explicitly rather than silently dropping it) |
| Registry mutation (`cli.py`'s `cluster_add`) | `ClusterRegistry.mutate` already exists as the storage primitive (`cluster_config.py`); the gap is the ~220 lines of construction/validation logic still inlined in the command | not yet sequenced | unsequenced |
| Session orchestration (`cli.py`'s `session_teardown`, ~1365 lines) | `session_lifecycle.py` already owns session state; teardown orchestration should call into it instead of duplicating decisions in the command | not yet sequenced, overlaps the R8+ wire-model split | unsequenced |
| Transport validation (`cli.py`'s `_run_transport_validation`) | folds naturally into `frp_transport.py` once modes (a)/(b) are real implementations, not probes | not yet sequenced | unsequenced, logically after R5 |
| `core_queue.py`'s five concerns (§4.2: storage/idempotency/leases/task-projection/schemas) | five-way split, each concern its own owner | `core_queue.py` (16,137 lines, one class) | not yet sequenced — the largest remaining monolith and out of scope for R3-R8+ |

Rows marked "not yet sequenced" are named here deliberately (ground rule 4:
deletions and gaps are first-class, not silently dropped) rather than being
assigned a slice number this document cannot yet justify with a concrete
extraction plan.

## 6. Error-surface doctrine

### 6.1 Two regimes today

**Typed path.** `src/clio_relay/errors.py` (36 lines) is a shallow, deliberate
hierarchy: `RelayError(RuntimeError)` (base, `:4`) →
`ObservationTimeoutError` (`:8`), `ConfigurationError` (`:12`),
`QueueConflictError` (`:16`) → `TaskInputParkConflictError` (`:20`, a
distinct subtype specifically so callers can discriminate by type rather than
message — see below), and `NotFoundError` (`:34`). `MAX_REFUSAL_MESSAGE_CHARS
= 2_000` (`jarvis_dispatch_failure.py:29`) caps `JarvisDispatchRefusal.message`
at both its typed (`:99`) and fallback (`:112`) construction sites.
`fastmcp_server.py` converts domain exceptions to typed `MCPError`s at each
call site rather than through one owner function: `_record` maps
`NotFoundError` → `INVALID_PARAMS` (`:994-1001`), `_handle_get` maps any
`Exception` → `INTERNAL_ERROR` while explicitly logging the traceback first so
it isn't lost (`:1003-1037`, citing #215), and `intercept_tool_call` maps
`QueueConflictError` → `INVALID_PARAMS` (`:1060-1133`, citing #218). There are
zero raw Python tracebacks anywhere under `src/clio_relay/`
(`traceback.format_exc()`/`import traceback`: zero matches) — but `str(exc)`
of an already-caught, already-typed exception reaching a wire response is a
widespread pattern (56 sites in `http_api.py` alone, e.g. `:1222`,
`:1224`, `:1339`; plus `mcp_server.py:525`/`:2326`, `fastmcp_server.py:1131`,
`browser_gateway.py:510`/`:581` — ~59+ sites total). Every observed instance
wraps a curated domain exception's message, not a bare traceback, but the
volume and lack of a single owner is exactly the concern §5 assigns to
`door_errors.py`.

**Raw paths.** `runner.py` caps stdout/stderr at READ time
(`MCP_SESSION_MAX_STDOUT_BYTES = 32 * 1024 * 1024` at `:47`,
`MCP_SESSION_MAX_STDERR_BYTES = 4 * 1024 * 1024` at `:48`, applied inside
`_run_mcp_session`'s reader at `:4838`) before writing the result into
`mcp-result.json` via `_write_mcp_result` (`:2228-2319`). `relay_ops.py`'s
`read_artifact_bytes` (`:200-247`) enforces `MAX_ARTIFACT_CONTENT_BYTES = 16
* 1_048_576` (`:44`, enforced at `:224`), exposed through
`mcp_server.py:3393` (local-result artifacts) and `mcp_server.py:3639`
(public/model artifacts) as base64 inside a single MCP response. `frp_check.py`
(48 lines, confirmed to exist under that exact name) captures raw `frpc`
stdout with no byte cap at all in `run_frpc_connection_check` (`:13-40`) —
only the failure path is bounded, and only to the last 12 lines (`:39`), not
bytes.

**The live holes.** The deliberately-bare re-raise sits at
`fastmcp_server.py:1106-1115`:

```python
1106        except TaskInputParkConflictError:
1107            # relay#218 rework: _park_agent_input's own CAS-exhaustion
1108            # conflict is an unrelated transient concurrency conflict, not a
1109            # client parameter problem -- it must never be mistyped as
1110            # INVALID_PARAMS. ...
1115            raise
```

Left unwrapped, it escapes through FastMCP's generic handler as a bare
internal error — a deliberate choice (documented in the comment, citing
#218) to avoid a worse mistyping, but still a gap `door_errors.py` should
close with a correctly-typed conversion rather than a bare re-raise.
`http_api.py` has zero global FastAPI exception handlers
(no `@app.exception_handler`/`app.add_exception_handler` anywhere under
`src/clio_relay/`; its one registered `app.add_middleware` at `:1046-1056`
is a request-body size limiter, not an error handler) across 107
hand-rolled `raise HTTPException(...)` sites — any unguarded route that
raises something other than `HTTPException` emits FastAPI's default bare
`Internal Server Error`.

### 6.2 The one translation owner

`door_errors.py` (R3, PLANNED): a pure `classify(exc) -> RelayFault` function
plus `as_mcp_error`/`as_http_problem` adapters over that one table — three
call surfaces (`fastmcp_server.py`, `http_api.py`'s handlers, `mcp_server.py`'s
stdio path), one classification. The 107 hand-rolled `HTTPException` sites in
`http_api.py` are explicitly **not** deleted by R3 — replacing 107 call
sites mechanically is its own later slice, named here so R3 is not judged
half-done for leaving them. R3's job is narrower and load-bearing: give every
surface one table to route *through*, so the next 107-site sweep is
mechanical instead of another archaeology expedition.

### 6.3 The agent-facing contract `clio-relay.error.v1`

Proposed by this document for R3/R6 (no such schema exists in code today —
confirmed by a repo-wide search): `schema_version`; `reason` (a frozen
`REASONS` registry, snake_case, ≤64 chars); `message` ≤2000 chars (T1, §6.4);
`retryable: bool`; `detail` ≤2000 chars, optional; `cluster`/`job_id`/`task_id`
optional; `evidence: {artifact_id}` by reference, never inline bytes; a
truncation record when any field was elided; the whole envelope ≤8KiB
(overflow drops `detail`, then `evidence`, in that order). Tracebacks or raw
`str(exc)` of an *unclassified* exception must never reach the wire — the
existing `str(exc)`-of-a-typed-exception pattern (§6.1) is compatible with
this contract once each of those 59+ sites routes through `door_errors.classify()`
instead of formatting the exception locally.

### 6.4 Byte budgets, three tiers

**T1 — refusal text, 2,000 chars, hard-truncated, in-band marker.**
Three independent precedents already agree on 2000 chars as the refusal-text
budget: `MAX_REFUSAL_MESSAGE_CHARS = 2_000`
(`jarvis_dispatch_failure.py:29`), `MAX_CHANNEL_EVENT_DETAIL_CHARS: Final =
2_000` (`control_channel.py:68`, used at `:212`), and an inline (unnamed)
`[:2_000]` slice on a JSON-encoded error body in
`remote_connection.py:920-924`. R6 should name one shared constant instead of
three independently-agreeing literals.

**T2 — agent-parsed payload, 65,536 bytes inline, never truncated; overflow
is a typed delivery-failure document.** `mcp_server.py` already implements
this shape exactly: `MAX_INLINE_MCP_RESULT_BYTES = 65_536` (`:174`),
`MCP_RESULT_DELIVERY_SCHEMA = "clio-relay.mcp-result-delivery.v1"` (`:175`),
used together at `:3500-3530` — under the limit, the result returns inline;
over it, a typed `dict` (not a dataclass or pydantic model today) carries
`content_truncated`, `result_available`, and a nested `delivery` object
(`schema_version`, `status`, `code`, `max_inline_bytes`,
`private_evidence_preserved`, `remote_side_effects_may_have_occurred`,
`message`). This is the T2 precedent R6 generalizes, and it is the one place
in the current codebase where "never truncate — hand back a typed overflow
document instead" is already real.

**T3 — durable operator evidence; read bounds stay generous, record bounds
should be head+tail but currently are not implemented that way.** The read
side is real and generous: `runner.py`'s 32 MiB stdout / 4 MiB stderr
read-time caps (`:47-48`, applied at `:4838`) and `relay_ops.py`'s 16 MiB
artifact-content read cap (`:44`, `:224`). **Correction against this
document's own working hint:** there is no additional head+tail bounding
applied when `runner.py`'s `_write_mcp_result` (`:2228-2319`) builds
`mcp-result.json` — `stdout`/`stderr` are written through unchanged from the
already-read-capped values (`:2298-2299`, sourced from `:2013-2014`); a
targeted search for `head_bytes`/`tail_bytes`/`1_048_576`/`262_144`-style
pairs in `runner.py` found none. The nearest real precedent for "bound the
middle, keep both ends" is `_BoundedTextTail` in
`src/clio_relay/jarvis_provider.py:36-74` — but it is **tail-only**, not
head+tail, and applies one shared `STREAM_RESULT_TAIL_MAX_CHARACTERS = 1024 *
1024` (1 MiB) bound identically to both `stdout_tail` and `stderr_tail`
(`:314-315`), not the asymmetric stdout/stderr split this document
originally hypothesized. R6's actual scope at T3 is therefore larger than
"generalize an existing head+tail bound": it has to *build* head+tail
record-time bounding for `runner.py`'s result document (extending the
tail-only `_BoundedTextTail` shape, or a new bounded-window primitive) rather
than lift one that already exists — recorded honestly in §6.5 below so R6 is
scoped correctly from the start.

The `clio-relay.truncation.v1` schema below is this document's proposal for
that still-to-be-built record-time bound, not a description of shipped code:
`{schema_version, truncated, retention: "head"|"head_tail", original_bytes,
retained_bytes, elided_bytes, marker, evidence_ref}`, with the marker string
`"[clio-relay: elided N bytes of stdout]"` written full-line, in-band, at the
elision point.

### 6.5 Specified-but-not-implemented ledger

| Item | Spec status | Code status | Issue |
|---|---|---|---|
| `door_errors.py` — one classify/adapt owner | Specified (§6.2) | Not started | tracked under #231 (R3) |
| `clio-relay.error.v1` agent-facing envelope | Specified (§6.3) | Not started | tracked under #231 (R3/R6) |
| `clio-relay.truncation.v1` + head+tail T3 record-time bounding | Specified (§6.4) | Not started — no head+tail bound exists in `runner.py` today, only the tail-only, non-split `_BoundedTextTail` in `jarvis_provider.py` | tracked under #231 (R6) |
| `TaskInputParkConflictError` typed conversion (replace the bare re-raise) | Specified (§6.1) | Bare re-raise still in place, `fastmcp_server.py:1106-1115` | tracked under #231 (R3) |
| `http_api.py`'s 107 `HTTPException` sites routed through `door_errors.classify()` | Specified (§6.2, explicitly deferred) | Not started | later slice beyond R3, named not tonight (§10) |
| `release_identity.py`'s `PinSite` table + bump command + preflight | Specified (§7) | Not started | #198, tracked under #231 (R7) |
| `frp_link.py` frp-substrate absorption | Specified (§4.3, §8.2) | Not started | tracked under #231 (R4) |

## 7. Release-identity + contract pins (#198)

[#198](https://github.com/iowarp/clio-relay/issues/198) was filed after
cutting 1.6.5 required hand-editing 11+ named sites in one commit
(`pyproject.toml`, `src/clio_relay/__init__.py`, `uv.lock`, three test version
literals, `docs/release-gate-1.0.yaml`, `examples/release-gate/report-matrix-1.0.json`,
`docs/release.md`, `docs/release-acceptance-1.0.md`, `README.md`) after four
dead protected tags (v1.6.1-v1.6.4) each died on a missed site. Three
distinct families of pin exist today, verified against the current tree
(`1.6.6`):

**Release version** (`pyproject.toml:3`, `src/clio_relay/__init__.py:5` —
both `"1.6.6"`) plus the mirrored copies `docs/release-gate-1.0.yaml:6`
(`release_version: "1.6.6"`) and `examples/release-gate/report-matrix-1.0.json:3`
(`"release_version": "1.6.6"`), each paired with a self-digest
(`acceptance_matrix_sha256` / `matrix_sha256`, value
`affef812c611feb1e8828a61347c824271f859ed3e74a1969247f8c26037b61e`) recorded
at `docs/release-gate-1.0.yaml:8`, `examples/release-gate/report-matrix-1.0.json:4`,
and asserted against in `tests/test_ci_validation.py:1793-1794`.

**Jarvis MCP contract version** — the "13-copy v3.7" story from §1. The
current tree carries the `v3.7` literal at 11 non-test sites
(`cluster_config.py:264`, `installation.py:54`, `jarvis_input_plane.py:7`
(docstring), `jarvis_mcp.py:41` and `:80`, `models.py:43`/`:506`/`:618`,
`remote_mcp.py:128`, `src/clio_relay/_contracts/jarvis-user-v3.7.json:3`,
`jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py:61`) plus 4 test
sites (`tests/test_jarvis_mcp_validation.py:2100`,
`tests/test_remote_mcp.py:120`/`:1391`/`:1410`) — 15 total. The commit-named
"copy 13" and the tree's current count of 11-15 are not in tension: the
commits fixed sites sequentially as each one was discovered failing, and the
count moves as the campaign both patches and re-audits; the concrete point —
too many sites for a human to enumerate reliably — holds regardless of the
exact number on any given day, which is precisely the failure mode
`release_identity.py`'s `PinSite` table (§9) exists to end.

**A live hole this document surfaces, not previously tracked:**
`docs/release-gate-1.0.yaml` pins the *retired* `clio-kit-jarvis-user-v3.6`
contract (`:131`, `:320`) against a tree that has shipped `v3.7` for multiple
release cycles — a fixture that drifted because nothing regenerates it
together with the source it's meant to validate, exactly the failure mode
#198 describes. The same file *also* pins clio-kit `2.6.6`
(`docs/release-gate-1.0.yaml:122`, `:231`, `:300`, `:302`) while
`.github/workflows/ci.yml` (`:62-64`, `:166-168`, two separate build jobs)
and `src/clio_relay/jarvis_mcp.py:32`
(`CLIO_KIT_JARVIS_MCP_VERSION = "2.7.2"`, checked against a cluster's
install spec at `bootstrap.py:5976-5977` and the remote bootstrap script
template at `bootstrap.py:7367-7368`) all pin `2.7.2`. Two independent
staleness bugs in one fixture file, both symptoms of the same missing
`PinSite` registry.

**Kit-pin digests.** The clio-kit wheel identity — filename, download URL,
and SHA-256 — is pinned twice in CI (`ci.yml:62-64` and `:166-168`, one pin
per build job) and referenced in `docs/operations.md:719` and
`docs/remote-mcp-federation.md:472`; `docs/release-gate-1.0.yaml` carries the
stale `2.6.6` variant noted above instead.

**R7's target:** one `release_identity.py` module holding a `PinSite` table
(path, line-or-key selector, pin family) covering all three families above,
one bump command that rewrites every site and recomputes self-digests (the
`validate_release_acceptance_matrix` logic #198 proposes reusing), and one
fast `release preflight` check that asserts the whole identity contract is
internally consistent in seconds — replacing a full `validate-local` battery
run as the only way to catch drift today.

**Precedent for byte-identical enforcement:** `jarvis-packages/clio_relay/clio_relay/process_containment.py`
is a deliberately vendored, byte-identical copy of
`src/clio_relay/process_containment.py`, policed by
`test_embedded_containment_source_is_an_exact_isolated_runtime_mirror`
(`tests/test_process_containment.py:50-55`), which reads both files
(`:52-53`) and asserts `embedded.read_bytes() == source.read_bytes()`
(`:55`). This is the model for how `release_identity.py` should treat any
future *content* pin (not just a version literal): a test that asserts
byte-identity rather than trusting two edits to stay in sync by convention.

## 8. Transports (#188) in the owner-module architecture

### 8.1 The seam that exists

`src/clio_relay/control_channel.py` (750 lines) already carries the seam
modes (a)/(b) slot into. `RelayTransport` is a Protocol at `:305-347` with
`mode`/`requires_user_authorization` properties and
`establish`/`open_stream_channel`/`is_alive`/`failure_detail`/`close`
methods. `SshForwardTransport` (`:350-562`) is the mode-(c) implementation
and the lifecycle template: `argv()` renders the exact ssh command
(`:405-435`), `establish` dials once and reads the framed bootstrap document
(`:437-467`), `open_stream_channel` unconditionally raises
`StreamChannelsUnavailable` (`:469-480` — multiplexing onto the held forward
isn't built for any mode yet), `close` closes stdin so the remote holder
exits before falling back to terminate/kill with timeouts (`:491-515`).
`build_transport` (`:614-659`) is the factory: `"ssh_forward"` →
`SshForwardTransport(...)` (`:635-652`); anything else falls through to the
refusal in §8.2. What must not move in R4/R5: the `RelayTransport` Protocol
shape and `SshForwardTransport` as the reference lifecycle — new transports
implement the same five-method surface, they don't renegotiate it.

### 8.2 Modes (a)/(b) as sibling owner modules

`frp_link.py` (R4, the substrate) absorbs the three independent "render TOML
→ spawn `frpc` → track/kill" copies identified in §4.3:
`service_runtime.py`'s scheduler-durable frp lifecycle (the largest, spread
across `:4488-9866`), `transport_probe.py`'s `run_frp_http_probe`
(`:211-337`, plus its own remote start/stop script at `:1213-1286`), and the
48-line `frp_check.py`'s `run_frpc_connection_check` (`:13-40`). `frp_transport.py`
(R5, the transports) hosts the new `RelayTransport` implementations for
`brokered_tcp`/`udp_rendezvous`, built on `frp_link.py` rather than
reimplementing process management a fourth time. Both new modules reuse
`relay_host.py`'s existing config-only renderers (`render_frps_config`
`:29-40`, `render_frpc_config` `:75-96`, `render_frpc_visitor_config`
`:99-123`) unchanged — that file is already the correctly-scoped single
owner for TOML rendering; nothing here duplicates it.

`transport_probe.py`'s `allow_stcp_fallback` parameter
(`run_frp_direct_http_probe`, `:338-402`; declared `:352`, checked `:372`) is
FORBIDDEN in production: on a `RelayError` from the XTCP direct-HTTP attempt
(`:356-370`), when true it silently re-runs as an STCP relay-point-carried
attempt (`:375-388`) — exactly the automatic mode-switching
`connection-model.md`'s "Never do this" section rules out ("the relay never
switches modes on its own"). It is reachable only from `cli.py`'s probe
subcommands and `live_acceptance.py`, never from `control_channel.py`'s
`build_transport` — confirmed not wired into any production path today, and
`frp_transport.py` must not inherit this fallback when modes (a)/(b) become
real.

Confirmed, both non-ssh modes currently refuse rather than degrade:

```python
# control_channel.py:653-658
if mode in ("brokered_tcp", "udp_rendezvous"):
    raise TransportModeUnavailable(
        f"relay transport mode {mode!r} is declared by the design but not implemented in "
        ...
```

### 8.3 The identity-anchor ruling

Mode (c) carries the bring-up identity document out of band, over the
ssh-authenticated act: `owned_session_channel_bootstrap_script()`
(`control_channel.py:565-611`) composes `session recovery-status` and
`session challenge-owned` output into one framed JSON document, printed
between `CHANNEL_BOOTSTRAP_BEGIN`/`END` markers, then blocks on
`exec cat >/dev/null` to hold the session open (`:610-611`) —
`SshForwardTransport._read_bootstrap` (`:517-549`) consumes it. It verifies
against a cluster-side owner token that never leaves the cluster,
documented directly on `OwnedSessionChannelBootstrap`
(`control_channel.py:124-134`, the load-bearing sentence at `:127-128`):
*"The owner token that signs `identity` is minted cluster-side and never
leaves the cluster, so the local relay cannot compute the expected identity
[itself]."*

In modes (a)/(b) there is no ssh act to carry that document over — no
per-connection authenticated channel exists before the frp handshake joins
the two outbound dials. **Ruling:** modes (a)/(b) declare a typed
`identity_anchor="preshared_link_secret"` (the stcp secret key + API token
pairing already named in `connection-model.md`'s mode (a) description),
recorded on the `ChannelLink`, stamped on every `ChannelEvent`, surfaced in
`RemoteConnectionRegistry.event_report()`, and REFUSED unless the cluster
definition explicitly opts in via a `FrpTransportConfig.identity_anchor`
field. Not silent, not defaulted — a cluster that doesn't set it does not
get to use modes (a)/(b) by falling through to a weaker anchor unannounced.
Follow-up issue: a client-verifiable bring-up key (asymmetric signature)
supersedes the residual noted in `connection-model.md`'s "Still deviating"
section ("the connection-lifetime identity nonce is weaker than a
per-operation proof"); filed as part of this document, see §12.
`connection-model.md` keeps its "Still deviating" entry until that follow-up
lands — this document does not resolve it, only names the anchor gap
precisely enough to scope the follow-up.

### 8.4 Dial-count invariants per mode

The unit contract for R5 (from `docs/connection-model.md`'s SSH-budget table
and `docs/one-link-control-plane.md`'s mode table, both normative and
unchanged by this document):

| Mode | Bring-up | Any number of later operations |
|---|---|---|
| (c) `ssh_forward` | 1 ssh (deploy, skippable) + 1 ssh (the held forward) = ≤2 | 0 new ssh; drop never respawns; reconnect costs exactly +1 (re-enters bring-up, user present) |
| (a) `brokered_tcp` | ≤1 ssh (deploy, skippable); 0 ssh for the rendezvous itself | 0 new ssh; reconnect costs 0 ssh (both relays simply dial out again) |
| (b) `udp_rendezvous` | same as (a); hole-punch failure falls back to (a)'s relay-point-carried TCP, staying within the same configured mode | same as (a); a failed punch is not a failed connection, never a typed refusal that surfaces to the operator as broken |

A punch failure in (b) is an in-mode degradation already sanctioned by
`connection-model.md` (falling back to (a)'s TCP path, not to another mode
entirely); a dial failure in any mode that isn't that one sanctioned
fallback is a typed refusal with zero stcp/xtcp config rendered — `frp_link.py`
must not render or attempt a connection speculatively.

## 9. Migration order + stopping points

R3 (`door_errors.py`) → R4 (`frp_link.py`) → R5 (`frp_transport.py`) → R6
(`bounded_payload.py`) → R7 (`release_identity.py`) → R8+ (`test_cli.py`
monkeypatch-seam rework → `relay-host` command-module extraction →
`session_lifecycle.py` wire-model extraction).

**Rationale per step.** R3 first: smallest, most independent seam (one
module, three call surfaces), and it immediately reduces the six-sites-style
cost for the next error-adjacent change. R4 before R5: `frp_transport.py`'s
`RelayTransport` implementations need `frp_link.py`'s substrate to exist, or
building the transports first recreates the "spawn frpc inline" duplication
§4.3/§8.2 are trying to end. R6 after the transport work: doing it right
after R5 keeps the transport-adjacent T1 constants and the
`ChannelEvent`/`identity_anchor` stamping from §8.3 landing in the same
review pass, even though R6's `mcp_server.py`/`runner.py` scope is otherwise
independent of transports. R7 last among the single-module slices:
`release_identity.py`'s `PinSite` table is easiest to get right once R3-R6
stop adding new scattered constants that would immediately need their own
pin entries. R8+ last because it is gated on nothing structural — only on
the monkeypatch-seam cost itself (§4.6), which doesn't shrink until someone
pays it down deliberately.

**Overlap analysis.** Transports (§8, R4/R5) and the rest of the
decomposition were checked for overlap and found clean: `frp_link.py`/
`frp_transport.py` touch `control_channel.py`, `service_runtime.py`,
`transport_probe.py`, `frp_check.py`, and `relay_host.py` (read-only reuse) —
none of which are targeted by R3, R6, R7, or R8+'s named extractions. The one
genuine overlap is `cli.py`'s `relay_host_app` command group (7 commands,
§4.1's sub-app inventory): its commands currently call directly into the
frp-lifecycle and probe code R4/R5 are restructuring, so extracting it into
its own command module (§5's `relay-host` row) before R5 lands would mean
extracting it twice — once now, and again after its callees move. That is
why the `relay-host` command-module extraction is sequenced inside R8+,
strictly after R5, rather than bundled into the earlier `cli.py`-focused work
implied by ground rule 2.

**Every stopping point is green.** Each slice above lands independently
buildable: ruff/pyright/both ratchets clean, `uv run pytest tests/ -m "not
integration"` clean, and the local release gate's `validate-local` battery
(§0's table tracks this per slice) passing before the next slice starts. No
slice depends on a later slice's code existing yet — R4 does not need R5's
transports, R6 does not need R7's pin registry — only on the file-boundary
overlaps named above.

## 10. What gets DELETED

**Done (R1):** `src/clio_relay/app_profiles/__init__.py`,
`src/clio_relay/package_adapters/__init__.py` (each a docstring-only stub with
zero importers), and the superseded `TASK.md` checklist.

### 10.1 R2 worked example: resolving a ratchet conflict honestly

R2's `runner.py` E501 fix is the first worked example of ground rule 5 (§2):
ruff-format's canonical fix for a 104-character set literal is to explode it
one item per line (+8 lines), which would have regressed the file's
zero-slack ratchet baseline (5758). Rather than bump the baseline up, the fix
splits the literal into two ruff-format-stable lines (`allowed = {...}` /
`allowed |= {...}`) and offsets the added line with an adjacent, independently
idiomatic simplification in the same function (merging a `typed.get(...)` /
`is not None` check into one walrus-assignment line), landing net zero
against the baseline. Removing/redesigning first, rather than releasing the
ratchet, is the standing default; see the ratchet's own docstring
("baseline may only ratchet DOWN").

**Planned (each named here so it isn't silently dropped when its slice lands):**

- R4: the duplicated frp process-lifecycle logic in `transport_probe.py`
  (`:211-337`, `:1213-1286`) and `frp_check.py` (`:13-40`) is deleted once
  both delegate to `frp_link.py` — see
  [#233](https://github.com/iowarp/clio-relay/issues/233).
- R3: the per-site error-rationale comments scattered across
  `fastmcp_server.py` (e.g. the `TaskInputParkConflictError` block at
  `:1106-1115`) move into `door_errors.py`'s docstrings once the table they
  explain has one home; the comments are deleted from the call sites, not
  duplicated.
- R5: `transport_probe.py`'s `allow_stcp_fallback` parameter
  (`run_frp_direct_http_probe`, `:338-402`) — confirmed today to have zero
  production reach (only `cli.py`'s probe subcommands and
  `live_acceptance.py` call it, never `control_channel.py`) — must not gain
  production reach when `frp_transport.py` lands. Either the parameter is
  deleted outright once probe-only callers no longer need it, or it stays
  permanently fenced to non-production probe code with a guard comment
  naming why (`connection-model.md`'s "Never do this" section forbids the
  behavior it enables).

**Named-not-tonight (real, deferred, not forgotten):**

- `service_runtime.py`'s status as the largest of the three frp-lifecycle
  copies is tracked as its own issue rather than folded silently into R4's
  general scope — [#233](https://github.com/iowarp/clio-relay/issues/233).
- The 107 hand-rolled `HTTPException` sites in `http_api.py` (§6.2): R3
  gives every surface one table to route *through*; rewriting all 107 call
  sites to use it is explicitly a later, mechanical slice, not part of R3.

## 11. Known deviations

Same discipline as `connection-model.md`'s "Known deviations" section: these
are defects this design pass surfaced, not descriptions of intended
behavior, and each is either already tracked or is tracked as of this
document.

- **`docs/release-gate-1.0.yaml` pins two retired identities.** It targets
  the retired `clio-kit-jarvis-user-v3.6` contract (`:131`, `:320`) against a
  tree that has shipped `v3.7` for multiple release cycles (§7), and pins
  clio-kit `2.6.6` (`:122`, `:231`, `:300`, `:302`) against a tree pinned at
  `2.7.2` everywhere else (`jarvis_mcp.py:32`, `.github/workflows/ci.yml`
  ×2). Both are symptoms of #198 (no single pin registry regenerates fixtures
  together with source) and are exactly what R7's `release_identity.py` is
  scoped to end.
- **No record-time head+tail bound exists for `runner.py`'s `mcp-result.json`
  today**, despite this document's own early working draft assuming one did
  (§6.4). `_write_mcp_result` writes stdout/stderr through unchanged from
  the upstream 32 MiB / 4 MiB read-time caps; the closest real precedent,
  `jarvis_provider.py`'s `_BoundedTextTail`, is tail-only and shares one 1
  MiB bound across both streams rather than the asymmetric split originally
  hypothesized. Tracked in §6.5's ledger, scoped into R6.
- **`allow_stcp_fallback` (`transport_probe.py:338-402`) is exactly the
  automatic mode-switching `connection-model.md`'s "Never do this" section
  forbids**, confined today to non-production probe code only by the
  accident of nothing calling it from `control_channel.py` — not by any
  guard that would stop it from being called that way. §10's deletion ledger
  and §8.2 both flag this so R5 does not inherit it by copy-paste.
- **`RelayJob.last_error` (`models.py:1515`) carries no `max_length` at the
  type level**, unlike its sibling `SchedulerCancelDisposition.last_error`
  (`models.py:292`, `max_length=16_384`). In practice every write site bounds
  it indirectly through `bounded_error_detail()`
  (`command_evidence.py:124-136`, capping to `ERROR_DETAIL_MAX_BYTES = 4_096`
  at `command_evidence.py:11`), so this is not an unbounded field in
  practice — but the type doesn't say so, and a future write site that
  skips `bounded_error_detail()` would silently regress it. Worth a
  `max_length` on the field itself when R6's byte-budget work touches
  `models.py`.

## 12. Issue map

- [#231](https://github.com/iowarp/clio-relay/issues/231) — this campaign's parent issue (design pass + ratchet CI + incremental migration)
- [#220](https://github.com/iowarp/clio-relay/issues/220) — layered acceptance + releases (relay core vs MCP door)
- [#198](https://github.com/iowarp/clio-relay/issues/198) — release-identity pins scattered across 11+ sites
- [#188](https://github.com/iowarp/clio-relay/issues/188) — productionize transport modes (a)/(b)
- [#182](https://github.com/iowarp/clio-relay/issues/182) — one-link control-plane restoration campaign
- [#218](https://github.com/iowarp/clio-relay/issues/218) — door `QueueConflictError` 500 on idempotent replay vs strict task-projection equality
- [#215](https://github.com/iowarp/clio-relay/issues/215) — door `tasks/get` bare `Internal server error` on a terminal-at-birth task
- [#228](https://github.com/iowarp/clio-relay/issues/228) — pinned jarvis-mcp-call endpoint resolves its launcher via the global `current` symlink
- [#232](https://github.com/iowarp/clio-relay/issues/232) — **new, filed by this document.** Client-verifiable per-operation bring-up proof, superseding the connection-lifetime identity nonce (§8.3)
- [#233](https://github.com/iowarp/clio-relay/issues/233) — **new, filed by this document.** Absorb `service_runtime.py`'s frp lifecycle into `frp_link.py` (R4; §4.3, §10)

## 13. Provenance

All line numbers, counts, and code excerpts in this document were verified
against `clio-relay` at commit `ee3120f702acd7dbb529e3548679c457c6b59088`
(branch `feat/231-owner-modules`, tree state as of 2026-08-14) using direct
reads plus targeted `grep`/`wc -l`/AST-parse checks — not from memory or the
originating issue's hint numbers, several of which had drifted (documented
inline at each correction: `cli.py`'s helper fan-out counts, the frp-lifecycle
line ranges in `service_runtime.py`, the wire-model range in
`session_lifecycle.py` (`:891-1435` → `:890-1433`), the owner-token doc
comment (`:126-134` → `:127-134`, load-bearing sentence at `:127-128`), the
v3.7 contract-pin count (13 named sequentially by commit message → 11
non-test/15 total in the current tree), the `HTTPException` count (~40 → 107),
and — most significantly — the claimed record-time head+tail bound in
`runner.py`, which does not exist in code today and is corrected throughout
§6.4/§6.5/§11 rather than reported as shipped).

Representative measurement commands:

```bash
# line counts / ratchet
wc -l src/clio_relay/cli.py
uv run --no-sync python scripts/check_file_size.py
uv run --no-sync python scripts/check_no_class_in_function.py

# function/helper fan-out (example: cli.py's _require_cluster)
grep -c '_require_cluster(' src/clio_relay/cli.py

# monkeypatch coupling
grep -c 'monkeypatch.setattr(cli' tests/test_cli.py

# contract-pin sweep
grep -rn 'v3\.7' src/clio_relay jarvis-packages/clio_relay --include='*.py'

# release-gate self-consistency
grep -n 'contract_id\|clio-kit-2\.\|release_version' docs/release-gate-1.0.yaml

# byte-budget precedents
grep -rn 'MAX_.*BYTES\|MAX_.*CHARS' src/clio_relay/control_channel.py \
  src/clio_relay/mcp_server.py jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py

# gates
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync pyright
```

## Related pages

- [architecture](../architecture.md) — roles, durable records, execution boundary.
- [connection-model](../connection-model.md) — the normative transport/reconnect/staging contract this document's §8 extends into an owner-module map.
- [one-link control plane](../one-link-control-plane.md) — the implemented `ssh_forward` transport this document's §8 generalizes to modes (a)/(b).
