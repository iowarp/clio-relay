# clio-relay architecture — 2026-08 decomposition design

**Status:** R1–R3 complete (2026-08-14); R4–R8+ open ·
**Origin:** owner correction 2026-08-13 — clio-relay drifted back to
monolithic god files · **Tracking:**
[iowarp/clio-relay#231](https://github.com/iowarp/clio-relay/issues/231)
(item 1: this doc). Mirrors the shape of clio-agent's
`docs/design/system-cleanup-2026-07.md`.

---

## 0. Execution status (living)

Kept current as slices land.

| Slice | Scope | State | Commits | Branch | Ratchet delta |
|---|---|---|---|---|---|
| **R1** | Port the file-size + class-in-function ratchet CI from clio-agent, wired into `local.file-size-ratchet`/`local.no-class-in-function` release-gate checks; delete dead `app_profiles/`, `package_adapters/`, stale `TASK.md` | **DONE** | `2713692` (ratchet CI), `429d3cb` (deletions) | `feat/231-owner-modules` | Establishes baselines (39 files file-size, 5 files class-in-function; §3). Deletion commit removed 62 unbaselined lines; zero baselined files touched. |
| **R2** | This design doc; three pre-existing hygiene/gate fixes (`remote_connection.py` pyright, `runner.py` ruff E501, and — found by opus review B1 — a `ruff format` drift on `installation.py`/`remote_mcp.py`/`session_lifecycle.py`) reddening the ratchet-gated release path | **DONE** — re-review approved at `dca177d` | `ee3120f` (hygiene fix), `e078d89` (gate fix, B1), `dca177d` (opus review B1-B5 + F-list revision, approved) | `feat/231-owner-modules` | Net zero on `runner.py` (§10.1) and `installation.py` (B1); `remote_mcp.py` 5309→5308 and `session_lifecycle.py` 8328→8326 ratchet down (B1). |
| **R3** | `door_errors.py` — the one error-translation owner (`classify(exc) -> RelayFault`, `as_mcp_error`/`as_http_problem`/`as_browser_gateway_error`), wired into `fastmcp_server.py`/`http_api.py`/`browser_gateway.py`; `mcp_server.py`'s stdio `_error()` (§6.1's third surface) is not wired, tracked as [#235](https://github.com/iowarp/clio-relay/issues/235) (§6.5) | **DONE** — opus re-review (F1-F16) applied in the same slice | `8d65b91` (pre-existing test-fake fix, found gating this slice), `c2a3a70` (door_errors + the three surfaces), `28e0fb4` (docs landing R3), `7a526e9` (re-review fixes F1-F16), plus the commit landing this revision | `feat/231-owner-modules` | `fastmcp_server.py` 1223→1212 (net −11, deletion outweighs the new call sites); `http_api.py` 3063→3122 and `browser_gateway.py` 826→885 ratchet UP across both passes (net +59/+59), justified in `scripts/check_file_size.py`'s own baseline comments each time (§2 ground rule 5: remove/redesign first — evaluated and rejected, since the growth is real structure — the one global handler, the fourth adapter, the F5 defense-in-depth guard, the F7 typed oversize marker — not a fix that could net negative). `door_errors.py` is new, 667 lines (cap 800). |
| **R4** | `frp_link.py` (~300-400 lines) — `transport_probe.py`'s local-visitor frp logic + `frp_check.py`, the substrate modes (a)/(b) build on. Does **not** include `service_runtime.py`'s copy or `transport_probe.py`'s remote-script generation — that larger absorption is [#233](https://github.com/iowarp/clio-relay/issues/233), sequenced later (§4.3, §8.2, §10) | PLANNED | — | — | — |
| **R5** | `frp_transport.py` — sibling `RelayTransport` implementations for modes (a)/(b) | PLANNED | — | — | — |
| **R6** | `bounded_payload.py` — the T1/T2/T3 byte-budget enforcement + `clio-relay.truncation.v1` | PLANNED | — | — | — |
| **R7** | `release_pins.py` — one `PinSite` registry + bump command + preflight (#198) | PLANNED | — | — | — |
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
| Release-identity pin sites that must move together on a version bump (#198) | 11+ (measured at 1.6.5; §7 recounts the current tree) | 1 (`release_pins.py`'s `PinSite` table) + 1 bump command |
| Bare/untyped error surfaces reaching a client (§6) | `http_api.py`: 107 hand-rolled `HTTPException` sites (`grep -c "raise HTTPException("`, 2026-08-14 — corrected from an earlier ~40 estimate; the file has grown), 0 global exception handlers; 1 deliberately-bare re-raise in `fastmcp_server.py:1106-1115` | 0 unclassified exceptions reach the wire; every surface routes through `door_errors.classify()` |
| `cli.py` inlined domain concerns (§4.1) | 4+ identified (identity/verification, registry mutation, session orchestration, transport validation) | 0 — all delegate to an owner module |
| frp process-lifecycle copies (§4.3) | 3 independent copies (`service_runtime.py`, `transport_probe.py`, `frp_check.py`); `relay_host.py` duplicates none of them — it is the shared config-only renderer all three already import | R4 absorbs `transport_probe.py`'s local-visitor half + `frp_check.py` into `frp_link.py` (~300-400 lines); `service_runtime.py`'s copy + `transport_probe.py`'s remote-script half are #233's larger, later absorption (see §4.3/§8.2/§10) |

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

frp process lifecycle (render config, spawn `frpc`, track, kill) is threaded
through the file: `_local_connector_intent` (`:4488-4501`, desktop-frpc path
identity), `_start_remote_connector` (`:5900-6107`, 208 lines, renders
`render_frpc_config` at `:5978`, launches remotely via SSH script calls
`:5999-6011`/`:6056-6064`), `_stop_allocation_connector` (`:6219-6296`),
`_start_local_visitor` (`:6335-6420`, 86 lines, renders
`render_frpc_visitor_config` at `:6355-6368` then directly `Popen`s the
local frpc binary at `:6381`), `_start_browser_proxy` (`:6421-6508`),
`_remote_allocation_frpc_start_script` (`:7661-7948`, 288 lines — corrected
from an earlier `:7661-7918`/258, which stopped mid-function at a false
`def` — see below), `_remote_frpc_start_script` (`:7951-8186`, 236 lines —
corrected from an earlier `:7951-8188`), `_remote_stop_script`
(`:8421-8650`, 230 lines — corrected from an earlier `:8421-8467`), plus
discovery/status scripts (`:8189-8420`, `:8654+`) and local process-group
verification (`:9732-9866`).

**A double-count this document's own earlier pass made, corrected:**
`signal_owned_processes` at `:8565` is not a second, real top-level function
distinct from `_remote_stop_script` — it is literal shell/Python *text*
inside `_remote_stop_script`'s own `return f"""..."""` body (`:8422-8650`),
part of a `python3 - ... <<'__CLIO_STOP_CONNECTOR__'` heredoc the generator
writes for the remote host to run; the `"frpc"` command-marker match at
`:8607` is inside that same embedded string, not live matching logic in this
module. The real, distinct, local (non-embedded) process-signaling
implementation lives under different names entirely:
`_signal_owned_posix_connector_processes` (`:9948`),
`_open_posix_process_fd` (`:9977`), `_send_posix_process_fd_signal`
(`:9986`). (The naive `grep '^def '` sweep that produced the earlier `:7918`
end-point for `_remote_allocation_frpc_start_script` made the identical
mistake — it matched `def atomic_json(path, payload):` at `:7919`, which is
also embedded heredoc text, not a sibling function.)

This is a genuine third copy, not merely config rendering. `relay_host.py`
(132 lines) is the shared, config-only source — `render_frps_config`
(`:29-40`), `render_frpc_config` (`:75-96`), `render_frpc_visitor_config`
(`:99-123`) — imported by both `service_runtime.py` and `transport_probe.py`.
But two *other* files independently spawn and manage the `frpc` process
themselves: `transport_probe.py`'s `run_frp_http_probe`
(`:211-337`, 127 lines, entirely local/visitor-side: writes the local
visitor TOML at `:253-268`, spawns the local `frpc` visitor process at
`:287-291`, polls `127.0.0.1` healthz at `:300-304` — it calls out to a
separate function, `_remote_probe_script` `:1213-1351` (139 lines,
corrected from an earlier `:1213-1286`/74, the same embedded-heredoc
undercount as above), for remote-script generation rather than embedding it
inline) and the 48-line `frp_check.py`'s `run_frpc_connection_check`
(`:13-40`, 28 lines, spawns
`subprocess.run([frpc_bin, "-c", str(config_path)], ...)` at `:25`). Three
independent "write frpc TOML, then spawn `frpc -c <config>`, then track it"
implementations for one substrate concern — but they are not absorbed in one
slice (§8.2, §9 correct an earlier version of this document that assumed
they were): R4 takes `run_frp_http_probe`'s local-visitor logic and
`frp_check.py` into `frp_link.py`; `service_runtime.py`'s much larger copy
and `_remote_probe_script`'s remote-script generation are
[#233](https://github.com/iowarp/clio-relay/issues/233), a later, separate
absorption.

### 4.4 `session_lifecycle.py` (8,326 lines): state machine + ~19-30 wire models + helpers

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
(`:5315-6218`, ~900 lines), `execute_owned_session_teardown`
(`:6749-7090`).

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
  `:7728`) — a near-duplicate of `runner.py`'s handle-cleanup helper below,
  down to an identically-laid-out `_ByHandleFileInformation` ctypes
  structure and matching Windows constant values (§5's owner-module row).
- **`mcp_server.py` (5,920 lines)** — "Stdio MCP server for relay job
  submission tools." `serve_stdio` (`:421-459`) is the JSON-RPC read loop;
  `_all_tool_definitions` (`:664-1764`, 1,101 lines) is one function
  returning the inline JSON-schema catalog for every relay MCP tool;
  `_call_tool` (`:1778-2217`, 440 lines) is an `if`/`elif` string-match
  dispatcher with exactly 43 `name == "relay_..."` branches routing to
  private business functions (`_submit_jarvis_pipeline` `:4160`, `_wait_job`
  `:3885`), plus two further non-literal branches for dynamic/virtual tool
  routing (`is_virtual_jarvis_tool(name)` `:1850`,
  `catalog is not None and name in catalog.tools` `:1884`). Its own stdio
  `_error(request_id, code, message, *, data=None)` helper (`:5906+`) is one
  of §6's four distinct error-surface shapes (§6.2).
- **`remote_mcp.py` (5,308 lines)** — deliberately separates remote schema
  discovery from local `tools/list` (docstring `:1-7`). 20 top-level
  classes, mostly pydantic models: `RemoteMcpSchemaCache` (`:664-789`, a
  `FileLock`-backed on-disk cache) and `RemoteMcpAcceptanceReport`
  (`:1136-1346`, itself carrying a business method,
  `to_live_validation_report` `:1157`). Catalog assembly
  (`build_virtual_remote_mcp_catalog` `:2125`) sits beside two large
  domain-specific validator families, interleaved with each other and the
  rest of the module rather than contiguous: 22 Spack functions spanning
  `:1369-4524` (1,347 lines total — e.g. `_spack_fresh_install_check`
  `:3378-3454`, `_spack_user_contract_check` `:4353-4524`) and 4
  scientific-catalog functions spanning `:3903-4698` (393 lines total —
  e.g. `_scientific_catalog_structured_result_check` `:3903-4048`) — inside
  a module whose own docstring frames it as generic virtualization (§5's
  validators-owner row).
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
| Error classification/translation | `door_errors.py` | scattered: `fastmcp_server.py` typed conversions (with one deliberately-bare exception, §6.1), `http_api.py` (107 hand-rolled `HTTPException` sites), `mcp_server.py`'s stdio `_error()`, `browser_gateway.py`'s `_error()` (§6.2's fourth surface) | R3 |
| frp process substrate, R4's actual scope: `transport_probe.py`'s local-visitor logic + `frp_check.py` | `frp_link.py` (~300-400 lines) | `transport_probe.py:211-337` (`run_frp_http_probe`, local/visitor-side only) + `frp_check.py:13-40` (`run_frpc_connection_check`); config-only rendering already centralized in `relay_host.py` (reused, not moved) | R4 |
| frp process substrate, the larger absorption: `service_runtime.py`'s copy (§4.3) + `transport_probe.py`'s remote-script generation (`_remote_probe_script:1213-1351`) | `frp_link.py` (extended) + new `frp_remote_scripts.py` (the two embedded remote-script generators, 288+236 = 524 lines, §4.3/§8.2/§10) | `service_runtime.py:5900-8650` (multiple functions), `transport_probe.py:1213-1351` | [#233](https://github.com/iowarp/clio-relay/issues/233) — explicitly NOT R4 (B4 correction to an earlier version of this document) |
| `RelayTransport` implementations for modes (a)/(b) | `frp_transport.py` | `control_channel.py`'s `build_transport` refuses both (`TransportModeUnavailable`, §8.2); `transport_probe.py` has probe-only, non-production logic | R5 |
| Byte-budget enforcement / truncation (T1/T2/T3, §6.4) | `bounded_payload.py` | constants scattered across `control_channel.py`, `remote_connection.py`, `mcp_server.py`, `runner.py` | R6 |
| Release-identity + contract pins (§7) | `release_pins.py` | `pyproject.toml`, `__init__.py`, `models.py` (×3), `jarvis_mcp.py` (×3, incl. `CLIO_KIT_JARVIS_MCP_VERSION`), `cluster_config.py`, `installation.py`, `remote_mcp.py`, `runner.py`, `bootstrap.py`, `.github/workflows/ci.yml` (×2 jobs), `docs/release-gate-1.0.yaml`, `examples/release-gate/*.json`, 4+ test files, plus the stale `docs/remote-mcp-federation.md` mirror (§7) | R7 |
| `cli.py`↔test monkeypatch seam (§4.6) | rework the injection seam itself (no new module — a DI seam `cli.py` exposes so extractions don't break 236+28 patch sites) | `tests/test_cli.py`, `tests/test_acceptance_report_defaults.py` | R8+ |
| `cli.py` shared plumbing (§4.1: `_run_or_exit` ×74, `_require_cluster` ×56, `_write_failed_acceptance_report` ×19, `_resolve_env_secret` ×19, `_acceptance_report_command` ×17, `default_report_path` ×18) | `cli_support.py` | `cli.py:19307`, `:19132`, `:18908`, `:19212`, `:838`; `default_report_path` imported from `validation_report.py:2006` | R8+ |
| `relay-host` command-module extraction (parsing/rendering only, ground rule 2) | new `cli_commands/relay_host.py`-shaped module (exact name TBD at R8+; owns `relay_host_app`'s 7 commands) | `cli.py` (`relay_host_app`) | R8+, sequenced after R5 (§9 overlap) |
| `session_lifecycle.py` wire models (§4.4) | a dedicated wire-model module (exact name TBD at R8+) | `session_lifecycle.py:890-1433` | R8+ |
| `session_lifecycle.py`'s state machine (§4.4: `inspect_owned_session_recovery_status`, `execute_owned_session_start`, `execute_owned_session_teardown`) | `session_lifecycle.py` itself — already the correct home; this row exists because every §4 concern gets a §5 row, and the state machine's "extraction" is simply what remains once the wire-models row above moves out | `session_lifecycle.py:2417-3098`, `:5315-6218`, `:6749-7090` | completes alongside R8+'s wire-model split, not a separate extraction |
| Sidecar/snapshot Windows file-handle cleanup (near-duplicated, not importable across the boundary) | no shared import is possible — `runner.py` is a separately wheel-packaged subprocess entry point (`pyproject.toml:44`/`:50`; no `src/clio_relay/mcp_call/`; launched via `sys.executable` at `endpoint.py:7157-7176`, own `__main__` at `runner.py:5757-5758`), so the honest resolution mirrors `process_containment.py`: keep two implementations, add a test policing byte-identity of the genuinely-shared substructure (the `_ByHandleFileInformation` ctypes layout + Windows constants), the same discipline as `tests/test_process_containment.py:50-55` (§7) | `endpoint.py:7728` (`_quarantine_windows_sidecar_by_handle` + 4 siblings `:7839-8000`), `runner.py:2691` (`_open_windows_snapshot_cleanup_handle` + 4 siblings `:2751-2959`) | unsequenced, R8+ or later — small and low-priority once named |
| `runner.py`'s other two inventoried concerns (§4.5): the hand-rolled subprocess JSON-RPC client, and Python wheel/distribution identity verification for the clio-kit runtime | named, not yet split into owners — no shared import is possible here either, same packaging constraint as the row above | `_open_process` (`:5311`), `_write_message` (`:5165`), the reader thread + `Queue` (`:5222`/`:5266`) for the JSON-RPC client; `_verified_wheel_archive` (`:4620`), `_installed_clio_kit_runtime_identity` (`:4030`) for wheel-identity verification | unsequenced — named, not sequenced, since neither has a concrete extraction target yet |
| `remote_mcp.py`'s Spack + scientific-catalog validator families (§4.5) | a `validators`-shaped owner module (exact name TBD) | `remote_mcp.py:1369-4524` (Spack, 22 functions/1,347 lines) + `:3903-4698` (scientific-catalog, 4 functions/393 lines) — interleaved with each other and with catalog assembly, so extraction needs reordering, not just a cut | unsequenced, post-campaign |
| `mcp_server.py`'s tool catalog + dispatcher (§4.5) | a catalog/dispatch owner module (exact name TBD) | `mcp_server.py:664-1764` (`_all_tool_definitions`, the JSON-schema catalog) + `:1778-2217` (`_call_tool`, the 43-branch dispatcher, §4.5) | unsequenced |
| `bootstrap.py`'s three collided concerns (§4.5: archive packaging, SSH orchestration + receipt validation, two embedded shell-script-template renderers at ~50% of the file) | named, not yet split into owners | `bootstrap.py` (8,733 lines, whole file) | unsequenced |
| `endpoint.py`'s `EndpointWorker` (job execution, scheduler cancellation, lease renewal, §4.5) plus its ~90 unrelated module-level functions (Jarvis execution-recovery bookkeeping, sidecar cleanup — row above) | named, not yet split into owners | `endpoint.py` (8,710 lines, whole file) | unsequenced |
| Identity/verification (the six-sites example, §1) | not yet sequenced — needs its own slice number beyond R8+ | `cli.py:14786` (`_verify_owner_session_teardown`), `remote_mcp.py` (×2), `endpoint.py`, `session_lifecycle.py`, `remote_cli.py` | unsequenced (§9 flags this explicitly rather than silently dropping it) |
| Registry mutation (`cli.py`'s `cluster_add`) | `ClusterRegistry.mutate` already exists as the storage primitive (`cluster_config.py`); the gap is the ~220 lines of construction/validation logic still inlined in the command | not yet sequenced | unsequenced |
| Session orchestration (`cli.py`'s `session_teardown`, ~1365 lines) | `session_lifecycle.py` already owns session state; teardown orchestration should call into it instead of duplicating decisions in the command | not yet sequenced, overlaps the R8+ wire-model split | unsequenced |
| Transport validation (`cli.py`'s `_run_transport_validation`) | folds naturally into `frp_transport.py` once modes (a)/(b) are real implementations, not probes | not yet sequenced | unsequenced, logically after R5 |
| `core_queue.py`'s five concerns (§4.2: storage/idempotency/leases/task-projection/schemas) | five-way split, each concern its own owner | `core_queue.py` (16,137 lines, one class) | not yet sequenced — the largest remaining monolith and out of scope for R3-R8+ |

Rows marked "not yet sequenced"/"unsequenced" are named here deliberately
(ground rule 4: deletions and gaps are first-class, not silently dropped)
rather than being assigned a slice number this document cannot yet justify
with a concrete extraction plan. Every concern inventoried in §4 now has a
row above — the five that were missing in an earlier version of this
document (sidecar cleanup, validators, catalog/dispatch, `cli.py` shared
plumbing, the state machine) plus explicit `bootstrap.py`/`endpoint.py` rows
were added per opus review B2.

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
of an already-caught exception reaching a wire response is a widespread
pattern: **199 sites in `src/clio_relay` (`grep -rn "str(exc)" src/clio_relay
--include="*.py" | wc -l`), 208 including `jarvis-packages/`** (corrected
from an earlier, badly undercounted "~59+" that mistook `http_api.py`'s own
56 for most of the total, rather than ~28% of it). Top holders:
`http_api.py` 56, `cli.py` 30, `session_lifecycle.py` 24,
`service_runtime.py` 23, `endpoint.py` 9, `transport_probe.py` 7. Every
observed instance wraps a curated domain exception's message, not a bare
traceback, but the volume and lack of a single owner is exactly the concern
§5 assigns to `door_errors.py`.

**A fourth error surface, distinct from the other three.**
`browser_gateway.py:692`'s `_error(self, status: int, message: str) -> None`
(13 call sites, `:510-654`) is the error path of `CapabilityProxyHandler`
(`:461+`), the `http.server`-based (not FastAPI, not FastMCP) loopback
proxy that gates browser-originated requests to sandboxed viewer processes
behind a capability token. It returns a bare `{"error": message}` JSON body
with a raw HTTP status — no `code`, `data`, `reason`, or `detail` field at
all, sharing none of the shapes of the other three surfaces below
(`fastmcp_server.py`'s `MCPError`, `http_api.py`'s `HTTPException`,
`mcp_server.py`'s own stdio `_error(request_id, code, message, *,
data=None)` JSON-RPC envelope at `:5906+`). It is the surface with the
least existing structure to build on, which is exactly why §6.2 folds it
into R3's scope rather than deferring it: there is no partial migration to
preserve, only a bare dict to replace.

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
plus `as_mcp_error`/`as_http_problem`/`as_browser_gateway_error` adapters
over that one table — **four** call surfaces (`fastmcp_server.py`,
`http_api.py`'s handlers, `mcp_server.py`'s stdio `_error`, and
`browser_gateway.py`'s `_error`, §6.1), one classification. `browser_gateway.py`
is folded into R3's scope despite being found only during this revision
(B3): it is small (13 call sites, one helper function) and has the least
existing structure of the four, so bringing it in now costs little and
avoids leaving a freshly-discovered fourth surface unaddressed by the same
slice that names it. The 107 hand-rolled `HTTPException` sites in
`http_api.py` are explicitly **not** deleted by R3 — replacing 107 call
sites mechanically is its own later slice, named here so R3 is not judged
half-done for leaving them. R3's job is narrower and load-bearing: give every
surface one table to route *through*, so the next 107-site sweep is
mechanical instead of another archaeology expedition.

### 6.3 The agent-facing contract `clio-relay.error.v1`

Proposed by this document for R3/R6 (no such schema exists in code today —
confirmed by a repo-wide search). **Codes are derived from the `REASONS`
table, never chosen at call sites**: a raise site produces (or is classified
into) a `reason`; `door_errors.classify()` looks up that reason's
`retryable`/`mcp_code`/`http_status` from one table. A call site never picks
its own status code — that is precisely the discipline the six-sites and
13-copy examples (§1) show this codebase doesn't have today.

**The seed `REASONS` set**, verified against real exception types and real
raise sites rather than invented (adjusted from an initial candidate list —
see corrections inline). **`mcp_code` reflects the shipped, post-re-review
values** — the opus review of the initial R3 landing (F1) found five of
these custom codes squatting on the MCP SDK's own reserved `-32000..-32019`
band (`mcp_types.jsonrpc.REQUEST_TIMEOUT` is `-32001`, the exact value
`mcp_task_input_park_conflict` originally used — a client that discriminates
by code alone, e.g. clio-agent's `tools/mcp_errors.py`, would have read a
park conflict as a transport timeout and retried it with timeout semantics).
Relay-owned custom codes now live in `-32050..-32059` instead; `-32007`
(`storage_admission_refused`) is the one deliberate exception, kept because
it is already shipped and pinned (`tests/test_production_admin_surfaces.py`):

| reason | retryable | mcp_code (shipped unless noted) | http_status | grounded in |
|---|---|---|---|---|
| `mcp_task_input_park_conflict` | true | `-32050` (was `-32001`, reallocated by F1) | 409 | `TaskInputParkConflictError` (`errors.py:20-31`), raised `fastmcp_server.py:528-530` inside `RelayMcpRuntime._park_agent_input` — **note:** `errors.py:27-30`'s docstring used to say "`RelayTasksExtension._park_agent_input`," which was stale; fixed in R3 to name the real owner, `RelayMcpRuntime`. |
| `mcp_task_conflict` | false | `INVALID_PARAMS` (`-32602`) | 409 | the task-identity-reuse `QueueConflictError` raised in `put_mcp_task` (`core_queue.py:7502-7504`). **Caveat**: bare `QueueConflictError` is raised 651 times across `core_queue.py` for unrelated invariants (lease-capacity gates, index migration, sealed-checkpoint validation, ...) — classifying by `isinstance(exc, QueueConflictError)` alone would over-match. R3 keys this reason off the MCP-task-scoped call path (`classify(exc, reason="mcp_task_conflict")`), not the exception type alone. |
| `mcp_task_status_reconciliation_failed` | true | `INTERNAL_ERROR` (`-32603`) | 500 | **already shipped, not just proposed**: `fastmcp_server.py:1003-1037`'s `_handle_get` catch-all already raises exactly `MCPError(code=INTERNAL_ERROR, ..., data={"reason": "mcp_task_status_reconciliation_failed", ...})` (citing #215). R3 adopts this reason string as-is into the frozen set rather than renaming shipped behavior. |
| `jarvis_dispatch_refused` | false | `INVALID_PARAMS` (`-32602`) | 422 | `JarvisDispatchRefusal` (`jarvis_dispatch_failure.py:32-55`) — **a different shape than the other nine**: a frozen dataclass a durable `jarvis_run` result *carries*, not a raised-and-caught exception. `door_errors.classify()` has an object-typed entry point (not just an `except`-clause dispatch) for it. **Declared, not yet emitted** (F11): no production call site constructs a durable `jarvis_run` result and hands its refusal to `classify()` today — this reason is forward-declared contract, exercised only by `tests/test_door_errors.py`'s own unit test. |
| `not_found` | false | `INVALID_PARAMS` (`-32602`) | 404 | `NotFoundError` (`errors.py:34-35`), 14 raise sites. Existing precedent (`fastmcp_server.py:994-1001`) already maps it to `INVALID_PARAMS`, not a not-found-shaped code — R3 keeps that mapping rather than "fixing" shipped behavior as a side effect of a naming pass. |
| `configuration_error` | false | `INVALID_PARAMS` (`-32602`) | 400 | `ConfigurationError` (`errors.py:12-13`), by far the heaviest-used typed exception: **1,084 raise sites across 35 files**. Genuinely heterogeneous (client-schema mismatches and server-side misconfiguration both raise it) — flagged here, not resolved, as a bucket that may need finer-grained sub-reasons in a later slice rather than one blanket mapping. |
| `storage_admission_refused` | true | `-32007` | 507 | `StorageAdmissionError(StorageRuntimeError)` (`storage_runtime.py:76-77`). **Already shipped, not proposed**: `mcp_server.py` already emits `-32007` with `data={"storage_decision": ...}`, `http_api.py:1225,1431` already map it to HTTP 507; `cli.py` renders it via `_echo_storage_admission_error` (`:11355`, `:12792`, `:19310`). R3 adopts both codes as-is, and is the one reason F1's -32050..-32059 reallocation deliberately did NOT touch (see above). |
| `storage_safety_violation` | false | `-32051` (reallocated by F1, was `-32008`) | 507 | a verification-pass gap beyond the seed ten: `StorageRuntimeViolation(StorageRuntimeError)` (`storage_runtime.py:80-81`), "raised after a running child crosses a durable storage safety boundary" — **not** a subclass of `StorageAdmissionError` above, so it needs its own reason rather than inheriting one. Not retryable — unlike admission, the boundary was already crossed, so retrying the same job does not undo it. Already caught by name at a public boundary, previously reasonless. |
| `observation_timeout` | true | `-32052` (reallocated by F1, was `-32002`) | 504 | `ObservationTimeoutError` (`errors.py:8-9`), 11 raise sites split across four files: `cli.py` ×5, `mcp_stdio_validation.py` ×4, `remote_cli.py` ×1, `remote_connection.py` ×1 (corrected from an earlier "all in `cli.py`"). |
| `launcher_resolution_failed` | false | `-32053` (reallocated by F1, was `-32003`) | 409 | the real function is `jarvis_mcp_command()` (`jarvis_mcp.py:236-346`), which raises bare `ValueError` at several sites (`:303`,`:312`,`:338`,`:342`,`:345`). **The gap**: `ValueError` is reused for dozens of unrelated failures throughout `jarvis_mcp.py` (contract loading, env parsing, discovery-cache validation), so type-based classification cannot isolate this reason on its own — either `jarvis_mcp_command` needs a distinct typed exception, or the two consumers (`http_api.py:1753-1758`, already mapping to HTTP 409, citing #228; `endpoint.py:5949-5951`, which today absorbs it into a bool+message tuple and never re-raises it at all) must convert before the door boundary. Out of scope for R3 to fix at the real site; named so R3 doesn't silently assume a type-based mapping that doesn't work here. **Declared, not yet emitted** (F11): the real #228 site is not wired through `door_errors` — this reason's shape is proven by an adapter-contract test on a synthetic route (`tests/test_door_errors.py`), not a live regression. |
| `owner_session_identity_refused` | false | `-32054` (reallocated by F1, was `-32004`) | 422 | a verification-pass gap beyond the seed ten: `OwnerSessionIdentityError(RelayError)` (`job_identity.py:39-40+`), already caught by name at `http_api.py:1158-1159` and `:2965`, mapped to HTTP 422 today. |
| `internal_error` | false | `INTERNAL_ERROR` (`-32603`) | 500 | the fallback for any exception **not** in this table — new vocabulary, not an existing generic bucket (the one live use of `INTERNAL_ERROR` today, `fastmcp_server.py:1031`, already carries the *specific* `mcp_task_status_reconciliation_failed` reason, not a generic one — R3 keeps that specificity rather than collapsing it). Traceback logged once, server-side, via `logger.exception(...)` (the existing `fastmcp_server.py:1029` pattern), never placed on the wire — this is what makes §3's "0 unclassified exceptions reach the wire" criterion meetable: every exception gets *some* reason, `internal_error` is simply the one nothing else claims. |
| `payload_too_large` | false | `-32055` (F7+F14, added by the R3 re-review) | 413 | `browser_gateway.py`'s `_request_body`'s own oversize check (`length > MAX_REQUEST_BODY_BYTES`) — a distinct, well-known HTTP concept in its own right, given its own typed `_RequestBodyTooLargeError` marker rather than folded into `configuration_error`'s blanket 400 alongside three unrelated protocol-validation failures (chunked encoding, a malformed `Content-Length`, a body that ended early), which still map to `configuration_error`. |

All 13 rows are frozen-set members (`tests/test_door_errors.py::test_every_reason_is_registered`); §6.5 below tracks which are live-emitting vs. declared-but-not-yet-emitted contract.

**Frozen-set discipline.** `REASONS` is a closed, tested set (a
parametrized test asserting the frozen collection's exact membership, in
the spirit of `tests/test_file_size_ratchet.py`'s baseline-only-shrinks
discipline but for a *set* rather than a *ceiling*). Adding a reason is a
deliberate contract change that edits the test alongside the table — never
a silent side effect of some other change. This is what makes `reason`
safe for an agent to pattern-match on: the vocabulary doesn't drift under
it between releases.

**The HTTP envelope — RFC 7807 (`application/problem+json`) plus
extensions.** `as_http_problem` produces:

```json
{
  "type": "urn:clio-relay:error:mcp_task_input_park_conflict",
  "title": "MCP task input park conflict",
  "status": 409,
  "detail": "the task's input round could not be admitted after CAS retries",

  "schema_version": "clio-relay.error.v1",
  "reason": "mcp_task_input_park_conflict",
  "retryable": true,
  "cluster": "ares",
  "job_id": "job-...",
  "task_id": "task-...",
  "evidence": {"artifact_id": "art-..."},
  "truncation": null
}
```

The first four fields (`type`, `title`, `status`, `detail`) are the RFC 7807
members — `type` is `urn:clio-relay:error:<reason>` (stable, dereferenceable
in spirit even though nothing is served at that URN today), `title` a short
human phrase derived from the reason, `status` and `detail` the HTTP-facing
restatement of the same facts the MCP path carries as `mcp_code`/`message`.
The remaining members are this document's extension: `schema_version`,
`reason`, `retryable`, the optional `cluster`/`job_id`/`task_id` triple,
`evidence` (an artifact-id *reference*, never inline bytes — §6.4's T3), and
`truncation` (the `clio-relay.truncation.v1` record, §6.4). `message`/`detail`
stay ≤2000 chars (T1, §6.4, hard-truncated by `_bounded_text`) — `truncation`
is `null` only when nothing was actually elided; a document whose `detail`
*was* cut must never claim `"truncation": null` (F2, opus re-review — the
original R3 landing hardcoded `null` unconditionally, a false statement on
any truncated document).

**Contract members always win (F4).** The envelope is built as
`{**fault.data, <the fields above>}` — `fault.data`'s keys are spread
first, and the RFC 7807/extension fields above are applied on top, so a
colliding data key (e.g. a caller accidentally passing
`data={"status": ...}`) can never shadow the real classified value.
`as_mcp_error`'s `data={**fault.data, "reason": fault.reason}` follows the
same discipline for `reason`.

**The whole envelope stays ≤8KiB, with no silent pass-through (F3).**
Overflow drops `evidence` first, then `truncation`, then any OTHER
extension member in a deterministic order — a single oversized or
non-JSON-serializable extension value that isn't literally named
`evidence` must not sail through unbounded (the original enforcement's
exact gap: an injected 9KiB member reached the wire at 11,213 bytes). Only
once every extension member is gone does the fallback truncate `detail`
itself and stamp `envelope_overflow: true`; the RFC 7807 core four plus
`reason`/`retryable` are never dropped, and `detail` is never reduced to an
empty string. Size is measured with `ensure_ascii=False` (F10) — the
default `ensure_ascii=True` inflates every non-ASCII character to a 6-byte
`\uXXXX` escape, materially overstating cost against the actual wire
encoding.

**Guarded against hostile input, not just malformed structure (F5).** A
raising `__str__` on the classified exception, or a typed exception whose
extension-data extraction (`.decision`/`.detail`) itself raises, must never
escape `classify()` — on the HTTP surface an unguarded crash there would
collapse straight into Starlette's `ServerErrorMiddleware`, replacing the
original exception with a new, undiagnosable one and losing the typed
response this contract exists to guarantee. `http_api.py`'s global handler
adds a second, independent guard around the whole
`classify()`/`as_http_problem()` call, falling back to a hardcoded
`internal_error` document if `door_errors` itself somehow still fails.

Tracebacks or raw `str(exc)` of an *unclassified* exception must never
reach the wire — the existing `str(exc)`-of-a-typed-exception pattern
(§6.1, 199/208 sites) is compatible with this contract once each site
routes through `door_errors.classify()` instead of formatting the
exception locally. `REASONS` itself is a `MappingProxyType` (F9) — read-only
at the type level, not merely by convention — and `classify()`'s table
injection point for tests is the private `_table=` keyword, not a public
part of the contract.

**Logging is "once" per this module, not per process (F6).** `internal_error`
logs its traceback exactly once here, via `logger.exception`. On the HTTP
surface, Starlette's `ServerErrorMiddleware` re-raises after sending the
response it built from the handler's return value (by design, so a real
ASGI server can still observe the error) and uvicorn logs that re-raise too
— a second, server-side-only log line there is expected, not a defect. A
streaming response that has already started sending bytes before an
exception is never covered by this contract either way: nothing can rewrite
headers/status once a response is underway, so a mid-stream failure stays a
transport-level cut, not a `clio-relay.error.v1` document.

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
`{schema_version, truncated, retention: "head"|"tail"|"head_tail",
original_bytes, retained_head_bytes, retained_tail_bytes, elided_bytes,
marker, evidence_ref}`. `retention` includes `"tail"` (not just
`"head"`/`"head_tail"`) specifically to describe the *existing*
`_BoundedTextTail` shape (§6.4) honestly once it adopts this schema, rather
than forcing R6 to redesign a working tail-only precedent into a head+tail
one it doesn't need. A single `retained_bytes` field is ambiguous once
`retention` can be `"head_tail"` (two separate retained spans, not one), so
the schema splits it into `retained_head_bytes`/`retained_tail_bytes` —
either is `0` when `retention` doesn't include that side (e.g.
`retained_head_bytes: 0` for `retention: "tail"`). The marker string
`"[clio-relay: elided N bytes of stdout]"` is written full-line, in-band, at
the elision point.

### 6.5 Specified-but-not-implemented ledger

| Item | Spec status | Code status | Issue |
|---|---|---|---|
| `door_errors.py` — one classify/adapt owner (four surfaces) | Specified (§6.2) | **DONE (R3)** — `classify`/`as_mcp_error`/`as_http_problem`/`as_browser_gateway_error`, wired into `fastmcp_server.py`/`http_api.py`/`browser_gateway.py`. `mcp_server.py`'s stdio `_error()` (§6.1's *third* surface named, not the fourth — `browser_gateway.py` is §6.1's fourth surface; an earlier revision of this ledger mislabeled the two) is not wired through it yet | [iowarp/clio-relay#235](https://github.com/iowarp/clio-relay/issues/235), tracked under #231 (R3) |
| `clio-relay.error.v1` agent-facing envelope (RFC 7807 + extensions) | Specified (§6.3) | **DONE (R3)** — the core four plus `schema_version`/`reason`/`retryable`/`truncation`, contract-members-always-win construction (F4), the ≤8KiB budget fully enforced with no silent pass-through (F3), hostile-input guards (F5); `cluster`/`job_id`/`task_id`/`evidence` context threading is not yet wired at any call site (R6 scope, §6.4) | tracked under #231 (R3/R6) |
| `REASONS` frozen set + membership test | Specified (§6.3) | **DONE (R3)** — 13 rows (the seed ten, the two verification-pass gaps, and `payload_too_large` added by the re-review), `tests/test_door_errors.py::test_every_reason_is_registered`; all relay-owned MCP codes moved out of the SDK's reserved `-32000..-32019` band (F1). Two rows (`jarvis_dispatch_refused`, `launcher_resolution_failed`) are declared, tested contract with no production call site emitting them yet (F11) | tracked under #231 (R3) |
| `browser_gateway.py`'s `_error()` routed through `door_errors.classify()` | Specified (§6.1, §6.2) | **DONE (R3)** for the two exception-path call sites (`:509`/`:580` `except ValueError`); the oversize branch specifically gets the dedicated `payload_too_large` reason (F7+F14), the other three `_request_body` failures stay `configuration_error`. The other 11 `_error()` call sites are access-control decisions with hardcoded statuses, out of R3's stated scope | tracked under #231 (R3) |
| T1 refusal-text truncation (`_bounded_text`, §6.4) | Specified (§6.3/§6.4) | **DONE (R3 re-review, F2)** — `_bounded_text` returns a populated `clio-relay.truncation.v1` record whenever it actually cuts a message; `as_http_problem`'s `truncation` field reflects it (never a hardcoded `null` on a truncated document) | tracked under #231 (R3) |
| `clio-relay.truncation.v1` + head+tail T3 record-time bounding | Specified (§6.4) | Not started — no head+tail bound exists in `runner.py` today, only the tail-only, non-split `_BoundedTextTail` in `jarvis_provider.py`. Distinct from the T1 row above, which R3's re-review did implement | tracked under #231 (R6) |
| `TaskInputParkConflictError` typed conversion (replace the bare re-raise) | Specified (§6.1) | **DONE (R3)** — `fastmcp_server.py`'s `intercept_tool_call` now raises `door_errors.as_mcp_error(door_errors.classify(exc))`, closing the live hole | tracked under #231 (R3) |
| `http_api.py`'s 107 `HTTPException` sites routed through `door_errors.classify()` | Specified (§6.2, explicitly deferred) | Not started | later slice beyond R3, named not tonight (§10) |
| `release_pins.py`'s `PinSite` table + bump command + preflight | Specified (§7) | Not started | #198, tracked under #231 (R7) |
| `frp_link.py` (R4 scope: `transport_probe.py` local-visitor + `frp_check.py`) | Specified (§4.3, §5, §8.2) | Not started | tracked under #231 (R4) |
| `frp_link.py` extension + `frp_remote_scripts.py` (`service_runtime.py`'s copy) | Specified (§4.3, §5, §8.2, §10) | Not started | [#233](https://github.com/iowarp/clio-relay/issues/233), separate from R4 (B4 correction) |

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
`release_pins.py`'s `PinSite` table (§9) exists to end.

**Live holes this document surfaces, not previously tracked (recounted
exactly, correcting an earlier pass's approximate line list):**
`docs/release-gate-1.0.yaml` pins the *retired* `clio-kit-jarvis-user-v3.6`
contract or the bare string `v3.6` at exactly **4 lines** (`:131`, `:320`,
`:1109`, `:1115`) against a tree that has shipped `v3.7` for multiple
release cycles — a fixture that drifted because nothing regenerates it
together with the source it's meant to validate, exactly the failure mode
#198 describes. The same file *also* pins clio-kit `2.6.6` at exactly **13
lines** (`:115`, `:121`, `:122`, `:226`, `:230`, `:231`, `:294`, `:299`,
`:300`, `:302`, `:309`, `:374`, `:1187`) while `.github/workflows/ci.yml`
(`:62-64`, `:166-168`, two separate build jobs) and
`src/clio_relay/jarvis_mcp.py:32` (`CLIO_KIT_JARVIS_MCP_VERSION = "2.7.2"`,
the sole literal `"2.7.2"` in the entire tree) all pin `2.7.2`. **Correction
to an earlier version of this document**: there is no literal `"2.7.2"`
duplicate at `bootstrap.py:5976-5977`/`:7367-7368` to go stale — both are
indirect references to the `CLIO_KIT_JARVIS_MCP_VERSION` constant (an
f-string interpolation and a shell case-arm respectively), not hardcoded
copies. `bootstrap.py:7351` (`JARVIS_MCP_VERSION="${CLIO_KIT_JARVIS_MCP_VERSION}"`)
is the one templated-placeholder site worth recording in `PinSite` — as a
placeholder, not a literal (§7's selector taxonomy, below). Two independent
staleness bugs in one fixture file, both symptoms of the same missing
`PinSite` registry.

**A third staleness bug, found verifying the "kit-pin digests" claim
below — this one a real content bug, not a citation nit.**
`docs/remote-mcp-federation.md:471` correctly names the default wheel as
`clio_kit-2.7.2-py3-none-any.whl`, but `:474` then calls its "canonical
contract" `clio-kit-jarvis-user-v3.6` and `:476` points at
`_contracts/jarvis-user-v3.6.json` — both should read `v3.7`
(`jarvis_mcp.py:41`'s `CLIO_KIT_JARVIS_USER_CONTRACT_ID`, `:80`'s contract
path). Worse: the SHA-256 digests quoted at `:479`/`:481`/`:483` are not
merely stale numbers — they are the exact **legacy v3.6** entries from
`remote_mcp.py`'s `CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID`/
`_WIRE_SHA256_BY_ID`/`_ARTIFACT_SHA256_BY_ID` tables (`:107`, `:115`,
`:123`), not the current v3.7 entries (`:104-105`, `:112-113`, `:120-121`).
The doc pairs the *current default wheel* with the *legacy* contract ID and
its legacy digests — a doc that would mislead an integrator into pinning a
retired contract against a current wheel, not just a cosmetic drift.

**Kit-pin digests.** The clio-kit wheel identity — filename, download URL,
and SHA-256 — is pinned twice in CI (`ci.yml:62-64` and `:166-168`, one pin
per build job) and referenced (correctly, for the wheel filename itself) in
`docs/operations.md:719` and `docs/remote-mcp-federation.md:471`;
`docs/release-gate-1.0.yaml` carries the stale `2.6.6` variant noted above
instead, and `remote-mcp-federation.md`'s contract-ID/digest trio noted
above is the separate, worse staleness bug just described.

**R7's target:** one `release_pins.py` module holding a `PinSite` table, one
bump command that rewrites every site and recomputes self-digests (the
`validate_release_acceptance_matrix` logic #198 proposes reusing), and one
fast `release preflight` check that asserts the whole identity contract is
internally consistent in seconds — replacing a full `validate-local` battery
run as the only way to catch drift today.

A single "line-or-key selector" is not enough to address every site found
above; `PinSite` needs a small selector taxonomy, evidenced directly by the
sites this section found:

- **line** — a fixed line number holding a literal (`pyproject.toml:3`,
  `src/clio_relay/__init__.py:5`).
- **key** — a structured key inside a JSON/YAML document, addressed by key
  path rather than line number since serialization can reorder keys
  (`docs/release-gate-1.0.yaml`'s `release_version:`/`acceptance_matrix_sha256:`
  keys, `examples/release-gate/report-matrix-1.0.json`'s
  `"release_version"`/`"matrix_sha256"` keys).
- **filename** — the pin is embedded in a filename or URL string, not a
  standalone value (`clio_kit-2.7.2-py3-none-any.whl`, referenced whole in
  `ci.yml`, `docs/operations.md`, `docs/remote-mcp-federation.md`).
- **placeholder** — a site that references the single source of truth
  indirectly (a variable interpolated into an embedded shell-script
  template) rather than holding a literal copy — `bootstrap.py:7351`'s
  `${CLIO_KIT_JARVIS_MCP_VERSION}`-shaped placeholder inside
  `render_linux_user_bootstrap_script`'s template string (§13's correction:
  there is no literal `"2.7.2"` duplicate at that site to go stale, only the
  one real definition at `jarvis_mcp.py:32`) — `PinSite` should record these
  too, distinctly, so a future audit doesn't re-flag a placeholder as a
  missed literal.
- **regex** — a pin recognized by pattern rather than an exact key/line, for
  fixture files where the same value recurs at varying, not-otherwise-typed
  locations (`docs/release-gate-1.0.yaml`'s repeated `2.6.6`/`v3.6` literals
  across unrelated YAML blocks — 13 and 4 sites respectively, §7 recount).
- **derived-digest-with-ordering** — a value computed FROM other pins
  (`acceptance_matrix_sha256`/`matrix_sha256`) that must be recomputed
  strictly *after* every other pin in its family is updated, never
  independently — the bump command's ordering constraint, not just its
  rewrite set.

Four of these six kinds (filename, placeholder, regex,
derived-digest-with-ordering) are not simple line/key literals — that is the
evidence this section's own recount surfaces for why `PinSite` needs the
taxonomy rather than a single selector shape.

**Precedent for byte-identical enforcement:** `jarvis-packages/clio_relay/clio_relay/process_containment.py`
is a deliberately vendored, byte-identical copy of
`src/clio_relay/process_containment.py`, policed by
`test_embedded_containment_source_is_an_exact_isolated_runtime_mirror`
(`tests/test_process_containment.py:50-55`), which reads both files
(`:52-53`) and asserts `embedded.read_bytes() == source.read_bytes()`
(`:55`). This is the model for how `release_pins.py` should treat any
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

**R4's actual budget, corrected** (an earlier version of this document
assumed R4 absorbs all three frp-lifecycle copies from §4.3 in one slice —
opus review B4 caught that the resulting ~1,900-line estimate silently
folded in `service_runtime.py`'s copy, which is out of scope here):
`frp_link.py` (R4, the substrate) is sized from just
`transport_probe.py`'s `run_frp_http_probe` (`:211-337`, 127 lines, purely
local/visitor-side) and the 48-line `frp_check.py`'s
`run_frpc_connection_check` (`:13-40`, 28 lines) — roughly 300-400 lines
with a reasonable amount of config/health glue, comfortably under the
800-line new-file cap (§2, ground rule 6). `service_runtime.py`'s much
larger copy (§4.3: `_start_remote_connector`, `_start_local_visitor`,
`_remote_allocation_frpc_start_script`, `_remote_frpc_start_script`,
`_remote_stop_script`, spanning `:5900-8650`) and `transport_probe.py`'s own
remote-script generator (`_remote_probe_script`, `:1213-1351`, 139 lines) are
**not** R4 — they are [#233](https://github.com/iowarp/clio-relay/issues/233),
a later, separate absorption landing as a planned two-file split:
`frp_link.py` (extended) plus a new `frp_remote_scripts.py` housing the two
large embedded remote-script generators
(`_remote_allocation_frpc_start_script` 288 lines +
`_remote_frpc_start_script` 236 lines = 524 lines, §4.3). `frp_transport.py`
(R5, the transports) hosts the new `RelayTransport` implementations for
`brokered_tcp`/`udp_rendezvous`, built on `frp_link.py` rather than
reimplementing process management a fourth time. Both R4 and the eventual
#233 modules reuse `relay_host.py`'s existing config-only renderers
(`render_frps_config` `:29-40`, `render_frpc_config` `:75-96`,
`render_frpc_visitor_config` `:99-123`) unchanged — that file is already
the correctly-scoped single owner for TOML rendering; nothing here
duplicates it.

`transport_probe.py`'s `allow_stcp_fallback` parameter
(`run_frp_direct_http_probe`, `:338-402`; declared `:352` with
**`bool = True` — the default itself is the hazard**, not a call site
opting in) is FORBIDDEN in production: on a `RelayError` from the XTCP
direct-HTTP attempt (`:356-370`), when true it re-runs as an STCP
relay-point-carried attempt (`:375-388`). This is *automatic*, not silent —
it happens visibly whenever triggered — but defaulting to `True` means
every caller gets it without opting in, which is the practical hazard:
`connection-model.md:85-86` states, under the "(c) SSH port forward"
transport-modes section (not, as an earlier version of this document
mis-cited, under "Never do this" — that section's nine bullets, `:273-304`,
don't mention mode-switching at all): *"The relay never switches modes on
its own — a connection whose configured link fails reports a typed link
failure and re-establishes the **same** configured mode; it does not try
another transport."* `allow_stcp_fallback=True` does exactly what that
sentence rules out. It is reachable only from `cli.py`'s probe subcommands
and `live_acceptance.py`, never from `control_channel.py`'s
`build_transport` — confirmed not wired into any production path today, and
`frp_transport.py` must not inherit either the fallback or its `True`
default when modes (a)/(b) become real.

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
`identity_anchor="preshared_link_secret"` — precisely,
`CLIO_RELAY_STCP_SECRET` (the mode-(a) pairing secret, `cluster_config.py:218`,
`connection-model.md:66`) paired with `CLIO_RELAY_FRP_TOKEN` (the
relay-point authentication token, `cluster_config.py:217`,
`connection-model.md:67`) — **not** `CLIO_RELAY_API_TOKEN`, a third,
genuinely distinct credential (owned-session/remote-API auth; all three are
enumerated separately, alongside two more, in `RELAY_CREDENTIAL_ENV_NAMES`
at `models.py:18-26`, confirming the codebase itself already treats them as
five distinct env vars, not aliases — an earlier version of this document
conflated the pairing with "the API token," which is precise about neither
name). This pairing, recorded on the `ChannelLink`, stamped on every
`ChannelEvent`, surfaced in
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
(`bounded_payload.py`) → R7 (`release_pins.py`) → R8+ (`test_cli.py`
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
`release_pins.py`'s `PinSite` table is easiest to get right once R3-R6
stop adding new scattered constants that would immediately need their own
pin entries. R8+ last because it is gated on nothing structural — only on
the monkeypatch-seam cost itself (§4.6), which doesn't shrink until someone
pays it down deliberately.

**Overlap analysis.** Real overlaps exist between slices; they are resolved
by ordering, not avoided by being independent:

- `control_channel.py` and `remote_connection.py` are targets of **both**
  R5 (the transport seam §8.1 rules must not move) **and** R6 (the T1 byte
  budgets §6.4 finds there — `MAX_CHANNEL_EVENT_DETAIL_CHARS` in
  `control_channel.py:68`, the inline `[:2_000]` slice in
  `remote_connection.py:920`). `remote_connection.py` imports from
  `control_channel.py` (`remote_connection.py:37`), so R6's edit to the
  imported module's constants has to land after R5 stabilizes the
  transport seam those two files share, not concurrently with it — this is
  §9's stated reason R6 sequences strictly after R5, not merely a
  scheduling preference.
- `mcp_server.py` is a target of **both** R3 (§6.2's translation-owner
  call surface) **and** R6 (§6.4's T2 precedent, `MAX_INLINE_MCP_RESULT_BYTES`
  at `mcp_server.py:174`). `mcp_server.py` imports from `service_runtime.py`
  (`mcp_server.py:153`) — that import is [#233](https://github.com/iowarp/clio-relay/issues/233)'s
  concern, not R4's (§8.2's B4 correction: R4 does not touch
  `service_runtime.py` at all), so `mcp_server.py`'s own R3/R6 edits are
  independent of both R4 and #233 in practice; the dependency is named here
  rather than asserted clean by omission.
- `cli.py`'s `relay_host_app` command group (7 commands, §4.1's sub-app
  inventory) is the one overlap that changes sequencing, not just ordering
  within a file: its commands call directly into the frp-lifecycle and
  probe code R4/R5 restructure, so extracting it into its own command
  module (§5's `relay-host` row) before R5 lands would mean extracting it
  twice — once now, and again after its callees move. That is why the
  `relay-host` command-module extraction is sequenced inside R8+, strictly
  after R5, rather than bundled into the earlier `cli.py`-focused work
  implied by ground rule 2.

**Why R4 is cheap where `cli.py` is not — concrete evidence, not just
assertion.** `transport_probe.py` already exposes a clean dependency-
injection seam for exactly the process-spawning logic R4 touches:
`process_factory: ProcessFactory | None = None` is a parameter on every
public probe entry point (`run_frp_http_probe:223`,
`run_frp_direct_http_probe:350`, `run_ssh_forward_http_probe:414`) and the
shared internal implementation (`_run_frp_http_probe_with_proxy_type:997`,
required there), each defaulting to the real spawner via
`factory = process_factory or _popen`. `tests/test_transport_probe.py`
uses this seam at exactly 14 sites (`:97`, `:148`, `:174`, `:216`, `:389`,
`:408`, `:449`, `:482`, `:522`, `:612`, `:695`, `:765`, `:840`, `:853`) —
injecting fake process factories, including a negative-test factory that
`raise`s if called at all (proving a "port already occupied" guard never
spawns) — with **zero** `Popen`-style monkeypatches in that test file
(`grep -n "Popen" tests/test_transport_probe.py`: no matches). Contrast
`cli.py`: its own subprocess usage (`import subprocess` at `cli.py:17`) has
no equivalent factory seam — `tests/test_cli.py` patches the module
attribute directly, `monkeypatch.setattr(cli.subprocess, "run", fake_run)`
at exactly 3 sites (`:9471`, `:9535`, `:9573`), the same
where-it's-looked-up-not-where-it's-implemented coupling §4.6 describes for
the other 236 patch sites. R4's extraction is cheap because the seam that
makes it safe already exists; `cli.py`'s extractions are not, because it
doesn't — this is why R8+ (which pays down exactly that coupling) is
sequenced as its own slice rather than assumed free.

None of these overlaps block the migration order in §9 — each is already
resolved by the R3→R4→R5→R6→R7→R8+ ordering stated there — but "the same
file has two owners across two slices" is a real property of this plan, not
an absence of overlap, and is recorded as such rather than claimed clean.

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

- R4: `transport_probe.py`'s local-visitor frp logic (`run_frp_http_probe`,
  `:211-337`) and `frp_check.py`'s `run_frpc_connection_check`
  (`:13-40`) are deleted from their current homes once both delegate to
  `frp_link.py`.
- R3: the per-site error-rationale comments scattered across
  `fastmcp_server.py` (e.g. the `TaskInputParkConflictError` block at
  `:1106-1115`) move into `door_errors.py`'s docstrings once the table they
  explain has one home; the comments are deleted from the call sites, not
  duplicated. `browser_gateway.py`'s bare `{"error": message}` construction
  at `:692` is deleted in favor of the `door_errors.as_http_problem`-shaped
  response (§6.1, §6.2).
- R5: `transport_probe.py`'s `allow_stcp_fallback` parameter
  (`run_frp_direct_http_probe`, `:338-402`, defaults `True`, §8.2) —
  confirmed today to have zero production reach (only `cli.py`'s probe
  subcommands and `live_acceptance.py` call it, never `control_channel.py`)
  — must not gain production reach, nor its `True` default, when
  `frp_transport.py` lands. Either the parameter is deleted outright once
  probe-only callers no longer need it, or it stays permanently fenced to
  non-production probe code with a guard comment naming why
  (`connection-model.md:85-86` rules out the exact behavior it enables).

**Planned, later ([#233](https://github.com/iowarp/clio-relay/issues/233),
separate from R4 — §4.3, §8.2, §9 B4 correction):**

- `service_runtime.py`'s copy — `_start_remote_connector`,
  `_start_local_visitor`, `_remote_allocation_frpc_start_script`,
  `_remote_frpc_start_script`, `_remote_stop_script` (`:5900-8650`) — and
  `transport_probe.py`'s `_remote_probe_script` (`:1213-1351`) are deleted
  from their current homes once delegating to `frp_link.py` (extended) and
  the new `frp_remote_scripts.py`. This is explicitly **not** part of R4's
  deletion above — an earlier version of this document folded it in by
  assuming R4 absorbed all three copies at once.

**Named-not-tonight (real, deferred, not forgotten):**

- The 107 hand-rolled `HTTPException` sites in `http_api.py` (§6.2): R3
  gives every surface one table to route *through*; rewriting all 107 call
  sites to use it is explicitly a later, mechanical slice, not part of R3.
- `errors.py:27-30`'s stale docstring on `TaskInputParkConflictError`
  (names `RelayTasksExtension._park_agent_input`; the real raise site is
  `RelayMcpRuntime._park_agent_input`, `fastmcp_server.py:483`, `:528-530`,
  §6.3) — a small, real, currently-shippable doc-comment bug, worth fixing
  in R3 alongside the reason it names, not deferred past it.

## 11. Known deviations

Same discipline as `connection-model.md`'s "Known deviations" section: these
are defects this design pass surfaced, not descriptions of intended
behavior, and each is either already tracked or is tracked as of this
document.

- **`docs/release-gate-1.0.yaml` pins retired identities on 17 lines total.**
  It targets the retired `clio-kit-jarvis-user-v3.6` contract or bare
  `v3.6` string on 4 lines (`:131`, `:320`, `:1109`, `:1115`) against a tree
  that has shipped `v3.7` for multiple release cycles (§7), and pins
  clio-kit `2.6.6` on 13 lines (`:115`,`:121`,`:122`,`:226`,`:230`,`:231`,
  `:294`,`:299`,`:300`,`:302`,`:309`,`:374`,`:1187`) against a tree pinned at
  `2.7.2` everywhere else (`jarvis_mcp.py:32`, `.github/workflows/ci.yml`
  ×2). `docs/remote-mcp-federation.md:474`/`:476`/`:479-483` compounds this
  with a real content bug of its own — pairing the *current* 2.7.2 wheel
  with the *legacy* v3.6 contract ID and its legacy SHA-256 digests, not
  merely stale text (§7). All three are symptoms of #198 (no single pin
  registry regenerates fixtures together with source) and are exactly what
  R7's `release_pins.py` is scoped to end.
- **No record-time head+tail bound exists for `runner.py`'s `mcp-result.json`
  today**, despite this document's own early working draft assuming one did
  (§6.4). `_write_mcp_result` writes stdout/stderr through unchanged from
  the upstream 32 MiB / 4 MiB read-time caps; the closest real precedent,
  `jarvis_provider.py`'s `_BoundedTextTail`, is tail-only and shares one 1
  MiB bound across both streams rather than the asymmetric split originally
  hypothesized. Tracked in §6.5's ledger, scoped into R6.
- **`allow_stcp_fallback` (`transport_probe.py:338-402`, defaults `True`) is
  exactly the automatic mode-switching `connection-model.md:85-86` rules
  out** (under the "(c) SSH port forward" section, not "Never do this" — an
  earlier version of this document mis-cited the location), confined today
  to non-production probe code only by the accident of nothing calling it
  from `control_channel.py` — not by any guard that would stop it from
  being called that way, and its `True` default means most callers get the
  behavior without opting in. §10's deletion ledger and §8.2 both flag this
  so R5 does not inherit it, default included, by copy-paste.
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
- **`browser_gateway.py`'s `_request_body` failures split 413 into 413 and
  400.** Before R3, all four `_request_body` conditions (chunked encoding,
  a malformed `Content-Length`, an oversized body, a body that ended early)
  returned a uniform, ad hoc 413 via `_error(413, str(exc))`. R3's initial
  landing routed all four to `door_errors.REASONS["configuration_error"]`
  (400) uniformly — collapsing a real distinction (a genuinely oversized
  body vs. three protocol-validation mistakes) into one bucket. The R3
  re-review (F7+F14) restored 413 for the oversize condition specifically,
  via a dedicated `payload_too_large` reason and a typed
  `_RequestBodyTooLargeError` marker distinguishing it from the other
  three, which remain 400/`configuration_error` — a genuine, permanent
  deviation from the pre-R3 behavior for those three (not merely a
  transient regression fixed later), since none of them has its own
  dedicated reason in the frozen table today.
- **`fastmcp_server.py:1031`'s original "traceback logged exactly once"
  framing (§6.3) undercounted the HTTP surface.** `door_errors.classify()`
  itself logs the traceback exactly once, but on the HTTP surface
  Starlette's `ServerErrorMiddleware` re-raises after `_relay_unhandled_
  exception_handler` returns (by design, so a real ASGI server can still
  observe the error), and uvicorn logs that re-raise too — a second,
  server-side-only log line there is expected, not a `door_errors` defect.
  Corrected in §6.3 and `door_errors.py`'s own module docstring by the R3
  re-review (F6).

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
- [#233](https://github.com/iowarp/clio-relay/issues/233) — **new, filed by this document.** Absorb `service_runtime.py`'s frp lifecycle + `transport_probe.py`'s remote-script generation into `frp_link.py`/`frp_remote_scripts.py` — explicitly separate from R4, not R4 itself (§4.3, §5, §8.2, §9, §10; B4 correction)
- [#235](https://github.com/iowarp/clio-relay/issues/235) — **new, filed by the R3 re-review.** Route `mcp_server.py`'s stdio `_error()` through `door_errors` — the one call surface named in §6.1/§6.2 that R3's own scoping left unwired (§6.5)

## 13. Provenance

This document has been through two verification passes. The first pass's
claims were checked against `clio-relay` at commit
`ee3120f702acd7dbb529e3548679c457c6b59088` (branch `feat/231-owner-modules`),
correcting several of the originating issue's hint numbers (documented
inline at each correction: `cli.py`'s helper fan-out counts, the
frp-lifecycle line ranges in `service_runtime.py`, the wire-model range in
`session_lifecycle.py` (`:891-1435` → `:890-1433`), the owner-token doc
comment (`:126-134` → `:127-134`), the `HTTPException` count (~40 → 107),
and — most significantly — a claimed record-time head+tail bound in
`runner.py` that does not exist in code).

A second pass (opus review, blockers B1-B5 plus an F-list) re-verified
against the tree after `e078d89` (the B1 gate fix) and corrected: the frp
R4-vs-`#233` scope split (an earlier version wrongly folded
`service_runtime.py`'s ~1,900-line copy into R4's budget, and double-counted
embedded-heredoc text as real functions in `service_runtime.py`'s line
ranges); five missing §5 owner-module rows (§4's sidecar-cleanup,
validators, catalog/dispatch, `cli.py` shared-plumbing, and state-machine
concerns each now have one); the `str(exc)` count (an isolated "~59+"
corrected to 199/208, with per-file holders); the release-gate/bootstrap.py/
remote-mcp-federation.md staleness recount (exact line lists, and a real
legacy-digest content bug in `remote-mcp-federation.md`, not just a citation
drift); the mode-switching quote's actual location
(`connection-model.md:85-86`, not "Never do this"); `allow_stcp_fallback`'s
`True` default as the real hazard; the identity-anchor secret pairing's
precise names (`CLIO_RELAY_STCP_SECRET` + `CLIO_RELAY_FRP_TOKEN`, not "the
API token"); and the §9 overlap analysis's honesty (rewritten from "found
clean" to "overlaps exist, resolved by ordering," with the
`transport_probe.py` `process_factory` injection-seam evidence added as the
concrete reason R4 is cheap where `cli.py` is not). The `release_identity.py`
module name was also renamed to `release_pins.py` throughout (it collided
with `session_lifecycle.py`'s unrelated `SessionApiReleaseIdentity` wire
model). All second-pass corrections are marked inline at the sections they
touch rather than only summarized here.

A third pass (opus review of the R3 *implementation*, findings F1-F16, all
verified by execution rather than reading) landed against the door_errors
code this document specifies and corrected: a real MCP-SDK reserved-code
collision (F1, five custom codes squatting on `-32000..-32019`); a false
"`truncation`: null" statement on documents this document's own T1 budget
had actually truncated (F2); an unenforced ≤8KiB budget that let a
non-"evidence"-named oversized extension member through at 11,213 bytes
(F3); contract fields losing to a colliding `fault.data` key instead of
always winning (F4); a hostile `__str__`/typed-data-extractor collapsing
classification entirely rather than degrading (F5); an over-broad "logged
exactly once" claim that did not account for Starlette's own re-raise +
uvicorn's second log line (F6); the browser_gateway 413→400 deviation this
document itself did not yet record (F7); a mislabeled "fourth surface"
(`mcp_server.py` vs. `browser_gateway.py`, §6.1/§6.5) and an un-owned
residual for the one surface R3 left unwired (F14, closed by
[#235](https://github.com/iowarp/clio-relay/issues/235)); plus F9/F10/F11/
F13/F15/F16 (a mutable `REASONS`, `ensure_ascii=True` byte measurement, two
declared-but-unemitted reasons presented as live regressions, a real port
TOCTOU in the test suite, and the same dispatch-mechanism error repeated in
two places). All third-pass corrections are marked inline at the sections
they touch.

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

# str(exc)-of-caught-exception count, by file (F1)
grep -rln 'str(exc)' src/clio_relay --include='*.py' | xargs -I{} sh -c 'grep -c "str(exc)" {}; echo {}'

# a naive `grep '^def '` sweep double-counts embedded heredoc text as real
# functions inside service_runtime.py's frp-script generators (§4.3, B4) --
# use a triple-quote scan or an AST parse instead, e.g.:
python3 -c "import ast; ast.parse(open('src/clio_relay/service_runtime.py').read())"

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
