# `core_queue.py` split design — 2026-08

**Status:** design only; no production extraction is part of R11.  
**Parent design:** [`relay-architecture-2026-08.md`](relay-architecture-2026-08.md).  
**Evidence snapshot:** `src/clio_relay/core_queue.py` is 16,137 lines at this
design point: the compatibility types and helpers begin at `core_queue.py:236`,
`ClioCoreQueue` occupies `core_queue.py:599-14308`, and module-level storage,
validation, lease, GC, and idempotency helpers continue through
`core_queue.py:16137`.

This document turns the parent design's five inventory-level labels into an
executable split. It follows the parent ground rules: one concern has one
owner, tests move with logic, deletion is part of every extraction, the
`core_queue.py` ratchet only decreases, every new file stays below 800 lines,
and no failure path gains a fallback.

## 1. concern inventory

The current class is not five contiguous blocks. Public operations are followed
by a second layer of derived-index, transition, storage, and codec helpers, so a
line-range inventory has to include both layers.

| Concern | Current ranges | Responsibilities | Internal coupling observed in the file |
|---|---|---|---|
| Storage, locking, layout, and canonical reads | Lock and root safety `core_queue.py:236-465,602-807`; write/read kernel `core_queue.py:13920-14308`; canonical-access checks `core_queue.py:14311-14439`; bounded descriptor reads and durable removal `core_queue.py:15496-15882` | Fair bounded lock ownership, private directory validation, staged atomic replacement, fsync, bounded record reads, identity validation, and safe unlink/GC primitives | This is the bottom dependency. Initialization calls root/layout/write operations (`core_queue.py:2474-2833`); every record concern ultimately calls `_write`, `_read_optional`, `_read_many`, or `_scan_many` (`core_queue.py:13943-14283`). It must not call a domain owner. |
| Store records and pure codecs | Store constants/family limits `core_queue.py:109-233`; idempotency, cancellation, lease-capacity, and legacy-output records `core_queue.py:469-575`; artifact projection `core_queue.py:578-596`; browser/session validators `core_queue.py:14442-14687`; lease-capacity/index codecs `core_queue.py:14887-15493`; task/idempotency/artifact/index codecs `core_queue.py:15885-16137` | Typed internal records, canonical documents and digests, pure identity/token parsing, and pure validation | These functions are leaves with respect to queue I/O. Their callers are the lease, artifact, task, session, migration, and idempotency owners; for example lease-capacity documents are constructed and decoded at `core_queue.py:14937-15267`, while submission digests are constructed at `core_queue.py:15906-15963`. |
| Startup, legacy audit, legacy-output migration, and index migration | Startup seal/audit `core_queue.py:635-2472`; initialization and readiness `core_queue.py:2474-2915`; index migration/repair `core_queue.py:2917-3644`; transition/index state and rebuild `core_queue.py:12216-12465,13296-13918`; migration helpers `core_queue.py:16056-16121` | Prove the queue layout before use, audit pre-index history once, migrate v0.9 output safely, advance bounded index migrations, reconcile crash intents, and expose readiness | `initialize` calls legacy audit, optional legacy-output migration, pending-transition recovery, and the lease-capacity gate (`core_queue.py:2527,2567,2682-2689`). Index migration calls job, artifact, gateway, task, scheduler, endpoint, and lease index rebuilders (`core_queue.py:13530-13699`). This concern therefore orchestrates owners; it does not own their record semantics. |
| Jobs, idempotency, global order, and terminal GC | Submission/idempotency/job CRUD `core_queue.py:3684-4365`; derived job writes and admission limits `core_queue.py:11782-12171`; active-index repair and terminal protection/collection `core_queue.py:12467-13076`; global order `core_queue.py:13078-13294`; digest/idempotency codecs `core_queue.py:15885-15963,16124-16137` | Resolve idempotent identity, submit and mutate jobs, page/scan jobs, maintain job/order indexes, and retire terminal jobs through a crash-safe tombstone/GC process | Submission verifies artifact-use records, owner-session intake, input-ingest quota, scheduler-cancel state, and the queued event (`core_queue.py:3812,3858-3873,3881-3902`). GC consults scheduler, gateway, owner-session, execution-cleanup, and artifact-lineage protections (`core_queue.py:12494-12647`). |
| Endpoint registry and fresh-endpoint index | Endpoint write `core_queue.py:3646-3682`; list/page/fresh scans `core_queue.py:4630-4809`; fresh index write `core_queue.py:12076-12132`; fresh-bucket/admission predicate helpers `core_queue.py:14704-14722` | Persist endpoint heartbeats, expose bounded reads, and maintain the freshness index used by admission/diagnosis | Depends on storage, global order, and migration completeness. It is read by lease admission, but does not own leasing (`core_queue.py:6540-6657`). |
| JARVIS input contracts and manifests | `core_queue.py:4367-4628` | Persist package input contracts, pipeline bindings/lineage, and immutable run manifests | Depends only on storage/locking and record models. Job submission later consumes the manifest-derived metadata through input quota and artifact owners (`core_queue.py:3777-3908`). |
| Lease admission, capacity, operational indexes, lifecycle, and stale recovery | Lease records `core_queue.py:510-549`; capacity audit/rebuild `core_queue.py:2835-2866,3193-3644`; reads and capacity/index substrate `core_queue.py:4811-5625`; admission/acquire/renew/recovery/release `core_queue.py:6523-7455`; derived stale queries `core_queue.py:11565-11764`; pure lease codecs `core_queue.py:14887-15493` | O(1) admission through a self-validating aggregate/checkpoint pair, exact lease references, acquire/renew/release, and crash-resumable stale recovery | Acquisition writes the canonical job and lease, operational indexes, and capacity transition (`core_queue.py:6842-6904`). Recovery and release update jobs/events, indexes, and capacity (`core_queue.py:7118-7449`). This concern calls jobs, events, storage, transition journal, and endpoint lookup; jobs do not call lease internals except for GC protection queries. |
| Scheduler-cancellation journal | Result/claim records `core_queue.py:478-506`; job cancellation and cancellation workflow `core_queue.py:5627-6538`; record paths/persistence `core_queue.py:11797-11910`; due-state helpers `core_queue.py:14725-14798` | Create exact scheduler identities, lease external cancel/confirmation attempts, record observations, and converge dispositions idempotently | Job cancel requests create pending cancellation state (`core_queue.py:5666-5748`); lease admission/recovery observes it (`core_queue.py:6523-6657,11713-11780`). The owner calls job/event/storage seams but must not own provider I/O. |
| Tasks, MCP task projection, events, and progress | Task/MCP CRUD and task events `core_queue.py:7457-7776`; job events `core_queue.py:8339-8394`; progress `core_queue.py:9445-9553`; task/event derived indexes and sequence heads `core_queue.py:11078-11092,11473-11563`; task canonicalization `core_queue.py:15885-15903` | Persist relay tasks, the SEP-2663 projection record, optimistic projection updates, cursor replay, and progress | Task writes call the order/retention index seam (`core_queue.py:12173-12196`); task and job events update job indexes (`core_queue.py:11473-11563`). FastMCP calls only `put_mcp_task`, `update_mcp_task_projection`, and `get_mcp_task` (`fastmcp_server.py:445-527,618-683`). |
| Execution-cleanup records | `core_queue.py:7788-8337` | Register/acknowledge cleanup, migrate the flat plan to shards, boundedly scan pending cleanup, and answer GC/recovery protection queries | Depends on storage and transition recovery. Jobs and stale recovery query it (`core_queue.py:8267-8285,12494-12605`); it does not perform process cleanup itself. |
| Artifacts, lineage, transforms, and input ingest | Artifact append plus ingest state machine `core_queue.py:8396-8927`; artifact pages and bidirectional use indexes `core_queue.py:8929-9381`; transform refs `core_queue.py:9383-9443`; quota helpers `core_queue.py:11955-12074`; provenance/ingest codecs `core_queue.py:14668-14687,15966-16053` | Assign artifact order, maintain forward/reverse content-pinned lineage, persist one transform, and run the owned input-ingest state machine | Job submission validates and writes artifact-use edges (`core_queue.py:3812,3858,3901`); input ingest updates jobs and emits events (`core_queue.py:8427-8927`); GC asks lineage for protections (`core_queue.py:12607-12647`). |
| Gateway sessions, browser attachments, owner sessions, monitor rules, and reverse indexes | Gateway CRUD `core_queue.py:9555-9794`; browser attachment transitions `core_queue.py:9796-10048`; owner-session admission/closure `core_queue.py:10050-10957`; monitor rules and all gateway/scheduler reverse indexes `core_queue.py:10959-11471`; gateway/session pure validators `core_queue.py:14442-14884` | Durable gateway state, exact browser attach/revoke CAS, owner-generation intake/closure, monitor records, and gateway/artifact/scheduler backlinks | Gateway writes rebuild reverse indexes (`core_queue.py:12198-12214`). Owner-session intake gates job submission and gateway creation (`core_queue.py:3881-3887,9561-9573`). Reverse-index synchronization reads artifacts, scheduler identities, jobs, tasks, and gateways (`core_queue.py:11078-11471`). |

### 1.1 allowed dependency direction

The split uses one shared `QueueContext` (root, fair lock, initialization state)
and explicit module calls. The allowed direction is:

```text
core_queue facade
    -> domain owners (jobs, leases, tasks, artifacts, gateways, sessions)
        -> index/transition owners
            -> queue_store + queue_layout
                -> pure record/codecs
```

Cross-domain calls are limited to named public seams. The current high-risk
edges that those seams must replace are:

- startup -> legacy audit/output, transitions, lease capacity
  (`core_queue.py:2527,2567,2682-2689`);
- jobs -> artifacts, owner-session admission, scheduler cancellation, events
  (`core_queue.py:3812,3858-3902`);
- leases -> jobs, capacity, operational indexes, transitions
  (`core_queue.py:6842-6904,7118-7449`);
- transitions -> job, lease, task, and gateway index repair
  (`core_queue.py:12244-12465`);
- terminal GC -> execution cleanup, scheduler/gateway/session indexes, and
  artifact lineage (`core_queue.py:12494-12647`); and
- gateway writes -> gateway reverse indexes (`core_queue.py:12198-12214`).

No owner may call back through `ClioCoreQueue`; that would create a cycle and
make a facade monkeypatch accidentally intercept internal behavior.

## 2. target owner-module map

`core_queue.py` remains the compatibility import and composition root. Its
target is below 800 lines: constructor/state, public typed forwarding methods,
and intentional re-exports only. It contains no storage, validation, index, or
domain implementation.

Every new module has a hard acceptance cap of **fewer than 800 physical
lines**. The planning budget below is lower so later docstrings and typing do
not land exactly against the ratchet.

| Concern owner | Budget | Responsibilities and public seams |
|---|---:|---|
| `queue_context.py` | 250 | `QueueContext` and narrow protocols for root, lock, and initialization state. No domain behavior. |
| `queue_store.py` | 750 | Fair lock, bounded canonical read/write, staged atomic replacement, fsync, and durable unlink. Seams: `read_optional`, `read_many`, `scan_many`, `write_model`, `write_json`, `unlink_durable`. Current source: `core_queue.py:236-465,13943-14308,15496-15882`. |
| `queue_layout.py` | 600 | Root/private-directory checks, durable keys, record-family size policy, canonical path validation. Seams: `prepare_root`, `require_layout`, `record_path`, `validate_canonical_access`. Current source: `core_queue.py:109-233,602-807,14311-14439`. |
| `queue_startup.py` | 700 | Initialization/readiness orchestration only. Seams: `initialize`, `readiness_info`, `reconcile_pending_transitions`. Current source: `core_queue.py:2474-2915`. |
| `queue_legacy_audit.py` | 750 | One-time family/event audit and sealed audit marker. Seams: `audit_before_initialization`, `read_audit_marker`, `write_audit_marker`. Current source: `core_queue.py:635-1258,2308-2472`. |
| `queue_legacy_output.py` | 750 | v0.9 output record decode, archive/receipt validation, migration, and retired receipt checks. Seams: `audit_output`, `migrate_output`, `validate_compatibility_access`, `retire_receipts`. Current source: `core_queue.py:1260-2306,12771-12803`. |
| `queue_index_state.py` | 650 | Sealed and extended migration documents plus completeness gates. Seams: `read_state`, `write_state`, `require_complete`, `ensure_extended_state`. Current source: `core_queue.py:882-1111,13296-13528,16056-16121`. |
| `queue_index_migration.py` | 750 | Bounded migration coordinator and family dispatch. Seams: `migrate_batch`, `reconcile_sources`, `repair_lease_indexes`. Current source: `core_queue.py:2917-3242,13530-13918`. |
| `queue_transitions.py` | 750 | Generic crash-intent write/replay and kind dispatch. Seams: `write_intent`, `recover_pending`. Current source: `core_queue.py:12216-12465`. Domain-specific appliers remain in their domain owners. |
| `queue_order_index.py` | 700 | Global order and per-job sequence/head records. Seams: `ensure_global`, `read_global_page`, `scan_global`, `read_job_page`, `increment_job_index`. Current source: `core_queue.py:11473-11563,13078-13294,13701-13868`. |
| `queue_idempotency.py` | 450 | Resolution, retired replay, committed record, and submission digest. Seams: `resolve`, `commit`, `retire`, `job_digest`. Current source: `core_queue.py:469-474,3684-3775,3910-3951,11789-11795,12686-12737,15906-15963,16124-16137`. |
| `queue_jobs.py` | 750 | Submit/get/list/scan/state/metadata/cancel-request CRUD and derived active/session/scheduler writes. Seams preserve the corresponding public `ClioCoreQueue` methods. Current source: `core_queue.py:3777-3908,3953-3974,4173-4365,5627-5811,11912-12171,12467-12492`. |
| `queue_job_gc.py` | 750 | Plan/collect/tombstone/quarantine/purge and protection aggregation. Seams: `plan_terminal_job_gc`, `collect_terminal_job`, `purge_quarantined_tree_batch`. Current source: `core_queue.py:3976-4171,12494-13076,15503-15693`. |
| `queue_endpoints.py` | 500 | Endpoint registration, pagination, bounded fresh scans, and fresh index. Current source: `core_queue.py:3646-3682,4630-4809,12076-12132,14704-14722`. |
| `queue_jarvis_inputs.py` | 500 | Package contracts, pipeline bindings/lineage, and run manifests. Current source: `core_queue.py:4367-4628`. |
| `queue_lease_records.py` | 700 | Lease index/capacity records and pure canonical codecs. Current source: `core_queue.py:510-549,14887-15493`. |
| `queue_lease_capacity.py` | 750 | Aggregate/checkpoint read/write, transition, audit, rebuild intent, and exact snapshot. Seams: `snapshot`, `audit`, `prepare_transition`, `apply_transition`, `prepare_rebuild`. Current source: `core_queue.py:2835-2866,3244-3587,4900-5178,6909-7012`. |
| `queue_lease_indexes.py` | 750 | Operational paths, reference validation, sync/delete/scan, and exact active-lease lookup. Current source: `core_queue.py:5180-5625,11565-11700`. |
| `queue_lease_recovery.py` | 700 | Due-expiry selection, single/batch stale recovery, and crash-intent application. Current source: `core_queue.py:7060-7354,11702-11780`. |
| `queue_leases.py` | 750 | Admission, acquire, renew, and release orchestration. Seams: `acquire_next_job`, `acquire_job`, `submit_and_acquire_job`, `renew_lease`, `release_lease`, bounded list/scan. Current source: `core_queue.py:4811-4874,6523-7058,7363-7455`. |
| `queue_scheduler_cancel.py` | 750 | Scheduler identity/disposition state machine and attempt/confirmation claims. Current source: `core_queue.py:478-506,5795-6538,11797-11910,14725-14798`. Provider execution remains outside the queue. |
| `queue_tasks.py` | 700 | Relay task CRUD and MCP task projection CAS. Seams: `append_task`, `put_mcp_task`, `update_mcp_task_projection`, `get_mcp_task`, task state/metadata/list/scan. Current source: `core_queue.py:7457-7694,12173-12196,15885-15903`. |
| `queue_events.py` | 500 | Job/task event append, page/drain, contiguous sequence heads. Current source: `core_queue.py:7696-7776,8339-8394,11473-11563`. |
| `queue_execution_cleanup.py` | 750 | Cleanup register/ack, shard migration, scans, and pending checks. Current source: `core_queue.py:7788-8337`. |
| `queue_progress.py` | 300 | Progress append/page/latest and job event emission. Current source: `core_queue.py:9445-9553`. |
| `queue_artifacts.py` | 700 | Artifact append/get/list/order and transform refs. Current source: `core_queue.py:578-596,8396-8425,8929-8992,9383-9443`. |
| `queue_artifact_lineage.py` | 700 | Forward/reverse artifact-use records, cursor order, validation, and GC protections. Current source: `core_queue.py:8994-9381,12607-12647,14668-14687,16022-16053`. |
| `queue_input_ingest.py` | 750 | Begin/fail/recover/reconcile/complete input ingest and owner-generation quota. Current source: `core_queue.py:8427-8927,11955-12074,15966-16019`. |
| `queue_gateways.py` | 700 | Gateway CRUD, teardown intent, canonical writes. Current source: `core_queue.py:9555-9794,10954-10957,12198-12214`. |
| `queue_browser_attachments.py` | 600 | Prepare/complete attach and begin/finish revoke with exact identity validation. Current source: `core_queue.py:9796-10048,14442-14548`. |
| `queue_owner_sessions.py` | 750 | Owner-generation start/open/closing/closed records, intake gate, membership, and validation. Current source: `core_queue.py:10050-10952,14620-14665`. |
| `queue_gateway_indexes.py` | 750 | Gateway/artifact/scheduler backlinks and scheduler-source synchronization. Current source: `core_queue.py:11078-11471,14551-14617,14827-14884`. |
| `queue_monitor_rules.py` | 350 | Monitor-rule append/get/page/scan/update and its active index. Current source: `core_queue.py:10959-11076`. |

The sub-splits are mandatory, not optional follow-up cleanup. In particular,
storage is split into context/store/layout; migration into startup, legacy
audit, legacy output, index state, index migration, and transitions; leases
into records, capacity, indexes, recovery, lifecycle, and scheduler cancel;
task projection into tasks/events/cleanup/progress; artifacts into artifact,
lineage, and ingest; and session/gateway state into gateway, browser,
owner-session, reverse-index, and monitor owners. Moving any of those current
multi-thousand-line concerns into a single replacement file would violate the
800-line rule immediately.

## 3. patch-seam analysis

### 3.1 required idiom

Every owner imports collaborators as modules and looks symbols up at call time:

```python
import clio_relay.queue_store as queue_store

queue_store.write_model(context, path, record)
```

Do not use `from clio_relay.queue_store import write_model` inside another
owner. A test patches `queue_jobs.queue_store.write_model`, the exact lookup
site. The first extraction adds an AST guard analogous to the CLI campaign's
guard: every audited cross-owner collaborator must be a module attribute call,
and every entry records its caller owner.

Public compatibility is separate from collaborator injection:

- `ClioCoreQueue` public methods remain typed forwarding methods, so class and
  instance patches of public methods retain their exact target.
- Private fault hooks move with their logic. Their tests move in the same slice
  and patch the owner module attribute; `core_queue.py` does not keep a private
  shim that no implementation reads.
- Constants move to the owner that reads them. Tests patch that owner module's
  constant. An intentional `core_queue.py` re-export may preserve imports, but
  is never presented as an injection seam.
- Imported collaborators (`open_private_atomic_file`, `utc_now`,
  `exclusive_migration_lifetime`, `os`, and `time`) are looked up through a
  module object owned by the extracted caller. Tests patch that lookup object.

### 3.2 exhaustive current patch inventory

An AST/read grep of `tests/*.py` found **102 sites across 60 targets**. There
are no `patch.object` sites targeting this module; all 102 use
`monkeypatch.setattr`. `Path.unlink`/`Path.replace` fault injection in
`tests/test_queue.py:244,793` targets the standard-library type rather than a
`core_queue` symbol and is therefore outside this table.

| Current target | Sites | Preservation after move |
|---|---|---|
| `core_queue_module.MAX_ARTIFACT_CONSUMERS` | `tests/test_artifact_lineage.py:388` | Move test and lookup to `queue_artifact_lineage.MAX_ARTIFACT_CONSUMERS`. |
| `queue._write_job_unlocked` | `tests/test_artifact_lineage.py:430` | Move failure injection to `queue_jobs.write_job` at the lookup in `queue_artifact_lineage`; no dead facade-private shim. |
| `queue._write_immutable_artifact_use_record` | `tests/test_artifact_lineage.py:469` | Patch `queue_artifact_lineage.write_immutable_use_record`. |
| `core_queue.ClioCoreQueue` | `tests/test_bootstrap_fast_path.py:2235,2410` | Exact import remains; no target change. |
| `ClioCoreQueue.set_owner_session_closed` | `tests/test_cli.py:2295,3354,3608,3747,3842` | Public facade method remains; exact target remains. |
| `ClioCoreQueue.scan_gateway_sessions` | `tests/test_cli.py:8163` | Public facade method remains. |
| `queue._ensure_global_order_entry_unlocked` | `tests/test_core_idempotency.py:50,53`; `tests/test_storage_managed_queue.py:376,379` | Patch `queue_idempotency.queue_order_index.ensure_global`; moved owner tests prove the lookup. |
| `queue._read_many` | `tests/test_core_index_safety.py:52` | Patch `queue_lease_recovery.queue_store.read_many`. |
| `core_queue_module.MAX_GATEWAY_INDEX_RECORDS` | `tests/test_core_index_safety.py:74` | Patch `queue_gateway_indexes.MAX_GATEWAY_INDEX_RECORDS`. |
| `queue._after_gc_checkpoint` | `tests/test_core_retention.py:180,197` | Keep the fault hook in `queue_job_gc`; move and patch there. |
| `queue._write` | `tests/test_core_retention.py:946` | Patch the exact `queue_owner_sessions.queue_store.write_model` lookup. |
| `queue.scan_execution_cleanup` | `tests/test_endpoint.py:851` | Public facade method remains. |
| `queue.read_event_page` | `tests/test_endpoint.py:5421` | Public facade method remains. |
| `worker.queue.scan_job_tasks`, `worker.queue.scan_jobs` | `tests/test_endpoint_bounded_reads.py:34,50` | Public facade methods remain. |
| string `clio_relay.core_queue.open_private_atomic_file` | `tests/test_fastmcp_server.py:410,1254` | Move to the storage owner's module lookup and update both strings to `clio_relay.queue_store.cluster_config.open_private_atomic_file`; the AST guard pins it. |
| `queue.update_mcp_task_projection` | `tests/test_fastmcp_server.py:477,1270` | Public facade method remains; critical to the in-flight task-admission work. |
| `ClioCoreQueue.get_job` | `tests/test_http_api.py:359,397` | Public facade method remains. |
| `ClioCoreQueue.get_task` | `tests/test_http_api.py:360,398` | Public facade method remains. |
| `ClioCoreQueue.drain_events`, `.drain_task_events`, `.list_monitor_rules` | `tests/test_http_api.py:361-363` | Public facade methods remain. |
| string `clio_relay.core_queue.utc_now` | `tests/test_input_staging.py:464` | Move to `queue_input_ingest.models.utc_now` (module lookup), with the logic test. |
| `ClioCoreQueue.scan_endpoints` | `tests/test_installation.py:2279` | Public facade method remains. |
| `queue.update_task_metadata` | `tests/test_jarvis_execution_recovery_guards.py:288` | Public facade method remains. |
| `ClioCoreQueue._after_legacy_output_migration_phase` | `tests/test_legacy_output_migration.py:200,218,552,561` | Move hook and tests to `queue_legacy_output.after_migration_phase`. |
| `ClioCoreQueue._iter_legacy_event_paths` | `tests/test_legacy_output_migration.py:263`; `tests/test_queue_startup_audit.py:66` | Move to `queue_legacy_audit.iter_legacy_event_paths`; caller imports owner module. |
| `core_queue_module.MAX_BOUNDED_SCAN_RECORDS` | `tests/test_legacy_output_migration.py:426`; `tests/test_queue_startup_audit.py:71,273` | Patch the reading owner (`queue_legacy_audit` or `queue_store`) at each test. |
| `core_queue_module.MAX_LEGACY_EVENT_AUDIT_RECORDS` | `tests/test_legacy_output_migration.py:427` | Patch `queue_legacy_audit.MAX_LEGACY_EVENT_AUDIT_RECORDS`. |
| `core_queue_module.MAX_LEGACY_OUTPUT_RECORD_BYTES` | `tests/test_legacy_output_migration.py:485,494` | Patch `queue_legacy_output.MAX_LEGACY_OUTPUT_RECORD_BYTES`. |
| `core_queue_module.MAX_LEGACY_OUTPUT_MIGRATION_BYTES` | `tests/test_legacy_output_migration.py:512` | Patch `queue_legacy_output.MAX_LEGACY_OUTPUT_MIGRATION_BYTES`. |
| `queue._scan_many` | `tests/test_operational_indexes.py:118` | Patch `queue_leases.queue_store.scan_many`. |
| `queue._sync_lease_operational_indexes_unlocked` | `tests/test_operational_indexes.py:403` | Patch `queue_index_migration.queue_lease_indexes.sync_operational_indexes`. |
| `queue.scan_active_jobs` | `tests/test_operational_indexes.py:705` | Public facade method remains. |
| `core_queue_module._read_bounded_record_bytes` | `tests/test_queue.py:208,310,368` | Move tests and patch `queue_store.read_bounded_record_bytes`. |
| `core_queue_module.ATOMIC_REPLACE_RETRY_SECONDS` | `tests/test_queue.py:462,568,677` | Patch `queue_store.ATOMIC_REPLACE_RETRY_SECONDS`. |
| `core_queue_module.os.open` | `tests/test_queue.py:463,529,635,678` | Patch `queue_store.os.open`; `queue_store` performs the lookup. |
| `core_queue_module.os.fstat` | `tests/test_queue.py:530,637` | Patch `queue_store.os.fstat`. |
| `core_queue_module.os.read` | `tests/test_queue.py:531,636` | Patch `queue_store.os.read`. |
| `core_queue_module.time.sleep` | `tests/test_queue.py:532,638` | Patch `queue_store.time.sleep`. |
| `core_queue_module.os.lstat` | `tests/test_queue.py:569,830` | Patch `queue_store.os.lstat` for reads and `queue_layout.os.lstat` for layout checks, according to the moved test's caller. |
| `core_queue_module.ATOMIC_REPLACE_ATTEMPTS` | `tests/test_queue.py:676` | Patch `queue_store.ATOMIC_REPLACE_ATTEMPTS`. |
| `core_queue_module._read_bounded_record_bytes_once` | `tests/test_queue.py:757` | Patch `queue_store.read_bounded_record_bytes_once`. |
| `queue._fsync_write_directory` | `tests/test_queue.py:879` | Patch `queue_store.fsync_write_directory`. |
| `queue.list_jobs` | `tests/test_queue.py:1428` | Public facade method remains. |
| `queue.lease_admission_capacity_snapshot` | `tests/test_queue_management.py:348` | Public facade method remains. |
| `core_queue_module.MAX_LIVE_LEASE_RECORDS` | `tests/test_queue_management.py:368` | Patch `queue_lease_capacity.MAX_LIVE_LEASE_RECORDS`. |
| `core_queue_module.ClioCoreQueue._scan_json_record_paths` | `tests/test_queue_readiness.py:109` | Move to and patch `queue_store.scan_json_record_paths`. |
| `ClioCoreQueue._audit_legacy_state_before_initialization` | `tests/test_queue_startup_audit.py:51,251,308`; `tests/test_worker_lifetime_lock.py:514,827` | Patch `queue_startup.queue_legacy_audit.audit_before_initialization`; move logic tests, retain lifetime integration tests against that lookup. |
| `ClioCoreQueue._audit_completed_legacy_output_state` | `tests/test_queue_startup_audit.py:56` | Patch `queue_legacy_output.audit_completed_state`. |
| `ClioCoreQueue._bounded_legacy_family_entries` | `tests/test_queue_startup_audit.py:61` | Patch `queue_legacy_audit.bounded_family_entries`. |
| `core_queue_module.exclusive_migration_lifetime` | `tests/test_queue_startup_audit.py:98`; `tests/test_worker_lifetime_lock.py:233` | Patch `queue_startup.worker_lifetime_lock.exclusive_migration_lifetime`. |
| `ClioCoreQueue._after_legacy_record_audit_phase` | `tests/test_queue_startup_audit.py:124,292,303` | Move hook and tests to `queue_legacy_audit.after_audit_phase`. |
| `queue.collect_terminal_job` | `tests/test_retention.py:79` | Public facade method remains. |
| `ClioCoreQueue.prepare_owner_session_start` | `tests/test_session_lifecycle.py:1163,1433,1975` | Public facade method remains. |
| `ClioCoreQueue.owner_session_generation_status` | `tests/test_session_lifecycle.py:2710` | Public facade method remains. |
| `queue.resolve_idempotent_submission` | `tests/test_storage_managed_queue.py:85` | Public facade method remains. |
| string `clio_relay.core_queue.ClioCoreQueue.submit_job` | `tests/test_storage_managed_queue.py:256` | Exact facade import remains; string stays valid. |
| `crashing._after_stale_recovery_job_write` | `tests/test_storage_managed_queue.py:456,505` | Move hook and tests to `queue_lease_recovery.after_job_write`. |
| `survivor.recover_stale_jobs` | `tests/test_storage_managed_queue.py:460,513` | Public facade method remains. |

The extraction slice that changes a target changes its tests in the same
commit. A temporary re-export is allowed only for import compatibility and is
deleted when no production or test importer remains; it must never be the only
reason a monkeypatch appears to succeed.

## 4. migration order

The order is bottom-up, except that the FastMCP task owner is deliberately late
to avoid the in-flight task-admission branch. Each slice is one reviewable
extraction. Every slice starts with a red delegation/patch-seam test, moves the
logic and its tests, deletes the old body, lowers `core_queue.py`'s ratchet by
the measured net removal, and runs the complete local gates.

| Slice | Extraction and why now | Failing-first test plan | Acceptance criteria |
|---|---|---|---|
| CQ1 | `queue_context.py` plus pure records/codecs colocated with their future owners. Leaf code has no I/O side effects (`core_queue.py:469-596,14442-15493,15885-16137`). | Add import/round-trip tests for every moved dataclass/document and an AST guard that fails until cross-owner calls use module attributes. | Byte-identical canonical JSON/digests; public constants/types re-exported where imported; all new files under 800; old definitions deleted. |
| CQ2 | `queue_store.py`. All later owners depend on this kernel; its hostile replacement/read coverage is already concentrated in `tests/test_queue.py:180-890`. | Move one bounded-read replacement race and one atomic-write fsync test first; patch `queue_store.os`/`time`; they fail before the owner exists/delegates. | All read/write race, hardlink, cross-device, staging cleanup, and fsync tests pass; no bare imported collaborator; no changed on-disk bytes or error type. |
| CQ3 | `queue_layout.py`. Separating path policy prevents every domain owner from importing storage internals (`core_queue.py:602-807,13956-14013,14380-14439`). | Add sabotage tests for unsafe intermediate/root and record-family limits against the new module. | `tests/test_queue_readiness.py:142-176` and startup tamper tests pass; owner-private and fail-closed behavior unchanged; storage has no domain imports. |
| CQ4 | `queue_legacy_audit.py`. It is startup-only and its crash/seal tests are isolated (`tests/test_queue_startup_audit.py:41-372`). | Move the marker fault hook test first and patch `queue_legacy_audit.after_audit_phase`; red until startup delegates. | Sealed startup performs no history scan; missing seal performs one bounded audit; fault hooks bite; old audit methods deleted. |
| CQ5 | `queue_legacy_output.py`. It depends only on CQ1-CQ4 and has phase-crash/budget coverage (`tests/test_legacy_output_migration.py:70-588`). | Move the archive-phase crash test and per-record/aggregate budget test first. | Authorization, byte bounds, archive/receipt hashes, sequence preservation, idempotent restart, and GC retirement stay exact; no compatibility fallback is added. |
| CQ6 | `queue_index_state.py`, then `queue_index_migration.py` in the same bounded migration extraction. This unlocks every domain extraction without duplicating migration dispatch (`core_queue.py:2917-3242,13296-13918`). | Add an AST/behavior test that patches one owner migration function and proves `queue_index_migration` looks it up by module attribute. | Bounded cursors and seal fields remain compatible; sabotage/tamper tests at `tests/test_queue.py:1279-1399` and `tests/test_core_global_pagination.py:130-158` pass; domain rebuild bodies are not moved here. |
| CQ7 | `queue_transitions.py` and `queue_order_index.py`. Both are shared infrastructure above storage and below domains (`core_queue.py:12216-12465,13078-13294`). | Move a hard-crash transition fixture and global-page window test; make each fail until module-attribute dispatch is wired. | Every existing transition kind replays; unknown/malformed kinds fail closed; stable bounded pages at `tests/test_core_global_pagination.py:31-158` remain exact. |
| CQ8 | `queue_idempotency.py`, `queue_endpoints.py`, and `queue_jarvis_inputs.py` as three consecutive leaf-domain extractions. They have no dependency on unextracted lease/task/gateway internals. | For each owner, move one existing behavior test and add a facade-delegation sabotage test before moving code. | Idempotency crash recovery (`tests/test_core_idempotency.py:34-99`), fresh endpoint bounded reads (`tests/test_queue_readiness.py:67-127`), and immutable input records preserve bytes/errors; each old body is deleted before the next extraction. |
| CQ9 | `queue_jobs.py`, followed by `queue_job_gc.py`. Jobs are the hub needed by leases/tasks/artifacts; GC is separated because its protection fan-in is independent (`core_queue.py:12494-12647`). | First prove `submit_job` calls owner-session, artifact-lineage, scheduler, event, and idempotency owners via sabotage. For GC, move the phase-crash test at `tests/test_core_retention.py:159-210`. | Submission/replay/state behavior is unchanged; every GC phase resumes; protection sets remain exact; `purge_quarantined_tree_batch` stays import-compatible; both modules under 800. |
| CQ10 | Lease records/capacity/indexes: `queue_lease_records.py`, `queue_lease_capacity.py`, `queue_lease_indexes.py`. This isolates the self-validating admission substrate before lifecycle moves. | Move aggregate/checkpoint tamper and index-rebuild crash tests first (`tests/test_lease_capacity.py:104-347`; `tests/test_operational_indexes.py:126-440`). | Generation/digest pairing, O(1) steady admission, exact audits, hardlink/ref validation, and repair intents remain fail-closed; no global scan fallback. |
| CQ11 | `queue_lease_recovery.py`, `queue_leases.py`, then `queue_scheduler_cancel.py`. Dependency direction is now jobs -> indexes/capacity, with provider I/O still external. | Move stale-recovery fault hooks, then acquire/renew/release restart tests (`tests/test_queue.py:1487-1584`), then cancellation claim races (`tests/test_scheduler_cancel_attempt_claims.py:411-878`). | No duplicate lease/execution; capacity generations advance exactly; recovery is crash-resumable; cancel/confirmation claims stay exactly-once and bounded; all public facade methods retain signatures. |
| CQ12 | `queue_execution_cleanup.py`. It is a leaf persistence state machine used by jobs/GC and has a clean shard boundary (`core_queue.py:7788-8337`). | Move flat-to-shard migration and truncated scan tests; patch the public facade scan to prove callers remain stable. | Register/ack/migration/scans are restart-safe and bounded; GC sees pending cleanup exactly as before; old paths remain readable only through the existing migration contract. |
| CQ13 | Artifact cluster: `queue_artifacts.py`, `queue_artifact_lineage.py`, `queue_input_ingest.py`. The required sub-split keeps each owner below 800. | Move reverse-index crash-gap tests (`tests/test_artifact_lineage.py:384-484`) and ingest abandonment/quota tests (`tests/test_input_artifact_ingest.py:127-744`) first. | Sequence/provenance identity, immutable edges, bounded user pages, transform uniqueness, ingest quota/idempotency, and cross-session refusal remain exact; job calls use module attributes. |
| CQ14 | Gateway cluster: `queue_gateways.py`, `queue_browser_attachments.py`, `queue_owner_sessions.py`, `queue_gateway_indexes.py`, `queue_monitor_rules.py`, one owner at a time. | Begin with concurrent browser attachment CAS tests (`tests/test_browser_attachment_queue.py:17-266`), then owner-generation crash/race tests (`tests/test_core_retention.py:373-1049`), then backlink bounds (`tests/test_core_index_safety.py:70-95`). | Gateway/session invariants, closure immutability, intake sealing, backlink cardinality, and monitor pagination remain exact; `service_runtime.py` and `session_lifecycle.py` need no semantic edit. |
| CQ15 | Task/event/progress cluster: `queue_tasks.py`, `queue_events.py`, `queue_progress.py`. Deliberately after `fix/234-task-admission` merges. | Rebase first. Move durable task/event tests, then run the FastMCP CAS/conflict tests at `tests/test_fastmcp_server.py:346-527,854-1270` red-before-green against unchanged facade methods. | `put_mcp_task` identity conflict and projection CAS types/messages remain exact; event cursors remain contiguous; progress still emits events; no FastMCP admission logic moves into queue owners. |
| CQ16 | Final `core_queue.py` facade collapse and deletion pass. | Add a reflection/API test comparing the public names/signatures captured before CQ1 with the final facade, plus the patch-seam AST guard over all owner edges. | `core_queue.py` is below 800; no implementation or private compatibility shim remains; all re-exports are justified by live imports; all tests and static gates pass with zero skips/failures. |

## 5. overlap analysis

### 5.1 `fastmcp_server.py` and `fix/234-task-admission`

`fastmcp_server.py` imports `ClioCoreQueue` at `fastmcp_server.py:49`, stores it
as the runtime queue at `fastmcp_server.py:292-298`, creates the durable MCP
task through `put_mcp_task` at `fastmcp_server.py:445-454`, and performs
optimistic projection updates/reloads at `fastmcp_server.py:513-527,618-683`.
The durable queue boundary is therefore exactly three public methods:
`put_mcp_task`, `update_mcp_task_projection`, and `get_mcp_task`.

The in-flight `fix/234-task-admission` branch touches that admission path. CQ15
must not start until it is merged and this worktree is rebased. The queue split
must not edit `fastmcp_server.py`, move post-admission input parking into the
queue, or change the exception distinction documented at
`fastmcp_server.py:445-527`: a genuine task-identity reuse is a bare
`QueueConflictError`, while exhausted input-parking CAS is the distinct
`TaskInputParkConflictError`. Keeping the facade methods and scheduling CQ15
late makes the source collision zero; the rebase plus FastMCP tests are the
semantic collision gate.

### 5.2 `session_lifecycle.py` and `session_wire_models.py`

`session_wire_models.py` owns wire types and explicitly leaves state-machine
logic in `session_lifecycle.py` (`session_wire_models.py:1-13`). It has no queue
storage ownership. `session_lifecycle.py` locally imports `ClioCoreQueue` where
it needs durable admission state (`session_lifecycle.py:1216-1278`) and calls
the owner-session public seams throughout start/teardown
(`session_lifecycle.py:4687-6286`).

`queue_owner_sessions.py` therefore owns only durable generation admission,
closing, membership, and closure invariants currently at
`core_queue.py:10050-10952`. It must not absorb remote process inspection,
wire selectors/results, cleanup orchestration, or session transport. No edit to
`session_wire_models.py` is expected; `session_lifecycle.py` keeps importing the
facade so its existing class patches remain valid.

### 5.3 `service_runtime.py`

`service_runtime.py` imports the facade at `service_runtime.py:47`, creates a
gateway record at `service_runtime.py:1160`, and repeatedly reads/updates that
record while it owns scheduler, connector, and browser lifecycle
(`service_runtime.py:1219-1426,2698-2897,3342-4310`). The queue split owns only
durable gateway CAS, browser-attachment records, and reverse indexes. It must
not move `_ssh`, scheduler submission, connector spawning, readiness, or stop
logic out of `service_runtime.py` (`service_runtime.py:1085-1120,2972+`). The
facade keeps all gateway public methods, so CQ14 should require no production
edit in `service_runtime.py`.

### 5.4 `frp_link.py`

`frp_link.py` defines the frp configuration/process substrate and explicitly
leaves `service_runtime.py`'s larger connector copy to separate issue #233
(`frp_link.py:1-47`). It imports cluster, error, and relay-host rendering
owners, not queue state (`frp_link.py:49-80`). No queue owner may import
`frp_link.py`; transport/process lifecycle stays above the durable gateway
record boundary. CQ14 therefore has no `frp_link.py` changes or collision.

### 5.5 `door_errors.py`

`door_errors.py` owns wire translation, including the call-path-scoped MCP task
conflict distinction (`door_errors.py:15-81`). It dispatches typed errors at
`door_errors.py:378-389`; it does not own queue invariants. All extracted
owners continue raising the existing types from `errors.py`. In particular,
CQ15 must preserve the exact `QueueConflictError` versus
`TaskInputParkConflictError` behavior used by `fastmcp_server.py:445-527`.
Moving exception classes into an owner module, wrapping them, or introducing a
fallback translation is out of scope.

## 6. deletion ledger

These are deletions, not migration cargo:

| Current code | Evidence | Deletion |
|---|---|---|
| `_unindex_gateway_session_unlocked` | Defined at `core_queue.py:11266-11280`; the live synchronization path calls `_unindex_gateway_session_id_unlocked` directly at `core_queue.py:11256-11264`, and the wrapper has no call site in `src/` or `tests/`. | Delete in CQ14. Do not recreate it in `queue_gateway_indexes.py`. |
| Duplicated canonical record decode/identity loop | `_read_many` repeats the path read, disappearance handling, filename/content identity check, and accumulation at `core_queue.py:14146-14175`; `_scan_many` repeats it at `core_queue.py:14178-14205`. | CQ2 keeps one private `read_records(paths, model, identity_field)` implementation. Exact-read overflow policy remains in `read_many`; truncation reporting remains in `scan_many`. |
| Duplicated gateway filter closure | Identical `matches` bodies occur in `list_gateway_sessions_page` and `scan_gateway_sessions` at `core_queue.py:9618-9624,9641-9647`. | CQ14 keeps one `gateway_session_predicate(cluster, state)` helper in `queue_gateways.py`. |
| Duplicated monitor-rule filter closure | Identical `matches` bodies occur at `core_queue.py:11010-11019,11035-11044`. | CQ14 keeps one `monitor_rule_predicate(job_id, enabled)` helper in `queue_monitor_rules.py`. |
| Private forwarding/fault hooks left in the facade | Current injected hooks include `_after_gc_checkpoint` (`core_queue.py:13060-13061`), `_after_legacy_output_migration_phase` (`core_queue.py:2305-2306`), `_after_legacy_record_audit_phase` (`core_queue.py:878-879`), and `_after_stale_recovery_job_write` (`core_queue.py:7356-7361`). | Move each hook to the owner module and its tests to the owner test. Delete the facade-private name; do not retain a no-op compatibility wrapper. |

No other private function is pre-authorized for deletion by this design. A
slice may add a deletion only after a repository-wide call-site read and a
failing-first proof that the removed path is unreachable or duplicated.

## 7. measurable exit criteria

The split is complete only when all of these are true:

1. `src/clio_relay/core_queue.py` is below 800 physical lines and is only a
   typed compatibility facade/composition root.
2. Every new source file named in section 2 is below 800 lines; the planned
   750-line budgets are not raised to evade the gate.
3. `scripts/check_file_size.py`'s `core_queue.py` baseline decreases in every
   extraction that removes lines and is absent after CQ16. No baseline entry is
   added for a new queue owner.
4. The 102 patch sites in section 3 are either unchanged public-facade patches
   or moved to the exact owner lookup in the same slice. The AST guard has one
   row per cross-owner collaborator and no bare function imports.
5. Every logic test moves to its owner test file with the logic. Facade/API,
   HTTP, FastMCP, service-runtime, and session-lifecycle integration tests stay
   with their callers.
6. The deletion ledger is empty: each row is deleted, not copied.
7. On-disk paths, JSON bytes/digests, exception types/messages, public method
   signatures, cursor semantics, lock boundaries, and crash-replay behavior are
   unchanged unless a separately approved behavior issue says otherwise.
8. There is no silent scan, repair, compatibility, transport, or error fallback.
   Existing fail-closed paths remain fail-closed.
9. Every slice passes `uv run ruff check --fix`, `uv run ruff format`,
   `uv run pyright`, and `uv run pytest` with no failure or skip, plus
   `uv run python scripts/check_file_size.py`.
10. CQ15 rebases after `fix/234-task-admission` and passes the task admission,
    input-parking CAS, typed-error, and reopen tests before it can merge.

## 8. sliced issue map

The following titles and acceptance specifications are ready to file. “All
local gates” means the commands in exit criterion 9, with zero skips/failures.

### CQ1 — refactor(queue): extract context and pure queue records/codecs

Extract `queue_context.py` and colocate pure internal records/codecs with the
section 2 owners. Add the cross-owner patch-seam AST guard before delegation so
the test is red. Preserve canonical serialization/digests and intentional
`core_queue` imports through explicit re-exports. Delete the old definitions.
Accept when every new file is below 800, the guard rejects bare collaborator
imports, round-trip fixtures are byte-identical, the `core_queue.py` baseline
decreases, and all local gates pass.

### CQ2 — refactor(queue): extract the bounded atomic storage kernel

Extract `queue_store.py`; move the storage race/fsync tests and rewrite their
patches to the owner lookup. Red proof: a moved replacement-race test and an
owner-delegation sabotage test fail before wiring. Consolidate the duplicated
record decode loop without changing exact-read overflow versus bounded-scan
semantics. Accept when all `tests/test_queue.py:180-890` behaviors pass, no
domain owner is imported by storage, old bodies are deleted, the ratchet drops,
the file is below 800, and all local gates pass.

### CQ3 — refactor(queue): extract queue layout and canonical path safety

Extract `queue_layout.py` and move root, directory, family-limit, and canonical
access checks. Red proof: unsafe-intermediate and oversized-family sabotage
tests target the new owner before wiring. Preserve owner-private permissions,
hardlink/reparse refusal, path identity, and exact error types. Accept when
readiness/startup tamper tests pass, no fallback repair is added, old bodies are
deleted, the ratchet drops, the file is below 800, and all local gates pass.

### CQ4 — refactor(queue): extract sealed startup legacy audit

Extract `queue_legacy_audit.py` and move its tests/fault hooks. Red proof: patch
`after_audit_phase` and prove startup observes it. Preserve one-time exclusive
audit, bounded scans, seal validation, and no-history sealed startup. Accept
when `tests/test_queue_startup_audit.py:41-372` and worker lifetime-lock tests
pass, all old methods/hooks are deleted, the ratchet drops, the file is below
800, and all local gates pass.

### CQ5 — refactor(queue): extract v0.9 output migration and receipts

Extract `queue_legacy_output.py`; move migration tests and budget constants.
Red proof: archive-phase crash and aggregate-budget tests patch the new owner.
Preserve authorization, archive/receipt hashes, sequence, compatibility reads,
retired receipt coverage, and restart idempotency. Accept when
`tests/test_legacy_output_migration.py:70-588` passes with no new fallback, old
code is deleted, the ratchet drops, the file is below 800, and all local gates
pass.

### CQ6 — refactor(queue): extract index state and bounded migration

Extract `queue_index_state.py` and `queue_index_migration.py`; leave domain
rebuilders in their owners. Red proof: patch one domain migration function at
the module lookup and prove dispatch reaches it. Preserve cursor/checkpoint
shape, bounded reconciliation, completeness gates, and tamper refusal. Accept
when migration/global-order sabotage tests pass, both files are below 800, old
coordinator/state code is deleted, the ratchet drops, and all local gates pass.

### CQ7 — refactor(queue): extract transition journal and order indexes

Extract `queue_transitions.py` and `queue_order_index.py`. Red proof: one hard
crash fixture fails until transition kind dispatch is wired, and one stable-page
test fails until the order owner is used. Preserve every intent kind, unknown
kind refusal, global/per-job cursor semantics, and lock boundaries. Accept when
hard-crash fixtures and `tests/test_core_global_pagination.py:31-158` pass, both
files are below 800, old bodies are deleted, the ratchet drops, and all local
gates pass.

### CQ8 — refactor(queue): extract idempotency, endpoints, and JARVIS input records

Land three consecutive commits/issues if review capacity requires it, one owner
per commit: `queue_idempotency.py`, `queue_endpoints.py`, then
`queue_jarvis_inputs.py`. Each starts with a red facade-delegation sabotage
test and moves its logic tests. Preserve digest identity/crash recovery, fresh
endpoint boundedness, and immutable contract/lineage/manifest records. Accept
each owner only when its old body is deleted, its file is below 800, the ratchet
drops, and all local gates pass before the next owner starts.

### CQ9 — refactor(queue): extract jobs and terminal GC

Extract `queue_jobs.py`, then `queue_job_gc.py`. Red proof: sabotage each named
submission collaborator, then move the GC phase-crash test. Preserve public
signatures, idempotent replay, active/order/session/scheduler derived indexes,
tombstone phases, quarantine-before-purge, and every protection source. Delete
the dead gateway unindex wrapper only in CQ14, not here. Accept when job,
idempotency, retention, and storage-managed queue suites pass, both files are
below 800, old bodies are deleted, the ratchet drops, and all local gates pass.

### CQ10 — refactor(queue): extract lease capacity and operational indexes

Extract `queue_lease_records.py`, `queue_lease_capacity.py`, and
`queue_lease_indexes.py`. Red proof: move aggregate/checkpoint tamper and
repair-intent crash tests to owner lookups. Preserve O(1) steady admission,
exact audit/repair, generation/digest pairing, reference identity, hardlink
refusal, and bounded scans. Accept when lease-capacity and operational-index
suites pass, all files are below 800, old bodies are deleted, the ratchet drops,
and all local gates pass.

### CQ11 — refactor(queue): extract lease lifecycle, recovery, and scheduler cancel

Extract `queue_lease_recovery.py`, `queue_leases.py`, then
`queue_scheduler_cancel.py`. Red proof: move stale-recovery fault hooks,
acquire/restart tests, and cancel claim races before each wiring step. Preserve
exactly-once leasing, capacity generations, crash replay, retry exhaustion,
cancel identity/disposition idempotency, and bounded claim leases. Provider I/O
must not enter these modules. Accept when lease, recovery, scheduler-cancel, and
storage-managed suites pass, every file is below 800, old bodies are deleted,
the ratchet drops, and all local gates pass.

### CQ12 — refactor(queue): extract execution-cleanup persistence

Extract `queue_execution_cleanup.py`. Red proof: move one flat-to-shard crash
test and one truncated scan test before delegation. Preserve path migration,
ack idempotency, pending queries, bounded reads, and GC protection semantics.
Accept when endpoint cleanup and retention integration tests pass, the file is
below 800, old bodies are deleted, the ratchet drops, and all local gates pass.

### CQ13 — refactor(queue): extract artifacts, lineage, transforms, and input ingest

Extract `queue_artifacts.py`, `queue_artifact_lineage.py`, and
`queue_input_ingest.py`. Red proof: move lineage crash-gap/cardinality tests and
ingest abandonment/quota tests to owner lookup sites. Preserve sequence mirrored
into provenance, immutable forward/reverse edges, cursor ordering, transform
uniqueness, owner-generation quota, and ingest retry identity. Accept when
artifact-lineage/input-ingest suites pass, all files are below 800, old bodies
are deleted, the ratchet drops, and all local gates pass.

### CQ14 — refactor(queue): extract gateway and owner-session persistence

Extract gateway, browser attachment, owner-session, gateway-index, and monitor
owners one at a time. Red proof: move browser CAS, owner-generation crash/race,
and backlink-bound tests before their wiring. Delete the dead
`_unindex_gateway_session_unlocked` wrapper and the duplicated gateway/monitor
predicates. Preserve service/session facade calls and require no semantic edit
to `service_runtime.py`, `session_lifecycle.py`, `session_wire_models.py`, or
`frp_link.py`. Accept when their integration suites plus gateway/retention/index
tests pass, every file is below 800, old bodies are deleted, the ratchet drops,
and all local gates pass.

### CQ15 — refactor(queue): extract task, event, and progress persistence after #234

Block until `fix/234-task-admission` merges; rebase before editing. Extract
`queue_tasks.py`, `queue_events.py`, and `queue_progress.py` without changing
FastMCP admission code. Red proof: move durable task/event tests and run the
FastMCP identity-conflict/CAS sabotage tests against the unchanged facade.
Preserve public signatures, exception types, optimistic concurrency, task input
rounds, cursor heads, and progress-emitted events. Accept when the full FastMCP
task suite passes, all files are below 800, old bodies are deleted, the ratchet
drops, and all local gates pass.

### CQ16 — refactor(queue): collapse `core_queue.py` to the compatibility facade

Delete all remaining implementation/private shims, keep only constructor/state,
typed public forwarding methods, and live import-compatibility re-exports. Red
proof: compare captured public names/signatures and run the complete patch-seam
AST guard before deleting the last bodies. Accept when `core_queue.py` is below
800 and removed from the file-size baseline, every re-export has a live importer,
the 102 audited patch sites resolve as specified, no owner exceeds 800, the
deletion ledger is empty, and all local gates pass with zero skips/failures.
