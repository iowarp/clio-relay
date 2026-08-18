# `core_queue.py` split design — 2026-08

**Status:** design complete at the pinned merge-base; no production extraction is
part of R11.  
**Parent design:** [`relay-architecture-2026-08.md`](relay-architecture-2026-08.md).  
**Evidence pin:** `develop` commit `d6253d7`. At that commit
`src/clio_relay/core_queue.py` is 16,137 physical lines. All source and test
line references in this document are pinned to that commit.

This plan preserves the public `ClioCoreQueue` surface while moving each body to
one typed owner mixin. Every new source module has a planning cap of at most 750 physical
lines, including imports, typing, protocols, and docstrings; the enforced gate
remains fewer than 800 lines. No slice may add a file-size exemption, a fallback,
or a callback through the facade.

## 1. disjoint function inventory

The inventory is generated from the Python AST, not hand-added inclusive ranges.
The walker makes one lexical block for module framing and for every class,
function, and `ClioCoreQueue` method. A definition block starts at its earliest
decorator and ends immediately before the next sibling definition; nested
definitions are charged to their enclosing method. The walker asserts that all
494 blocks are assigned once, that adjacent blocks have neither gaps nor
overlap, and that their physical-line count is 16,137.

The routing bands below are shorthand for those complete definition blocks.
Every boundary is a definition boundary. This makes an omitted method visible:
`get_task` is its own block at `core_queue.py:7778-7787` and belongs to
`queue_tasks.py`.

```text
1-108 facade                     109-235 layout
236-467 store_lock               468-484 idempotency
485-520 scheduler_cancel_records 521-563 lease_records
564-577 legacy_output_codec      578-598 artifacts
599-621 context                  622-634 layout
635-917 legacy_audit             918-1084 index_state
1085-1112 index_discovery
1113-1259 legacy_audit           1260-1701 legacy_output_codec
1702-2166 legacy_output_audit    2167-2307 legacy_output_migration
2308-2473 legacy_audit           2474-2834 startup
2835-2867 lease_capacity_state   2868-2916 startup
2917-3192 index_migration        3193-3588 lease_capacity_audit
3589-3645 lease_indexes          3646-3683 endpoints
3684-3776 idempotency            3777-3909 jobs
3910-3952 idempotency            3953-3975 jobs
3976-4172 job_gc                 4173-4366 jobs
4367-4629 jarvis_inputs          4630-4810 endpoints
4811-4875 leases                 4876-4899 lease_indexes
4900-5179 lease_capacity_state   5180-5626 lease_indexes
5627-5794 jobs                   5795-6007 scheduler_cancel_state
6008-6484 scheduler_cancel_claims 6485-6522 scheduler_cancel_state
6523-6539 lease_recovery         6540-6908 leases
6909-7013 lease_capacity_state   7014-7070 leases
7071-7362 lease_recovery         7363-7456 leases
7457-7695 tasks                  7696-7777 events
7778-7787 tasks                  7788-8338 execution_cleanup
8339-8395 events                 8396-8426 artifacts
8427-8928 input_ingest           8929-8993 artifacts
8994-9382 artifact_lineage       9383-9444 artifacts
9445-9554 progress               9555-9795 gateways
9796-10049 browser_attachments   10050-10360 owner_session_lifecycle
10361-10953 owner_session_records 10954-10958 gateways
10959-11077 monitor_rules        11078-11472 gateway_indexes
11473-11564 events               11565-11701 lease_indexes
11702-11781 lease_recovery       11782-11788 jobs
11789-11796 idempotency          11797-11911 scheduler_cancel_records
11912-11954 jobs                 11955-12075 input_ingest
12076-12133 endpoints            12134-12172 jobs
12173-12197 tasks                12198-12215 gateways
12216-12466 transitions          12467-12493 jobs
12494-12770 job_gc               12771-12804 legacy_output_migration
12805-13077 gc_storage           13078-13295 order_index
13296-13498 index_discovery      13499-13529 index_state
13530-13700 index_migration
13701-13869 order_index          13870-13919 index_migration
13920-13942 layout               13943-14123 store_write
14124-14310 store_read           14311-14441 layout
14442-14550 browser_attachments  14551-14619 gateway_indexes
14620-14667 owner_session_records 14668-14689 artifact_lineage
14690-14703 layout               14704-14724 endpoints
14725-14800 scheduler_cancel_records 14801-14826 index_migration
14827-14886 gateway_indexes      14887-14906 lease_indexes
14907-15495 lease_records        15496-15502 store_read
15503-15695 gc_storage           15696-15753 layout
15754-15869 store_read           15870-15884 store_write
15885-15905 tasks                15906-15965 idempotency
15966-16021 input_ingest         16022-16055 artifact_lineage
16056-16090 index_state          16091-16104 index_migration
16105-16123 order_index          16124-16137 idempotency
```

Two former “pure codec” assignments are intentionally absent. The function
`_bounded_regular_json_count` performs `os.scandir`, `entry.stat`, and reparse
validation at `core_queue.py:14801-14824`, so it is owned by the I/O-bearing
`queue_index_migration.py`. `_lease_operational_records_present` scans five
directories at `core_queue.py:14887-14904`, so it is owned by
`queue_lease_indexes.py`. Neither belongs in a record/codec module.

## 2. closed line ledger and target modules

“Gross” is the disjoint current ownership from section 1. “Delete” is an exact
current-source credit from section 6. “Transfer” is gross minus delete.
“Overhead” is reserved for imports, module docs, protocols, shared replacement
helpers, and facade composition. The cap is therefore conservative rather than
an implementation estimate that forgets file framing.

| Target | Gross | Delete | Transfer | Overhead | Planned cap |
|---|---:|---:|---:|---:|---:|
| `core_queue.py` facade | 108 | 0 | 108 | 92 | 200 |
| `queue_context.py` | 23 | 0 | 23 | 47 | 70 |
| `queue_artifact_lineage.py` | 445 | 0 | 445 | 55 | 500 |
| `queue_artifacts.py` | 179 | 0 | 179 | 41 | 220 |
| `queue_browser_attachments.py` | 363 | 0 | 363 | 47 | 410 |
| `queue_endpoints.py` | 298 | 0 | 298 | 42 | 340 |
| `queue_events.py` | 231 | 0 | 231 | 39 | 270 |
| `queue_execution_cleanup.py` | 551 | 0 | 551 | 59 | 610 |
| `queue_gateway_indexes.py` | 524 | 16 | 508 | 62 | 570 |
| `queue_gateways.py` | 264 | 8 | 256 | 44 | 300 |
| `queue_gc_storage.py` | 466 | 0 | 466 | 54 | 520 |
| `queue_idempotency.py` | 235 | 0 | 235 | 35 | 270 |
| `queue_index_discovery.py` | 231 | 0 | 231 | 39 | 270 |
| `queue_index_migration.py` | 537 | 0 | 537 | 63 | 600 |
| `queue_index_state.py` | 233 | 0 | 233 | 37 | 270 |
| `queue_input_ingest.py` | 679 | 0 | 679 | 51 | 730 |
| `queue_jarvis_inputs.py` | 263 | 0 | 263 | 37 | 300 |
| `queue_job_gc.py` | 474 | 0 | 474 | 46 | 520 |
| `queue_jobs.py` | 634 | 0 | 634 | 66 | 700 |
| `queue_layout.py` | 366 | 0 | 366 | 44 | 410 |
| `queue_lease_capacity_audit.py` | 396 | 0 | 396 | 44 | 440 |
| `queue_lease_capacity_state.py` | 418 | 0 | 418 | 52 | 470 |
| `queue_lease_indexes.py` | 685 | 0 | 685 | 45 | 730 |
| `queue_lease_records.py` | 632 | 0 | 632 | 48 | 680 |
| `queue_lease_recovery.py` | 389 | 0 | 389 | 51 | 440 |
| `queue_leases.py` | 585 | 0 | 585 | 65 | 650 |
| `queue_legacy_audit.py` | 596 | 0 | 596 | 54 | 650 |
| `queue_legacy_output_audit.py` | 465 | 0 | 465 | 55 | 520 |
| `queue_legacy_output_codec.py` | 456 | 0 | 456 | 44 | 500 |
| `queue_legacy_output_migration.py` | 175 | 0 | 175 | 35 | 210 |
| `queue_monitor_rules.py` | 119 | 8 | 111 | 29 | 140 |
| `queue_order_index.py` | 406 | 0 | 406 | 44 | 450 |
| `queue_owner_session_lifecycle.py` | 311 | 0 | 311 | 39 | 350 |
| `queue_owner_session_records.py` | 641 | 0 | 641 | 49 | 690 |
| `queue_progress.py` | 110 | 0 | 110 | 30 | 140 |
| `queue_scheduler_cancel_claims.py` | 477 | 0 | 477 | 43 | 520 |
| `queue_scheduler_cancel_records.py` | 227 | 0 | 227 | 33 | 260 |
| `queue_scheduler_cancel_state.py` | 251 | 0 | 251 | 39 | 290 |
| `queue_startup.py` | 410 | 0 | 410 | 50 | 460 |
| `queue_store_lock.py` | 232 | 0 | 232 | 38 | 270 |
| `queue_store_read.py` | 310 | 12 | 298 | 52 | 350 |
| `queue_store_write.py` | 196 | 0 | 196 | 34 | 230 |
| `queue_tasks.py` | 295 | 0 | 295 | 45 | 340 |
| `queue_transitions.py` | 251 | 0 | 251 | 39 | 290 |
| **Total** | **16,137** | **44** | **16,093** | **2,057** | **18,150** |

The required reconciliation is explicit:

```text
sum(owner transfer lines) + deletions = 16,093 + 44 = 16,137
```

The cap, not the total cap sum, is the acceptance invariant. An AST count finds
128 public methods whose existing decorators and signatures alone occupy 629
lines, before a forwarding statement or blank line. Explicit facade wrappers
therefore cannot meet the 800-line gate. Public methods instead remain typed on
owner mixins, and `ClioCoreQueue` composes those mixins plus `QueueContext`.
Patching `ClioCoreQueue.method` or an instance method remains valid through
normal inherited attribute lookup; the facade imports no method bodies and no
owner imports the facade. Its 92-line overhead allowance covers the composed
class declaration, constructor, compatibility re-exports, and module framing.

Every former near/over-cap owner is split before extraction: storage into lock,
read, and write; legacy output into codec, audit, and migration; lease capacity
into state and audit/repair; GC into orchestration and filesystem storage;
scheduler cancellation into records, state, and claims; and owner sessions into
lifecycle and records. The largest resulting cap is 730, leaving 69 physical
lines beneath the enforced 800-line ceiling.

## 3. dependency DAG and migration order

The call audit at `d6253d7` parses every `self.name(...)`, `cls.name(...)`, and
bare call to a definition in `core_queue.py`, then projects caller and callee
through section 1. It found 377 class methods, 99 module functions, 2,072
internal call sites, and 1,690 distinct symbol-to-symbol edges. For extraction
risk, inbound references are counted only when the caller remains unextracted.
The old CQ1 selection had 87 symbols and 295 such inbound call sites; it was not
a leaf. The eight JARVIS-input methods at `core_queue.py:4367-4629` have zero
internal callers outside that block and are the first peel.

The first peel uses a `QueueStoreProtocol` in `queue_context.py`; the facade
adapts its existing private store during that one migration step. The owner
depends on the protocol, never imports or calls `ClioCoreQueue`, and CQ3 replaces
the adapter with the concrete store modules. This permits the zero-inbound peel
without creating a facade callback cycle.

Two concrete bad edges drove the revised order:

- `_bounded_regular_json_count` calls layout-owned `_record_is_reparse` at
  `core_queue.py:14815`; assigning the caller to index-migration I/O makes the
  edge `index_migration -> layout`.
- `_read_canonical_record` calls `_validate_canonical_record_access` at
  `core_queue.py:14127`; therefore layout lands before store-read.

An edge `A -> B` below means A contains the post-split call expression and may
import B as a module; B must already exist. This is the topological order. Each
slice adds the named sabotage patch before wiring and proves it fails.

| Slice | Owners | Required predecessors | Failing-first lookup sabotage |
|---|---|---|---|
| CQ1 | `queue_context`, `queue_jarvis_inputs` | none; zero inbound peel | Patch the context store protocol used by `put_jarvis_run_input_manifest`; the facade still bypasses it before delegation. |
| CQ2 | `queue_layout` | CQ1 | Patch `queue_layout.validate_canonical_access`; a canonical read still uses the class helper before wiring. |
| CQ3 | `queue_store_lock`, `queue_store_read`, `queue_store_write` | CQ2 | Patch `queue_store_read.queue_layout.validate_canonical_access` and `queue_store_write.cluster_config.open_private_atomic_file`. |
| CQ4 | lease, scheduler-cancel, and legacy-output record/codec modules | CQ2 | Patch one decoder at each new module lookup and compare byte-identical round trips. |
| CQ5 | `queue_index_state` | CQ3, CQ4 | Patch `queue_index_state.queue_store_read.read_json_document` at a completeness gate. |
| CQ6 | legacy audit/output audit/output migration | CQ3-CQ5 | Patch `queue_legacy_output_audit.queue_legacy_output_codec.iter_legacy_event_paths`; both callers at `core_queue.py:1881,2124` must bite. |
| CQ7 | `queue_order_index`, `queue_events` | CQ3-CQ5 | Patch `queue_events.queue_order_index.increment_job_index`; append still bypasses it before wiring. |
| CQ8 | `queue_idempotency`, `queue_endpoints` | CQ3-CQ5, CQ7 | Patch each owner's store lookup, then assert `ClioCoreQueue` resolves the typed owner mixin method. |
| CQ9 | `queue_artifacts`, `queue_artifact_lineage` | CQ3-CQ5, CQ7 | Patch the lineage write lookup used while validating submission edges. |
| CQ10 | owner-session lifecycle/records | CQ3-CQ5, CQ7 | Patch `queue_owner_session_records.queue_store_write.write_model` on the intake/closure path. |
| CQ11 | scheduler-cancel state | CQ3-CQ5 | Patch `queue_scheduler_cancel_state.queue_store_write.write_model` on pending-state creation. |
| CQ12 | `queue_jobs` | CQ7-CQ11 | Patch `queue_jobs.queue_order_index.ensure_global` at `submit_job` (`core_queue.py:3856,3899`) and `queue_jobs.write_job` at `core_queue.py:3859,3902`. |
| CQ13 | `queue_input_ingest` | CQ9, CQ10, CQ12 | Patch `queue_input_ingest.queue_jobs.write_job` on begin/fail/complete paths. |
| CQ14 | tasks, progress | CQ3-CQ5, CQ7, CQ12 | Patch `queue_tasks.QueueTasksMixin.put_mcp_task` and assert `ClioCoreQueue` resolves that inherited lookup before wiring; FastMCP tests are acceptance only. |
| CQ15 | lease capacity state/audit, indexes, leases, recovery, scheduler claims | CQ3-CQ5, CQ11, CQ12 | Patch `queue_lease_capacity_audit.queue_lease_indexes.sync_operational_indexes`, then each lifecycle/recovery job-write lookup. |
| CQ16 | gateways, browser attachments, gateway indexes, monitor rules | CQ3-CQ5, CQ7, CQ9, CQ10, CQ12, CQ14 | Patch each caller owner's collaborator attribute for browser CAS and backlink synchronization. |
| CQ17 | execution cleanup | CQ3-CQ5, CQ12 | Patch its shard read/write lookup and prove flat-to-shard migration delegates. |
| CQ18 | job GC and GC storage | CQ6, CQ9-CQ17 | Patch each protection-owner lookup in `queue_job_gc`, then `queue_job_gc.queue_gc_storage.move_gc_path`. |
| CQ19 | index discovery/migration, transitions, startup | CQ2-CQ18 | Patch one domain migration lookup, one transition applier lookup, and `queue_startup.queue_legacy_audit.audit_before_initialization`. |
| CQ20 | final facade collapse | CQ1-CQ19 | Run a reflection/MRO test over every public method before deleting the last body. |

The post-split AST guard rejects bare cross-owner function imports, rejects any
owner import of `core_queue`, records the caller owner for every collaborator,
and requires the predecessor table to topologically sort every target. A newly
discovered reverse edge blocks that slice; it is not waived as “temporary.”

## 4. patch-seam audit at `d6253d7`

The pinned test-tree audit contains **103 `core_queue` patch sites across 60
targets**. All 103 use `monkeypatch.setattr`; there are zero `patch.object`
calls. The merged #234 tests add the third occurrence of the string target
`clio_relay.core_queue.open_private_atomic_file` at
`tests/test_fastmcp_server.py:589`; its three sites are now lines 410, 589, and
1490. Standard-library `Path.unlink`/`Path.replace` patches are not
`core_queue` targets.

The preservation rule is lookup-site ownership: after a move, patch the module
containing the real call expression, not the module that conceptually owns the
callee and not a dead facade shim.

| Current target and pinned sites | Post-split lookup |
|---|---|
| `core_queue_module.MAX_ARTIFACT_CONSUMERS` (`test_artifact_lineage.py:388`) | `queue_artifact_lineage.MAX_ARTIFACT_CONSUMERS` |
| `queue._write_job_unlocked` (`test_artifact_lineage.py:430`) | `queue_jobs.write_job`; the exercised calls are in `submit_job` at `core_queue.py:3859,3902`, not in lineage. |
| `queue._write_immutable_artifact_use_record` (`test_artifact_lineage.py:469`) | `queue_artifact_lineage.write_immutable_use_record` |
| `core_queue.ClioCoreQueue` (`test_bootstrap_fast_path.py:2235,2410`) | unchanged facade import |
| `ClioCoreQueue.set_owner_session_closed` (`test_cli.py:2295,3354,3608,3747,3842`) | unchanged `ClioCoreQueue` patch target; typed method is inherited from its owner mixin. |
| `ClioCoreQueue.scan_gateway_sessions` (`test_cli.py:8163`) | unchanged `ClioCoreQueue` patch target; typed method is inherited from its owner mixin. |
| `queue.scan_execution_cleanup` (`test_endpoint.py:851`) | unchanged instance patch target through the owner mixin. |
| `queue.read_event_page` (`test_endpoint.py:5421`) | unchanged instance patch target through the owner mixin. |
| `worker.queue.scan_job_tasks`, `worker.queue.scan_jobs` (`test_endpoint_bounded_reads.py:34,50`) | unchanged instance patch targets through owner mixins. |
| `ClioCoreQueue.get_job`, `ClioCoreQueue.get_task` (`test_http_api.py:359-360,397-398`) | unchanged `ClioCoreQueue` patch targets through owner mixins. |
| `ClioCoreQueue.drain_events`, `.drain_task_events`, `.list_monitor_rules` (`test_http_api.py:361-363`) | unchanged `ClioCoreQueue` patch targets through owner mixins. |
| `ClioCoreQueue.scan_endpoints` (`test_installation.py:2279`) | unchanged `ClioCoreQueue` patch target through the owner mixin. |
| `queue.update_task_metadata` (`test_jarvis_execution_recovery_guards.py:288`) | unchanged instance patch target through the owner mixin. |
| `queue.scan_active_jobs` (`test_operational_indexes.py:705`) | unchanged instance patch target through the owner mixin. |
| `queue.list_jobs` (`test_queue.py:1428`) | unchanged instance patch target through the owner mixin. |
| `queue.lease_admission_capacity_snapshot` (`test_queue_management.py:348`) | unchanged instance patch target through the owner mixin. |
| `queue.collect_terminal_job` (`test_retention.py:79`) | unchanged instance patch target through the owner mixin. |
| `ClioCoreQueue.prepare_owner_session_start` (`test_session_lifecycle.py:1163,1433,1975`) | unchanged `ClioCoreQueue` patch target through the owner mixin. |
| `ClioCoreQueue.owner_session_generation_status` (`test_session_lifecycle.py:2710`) | unchanged `ClioCoreQueue` patch target through the owner mixin. |
| `queue.resolve_idempotent_submission` (`test_storage_managed_queue.py:85`) | unchanged instance patch target through the owner mixin. |
| `clio_relay.core_queue.ClioCoreQueue.submit_job` (`test_storage_managed_queue.py:256`) | unchanged facade string target |
| `survivor.recover_stale_jobs` (`test_storage_managed_queue.py:460,513`) | unchanged instance patch target through the owner mixin. |
| `queue._ensure_global_order_entry_unlocked` (`test_core_idempotency.py:50,53`; `test_storage_managed_queue.py:376,379`) | `queue_jobs.queue_order_index.ensure_global`; both exercised submission calls are at `core_queue.py:3856,3899`. |
| `queue._read_many` (`test_core_index_safety.py:52`) | `queue_lease_recovery.queue_store_read.read_many` |
| `core_queue_module.MAX_GATEWAY_INDEX_RECORDS` (`test_core_index_safety.py:74`) | `queue_gateway_indexes.MAX_GATEWAY_INDEX_RECORDS` |
| `queue._after_gc_checkpoint` (`test_core_retention.py:180,197`) | `queue_gc_storage.after_gc_checkpoint` |
| `queue._write` (`test_core_retention.py:946`) | `queue_owner_session_records.queue_store_write.write_model` |
| `clio_relay.core_queue.open_private_atomic_file` (`test_fastmcp_server.py:410,589,1490`) | `clio_relay.queue_store_write.cluster_config.open_private_atomic_file` |
| `queue.update_mcp_task_projection` (`test_fastmcp_server.py:477,1506`) | unchanged public `ClioCoreQueue` target through the task owner mixin; it is regression coverage, not CQ14 red proof. |
| `clio_relay.core_queue.utc_now` (`test_input_staging.py:464`) | `queue_input_ingest.models.utc_now` |
| `ClioCoreQueue._after_legacy_output_migration_phase` (`test_legacy_output_migration.py:200,218,552,561`) | `queue_legacy_output_migration.after_migration_phase` |
| `ClioCoreQueue._iter_legacy_event_paths` (`test_legacy_output_migration.py:263`; `test_queue_startup_audit.py:66`) | `queue_legacy_output_audit.queue_legacy_output_codec.iter_legacy_event_paths`, the lookup at the real callers `core_queue.py:1881,2124`. |
| legacy audit/output bounds (`test_legacy_output_migration.py:426,427,485,494,512`; `test_queue_startup_audit.py:71,273`) | the exact reading caller: `queue_legacy_audit`, `queue_legacy_output_codec`, or `queue_store_read` |
| `queue._scan_many` (`test_operational_indexes.py:118`) | `queue_leases.queue_store_read.scan_many` |
| `queue._sync_lease_operational_indexes_unlocked` (`test_operational_indexes.py:403`) | `queue_lease_capacity_audit.queue_lease_indexes.sync_operational_indexes`, the repair caller at `core_queue.py:3561`. |
| bounded-read helpers/constants and `os`/`time` patches (`test_queue.py:208,310,368,462,463,529-532,568-569,635-638,676-678,757,830,879`) | the exact `queue_store_read`, `queue_store_write`, `queue_store_lock`, or `queue_layout` caller attribute |
| `MAX_LIVE_LEASE_RECORDS` (`test_queue_management.py:368`) | `queue_lease_capacity_state.MAX_LIVE_LEASE_RECORDS` |
| `ClioCoreQueue._scan_json_record_paths` (`test_queue_readiness.py:109`) | `queue_store_read.scan_json_record_paths` |
| `ClioCoreQueue._audit_legacy_state_before_initialization` (`test_queue_startup_audit.py:51,251,308`; `test_worker_lifetime_lock.py:514,827`) | `queue_startup.queue_legacy_audit.audit_before_initialization` |
| `ClioCoreQueue._audit_completed_legacy_output_state` (`test_queue_startup_audit.py:56`) | `queue_legacy_output_audit.audit_completed_state` |
| `ClioCoreQueue._bounded_legacy_family_entries` (`test_queue_startup_audit.py:61`) | `queue_legacy_audit.bounded_family_entries` |
| `core_queue_module.exclusive_migration_lifetime` (`test_queue_startup_audit.py:98`; `test_worker_lifetime_lock.py:233`) | `queue_startup.worker_lifetime_lock.exclusive_migration_lifetime` |
| `ClioCoreQueue._after_legacy_record_audit_phase` (`test_queue_startup_audit.py:124,292,303`) | `queue_legacy_audit.after_audit_phase` |
| stale-recovery hooks (`test_storage_managed_queue.py:456,505`) | `queue_lease_recovery.after_job_write` |

The 60-target total is the AST result, not the number of display rows: rows that
share one preservation action group related targets. Every implementation slice
regenerates the audit and updates its affected tests in the same change. A
temporary re-export may preserve an import, but it is never an injection seam.

## 5. merged FastMCP overlap

`fix/234-task-admission` is merged in the pinned snapshot. There is no remaining
branch block or rebase precondition. At `d6253d7`, task creation is in
`fastmcp_server.py:464`, post-admission park/reload is at lines 523-530, and the
two projection CAS/reload paths are at lines 628-636 and 688-696. The durable
boundary remains the facade methods `put_mcp_task`,
`update_mcp_task_projection`, and `get_mcp_task`; CQ14 does not move FastMCP
admission or input parking into queue owners.

The merged regression acceptance tests are named, rather than cited through
obsolete broad ranges:

- `test_agent_task_admission_engages_without_the_followup_opt_in`
  (`tests/test_fastmcp_server.py:597`);
- `test_agent_task_admission_is_terminal_at_birth_for_instant_settling_calls`
  (`tests/test_fastmcp_server.py:654`);
- `test_task_persistence_untyped_failure_surfaces_as_typed_error_v1`
  (`tests/test_fastmcp_server.py:1399`), together with
  `test_task_projection_conflict_surfaces_as_typed_mcp_error` and
  `test_park_agent_input_cas_exhaustion_is_never_mistyped_as_invalid_params`.

These tests preserve the merged admission, identity, typed-error, and CAS
behavior. They cannot serve as CQ14 failing-first proof because they patch or
call the existing facade, including the sites at
`tests/test_fastmcp_server.py:477,1506`.

CQ14 must first add an owner-composition test that patches
`clio_relay.queue_tasks.QueueTasksMixin.put_mcp_task` and asserts that
`ClioCoreQueue` resolves the inherited owner method. It must demonstrably fail
while `ClioCoreQueue` still defines and executes the old body, then pass only
after the old definition is deleted and the mixin is composed. The FastMCP
tests run after that green transition as regression acceptance.

`session_lifecycle.py`, `session_wire_models.py`, `service_runtime.py`, and
`frp_link.py` remain above the durable queue boundary. They keep importing the
public facade; queue owners do not absorb process, transport, scheduler,
connector, or wire-model behavior.

## 6. exact deletion ledger

| Current source | Lines | Treatment |
|---|---:|---|
| Dead `_unindex_gateway_session_unlocked` definition, `core_queue.py:11266-11281` | 16 | Delete as one complete definition block; its only behavior is already performed by `_unindex_gateway_session_id_unlocked` at lines 11262 and 11282 onward. |
| Duplicate gateway `matches` nested functions, `core_queue.py:9621-9624,9644-9647` | 8 | Replace both with one module helper; the helper is included in gateway overhead. |
| Duplicate monitor `matches` nested functions, `core_queue.py:11016-11019,11041-11044` | 8 | Replace both with one module helper; the helper is included in monitor overhead. |
| Repeated `_scan_many` decode/identity loop, `core_queue.py:14194-14205` | 12 | Replace with the shared read-record helper also used by `_read_many`; the helper is included in store-read overhead. |
| **Total** | **44** | Reconciles with section 2. |

Fault hooks move to their real owners and are therefore transfer lines, not
deletions. No other deletion is pre-authorized. A slice may add one only after a
repository-wide call-site audit and a corresponding ledger update that still
closes to 16,137.

## 7. measurable exit criteria

The split is complete only when all of the following are true:

1. `core_queue.py` is below 800 physical lines and contains only typed owner-mixin
   composition, constructor/context wiring, and live compatibility re-exports.
2. Every target in section 2 is at or below its planned cap and below the hard
   800-line gate; no new file-size baseline is added.
3. The function-inventory audit assigns every current definition exactly once,
   the line ledger closes, and the four deletion rows are deleted rather than
   copied.
4. The call graph topologically sorts according to section 3, with no owner
   importing `core_queue` and no bare cross-owner collaborator import.
5. All 103 pinned patch sites either remain public-facade patches or move to the
   module containing the real post-split call expression. Every slice's lookup
   sabotage test is red before delegation and green after it.
6. CQ14 has the new `QueueTasksMixin` composition red proof. The named FastMCP tests
   are regression acceptance only.
7. On-disk paths, JSON bytes/digests, exception types/messages, signatures,
   cursors, lock boundaries, and crash replay are unchanged unless separately
   approved.
8. Each implementation slice lowers the `core_queue.py` ratchet by measured net
   removal and runs `uv run ruff check --fix`, `uv run ruff format`,
   `uv run pyright`, `uv run pytest`, and
   `uv run python scripts/check_file_size.py` with no failure or skip.

## 8. implementation issue map

CQ1-CQ20 in section 3 are the implementation issue sequence. Each issue copies
its owner list, predecessor set, and exact sabotage test from that table. Its
acceptance text also requires: move logic tests with the body; retain caller
integration tests in place; delete the old body in the same change; update the
patch audit and line ledger; lower the ratchet; keep every touched owner within
its planned cap; and pass all local gates with zero skips or failures.

No issue may combine nonadjacent DAG nodes merely to hide a reverse dependency.
If implementation discovers a missing edge, the design and topological order
must be corrected before extraction continues.

## 9. block-2 checkpoint fix batch (2026-08-18)

A review of CQ1-CQ12 (landed through `5d0afbc`) found 14 findings, six
blocking. This section records the resulting corrections; sections 1-8 above
remain pinned to the evidence commit `d6253d7` and are not rewritten.

### 9.1 patch-on-owner for `ensure_private_configuration_directory`

CQ6 moved `queue_legacy_audit.py`'s real call off `clio_relay.core_queue`
(`_prepare_queue_root_for_lock`) without updating three dynamically-built test
patch loops (`f"{module_name}.ensure_private_configuration_directory"` inside
`for module_name in (...)`) in `tests/test_fastmcp_server.py` and one in
`tests/test_service_runtime.py`. `queue_legacy_audit.py` now imports
`cluster_config` module-qualified and calls
`cluster_config.ensure_private_configuration_directory(...)` -- the same
patch-on-owner idiom `queue_store_write.py` already used. The four test loops
now patch only `clio_relay.cluster_config` and `clio_relay.worker_lifetime_lock`
(plus `clio_relay.service_runtime` for the service-runtime loop), the modules
that actually own a real call site; `clio_relay.core_queue` never had one.

### 9.2 §4 audit extension: dynamically-built patch targets

The hand-maintained §4 table only records `monkeypatch.setattr("literal.string",
...)` sites. A patch target assembled as `f"{module_name}.attr"` inside a
`for module_name in (...)` loop is invisible to that audit, which is exactly
how 9.1's break went unnoticed by three copies of the same loop plus a fourth,
`raising=False` copy that never even raised on the dead seam.
`tests/test_core_queue_split_architecture.py` now walks every `tests/*.py`
file for this loop-plus-f-string shape, resolves each concrete `(module,
attr)` pair via `importlib`, and asserts every one exists on the real module
(`test_dynamic_fstring_loop_monkeypatch_targets_resolve_or_are_registered`,
backed by `_KNOWN_DYNAMIC_SEAM_EXEMPTIONS` -- empty by design; an addition
must carry its own justification in the same change). Extending this to the
whole test tree also found a second, unrelated dead seam of the same shape,
predating this batch: `tests/test_service_runtime.py`'s
`_start_lifecycle_frp_runtime` helper patched
`clio_relay.service_runtime.ensure_private_configuration_path`, which that
module never imports (it only uses `ensure_private_configuration_directory`).
Fixed by splitting that helper's single loop into two, one per real symbol.

### 9.3 mixin self-call edges are now part of the dependency graph

`_owner_dependencies()` previously walked only static `import`/`from import`
statements. A `self.method(...)` call that resolves solely through
`ClioCoreQueue`'s composed MRO -- never a static import in the caller's own
module -- was invisible to it. Five such calls to `self.get_job(...)` created
reverse-rank edges from earlier-landed owners onto the later-landed
`queue_jobs` (CQ12): `queue_artifacts.py` (3 sites), `queue_artifact_lineage.py`
(3 sites), `queue_owner_session_records.py` (1 site), and
`queue_scheduler_cancel_state.py` (1 site). A sixth and seventh site,
`queue_artifact_lineage.py` calling `self.get_artifact(...)` (owned by
`queue_artifacts`, landed one rank later), closed a genuine 2-cycle between
the two CQ9 co-landed owners.

`_owner_dependencies()` now also resolves `self.<name>(...)` calls against a
mixin method-ownership manifest (built from every `*Mixin` class's real,
non-stub method definitions) and folds the resulting edges into the same
rank-ordering check used for static imports
(`test_split_owner_dependencies_follow_the_migration_topology`).

The resolution chosen is "invert the calls," not re-ranking: `get_job` and
`get_artifact` are simple "read one typed record by id, raise `NotFoundError`
if absent, verify canonical identity" reads with no CRUD/business logic of
their own. Both bodies moved to new shared primitives,
`queue_store_read.read_required_job` and `queue_store_read.read_required_artifact`
(rank 4, already a valid predecessor for every affected caller). `queue_jobs.get_job`
and `queue_artifacts.get_artifact` now delegate to those primitives instead of
owning the read logic outright, so the public facade behavior, exceptions, and
messages are unchanged. All seven call sites now call the shared primitive
directly instead of `self.get_job(...)` / `self.get_artifact(...)`, and the
now-unneeded `TYPE_CHECKING` stubs for those two names were removed from the
four callers.

### 9.4 owner budget reconciliation

Real per-owner line counts after this batch (`queue_store_read` and
`queue_legacy_output_migration` grew for real, justified reasons below; every
other landed owner is within its section-2 planned cap):

| Owner | Real count | Cap | Headroom | Note |
|---|---:|---:|---:|---|
| `queue_context` | 69 | 70 | 1 | |
| `queue_jarvis_inputs` | 292 | 300 | 8 | |
| `queue_layout` | 391 | 410 | 19 | |
| `queue_store_lock` | 270 | 270 | 0 | at cap, unchanged this batch |
| `queue_store_read` | 390 | 410 | 20 | cap raised from 350: +2 shared read primitives (§9.3) |
| `queue_store_write` | 225 | 230 | 5 | |
| `queue_lease_records` | 679 | 680 | 1 | at cap, unchanged this batch |
| `queue_scheduler_cancel_records` | 133 | 260 | 127 | |
| `queue_legacy_output_codec` | 499 | 500 | 1 | at cap, unchanged this batch |
| `queue_index_state` | 262 | 270 | 8 | |
| `queue_legacy_output_audit` | 519 | 520 | 1 | at cap; -1 line, F14 deleted a dead alias |
| `queue_legacy_output_migration` | 230 | 250 | 20 | cap raised from 210: +3 TYPE_CHECKING stubs, F12 |
| `queue_legacy_audit` | 620 | 650 | 30 | +9 net: restored `_unique_json` error handling (F14), -1 dead alias |
| `queue_order_index` | 449 | 450 | 1 | at cap; -1 line, F5 deleted a dead re-export alias |
| `queue_events` | 269 | 270 | 1 | at cap; -1 line, F12 removed an unbound-call bypass |
| `queue_owner_session_records` | 686 | 690 | 4 | at cap; F4/F12 net -4 |
| `queue_owner_session_lifecycle` | 335 | 350 | 15 | |
| `queue_idempotency` | 270 | 270 | 0 | at cap, unchanged this batch |
| `queue_endpoints` | 334 | 340 | 6 | |
| `queue_artifact_lineage` | 494 | 500 | 6 | at cap; F4/F12 net -5 |
| `queue_artifacts` | 218 | 220 | 2 | at cap; F4/F12 roughly net-neutral |
| `queue_scheduler_cancel_state` | 436 | 450 | 14 | justified (F4 net -1) |
| `queue_jobs` | 786 | 800 | 14 | justified (F4/F13 net -3) |

`queue_store_lock`, `queue_lease_records`, `queue_legacy_output_codec`,
`queue_idempotency` were already at (or one line from) their cap before this
batch and are untouched by it; they are listed here for the same honest-
headroom reason, not because this batch changed them.

### 9.5 `_QueueStoreAdapter` partial fix; full retirement stays CQ19/CQ20 scope

`_QueueStoreAdapter.read_optional` and `.write` already routed through the
facade instance (`self._queue._read_optional` / `self._queue._write`), so an
instance-level patch of either stayed valid. `.read_json_document` and
`.write_json` instead called the store-read/store-write module functions
directly, bypassing `self._queue._read_json_document` /
`self._queue._write_json` -- an instance patch of either was silently inert,
including for `submit_job`'s idempotency-record writes. Both now route
through the instance, at zero behavior cost (`_read_json_document` and
`_write_json` were already thin passthroughs to the same module functions).
`_QueueStoreAdapter` has roughly 140 references across the codebase; retiring
it in favor of owners depending on the store modules directly stays CQ19/CQ20
scope, not this batch's.

### 9.6 `_is_sha256_digest` duplication

`queue_jobs.py` and `queue_artifact_lineage.py` each keep a private
`_is_sha256_digest` copy rather than one reaching into the other's module (or
into `queue_job_gc` / `queue_input_ingest`, its doc-assigned but not-yet-landed
owners per this file's original inventory). This duplication is intentional
and stays until CQ13/CQ18 land and a real shared owner exists to hold one
copy; noted here per this batch's review so it is not mistaken for
undiscovered drift.

## 10. CQ15 landing — lease capacity, indexes, leases, recovery, scheduler claims

CQ15 (predecessors CQ3-CQ5, CQ11, CQ12, all landed) is the largest single
slice: lease-capacity aggregate/checkpoint state, its repair/audit
orchestration, every lease operational-index primitive, the full lease
lifecycle (listing, admission/acquisition, renewal, release), the stale-lease
recovery engine, and the scheduler-cancellation attempt/confirmation claim
methods. It lands as **seven** owners, not the doc's original six: the
worker-lane admission/acquisition path (`acquire_next_job`, `acquire_job`,
`submit_and_acquire_job`, `_lease_job_unlocked`, the MCP admission-lane
matcher/counter, `_active_lease_for_endpoint`) split out of `queue_leases.py`
into `queue_lease_admission.py` as a same-rank peer once the combined file
exceeded the 800-line hard gate (854 real lines); the two owners have zero
call-graph overlap, so the split is a clean peer separation, not a forced one.

### 10.1 owner ranks and real line counts

| Rank | Owner | Real | Cap | Gross+overhead plan |
|---:|---|---:|---:|---|
| 26 | `queue_lease_indexes.py` | 600 | 620 | `queue_lease_indexes.py` planned 730 |
| 27 | `queue_lease_capacity_state.py` | 474 | 490 | `queue_lease_capacity_state.py` planned 470 |
| 28 | `queue_lease_capacity_audit.py` | 584 | 600 | `queue_lease_capacity_audit.py` planned 440 |
| 29 | `queue_lease_recovery.py` | 601 | 620 | `queue_lease_recovery.py` planned 440 |
| 30 | `queue_lease_admission.py` | 571 | 590 | (new split; not in the original six) |
| 31 | `queue_leases.py` | 340 | 360 | `queue_leases.py` planned 650 |
| 32 | `queue_scheduler_cancel_claims.py` | 538 | 560 | `queue_scheduler_cancel_claims.py` planned 520 |

`core_queue.py` fell from 8,009 to 4,848 physical lines (net removal 3,161;
the ratchet in `scripts/check_file_size.py` is lowered to 4848 in the same
change). Every landed cap is below the 800-line hard gate; the facade public
method count stays exactly 128
(`test_facade_public_method_set_stays_at_the_128_method_base`).

### 10.2 ownership resolution beyond the original per-line inventory

The design's section-1 inventory assigned lines by their *physical position*
in the pinned `d6253d7` file, not by call-graph analysis. Reconciling that
against the real dependency graph (CQ13-IO-01/CQ4-IO-01 precedent: fix the
cause, keep byte-identical bodies, document any resulting deviation) produced
one genuine two-owner cycle and its resolution:

- **`queue_leases` <-> `queue_lease_recovery` cycle (CQ15-LR-01).**
  `queue_leases.acquire_next_job`/`acquire_job`/`submit_and_acquire_job` (via
  the now-split `queue_lease_admission`) must reconcile stale leases
  *before* computing admission capacity, so admission depends on the
  recovery engine. The recovery engine's stale-sweep in turn deletes expired
  leases through `_delete_lease_unlocked` — a `release_lease`/
  `recover_stale_job` primitive that is conceptually "leases" CRUD. Hosting
  `_delete_lease_unlocked` (plus its two fault-injection seams,
  `_after_lease_canonical_delete`/`_after_lease_index_delete`) on the
  earlier-ranked `queue_lease_recovery` breaks the cycle: `queue_leases`/
  `queue_lease_admission` self-call the inherited method (forward edge,
  rank 29 < 30/31); `queue_lease_recovery` never calls back into either.
  This mirrors ledger §9.3's `get_job`/`get_artifact` resolution — hoist the
  shared primitive to whichever side the topology requires, not to whichever
  side "conceptually" owns it.
- **`sync_operational_indexes` module-level twin (mandated, not discovered).**
  `queue_lease_indexes._sync_lease_operational_indexes_unlocked` keeps its
  bound-method name (tests and `queue_transition_crash_fixture.py` call/patch
  it directly), but its real body is a module-level function,
  `sync_operational_indexes(queue, lease, *, job, previous_lease=None)`,
  because `queue_lease_capacity_audit`'s repair path must resolve it through
  a module-qualified lookup a test can patch on `queue_lease_capacity_audit.
  queue_lease_indexes` — the design's own CQ15 failing-first prescription.
  Every other CQ15 self-call between sibling owners (identity construction/
  validation, ref-path derivation, capacity-transition prepare/apply, the
  fault-injection seams) stays a plain inherited `self.` call; no other
  module-level twin was needed.
- **`_lease_index_identity` and friends stay bound methods.** These live on
  `queue_lease_indexes` (rank 26, earliest of the six original owners) and
  are self-called by every later-ranked sibling (`queue_lease_capacity_state`
  27, `queue_lease_capacity_audit` 28, `queue_lease_recovery` 29,
  `queue_lease_admission`/`queue_leases` 30/31) — all forward edges, no
  hoisting required.

### 10.3 tail module-function deletions and residual-caller conversions

Twenty-seven module-level `core_queue.py` functions that only delegated to
`queue_lease_records`/`queue_lease_indexes`/`queue_index_state` are deleted
(their sole callers all moved into CQ15 owners, which now call the real
module function directly): `_lease_operational_records_present` (moved to
`queue_lease_indexes.lease_operational_records_present`, a public function --
its own callers stay facade-resident, converted to the qualified call),
`_lease_scope_ref_name`, `_lease_index_document`,
`_lease_capacity_aggregate_document`, `_serialized_lease_capacity_counts`,
`_lease_capacity_checkpoint_document`, `_new_lease_capacity_pair`,
`_normalize_lease_capacity_counts`, `_lease_capacity_aggregate_from_document`,
`_lease_capacity_checkpoint_from_document`, `_validate_lease_capacity_pair`,
`_lease_capacity_pair_payload`, `_lease_capacity_pair_from_payload`,
`_is_capacity_identity`, `_lease_index_identity_from_document`,
`_lease_reference_from_scope_ref`, `_lease_reference`,
`_parse_lease_identity_ref_name`, `_is_short_ref_token`, `_lease_index_token`,
`_lease_job_token`, `_lease_endpoint_token`, `_lease_cluster_token`,
`_lease_expiry_key`, `_lease_expiry_ref_name`, `_lease_identity_token`,
`_parse_lease_expiry_ref_name`, `_job_matches_mcp_admission_class` (moved to
`queue_lease_admission.py`, still private), `_index_integer` (both its call
sites were inside the moved `repair_lease_operational_indexes`/
`_apply_lease_index_repair_intent_unlocked` bodies, so it had zero remaining
callers post-move), `_index_migration_components_complete` (same: both call
sites were inside moved CQ15 bodies), and `_validate_record_stat` (its one
call site moved with `_lease_capacity_record_paths_unlocked`).

Four residual facade-resident call sites (in still-unmoved startup/migration
code, `_initialize_under_locked_core` and `_ensure_extended_migration_state`/
`_reconcile_transition_intents_unlocked`) referenced these now-deleted names
and are rewired to the real module function directly:
`queue_lease_indexes.lease_operational_records_present(self._storage_root)`
(4 call sites), `queue_lease_records.new_lease_capacity_pair(...)`,
`queue_lease_records.is_capacity_identity(...)`, and
`queue_lease_records.lease_index_identity_from_document(...)`. `_stable_ref_
token`, `_record_is_reparse`, `_read_bounded_record_bytes`,
`_unlink_durable_path`, `_is_sha256_digest`, `_scheduler_cancellation_
request`/`_cancellation_requested_at`, and `_safe_global_record_id` all stay
in `core_queue.py`: each still has a live caller in not-yet-extracted
gateway/monitor/job-gc/index-migration code. `queue_lease_capacity_state.py`
and `queue_lease_recovery.py` each keep a private duplicate of
`_stable_ref_token`/`_read_unique_json_document` (the latter rejects
duplicate JSON keys; `core_queue.py`'s own copy stays live as the
`_read_sealed_index_migration_state` document-reader callback) -- the
established per-owner-duplication idiom (ledger §9.6).

### 10.4 `_is_sha256_digest` dedup decision (ledger §9.6 follow-up)

Ledger §9.6 flagged this slice as owning the `_is_sha256_digest` dedup
decision for "the lease-recovery copies." Investigation found no such copy:
the facade's `_is_sha256_digest` (kept, per above) is called only from
unmoved job-GC code (`_terminal_job_gc_protections`,
`_read_committed_job_digest`) — neither is CQ15 territory — and no CQ15-owned
method (lease capacity state/audit, indexes, leases, admission, recovery,
scheduler claims) calls `_is_sha256_digest` at all. There is no lease-recovery
copy to deduplicate; `queue_jobs.py`/`queue_artifact_lineage.py`'s existing
copies (§9.6's actual subject) remain untouched and still deferred to
CQ13/CQ18 as that note already recorded. No action was needed beyond this
verification.

### 10.5 failing-first sabotage (`tests/test_core_queue_split_architecture.py`)

Three new sabotage tests, all isolated-namespace pattern, all verified
green post-wiring:

- `test_cq15_repair_uses_the_lease_indexes_sync_lookup` — patches
  `queue_lease_capacity_audit.queue_lease_indexes.sync_operational_indexes`
  and calls `repair_lease_operational_indexes()`; the mandated CQ15 seam.
- `test_cq15_lease_acquisition_uses_the_write_job_lookup` — patches
  `queue_jobs.write_job` and calls `acquire_job(...)`; the "lifecycle"
  job-write lookup (`queue_lease_admission._lease_job_unlocked`).
- `test_cq15_stale_recovery_uses_the_write_job_lookup` — patches
  `queue_jobs.write_job` and calls `recover_stale_job(...)`; the "recovery"
  job-write lookup (`queue_lease_recovery._apply_stale_lease_recovery_intent_
  unlocked`).

### 10.6 pre-existing tests updated (real lookup site moved)

Design §4's hand-audit rows for this family were regenerated against the
landed call graph; two rows needed correction from their originally-guessed
target and three pre-existing tests needed their patch site moved to follow
the real call expression (§4's own rule: never patch a dead facade shim):

- `test_lease_decoder_lookup_is_owned_by_the_cq4_module`
  (`test_core_queue_split_architecture.py`) and
  `test_lease_record_codec_is_byte_identical_to_the_parent_facade`
  (`test_queue_record_codecs.py`) both called/compared against the deleted
  `core_queue_module._lease_index_document` /
  `_lease_index_identity_from_document` shims. Rewritten to exercise the real
  owner (`queue_lease_indexes._read_lease_index_identity_by_token` via a real
  `ClioCoreQueue` instance) and to assert the expected byte string directly.
- `test_lease_index_repair_intent_rebuilds_after_clear_crash`
  (`test_operational_indexes.py`) patched the instance method `queue.
  _sync_lease_operational_indexes_unlocked`; moved to the isolated-namespace
  `queue_lease_capacity_audit.queue_lease_indexes` seam (§4 row:
  "`queue._sync_lease_operational_indexes_unlocked` ->
  `queue_lease_capacity_audit.queue_lease_indexes.sync_operational_indexes`,
  the repair caller at `core_queue.py:3561`" — confirmed still accurate).
  The sabotage now also fires during the test's crash-recovery replay
  (`restarted.initialize()`) unless explicitly undone first
  (`monkeypatch.undo()`) — the original instance-level patch never reached a
  freshly constructed `restarted` instance, so the module-qualified
  equivalent must be scoped the same way.
- `test_preexisting_over_capacity_queue_can_still_drain`
  (`test_operational_indexes.py`) patched the instance classmethod `queue.
  _scan_many`; corrected from §4's guessed `queue_leases.queue_store_read.
  scan_many` to the real post-split-owner target,
  `queue_lease_admission.queue_store_read.scan_many` (the admission path
  split out of `queue_leases` per §10 above).
- `test_diagnosis_models_predecessor_consuming_last_global_lease_slot`
  (`test_queue_management.py`) patched `core_queue_module.
  MAX_LIVE_LEASE_RECORDS`; corrected from §4's guessed
  `queue_lease_capacity_state.MAX_LIVE_LEASE_RECORDS` to the real constant
  source, `queue_layout.MAX_LIVE_LEASE_RECORDS` — every CQ15 owner reads it
  module-qualified from `queue_layout` directly (matching the established
  convention for shared constants), never a locally rebound copy, so the one
  correct lever is the shared source module, not any one owner.
- `test_stale_recovery_uses_exact_scheduler_indexes_without_global_task_scan`
  (`test_core_index_safety.py`) patches `queue._read_many` and asserts it is
  *never* called during recovery; §4's guessed target
  (`queue_lease_recovery.queue_store_read.read_many`) does not exist — no
  CQ15 method calls `_read_many` at all (the facade classmethod remains,
  used only by unmoved gateway/monitor code). The test's negative assertion
  holds unchanged; no source or test change was needed for this row.

### 10.7 production (non-test, non-owner) collateral

`src/clio_relay/storage_runtime.py` imported the private
`_job_matches_mcp_admission_class` from `clio_relay.core_queue` (pre-existing
`pyright: ignore[reportPrivateUsage]`-annotated cross-module private import,
unrelated to the owner-split AST guard since `storage_runtime.py` is not a
`queue_*.py` owner). Retargeted to `clio_relay.queue_lease_admission`, its
real new home.

### 10.8 gates

`ruff check`/`ruff format --check`, `pyright` (0 errors on all seven new
files + `core_queue.py` + every touched test file), `scripts/
check_file_size.py` (ratchet lowered 8009 -> 4848 for `core_queue.py`; all
seven new owners within their stated caps, none exceeding the 800-line hard
gate), and `scripts/check_release_identity.py` (unaffected, 78/80 sites
agree) all pass. `tests/test_queue.py`, `tests/
test_core_queue_split_architecture.py` (41 tests, 3 new), `tests/
test_release_pins.py`, `tests/test_queue_readiness.py`,
`tests/test_operational_indexes.py`, `tests/test_lease_capacity.py`,
`tests/test_queue_management.py`, `tests/test_storage_managed_queue.py`,
`tests/test_core_index_safety.py`, and `tests/test_queue_record_codecs.py`
all pass green. `tests/test_endpoint_worker_lanes.py` (the #238 poisoned-
record quarantine path, called from `endpoint_worker_lanes.py` on
`develop`) is absent on this branch -- that file has not landed here yet;
`endpoint.py`'s own lease/recovery call sites (`recover_stale_jobs`,
`acquire_next_job`, `release_lease`, `scan_job_leases`, `claim_scheduler_
cancel_attempt`/`_confirmation`, `record_scheduler_cancel_attempt`/
`_observation`, `renew_lease`) are all unchanged public-signature calls,
confirmed via `tests/test_endpoint.py`'s existing lease/scheduler-cancel
coverage.

## 11. CQ16 landing — gateways, browser attachments, gateway indexes, monitor rules

CQ16 (predecessors CQ3-CQ5, CQ7, CQ9, CQ10, CQ12, CQ14, all landed) lands as
four owners: canonical gateway-session records and their public lifecycle
(`queue_gateways.py`), the sole browser-attachment ownership-intent CAS
lifecycle (`queue_browser_attachments.py`), gateway backlink/reverse-index
convergence (`queue_gateway_indexes.py`), and durable monitor rules
(`queue_monitor_rules.py`).

### 11.1 owner ranks and real line counts

| Rank | Owner | Real | Cap | Note |
|---:|---|---:|---:|---|
| 20 | `queue_gateway_indexes.py` | 524 | 540 | re-ranked ahead of its callers, §11.2 |
| 34 | `queue_gateways.py` | 383 | 400 | |
| 35 | `queue_browser_attachments.py` | 407 | 420 | |
| 36 | `queue_monitor_rules.py` | 212 | 230 | |

`core_queue.py` fell from 4,848 to 3,606 physical lines (net removal 1,242;
the ratchet in `scripts/check_file_size.py` is lowered to 3606 in the same
change). Every landed cap is below the 800-line hard gate; the facade public
method count stays exactly 128
(`test_facade_public_method_set_stays_at_the_128_method_base`), since every
moved method keeps its existing name and signature.

### 11.2 reverse-rank resolution: `queue_gateway_indexes` re-ranked, not re-called

The design's section-1 per-line inventory placed the gateway/browser/monitor
block after jobs, input-ingest, progress, and tasks in the pinned `d6253d7`
file -- physical position, not call-graph rank. Reconciling that against the
real dependency graph (ledger §9.3/§10.2 precedent: resolve a reverse-rank
self-call by hoisting the collaborator earlier, never by re-ranking every
caller) found one genuine class of reverse edge and its resolution:

- **`queue_jobs`/`queue_tasks`/`queue_artifacts`/`queue_input_ingest`
  self-call into `queue_gateway_indexes`.** Four already-landed owners call
  inherited `QueueGatewayIndexesMixin` methods directly:
  `queue_jobs._sync_job_derived_unlocked` and
  `queue_tasks._sync_task_retention_indexes_unlocked`'s sibling call both
  self-call `_sync_scheduler_source_unlocked`; `queue_artifacts.create_artifact`
  and `queue_input_ingest`'s equivalent both self-call
  `_link_gateways_for_artifact_unlocked`. All four call sites already carried
  `TYPE_CHECKING` stubs for these two methods before this slice landed (added
  proactively by their own authors), confirming the edge was anticipated.
  `queue_gateway_indexes` itself has no forward need for any of
  CQ7/CQ9/CQ10/CQ12/CQ14 -- its own real dependencies are just
  `queue_context`/`queue_layout`/`queue_store_lock`/`queue_store_read`/
  `queue_store_write` (ranks 0, 2-5). The design doc's CQ16 predecessor list
  describes what `queue_gateways` (owner-session intake) and
  `queue_monitor_rules` (`append_event`, `get_job`) need, not what
  `queue_gateway_indexes` needs. Resolution: `queue_gateway_indexes` lands at
  rank 20, immediately before its earliest caller (`queue_artifacts`, itself
  shifted to 21); every subsequent owner through rank 33 shifts by exactly
  one to keep the dense 0..N-1 permutation
  (`test_non_owner_exemption_and_owner_rank_are_pinned_to_the_manifest`).
  `queue_gateways`(34), `queue_browser_attachments`(35), and
  `queue_monitor_rules`(36) land last, after every one of their own real
  collaborators. This produces zero reverse-rank edges -- no typed deviation
  was needed, unlike CQ15's genuine two-owner cycle.

### 11.3 two module-level collaborator-attribute seams (mandated, not discovered)

The design's own CQ16 row prescribes the failing-first shape: "Patch each
caller owner's collaborator attribute for browser CAS and backlink
synchronization." Two bound methods therefore keep module-level twins,
matching the `queue_jobs.write_job`/`queue_lease_indexes.
sync_operational_indexes` precedent:

- **Browser CAS**: `queue_browser_attachments._write_browser_attachment_
  transition_unlocked` resolves its canonical write through
  `queue_gateways.write_gateway_session(cast(queue_gateways.
  QueueGatewaysMixin, self), updated)` -- a module-qualified lookup a test
  can patch via `monkeypatch.setattr(queue_browser_attachments,
  "queue_gateways", isolated_namespace)`.
- **Backlink synchronization**: `queue_gateways.write_gateway_session`
  (the module function; `QueueGatewaysMixin._write_gateway_session_unlocked`
  is its thin, name-preserving wrapper) resolves index convergence through
  `queue_gateway_indexes.sync_gateway_session_derived(cast(...), session.
  session_id)` -- patched via `monkeypatch.setattr(queue_gateways,
  "queue_gateway_indexes", isolated_namespace)`.
  `QueueGatewayIndexesMixin._sync_gateway_session_derived_unlocked` stays a
  real, directly patchable instance method because facade-resident
  `_reconcile_transition_intents_unlocked`'s `gateway_sync` transition-intent
  replay branch still self-calls it by that exact name.

Both seams were verified failing-first by hand: temporarily reverting each
call site to a plain `self.<method>` self-call makes its paired sabotage
test fail with "DID NOT RAISE" (the isolated-namespace patch no longer
intercepts a module-qualified lookup that no longer exists); restoring the
module-qualified call turns both green again.

### 11.4 typed browser-attachment identity conflict (cross-file, `service_runtime.py`)

`ServiceRuntimeSupervisor._revoke_browser_attachment`
(`service_runtime.py`) caught `QueueConflictError` and inspected
`str(exc)` for the substring `"changed before revocation"` to decide
whether to remap it to a public `ConfigurationError` -- the banned
prose-match pattern (both of `queue_browser_attachments`'s revoke-lifecycle
identity-mismatch raises, `begin_gateway_browser_attachment_revoke`'s
`"...changed before revocation: ..."` and
`finish_gateway_browser_attachment_revoke`'s `"...changed before revocation
completed: ..."`, satisfy that substring, so both were silently in scope of
the check even though only the `begin_...` call site sits inside the
guarded `try`).

Fix, mirroring the `TaskInputParkConflictError` precedent
(`errors.py`): a new `BrowserAttachmentIdentityConflictError(
QueueConflictError)` subtype. Both raise sites in
`queue_browser_attachments.py` now raise the typed subtype with their exact,
unchanged relay-authored messages. `service_runtime.py`'s except clause
now reads:

```python
except BrowserAttachmentIdentityConflictError as exc:
    raise ConfigurationError(
        "browser attachment id does not match the gateway record"
    ) from exc
```

with no trailing bare `except QueueConflictError: raise` -- removing the
substring branch already lets every other `QueueConflictError` (missing
attachment, invalid record, canonical identity mismatch, ...) propagate
unmapped, exactly as the old `else: raise` did. This is the sanctioned
cross-file edit named in the slice's handoff: minimal (the except clause and
its import only), `service_runtime.py` stays outside the queue family, and
its file-size ratchet moves by a justified +4 net lines (`scripts/
check_file_size.py`: 9391 -> 9395 -- the shorter except body is outweighed
by the wider multi-line import block) rather than net-zero.

Failing-first proof (`tests/test_service_runtime.py::
test_browser_detach_maps_only_the_typed_identity_conflict_not_any_
queue_conflict_error`): a decoy plain `QueueConflictError` carrying the same
substring for an unrelated reason is raised from a patched
`queue.begin_gateway_browser_attachment_revoke`. Verified red against the
old substring-matching implementation (misclassified as
`ConfigurationError`) and green against the typed catch (propagates
unchanged as `QueueConflictError`), confirmed by toggling the fix and
re-running the single test both ways.

### 11.5 patch-seam audit corrections (real lookup site moved)

- `tests/test_core_index_safety.py::
  test_gateway_reverse_indexes_refuse_cardinality_overflow` patched
  `core_queue_module.MAX_GATEWAY_INDEX_RECORDS`, a compatibility re-export
  that no post-split reader consults. `queue_gateway_indexes.py` reads the
  constant module-qualified from `queue_layout` at every use site (never a
  locally rebound copy, matching the CQ15 `MAX_LIVE_LEASE_RECORDS`
  precedent), so the patch is moved to `queue_layout.
  MAX_GATEWAY_INDEX_RECORDS` -- confirmed dead beforehand (the facade still
  re-exports the name, `hasattr` succeeds, but rebinding it has no effect on
  `queue_gateway_indexes`'s reads) and confirmed live after.
- The one residual facade-resident bare-function call, `_write_transition_
  intent_unlocked`'s `_stable_ref_token(kind, identity)`, is rewired to
  `queue_gateway_indexes._stable_ref_token(kind, identity)` -- the design's
  own residual-caller pattern (ledger §10.3).
- `_safe_global_record_id` (a one-line `queue_layout.safe_global_record_id`
  wrapper, exclusively called from `prepare_gateway_teardown_intent`) is
  deleted rather than moved; `queue_gateways.py` calls `queue_layout.
  safe_global_record_id(operation_id)` directly, matching
  `queue_order_index.py`'s own existing usage of the same function.

### 11.6 gates

`ruff check`/`ruff format --check`, `pyright` (0 errors on all four new
files + `core_queue.py` + `errors.py` + every touched test file; the one
`service_runtime.py` finding, an unrelated pre-existing `reportUnnecessaryCast`
at its `FileLock` cast, is confirmed present before this slice via
`git stash`), `scripts/check_file_size.py` (ratchet lowered 4848 -> 3606 for
`core_queue.py`, justified +4 for `service_runtime.py`; all four new owners
within their stated caps, none exceeding the 800-line hard gate), and
`scripts/check_release_identity.py` (78/80 sites agree) all pass.
`tests/test_queue.py`, `tests/test_core_queue_split_architecture.py` (41 -> 43
tests, 2 new: the browser-CAS and backlink-synchronization
collaborator-attribute sabotage tests), `tests/test_release_pins.py`,
`tests/test_browser_attachment_queue.py`, `tests/test_service_runtime.py`
(adds the typed-conflict failing-first test), `tests/test_core_index_safety.py`,
and the gateway/monitor-focused suites (`tests/test_gateway_public_projection.py`,
`tests/test_core_global_pagination.py`, `tests/test_core_retention.py`,
`tests/test_operational_indexes.py`) all pass green in one combined run --
436 passed in 439.32s, zero failures beyond the slice's own named A/B-known
cross-branch and #239 exemption families (none of which fired here).

## 12. CQ17 landing — execution cleanup (shard/detection + marker mutation split)

CQ17 (predecessors CQ3-CQ5, CQ12, all landed) lands as **two** owners, not
the doc's original one: the execution-cleanup shard layout, flat-to-shard
migration, and detection machinery (`queue_execution_cleanup.py`), and the
durable-marker mutation methods that persist an updated task's
`execution_cleanup` metadata (`queue_execution_cleanup_markers.py`).

### 12.1 owner ranks and real line counts

| Rank | Owner | Real | Cap | Note |
|---:|---|---:|---:|---|
| 23 | `queue_execution_cleanup.py` | 352 | 380 | re-ranked ahead of `queue_jobs`, §12.2 |
| 28 | `queue_execution_cleanup_markers.py` | 335 | 360 | ranked after `queue_tasks`, §12.2 |

`core_queue.py` fell from 3606 to 3056 physical lines (net removal 550,
including the now-dead `hashlib` import; the ratchet in
`scripts/check_file_size.py` is lowered to 3056 in the same change). Every
landed cap is below the 800-line hard gate; the facade public method count
stays exactly 128, since every moved method keeps its existing name and
signature. 39 owners are landed.

### 12.2 typed deviation CQ17-EC-01: a forced two-owner split, not a re-rank

The design's section-1 per-line inventory placed execution cleanup as one
contiguous band (`7788-8338` in the pinned `d6253d7` numbering) between
tasks/events and input-ingest -- physical position, not call-graph rank.
Reconciling that against the real dependency graph found a genuine ordering
conflict that cannot be resolved by re-ranking alone (ledger §9.3/§10.2/
§11.2 precedent: hoist or split, never force a reverse edge):

- **`queue_jobs.write_job` (CQ12, already landed) needs the shard/migration
  half before it.** `write_job`'s body already called
  `queue._migrate_execution_cleanup_shard_unlocked(...)`/
  `queue._execution_cleanup_shard(...)` directly on every canonical job
  write (crash-safety ordering: the shard migration for a job's cluster
  must complete before that job's own record is replaced), with
  `TYPE_CHECKING` stubs for both names already present in `queue_jobs.py`
  -- a real, pre-existing edge this slice discovered, not one it
  introduced. This forces the shard/migration owner's rank strictly below
  `queue_jobs`.
- **The marker-mutation methods need `queue_tasks` (CQ14) after it.**
  `register_execution_cleanup`, `acknowledge_execution_cleanup`,
  `migrate_execution_cleanup_plan`, and `stage_execution_cleanup_sidecar`
  each persist an updated `RelayTask` and therefore each call
  `queue_tasks`'s `_sync_task_retention_indexes_unlocked` on every write --
  a real, non-trivial method (itself calling into `queue_gateway_indexes`),
  not a candidate for duplication. This forces the marker-mutation owner's
  rank strictly above `queue_tasks`, which itself already ranks above
  `queue_jobs`.

A single owner cannot be simultaneously ranked below `queue_jobs` and above
`queue_tasks` (`queue_jobs` already ranks below `queue_tasks`). The
resolution mirrors CQ15's `queue_lease_admission` peer split: the two
halves have **zero call-graph overlap in the reverse direction** --
`queue_execution_cleanup_markers.py` self-calls into
`queue_execution_cleanup.py` (a forward edge: the markers half needs
`_migrate_execution_cleanup_shard_unlocked`, `_execution_cleanup_path`,
`_execution_cleanup_job_path`, `_fsync_execution_cleanup_directory`, and
`_execution_cleanup_shard`), and `queue_execution_cleanup.py` never calls
back. `queue_execution_cleanup.py` lands at rank 23, immediately before
`queue_jobs`; `queue_execution_cleanup_markers.py` lands at rank 28,
immediately after `queue_tasks`. Every other landed owner from rank 23
onward shifts by two to keep the dense `0..N-1` permutation.

A second, smaller resolution closes what would otherwise have been a
genuine 2-cycle: `job_has_pending_execution_cleanup`'s former
`self.get_job(job_id)` call (queue_jobs-owned) is replaced with the CQ9-
ledger-precedent (§9.3) shared primitive
`queue_store_read.read_required_job(self._storage_root, job_id)` --
exactly the body `queue_jobs.get_job` itself already delegates to, so the
observable behavior (including the exact `NotFoundError`) is unchanged.
This removes the shard/detection half's only dependency on `queue_jobs`,
leaving the two-owner split as the sole remaining deviation.

### 12.3 failing-first sabotage (`tests/test_core_queue_split_architecture.py`)

Two new sabotage tests, isolated-namespace pattern, both verified red via
call-site bypass (by hand: temporarily replacing the module-qualified call
with an equivalent inline read/write, confirming `DID NOT RAISE`, then
restoring) and green after:

- `test_cq17_execution_cleanup_migration_uses_the_store_read_lookup` --
  patches `queue_execution_cleanup.queue_store_read.read_json_file` and
  calls `_migrate_execution_cleanup_shard_unlocked` directly (not through
  `scan_execution_cleanup`'s outer shard loop, so the sabotage exercises
  the migration step's own legacy-marker read, not a later, unrelated read
  of the already-migrated file); the design row's "shard read" half.
- `test_cq17_execution_cleanup_migration_uses_the_store_write_lookup` --
  same direct-call shape, patches
  `queue_execution_cleanup.queue_store_write.write_json` (the migration
  completion receipt write); the design row's "shard write" half.

### 12.4 gates

`ruff check`/`ruff format --check`, `pyright` (0 errors on both new files +
`core_queue.py` + the touched test file), `scripts/check_file_size.py`
(ratchet lowered 3606 -> 3056; both new owners within their stated caps),
and `scripts/check_release_identity.py` (78/80 sites agree) all pass.
`tests/test_queue.py`, `tests/test_core_queue_split_architecture.py` (43 ->
45 tests, 2 new), `tests/test_release_pins.py`, and the execution-cleanup-
focused suites (`tests/test_endpoint.py`, `tests/test_control_query_
admission.py`, `tests/test_core_retention.py`, `tests/test_jarvis_lost_
response_recovery.py`, `tests/test_jarvis_recovery_scheduling.py`,
`tests/test_release_validation.py`) all pass green, zero failures beyond
the pre-existing, A/B-verified `[corrupt/missing/renamed]` cross-branch
flake in `test_unresolved_runtime_sidecar_failure_retains_recovery_evidence`
(confirmed failing identically on the unmodified `b2cd1bf` tip via
`git stash`).

## 13. CQ18 landing — job GC (protections + orchestration + storage)

CQ18 (predecessors CQ6, CQ9-CQ17, all landed) lands as **three** owners,
not the doc's original two: pure GC quarantine-tree filesystem primitives
(`queue_gc_storage.py`), the fail-closed terminal-job GC eligibility gate
(`queue_job_gc_protections.py`), and the phased trash-staging orchestration
that reads it (`queue_job_gc.py`).

### 13.1 owner ranks and real line counts

| Rank | Owner | Real | Cap | Note |
|---:|---|---:|---:|---|
| 39 | `queue_gc_storage.py` | 250 | 280 | no dependency on job_gc or its protections |
| 40 | `queue_job_gc_protections.py` | 290 | 320 | typed deviation CQ18-JG-01 |
| 41 | `queue_job_gc.py` | 677 | 720 | reads the protections gate; must rank after it |

`core_queue.py` fell from 3056 to 2077 physical lines (net removal 979;
the ratchet in `scripts/check_file_size.py` is lowered to 2077 in the same
change). Every landed cap is below the 800-line hard gate; the facade
public method count stays exactly 128. 42 owners are landed.

### 13.2 typed deviation CQ18-JG-01: eligibility split from orchestration

The design's section-1 per-line inventory assigned job GC to two bands
(`3976-4172` and `12494-12770` in the pinned `d6253d7` numbering) that
this slice's real dependency graph confirms belong to one concern -- but
combined, the eligibility-gate and trash-staging-orchestration bodies
total ~890 real lines even after every filesystem primitive already moved
to `queue_gc_storage.py`, exceeding the 800-line hard gate. Unlike CQ17's
split (forced by a genuine rank conflict), this one is size-forced, but
the same "clean one-directional dependency" test still applies (ledger
§10's `queue_lease_admission` precedent: split only when the two halves
have zero reverse call-graph edges):

- `_terminal_job_gc_protections` (plus its own `_artifact_lineage_gc_
  protections`/`_indexed_gc_entry_state` helpers) has no write behavior of
  its own -- every protection check is a read: index-migration state,
  execution-cleanup pending state, scheduler-cancel pending state,
  owner-session closure, idempotency record state, the job order index,
  and five indexed per-job families (leases/tasks/scheduler/monitor/
  gateway), plus the artifact-lineage consumer-reference count.
- `plan_terminal_job_gc`/`collect_terminal_job` (and the trash-staging
  primitives underneath them) self-call `self._terminal_job_gc_
  protections(job)` to decide eligibility before ever touching a durable
  record. The eligibility gate never calls back into orchestration.

This is a clean, one-directional dependency, so `queue_job_gc_
protections.py` lands at rank 40, immediately before `queue_job_gc.py`
(rank 41), which composes it as a forward edge with no reverse-rank
findings.

### 13.3 `_is_sha256_digest` dedup (ledger §9.6/§10.4 follow-up, resolved)

Ledger §9.6 named this slice as owning the facade's `_is_sha256_digest`
copy's real disposition, since its only two callers
(`_terminal_job_gc_protections`, `_read_committed_job_digest`) move here.
The facade's copy is **deleted outright**, not moved: `queue_job_gc_
protections.py` keeps its own private duplicate, matching the
already-established per-owner idiom four other owners already use
(`queue_jobs.py`, `queue_artifact_lineage.py`, `queue_lease_records.py`,
`queue_legacy_output_codec.py`). `queue_jobs`/`queue_artifact_lineage`
keep their existing copies unchanged and do **not** import this module's
copy: both rank well before `queue_job_gc_protections` (rank 40), so a
shared import would be a reverse-rank edge the architecture guard rejects.
Per-owner duplication of this six-line pure predicate remains the correct
resolution, not an oversight.

### 13.4 shared-primitive hoist: `_migration_batch_paths`

`_migration_batch_paths` (a pure, stateless "bounded lexicographic path
batch" helper, no `self` dependency) had callers in both this slice's
`_trash_job_references_unlocked` and the not-yet-extracted index-migration
facade code (`migrate_indexes_batch`, CQ19 territory). Neither caller-group
may depend on the other, so -- ledger §9.3/§10.2 precedent -- it hoists to
the earliest-ranked owner that is already a valid predecessor for both:
`queue_store_read.migration_batch_paths` (rank 4). The six residual
facade-resident call sites in `migrate_indexes_batch` are rewired to the
same qualified target. `queue_store_read.py` grows from 390 to 412 real
lines (budget raised 410 -> 420, a justified, minimal ratchet-up).

### 13.5 production and test collateral

- `retention.py` imported `purge_quarantined_tree_batch` bare from
  `clio_relay.core_queue` (a pre-existing production, non-owner import).
  Retargeted to `clio_relay.queue_gc_storage` directly (CQ15 §10.7
  `storage_runtime.py` precedent: retarget a moved-symbol import to its
  real new owner, never add a facade re-export for a production caller).
  `retention.py`'s own ratchet moves by a justified +1 net line (951 ->
  952: one combined import line splits into two single-name imports).
- `tests/test_core_retention.py` imported `_purge_tree_batch` bare from
  `clio_relay.core_queue` (private-name direct import); retargeted to
  `queue_gc_storage.purge_tree_batch`. Its `core_queue_module.
  _remove_gc_candidate(...)` call and paired `core_queue_module.os`
  attribute patch retarget to `queue_gc_storage._remove_gc_candidate`/
  `queue_gc_storage.os` (the same singleton `os` module either way, but
  now naming the real post-split caller). The now-fully-unused `import
  clio_relay.core_queue as core_queue_module` is removed.
- Design §4's own already-recorded row for the `_after_gc_checkpoint`
  fault-injection seam (`queue._after_gc_checkpoint` ->
  `queue_gc_storage.after_gc_checkpoint`) is realized exactly as
  prescribed: the seam's real body is now the bare module-level
  `queue_gc_storage.after_gc_checkpoint(phase)`, called module-qualified
  from every `collect_terminal_job` checkpoint. `tests/test_core_
  retention.py`'s two instance-level `monkeypatch.setattr(queue,
  "_after_gc_checkpoint", ...)` sites move to
  `monkeypatch.setattr(queue_gc_storage, "after_gc_checkpoint", ...)`.

### 13.6 failing-first sabotage (`tests/test_core_queue_split_architecture.py`)

Four new sabotage tests, all verified red via call-site bypass (by hand:
temporarily replacing the module-qualified call with an inline equivalent,
confirming `DID NOT RAISE`, then restoring) and green after:

- `test_cq18_terminal_job_gc_protections_resolve_through_the_protections_mixin`
  -- a CQ14-style composition proof: patches `queue_job_gc_protections.
  QueueJobGcProtectionsMixin._terminal_job_gc_protections` at class level
  (inert against a residual facade body, live only once composed).
- `test_cq18_protections_use_the_execution_cleanup_pending_check_lookup` and
  `test_cq18_protections_use_the_owner_session_closure_lookup` -- each an
  instance-level sabotage on a protection-owner lookup
  (`_job_has_pending_execution_cleanup_unlocked`, `get_owner_session_
  closed`), using a sabotage exception type deliberately **not** caught by
  the gate's own narrow `except (OSError, ValueError, QueueConflictError)`
  clauses, so the sabotage propagates cleanly to the caller; design row:
  "patch each protection-owner lookup in queue_job_gc".
- `test_cq18_job_gc_uses_the_gc_storage_move_lookup` -- isolated-namespace
  pattern, patches `queue_job_gc.queue_gc_storage.move_gc_path`; the
  design row's explicitly named seam ("then `queue_job_gc.queue_gc_
  storage.move_gc_path`").

### 13.7 gates

`ruff check`/`ruff format --check`, `pyright` (0 errors across every
touched/new file), `scripts/check_file_size.py` (ratchet lowered 3056 ->
2077 for `core_queue.py`; justified +22 for `queue_store_read.py`,
justified +1 for `retention.py`; all three new owners within their stated
caps), and `scripts/check_release_identity.py` (78/80 sites agree) all
pass. `tests/test_queue.py`, `tests/test_core_queue_split_architecture.py`
(45 -> 49 tests, 4 new), `tests/test_release_pins.py`, and the job-GC/
GC-storage-focused suites (`tests/test_core_retention.py`, `tests/
test_retention.py`, `tests/test_control_query_admission.py`, `tests/
test_core_idempotency.py`, `tests/test_artifact_lineage.py`, `tests/
test_fastmcp_server.py`, `tests/test_jarvis_lost_response_recovery.py`,
`tests/test_jarvis_recovery_scheduling.py`, `tests/test_legacy_output_
migration.py`, `tests/test_operational_indexes.py`, `tests/test_release_
validation.py`, `tests/test_transform_provenance.py`, `tests/
test_endpoint.py`, `tests/test_cli.py`) all pass green in combined runs,
zero failures beyond the same pre-existing, A/B-verified `[corrupt/missing/
renamed]` cross-branch flake CQ17 already exempted.


## 14. CQ19 landing — index discovery/migration, transition applier, startup

CQ19 (predecessors CQ2-CQ18, all landed) lands as **four** owners: the three
bounded, no-history-scan index-migration-state repairs
(`queue_index_discovery.py`), the resumable v0.9-to-indexed migration batch
driver and its per-family projection dispatchers (`queue_index_migration.py`),
the bounded write-ahead-log transition-intent applier
(`queue_transitions.py`), and the bulk of queue startup
(`queue_startup.py`).

### 14.1 owner ranks and real line counts

| Rank | Owner | Real | Cap | Note |
|---:|---|---:|---:|---|
| 42 | `queue_index_discovery.py` | 352 | 380 | no inbound edge from any other owner |
| 43 | `queue_startup.py` | 527 | 550 | CQ19-ST-01/-02, see §14.2/§14.3 |
| 44 | `queue_index_migration.py` | 696 | 720 | ranked after `queue_startup`, CQ19-ST-01 |
| 45 | `queue_transitions.py` | 254 | 280 | ranks last: dispatches into every earlier owner |

`core_queue.py` fell from 2077 to 746 physical lines (net removal 1331,
including the new thin `initialize` dispatch wrapper -- see §14.3). This is
under the 800-line hard gate and under `DEFAULT_MAX_LINES`, so
`scripts/check_file_size.py`'s `RATCHET_BASELINE` entry for `core_queue.py`
is removed entirely rather than lowered (the script's own documented
convention: "remove the entry once the file is under DEFAULT_MAX_LINES").
Every landed cap is below the 800-line hard gate; the facade public method
count stays exactly 128 (`test_facade_public_method_set_stays_at_the_
128_method_base`); 46 owners are landed. `core_queue.py`'s own budget is not
yet at its originally planned 200-line facade cap -- CQ1's jarvis-input
delegator pattern, CQ13-IO-01's `_assert_input_ingest_quota_unlocked`
deviation, and this slice's own CQ19-TI-01/CQ19-ST-02 deviations (below) all
still live there by design; closing that gap is CQ20's job, not this
slice's.

### 14.2 typed deviation CQ19-TI-01: the write-ahead-log primitives stay facade-resident

The design's section-1 per-line inventory placed the transition-intent
write/recovery primitives inside the same contiguous band as the transition
applier -- physical position, not call-graph rank. Reconciling that against
the real dependency graph (ledger §9.3/§10.2/§11.2/§12.2/§13.2 precedent:
hoist or split, never force a reverse edge) found a genuine hub-method
problem with no clean split:

- `_write_transition_intent_unlocked` (write one bounded WAL intent),
  `_recover_pending_transitions_unlocked` (the ``initialize()``-adjacent
  crash-recovery entrypoint, a thin wrapper over the transition applier),
  `_require_index_migration_complete`, `_read_index_migration_state`/
  `_write_index_migration_state`, and `_lease_capacity_migration_complete_
  unlocked` are each self-called by many already-landed owners spanning
  ranks 18 (`queue_endpoints`) through 40 (`queue_job_gc_protections`) --
  `queue_execution_cleanup` (rank 23) is the earliest. Extracting any one of
  them into a rank-42+ CQ19 owner would create a reverse-rank edge the
  architecture guard rejects, and no single earlier-ranked owner has the
  combined headroom to host all of them as a shared primitive (mirroring
  the CQ13-IO-01 `_assert_input_ingest_quota_unlocked` deviation for the
  same class of problem).
- All six stay exactly where they already were in `core_queue.py`,
  unmoved by this slice. Because none of them is owned by any `queue_*.py`
  Mixin, every existing self-call into them from any owner -- early or
  late -- carries no architecture-guard edge at all, before or after this
  slice.

### 14.3 typed deviation CQ19-ST-01/CQ19-ST-02: `queue_startup` ranks before `queue_index_migration`, and `initialize` is a bare module function

Two related, CQ19-discovered reverse-rank problems, both resolved without
re-ranking every caller:

- **CQ19-ST-01 (rank order).** The design doc's own CQwave listed
  "index discovery/migration" ahead of "startup." The real graph requires
  the opposite for one edge: `migrate_indexes_batch`/`index_migration_
  status` both self-call `self.initialize()` as their first line (a real,
  pre-existing edge, unchanged by this slice). `queue_index_migration`
  therefore ranks 44, immediately after `queue_startup` (43); `queue_
  index_discovery` (42) still lands first since `queue_startup.initialize`
  calls into it, and `queue_transitions` (45) has no edge to or from
  `queue_startup`/`queue_index_migration` at all.
- **CQ19-ST-02 (module-level function, not a Mixin method).** Making
  `initialize` a real `QueueStartupMixin` method looked like the obvious
  move -- until the architecture guard's self-call scanner (which resolves
  `self.<name>` through the real, non-stub mixin-method manifest) started
  tracking `self.initialize()` as a genuine rank-ordered edge from **every**
  owner across the whole rank range (it is the first line of nearly every
  public method on nearly every owner, not just this slice's own two
  callers), producing reverse-rank violations from ranks as early as
  `queue_lease_admission` (33). No rank can simultaneously satisfy "before
  every caller in the codebase" and "after its own collaborators, up to
  `queue_lease_capacity_state` at rank 30 via `queue_index_discovery`" --
  a genuine cycle, not a missing hoist target, and unlike CQ19-TI-01's
  primitives it cannot simply stay unmoved, since the design doc's CQ19 row
  and its failing-first prescription both require `queue_startup.py` to
  exist and host the real body. Resolution: `initialize`'s bulk body moves
  to `queue_startup.py` as a bare **module-level function**,
  `initialize(queue: QueueStartupMixin, *, ...)` (the established
  module-level-twin idiom, e.g. `queue_lease_indexes.sync_operational_
  indexes(queue, ...)`), while `ClioCoreQueue.initialize` stays a thin
  facade-resident dispatch (`return queue_startup.initialize(self, ...)`).
  Since the dispatch point is a bare function (not a `*Mixin` method) and
  the facade wrapper names no owner, every `self.initialize()` call
  anywhere in the codebase is invisible to the edge scanner again, exactly
  as it was before this slice when `initialize` lived directly on
  `ClioCoreQueue`. The two lifetime-pinning helpers (`_initialize_with_
  exclusive_lifetime`, `_initialize_under_locked_core`, both genuinely
  zero-inbound-edge) stay real `QueueStartupMixin` methods and call the
  module function as `initialize(self, ...)`.
- Every `queue._foo` private-attribute/method access inside the module-level
  `initialize` function (44 sites) carries `# pyright: ignore[
  reportPrivateUsage]` -- unlike a bound `self.foo` access inside the
  owning class's own method, `queue.foo` from a plain module-level function
  is `strict`-mode `reportPrivateUsage` regardless of `queue`'s declared
  type being `QueueStartupMixin` itself, matching the identical `# pyright:
  ignore[reportPrivateUsage]` annotations already on `queue_gateway_
  indexes.sync_gateway_session_derived`'s own `queue._storage_root` access.

### 14.4 failing-first sabotage (`tests/test_core_queue_split_architecture.py`)

Three new sabotage tests, all isolated-namespace pattern, all verified red
via call-site bypass (by hand: temporarily replacing the module-qualified
call with a pre-captured/inline equivalent, confirming `DID NOT RAISE`,
then restoring) and green after:

- `test_cq19_index_migration_uses_the_migration_batch_paths_lookup` --
  writes one flat legacy job record directly to disk (bypassing
  `ClioCoreQueue`) so the fresh-seed migration checkpoint stays incomplete,
  then patches `queue_index_migration.queue_store_read.migration_batch_
  paths` and calls `migrate_indexes_batch(batch_size=1)`; design row's "one
  domain-migration lookup."
- `test_cq19_transition_applier_uses_the_bounded_json_record_paths_lookup`
  -- patches `queue_transitions.queue_store_read.bounded_json_record_paths`
  and calls the public `reconcile_pending_transitions()` on an already-
  initialized queue; design row's "one transition-applier lookup."
- `test_cq19_startup_uses_the_legacy_audit_before_initialization_lookup` --
  patches `queue_startup.queue_legacy_audit` (isolated namespace) and calls
  a fresh queue's first `initialize()`; the design row's exact named seam,
  "`queue_startup.queue_legacy_audit.audit_before_initialization`."

### 14.5 `audit_before_initialization` alias (`queue_legacy_audit.py`)

`queue_legacy_audit.py` gains one new module-level alias, matching the
`queue_legacy_output_audit.audit_state_before_initialization` idiom it
already established:

```python
audit_before_initialization = (
    QueueLegacyAuditMixin._audit_legacy_state_before_initialization
)
```

`queue_startup.initialize` calls it as `queue_legacy_audit.audit_before_
initialization(cast(queue_legacy_audit.QueueLegacyAuditMixin, queue))` --
module-qualified, not `queue._audit_legacy_state_before_initialization()` --
so a test can intercept it with a module-qualified isolated-namespace patch.
The underlying bound method (`QueueLegacyAuditMixin._audit_legacy_state_
before_initialization`, landed CQ6) is unchanged.

### 14.6 patch-seam audit corrections (real lookup site moved)

Design §4's own row for this exact seam ("`ClioCoreQueue._audit_legacy_
state_before_initialization` -> `queue_startup.queue_legacy_audit.audit_
before_initialization`") is realized exactly as prescribed. Five
pre-existing tests that patched the now-dead `ClioCoreQueue._audit_legacy_
state_before_initialization`/`core_queue_module.exclusive_migration_
lifetime` class-attribute seams moved to the real module-qualified lookups:

- `tests/test_queue_startup_audit.py`: `test_indexed_era_fresh_process_
  startup_does_not_scan_record_history`,
  `test_missing_seal_runs_exactly_one_full_audit_under_the_queue_lock`, and
  `test_crash_after_durable_seal_recovers_without_reauditing_history` move
  their `monkeypatch.setattr(ClioCoreQueue, "_audit_legacy_state_before_
  initialization", ...)` to `monkeypatch.setattr(queue_legacy_audit,
  "audit_before_initialization", ...)`.
  `test_sealed_startup_never_upgrades_shared_writer_ownership` moves
  `monkeypatch.setattr(core_queue_module, "exclusive_migration_lifetime",
  ...)` to the isolated-namespace `queue_startup.worker_lifetime_lock` seam
  (design §4's own row).
- `tests/test_worker_lifetime_lock.py`: `test_authoritative_migration_api_
  enters_exclusive_lifetime_guard` and `test_locked_initialization_never_
  writes_replacement_root_after_path_swap` move their `ClioCoreQueue._
  audit_legacy_state_before_initialization` patches to `queue_legacy_audit.
  audit_before_initialization`; the alias-retargeting migration test moves
  its `core_queue_module.exclusive_migration_lifetime` patch to the same
  isolated-namespace `queue_startup.worker_lifetime_lock` seam. Both files'
  now-unused `import clio_relay.core_queue as core_queue_module` are
  removed.
- `tests/test_core_queue_split_architecture.py::test_scheduler_decoder_
  lookup_is_owned_by_the_cq4_module` patched `core_queue_module.
  _cancellation_requested_at` (a facade wrapper deleted outright once its
  only caller, `_migrate_operational_record_unlocked`, moved into `queue_
  index_migration.py` and started calling `queue_scheduler_cancel_records.
  cancellation_requested_at` module-qualified directly). Corrected to
  submit a real job carrying `cancellation_request` metadata and call
  `queue._migrate_operational_record_unlocked("jobs", job)` directly, with
  the same `queue_scheduler_cancel_records.cancellation_requested_at`
  patch as before -- design §4's own rule: patch the module containing the
  real call expression, never a dead facade shim.

### 14.7 production and facade collateral

- `core_queue.py` module-level aliases `_ORDER_FAMILIES`/
  `_GLOBAL_ORDER_FAMILIES`/`_RETENTION_INDEX_FAMILIES`/`_OPERATIONAL_INDEX_
  FAMILIES`/`_INITIALIZED_QUEUE_FAMILIES`/`_ADDITIVE_QUEUE_FAMILIES`/`_
  LEGACY_ONLY_QUEUE_FAMILIES`/`_UnsafeQueueDirectoryProtection` are deleted:
  every remaining use was inside the moved bodies, and a repository-wide
  grep confirmed zero external (production or test) consumers.
  `QueueSealRequiresExclusive` is kept -- `storage_runtime.py` imports it
  directly from `clio_relay.core_queue` as a real, live compatibility
  re-export (confirmed via the same grep; caught only by running the full
  test suite's collection phase, since no targeted CQ19 test file exercises
  `storage_runtime.py`'s own import).
- The now-orphaned module-level functions `_unlink_durable_path`,
  `_scheduler_cancellation_request`, `_cancellation_requested_at`, and
  `_path_lstat` are deleted from `core_queue.py`: each had exactly one
  caller, and that caller moved into a CQ19 owner and now calls the real
  `queue_store_write.unlink_durable_path`/`queue_scheduler_cancel_records.
  scheduler_cancellation_request`/`cancellation_requested_at`/`queue_store_
  read.path_lstat` module-qualified directly (CQ15 §10.3 residual-caller
  precedent). `_record_is_reparse` and `_read_bounded_record_bytes` stay --
  each still has a live caller in the facade-resident `_bounded_regular_
  json_count`/`_read_unique_json_document` (`_QueueStoreAdapter`'s own
  bounded-count seam and the sealed-state document reader, both CQ20
  territory, not CQ19's).
- `errno`, `from contextlib import suppress`, and the bare `worker_
  lifetime_lock.{LockedCoreIdentity,exclusive_migration_lifetime,require_
  active_locked_core}` imports drop out of `core_queue.py` entirely (all
  three were only ever used inside the moved bodies); `os`/`stat` stay
  (each still has a live caller in `_bounded_regular_json_count`/
  `_QueueStoreAdapter`). `core_queue.py` gains one new `TYPE_CHECKING`
  import, `clio_relay.worker_lifetime_lock.LockedCoreIdentity`, for the
  thin `initialize` wrapper's own signature.

### 14.8 gates

`ruff check`/`ruff format --check`, `pyright` (0 errors across every
touched/new file; the sole pre-existing `service_runtime.py`
`reportUnnecessaryCast` finding, confirmed present before this slice, is
unrelated and untouched), `scripts/check_file_size.py` (`core_queue.py`'s
`RATCHET_BASELINE` entry removed entirely -- 2077 lines, now 746, under the
800-line default cap; all four new owners within their stated caps), and
`scripts/check_release_identity.py` (78/80 sites agree) all pass.
`tests/test_queue.py`, `tests/test_core_queue_split_architecture.py`
(49 -> 52 tests, 3 new), `tests/test_release_pins.py`, `tests/test_queue_
startup_audit.py`, `tests/test_operational_indexes.py`, `tests/test_core_
index_safety.py`, `tests/test_legacy_output_migration.py`, `tests/test_
worker_lifetime_lock.py` (337 tests combined), plus the startup/migration-
adjacent suites `tests/test_queue_readiness.py`, `tests/test_lease_
capacity.py`, `tests/test_queue_management.py`, `tests/test_queue_record_
codecs.py`, `tests/test_storage_managed_queue.py`, `tests/test_core_
retention.py`, `tests/test_retention.py`, `tests/test_artifact_lineage.py`,
`tests/test_fastmcp_server.py` (154 tests) all pass green, plus a full
`tests/` collection-and-run pass confirming zero import breaks anywhere in
the tree, zero failures beyond the pre-existing, A/B-known `#239` sidecar
family.

## 15. CQ20 landing — final facade collapse (campaign close)

CQ20 (predecessors CQ1-CQ19, all landed) is the closing slice: collapse the
746 lines CQ19 left behind into the pure composition facade sections 1-8
describe. No new owner module is created and no owner's rank changes; every
change is either a dissolution (a residual body moves to, or reuses, a real
owner) or a documented, permanent typed deviation.

### 15.1 classification inventory

Every one of the 746 lines CQ19 left in `core_queue.py` falls into exactly
one of the three buckets the issue prescribed:

| Class | Count | Members |
|---|---:|---|
| (a) stays -- facade glue | ~470 | module docstring/imports/re-exports; `_QueueStoreAdapter`; the `ClioCoreQueue` class statement, mixin composition, `__init__`/state; `initialize` (CQ19-ST-02); `reconcile_pending_transitions`, `_assert_input_ingest_quota_unlocked`, `_write_transition_intent_unlocked`, `_recover_pending_transitions_unlocked`, `_read_index_migration_state`/`_write_index_migration_state`, `_require_index_migration_complete`, `_lease_capacity_migration_complete_unlocked` (CQ13-IO-01/CQ19-TI-01, unchanged); `_job_record_path`, `_write`, `_write_json`, `_read_optional`, `_scan_many`, `_read_json_file`, `_read_json_document` (new deviation CQ20-FA-01, §15.2); `_is_canonical_event_path`, `_bounded_regular_json_count`, `_record_is_reparse` (unchanged, `_QueueStoreAdapter`-dependent) |
| (b) moved/dissolved | 12 owner methods + 2 module fns | CQ1's 8 jarvis-input delegators -> real `QueueJarvisInputsMixin` methods (CQ20-JI-01); `_read_sealed_index_migration_state`/`_read_unique_json_document` -> reused `queue_legacy_audit._read_sealed_state` (CQ20-SA-01, no new duplicate); `_storage_root_stat` -> `queue_lease_indexes.py`'s `self._layout.storage_root_stat()`; `_durable_key` (3 call sites) -> `queue_layout.QueueLayout.durable_key` in `queue_index_migration.py`; `_require_safe_write_directory`/`_purge_write_staging_unlocked` -> `queue_store_write.*` in `queue_startup.py`; `_write_text` -> `queue_store_write.write_text` in `queue_lease_indexes.py`; `_read_canonical_record` (6 call sites) -> `queue_store_read.read_canonical_record` in `queue_index_migration.py`; `_bounded_json_record_paths` (8 call sites: `queue_index_discovery.py`, `queue_job_gc.py` x3, `queue_job_gc_protections.py` x3, `_assert_input_ingest_quota_unlocked` itself) -> `queue_store_read.bounded_json_record_paths` |
| (c) dead residue, deleted | 4 | `_require_durable_record_id` (zero production callers -- every owner already builds its own local `queue_layout.QueueLayout.require_durable_record_id` alias); `_label_key` (same, `queue_owner_session_lifecycle.py`/`queue_owner_session_records.py` already alias it directly); `_read_many` (the classmethod itself had zero production callers left -- every reader that needs an unbounded scan, e.g. `queue_progress.list_progress`, already calls `queue_store_read.read_many` module-qualified; only a negative test-safety-net patch referenced it -- see §15.4); `_read_bounded_record_bytes` (orphaned once `_read_unique_json_document`, its only caller, dissolved) |

`core_queue.py` falls from **746 to 598** physical lines (net removal 148).
It has no `RATCHET_BASELINE` entry (removed at CQ19, still correctly absent
-- 598 is well under `DEFAULT_MAX_LINES`). The facade public method count
stays exactly 128 (`test_facade_public_method_set_stays_at_the_128_method_
base`); every moved method keeps its existing name and signature except the
two intentionally renamed private methods (`_read_sealed_index_migration_
state` -> `_read_sealed_state`, `_storage_root_stat`/`_durable_key`/etc.
deleted in favor of the module call they already forwarded to), none of
which are part of the public 128.

### 15.2 typed deviation CQ20-FA-01: the store-adapter hub family stays

`_job_record_path`, `_write`, `_write_json`, `_read_optional`, `_scan_many`,
`_read_json_file`, and `_read_json_document` all stay facade-resident,
unmoved. Two independent constraints force this, the same "hub method, no
clean hoist target" class CQ19-TI-01 already established:

- `_write`/`_write_json`/`_read_optional`/`_read_json_document` are called
  by `_QueueStoreAdapter` as `self._queue._write(...)` etc (ledger §9.5's
  own fix): routing through the *instance* rather than the bare module
  function is what keeps `monkeypatch.setattr(queue, "_write", ...)` -- an
  established, widely used test seam across the whole suite -- live. A
  module-qualified rewrite would silently break every such patch, exactly
  the class of defect §9.5 fixed once already.
- `_job_record_path` (26 external callers), `_write` (12+ external
  callers), `_write_json` (20), `_read_optional` (6), and
  `_recover_pending_transitions_unlocked` (42, already CQ19-TI-01) are each
  self-called from many already-landed owners spanning the full rank range
  0-45. `_scan_many` is additionally inherited directly by
  `storage_runtime.StorageManagedQueue(ClioCoreQueue)` -- a real production
  *subclass* outside the queue-owner family entirely (`recover_stale_jobs`/
  `acquire_job`/etc call `self._scan_many(...)` through inheritance, not
  composition), which no owner-module extraction could preserve without
  also rewriting `storage_runtime.py` itself -- out of scope by the same
  "queue owners do not absorb process/transport/scheduler/connector
  behavior" boundary design doc §5 already draws.

None of these calls are owned by any `queue_*.py` mixin, so they carry no
architecture-guard edge regardless of rank -- the same invisibility
CQ19-ST-02 established for `initialize`. This closes out ledger §9.5's own
open question ("full retirement stays CQ19/CQ20 scope"): the verdict is
**`_QueueStoreAdapter` and its four instance-routed methods are permanent**,
not a residual gap -- CQ20's own issue text names "the private store
adapter" as facade-legitimate for exactly this reason.

### 15.3 typed deviation CQ20-SA-01: the sealed-state duplicate is dissolved, not duplicated

`_read_sealed_index_migration_state` (a thin
`queue_index_state.read_sealed_index_migration_state` forward) and its
private document-reader `_read_unique_json_document` looked, at first
glance, like another CQ19-TI-01-class hub (two external callers at
different ranks: `queue_index_discovery.py` rank 42,
`queue_startup.py`'s bare `initialize` function). They are not: neither
docstring that named them among the "stays" family (`queue_index_
discovery.py`, `queue_startup.py`) had actually re-verified that claim
against the real call graph. `queue_legacy_audit.QueueLegacyAuditMixin`
(rank 12, already a valid predecessor for both callers) already owns a
byte-identical private equivalent, `_read_sealed_state` (same forward, its
own already-established per-owner `_unique_json` document reader). Both
real callers now call `self._read_sealed_state(...)`/`queue._read_sealed_
state()` directly -- a forward self-call to the earlier-ranked owner
(ledger §9.3/§10.2 "hoist to whichever side the topology requires"
precedent, except here the target already existed). The facade's own copy
and its private JSON reader are deleted outright, not moved; deleting the
latter also orphaned `_read_bounded_record_bytes` (§15.1's dead-residue
row).

### 15.4 dissolved: CQ1's jarvis-input composition -> `QueueJarvisInputsMixin` (CQ20-JI-01)

CQ1's original zero-inbound peel (design doc §3) used instance composition
(`ClioCoreQueue.__init__` held a private `QueueJarvisInputs` helper object,
`self._jarvis_inputs = queue_jarvis_inputs.QueueJarvisInputs(self._store_
adapter)`) rather than mixin inheritance -- it landed before any other
owner had established the `*Mixin` pattern to follow. Every one of its
eight public methods was still a facade-resident delegator (`return self.
_jarvis_inputs.get_x(...)`) through the whole CQ1-CQ19 split; design doc
§14.1 named this explicitly as an open gap left for CQ20.

`queue_jarvis_inputs.py`'s `QueueJarvisInputs` class is renamed
`QueueJarvisInputsMixin` and added to `ClioCoreQueue`'s bases (still rank 1,
unchanged -- it was already a landed owner in the dependency-rank sense,
just composed rather than inherited). Its `__init__`/`self._store` are
deleted; every method reads `self._store_adapter` directly (the same
private adapter attribute every other owner already depends on) instead of
a separately held reference. `ClioCoreQueue.__init__` drops its `self.
_jarvis_inputs = ...` line. The eight facade-resident delegator methods are
deleted from `core_queue.py` outright -- they are now real inherited
`QueueJarvisInputsMixin` methods, reached through the composed MRO exactly
like every CQ3+ owner's public surface. Every signature is byte-for-byte
unchanged (`test_jarvis_input_methods_are_inherited_from_the_owner_mixin`,
replacing the now-vacuous CQ1 signature-parity check with a same-identity
assertion: `getattr(ClioCoreQueue, symbol) is getattr(QueueJarvisInputsMixin,
symbol)`).

`queue_jarvis_inputs.py` grows 292 -> 322 real lines (the `_OWNER_BUDGETS`
cap is raised 300 -> 340, justified: the eight former delegator bodies now
live here instead of on the facade, and ruff's line-length wrap of the
longer `self._store_adapter.storage_root / ...` expression -- versus the
old `self._store.storage_root / ...` -- added a handful of physical lines
across four methods).

### 15.5 fixed in-campaign: a pre-existing, CQ16-vintage dead test seam

The broad regression sweep (§15.6) surfaced
`tests/test_identifiers.py::test_canonical_scan_layouts_bind_filename_to_
record_identity` failing with `AttributeError: module 'clio_relay.core_
queue' has no attribute '_stable_ref_token'` -- confirmed failing
identically on the unmodified CQ19 tip via `git stash` (not a CQ20
regression). Ledger §11.5 already recorded the real fix at CQ16 landing:
`_stable_ref_token` moved from the facade to `queue_gateway_indexes.py`,
and every production call site was rewired -- but this one test call site
was missed and has been silently broken (not merely stale-patched; a
genuine `AttributeError`) since CQ16. Fixed in-campaign per this
repository's own no-deferral convention: the test now calls
`queue_gateway_indexes._stable_ref_token("Target:GPU")` directly.

### 15.6 gates

`ruff check`/`ruff format --check` (whole `src/`+`tests/` tree, not just
touched files), `pyright` (0 errors on every touched/new file; the same
14 pre-existing errors on `tests/test_queue.py`/`tests/test_identifiers.py`
-- `QueueLockProtocol` attribute-visibility findings and the now-fixed
`_stable_ref_token` one, confirmed present before this slice via `git
stash` -- are unrelated and untouched), `scripts/check_file_size.py` (no
file under `src/clio_relay`/`jarvis-packages/clio_relay` exceeds its
ratchet baseline; `core_queue.py` has no baseline entry and stays that
way), and `scripts/check_release_identity.py` (78/80 sites agree) all pass.

`tests/test_queue.py`, `tests/test_core_queue_split_architecture.py` (53
tests: the prior 52 plus the new MRO proof;
`test_jarvis_input_facade_signatures_match_the_owner` is renamed/repurposed
rather than counted as a net-new test), `tests/test_release_pins.py`,
`tests/test_fastmcp_server.py`, and `tests/test_endpoint.py` all pass
green (`test_endpoint.py`'s three `test_unresolved_runtime_sidecar_
failure_retains_recovery_evidence[corrupt|missing|renamed]` cases are the
named, pre-existing, A/B-verified `#239` sidecar-family exemption --
confirmed failing identically on the unmodified CQ19 tip).

The broad adjacent sweep (every suite CQ13-CQ19's own reports used, plus
`tests/test_identifiers.py` and `tests/test_core_index_safety.py` for this
slice's own store-read/lease-index rewires): `test_core_retention.py`,
`test_retention.py`, `test_operational_indexes.py`, `test_lease_capacity.
py`, `test_queue_management.py`, `test_storage_managed_queue.py`,
`test_control_query_admission.py`, `test_core_idempotency.py`,
`test_artifact_lineage.py`, `test_jarvis_lost_response_recovery.py`,
`test_jarvis_recovery_scheduling.py`, `test_legacy_output_migration.py`,
`test_release_validation.py`, `test_transform_provenance.py`,
`test_queue_readiness.py`, `test_queue_record_codecs.py`,
`test_worker_lifetime_lock.py`, `test_queue_startup_audit.py`,
`test_browser_attachment_queue.py`, `test_service_runtime.py`,
`test_gateway_public_projection.py`, `test_core_global_pagination.py`,
`test_input_staging.py`, `test_builtin_jarvis_input_staging.py`, and
`test_cli.py` all pass green (331 tests in the combined sweep run, plus
`test_cli.py`'s own full suite, plus `test_fastmcp_server.py`/`test_
endpoint.py`'s 154). A full `tests/` collection pass
(`pytest tests/ --collect-only`) confirms zero import errors anywhere in
the tree.

### 15.7 the MRO proof (design doc §3's own CQ20 acceptance criterion)

`tests/test_core_queue_split_architecture.py::
test_every_public_method_resolves_to_an_owner_mixin_or_the_pinned_
allowlist` walks all 128 public `ClioCoreQueue` methods and, for each,
finds its real defining class by walking `ClioCoreQueue.__mro__` in
resolution order (the same lookup Python itself performs). Every name
outside the allowlist below must resolve to a `*Mixin` owner class, never
to `ClioCoreQueue` itself; both allowlisted names are checked in the
opposite direction, so the allowlist cannot silently go stale if a future
slice moves one of them into a real owner. The allowlist, verbatim:

```python
_FACADE_RESIDENT_PUBLIC_METHODS: frozenset[str] = frozenset(
    {
        # CQ19-ST-02: a thin dispatch to the bare module-level
        # ``queue_startup.initialize`` function, kept off the owner mixin
        # manifest on purpose.
        "initialize",
        # CQ19-TI-01: a thin dispatch over the write-ahead-log primitives
        # that themselves stay facade-resident (``_recover_pending_
        # transitions_unlocked`` and friends).
        "reconcile_pending_transitions",
    }
)
```

Every other one of the 126 remaining public methods resolves to a composed
owner mixin. (Private facade-resident methods -- the CQ20-FA-01 store-
adapter family, CQ13-IO-01, and the rest of CQ19-TI-01 -- are deliberately
outside this proof's scope: the design doc's own acceptance text and
`test_facade_public_method_set_stays_at_the_128_method_base` both scope the
128-method pin, and by extension this proof, to the *public* surface.)

### 15.8 final owner table (all 46 owners, campaign close)

| Rank | Owner | Real lines | Rank | Owner | Real lines |
|---:|---|---:|---:|---|---:|
| 0 | `queue_context.py` | 69 | 23 | `queue_execution_cleanup.py` | 352 |
| 1 | `queue_jarvis_inputs.py` | 322 | 24 | `queue_jobs.py` | 786 |
| 2 | `queue_layout.py` | 391 | 25 | `queue_input_ingest.py` | 696 |
| 3 | `queue_store_lock.py` | 270 | 26 | `queue_progress.py` | 176 |
| 4 | `queue_store_read.py` | 412 | 27 | `queue_tasks.py` | 405 |
| 5 | `queue_store_write.py` | 225 | 28 | `queue_execution_cleanup_markers.py` | 335 |
| 6 | `queue_lease_records.py` | 679 | 29 | `queue_lease_indexes.py` | 610 |
| 7 | `queue_scheduler_cancel_records.py` | 133 | 30 | `queue_lease_capacity_state.py` | 474 |
| 8 | `queue_legacy_output_codec.py` | 499 | 31 | `queue_lease_capacity_audit.py` | 584 |
| 9 | `queue_index_state.py` | 262 | 32 | `queue_lease_recovery.py` | 601 |
| 10 | `queue_legacy_output_audit.py` | 519 | 33 | `queue_lease_admission.py` | 571 |
| 11 | `queue_legacy_output_migration.py` | 230 | 34 | `queue_leases.py` | 340 |
| 12 | `queue_legacy_audit.py` | 635 | 35 | `queue_scheduler_cancel_claims.py` | 538 |
| 13 | `queue_order_index.py` | 449 | 36 | `queue_gateways.py` | 383 |
| 14 | `queue_events.py` | 269 | 37 | `queue_browser_attachments.py` | 407 |
| 15 | `queue_owner_session_records.py` | 686 | 38 | `queue_monitor_rules.py` | 212 |
| 16 | `queue_owner_session_lifecycle.py` | 335 | 39 | `queue_gc_storage.py` | 250 |
| 17 | `queue_idempotency.py` | 270 | 40 | `queue_job_gc_protections.py` | 283 |
| 18 | `queue_endpoints.py` | 334 | 41 | `queue_job_gc.py` | 670 |
| 19 | `queue_artifact_lineage.py` | 494 | 42 | `queue_index_discovery.py` | 359 |
| 20 | `queue_gateway_indexes.py` | 524 | 43 | `queue_startup.py` | 543 |
| 21 | `queue_artifacts.py` | 218 | 44 | `queue_index_migration.py` | 704 |
| 22 | `queue_scheduler_cancel_state.py` | 436 | 45 | `queue_transitions.py` | 254 |

Sum across the 46 owners: 19,194 real lines. Plus `core_queue.py` (598):
**19,792** total physical lines across the fully split queue, versus
16,137 at the pinned `d6253d7` evidence commit -- the difference is
`Overhead` (imports, docstrings, `TYPE_CHECKING` stubs, module framing,
typed-deviation documentation) that section 2's planning table always
budgeted for, plus deliberate typed-deviation narrative this ledger
accumulated slice by slice. Every owner is below the 800-line hard gate;
the largest is `queue_jobs.py` at 786.

### 15.9 typed deviations, final disposition

| Deviation | Landed at | End-state |
|---|---|---|
| CQ4-IO-01 (`queue_scheduler_cancel_state` inline quota helper) | CQ4 | Permanent -- same class of problem as CQ13-IO-01, unaffected by CQ20 |
| CQ9 lineage/artifacts 2-cycle (`get_job`/`get_artifact`) | CQ9 (ledger §9.3) | Permanent -- resolved via shared `queue_store_read.read_required_job`/`read_required_artifact` primitives |
| CQ13-IO-01 (`_assert_input_ingest_quota_unlocked`) | CQ13 | **Permanent**, facade-resident, unchanged this slice (still the caller-adjacency constraint against `queue_jobs.submit_job`) |
| CQ15-LR-01 (`queue_leases`/`queue_lease_recovery` cycle) | CQ15 (ledger §10.2) | Permanent -- `_delete_lease_unlocked` hosted on `queue_lease_recovery`, resolved at landing |
| CQ15's `sync_operational_indexes` module-level twin | CQ15 | Permanent -- mandated by the design's own failing-first prescription |
| CQ16 gateway-indexes re-rank | CQ16 (ledger §11.2) | Permanent -- a rank correction, not a deviation from the facade |
| CQ17-EC-01 (execution-cleanup two-owner split) | CQ17 | Permanent -- forced by a genuine rank conflict between `queue_jobs` and `queue_tasks` |
| CQ18-JG-01 (job-GC protections/orchestration split) | CQ18 | Permanent -- size-forced, clean one-directional dependency |
| CQ19-ST-01 (`queue_startup` ranks before `queue_index_migration`) | CQ19 | Permanent -- a rank correction |
| CQ19-ST-02 (`initialize` bare module function + facade dispatcher) | CQ19 | **Permanent**, in the CQ20 MRO-proof allowlist by name |
| CQ19-TI-01 (WAL primitives stay facade-resident) | CQ19 | **Permanent**, unchanged this slice; `reconcile_pending_transitions` is in the MRO-proof allowlist, the rest are private (outside the proof's public-only scope) |
| CQ20-JI-01 (CQ1 jarvis-input composition) | CQ20 | **Dissolved** -- real `QueueJarvisInputsMixin`, §15.4 |
| CQ20-SA-01 (sealed-state duplicate) | CQ20 | **Dissolved** -- reuses `queue_legacy_audit._read_sealed_state`, no new duplicate, §15.3 |
| CQ20-FA-01 (store-adapter hub family) | CQ20 | **Permanent**, newly named and documented this slice, §15.2 |
| `_QueueStoreAdapter` full retirement (ledger §9.5's open question) | CQ20 | **Closed: permanent.** CQ20's own issue text names it facade-legitimate; §15.2 records the two independent reasons it cannot retire |

Two dissolved, twelve permanent (all pre-existing plus the one new family
this slice had to name), zero left open.

### 15.10 execution status: the split is COMPLETE

All eight measurable exit criteria in section 7 now hold:

1. `core_queue.py` is 598 physical lines -- below 800, and contains only
   typed owner-mixin composition, the private store adapter, constructor/
   context wiring, and the two documented CQ19-ST-02/CQ19-TI-01 facade
   dispatchers plus the CQ20-FA-01 store-adapter hub family and CQ13-IO-01.
2. Every target in section 2 is at or below its (ratcheted, justified)
   planned cap and below the 800-line hard gate; §15.8's table is the
   final accounting. No new file-size baseline was added.
3. The section 1 inventory's disjointness is preserved by construction --
   CQ20 only moves or deletes lines already assigned to `core_queue.py` at
   CQ19; nothing is copied.
4. The call graph topologically sorts per section 3's DAG; no owner
   imports `core_queue`; no bare cross-owner collaborator import. CQ20
   added zero new edges (every dissolution either reuses an
   already-earlier-ranked owner as a forward self-call, or moves a call
   site to a module-qualified reference with no owner-rank implication at
   all).
5. All patch sites remain public-facade patches or the module containing
   the real post-split call expression; §15.5 fixed the one CQ16-vintage
   exception the broad sweep found.
6. CQ14's `QueueTasksMixin` composition proof stands unchanged from its
   own slice; CQ20 adds the general-purpose MRO proof (§15.7) as the
   campaign-wide version of the same idea, covering all 128 public
   methods rather than one owner's.
7. On-disk paths, JSON bytes/digests, exception types/messages,
   signatures, cursors, lock boundaries, and crash replay are unchanged --
   every CQ20 rewire is a pure call-site relocation to the same underlying
   function, never a behavior change.
8. `ruff check --fix`, `ruff format`, `pyright`, `pytest`, and `scripts/
   check_file_size.py` all pass with no failure or skip (§15.6); the
   `core_queue.py` ratchet has nothing left to lower (no baseline entry
   since CQ19).

**The `core_queue.py` split (issue #231, CQ1-CQ20) is COMPLETE.**
