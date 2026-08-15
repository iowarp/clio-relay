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
