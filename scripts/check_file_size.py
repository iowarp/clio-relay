#!/usr/bin/env python3
"""Ratchet guard against god-files in the clio_relay source tree.

This check exists to prevent god-files from re-accreting now that the
owner-module decomposition (iowarp/clio-relay#231) is under way. It walks
``src/clio_relay/**/*.py`` and ``jarvis-packages/clio_relay/**/*.py`` and
enforces a per-file line-count ratchet:

* A file **not** in :data:`RATCHET_BASELINE` may not exceed
  :data:`DEFAULT_MAX_LINES` -- a brand-new god-file fails the check.
* A file **in** :data:`RATCHET_BASELINE` (a known-oversized module still
  awaiting decomposition) may not exceed its *recorded* line count -- it can
  shrink but never grow past where it is today.
* A :data:`RATCHET_BASELINE` entry that no longer names a file on disk is
  itself a failure: a stale entry silently hides a file that either moved or
  was deleted without cleaning up the ratchet, and would otherwise mask a
  new file quietly reusing the same relative path.

The baseline may only ratchet DOWN. When a file is brought under the cap, or
merely shrinks, the check reports the ratchet-down and the same change that
shrank it updates :data:`RATCHET_BASELINE` (lowering the number, or removing
the entry once the file is under ``DEFAULT_MAX_LINES``). Ratchet-down reports
are advisory: they do not fail the build.

Run as part of CI (blocking) and locally::

    uv run python scripts/check_file_size.py
    uv run python scripts/check_file_size.py --max 600
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

# Default maximum number of lines a single *non-baselined* source module may
# contain. New files must stay under this cap.
DEFAULT_MAX_LINES = 800

# Per-file ratchet baseline: the modules over DEFAULT_MAX_LINES at their
# current line counts, measured against the tree at iowarp/clio-relay#231
# (item 2). This mapping may only ratchet DOWN -- when a file shrinks, lower
# its number here (or drop the entry once it falls under DEFAULT_MAX_LINES)
# in the same change. Paths are relative to the repository root and use
# forward slashes.
RATCHET_BASELINE: dict[str, int] = {
    # #231 R6: +28 net lines -- the T3 record-time head+tail bound (doc §6.4)
    # applied where _write_mcp_result builds the durable result document:
    # a bounded_payload import, the bound_stream_capture call pair, and the
    # two new stdout_truncation/stderr_truncation result fields. No deletion
    # offsets it -- this is genuinely new structure the doc's own §6.4/§6.5
    # ledger names as never having existed before R6, not a fixable
    # regression. A justified, minimal ratchet-up.
    "jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py": 5782,
    "jarvis-packages/clio_relay/clio_relay/process_containment.py": 2678,
    # #158: +17 net lines -- the preflight script now travels on STDIN instead
    # of in argv. Some ssh clients silently truncate a long command-line
    # argument (the MSYS2 OpenSSH in Git for Windows drops everything past
    # roughly 8-10 KB; the preflight is ~11 KB), and the remote shell then
    # reports a syntax error for a script that was cut mid-token, naming
    # neither the truncation nor the transport. The added lines are the _run
    # input_bytes passthrough and the comment recording the hazard.
    # #158: +4 further lines -- the uv adoption check compares the version
    # TOKEN instead of the whole `uv --version` line. uv prints a platform
    # suffix, so a byte-identical pinned uv (its sha256 already verified) was
    # rejected over cosmetics.
    # #158: +37 net lines -- the descriptor-pinned directory walk resolves the
    # SITE prefix (the operator's home) before pinning, so a cluster whose
    # /home is a symlink onto shared storage (ares: /home -> /mnt/common) can
    # be bootstrapped at all. Everything below the prefix is bootstrap-owned
    # and stays unresolved, so an owned intermediate swapped for a symlink
    # between two journal actions is still refused rather than laundered by
    # realpath. Two small helpers plus the comment recording why the guard is
    # scoped exactly there.
    # clio-relay#242: +104 net lines -- the two receipt-minting heredocs
    # (relay-only reconcile + fresh install) each gain a per-surface,
    # INTEGRITY-only jarvis contract probe (`probe_surface_contract_identity`)
    # that runs before the strict, single-id probe and records the result on
    # the receipt's new `contract_surfaces`/`contract_degradations` fields
    # instead of letting an unconditional strict ask kill the whole cluster
    # bootstrap; the relay-only reconcile path's own post-write verification
    # heredoc gains the matching tolerance for a RECORDED, below-pin jarvis
    # surface. No deletion offsets it -- this is the genuinely new
    # capability-by-negotiation logic the doctrine comment on #242 describes,
    # not a fixable regression. A justified, minimal ratchet-up.
    # #158: +25 net lines -- the JARVIS resource-graph activation check now
    # verifies what JARVIS actually attests (that the SOURCE it read is our
    # packaged builtin, carrying the digest it reported) instead of demanding
    # the ACTIVATED file be a byte copy of it. JARVIS normalizes the graph while
    # activating it, so byte equality failed every fresh bootstrap.
    # #158: +13 net lines -- both provider verifications resolve the ELF string
    # table by the segment holding its START and then bound the range against
    # the file, instead of requiring one PT_LOAD to contain the whole table. A
    # real .dynstr (CPython 3.12.13 as shipped by uv) spans two contiguous
    # segments, so the old form found zero candidates and refused every staged
    # upgrade as "ambiguous". Mostly the comments recording that layout.
    # clio-relay#247/#254: -629 net lines -- receipt-shape/JARVIS-repository-
    # provenance validation (`_validate_bootstrap_receipt` and its four
    # helpers) moved to the new owner module bootstrap_receipt_validation.py,
    # offsetting the #247 state-aware forward-recovery dispatch (delegates to
    # the new bootstrap_recovery.py) and the #254 jarvis-venv staging guard
    # and promotion wiring (delegates to the new bootstrap_jarvis_staging.py).
    # A net ratchet-down even after both fixes' own new call sites.
    "src/clio_relay/bootstrap.py": 8356,
    # #158 journal hardening (site-prefix walk + cross-call swap refusal): 1534
    # measured; restored after a merge-resolution slip dropped the entry.
    "src/clio_relay/bootstrap_journal.py": 1534,
    # #158: +6 net lines -- the receipt binds that activation evidence was
    # recorded, rather than equating the activated digest with the packaged
    # source digest, which JARVIS's normalization makes legitimately unequal.
    "src/clio_relay/bootstrap_reconcile.py": 4468,
    # #231 R9 fix batch: -32 net lines after moving overload/error rendering
    # into browser_gateway_errors.py; every former ad-hoc gateway response now
    # uses the door owner without recreating the core/gateway import cycle.
    "src/clio_relay/browser_gateway.py": 853,
    # #231 R7: +12 net lines -- compute_release_acceptance_matrix_sha256 is
    # extracted from validate_release_acceptance_matrix's inline hashlib.sha256
    # call so release_pins.py's bump command can reuse the exact same digest
    # computation (doc's §7 "derived-digest-with-ordering" rule: never
    # recompute the hash independently). Single owner, ground rule 1 -- a
    # justified, minimal ratchet-up.
    "src/clio_relay/ci_validation.py": 3787,
    # #231 R6 review fixes: +22 net lines -- F6, `job read-artifact` exits 1
    # on a T2 refusal (is_delivery_refusal) instead of a silent 0 alongside
    # a body that says result_available: false; F5, the shared
    # `_decode_artifact_envelope` (four callers) reports a refusal's own
    # message/code instead of the generic "must use base64 encoding". A
    # justified, minimal ratchet-up.
    # #231 R7: +20 net lines -- `release preflight`, one thin command
    # registration delegating entirely to `release_pins.run_preflight`/
    # `render_preflight` (ground rule 2: cli.py parses and renders only). A
    # justified, minimal ratchet-up.
    # #231 R8(i): -18 net lines -- the monkeypatch-seam rework (doc §4.6/§9)
    # converts every collaborator symbol cli.py imported by bare name into a
    # module-attribute call site (`import clio_relay.X as X`, `X.symbol(...)`
    # instead of `from clio_relay.X import symbol`), so tests patch the
    # symbol where it is looked up (the owner module) and survive future
    # command-module extraction. Net negative: the added `import ... as ...`
    # lines are outweighed by the removed multi-line `from ... import (...)`
    # blocks they replace. A ratchet-down.
    # #231 R6-fix review, A2: +3 net lines -- `_decode_artifact_envelope`'s
    # delivery-refusal message extraction delegates to
    # `bounded_payload.describe_delivery_refusal` (single owner) instead of
    # re-deriving the same fallback-text extraction inline. A justified,
    # minimal ratchet-up.
    # #231 R8(ii): -579 net lines -- the first cli.py command-module
    # extraction (doc §5's `relay-host` row): the seven `relay_host_app`
    # commands move to the new `cli_relay_host.py` (600 lines, its own new
    # ratchet-exempt file, cap 800), and the six shared-plumbing helpers the
    # doc's cli_support.py row names (`_run_or_exit`, `_require_cluster`,
    # `_write_failed_acceptance_report`, `_resolve_env_secret`,
    # `_acceptance_report_command`, plus the private `_local_secret`/
    # `_echo_storage_admission_error` each depends on) move to the new
    # `cli_support.py` (185 lines), with cli.py keeping each under its
    # original name as a one-line re-export so its ~200 other bare-name
    # call sites and every existing `monkeypatch.setattr(cli, "_X", ...)`
    # test patch keep working unchanged. A handful of the re-export lines
    # (and the two new modules' own top-of-file directives) carry a
    # `# pyright: ignore[reportPrivateUsage]`/`reportUnusedFunction=false`
    # comment -- both files legitimately reach the other's underscore-
    # prefixed names by design (their own docstrings explain why), the same
    # shape `http_api.py`'s own `reportUnusedFunction=false` already covers
    # for decorator-registered-only route handlers. The largest single
    # ratchet-down of the #231 campaign so far.
    # #231 R8(ii) review fix (F3/F4): +58 net lines -- the five cli_support.py
    # re-exports (`_run_or_exit`, `_require_cluster`,
    # `_write_failed_acceptance_report`, `_resolve_env_secret`,
    # `_echo_storage_admission_error`) become thin forwarders instead of bare
    # object re-bindings: a bare `_run_or_exit = cli_support._run_or_exit`
    # captures the owner's function object at import time, so
    # `monkeypatch.setattr(cli_support, "_run_or_exit", ...)` never reached a
    # caller holding the old reference -- a silent no-op that only
    # `monkeypatch.setattr(cli, ...)` could see. A forwarder re-reads
    # `cli_support.<symbol>` on every call, restoring both patch directions.
    # Interim layer, not a final shape -- net deletion arrives when the other
    # ~15 sub-apps migrate onto `cli_support.X(...)` directly (unsequenced
    # future work, same as the R8(ii) note above). A justified, minimal
    # ratchet-up.
    # #158: +17 net lines -- `cluster bootstrap` gained a fourth recorded
    # check (cluster.bootstrap.runtime-pin) that re-points the registry at the
    # runtime it just produced, so an install can no longer leave a dead
    # relay_executable pointer behind. The reconciliation itself and its
    # operator-visible line rendering live in the new owner module
    # src/clio_relay/bootstrap_pin.py; what remains here is the recorder.check
    # block, which must sit inside the command's evidence scope. A justified,
    # minimal ratchet-up.
    # #158 (probe): +14 further lines -- the `cluster probe` command, a
    # read-only recon surface that never dereferences relay_executable, so a
    # deployment whose pin is dead can still be inspected. Its logic lives in
    # the new owner module src/clio_relay/cluster_probe.py; only the Typer
    # command body is here.
    # #158 (review F1): +1 further line -- the runtime-pin check now passes the
    # remote presence observation, so a pin is only rewritten when proven
    # absent and a valid custom pin survives bootstrap. The observation itself
    # lives in cluster_probe.pinned_runtime_present.
    "src/clio_relay/cli.py": 18849,
    # #231 R5: +16 net lines -- FrpTransportConfig gains proxy_name +
    # identity_anchor (the §8.3 typed opt-in frp_transport.py's build_transport
    # refusal reads) plus the IdentityAnchor type alias and its docstring. No
    # deletion offsets it: these are two new, real config fields, not a fixable
    # regression.
    "src/clio_relay/cluster_config.py": 1863,
    # #231 CQ8: idempotency admission and endpoint registration/heartbeat
    # ownership move behind typed owner/store seams, lowering the facade
    # ratchet by 504 lines.
    # #231 CQ9 fix round: restore the CQ1 protocol alias while deleting two
    # confirmed facade corpse wrappers. Net facade ratchet-down: 11011 -> 11004.
    # #231 CQ10: move owner-session lifecycle/record bodies and identity
    # validators into their two budgeted owners, then compose the lifecycle
    # mixin and retain only qualified owner lookups. Net: 11004 -> 10065.
    # #231 CQ11: move the scheduler-cancellation pending/disposition public
    # methods and the CQ4-IO-01 deviation's four durable-state helpers
    # (_scheduler_cancel_record_path/_ensure_scheduler_cancel_pending_unlocked/
    # _require_scheduler_cancel_pending_unlocked/
    # _persist_scheduler_cancel_record_unlocked, deliberately left resident at
    # CQ4 because queue_scheduler_cancel_records.py is a store-independent
    # codec module) into the new queue_scheduler_cancel_state.py owner, and
    # deletes the two now-dead _scheduler_cancel_record_is_due/
    # _scheduler_cancel_due_sort_key module shims a repository-wide call-site
    # audit found unreferenced once their only caller moved. Net: 10065 ->
    # 9688.
    # #231 CQ12: move submit_job, the job CRUD/paging/scan surface, and the
    # state-transition methods (update_job_state/cancel_job_if_active/
    # acknowledge_job_cancellation/update_job_metadata) plus their unlocked
    # write/capacity/index primitives into the new queue_jobs.py owner.
    # submit_job's two _ensure_global_order_entry_unlocked call sites become
    # direct queue_order_index.ensure_global calls (CQ7's owner); the real
    # _write_job_unlocked body becomes the module-level write_job, and the
    # facade's old method is replaced by a thin instance-method wrapper so
    # every not-yet-extracted caller elsewhere keeps working unchanged.
    # Deletes the now-dead _committed_idempotency_record and _UNSET module
    # aliases a repository-wide call-site audit found unreferenced once
    # their only callers moved. Net: 9688 -> 9043.
    # #231 CQ13: move the input-artifact ingest lifecycle (begin/fail/
    # recover/reconcile/complete, the two event-exists predicates, and the
    # module-level attempt/identity-compare helpers) into the new
    # queue_input_ingest.py owner. CQ13-IO-01 typed deviation:
    # _assert_input_ingest_quota_unlocked stays facade-resident -- its only
    # external caller, queue_jobs.submit_job (786/800, no headroom), would
    # otherwise create a reverse-rank queue_jobs -> queue_input_ingest self-
    # call edge the architecture guard rejects, and no earlier-ranked owner
    # has ~90 spare lines to host it as a shared primitive instead; its
    # one-caller quota-consumption predicate,
    # _input_ingest_consumes_quota_unlocked, carries no such constraint and
    # moved as designed. Net: 8991 -> 8430.
    # #231 CQ14: move the task and MCP task-projection CRUD (append_task,
    # put_mcp_task/update_mcp_task_projection/get_mcp_task -- the durable
    # boundary the merged #234 admission/park machinery persists through,
    # unmoved itself per design §5 -- update_task_state/_metadata, list/page/
    # scan_job_tasks, get_task) and structured job-progress CRUD
    # (append_progress, list/page_progress, latest_job_progress), plus their
    # unlocked write/derived-index primitives and the module-level
    # _canonical_mcp_task_arguments identity-compare helper, into the new
    # queue_tasks.py and queue_progress.py owners. No typed deviation: every
    # collaborator (queue_jobs.get_job, queue_events.append_event, every
    # queue_order_index job-index primitive) is an already-landed
    # earlier-ranked owner, and _job_record_path/_write_transition_intent_
    # unlocked/_recover_pending_transitions_unlocked stay facade-resident
    # (still un-extracted, self-called as before). _sync_scheduler_source_
    # unlocked stayed facade-resident too at CQ14 landing time, but was
    # itself extracted later, to queue_gateway_indexes.py at CQ16 -- this
    # net delta is unaffected (it predates that move), corrected here only
    # so the claim does not go on describing a fact that stopped being true
    # two slices later (N14, closing-round review). Net: 8430 -> 8009.
    # #231 CQ15 (N14, closing-round review: this entry was missing): the
    # lease/recovery family, seven owners -- queue_lease_indexes,
    # queue_lease_capacity_state, queue_lease_capacity_audit, queue_lease_
    # recovery, queue_lease_admission (gate-forced split from queue_leases
    # at 854 > 800, zero call-graph overlap), queue_leases, queue_
    # scheduler_cancel_claims. 27 delegating tail shims deleted; residual
    # facade call sites rewired to direct module calls. Typed deviations:
    # CQ15-LR-01 (_delete_lease_unlocked hosted on queue_lease_recovery to
    # break a genuine admission<->recovery cycle, ledger §9.3 precedent);
    # the sync_operational_indexes module twin (design-prescribed patchable
    # seam); the gate-forced admission split above. _is_sha256_digest: no
    # lease-recovery copy exists -- the facade copy stays with its unmoved
    # job-GC callers (CQ18, §13.3). Net: 8009 -> 4848.
    # #231 CQ16: gateways, browser attachments, gateway indexes, and monitor
    # rules move to queue_gateways.py / queue_browser_attachments.py /
    # queue_gateway_indexes.py / queue_monitor_rules.py. Net: 4848 -> 3606.
    # #231 CQ17: execution cleanup (shard layout/migration/detection plus the
    # durable-marker mutation half, split CQ17-EC-01) moves to queue_
    # execution_cleanup.py / queue_execution_cleanup_markers.py; the facade's
    # now-dead `hashlib` import is also dropped. Net: 3606 -> 3056.
    # #231 CQ18: job GC (eligibility protections plus phased trash-staging
    # collection, split CQ18-JG-01) moves to queue_job_gc.py / queue_job_gc_
    # protections.py; GC quarantine-tree storage moves to queue_gc_storage.py.
    # Net: 3056 -> 2077.
    # #231 CQ19: index discovery (schema-upgrade/gate-reconciliation/state-
    # extension) moves to queue_index_discovery.py, the bounded migration
    # batch driver to queue_index_migration.py, the transition-intent
    # applier to queue_transitions.py, and queue startup (initialize plus
    # its locked-core/permission-repair helpers) to queue_startup.py. Net:
    # 2077 -> 746 (corrected from a stale "713" -- the real count at the
    # CQ19 landing commit, 17061e4 -- N14, closing-round review) -- under
    # the 800-line default cap, so core_queue.py drops out of the ratchet
    # baseline entirely (this script's own documented convention: "remove
    # the entry once the file is under DEFAULT_MAX_LINES").
    "src/clio_relay/deployment.py": 1243,
    # #231 R9 fix round 3: cohesive wire-adapter owner split out of
    # door_errors.py. Both sides are recorded exactly even below the default
    # cap so this decomposition cannot silently re-accrete.
    # Campaign merge: values are the MEASURED merged-tree counts.
    "src/clio_relay/door_error_adapters.py": 170,
    # Pattern-triggered observation adds two typed refusal reasons to the
    # frozen door registry; the measured post-change owner count is 744.
    "src/clio_relay/door_errors.py": 744,
    # #231 R6 review fixes: +9 net lines -- F4, `_write_recovered_jarvis_
    # run_result`'s `recovered_document` now nulls `stdout_truncation`/
    # `stderr_truncation` alongside the blanked `stdout`/`stderr`, instead
    # of inheriting a stale populated record from the spread source
    # document. A justified, minimal ratchet-up.
    # #238: +24 net lines -- the daemon-mode worker-slot silent-death fix.
    # `_serve_worker_slot`'s per-slot loop and
    # `_reconcile_pending_execution_cleanup`'s one unguarded recovery-intent
    # fetch now delegate to the new `endpoint_worker_lanes.py` owner module
    # (quarantine_relay_error / run_worker_lane_iteration) instead of
    # growing inline case logic here; the two call sites plus one new
    # import account for the delta. A justified, minimal ratchet-up -- the
    # alternative (leaving either call site unguarded) is the silent
    # slot-death and 0-byte-log defect the issue reports.
    "src/clio_relay/endpoint.py": 8743,
    # relay#234 adversarial review, finding 1: +24 net lines --
    # `intercept_tool_call`'s conflict handling caught only
    # `TaskInputParkConflictError`/`QueueConflictError`; anything else
    # `create_task` raised (disk-full, permission) escaped through
    # FastMCP's own generic handler untyped, violating the error.v1/
    # no-silent-fallback doctrine. Added an `except MCPError: raise` (never
    # re-classify an already-typed error) followed by a catch-all that
    # routes every other exception through `door_errors.classify`/
    # `as_mcp_error`. A justified, minimal ratchet-up.
    # clio-relay#242 actionability audit: +23 net lines -- the
    # `mcp_task_conflict` QueueConflictError handler passed no explicit
    # `message=`, so `door_errors.classify()` fell back to the generic
    # reason title ("MCP task conflict.") instead of naming the conflicting
    # task -- discovered because it made the PRE-EXISTING
    # `test_task_projection_conflict_surfaces_as_typed_mcp_error` fail on
    # `develop` before this change (its "different semantics" assertion
    # expected raw exception detail that was never reaching the wire). Now
    # builds an authored, actionable message naming the conflicting
    # task_id and the `tasks/get` query verb. A justified, minimal
    # ratchet-up for a real bug fix, not accretion.
    # Pattern observation errors are converted to typed MCP errors at the
    # native FastMCP boundary; the measured post-change count is 1267.
    "src/clio_relay/fastmcp_server.py": 1267,
    # #231 R3: +24 net lines (door_errors import + the ONE global
    # Exception-handler function + its registration) -- deliberately not
    # offset by deleting any of the 107 existing HTTPException sites, which
    # the same slice's design doc explicitly keeps in place (§6.2). +35 more
    # from the opus re-review's F5/F15: a logger + a hardcoded fallback
    # document so the handler survives door_errors itself failing, plus the
    # corrected build_middleware_stack docstring. A justified, minimal
    # ratchet-up rather than a same-file deletion this slice does not own.
    # #231 R6 review fixes: +29 more -- F2, `GET /artifacts/{id}/content`
    # routes an over-budget read through door_errors' existing
    # payload_too_large door (413) instead of answering 200 with a body
    # that merely says result_available: false. A justified, minimal
    # ratchet-up.
    # #231 R6-fix review, A2: -2 net lines -- the same route's delivery-
    # refusal message extraction now delegates to
    # `bounded_payload.describe_delivery_refusal` instead of a 4-line
    # inline `delivery.get("message", ...)` extraction. A ratchet-down.
    # #231 R9: the original -12-site migration baseline was recorded as
    # 3,137 before a later 7-line handler-variance fix; the pre-batch file
    # was honestly 3,144 lines. This fix batch adds owner registrations for
    # framework validation/HTTP errors, typed WebSocket close reasons, and
    # split session-binding courses of action. Final physical count: 3,194.
    # #231 R9 fix round 2: +60 lines make the 58 exception-backed sites'
    # public-message disposition explicit. Relay-authored validation and
    # conflict catches opt into the typed marker; the mixed ingest catch
    # marks QueueConflictError only and keeps OS/runtime text private.
    # #231 R9 fix round 3: +6 lines repoint every HTTP/WebSocket surface
    # adapter call to the cohesive door_error_adapters owner.
    # clio-relay#242 actionability audit: +26 net lines -- `mcp_submission_
    # conflict` (the live case: the ares agent's spack_find hit this reason
    # with no way to tell retry from refresh-and-resubmit apart) and
    # `job_submission_conflict` now carry an authored, actionable `message=`
    # (refresh-discovery guidance; the at-fault idempotency_key plus the
    # retry-with-a-new-key move) instead of only the raw invariant text. A
    # justified, minimal ratchet-up.
    "src/clio_relay/http_api.py": 3267,
    "src/clio_relay/input_staging.py": 814,
    # clio-relay#242: -12 net lines -- `_run_json_probe`/`_mcp_contract_digest`
    # move to the new contract_gate.py owner module (single owner for every
    # contract-identity probe/digest, reused by the per-surface bootstrap
    # enumeration), offsetting the new InstallReceipt contract_surfaces/
    # contract_degradations fields and the typed use-time refusal in
    # verify_remote_clio_kit_native_execution_component. A ratchet-down.
    # clio-relay#242 dev-mode course correction: +5 net lines --
    # `verify_remote_clio_kit_native_execution_component`'s below-pin jarvis
    # refusal now delegates to `contract_gate.require_surface_contract`
    # (dev-mode aware) instead of raising `contract_surface_unavailable`
    # inline, and returns the worker's unverified runtime identity when dev
    # mode defers rather than falling into the generic "omitted" error. A
    # justified, minimal ratchet-up.
    "src/clio_relay/installation.py": 3711,
    "src/clio_relay/jarvis_execution.py": 875,
    "src/clio_relay/jarvis_mcp.py": 947,
    # #231 R6-fix review, A6: +1 net line -- `_execution_query_contract_evidence`'s
    # own `expected_filters` set was a stale v3.6-shaped copy that never
    # gained `content_max_bytes` when the v3.7 contract added it (7ea003a
    # touched `installation.py`'s equivalent guard but missed this one) --
    # the acceptance validator was reporting a fully compliant remote
    # contract as FAILED. A ratchet-up for a one-line real bug fix, not
    # accretion.
    "src/clio_relay/jarvis_mcp_validation.py": 2672,
    # #231 R6 review fixes: +11 net lines -- F5, `_load_source` checks
    # is_delivery_refusal on the source envelope FIRST and raises the
    # refusal's own message/code, instead of the generic "is not a base64
    # envelope" that misdescribes why the artifact is unavailable. A
    # justified, minimal ratchet-up.
    # #231 R6-fix review, A2: +2 net lines -- the message extraction now
    # delegates to `bounded_payload.describe_delivery_refusal`; the
    # multi-line f-string it feeds grew by one line in exchange. A
    # justified, minimal ratchet-up.
    "src/clio_relay/jarvis_service_runtime.py": 1171,
    # #231 R6 review fixes: +20 net lines -- F5, both `_verify_completed_
    # job`'s inline artifact/provenance checks and the new shared
    # `_delivery_refusal_error` helper now recognize a T2 refusal by its
    # own message/code before falling into the generic "not base64
    # encoded" complaint. A justified, minimal ratchet-up.
    # #231 R6-fix review, A2: +1 net line -- `_delivery_refusal_error`'s
    # message extraction now delegates to
    # `bounded_payload.describe_delivery_refusal`. A justified, minimal
    # ratchet-up.
    # #231 R6-fix review, A1: +22 net lines -- `_remote_shell`'s non-zero-
    # exit path now recognizes a T2 delivery-refusal document on stdout
    # (`bounded_payload.parse_delivery_refusal`) via a new
    # `_remote_command_failure` helper, before falling into the generic
    # "remote command failed: <blob>" that discarded the refusal's own
    # typed code/message. A justified, minimal ratchet-up.
    "src/clio_relay/live_acceptance.py": 5470,
    # #231 R6: +10 net lines -- _verified_local_mcp_result now checks
    # bounded_payload.is_delivery_refusal on the envelope
    # relay_ops.read_artifact_bytes returns, so a T2 refusal (the durable
    # mcp_result artifact itself over MAX_ARTIFACT_CONTENT_BYTES) surfaces
    # as-is instead of falling into _decode_verified_mcp_result's generic
    # malformed-envelope ValueError. R6 review fixes: +13 more (5930 ->
    # 5943) -- F1, _mcp_tool_result_failed discriminates on the refusal
    # shape (is_delivery_refusal + delivery.status) instead of one named
    # code, via a new shared _delivery_refusal_failed helper; F5, the same
    # helper is also checked against the tool's own top-level result (not
    # only nested under mcp_result), covering relay_read_artifact reading a
    # too-large artifact directly; F7, _bounded_mcp_result's inline failure
    # dict migrated onto bounded_payload.build_delivery_refusal, retiring
    # the local MCP_RESULT_DELIVERY_SCHEMA constant (single owner, the
    # slice's own rule). Each pass justified, minimal.
    # #231 R6-fix review, A4: -14 net lines -- `_delivery_refusal_failed`
    # is promoted to `bounded_payload.is_delivery_refusal_failed` (the
    # single owner every FAILURE-discriminating caller shares, not only
    # the MCP tool-result boundary); its local definition here is deleted,
    # the two call sites now call the imported name (-12), plus a stray
    # doubled blank line `ruff format` collapsed at the deletion site
    # (-2). A ratchet-down.
    # The curated relay_observe schema and local/routed pattern dispatcher
    # grew for until_pattern/pattern_scope; the measured post-change count is
    # 6098. The matching/long-poll mechanics live in the sub-800 observation
    # owner; the additional routed lines retain incremental remote log cursors
    # so long-running jobs do not rescan only the first log page.
    "src/clio_relay/mcp_server.py": 6098,
    # #231 R9 fix round 3: +16 lines keep subprocess stderr out of the marked
    # timeout message and log its bounded diagnostic once server-side.
    "src/clio_relay/mcp_stdio_validation.py": 1285,
    # #231 R9 fix round 2: +3 lines retain v3.6 as a handle-first execution
    # contract while v3.7 remains the current input-staging contract.
    "src/clio_relay/models.py": 2299,
    "src/clio_relay/process_containment.py": 2678,
    "src/clio_relay/queue_management.py": 1671,
    # +11 net lines -- the first live worker_status() read raced a
    # just-registered fleet's own background slot heartbeats
    # (worker_generation_complete could read transiently False before the
    # supervised generation settled), failing run_queue_management_validation
    # with "configured kind concurrency is not an object" under full-suite
    # timing. Retried on the module's usual bounded budget instead of a
    # one-shot read; a real misconfiguration (no worker_generation_id at all)
    # still fails immediately. A justified, minimal ratchet-up.
    "src/clio_relay/queue_validation.py": 1546,
    # #231 R5: +28 net lines -- an `identity_anchor` property (derived from
    # cluster config, independent of link state, §8.3) plus stamping it on
    # every `channel_event(...)` call site (9) and surfacing it in
    # `event_report()`/`_retired_report()`. No logic here is rewritten --
    # `_verify_bootstrap`'s own checks are untouched -- only this wiring is
    # new, so nothing in the file was a candidate for deletion first.
    # R5 opus review fix set: +52 more (1006 -> 1058) -- `build_transport`
    # moves inside `_establish`'s try (R2, a typed refusal must reach the
    # ledger as `establish_failed`, not propagate with a dangling
    # `establishing`), `authorization_required` gates on
    # `transport.requires_user_authorization` instead of the connection-level
    # setting (R7), `identity_anchor` prefers the held link's own snapshot
    # over live config (R9), `close()` reads back a residual
    # `config_cleanup_error` (R3), plus anchor-aware wording corrections
    # (R12). Real behavioral fixes from a security-relevant review, not a
    # deletable regression.
    # #231 R6-fix review, A1: +15 net lines -- the owned-session API's non-
    # 2xx path now recognizes a T2 delivery-refusal document
    # `door_errors.as_http_problem` spread into the 413 problem body (doc
    # §6.4, F4) via `bounded_payload.is_delivery_refusal`, surfacing its
    # own typed code/message instead of the generic "HTTP {status}: {raw
    # json blob}". A justified, minimal ratchet-up.
    "src/clio_relay/remote_connection.py": 1073,
    # #231 R6 review fixes: +11 net lines -- F5,
    # `_control_query_discovery_artifact_bytes` checks is_delivery_refusal
    # on the envelope FIRST and raises the refusal's own message/code,
    # instead of the generic "encoding is unsupported" that misdescribes
    # why the artifact is unavailable. A justified, minimal ratchet-up.
    # clio-relay#242 dev-mode course correction: +60 net lines -- the
    # declared-contract catalog-withholding gate (the exact "jarvis withheld
    # ... declared contract ... failed: ...drifted" failure the ares live
    # run hit) and the server-artifact-identity dev-mode bypass (previously
    # silent) now both stay LOUD when dev mode defers them: a `logger`, the
    # `RemoteMcpCatalogIssue.enforcement` marker field plus its docstring,
    # and a WARNING log call at each site. No deletion offsets it -- this is
    # genuinely new structure the owner ruling requires, not a fixable
    # regression. A justified, minimal ratchet-up.
    "src/clio_relay/remote_mcp.py": 5377,
    # #231 R9 fix round 3: +7 lines keep Pydantic receipt validation detail
    # out of the public conflict while logging it once server-side.
    # #231 CQ18: +1 line -- purge_quarantined_tree_batch's real home moved to
    # queue_gc_storage.py; the combined `from clio_relay.core_queue import
    # ClioCoreQueue, purge_quarantined_tree_batch` splits into two single-name
    # import lines (CQ15 §10.7 precedent: retarget a moved-symbol import to
    # its real new owner, never a facade re-export).
    "src/clio_relay/retention.py": 952,
    "src/clio_relay/runtime_metadata.py": 1749,
    "src/clio_relay/scheduler_providers.py": 1153,
    # #231 R10: the local owned-visitor render/write/spawn path now delegates
    # to frp_link.py, while the three remote frpc start/stop script generators
    # moved to the under-800-line frp_remote_scripts.py owner.  -772 net lines.
    # #231 CQ16 cross-file fix: +4 net lines -- _revoke_browser_attachment's
    # except clause now discriminates BrowserAttachmentIdentityConflictError
    # by type instead of a substring match on QueueConflictError (the banned
    # prose-match pattern). Campaign merge: 9391 base -5 (#242 gating)
    # +4 (CQ16) = 9390, the measured merged count.
    "src/clio_relay/service_runtime.py": 9390,
    # #231 R8(iii) (design doc §4.4, issue #237): the wire-model cluster
    # (`:890-1433` -- one frozen dataclass + 16 pydantic.BaseModel types, 542
    # lines) plus its 2 bound constants moved to the new
    # session_wire_models.py, re-exported here under their original names
    # (RemoteSession is not re-exported -- nothing outside the wire-model
    # module ever referenced it, confirmed by ruff F401). 8326 -> 7801.
    # #158: +46 net lines -- new typed structure in the SSH transport that the
    # file never had. (1) _raise_if_relay_executable_missing, one guard shared
    # by both SSH transports, classifies shell status 127 as a dead
    # relay_executable pin instead of collapsing it into
    # _RemoteSessionCommandAmbiguous; (2) start_remote_session_durable finally
    # HANDLES that ambiguity by resolving it against durable remote state
    # instead of letting it escape as a bare RelayError; (3)
    # query_remote_session_start re-raises the dead-pin error ahead of its
    # blanket RelayError handler, which otherwise rewrote it as
    # starting/retryable and rebuilt the retry-forever loop. The shared message
    # factory and the deadline exception class were both pushed out to
    # errors.py to keep this minimal; what remains needs the ClusterDefinition
    # and the bounded result in hand, so it cannot move. A justified, minimal
    # ratchet-up.
    "src/clio_relay/session_lifecycle.py": 7840,
    "src/clio_relay/spool.py": 964,
    "src/clio_relay/storage_policy.py": 1826,
    # #231 R9 fix round 2: +11 lines mark storage-policy refusals as public
    # while exposing only StorageDecision.message, never its serialized
    # exception payload.
    "src/clio_relay/storage_runtime.py": 1124,
    # N13 (closing-round review): +2 lines -- the blanket `# pyright:
    # ignore` on the cross-owner `_job_matches_mcp_admission_class` import
    # is re-narrowed to `[reportPrivateUsage]`, which forces the import
    # onto its own three-line parenthesized form to keep the ignore
    # comment on the diagnostic's own line under the 100-col limit.
    # #231 R4: local-visitor spawn/health/cleanup delegates to the new
    # frp_link.py substrate (HeldFrpVisitor) instead of duplicating it;
    # run_frp_http_probe collapses into a thin proxy_type="stcp" wrapper
    # around _run_frp_http_probe_with_proxy_type. -100 net (1849 -> 1749).
    # +46 more from the R4 opus review fix set (F2/F3/F9): a residual-
    # secret-config-file resource in the cleanup ledger (F3, a real new
    # resource kind, not fixable by deletion), the visitor-failure-message
    # prefix helper (F2), and threading `visitor` through as Optional so a
    # spawn failure still reaches cleanup (F9). Evaluated ground rule 5
    # (remove/redesign first) and rejected -- this is the review's required
    # safety/diagnostic behavior, not accreted duplication. 1749 -> 1795.
    # #231 R8(iii): wire-model import collapses from a 7-line multi-import
    # block to a single-line `from clio_relay.session_wire_models import
    # CleanupResource, OwnedSessionStartResult` -- ratchet down. 1795 -> 1794.
    "src/clio_relay/transport_probe.py": 1794,
    # #231 split/validation-report S1: the pydantic/StrEnum wire-model
    # catalog (LiveValidationReport, ReleaseGatePolicy, InstallSource, ...)
    # moved to validation_schema.py (650 lines) and the byte/count budget
    # constants moved to validation_limits.py (37 lines), each re-exported
    # here via the `X as X` self-import idiom (door_errors.py's precedent)
    # for the existing public/monkeypatch surface -- one name per line so
    # ruff's F401 does not prune a name this module no longer references
    # internally. 5458 -> 4924.
    # #231 split/validation-report S2: ValidationRecorder + the seeded-report
    # factory (new_live_validation_report/_validation_evidence_trust) moved
    # to validation_recorder.py (517 lines), same re-export treatment.
    # 4924 -> 4480.
    # #231 split/validation-report S3: acceptance-line fact classification
    # (line_proves_success/acceptance_scope + the fact-value catalogs) moved
    # to acceptance_facts.py (160 lines). No re-export needed -- the only
    # internal caller (validation_recorder.py) now imports it directly.
    # 4480 -> 4336.
    # #231 split/validation-report S4: credential/secret redaction
    # (sensitive_key/collect_sensitive_values/redact_sensitive_value/
    # redacted_invocation/redact_url/redact_sensitive_values) moved to
    # redaction.py (155 lines). redact_sensitive_values is re-exported
    # (external HTTP/MCP/public-records callers); the private helpers were
    # internal-only, so validation_recorder.py and the three still-inline
    # call sites here import directly from redaction.py. 4336 -> 4211.
    # #231 split/validation-report S5: artifact-identity verification (wheel/
    # PyPI/VCS-commit provenance binding a claimed artifact_sha256 to the
    # bytes this process loaded) moved to artifact_identity_verification.py
    # (563 lines -- over the 150-500 sweet spot but under the 800 cap; one
    # coherent concern, not split further). Every top-level entry point
    # test_validation_report.py exercises directly (not just the ones this
    # module's own remaining code still calls) is re-exported under its
    # original private name -- three via `public_name as _old_name` plus a
    # forced-keep lint suppression comment (ruff's unused-import exemption
    # only recognizes a literal `X as X` self-import, not a rename) since a
    # genuinely unused rename import is otherwise pruned silently. 4211 ->
    # 3751.
    "src/clio_relay/validation_report.py": 3751,
}

# Roots of the source tree to scan, relative to the repository root. Tests
# (tests/, jarvis-packages/clio_relay/*/tests, ...) are intentionally excluded.
SRC_ROOTS: tuple[str, ...] = ("src/clio_relay", "jarvis-packages/clio_relay")


class Failure(NamedTuple):
    """A file (or baseline entry) that breaks the ratchet (fails the check)."""

    rel: str
    # Named line_count, not count: NamedTuple is a tuple subclass and a field
    # literally named "count" overrides tuple.count() with an incompatible
    # type under strict type checking (reportIncompatibleMethodOverride).
    line_count: int
    kind: str  # "new" (non-baselined over cap), "regressed" (over recorded), or "stale"
    limit: int  # the cap broken (new/regressed), or the last recorded count (stale)


class RatchetDown(NamedTuple):
    """A baselined file that shrank -- advisory, not a failure."""

    rel: str
    line_count: int  # see the line_count comment on Failure above
    baseline: int
    under_cap: bool  # True once line_count <= max_lines (drop the entry entirely)


class Result(NamedTuple):
    """Outcome of a scan: failures fail the build, ratchet_downs are advisory."""

    failures: list[Failure]
    ratchet_downs: list[RatchetDown]


def _repo_root() -> Path:
    """Return the repository root (parent of the ``scripts`` directory)."""
    return Path(__file__).resolve().parent.parent


def _count_lines(path: Path) -> int:
    """Return the number of lines in ``path``."""
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return sum(1 for _ in handle)


def check_file_size(
    scan_roots: Sequence[Path],
    *,
    rel_to: Path | None = None,
    max_lines: int = DEFAULT_MAX_LINES,
    baseline: dict[str, int] | None = None,
) -> Result:
    """Evaluate the per-file line-count ratchet under ``scan_roots``.

    Args:
        scan_roots: Directory trees to walk for ``*.py`` files. Must be
            non-empty.
        rel_to: Base directory used to compute the forward-slash relative
            path that keys into ``baseline``. Defaults to ``scan_roots[0]``.
        max_lines: Cap applied to files not present in ``baseline``.
        baseline: Per-file recorded line counts. Defaults to
            :data:`RATCHET_BASELINE`.

    Returns:
        A :class:`Result` splitting build-failing offenders (including stale
        baseline entries) from advisory ratchet-down reports.

    Raises:
        ValueError: If ``scan_roots`` is empty.
    """
    if not scan_roots:
        raise ValueError("check_file_size requires at least one scan root")
    if baseline is None:
        baseline = RATCHET_BASELINE
    base = rel_to if rel_to is not None else scan_roots[0]

    failures: list[Failure] = []
    ratchet_downs: list[RatchetDown] = []
    seen: set[str] = set()
    for scan_root in scan_roots:
        for path in sorted(scan_root.rglob("*.py")):
            rel = path.relative_to(base).as_posix()
            seen.add(rel)
            count = _count_lines(path)
            recorded = baseline.get(rel)
            if recorded is None:
                if count > max_lines:
                    failures.append(Failure(rel, count, "new", max_lines))
                continue
            if count > recorded:
                failures.append(Failure(rel, count, "regressed", recorded))
            elif count < recorded:
                ratchet_downs.append(
                    RatchetDown(rel, count, recorded, under_cap=count <= max_lines)
                )

    for rel in sorted(baseline):
        if rel not in seen:
            failures.append(Failure(rel, 0, "stale", baseline[rel]))

    failures.sort(key=lambda entry: entry.rel)
    return Result(failures=failures, ratchet_downs=ratchet_downs)


def _print_report(result: Result, max_lines: int) -> None:
    """Print the ratchet report (failures then advisory ratchet-downs)."""
    for entry in result.ratchet_downs:
        if entry.under_cap:
            print(
                f"OK (ratchet down): {entry.rel} is now {entry.line_count} lines "
                f"(<= {max_lines}) -- remove it from RATCHET_BASELINE in "
                "scripts/check_file_size.py."
            )
        else:
            print(
                f"OK (ratchet down): {entry.rel} shrank {entry.baseline} -> "
                f"{entry.line_count} -- lower its RATCHET_BASELINE entry to "
                f"{entry.line_count} in scripts/check_file_size.py."
            )

    if not result.failures:
        roots = " and ".join(SRC_ROOTS)
        print(
            f"OK: no file under {roots} exceeds its ratchet baseline "
            f"(cap {max_lines} for new files)."
        )
        return

    print(f"FAIL: {len(result.failures)} file(s) break the size ratchet (#231):")
    for entry in result.failures:
        if entry.kind == "new":
            print(f"  {entry.rel}:{entry.line_count} (new file exceeds cap {entry.limit})")
        elif entry.kind == "regressed":
            print(
                f"  {entry.rel}:{entry.line_count} (regressed past recorded baseline {entry.limit})"
            )
        else:
            print(
                f"  {entry.rel} (stale RATCHET_BASELINE entry: recorded {entry.limit} lines "
                "but this path no longer exists; remove it from RATCHET_BASELINE in "
                "scripts/check_file_size.py)"
            )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Return 0 if the ratchet holds, 1 on any failure."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max",
        type=int,
        default=DEFAULT_MAX_LINES,
        help=f"Cap for non-baselined files (default: {DEFAULT_MAX_LINES}).",
    )
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    result = check_file_size(
        [repo_root / root for root in SRC_ROOTS],
        rel_to=repo_root,
        max_lines=args.max,
    )
    _print_report(result, args.max)
    return 1 if result.failures else 0


if __name__ == "__main__":
    sys.exit(main())
