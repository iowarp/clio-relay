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
    # process_containment.py split iowarp/clio-relay#231: the facade is now
    # 197 lines with the implementation moved into fifteen
    # process_containment_*.py owner modules (mirrored here byte-identical
    # to src/clio_relay, per the isolated-runtime-mirror test), so this
    # entry is removed rather than ratcheted down.
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
    # 2026-08-19 (+23, justified; coordinator-approved): #257 staged
    # whole-generation activation -- three extraction rounds moved 88% of
    # the growth into bootstrap_full_activation_staging.py; the residual
    # is dependency-free heredoc that cannot leave the renderer (same
    # shape as the mcp_call/runner.py +28 precedent).
    # #255: -7442 net lines -- the renderer decomposition. Pure-constant/
    # embedded-source blocks move to bootstrap_constants.py,
    # bootstrap_staged_provider_source.py, bootstrap_receipt_classifier_
    # source.py, bootstrap_preparing_root_source.py,
    # bootstrap_pinned_copy_sources.py, bootstrap_candidate_uv_install_
    # source.py, and bootstrap_worker_proof_source.py (pure data, no
    # bootstrap.py dependency, so every owner and bootstrap.py's own
    # re-export both import them directly -- no circular import). The two
    # ~2000/2600-line rendered-script bodies (_relay_only_reconcile_script,
    # render_linux_user_bootstrap_script) are split into four/five
    # sequential fragment functions each (bootstrap_reconcile_script_*.py,
    # bootstrap_script_*.py); the outer wrapper functions stay resident and
    # unchanged in shape (signature/docstring/setup), concatenating the
    # fragments -- verified byte-identical against the pre-split renderer
    # for representative inputs. bootstrap_worker_fence_script.py takes the
    # pure-renderer worker-fence pair (no monkeypatch dependency).
    # bootstrap_ssh_deploy.py takes _bootstrap_preflight_over_ssh and
    # bootstrap_cluster_over_ssh (both monkeypatch TARGETS); every call
    # site inside them that reaches a collaborator the test suite
    # monkeypatches at the bootstrap.py facade (`_run`,
    # `create_bootstrap_archive`, `render_linux_user_bootstrap_script`,
    # `_verify_persistent_bootstrap_receipt`, `_validate_relay_bootstrap_
    # wheel`, `uuid4`, `BootstrapPreflightResult`) or that simply still
    # lives there (`bootstrap_relay_identity`, `_bootstrap_desired_state`,
    # `_is_clio_relay_git_checkout`, `_sha256_regular_file`,
    # `_validate_ssh_destination`, `_remaining_public_deadline`) is
    # rewritten to a qualified, call-time `bootstrap.<name>(...)` lookup
    # (the same forwarder idiom cli.py's R8(ii) decomposition established)
    # instead of a bare/early-bound reference, so
    # `monkeypatch.setattr(bootstrap, "X", ...)` in the existing test suite
    # keeps reaching the real call site. bootstrap_frp_local_install.py
    # takes the local (Windows) frp installer family the same way --
    # `_install_frp_from_release_archive`/`_assert_frp_pair` are
    # string-path monkeypatch targets
    # (`monkeypatch.setattr("clio_relay.bootstrap.X", ...)`), so their
    # call sites inside `install_local_frp` are qualified too; `platform`
    # and `shutil` stay imported in bootstrap.py itself (unused in its own
    # body now) purely because tests patch `clio_relay.bootstrap.platform.*`
    # / `bootstrap.shutil.which` by that exact string path, and both are
    # the same singleton module object every importer shares, so the
    # patch is observed regardless of which file's code actually calls it.
    # bootstrap.py is now 925 lines, an assembly over ~18 new owner
    # modules plus the pre-existing bootstrap_journal.py/bootstrap_
    # reconcile.py/bootstrap_recovery.py/bootstrap_jarvis_staging.py/
    # bootstrap_receipt_validation.py/bootstrap_pin.py/bootstrap_full_
    # activation_staging.py/bootstrap_acceptance.py/bootstrap_provider_
    # build_info.py family -- still above DEFAULT_MAX_LINES (800), so the
    # entry stays, ratcheted down from 8379.
    "src/clio_relay/bootstrap.py": 925,
    # #158 journal hardening (site-prefix walk + cross-call swap refusal): 1534
    # measured; restored after a merge-resolution slip dropped the entry.
    "src/clio_relay/bootstrap_journal.py": 1534,
    # #158: +6 net lines -- the receipt binds that activation evidence was
    # recorded, rather than equating the activated digest with the packaged
    # source digest, which JARVIS's normalization makes legitimately unequal.
    # 2026-08-19 (+21, justified): the receipt-verification gates in
    # resolve_receipt_bound_jarvis_python now DEFER under dev mode loudly
    # instead of crash-looping a hand-deployed worker (the un-deferred
    # execution_runtime_verified check restarted the ares worker every ~10s;
    # #250 family).
    # #255 split/bootstrap-reconcile: bootstrap_reconcile.py becomes a thin
    # 184-line facade (schema/constant + every public and private symbol
    # re-exported verbatim under its original name) over seventeen new owner
    # modules -- bootstrap_reconcile_constants.py (33), _primitives.py (351,
    # the acyclic filesystem/identity base every other owner imports from),
    # _models.py (272), _transaction.py (291), _locks.py (122),
    # _execution_identity.py (264), _readiness.py (111),
    # _activation_paths.py (548 -- above the 150-500 sweet spot: it owns
    # _verify_stable_symlink, verified through by generation inspection,
    # JARVIS wrapper binding, repository reconciliation, and reconcile
    # planning alike, so splitting it further would mean rewriting that
    # shared primitive's home, not moving it), _inspection.py (333),
    # _jarvis_wrapper_binding.py (380), _generation_staging.py (325),
    # _replacement_provider.py (245), _planning.py (409 -- one function,
    # plan_bootstrap_reconcile, deliberately kept alone since a further cut
    # would mean rewriting its body), _planning_support.py (242),
    # _receipt.py (358), _builtin_repos.py (213), _repository.py (443).
    # Under DEFAULT_MAX_LINES -- entry removed per ground rule 5.
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
    # #231: -201 net lines -- provenance_primitives.py (clio-relay#231) is
    # extracted first as the shared owner for JSON/type primitives, the
    # ProvenanceError/GitHubNotFound exceptions, the GitHubJsonFetcher
    # protocol + _github_fetcher, and the release policy constants every
    # other ci_validation owner module depends on.
    # #231: -206 net lines -- payload_policy.py becomes the owner for
    # archive-member filename/size policy (tag/candidate/tag-binding/
    # promotion payload name validators + limits) and the SHA256SUMS
    # checksum-manifest read/write pair.
    # #231: -405 net lines -- distribution_archive.py becomes the owner for
    # safe, bounded wheel/sdist inspection (ZIP/tar member reads, path
    # safety, ZIP64/central-directory preflight, core-metadata identity
    # binding) assembled into build_distribution_archive_receipt.
    # #231: -556 net lines -- branch_protection.py becomes the owner for the
    # repository governance receipt lifecycle (build/verify/fetch-live/
    # verify-live) and the raw branch/tag/environment/immutable-releases
    # protection-receipt builders it assembles from.
    # #231: -236 net lines -- release_identity.py becomes the owner for
    # resolving/verifying a live GitHub release's identity and gating
    # persistent mutations on protected main/tag/governance/release state.
    # #231: -440 net lines -- candidate_provenance.py becomes the owner for
    # the pre-tag receipt chain: sealing a merge-queue candidate build (one
    # build + six matrix-report validations, incl. the complementary
    # POSIX/Windows platform-marked-test partition proof) and binding a
    # protected release tag to that tested tree via its merged pull request.
    # #231: -231 net lines -- ci_run_status.py becomes the owner for CI run
    # and job identity: selecting the sole successful merge-queue ci.yml run
    # for a commit, and building/verifying the CI status receipt that binds
    # it to the already-sealed candidate build and tag binding.
    # #231: ci_validation.py is now under DEFAULT_MAX_LINES (701 lines, an
    # assembly/facade only -- re-exports + the argparse CLI -- after
    # provenance_primitives.py/payload_policy.py/distribution_archive.py/
    # branch_protection.py/release_identity.py/candidate_provenance.py/
    # ci_run_status.py/actions_artifact.py/release_assets.py each took one
    # owner concern). Entry removed per ground rule 5 -- ratchet down.
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
    # #231 cli.py decomposition: doctor/live-test top-level command-module
    # extraction (cli_diagnostics.py, 452 lines): -348 net lines.
    # #231 cli.py decomposition: init/install-frp top-level command-module
    # extraction (cli_init.py, 87 lines): -31 net lines.
    # #231 cli.py decomposition: installation-write-receipt/installation-info/
    # bootstrap-inspect top-level command-module extraction
    # (cli_installation_receipt.py, 446 lines): -346 net lines.
    # #231 cli.py decomposition: jarvis-mcp top-level command-group extraction
    # -- the thin command layer only (jarvis-runtime-authority/mcp-call/
    # jarvis-mcp-call/jarvis-mcp-refresh/mcp-server into cli_jarvis_mcp.py,
    # 662 lines; jarvis-mcp-validate alone into cli_jarvis_mcp_validate.py,
    # 538 lines, since combined they would exceed the 800-line cap). The
    # ~2,450-line JARVIS execution-query engine these commands call stays
    # cli.py-resident (unsequenced future work, see cli_jarvis_mcp.py's own
    # docstring): -1004 net lines.
    # #231 cli.py decomposition: shared-plumbing relocation pass --
    # _managed_queue_from_env/_submit_managed_job/_json_object/
    # _json_text_from_option/_environment_references/_artifact_use_refs/
    # _artifact_use_cli_value/_artifact_use_idempotency_suffix real bodies
    # moved to cli_support.py, cli.py keeps each as a thin forwarder under
    # its original name: -50 net lines.
    # #231 cli.py decomposition: remote_mcp_app extraction -- the register/
    # unregister/list/reload/refresh command layer plus its exclusive cache
    # helpers into cli_remote_mcp.py (557 lines), remote-mcp-validate's
    # thin command body into cli_remote_mcp_validate.py (402 lines), and the
    # ~780-line spack-configuration validation engine it drives into the new
    # real owner module remote_mcp_validation.py. cli.py keeps only the
    # shared discovery/artifact-reading helpers still used by the resident
    # JARVIS execution-query engine: -1464 net lines.
    "src/clio_relay/cli.py": 9679,
    # #231 R5: +16 net lines -- FrpTransportConfig gains proxy_name +
    # identity_anchor (the §8.3 typed opt-in frp_transport.py's build_transport
    # refusal reads) plus the IdentityAnchor type alias and its docstring. No
    # deletion offsets it: these are two new, real config fields, not a fixable
    # regression.
    # split/cluster-config-w2: cluster_config.py is now a 127-line facade
    # (assembly/re-exports + default_registry_path only) over eight owner
    # modules -- cluster_config_models.py (Pydantic schema),
    # cluster_config_registry.py (ClusterRegistry + cluster_route_revision),
    # cluster_config_io.py (bounded configuration reads), and the four-module
    # Windows-ACL split (cluster_config_windows_primitives.py,
    # cluster_config_windows_acl.py, cluster_config_windows_paths.py,
    # cluster_config_windows_guard.py). Comfortably under DEFAULT_MAX_LINES
    # with no baseline entry needed. Entry removed per ground rule 5 --
    # ratchet down.
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
    # #231 endpoint split, slice 1: -90 net lines -- the three filesystem-
    # identity dataclasses (`_PackageProgressLogState`, `_RuntimeSidecarAnchor`,
    # `_RecoveryDirectoryAnchor`) plus every progress/runtime-sidecar byte-
    # budget constant and the Windows kernel32 constant set move to the new
    # leaf owner module `endpoint_sidecar_types.py` (160 lines, under the
    # default cap). Both `EndpointWorker` and the ~90 still-co-resident
    # module-level helper functions now import these forward from the new
    # module by the same names, so every existing `endpoint.<name>` access
    # and monkeypatch target keeps resolving unchanged. Net: 8743 -> 8653.
    # #231 endpoint split, slice 2: -63 net lines -- the package-progress-log
    # path/identity primitives (_progress_log_identity,
    # _normalize_package_progress_log_path, _validated_native_subprocess_cwd,
    # _render_progress_log_identity, _open_package_progress_log) move to the
    # new leaf owner module `endpoint_progress_log_io.py` (94 lines). Net:
    # 8653 -> 8590.
    # #231 endpoint split, slice 3: -143 net lines -- the runtime-sidecar
    # filesystem-anchor lifecycle (_runtime_sidecar_anchor,
    # _runtime_sidecar_anchor_from_metadata, _validate_runtime_sidecar_stat,
    # _precreate_runtime_sidecar, _open_owned_sidecar) moves to the new owner
    # module `endpoint_runtime_sidecar_anchor.py` (183 lines). Net:
    # 8590 -> 8447.
    # #231 endpoint split, slice 4: -339 net lines -- the Windows ctypes
    # sidecar-handle primitives (_open_windows_cleanup_handle,
    # _windows_handle_information, _mark_windows_handle_for_rename,
    # _close_windows_cleanup_handle, _validate_windows_sidecar_handle,
    # _quarantine_windows_sidecar_by_handle, _remove_execution_sidecars_
    # windows) move to the new owner module
    # `endpoint_windows_sidecar_handles.py` (387 lines). The one function that
    # would otherwise create a cycle between this module and the still-
    # co-resident execution-sidecar cleanup orchestration,
    # _execution_sidecar_quarantine_name (a pure function of one anchor, no
    # cleanup state), relocates to `endpoint_runtime_sidecar_anchor.py`
    # instead (183 -> 210 lines there) -- both windows-handles and the still-
    # co-resident orchestration depend on it from that one leaf, and neither
    # depends on the other. Net: 8447 -> 8108.
    # #231 endpoint split, slice 5: -504 net lines -- the private JARVIS
    # execution-recovery directory lifecycle (timestamp/process-identity
    # validation, directory-anchor build/restore/validate,
    # open-or-create/close/revalidate, and the bounded recovery-result read/
    # remove primitives) plus the generic private-JSON-file serialize/
    # atomic-write primitive it's written through move to the new owner
    # module `endpoint_recovery_directory.py` (577 lines -- above the 150-500
    # sweet spot but under the 800 real-seam-split threshold; the module's
    # own docstring documents why its three internal layers stay one module
    # rather than a forced cut). Four functions with no remaining endpoint.py
    # call site (_private_json_payload, _recovery_directory_anchor_from_
    # metadata, _recovery_directory_anchor_from_stat, _validate_recovery_
    # directory_stat) dropped out of endpoint.py's forward import entirely;
    # no test referenced them directly, so no test re-pointing was needed.
    # Net: 8108 -> 7604.
    # #231 endpoint split, slice 6: -469 net lines -- JARVIS execution-
    # recovery dispatch trust and orchestration (_trusted_jarvis_mcp_route
    # and every function that calls it: intent build/validate, pending
    # check, result-identity/trust checks, dispatch-refusal attribution and
    # rendering, runtime-recovery-state restore, execution-query
    # attestation validation) plus the MCP runner environment/command
    # construction move to the new owner module
    # `endpoint_jarvis_recovery.py` (553 lines). Every moved function still
    # has a direct `EndpointWorker` call site, so all eleven stay forward-
    # imported into endpoint.py under their original names; six collateral
    # imports (jarvis_mcp_command, jarvis_cd_lock_binding_expectation,
    # jarvis_mcp_server_artifact_binding_verified,
    # remote_mcp_server_artifact_binding_verified, jarvis_dispatch_refusal,
    # REGISTERED_JARVIS_EXECUTION_CONTRACTS) and MCP_RUNNER_BASE_ENV_NAMES
    # drop out of endpoint.py's own imports entirely once their only
    # remaining callers moved with the functions that used them.
    # test_endpoint.py's and test_jarvis_execution_recovery_guards.py's
    # `monkeypatch.setattr(endpoint_module, "jarvis_mcp_command", ...)` /
    # `endpoint_module.jarvis_cd_lock_binding_expectation()` sites (24 call
    # sites total) re-point to the new module -- the internal call from
    # `_trusted_jarvis_mcp_route` now resolves in
    # `endpoint_jarvis_recovery`'s own globals, not endpoint.py's, so the
    # old patch target went dead. Net: 7604 -> 7135.
    # #231 endpoint split, slice 7: -372 net lines -- cross-platform
    # execution-sidecar quarantine orchestration (the durable cleanup-plan/
    # quarantine-path-restore/acknowledgment builders, the Linux
    # renameat2(RENAME_NOREPLACE) primitive, the cross-platform
    # _remove_execution_sidecars orchestrator, and anchor-descriptor
    # release) moves to the new owner module
    # `endpoint_execution_sidecar_cleanup.py` (426 lines). Only
    # `_remove_execution_sidecars` keeps a remaining `EndpointWorker` call
    # site; the other five functions (plus `_execution_sidecar_quarantine_
    # name`/`_validate_runtime_sidecar_stat` on the already-extracted
    # `endpoint_runtime_sidecar_anchor.py`, and
    # `_remove_execution_sidecars_windows` on `endpoint_windows_sidecar_
    # handles.py`) drop out of endpoint.py's own imports entirely, taking
    # the now-dead `ctypes`/`errno`/`stat` imports and two schema constants
    # with them. test_endpoint.py's seven direct
    # `_execution_sidecar_quarantine_name` calls and one `_rename_noreplace_
    # at` monkeypatch/direct-call pair re-point to the new modules per the
    # same rule slice 3/4/6 established. Net: 7135 -> 6763.
    # #231 endpoint split, slice 8: -447 net lines -- package-progress
    # observation trust (bounded sidecar-record reading/checkpointing plus
    # provider/MCP-bridge/native-HMAC notification cross-checks) moves to
    # the new owner module `endpoint_progress_trust.py` (505 lines --
    # slightly above the 150-500 sweet spot; the module's docstring
    # documents why its two layers stay together). Only
    # `_normalized_provider_distribution` has no remaining `EndpointWorker`
    # call site (its only caller, `_trusted_mcp_progress_metadata`, moved
    # with it); no test referenced it directly. Net: 6763 -> 6316.
    # #231 endpoint split, slice 9: -501 net lines -- the ENTIRE remaining
    # module-level function tail (everything after `EndpointWorker`) moves
    # out, split along its last real seam: worker-environment identity +
    # scheduler-naming/status normalization + bounded coercion helpers
    # (`endpoint_worker_environment.py`, 330 lines) and durable job/task/
    # runtime-metadata classification predicates
    # (`endpoint_scheduler_metadata.py`, 292 lines). endpoint.py is now
    # exactly `EndpointWorker` (plus its module-scope constants/imports) --
    # the assembly the doc's own inventory named as the file's largest
    # single concern, still unsequenced there. `bootstrap_cluster_
    # environment` (a public, non-underscore name) has zero remaining
    # callers anywhere in the repository (verified by a full-tree grep
    # before the move, not just endpoint.py) and drops out of endpoint.py's
    # forward import entirely -- preserved as-is per this campaign's scope
    # (decomposition, not dead-code removal); `_scheduler_name_from_document`
    # similarly has no remaining `EndpointWorker` call site (only its
    # recursive self-calls and its one caller, `_scheduler_name_from_yaml`,
    # which moved with it). No test referenced either directly. Net:
    # 6316 -> 5815.
    # clio-relay#259 (merged into this split's facade): the console log
    # stream's wiring into the job-run method -- a per-job ConsoleLiveTailer
    # local, the _wrap_poll/_tail_console_stream pair composing the #259 tail
    # step onto the existing on_poll cadence without touching
    # _poll_running_job's own body, the console.log artifact append alongside
    # stdout/stderr, and _flush_terminal_console plus its console_tailer
    # thread-through in _append_optional_result_artifacts /
    # _append_spool_artifact_once. The bulk of the new logic (resolution,
    # tailing, terminal flush) lives in the new owner module
    # console_stream.py, not here -- this is glue only, landing on top of the
    # already-decomposed EndpointWorker facade rather than the pre-split
    # 8843-line monolith develop's history describes. A justified,
    # minimal ratchet-up.
    "src/clio_relay/endpoint.py": 5915,
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
    # installation.py's own ratchet-baseline entry and history comment were
    # removed here (iowarp/clio-relay#231 split/installation): the file is
    # now 409 lines (an assembly/facade over its owner modules --
    # distribution_source_identity.py, installation_receipt_models.py,
    # native_jarvis_contract.py, persistent_uv_tool_probe.py,
    # python_distribution_probe.py, wheel_record_closure.py,
    # component_runtime_identity.py, component_verification_remote.py,
    # worker_runtime_verification.py), comfortably under DEFAULT_MAX_LINES
    # with no baseline entry needed.
    "src/clio_relay/jarvis_execution.py": 875,
    "src/clio_relay/jarvis_mcp.py": 947,
    # jarvis_mcp_validation.py's own ratchet-baseline entry and history
    # comment (most recently: #231 R6-fix review, A6, the `expected_filters`
    # `content_max_bytes` fix) were removed here (split/jarvis-mcp-validation):
    # the file is now 38 lines (a facade only -- two re-exports) after its
    # evidence-building logic moved to eight owner modules --
    # jarvis_mcp_validation_core.py (JSON/type primitives), _contract.py
    # (local/remote tool-contract validation), _package_search.py
    # (``jarvis_describe`` call evidence), _execution_query.py (post-run
    # ``jarvis_get_execution`` evidence), _progress_semantics.py (one native
    # progress event's semantics), _lifecycle_progress.py (execution-query
    # lifecycle/package-progress evidence, 482 lines, re-exported here under
    # its private name because tests call it directly via
    # ``jarvis_validation._jarvis_query_lifecycle_progress_evidence``),
    # _live_progress.py (``jarvis_run`` native progress-notification
    # evidence), and _report.py (``build_jarvis_mcp_validation_report``, 797
    # lines -- one indivisible orchestrating function, at the sweet-spot cap
    # like ``service_runtime_start.py``'s precedent) -- comfortably under
    # DEFAULT_MAX_LINES with no baseline entry needed.
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
    "src/clio_relay/live_acceptance.py": 868,
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
    # clio-relay#264: +9 net lines -- relay_list_artifacts/relay_read_artifact
    # were the only two artifact-facing tools missing the cluster/
    # route_revision routing every sibling job/artifact tool already has
    # (relay_status, relay_cancel, relay_artifact_lineage, relay_wait), so a
    # jarvis execution dispatched to a configured remote cluster always
    # answered not-found for its own registered artifacts. Both dispatch
    # bodies now resolve the caller's asserted cluster (the existing
    # _job_target) and delegate the local/remote/owned fetch mechanics to
    # the new sub-800 artifact_routing.py owner; two schema property blocks
    # (cluster/route_revision + dependentRequired, matching relay_artifact_
    # lineage's shape) account for the added lines, offset by deleting the
    # now-dead local-only _read_model_artifact_bytes. A justified, minimal
    # ratchet-up: 6098 -> 6107.
    "src/clio_relay/mcp_server.py": 6107,
    # #231 R9 fix round 3: +16 lines keep subprocess stderr out of the marked
    # timeout message and log its bounded diagnostic once server-side.
    "src/clio_relay/mcp_stdio_validation.py": 1285,
    # split/models-w2 (#231): models.py's own ratchet-baseline entry and its
    # v3.6/v3.7 history comment are removed here: the file is now a
    # re-export facade (~170 lines, well under DEFAULT_MAX_LINES) over eleven
    # new domain owner modules -- models_shared.py (cross-domain constants +
    # canonical-JSON/identity helpers), models_enums.py (durable
    # state-machine enums), models_artifact_provenance.py (W3C-PROV artifact
    # use/transform records), models_jarvis_package.py (package input
    # contracts), models_jarvis_pipeline.py (pipeline staged-input lineage/
    # bindings/run manifests), models_job_specs.py (the JobSpec union and its
    # members), models_job.py (RelayJob plus its GC/closure lifecycle),
    # models_job_telemetry.py (task/event/progress/cursor/lease records),
    # models_mcp_admission.py (MCP control-query authority + SEP-2663 task
    # records), models_scheduling.py (endpoint + scheduler-cancellation +
    # scheduler/connector observation records), models_gateway.py (artifact
    # index + gateway/service-runtime records). Every one of the original
    # 111 module-level names is re-exported here under its original name, a
    # pure move verified by an exhaustive module-attribute diff against the
    # pre-split tree.
    # NOTE (integrate-w2 merge accounting): the models-w2 branch forked from
    # develop before split/process-containment-w2 landed, so its copy of this
    # dict still carried process_containment.py's pre-split entry ("src/
    # clio_relay/process_containment.py": 2678) immediately after this
    # comment. That entry is correctly omitted here -- split/process-
    # containment-w2 (merged into integrate-w2 first) already brought
    # process_containment.py under DEFAULT_MAX_LINES and removed its baseline
    # entry per ground rule 5; re-adding it would resurrect a stale, already-
    # superseded ratchet ceiling on a file this merge did not touch.
    # split/queue-management-w2: queue_management.py is now a 70-line
    # re-export facade (entry removed -- under DEFAULT_MAX_LINES). The
    # implementation moved to eight single-concern owner modules:
    # queue_diagnosis_constants.py, queue_worker_capacity.py,
    # queue_admission_snapshot.py, queue_listing.py,
    # queue_admission_simulation.py, queue_diagnosis.py,
    # queue_stale_recovery.py, queue_worker_status.py -- all under
    # DEFAULT_MAX_LINES, none needs a baseline entry either. (NOTE:
    # queue-management-w2 also forked before models-w2/process-containment-w2
    # landed and still carried their stale 2299/2678 entries here; omitted
    # for the same reason as above.)
    # queue_validation.py split (split/queue-validation-w2): the module is
    # now an assembly/facade only -- every real concern moved verbatim to
    # seven new owner modules (live_validation_constants.py/_support.py/
    # _process.py/_capacity.py/_jobs.py/_cleanup.py, plus
    # live_validation_orchestrator.py for the single
    # run_queue_management_validation entry point, 566 lines -- above the
    # 150-500 sweet spot but under the 800 cap, the same "one real,
    # undividable concern" precedent as endpoint_recovery_directory.py's
    # note in this file). The facade itself is 164 lines (re-exports only,
    # every original name kept so no importer changed), comfortably under
    # DEFAULT_MAX_LINES -- entry removed per this script's own documented
    # convention: "remove the entry once the file is under
    # DEFAULT_MAX_LINES". (NOTE: this branch also forked before models-w2/
    # process-containment-w2/queue-management-w2 landed and still carried
    # their stale 2299/2678/1671 entries plus its own now-superseded +11
    # net-lines ratchet-up note for the pre-split queue_validation.py=1546
    # entry; both omitted for the same reason as above -- the file this
    # merge integrates is the split's facade, not the pre-split module the
    # ratchet-up note described.)
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
    # #231 slice 1 (design doc §4.5/§5): the JSON/JSON-Schema validation
    # primitives (_validate_json_schema, _require_bounded_json_structure,
    # _require_finite_json, _bounded_diagnostic, _reject_nonfinite_json_constant,
    # _NonFiniteJsonError, _JsonSchemaInstanceValidator, the composed/flat
    # schema-key sets, the per-dialect validator map, and their three bound
    # constants) moved to the new remote_mcp_schema_validation.py (172 lines,
    # under DEFAULT_MAX_LINES -- no baseline entry needed). Private helpers
    # with no external callers, so remote_mcp.py imports them directly rather
    # than re-exporting. 5377 -> 5255.
    # #231 slice 2: RemoteMcpToolSchema, RemoteMcpDiscoveryProvenance,
    # is_remote_mcp_control_query, _parse_remote_tool, and the identity/
    # verification helpers (_is_sha256, _server_artifact_verified,
    # _immutable_remote_mcp_install_verified, _stable_digest) moved to the new
    # remote_mcp_tool_schema.py (222 lines, under DEFAULT_MAX_LINES). The
    # first three are re-exported under their original names (external
    # importers across several modules and tests); the rest are private with
    # no callers outside remote_mcp.py. 5255 -> 5099.
    # #231 slice 3: the release-acceptance evidence wire model cluster
    # (RemoteMcpCatalogIssue through RemoteMcpAcceptanceReport, 10 classes;
    # _acceptance_artifact_resource, _append_spack_transition_resources; the
    # two path-canonicalization primitives their validators call) moved to
    # the new remote_mcp_acceptance_models.py (769 lines, under
    # DEFAULT_MAX_LINES). Every model class remote_mcp.py still references
    # is re-exported via `from ... import`. Three of the four bound
    # Spack-configuration constants have no reader left in this file's own
    # body but cli.py imports them directly, so they are re-exported via
    # qualified assignment instead (`X = remote_mcp_acceptance_models.X`) --
    # ruff's unused-import check has no equivalent for a plain module-level
    # assignment, unlike the `from ... import` it kept stripping as dead
    # de facto proving those three names really are body-unused now. This
    # is why the net reduction (5099 -> 4445) is 16 lines short of a
    # forced-contiguous cut: the qualified-assignment block plus the
    # explanatory comments are new, real structure this re-export needs.
    # RemoteMcpSpackConfigurationComponentObservation has no importer at
    # all (confirmed by ruff F401 and grep), so it alone stays unexported.
    # The validator *functions* that build these reports
    # (build_remote_mcp_acceptance_report, the Spack/scientific-catalog
    # families) stay here -- design doc §4.5 names that cluster as needing
    # reordering, a separate future slice, not a contiguous cut alongside
    # the models.
    # #231 slice 4: the virtual-tool alias assignment/collision-resolution
    # cluster (_assign_aliases, _collision_alias, _bounded_base_alias,
    # _alias_with_suffix, _profile_allows, _safe_name, the compiled
    # _SAFE_NAME_PATTERN, and the two bound alias constants) moved to the
    # new remote_mcp_aliasing.py (122 lines, under DEFAULT_MAX_LINES). None
    # have a caller outside remote_mcp.py's own catalog-assembly code
    # (confirmed by grep), so no re-export is needed -- only
    # MAX_VIRTUAL_REMOTE_MCP_CANDIDATES is imported back (the
    # catalog-assembly candidate-limit check still reads it). 4445 -> 4376.
    # #231 slice 5: the local relay control envelope injection cluster
    # (inject_cluster_argument, virtual_schema_error,
    # remote_input_schema_requires_wrapper, _contains_document_root_reference,
    # _schema_identifier_keyword, _schema_establishes_embedded_resource,
    # _relocate_legacy_local_references, VIRTUAL_REMOTE_MCP_RELAY_CONTROL_SCHEMAS/
    # _FIELDS, MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES) moved to the new
    # remote_mcp_schema_wrapping.py (271 lines, under DEFAULT_MAX_LINES).
    # inject_cluster_argument and VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS are
    # re-exported (tests import the former; queue_tasks.py and this file's
    # own catalog-assembly body import/read the latter). The rest have no
    # caller outside remote_mcp.py's own body (confirmed by grep), so no
    # re-export. 4376 -> 4171.
    # #231 slice 6: the schema discovery cache (RemoteMcpSchemaCacheEntry,
    # RemoteMcpSchemaCache, _fsync_cache_directory, the digest/fingerprint
    # helpers, and cache_entry_from_discovery_artifact) moved to the new
    # remote_mcp_cache.py (428 lines, under DEFAULT_MAX_LINES). Eight of the
    # nine public names have a real reader elsewhere in this file's own
    # catalog-assembly/admission-resolution body (confirmed by grep), so
    # they are imported via a plain `from ... import`, which is also the
    # re-export cli.py/mcp_server.py/jarvis_mcp.py/jarvis_mcp_validation.py
    # rely on. remote_mcp_server_artifact_binding_verified has no reader
    # left in this file's own body -- only endpoint.py and
    # jarvis_service_runtime.py import it directly -- so it is re-exported
    # via qualified assignment instead. 4171 -> 3839.
    # #231 slice 7: the agent-facing JSON-Schema builder cluster
    # (cluster_route_revision_json_schema, VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA,
    # jarvis_service_runtime_handoff_json_schema, virtual_jarvis_job_output_schema)
    # moved to the new remote_mcp_wire_schemas.py (172 lines, under
    # DEFAULT_MAX_LINES). All four are re-exported -- mcp_server.py,
    # jarvis_mcp.py, jarvis_mcp_validation.py, and tests import them
    # directly. VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA and
    # virtual_jarvis_job_output_schema have a real local reader too
    # (VirtualRemoteMcpTool.definition), so they use a plain `from ...
    # import`; cluster_route_revision_json_schema and
    # jarvis_service_runtime_handoff_json_schema have no reader left in
    # this file's own body (both calls moved into the new module's own
    # definitions), so they are re-exported via qualified assignment
    # instead. virtual_jarvis_job_output_schema imports
    # CLIO_KIT_JARVIS_USER_TOOL_NAMES (a contract-pin constant still here)
    # at function scope, the proven idiom for the load-order circular
    # import a module-scope import back would create. 3839 -> 3728.
    "src/clio_relay/remote_mcp.py": 3728,
    # #231 R9 fix round 3: +7 lines keep Pydantic receipt validation detail
    # out of the public conflict while logging it once server-side.
    # #231 CQ18: +1 line -- purge_quarantined_tree_batch's real home moved to
    # queue_gc_storage.py; the combined `from clio_relay.core_queue import
    # ClioCoreQueue, purge_quarantined_tree_batch` splits into two single-name
    # import lines (CQ15 §10.7 precedent: retarget a moved-symbol import to
    # its real new owner, never a facade re-export).
    "src/clio_relay/retention.py": 952,
    # split/runtime-metadata-w2: runtime_metadata.py becomes a 113-line
    # assembly/facade only (re-exports, no logic) after its nine concerns --
    # schema/state vocabulary (runtime_metadata_types.py), the normalized
    # JarvisRuntimeMetadata document (runtime_metadata_core_model.py), loose
    # payload coercion (runtime_metadata_coercion.py), strict native field
    # validators (runtime_metadata_native_validators.py), the exact native
    # JARVIS document family + clio-kit projection
    # (runtime_metadata_native_documents.py), merge/lifecycle-regression
    # guards (runtime_metadata_merge.py), native-document normalization
    # (runtime_metadata_native_normalize.py), MCP-result/legacy compatibility
    # decoding (runtime_metadata_mcp_normalize.py), and the authenticated
    # sidecar record codec (runtime_metadata_sidecar.py) -- each its own
    # owner module, all comfortably under DEFAULT_MAX_LINES. Entry removed
    # per ground rule 5 -- ratchet down.
    "src/clio_relay/scheduler_providers.py": 1153,
    # #231 R10: the local owned-visitor render/write/spawn path now delegates
    # to frp_link.py, while the three remote frpc start/stop script generators
    # moved to the under-800-line frp_remote_scripts.py owner.  -772 net lines.
    # #231 CQ16 cross-file fix: +4 net lines -- _revoke_browser_attachment's
    # except clause now discriminates BrowserAttachmentIdentityConflictError
    # by type instead of a substring match on QueueConflictError (the banned
    # prose-match pattern). Campaign merge: 9391 base -5 (#242 gating)
    # +4 (CQ16) = 9390, the measured merged count.
    # #231 service-runtime split, slice 1: the zero-dependency primitives
    # (untyped-dict coercion helpers, two small connector-config validators,
    # the cleanup-resource gateway binder, and the just-started-process-group
    # rollback helper) moved to the new service_runtime_primitives.py (96
    # lines). Every internal call site was requalified to `_primitives.<name>`
    # so the existing `monkeypatch.setattr(service_runtime, ...)` tests that
    # target these names keep failing loudly instead of silently no-op'ing;
    # the one test that patched `_terminate_just_started_process_group`
    # in-place was repointed to the new module. Net -46 lines even though 49
    # lines moved out: qualifying ~220 call sites pushed several lines past
    # the 100-col limit and `ruff format` reflowed them across more lines.
    # 9390 -> 9344.
    # #231 service-runtime split, slice 2: the ten zero-dependency wire/result
    # types and the CommandRunner protocol (three narrow RelayError
    # subclasses, five frozen dataclasses, the Protocol) moved to the new
    # service_runtime_types.py (161 lines). 9344 -> 9211.
    # #231 service-runtime split, slice 3: the mutually-coupled
    # scheduler-submission-parsing + gateway-intent + completed-resource
    # validation cluster moved to the new service_runtime_scheduler_contracts.py
    # (800 lines, at the cap -- two sibling concerns that call back into each
    # other, documented in the module docstring rather than force-split).
    # 9211 -> 8502.
    # #231 service-runtime split, slice 4: the local desktop-connector
    # process discovery/identity/signaling cluster (POSIX pidfd primitives +
    # Windows CIM enumeration) moved to the new
    # service_runtime_connector_identity.py (682 lines). 8502 -> 7866.
    # #231 service-runtime split, slice 5: the concrete SubprocessCommandRunner
    # (CommandRunner protocol default implementation) plus its stdin-delivery
    # helper moved to the new service_runtime_command_runner.py (154 lines).
    # 7866 -> 7740.
    # #231 service-runtime split, slice 6: the absolute-deadline bounded HTTP
    # readiness reader plus loopback-port and browser-attachment-support
    # helpers moved to the new service_runtime_readiness.py (242 lines).
    # 7740 -> 7537.
    # #231 service-runtime split, slice 7: the SSH-delivered embedded
    # shell/Python script generators for scheduler-submission tracking
    # (reserve/capture/verify one exact submission through a durable,
    # race-safe sidecar) moved to the new
    # service_runtime_submission_scripts.py (627 lines). 7537 -> 6948.
    # #231 service-runtime split, slice 8: the SSH-delivered embedded
    # shell/Python script generators for the scheduler-allocation
    # connector-step lifecycle (step status/cancel/reconcile, remote HTTP
    # health probe, connector discovery/status by durable identity sidecar)
    # moved to the new service_runtime_connector_step_scripts.py (448
    # lines). This closes out the module-level function extractions named
    # in the concern inventory; service_runtime.py is now imports + module
    # constants + the ServiceRuntimeSupervisor class only. 6948 -> 6523.
    # #231 service-runtime split, slice 9: the three frozen outcome
    # dataclasses (ServiceRuntimeStartResult/ServiceRuntimePendingResult/
    # ServiceRuntimeStopResult) plus their to_live_validation_report
    # conversions and the ten RUNTIME_*_CHECK_ID identifiers moved to the
    # new service_runtime_results.py (722 lines); re-exported here under a
    # `# noqa: F401` header since cli.py/mcp_server.py/live_acceptance.py
    # bare-import them and cli.py is out of this split's scope to edit.
    # This was the last extractable module-level content -- everything
    # remaining is imports, module constants, and the
    # ServiceRuntimeSupervisor class itself. 6523 -> 5840.
    # #231 service-runtime split, slice 10 (class-mixin split begins): the
    # ServiceRuntimeSupervisor.__init__ construction, the per-gateway
    # cross-process transition lock, durable-session update helpers, the
    # shared SSH transport, JARVIS authorization resolution, and the two
    # durable-failure recorders moved to the new
    # service_runtime_core.py (282 lines) as `_ServiceRuntimeCoreMixin` --
    # the first slice of the class body itself (module-level content is
    # exhausted; from here every slice peels one mixin off
    # ServiceRuntimeSupervisor, which now derives from the mixin as its
    # first base). 5840 -> 5613.
    # #231 service-runtime split, slice 11: the start/resume-start state
    # machine (start, resume_start, _resume_start_locked,
    # _complete_runtime_start_locked, the connector reuse/launch/recovery
    # predicates, _ready_start_result, _rollback_runtime_start) moved to the
    # new service_runtime_start.py (796 lines, at the sweet-spot cap -- one
    # cohesive state machine, documented in the module docstring rather than
    # force-split) as `_ServiceRuntimeStartMixin`. The shared
    # _RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS constant (used by this
    # mixin plus the not-yet-extracted jarvis-bind and browser clusters)
    # moved to service_runtime_readiness.py, which every caller already
    # imports, rather than being duplicated three times. 5613 -> 4873.
    # #231 service-runtime split, slice 12: the JARVIS-bound runtime binding
    # cluster (bind_verified_jarvis_runtime, its identity/policy helpers,
    # _validate_jarvis_binding_session, _resume_jarvis_binding_locked,
    # _jarvis_connector_start_intent, _rollback_jarvis_binding, plus the two
    # schema constants that move with their only callers) moved to the new
    # service_runtime_jarvis_bind.py (768 lines, at the sweet-spot cap -- one
    # cohesive state machine, documented in the module docstring rather than
    # force-split) as `_ServiceRuntimeJarvisBindMixin`. 4873 -> 4169.
    # #231 service-runtime split, slice 13: the browser sandbox attach/detach
    # cluster (browser_attach, browser_detach, their serialized
    # implementations, the shared _revoke_browser_attachment revocation, and
    # _revoke_browser_for_runtime_cleanup) moved to the new
    # service_runtime_browser.py (465 lines) as `_ServiceRuntimeBrowserMixin`.
    # 4169 -> 3749.
    # #231 service-runtime split, slice 14: the teardown (stop) cluster --
    # stop, _stop_serialized, and the teardown-policy quartet it exclusively
    # calls (_prepare_teardown_intent, _prepare_teardown_policy,
    # _validate_teardown_policy, _completed_teardown_result), pulled together
    # from two non-adjacent spans since the quartet sits physically after the
    # detach/attach cluster -- moved to the new service_runtime_stop.py (688
    # lines) as `_ServiceRuntimeStopMixin`. The two teardown schema constants
    # move with their only callers. 3749 -> 3107.
    # #231 service-runtime split, slice 15: desktop-connector-only detach --
    # detach, _detach_serialized, _prepare_detach_intent,
    # _completed_detach_result, _consume_completed_detach_for_attach, and the
    # three resumability predicates (interleaved with the intent helpers in
    # the original source since they are one concern: what a detached
    # generation proves and who may resume it) -- moved to the new
    # service_runtime_detach.py (564 lines) as `_ServiceRuntimeDetachMixin`.
    # The attach mixin calls back into this module's predicates via `self`.
    # 3107 -> 2596.
    # #231 service-runtime split, slice 16: desktop-connector reattachment --
    # attach, _attach_serialized -- moved to the new service_runtime_attach.py
    # (353 lines) as `_ServiceRuntimeAttachMixin`. It calls back into the
    # detach mixin's resumability predicates via `self`. 2596 -> 2292.
    # #231 service-runtime split, slice 17: ownership-intent reconciliation --
    # the crash-recovery core _reconcile_ownership_intents (recovers
    # scheduler submission and connector identities written before a hard
    # exit by consulting each durable intent's SSH-observed sidecar),
    # _reconcile_allocation_connector_intent, the two identity-binding
    # validators it calls, _connector_records_match, and
    # _local_connector_intent -- moved to the new
    # service_runtime_reconciliation.py (732 lines) as
    # `_ServiceRuntimeReconciliationMixin`. 2292 -> 1614.
    # #231 service-runtime split, slice 18: scheduler/runtime observation and
    # verification -- _verified_scheduler_submission,
    # _quiesced_owner_source_recovery_is_authorized,
    # _observe_allocation_and_health_once (the single-shot, never-blocking
    # observation core) and its _record_runtime_observation_pending
    # persister, _retained_scheduler_resource, and the scheduler-polling
    # primitives -- moved to the new service_runtime_observation.py (673
    # lines) as `_ServiceRuntimeObservationMixin`. Slice 18b: the shared
    # desktop-connector stop primitive _stop_local_connector (called from
    # five other mixins) and its _remove_unpublished_local_connector_files
    # cleanup split out separately into service_runtime_local_connector.py
    # (171 lines) as `_ServiceRuntimeLocalConnectorMixin`, since bundling it
    # with observation would have crossed the 800-line cap. 1614 -> 854.
    # #231 service-runtime split, slice 19 (final): the last two clusters --
    # remote/allocation connector lifecycle (_start_remote_connector,
    # _allocation_connector_identity, _poll_allocation_connector_step,
    # _stop_allocation_connector, _retained_allocation_connector_resource)
    # moved to the new service_runtime_remote_connector.py (497 lines) as
    # `_ServiceRuntimeRemoteConnectorMixin`; local process start + HTTP
    # health waits (_start_local_visitor, _start_browser_proxy,
    # _wait_for_jarvis_health, _wait_for_browser_health,
    # _wait_for_local_health) moved to the new service_runtime_local_start.py
    # (376 lines) as `_ServiceRuntimeLocalStartMixin`. service_runtime.py is
    # now assembly-only: imports, the mixin composition list, and the class
    # docstring recording it. 854 -> 78. #231 CLOSED for this file.
    "src/clio_relay/service_runtime.py": 78,
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
    # split/session-lifecycle rework (#231, slices A-K): session_lifecycle.py
    # was 7840 lines at the start of this rework. Slices A-J moved every
    # bare-imported (cli.py) concern out to owner modules one at a time,
    # each ratcheted down here in turn; slice K then found that the two
    # remaining resident clusters (inspect_owned_session_recovery_status +
    # its private helper, and the SSH-remote-orchestration group) reach
    # session_lifecycle only through cli.py's MODULE-QUALIFIED attribute
    # access (`session_lifecycle.inspect_owned_session_recovery_status(...)`),
    # not a bare import -- and Python resolves a re-exported name off a
    # module identically to one defined there, so the same
    # cli.py-compatibility re-export trick used for every bare-imported name
    # in slices A-J also covers qualified access. Slice K moved
    # inspect_owned_session_recovery_status (self-contained: no other
    # resident function was its consumer or dependency) to the new
    # session_recovery_inspection.py owner and re-exported it the same way.
    # 3461 -> 1357 -> 582 lines, back under the 800-line default cap --
    # session_lifecycle.py graduates out of RATCHET_BASELINE. The remaining
    # SSH-remote-orchestration cluster (start_remote_session,
    # status_remote_session, start_remote_session_durable,
    # teardown_remote_session, finalize_remote_session_cleanup_report,
    # read_remote_session_cleanup_report, detach_remote_session,
    # publish_owned_session_api_startup_receipt) and cli.py's own
    # compatibility re-export block are what remain; full slice-by-slice
    # detail lives in the split/session-lifecycle branch history.
    # split/session-lifecycle slice J (#231): execute_owned_session_start
    # alone is ~910 lines of crash-recovery start logic (systemd containment,
    # broker handoff, resumable-attempt promotion) that does not decompose
    # along a clean second seam without restructuring the function itself --
    # out of scope for a mechanical extraction slice. Matches the
    # queue_management.py/queue_validation.py precedent of a ratcheted,
    # justified new-file cap above the 800-line default.
    "src/clio_relay/session_start_execution.py": 1190,
    # split/session-lifecycle slice J (#231): the failed-start teardown path
    # (_execute_owned_failed_start_teardown, 243 lines) plus
    # execute_owned_session_teardown (342 lines) and their three small
    # private helpers form one cohesive, already-minimal cluster; splitting
    # execute_owned_session_teardown itself out of its own helper cluster
    # would separate functions that only ever call each other. 22 lines over
    # the 800 default.
    "src/clio_relay/session_cleanup_execution.py": 822,
    # split/session-lifecycle slice K (#231): inspect_owned_session_recovery_
    # status is the single dominant read path every recovery/start/teardown
    # decision in the split verifies against -- durable metadata, process
    # identity, cluster-registry, and core-admission agreement all have to be
    # read and cross-checked in one place. It does not decompose along a
    # clean second seam without restructuring the function itself, out of
    # scope for a mechanical extraction slice. Matches the
    # queue_management.py/queue_validation.py precedent of a ratcheted,
    # justified new-file cap above the 800-line default.
    "src/clio_relay/session_recovery_inspection.py": 838,
    # session_lifecycle.py itself has no RATCHET_BASELINE entry here: the
    # split above already took it under the 800-line default cap (582 lines).
    # develop (pre-split) still carries its own "session_lifecycle.py": 7840
    # baseline for the monolith this facade replaces -- that number describes
    # a file that no longer exists on this branch, so it is dropped rather
    # than resurrected; nothing it protected needed porting (the split's own
    # accounting already covers session_lifecycle.py's real content).
    # clio-relay#259: LOG_STREAM_NAMES/LogStreamName widened the job log-stream
    # vocabulary from {stdout, stderr} to {stdout, stderr, console} in place
    # (Literal pins at append_log/read_log/mark_truncation_event_recorded plus
    # the capture-state loops and validator), and added append_console for
    # symmetry with append_stdout/append_stderr. A justified, minimal
    # ratchet-up.
    "src/clio_relay/spool.py": 1000,
    # split/storage-policy-w2: storage_policy.py's own ratchet-baseline entry
    # (1826) is removed here. The wire types/limits/error vocabulary moved to
    # storage_policy_types.py (280 lines), the ledger content codec to
    # storage_ledger_codec.py (215 lines), the filesystem-identity and
    # durable-I/O primitives built on it to storage_file_io.py (256 lines),
    # and StoragePolicy's reservation-CRUD/status-health surfaces to the
    # StorageReservationLedgerMixin/StorageSnapshotMixin mixins
    # (storage_reservation_ledger.py, 457 lines; storage_snapshot.py, 230
    # lines) it composes. ``scan_tree``, ``_scandir_verified``, and
    # ``_replace_file`` -- each individually monkeypatched by name in the test
    # suite via ``storage_module.<name>`` -- plus every caller that reaches one
    # of them by bare (non-``self.``) name (``StoragePolicy._stable_tree_
    # snapshot``, ``StoragePolicy.check_runtime_job``, ``StoragePolicy.
    # _write_ledger``) stay resident on the facade, which is now 511 lines
    # (an assembly + re-export surface, comfortably under the 800-line
    # default cap): no baseline entry needed.
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
    # split/transport-probe-w2 (#231): five owner modules --
    # transport_probe_primitives.py (151: ManagedProcess protocol, probe
    # callback aliases, small process/health/shell helpers),
    # transport_probe_evidence.py (101: structured cleanup-evidence
    # assembly), transport_probe_session_lifecycle.py (363: SSH-forward
    # session start/detach/teardown verification),
    # transport_probe_remote_script.py (159: the remote FRP bootstrap
    # script), transport_probe_remote_cleanup_models.py (69: the remote
    # cleanup payload's pydantic shape) and transport_probe_remote_cleanup.py
    # (446: its token-verified stop-and-report logic). Five functions
    # (run_frp_http_probe, run_frp_direct_http_probe,
    # run_ssh_forward_http_probe, _run_frp_http_probe_with_proxy_type,
    # _finish_frp_probe_cleanup) stay resident rather than moving with their
    # concern: tests/test_transport_probe.py patches
    # clio_relay.transport_probe._wait_for_healthz/_cleanup_remote_probe/
    # teardown_remote_session/detach_remote_session directly and expects the
    # probe orchestration to see the fake, which only holds while that call
    # site's enclosing def resolves the bare name against this module's own
    # namespace at call time -- moving the caller elsewhere silently
    # un-patches those tests (transport_probe.py's own module docstring has
    # the full explanation). 1794 -> 696, back under the 800-line default
    # cap -- transport_probe.py graduates out of RATCHET_BASELINE.
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
    # #231 split/validation-report S6: the Spack fresh-install transition
    # check (one release-policy requirement bound against an exact
    # preinstall/install/postinstall job/check/artifact evidence graph)
    # moved to spack_transition_checks.py (612 lines -- over the sweet spot,
    # under the 800 cap; one coherent concern). Only its entry point has a
    # caller left in this file (gate evaluation); no inner binding helper is
    # tested directly, so nothing else needed re-export. 3751 -> 3174.
    # #231 split/validation-report S7: install-source-detection primitives --
    # over 800 lines combined, a real three-way seam split (ground rule per
    # the split recipe). regular_file_identity.py (92 lines) is the shared
    # snapshot-verified-read leaf both other modules and this file's own
    # remaining _detect_launcher_receipt/detect_install_source depend on;
    # process_ancestry.py (226 lines) walks the OS parent chain for the
    # launching uv executable; uv_tool_receipt.py (474 lines) binds the
    # install-once uv-tool receipt + installed-RECORD closure. Every symbol
    # this file's own remaining orchestration still calls, plus the two uv
    # receipt functions test_validation_report.py exercises directly, are
    # re-exported. 3174 -> 2482.
    # #231 split/validation-report S8: release-gate evaluation -- over 900
    # lines combined, a real three-way seam split. release_gate_targets.py
    # (268 lines) binds reports and policy pins to one physical cluster
    # target identity; release_gate_resources.py (206 lines) matches a
    # requirement's stateful resources and JARVIS execution; both are
    # coherent sub-concerns release_gate_evaluation.py (588 lines, the core
    # evaluate_release_gate orchestration) calls into. Nothing in this
    # file's own remaining code (report I/O, the durable validation
    # directory) calls any of it any more except the public
    # evaluate_release_gate entry point cli.py imports, so only that one
    # name is re-exported; two back-references (validation_schema.py's
    # _normalized_hostname, already pointed at
    # artifact_identity_verification.py for is_official_github_release_wheel
    # in S7) are re-pointed at their real owner modules directly instead of
    # hopping through this file. 2482 -> 1534.
    # #231 split/validation-report S9: the durable validation directory --
    # over 1000 lines, a real three-way seam split. validation_directory_
    # windows.py (314 lines) pins/verifies/creates directories through a
    # raw CreateFileW handle (Windows has no O_NOFOLLOW/dir_fd equivalent);
    # validation_writer_lock.py (317 lines) is the cross-platform parent-
    # wide writer lock plus its stale-.pending sweep, built on the windows
    # primitives; durable_validation_write.py (537 lines) is the top-level
    # orchestration (durably_ensure_validation_directory + the atomic
    # text-replace pair) built on both. validation_report.py is now under
    # DEFAULT_MAX_LINES -- entry removed. 1534 -> 505.
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
