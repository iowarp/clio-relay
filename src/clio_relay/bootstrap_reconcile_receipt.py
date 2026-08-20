"""Machine-readable v2 bootstrap acceptance receipt construction.

``make_bootstrap_receipt`` assembles the full acceptance receipt for a
completed bootstrap run; ``validate_jarvis_builtin_result`` is the bounded
contract check its JARVIS resource-graph section depends on
(iowarp/clio-relay#255).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Literal, cast

from clio_relay.bootstrap_reconcile_constants import BOOTSTRAP_RECEIPT_SCHEMA
from clio_relay.bootstrap_reconcile_models import (
    BootstrapDesiredState,
    BootstrapInspection,
    JarvisStateEvidence,
)
from clio_relay.bootstrap_reconcile_primitives import _atomic_json, _is_sha256
from clio_relay.bootstrap_reconcile_transaction import BootstrapTransactionJournal


def validate_jarvis_builtin_result(
    result: dict[str, object],
    *,
    requested_profile: str,
) -> None:
    """Validate the bounded JARVIS builtin resource-graph result contract."""
    expected_fields = {
        "schema_version",
        "profile",
        "action",
        "available",
        "source",
        "source_sha256",
        "catalog",
    }
    if set(result) != expected_fields:
        raise ValueError("JARVIS builtin graph result has an unexpected shape")
    if (
        result.get("schema_version") != "jarvis.resource-graph-builtin.v1"
        or result.get("profile") != requested_profile
    ):
        raise ValueError("JARVIS builtin graph result does not match the requested profile")
    raw_catalog = result.get("catalog")
    if not isinstance(raw_catalog, list):
        raise ValueError("JARVIS builtin graph catalog is invalid")
    catalog = cast(list[object], raw_catalog)
    if len(catalog) > 128 or any(
        not isinstance(profile, str)
        or not profile
        or len(profile) > 256
        or profile != profile.strip()
        or profile in {".", ".."}
        or "/" in profile
        or "\\" in profile
        or any(ord(character) < 32 or ord(character) == 127 for character in profile)
        for profile in catalog
    ):
        raise ValueError("JARVIS builtin graph catalog is invalid")
    typed_catalog = cast(list[str], catalog)
    if typed_catalog != sorted(set(typed_catalog)):
        raise ValueError("JARVIS builtin graph catalog is invalid")
    action = result.get("action")
    available = result.get("available")
    source = result.get("source")
    source_sha256 = result.get("source_sha256")
    if action == "loaded":
        if (
            available is not True
            or not isinstance(source, str)
            or not source
            or len(source) > 4096
            or not PurePosixPath(source).is_absolute()
            or any(character in source for character in "\x00\r\n")
            or not _is_sha256(source_sha256)
            or requested_profile not in typed_catalog
        ):
            raise ValueError("loaded JARVIS builtin graph evidence is invalid")
    elif action == "unavailable":
        if (
            available is not False
            or source is not None
            or source_sha256 is not None
            or requested_profile in typed_catalog
        ):
            raise ValueError("unavailable JARVIS builtin graph evidence is invalid")
    else:
        raise ValueError("JARVIS builtin graph result has an invalid action")


def make_bootstrap_receipt(
    *,
    invocation_id: str,
    desired: BootstrapDesiredState,
    outcome: Literal[
        "noop_verified",
        "verified_after_transfer",
        "repaired",
        "reconciled",
        "full",
    ],
    inspection: BootstrapInspection,
    started_at: datetime,
    transaction: BootstrapTransactionJournal | None,
    previous_generation: str | None,
    active_generation: str | None,
    components: dict[str, dict[str, object]] | None = None,
    duration_seconds: float = 0.0,
    inspection_duration_seconds: float = 0.0,
    downloads: list[dict[str, object]] | None = None,
    service_restart_count: int = 0,
    service_start_count: int = 0,
    service_stop_count: int = 0,
    service_enable_count: int = 0,
    queue_action: Literal["verified_read_only", "audited_and_sealed"] = ("verified_read_only"),
    queue_duration_seconds: float = 0.0,
    jarvis_init_action: Literal["preserved", "initialized"] = "preserved",
    jarvis_init_duration_seconds: float = 0.0,
    jarvis_graph_action: Literal["preserved", "loaded", "built"] = "preserved",
    jarvis_graph_duration_seconds: float = 0.0,
    jarvis_builtin_result: dict[str, object] | None = None,
    jarvis_commands: list[list[str]] | None = None,
    jarvis_state_before: JarvisStateEvidence | None = None,
    jarvis_repo_reconciliation: dict[str, object] | None = None,
    initial_inspection_reasons: list[str] | None = None,
    service_active_before: bool | None = None,
    service_enabled_before: bool | None = None,
    service_active_after: bool | None = None,
    service_enabled_after: bool | None = None,
    service_pending_install: bool = False,
    payload_transfer_count: int = 0,
    payload_transfer_bytes: int = 0,
) -> dict[str, object]:
    """Build the machine-readable v2 receipt for a completed acceptance run."""
    if (
        min(
            duration_seconds,
            inspection_duration_seconds,
            queue_duration_seconds,
            jarvis_init_duration_seconds,
            jarvis_graph_duration_seconds,
        )
        < 0
    ):
        raise ValueError("bootstrap duration cannot be negative")
    if (
        min(
            service_restart_count,
            service_start_count,
            service_stop_count,
            service_enable_count,
            payload_transfer_count,
            payload_transfer_bytes,
        )
        < 0
    ):
        raise ValueError("service action counts cannot be negative")
    component_evidence = components or _default_noop_components(
        desired,
        duration_seconds=duration_seconds,
    )
    commands = jarvis_commands or []
    if any(not command or any(not value for value in command) for command in commands):
        raise ValueError("JARVIS command evidence must contain non-empty argument vectors")
    if jarvis_graph_action == "preserved":
        if jarvis_builtin_result is not None:
            raise ValueError("a preserved JARVIS graph cannot claim builtin activation")
    else:
        if desired.jarvis_resource_graph_profile is None or jarvis_builtin_result is None:
            raise ValueError("JARVIS graph activation requires exact builtin result evidence")
        validate_jarvis_builtin_result(
            jarvis_builtin_result,
            requested_profile=desired.jarvis_resource_graph_profile,
        )
        expected_builtin_action = "loaded" if jarvis_graph_action == "loaded" else "unavailable"
        if jarvis_builtin_result["action"] != expected_builtin_action:
            raise ValueError("JARVIS graph action does not match builtin activation evidence")
        if jarvis_graph_action == "loaded" and not _is_sha256(
            inspection.jarvis_state.resource_graph_sha256
        ):
            # JARVIS normalizes the graph while activating it, so the activated
            # file is a derivative of the packaged builtin and its digest
            # legitimately differs from source_sha256 -- requiring equality
            # here failed every fresh bootstrap (#158). Source identity is
            # proven cluster-side, against the packaged file itself; the
            # receipt binds that activation evidence was recorded.
            raise ValueError(
                "loaded JARVIS graph did not record an activated resource graph digest"
            )
        if jarvis_graph_action == "built" and not desired.allow_jarvis_resource_graph_build:
            raise ValueError("JARVIS graph build was not enabled by the desired state")
    before = jarvis_state_before or inspection.jarvis_state
    repo_evidence = jarvis_repo_reconciliation or {
        "link_action": "reused",
        "link": desired.managed_jarvis_repo,
        "target": None,
        "repositories": {
            "action": "reused",
            "managed_repo": None,
            "added_managed_repos": [],
            "removed_previous_managed_repos": [],
            "before_sha256": before.repos_sha256,
            "after_sha256": inspection.jarvis_state.repos_sha256,
        },
    }
    return {
        "schema_version": BOOTSTRAP_RECEIPT_SCHEMA,
        "invocation_id": invocation_id,
        "bootstrap_profile": desired.bootstrap_profile,
        "relay_install_spec": desired.relay_install_spec,
        "desired_fingerprint": desired.fingerprint,
        "outcome": outcome,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "plan": {
            "mode": "none" if outcome == "noop_verified" else "reconcile",
            "reasons": inspection.reasons,
        },
        "transaction": (transaction.model_dump(mode="json") if transaction is not None else None),
        "generation": {
            "previous": previous_generation,
            "active": active_generation,
            "current_target": inspection.current_generation_target,
        },
        "duration_seconds": duration_seconds,
        "inspection": {
            "duration_seconds": inspection_duration_seconds,
            "read_only": True,
            "initial_reasons": initial_inspection_reasons or [],
        },
        "components": component_evidence,
        "operations": {
            "downloads": downloads or [],
            "download_count": len(downloads or []),
            "service_restart_count": service_restart_count,
            "service_start_count": service_start_count,
            "service_stop_count": service_stop_count,
            "service_enable_count": service_enable_count,
            "scheduler_submission_count": 0,
            "scheduler_cancellation_count": 0,
            "generation_gc_count": 0,
            "payload_transfer_count": payload_transfer_count,
            "payload_transfer_bytes": payload_transfer_bytes,
        },
        "install_receipt_sha256": inspection.install_receipt_sha256,
        "jarvis_state": inspection.jarvis_state.model_dump(mode="json"),
        "jarvis_initialization": {
            "action": jarvis_init_action,
            "duration_seconds": jarvis_init_duration_seconds,
        },
        "jarvis_resource_graph": {
            "action": jarvis_graph_action,
            "duration_seconds": jarvis_graph_duration_seconds,
            "benchmark_enabled": False,
            "selected_profile": desired.jarvis_resource_graph_profile,
            "allow_build_fallback": desired.allow_jarvis_resource_graph_build,
            "builtin_result": jarvis_builtin_result,
        },
        "jarvis_commands": {
            "count": len(commands),
            "argv": commands,
        },
        "jarvis_preservation": {
            "before": before.model_dump(mode="json"),
            "after": inspection.jarvis_state.model_dump(mode="json"),
            "config_byte_identical": (
                before.config_sha256 == inspection.jarvis_state.config_sha256
            ),
            "resource_graph_byte_identical": (
                before.resource_graph_sha256 == inspection.jarvis_state.resource_graph_sha256
            ),
            "repositories_byte_identical": (
                before.repos_sha256 == inspection.jarvis_state.repos_sha256
            ),
            "repositories": repo_evidence,
        },
        "queue": inspection.readiness.queue,
        "queue_operation": {
            "action": queue_action,
            "duration_seconds": queue_duration_seconds,
            "records_examined": (
                inspection.readiness.queue.get("records_examined")
                if inspection.readiness.queue is not None
                else None
            ),
            "bounds": (
                inspection.readiness.queue.get("bounds")
                if inspection.readiness.queue is not None
                else None
            ),
        },
        "worker": inspection.readiness.model_dump(mode="json"),
        "service": {
            "name": desired.worker_service,
            "pending_install": service_pending_install,
            "active_before": service_active_before,
            "enabled_before": service_enabled_before,
            "active_after": (
                inspection.readiness.service_was_active
                if service_active_after is None
                else service_active_after
            ),
            "enabled_after": (
                inspection.readiness.service_was_enabled
                if service_enabled_after is None
                else service_enabled_after
            ),
        },
        "preservation": {
            "scheduler_jobs_cancelled": False,
            "old_generations_retained": True,
            "jarvis_init_on_existing_root": False,
        },
    }


def _default_noop_components(
    desired: BootstrapDesiredState,
    *,
    duration_seconds: float,
) -> dict[str, dict[str, object]]:
    identities: dict[str, object] = {
        "clio-relay": {
            "install_spec": desired.relay_install_spec,
            "artifact_sha256": desired.relay_artifact_sha256,
        },
        "clio-kit": {
            "version": desired.clio_kit_version,
            "artifact_sha256": desired.clio_kit_artifact_sha256,
        },
        "jarvis-cd": {
            "version": desired.jarvis_cd_version,
            "artifact_sha256": desired.jarvis_cd_wheel_sha256,
        },
        "jarvis-util": {"commit": desired.jarvis_util_commit},
        "frp": {
            "version": desired.frp_version,
            "frpc_sha256": desired.frpc_sha256,
            "frps_sha256": desired.frps_sha256,
        },
        "uv": {"version": desired.uv_version, "sha256": desired.uv_sha256},
    }
    return {
        name: {
            "action": "reused",
            "observed_identity": identity,
            "duration_seconds": duration_seconds,
        }
        for name, identity in identities.items()
    }


def write_bootstrap_receipt(path: Path, receipt: dict[str, object]) -> None:
    """Atomically persist one current invocation acceptance receipt."""
    _atomic_json(path, receipt)
