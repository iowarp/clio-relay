"""Route-agnostic staging plane for declared JARVIS package inputs.

Both JARVIS doors reach the cluster through the same package contract: a package
description declares which settings name caller-local files, and clio-relay
snapshots, ingests, and rewrites exactly those settings before the configuration
reaches JARVIS. This module owns that orchestration so the registered remote-MCP
route (``clio-kit-jarvis-user-v3.7.1``) and the built-in virtual JARVIS door share
one implementation instead of two drifting copies.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast
from uuid import uuid4

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.input_staging import (
    JarvisPackageInputContract,
    jarvis_package_input_contract_from_record,
    jarvis_package_input_contract_record,
    jarvis_package_input_route,
    jarvis_pipeline_input_route,
    jarvis_run_input_drift,
    merge_artifact_uses,
    parse_jarvis_package_input_contract,
    reconcile_jarvis_run_inputs,
    stage_jarvis_add_step_inputs,
    stage_jarvis_edit_step_inputs,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_USER_CONTRACT_ID,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    CLIO_KIT_JARVIS_USER_WIRE_SHA256,
    JARVIS_MCP_CACHE_SERVER_NAME,
)
from clio_relay.models import (
    ArtifactUse,
    JarvisPackageInputRoute,
    JarvisPipelineInputBinding,
    JarvisPipelineInputBindings,
    JarvisPipelineInputRoute,
    JarvisRunInputManifest,
    artifact_use_payload,
)

JSON = dict[str, Any]
BUILTIN_JARVIS_SERVER_NAME = JARVIS_MCP_CACHE_SERVER_NAME
BUILTIN_JARVIS_ROUTE_SCHEMA = "clio-relay.builtin-jarvis-route.v1"


class JarvisInputSessionCache(Protocol):
    """The per-client cache of package semantics observed by one MCP session."""

    def remember_jarvis_package_inputs(self, contract: JarvisPackageInputContract) -> None:
        """Remember one structured package description for this initialized client."""

    def jarvis_package_inputs(self, cache_key: str) -> JarvisPackageInputContract | None:
        """Return one exact structured package input contract, if it was observed."""
        ...

    def remember_jarvis_pipeline_inputs(
        self,
        cache_key: str,
        uses: tuple[ArtifactUse, ...],
    ) -> None:
        """Remember immutable inputs accepted by one exact JARVIS pipeline route."""

    def jarvis_pipeline_inputs(self, cache_key: str) -> tuple[ArtifactUse, ...]:
        """Return immutable inputs previously accepted for one exact pipeline route."""
        ...


@dataclass(frozen=True, slots=True)
class JarvisStagingRoute:
    """One exact JARVIS door identity that staged inputs are bound to."""

    cluster: str
    server_name: str
    cluster_route_revision: str
    registration_revision: str
    expected_server_artifact_digest: str | None
    remote_tool_name: str
    carries_run_input_manifest: bool
    reconciles_every_description: bool


@dataclass(frozen=True, slots=True)
class JarvisInputPlan:
    """Staged arguments and the durable records one JARVIS call must reconcile."""

    route: JarvisStagingRoute
    arguments: JSON
    automatic_artifact_uses: tuple[ArtifactUse, ...] = ()
    require_terminal_wait: bool = False
    reconciliation_required: bool = False
    run_input_manifest: JarvisRunInputManifest | None = None
    run_idempotency_key: str | None = None
    described_package_name: str | None = None
    package_route: JarvisPackageInputRoute | None = None
    package_cache_key: str | None = None
    pipeline_route: JarvisPipelineInputRoute | None = None
    pipeline_cache_key: str | None = None
    staged_bindings: tuple[JarvisPipelineInputBinding, ...] = ()
    removed_binding_identities: tuple[tuple[str, str], ...] = ()
    staged_manifest_sha256: str | None = None
    binding_mutation_required: bool = False


def builtin_jarvis_registration_revision() -> str:
    """Return the pinned identity of the relay's own built-in JARVIS door.

    The registered route is identified by its remote-MCP registration. The
    built-in door has no registration: its equivalent binding is the exact
    clio-kit release and user contract this relay ships, so staged inputs never
    cross a JARVIS contract or launcher change.
    """
    return _stable_digest(
        {
            "schema_version": BUILTIN_JARVIS_ROUTE_SCHEMA,
            "server_name": BUILTIN_JARVIS_SERVER_NAME,
            "contract": CLIO_KIT_JARVIS_USER_CONTRACT_ID,
            "contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            "wire_sha256": CLIO_KIT_JARVIS_USER_WIRE_SHA256,
            "clio_kit_version": CLIO_KIT_JARVIS_MCP_VERSION,
        }
    )


def builtin_jarvis_staging_route(
    *,
    cluster: str,
    cluster_route_revision: str,
    expected_server_artifact_digest: str | None,
    remote_tool_name: str,
) -> JarvisStagingRoute:
    """Return the exact staging identity of one built-in JARVIS door call."""
    return JarvisStagingRoute(
        cluster=cluster,
        server_name=BUILTIN_JARVIS_SERVER_NAME,
        cluster_route_revision=cluster_route_revision,
        registration_revision=builtin_jarvis_registration_revision(),
        expected_server_artifact_digest=expected_server_artifact_digest,
        remote_tool_name=remote_tool_name,
        # The durable MCP call spec and the cluster-side mcp_call runner accept a
        # resolved run manifest only for the registered contract, so the built-in
        # door refuses drifted content instead of silently running stale bytes.
        carries_run_input_manifest=False,
        # A registered route reconciles every description because the structured
        # result is also its registration acceptance evidence. The built-in door
        # has no registration to accept, so only a package description - the one
        # that carries input bindings - is reconciled, and every other JARVIS
        # description keeps its asynchronous relay receipt.
        reconciles_every_description=False,
    )


def prepare_jarvis_inputs(
    forwarded_arguments: JSON,
    *,
    route: JarvisStagingRoute,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    session: JarvisInputSessionCache | None,
    resolve_definition: Callable[[str], ClusterDefinition],
    requested_idempotency_key: str | None,
) -> JarvisInputPlan:
    """Stage every declared local input this JARVIS call carries, or refuse loudly."""
    if route.remote_tool_name == "jarvis_describe":
        return _prepare_describe(forwarded_arguments, route=route)
    if route.remote_tool_name == "jarvis_add_step":
        return _prepare_add_step(
            forwarded_arguments,
            route=route,
            queue=queue,
            settings=settings,
            session=session,
            resolve_definition=resolve_definition,
        )
    if route.remote_tool_name == "jarvis_edit_step":
        return _prepare_edit_step(
            forwarded_arguments,
            route=route,
            queue=queue,
            settings=settings,
            resolve_definition=resolve_definition,
        )
    if route.remote_tool_name == "jarvis_run":
        return _prepare_run(
            forwarded_arguments,
            route=route,
            queue=queue,
            settings=settings,
            session=session,
            resolve_definition=resolve_definition,
            requested_idempotency_key=requested_idempotency_key,
        )
    return JarvisInputPlan(route=route, arguments=forwarded_arguments)


def jarvis_submission_idempotency_key(
    plan: JarvisInputPlan,
    *,
    merged_artifact_uses: list[ArtifactUse],
    requested_idempotency_key: str | None,
) -> str | None:
    """Return the exact key a staged submission must reuse, or None when free."""
    if plan.run_idempotency_key is not None:
        return plan.run_idempotency_key
    if requested_idempotency_key:
        return requested_idempotency_key
    if not plan.reconciliation_required:
        return None
    return "mcp:virtual:reconcile:" + _stable_digest(
        {
            "cluster": plan.route.cluster,
            "server_name": plan.route.server_name,
            "cluster_route_revision": plan.route.cluster_route_revision,
            "registration_revision": plan.route.registration_revision,
            "expected_server_artifact_digest": plan.route.expected_server_artifact_digest,
            "tool": plan.route.remote_tool_name,
            "arguments": plan.arguments,
            "used_artifact_refs": [artifact_use_payload(item) for item in merged_artifact_uses],
        }
    )


def record_jarvis_inputs(
    result: JSON,
    *,
    plan: JarvisInputPlan,
    queue: ClioCoreQueue,
    session: JarvisInputSessionCache | None,
) -> None:
    """Accept package semantics and staged lineage only from a terminal result."""
    tool = plan.route.remote_tool_name
    if tool == "jarvis_describe":
        if plan.reconciliation_required:
            require_terminal_staging_reconciliation(result, operation="jarvis_describe")
        _record_described_package(result, plan=plan, queue=queue, session=session)
        return
    if tool == "jarvis_add_step" and plan.automatic_artifact_uses:
        require_terminal_staging_reconciliation(result, operation="jarvis_add_step")
    if tool == "jarvis_edit_step" and plan.binding_mutation_required:
        require_terminal_staging_reconciliation(result, operation="jarvis_edit_step")
    if not _accepted_terminal_result(result):
        return
    if tool == "jarvis_add_step" and plan.pipeline_route is not None and plan.staged_bindings:
        observed_step_id = jarvis_add_step_result_step_id(result)
        expected_step_ids = {item.step_id for item in plan.staged_bindings}
        if expected_step_ids != {observed_step_id}:
            raise ValueError(
                "jarvis_add_step returned a different step identity than its staged inputs"
            )
        queue.update_jarvis_pipeline_input_bindings(
            plan.pipeline_route,
            upserts=plan.staged_bindings,
        )
    if (
        tool == "jarvis_edit_step"
        and plan.pipeline_route is not None
        and plan.binding_mutation_required
    ):
        queue.update_jarvis_pipeline_input_bindings(
            plan.pipeline_route,
            upserts=plan.staged_bindings,
            remove=plan.removed_binding_identities,
        )
    if (
        tool == "jarvis_add_step"
        and plan.pipeline_route is not None
        and plan.pipeline_cache_key is not None
        and plan.automatic_artifact_uses
        and plan.staged_manifest_sha256 is not None
    ):
        durable_lineage = queue.merge_jarvis_pipeline_input_lineage(
            plan.pipeline_route,
            plan.automatic_artifact_uses,
            manifest_sha256=plan.staged_manifest_sha256,
        )
        if session is not None:
            session.remember_jarvis_pipeline_inputs(
                plan.pipeline_cache_key,
                durable_lineage.artifact_uses,
            )


def require_terminal_staging_reconciliation(result: JSON, *, operation: str) -> None:
    """Refuse to lose package semantics or staged-input lineage after a bounded wait."""
    if result.get("terminal") is True:
        return
    cluster = result.get("cluster")
    job_id = result.get("job_id")
    route_revision = result.get("route_revision")
    handle = f"cluster={cluster!r}, job_id={job_id!r}, route_revision={route_revision!r}"
    raise ValueError(
        f"{operation} did not become terminal during bounded contract reconciliation; "
        f"the durable relay job remains observable ({handle}). Use relay_wait on that exact "
        "handle, then retry this call with identical arguments. Relay reuses the deterministic "
        "reconciliation identity when idempotency_key was omitted. No package contract or "
        "staged-input lineage was accepted locally."
    )


def jarvis_add_step_result_step_id(result: JSON) -> str:
    """Extract the exact accepted step identity from a terminal add-step result."""
    raw_mcp_result = result.get("mcp_result")
    if not isinstance(raw_mcp_result, dict):
        raise ValueError("jarvis_add_step terminal result omitted its MCP result")
    raw_structured = cast(JSON, raw_mcp_result).get("structured_result")
    if not isinstance(raw_structured, dict):
        raise ValueError("jarvis_add_step terminal result omitted structured output")
    structured = cast(JSON, raw_structured)
    raw_nested = structured.get("result")
    has_flat_identity = "step_id" in structured
    if has_flat_identity and raw_nested is not None:
        raise ValueError("jarvis_add_step structured output has ambiguous result envelopes")
    if has_flat_identity:
        raw_payload = structured
    elif isinstance(raw_nested, dict):
        # Compatibility for relay evidence produced by the historical test adapter.
        # FastMCP's declared JARVIS contract returns the object itself.
        raw_payload = cast(JSON, raw_nested)
    else:
        raise ValueError("jarvis_add_step structured output omitted its step identity")
    step_id = raw_payload.get("step_id")
    if not isinstance(step_id, str) or not step_id:
        raise ValueError("jarvis_add_step structured output omitted its step identity")
    return step_id


def _prepare_describe(forwarded_arguments: JSON, *, route: JarvisStagingRoute) -> JarvisInputPlan:
    # v3.6 descriptions are control queries whose structured result is part of
    # the agent-facing contract. Reconcile them transparently even when the
    # generated default is omitted or a legacy client sends false.
    described_package_name: str | None = None
    package_route: JarvisPackageInputRoute | None = None
    package_cache_key: str | None = None
    if forwarded_arguments.get("target") == "package":
        described_package_name = _required_argument(forwarded_arguments, "package_name")
        package_route = _package_route(route, package_name=described_package_name)
        package_cache_key = package_route.identity_sha256()
    reconciled = route.reconciles_every_description or described_package_name is not None
    return JarvisInputPlan(
        route=route,
        arguments=forwarded_arguments,
        require_terminal_wait=reconciled,
        reconciliation_required=reconciled,
        described_package_name=described_package_name,
        package_route=package_route,
        package_cache_key=package_cache_key,
    )


def _prepare_add_step(
    forwarded_arguments: JSON,
    *,
    route: JarvisStagingRoute,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    session: JarvisInputSessionCache | None,
    resolve_definition: Callable[[str], ClusterDefinition],
) -> JarvisInputPlan:
    package_name = _required_argument(forwarded_arguments, "package_name")
    package_route = _package_route(route, package_name=package_name)
    package_cache_key = package_route.identity_sha256()
    contract = None if session is None else session.jarvis_package_inputs(package_cache_key)
    if contract is None:
        durable_contract = queue.get_jarvis_package_input_contract(package_route)
        if durable_contract is not None:
            contract = jarvis_package_input_contract_from_record(durable_contract)
            if session is not None:
                session.remember_jarvis_package_inputs(contract)
    if contract is None:
        raise ValueError(
            "jarvis_add_step requires a successful jarvis_describe package call on "
            "this exact cluster route before package configuration"
        )
    staged = stage_jarvis_add_step_inputs(
        forwarded_arguments,
        contract=contract,
        definition=resolve_definition(route.cluster),
        settings=settings,
    )
    # Pipeline lineage is bound to the owner session generation that staged it.
    # A package configuration that stages nothing needs no lineage route, so a
    # package without declared file inputs stays usable without an owned session.
    pipeline_route = (
        _pipeline_route(
            route,
            settings=settings,
            pipeline_id=_required_argument(staged.arguments, "pipeline_id"),
        )
        if staged.bindings
        else None
    )
    return JarvisInputPlan(
        route=route,
        arguments=staged.arguments,
        automatic_artifact_uses=staged.artifact_uses,
        # Staged inputs become durable pipeline lineage only after JARVIS accepts
        # this exact add-step. Never return an asynchronous receipt that can lose
        # that acceptance edge across an MCP restart.
        require_terminal_wait=bool(staged.artifact_uses),
        reconciliation_required=bool(staged.artifact_uses),
        package_route=package_route,
        package_cache_key=package_cache_key,
        pipeline_route=pipeline_route,
        pipeline_cache_key=None if pipeline_route is None else pipeline_route.identity_sha256(),
        staged_bindings=staged.bindings,
        staged_manifest_sha256=staged.manifest_sha256,
    )


def _prepare_edit_step(
    forwarded_arguments: JSON,
    *,
    route: JarvisStagingRoute,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    resolve_definition: Callable[[str], ClusterDefinition],
) -> JarvisInputPlan:
    if not _tracked_bindings_are_reachable(settings):
        return JarvisInputPlan(route=route, arguments=forwarded_arguments)
    pipeline_route = _pipeline_route(
        route,
        settings=settings,
        pipeline_id=_required_argument(forwarded_arguments, "pipeline_id"),
    )
    current_bindings = queue.get_jarvis_pipeline_input_bindings(pipeline_route)
    if current_bindings is None:
        return JarvisInputPlan(
            route=route,
            arguments=forwarded_arguments,
            pipeline_route=pipeline_route,
            pipeline_cache_key=pipeline_route.identity_sha256(),
        )
    staged = stage_jarvis_edit_step_inputs(
        forwarded_arguments,
        current=current_bindings,
        definition=resolve_definition(route.cluster),
        settings=settings,
    )
    binding_mutation_required = bool(staged.bindings or staged.removed_binding_identities)
    return JarvisInputPlan(
        route=route,
        arguments=staged.arguments,
        automatic_artifact_uses=staged.artifact_uses,
        require_terminal_wait=binding_mutation_required,
        reconciliation_required=bool(staged.artifact_uses),
        pipeline_route=pipeline_route,
        pipeline_cache_key=pipeline_route.identity_sha256(),
        staged_bindings=staged.bindings,
        removed_binding_identities=staged.removed_binding_identities,
        staged_manifest_sha256=staged.manifest_sha256,
        binding_mutation_required=binding_mutation_required,
    )


def _prepare_run(
    forwarded_arguments: JSON,
    *,
    route: JarvisStagingRoute,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    session: JarvisInputSessionCache | None,
    resolve_definition: Callable[[str], ClusterDefinition],
    requested_idempotency_key: str | None,
) -> JarvisInputPlan:
    if not _tracked_bindings_are_reachable(settings):
        return JarvisInputPlan(route=route, arguments=forwarded_arguments)
    pipeline_route = _pipeline_route(
        route,
        settings=settings,
        pipeline_id=_required_argument(forwarded_arguments, "pipeline_id"),
    )
    pipeline_cache_key = pipeline_route.identity_sha256()
    run_idempotency_key = requested_idempotency_key or _fresh_idempotency_key(route)
    current_bindings = queue.get_jarvis_pipeline_input_bindings(pipeline_route)
    if current_bindings is None or not current_bindings.bindings:
        durable_lineage = queue.get_jarvis_pipeline_input_lineage(pipeline_route)
        durable_uses = () if durable_lineage is None else durable_lineage.artifact_uses
        session_uses = (
            [] if session is None else list(session.jarvis_pipeline_inputs(pipeline_cache_key))
        )
        inherited_uses = tuple(merge_artifact_uses(session_uses, durable_uses))
        return JarvisInputPlan(
            route=route,
            arguments=forwarded_arguments,
            automatic_artifact_uses=inherited_uses,
            reconciliation_required=bool(inherited_uses),
            run_idempotency_key=run_idempotency_key,
            pipeline_route=pipeline_route,
            pipeline_cache_key=pipeline_cache_key,
        )
    run_input_manifest = queue.get_jarvis_run_input_manifest(
        pipeline_route,
        idempotency_key=run_idempotency_key,
    )
    if run_input_manifest is None:
        if not route.carries_run_input_manifest:
            _require_staged_content_is_current(current_bindings, settings=settings, route=route)
        resolutions = reconcile_jarvis_run_inputs(
            current_bindings,
            definition=resolve_definition(route.cluster),
            settings=settings,
        )
        run_input_manifest = queue.put_jarvis_run_input_manifest(
            JarvisRunInputManifest.create(
                route=pipeline_route,
                idempotency_key=run_idempotency_key,
                resolutions=resolutions,
            )
        )
        queue.update_jarvis_pipeline_input_bindings(
            pipeline_route,
            upserts=tuple(item.binding for item in run_input_manifest.resolutions),
        )
    return JarvisInputPlan(
        route=route,
        arguments=forwarded_arguments,
        automatic_artifact_uses=run_input_manifest.artifact_uses,
        reconciliation_required=bool(run_input_manifest.artifact_uses),
        run_input_manifest=run_input_manifest,
        run_idempotency_key=run_idempotency_key,
        pipeline_route=pipeline_route,
        pipeline_cache_key=pipeline_cache_key,
    )


def _require_staged_content_is_current(
    current_bindings: JarvisPipelineInputBindings,
    *,
    settings: RelaySettings,
    route: JarvisStagingRoute,
) -> None:
    """Refuse a run whose staged bytes no longer match the caller's workspace."""
    drifted = jarvis_run_input_drift(current_bindings, settings=settings)
    if not drifted:
        return
    raise ValueError(
        "local input content changed after it was staged for this pipeline: "
        f"{', '.join(drifted)}. The "
        f"{route.server_name} JARVIS route cannot restage inside jarvis_run; call "
        "jarvis_edit_step with the same setting to stage the new content, then run again."
    )


def _record_described_package(
    result: JSON,
    *,
    plan: JarvisInputPlan,
    queue: ClioCoreQueue,
    session: JarvisInputSessionCache | None,
) -> None:
    if plan.described_package_name is None or plan.package_cache_key is None:
        return
    package_contract = parse_jarvis_package_input_contract(
        result,
        cache_key=plan.package_cache_key,
    )
    if result.get("state") == "succeeded" and package_contract is None:
        raise ValueError(
            "successful jarvis_describe package call omitted its package input contract"
        )
    if package_contract is None:
        return
    if plan.described_package_name not in package_contract.package_names:
        raise ValueError("jarvis_describe returned a different package than the requested package")
    for package_name in package_contract.package_names:
        alias_route = _package_route(plan.route, package_name=package_name)
        alias_contract = JarvisPackageInputContract(
            cache_key=alias_route.identity_sha256(),
            package_names=package_contract.package_names,
            local_file_settings=package_contract.local_file_settings,
            settings_sha256=package_contract.settings_sha256,
        )
        saved_contract = queue.put_jarvis_package_input_contract(
            jarvis_package_input_contract_record(route=alias_route, contract=alias_contract)
        )
        if session is not None:
            session.remember_jarvis_package_inputs(
                jarvis_package_input_contract_from_record(saved_contract)
            )


def _package_route(route: JarvisStagingRoute, *, package_name: str) -> JarvisPackageInputRoute:
    return jarvis_package_input_route(
        cluster=route.cluster,
        server_name=route.server_name,
        cluster_route_revision=route.cluster_route_revision,
        registration_revision=route.registration_revision,
        expected_server_artifact_digest=route.expected_server_artifact_digest,
        package_name=package_name,
    )


def _tracked_bindings_are_reachable(settings: RelaySettings) -> bool:
    """Return whether staged pipeline bindings can exist for this relay identity.

    Every durable binding names the owner session generation that staged it, so
    without an active owned session no binding of any pipeline is addressable and
    there is nothing for an edit or a run to reconcile.
    """
    return (
        settings.owner_session_id is not None and settings.owner_session_generation_id is not None
    )


def _pipeline_route(
    route: JarvisStagingRoute,
    *,
    settings: RelaySettings,
    pipeline_id: str,
) -> JarvisPipelineInputRoute:
    return jarvis_pipeline_input_route(
        cluster=route.cluster,
        server_name=route.server_name,
        cluster_route_revision=route.cluster_route_revision,
        registration_revision=route.registration_revision,
        expected_server_artifact_digest=route.expected_server_artifact_digest,
        pipeline_id=pipeline_id,
        owner_session_id=settings.owner_session_id,
        owner_session_generation_id=settings.owner_session_generation_id,
    )


def _accepted_terminal_result(result: JSON) -> bool:
    return result.get("state") == "succeeded" and result.get("terminal") is True


def _fresh_idempotency_key(route: JarvisStagingRoute) -> str:
    return f"mcp:virtual:{route.cluster}:{route.server_name}:{route.remote_tool_name}:{uuid4().hex}"


def _required_argument(arguments: JSON, key: str) -> str:
    value = arguments.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _stable_digest(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
