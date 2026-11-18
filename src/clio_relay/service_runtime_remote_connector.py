"""Remote connector lifecycle for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 19a, class-mixin
split): ``_start_remote_connector`` launches a plain SSH-forwarded frpc
process. Allocation-scoped connectors (an frpc step inside a scheduler
allocation) are retired: start refuses them, and
``_stop_allocation_connector``/``_retained_allocation_connector_resource``
validate a record left by an older relay and report the step as a residual a
person must cancel. ``_allocation_connector_identity`` validates that record.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.definition``, ``self.cluster``,
``self.token``, ``self.secret_key``) and calls back into sibling mixins
through ``self`` (``self._ssh``). Python's MRO resolves every one of those
through whichever mixin defines it regardless of call origin, so no
cross-mixin qualification is used. The class docstring in
``service_runtime.py`` records the full mixin composition.
"""

from __future__ import annotations

import json
from typing import Literal

from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_link import FrpLinkConfig, render_proxy_config
from clio_relay.frp_remote_scripts import (
    remote_frpc_start_script as _remote_frpc_start_script,
)
from clio_relay.models import (
    GatewaySession,
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    ServiceRuntimeSpec,
)
from clio_relay.relay_host import FrpTransportProtocol
from clio_relay.service_runtime_scheduler_names import (
    allocation_connector_residual,
    canonical_scheduler,
    supports_allocation_connector,
)
from clio_relay.session_wire_models import CleanupResource


class _ServiceRuntimeRemoteConnectorMixin:
    """Launch, poll, and stop the remote (SSH/allocation-scoped) connector."""

    def _start_remote_connector(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        node: str,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        transport = self.definition.frp_transport
        server_addr = _primitives._require_server_addr(transport.server_addr, self.cluster)
        config = render_proxy_config(
            FrpLinkConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                protocol=FrpTransportProtocol(transport.protocol),
                token=self.token,
                secret_key=self.secret_key,
                proxy_name=proxy_name,
            ),
            proxy_type=_primitives._frp_proxy_type(spec.transport_mode),
            local_ip=node,
            local_port=spec.service_port,
        )
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent,
            "connector_generation_id",
        )
        output = self._ssh(
            _remote_frpc_start_script(
                definition=self.definition,
                session_id=session.session_id,
                config_text=config,
                owner_token=owner_token,
                connector_generation_id=connector_generation_id,
            )
        )
        metadata = _scheduler_contracts._key_value_output(output)
        expected_fields = {
            "remote_frpc_pid",
            "remote_frpc_pgid",
            "connector_generation_id",
            "remote_frpc_config",
            "remote_frpc_log",
        }
        if set(metadata) != expected_fields:
            raise RelayError("remote connector start returned an invalid response shape")
        try:
            pid = int(metadata["remote_frpc_pid"])
            process_group_id = int(metadata["remote_frpc_pgid"])
        except ValueError as exc:
            raise RelayError("remote connector start returned an invalid process identity") from exc
        if pid <= 0 or process_group_id != pid:
            raise RelayError("remote connector start returned an invalid process group identity")
        if metadata["connector_generation_id"] != connector_generation_id:
            raise RelayError("remote connector start identity did not match its durable intent")
        config_path = _scheduler_contracts._validated_remote_session_file(
            metadata["remote_frpc_config"],
            session_id=session.session_id,
            filename="remote-frpc.toml",
        )
        log_path = _scheduler_contracts._validated_remote_session_file(
            metadata["remote_frpc_log"],
            session_id=session.session_id,
            filename="remote-frpc.log",
        )
        if config_path.parent != log_path.parent:
            raise RelayError("remote connector start returned paths from different sessions")
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": pid,
            "process_group_id": process_group_id,
            "connector_generation_id": connector_generation_id,
            "owner_token": owner_token,
            "config_path": config_path.as_posix(),
            "log_path": log_path.as_posix(),
        }
        return connector

    def _allocation_connector_identity(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> SchedulerConnectorStepIdentity:
        """Validate exact provider, allocation, placement, and step ownership."""
        if (
            connector.get("owner") != "clio-relay"
            or connector.get("session_id") != session_id
            or connector.get("execution_scope") != "scheduler_allocation"
            or connector.get("pid") is not None
            or connector.get("process_group_id") is not None
        ):
            raise RelayError("allocation connector ownership record is invalid")
        try:
            step = SchedulerConnectorStepIdentity.model_validate_json(
                json.dumps(
                    connector.get("scheduler_step"),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
            placement = SchedulerConnectorPlacement.model_validate_json(
                json.dumps(
                    connector.get("placement"),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("allocation connector has invalid provider-native identity") from exc
        generation_id = _primitives._optional_str(connector.get("connector_generation_id"))
        provider_name = _primitives._optional_str(connector.get("scheduler_provider"))
        scheduler_job_id = _primitives._optional_str(connector.get("scheduler_native_id"))
        scheduler_step_id = _primitives._optional_str(connector.get("scheduler_step_id"))
        step_marker = _primitives._optional_str(connector.get("scheduler_step_marker"))
        config_path = _primitives._optional_str(connector.get("config_path"))
        log_path = _primitives._optional_str(connector.get("log_path"))
        if None in {
            generation_id,
            provider_name,
            scheduler_job_id,
            scheduler_step_id,
            step_marker,
            config_path,
            log_path,
        }:
            raise RelayError("allocation connector ownership record is incomplete")
        assert generation_id is not None
        assert provider_name is not None
        assert scheduler_job_id is not None
        assert scheduler_step_id is not None
        assert step_marker is not None
        try:
            canonical_provider = canonical_scheduler(provider_name)
        except ConfigurationError as exc:
            raise RelayError(f"allocation connector provider is invalid: {exc}") from exc
        if not supports_allocation_connector(canonical_provider):
            raise RelayError("allocation connector provider lacks step lifecycle semantics")
        if (
            canonical_provider != provider_name
            or step.scheduler != provider_name
            or step.scheduler_job_id != scheduler_job_id
            or step.scheduler_step_id != scheduler_step_id
            or step.step_marker != step_marker
            or step_marker != _scheduler_contracts._connector_step_marker(session_id, generation_id)
            or placement.scheduler != provider_name
            or placement.scheduler_job_id != scheduler_job_id
            or placement.placement_host != step.placement_host
            or placement.allocation_node_count != 1
            or step.verified is not True
            or placement.verified is not True
        ):
            raise RelayError("allocation connector identities disagree")
        return step

    def _stop_allocation_connector(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Report a connector step left by an older relay as a residual to cancel."""
        return self._allocation_connector_residual(
            session_id=session_id,
            connector=connector,
            action="stop",
        )

    def _retained_allocation_connector_resource(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Report a connector step left by an older relay as a residual on detach."""
        return self._allocation_connector_residual(
            session_id=session_id,
            connector=connector,
            action="retain",
        )

    def _allocation_connector_residual(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
        action: Literal["stop", "retain"],
    ) -> CleanupResource:
        identity = self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        return CleanupResource(
            kind="remote_connector",
            resource_id=identity.scheduler_step_id,
            location=identity.placement_host,
            provider=identity.scheduler,
            action=action,
            ownership_verified=True,
            outcome="refused",
            verified_after_operation=False,
            residual=True,
            detail=allocation_connector_residual(
                scheduler_job_id=identity.scheduler_job_id,
                scheduler_step_id=identity.scheduler_step_id,
            ),
            metadata={
                "scheduler_job_id": identity.scheduler_job_id,
                "scheduler_step_id": identity.scheduler_step_id,
                "scheduler_step_marker": identity.step_marker,
                "placement_host": identity.placement_host,
                "parent_scheduler_job_retained": True,
            },
        )
