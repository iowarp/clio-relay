"""Remote/allocation connector lifecycle for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 19a, class-mixin
split): ``_start_remote_connector`` (launch either a plain SSH-forwarded
frpc process or, when an allocation provider/job id is given, a
scheduler-allocation-scoped connector bound to one exact provider-native
step), ``_allocation_connector_identity`` (validate the complete
provider/allocation/placement/step ownership chain for an allocation
connector), ``_poll_allocation_connector_step``/``_stop_allocation_connector``/
``_retained_allocation_connector_resource`` (poll, cancel-and-prove-absent,
or prove-still-active one exact scheduler connector step). The two
connector-step cleanup timing constants move with their only caller.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.definition``, ``self.cluster``,
``self.token``, ``self.secret_key``, ``self.sleep``) and calls back into
sibling mixins through ``self`` (``self._ssh``, ``self._set_ownership_intent``).
Python's MRO resolves every one of those through whichever mixin defines it
regardless of call origin, so no cross-mixin qualification is used. The
class docstring in ``service_runtime.py`` records the full mixin
composition.
"""

from __future__ import annotations

import json
import math

from clio_relay import service_runtime_connector_step_scripts as _connector_step_scripts
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_submission_scripts as _submission_scripts
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.frp_link import FrpLinkConfig, render_proxy_config
from clio_relay.frp_remote_scripts import (
    remote_allocation_frpc_start_script as _remote_allocation_frpc_start_script,
)
from clio_relay.frp_remote_scripts import (
    remote_frpc_start_script as _remote_frpc_start_script,
)
from clio_relay.models import (
    GatewaySession,
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    SchedulerConnectorStepStatus,
    ServiceRuntimeSpec,
)
from clio_relay.relay_host import FrpTransportProtocol
from clio_relay.scheduler_providers import (
    SchedulerAllocationConnectorProvider,
    provider_for_scheduler,
)
from clio_relay.session_wire_models import CleanupResource

_CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS = 30.0
_CONNECTOR_STEP_CLEANUP_POLL_SECONDS = 0.25


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
        allocation_provider: str | None = None,
        allocation_job_id: str | None = None,
    ) -> dict[str, object]:
        if (allocation_provider is None) != (allocation_job_id is None):
            raise ConfigurationError(
                "allocation_provider and allocation_job_id must be provided together"
            )
        placement: SchedulerConnectorPlacement | None = None
        step_marker: str | None = None
        if allocation_provider is not None and allocation_job_id is not None:
            provider = provider_for_scheduler(allocation_provider)
            if not isinstance(provider, SchedulerAllocationConnectorProvider):
                raise ConfigurationError(
                    f"scheduler provider {allocation_provider!r} cannot launch an "
                    "allocation-scoped connector"
                )
            raw_placement = _scheduler_contracts._last_json_object(
                self._ssh(
                    _submission_scripts._remote_scheduler_script(
                        definition=self.definition,
                        operation="connector-placement",
                        provider=allocation_provider,
                        scheduler_job_id=allocation_job_id,
                    )
                )
            )
            try:
                placement = SchedulerConnectorPlacement.model_validate_json(
                    json.dumps(raw_placement, separators=(",", ":"), allow_nan=False)
                )
            except ValueError as exc:
                raise RelayError(
                    "scheduler provider returned invalid connector placement evidence"
                ) from exc
            if (
                placement.scheduler != allocation_provider
                or placement.scheduler_job_id != allocation_job_id
                or placement.allocation_node_count != 1
                or placement.verified is not True
            ):
                raise RelayError("scheduler connector placement identity did not match binding")
            step_marker = _scheduler_contracts._connector_step_marker(
                session.session_id,
                _scheduler_contracts._required_intent_str(
                    ownership_intent,
                    "connector_generation_id",
                ),
            )
            ownership_intent = _scheduler_contracts._new_ownership_intent(
                "starting",
                owner_token=_scheduler_contracts._required_intent_str(
                    ownership_intent, "owner_token"
                ),
                connector_generation_id=_scheduler_contracts._required_intent_str(
                    ownership_intent,
                    "connector_generation_id",
                ),
                execution_scope="scheduler_allocation",
                scheduler_provider=allocation_provider,
                scheduler_native_id=allocation_job_id,
                scheduler_step_marker=step_marker,
                placement=placement.model_dump(mode="json"),
            )
            # Persist the allocation, placement, and unique step marker before
            # A detached ``srun`` can create a scheduler-side process.
            self._set_ownership_intent(
                session,
                "remote_connector",
                ownership_intent,
            )
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
        if allocation_provider is not None and allocation_job_id is not None:
            if placement is None or step_marker is None:
                raise AssertionError("allocation placement and step marker were not resolved")
            output = self._ssh(
                _remote_allocation_frpc_start_script(
                    definition=self.definition,
                    session_id=session.session_id,
                    config_text=config,
                    owner_token=owner_token,
                    connector_generation_id=connector_generation_id,
                    allocation_provider=allocation_provider,
                    allocation_job_id=allocation_job_id,
                    placement=placement,
                    step_marker=step_marker,
                )
            )
            start_result = _scheduler_contracts._last_json_object(output)
            if start_result.get("schema_version") != "clio-relay.allocation-connector-start.v1":
                raise RelayError("allocation connector start returned the wrong schema")
            if (
                start_result.get("session_id") != session.session_id
                or start_result.get("connector_generation_id") != connector_generation_id
            ):
                raise RelayError("allocation connector start identity did not match its intent")
            raw_step = start_result.get("step_identity")
            try:
                step_identity = SchedulerConnectorStepIdentity.model_validate_json(
                    json.dumps(raw_step, separators=(",", ":"), allow_nan=False)
                )
            except (TypeError, ValueError) as exc:
                raise RelayError(
                    "allocation connector start returned invalid scheduler step identity"
                ) from exc
            if (
                step_identity.scheduler != allocation_provider
                or step_identity.scheduler_job_id != allocation_job_id
                or step_identity.placement_host != placement.placement_host
                or step_identity.step_marker != step_marker
                or step_identity.verified is not True
            ):
                raise RelayError("allocation connector scheduler step identity did not match")
            config_path = _primitives._optional_str(start_result.get("config_path"))
            log_path = _primitives._optional_str(start_result.get("log_path"))
            if config_path is None or log_path is None:
                raise RelayError("allocation connector start omitted its owned paths")
            return {
                "owner": "clio-relay",
                "session_id": session.session_id,
                "execution_scope": "scheduler_allocation",
                "scheduler_provider": allocation_provider,
                "scheduler_native_id": allocation_job_id,
                "scheduler_step_id": step_identity.scheduler_step_id,
                "scheduler_step_marker": step_marker,
                "scheduler_step": step_identity.model_dump(mode="json"),
                "connector_generation_id": connector_generation_id,
                "owner_token": owner_token,
                "config_path": config_path,
                "log_path": log_path,
                "placement": placement.model_dump(mode="json"),
            }
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
            provider = provider_for_scheduler(provider_name)
        except ConfigurationError as exc:
            raise RelayError(f"allocation connector provider is invalid: {exc}") from exc
        if not isinstance(provider, SchedulerAllocationConnectorProvider):
            raise RelayError("allocation connector provider lacks step lifecycle semantics")
        if (
            provider.name != provider_name
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

    def _poll_allocation_connector_step(
        self,
        identity: SchedulerConnectorStepIdentity,
    ) -> SchedulerConnectorStepStatus:
        """Poll one exact provider-native connector step over the cluster boundary."""
        output = self._ssh(
            _connector_step_scripts._remote_connector_step_status_script(
                definition=self.definition,
                provider=identity.scheduler,
                scheduler_job_id=identity.scheduler_job_id,
                scheduler_step_id=identity.scheduler_step_id,
                placement_host=identity.placement_host,
            )
        )
        try:
            status = SchedulerConnectorStepStatus.model_validate_json(
                json.dumps(
                    _scheduler_contracts._last_json_object(output),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("scheduler returned invalid connector step status") from exc
        if (
            status.scheduler != identity.scheduler
            or status.scheduler_job_id != identity.scheduler_job_id
            or status.scheduler_step_id != identity.scheduler_step_id
            or status.placement_host != identity.placement_host
            or status.verified is not True
        ):
            raise RelayError("scheduler connector step status identity did not match")
        return status

    def _stop_allocation_connector(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Cancel one exact scheduler step and prove its compute-node absence."""
        identity = self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(identity)
        cancel_error: str | None = None
        canceled = False
        if status.state == "active":
            try:
                result = _scheduler_contracts._last_json_object(
                    self._ssh(
                        _connector_step_scripts._remote_connector_step_cancel_script(
                            definition=self.definition,
                            provider=identity.scheduler,
                            scheduler_job_id=identity.scheduler_job_id,
                            scheduler_step_id=identity.scheduler_step_id,
                        )
                    )
                )
                if (
                    result.get("scheduler") != identity.scheduler
                    or result.get("scheduler_job_id") != identity.scheduler_job_id
                    or result.get("scheduler_step_id") != identity.scheduler_step_id
                    or result.get("cancel_requested") is not True
                    or result.get("accepted") is not True
                    or result.get("returncode") != 0
                ):
                    raise RelayError("scheduler did not accept exact connector-step cancellation")
                canceled = True
            except RelayError as exc:
                cancel_error = str(exc)
            attempts = max(
                1,
                math.ceil(
                    _CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS / _CONNECTOR_STEP_CLEANUP_POLL_SECONDS
                ),
            )
            for attempt in range(attempts):
                status = self._poll_allocation_connector_step(identity)
                if status.state == "absent":
                    break
                if attempt + 1 < attempts:
                    self.sleep(_CONNECTOR_STEP_CLEANUP_POLL_SECONDS)
        if status.state != "absent":
            detail = "scheduler connector step remains active after exact-step cancellation"
            if cancel_error is not None:
                detail = f"{detail}: {cancel_error}"
            raise RelayError(detail)
        return CleanupResource(
            kind="remote_connector",
            resource_id=identity.scheduler_step_id,
            location=identity.placement_host,
            provider=identity.scheduler,
            action="stop",
            ownership_verified=True,
            outcome="stopped" if canceled else "missing",
            verified_after_operation=True,
            observed_state="absent",
            detail=(
                "exact scheduler connector step absence confirmed"
                + (f" after cancellation error: {cancel_error}" if cancel_error else "")
            ),
            metadata={
                "scheduler_job_id": identity.scheduler_job_id,
                "scheduler_step_id": identity.scheduler_step_id,
                "scheduler_step_marker": identity.step_marker,
                "placement_host": identity.placement_host,
                "parent_scheduler_job_retained": True,
            },
        )

    def _retained_allocation_connector_resource(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Prove that a detached allocation-scoped connector remains active."""
        identity = self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(identity)
        retained = status.state == "active"
        return CleanupResource(
            kind="remote_connector",
            resource_id=identity.scheduler_step_id,
            location=identity.placement_host,
            provider=identity.scheduler,
            action="retain",
            ownership_verified=True,
            outcome="retained" if retained else "failed",
            verified_after_operation=True,
            observed_state=status.state,
            residual=not retained,
            detail=(
                "exact scheduler connector step retained for reattachment"
                if retained
                else "scheduler confirms the allocation connector step is absent"
            ),
            metadata={
                "scheduler_job_id": identity.scheduler_job_id,
                "scheduler_step_id": identity.scheduler_step_id,
                "scheduler_step_marker": identity.step_marker,
                "placement_host": identity.placement_host,
                "parent_scheduler_job_retained": True,
            },
        )
