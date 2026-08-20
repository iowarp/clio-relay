"""Local process start and HTTP health waits for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 19b, class-mixin
split): ``_start_local_visitor`` (launch the owned desktop-side frpc visitor
process) and ``_start_browser_proxy`` (write the browser gateway config and
launch its owned capability proxy without ever placing either secret on
disk), plus the three bounded HTTP health waits they are followed by --
``_wait_for_jarvis_health`` (schema-versioned anonymous-vs-authenticated
boundary proof), ``_wait_for_browser_health`` (proves exact
sandbox-origin CORS), and ``_wait_for_local_health``. The
``_LOCAL_CONNECTOR_WRAPPER_CODE`` and ``_MAX_LOCAL_HEALTH_BYTES`` constants
move with their only callers.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.settings``, ``self.definition``,
``self.cluster``, ``self.token``, ``self.secret_key``, ``self.runner``,
``self.sleep``). Python's MRO resolves every cross-mixin call through
whichever mixin defines it regardless of call origin, so no cross-mixin
qualification is used. The class docstring in ``service_runtime.py``
records the full mixin composition.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Literal, cast

import httpx

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_types as _types
from clio_relay.browser_gateway import (
    CAPABILITY_ENV,
    UPSTREAM_AUTHORIZATION_ENV,
    BrowserGatewayBootstrap,
    BrowserGatewayConfig,
)
from clio_relay.errors import RelayError
from clio_relay.frp_link import FrpLinkConfig, start_owned_frp_visitor
from clio_relay.jarvis_service_runtime import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V1,
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
)
from clio_relay.models import GatewaySession, ServiceRuntimeSpec
from clio_relay.relay_host import FrpTransportProtocol

_LOCAL_CONNECTOR_WRAPPER_CODE = (
    "import subprocess,sys; "
    "_owner_token=sys.argv[1]; "
    "_generation_id=sys.argv[2]; "
    "child=subprocess.Popen(sys.argv[3:]); "
    "raise SystemExit(child.wait())"
)
_MAX_LOCAL_HEALTH_BYTES = 64 * 1024


class _ServiceRuntimeLocalStartMixin:
    """Start the local desktop connectors and wait for their health."""

    def _start_local_visitor(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        transport = self.definition.frp_transport
        server_addr = _primitives._require_server_addr(transport.server_addr, self.cluster)
        runtime_dir = self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "config_path")
        ).resolve()
        stdout_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stdout_path")
        ).resolve()
        stderr_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stderr_path")
        ).resolve()
        metadata_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "metadata_path")
        ).resolve()
        owned_paths = (config_path, stdout_path, stderr_path, metadata_path)
        if any(path.parent != runtime_dir.resolve() for path in owned_paths):
            raise RelayError("desktop connector ownership intent escaped its runtime directory")
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent,
            "connector_generation_id",
        )
        visitor_type = _primitives._frp_proxy_type(spec.transport_mode)
        visitor = start_owned_frp_visitor(
            frpc_bin=self.settings.frpc_bin,
            config=FrpLinkConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                protocol=FrpTransportProtocol(transport.protocol),
                token=self.token,
                secret_key=self.secret_key,
                proxy_name=proxy_name,
            ),
            local_bind_addr=spec.desktop_bind_addr,
            local_bind_port=spec.desktop_bind_port,
            visitor_type=visitor_type,
            keep_tunnel_open=visitor_type == "xtcp",
            config_path=config_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            owner_token=owner_token,
            connector_generation_id=connector_generation_id,
            command_prefix=[
                sys.executable,
                "-c",
                _LOCAL_CONNECTOR_WRAPPER_CODE,
                owner_token,
                connector_generation_id,
            ],
            process_factory=self.runner.popen,
            identity_factory=self.runner.local_process_identity,
            rollback=_primitives._terminate_just_started_process_group,
        )
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": visitor.pid,
            "process_group_id": visitor.process_group_id,
            "process_start_marker": visitor.process_start_marker,
            "owner_token": visitor.owner_token,
            "connector_generation_id": connector_generation_id,
            "config_path": str(visitor.config_path),
            "stdout_path": str(visitor.stdout_path),
            "stderr_path": str(visitor.stderr_path),
            "metadata_path": str(metadata_path),
        }
        _connector_identity._write_local_connector_sidecar(metadata_path, connector)
        return connector

    def _start_browser_proxy(
        self,
        *,
        session: GatewaySession,
        config: BrowserGatewayConfig,
        capability: str,
        upstream_authorization: str | None,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        """Start one owned capability proxy without placing either secret on disk."""
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        config_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "config_path")
        ).resolve()
        stdout_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stdout_path")
        ).resolve()
        stderr_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stderr_path")
        ).resolve()
        metadata_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "metadata_path")
        ).resolve()
        if any(
            path.parent != runtime_dir
            for path in (config_path, stdout_path, stderr_path, metadata_path)
        ):
            raise RelayError("browser proxy ownership intent escaped its runtime directory")
        temporary = config_path.with_suffix(f"{config_path.suffix}.{os.getpid()}.tmp")
        temporary.write_text(config.model_dump_json(indent=2) + "\n", encoding="utf-8")
        temporary.chmod(0o600)
        os.replace(temporary, config_path)
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent, "connector_generation_id"
        )
        environment = os.environ.copy()
        environment.pop(CAPABILITY_ENV, None)
        environment.pop(UPSTREAM_AUTHORIZATION_ENV, None)
        environment["CLIO_RELAY_CONNECTOR_OWNER_TOKEN"] = owner_token
        environment["CLIO_RELAY_CONNECTOR_GENERATION_ID"] = generation_id
        bootstrap = (
            BrowserGatewayBootstrap(
                capability=capability,
                upstream_authorization=upstream_authorization,
            )
            .model_dump_json()
            .encode("utf-8")
        )
        process = self.runner.popen(
            [
                sys.executable,
                "-c",
                _LOCAL_CONNECTOR_WRAPPER_CODE,
                owner_token,
                generation_id,
                sys.executable,
                "-m",
                "clio_relay.browser_gateway",
                "--config",
                str(config_path),
                "--process-label",
                "clio-relay-browser-frpc-proxy",
            ],
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=environment,
            isolate_process_group=True,
            input_bytes=bootstrap,
        )
        try:
            identity = self.runner.local_process_identity(
                pid=process.pid,
                owner_token=owner_token,
                expected_config=str(config_path),
            )
        except BaseException:
            _primitives._terminate_just_started_process_group(process.pid)
            raise
        proxy: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "attachment_id": config.attachment_id,
            "pid": process.pid,
            "process_group_id": identity.process_group_id,
            "process_start_marker": identity.process_start_marker,
            "owner_token": identity.owner_token,
            "connector_generation_id": generation_id,
            "config_path": str(config_path),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "metadata_path": str(metadata_path),
        }
        _connector_identity._write_local_connector_sidecar(metadata_path, proxy)
        return proxy

    def _wait_for_jarvis_health(
        self,
        health_url: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
        runtime_schema_version: Literal["jarvis.service-runtime.v1", "jarvis.service-runtime.v2"],
        authorization: str | None,
        max_attempts: int | None = None,
    ) -> None:
        """Prove the versioned JARVIS HTTP authorization boundary is live."""
        if runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
            if authorization is not None:
                raise _types._DefinitiveRuntimeObservationError(
                    "legacy JARVIS service runtime unexpectedly resolved authorization"
                )
        elif runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V2:
            if authorization is None:
                raise _types._DefinitiveRuntimeObservationError(
                    "authenticated JARVIS service runtime omitted authorization"
                )
        else:
            raise _types._DefinitiveRuntimeObservationError(
                "JARVIS service runtime schema is unsupported"
            )
        deadline = time.monotonic() + timeout_seconds
        last_error = "no response"
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            try:
                anonymous = _readiness._read_bounded_http_response(
                    health_url,
                    headers=None,
                    maximum_bytes=None,
                    deadline=deadline,
                )
                if runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
                    if 200 <= anonymous.status_code < 300:
                        return
                    last_error = f"legacy anonymous health status={anonymous.status_code}"
                else:
                    if 200 <= anonymous.status_code < 300:
                        raise _types._DefinitiveRuntimeObservationError(
                            "authenticated JARVIS service health accepted an anonymous request"
                        )
                    if anonymous.status_code != 401:
                        last_error = f"anonymous health status={anonymous.status_code}"
                    else:
                        authenticated = _readiness._read_bounded_http_response(
                            health_url,
                            headers={"Authorization": cast(str, authorization)},
                            maximum_bytes=None,
                            deadline=deadline,
                        )
                        if 200 <= authenticated.status_code < 300:
                            return
                        if authenticated.status_code in {401, 403}:
                            raise _types._DefinitiveRuntimeObservationError(
                                "authenticated JARVIS service rejected its verified authority"
                            )
                        last_error = f"authenticated health status={authenticated.status_code}"
            except httpx.HTTPError:
                last_error = "HTTP transport failed"
            if max_attempts is not None and attempts >= max_attempts:
                break
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"JARVIS service health boundary was not ready: {last_error}")

    def _wait_for_browser_health(
        self,
        health_url: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> None:
        """Prove the capability proxy forwards health with exact sandbox-origin CORS."""
        deadline = time.monotonic() + timeout_seconds
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                response = _readiness._read_bounded_http_response(
                    health_url,
                    headers={"Origin": "null"},
                    maximum_bytes=None,
                    deadline=deadline,
                )
                if (
                    200 <= response.status_code < 300
                    and response.headers.get("access-control-allow-origin") == "null"
                ):
                    return
                last_error = (
                    f"status={response.status_code}; "
                    "access-control-allow-origin was not exactly null"
                )
            except httpx.HTTPError:
                last_error = "HTTP transport failed"
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"browser capability gateway did not become ready: {last_error}")

    def _wait_for_local_health(
        self,
        health_url: str,
        timeout_seconds: float,
        poll_seconds: float,
        *,
        expected_body: str | None = None,
        max_attempts: int | None = None,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: str | None = None
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            try:
                response = _readiness._read_bounded_http_response(
                    health_url,
                    headers=None,
                    maximum_bytes=_MAX_LOCAL_HEALTH_BYTES,
                    deadline=deadline,
                )
                if 200 <= response.status_code < 300:
                    if expected_body is None or response.content == expected_body.encode("utf-8"):
                        return
                    last_error = "HTTP response body did not match the runtime identity"
                else:
                    last_error = f"HTTP {response.status_code}"
            except httpx.HTTPError as exc:
                last_error = str(exc)
            if max_attempts is not None and attempts >= max_attempts:
                break
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"local service health probe failed: {health_url}: {last_error}")
