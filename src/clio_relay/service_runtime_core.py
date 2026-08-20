"""Supervisor construction, transition locking, and the shared SSH transport.

Extracted from ``service_runtime.py`` (#231 rework slice, class-mixin split
§9): ``ServiceRuntimeSupervisor``'s ``__init__`` and the small, universally
depended-on primitives every other mixin calls into -- the per-gateway
cross-process transition lock (``_gateway_transition_lock_path``,
``_acquire_gateway_transition_lock``, ``_gateway_transition_lock``),
durable-session helpers (``_validate_gateway_transition_session``,
``_runtime_start_session_after_lock``, ``_update``,
``_set_ownership_intent``, ``_gateway_with_ownership_intent``), the shared
remote-command transport (``_ssh``), JARVIS authorization resolution
(``_jarvis_runtime_authorization``), and the two durable-failure recorders
(``_record_runtime_start_failure``, ``_record_attach_failure``).

This is a mixin, not a standalone class: it depends on ``self.settings``,
``self.queue``, ``self.cluster``, ``self.definition``, ``self.token``,
``self.secret_key``, ``self.runner``, and ``self.sleep``, all of which
``__init__`` here sets -- callers must include this mixin exactly once, and
first, in ``ServiceRuntimeSupervisor``'s base list (the class docstring in
``service_runtime.py`` records the full composition). Every other mixin
calls back into ``self._ssh``/``self._update``/the transition-lock helpers
freely; that is the whole point of a mixin split -- ``self`` resolves
through the composed class's MRO regardless of which mixin defines a given
attribute, so no qualification is needed for these cross-mixin calls.
"""

from __future__ import annotations

import hashlib
import subprocess
import time
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import cast

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from clio_relay import service_runtime_command_runner as _command_runner
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_types as _types
from clio_relay.cluster_config import ClusterDefinition, ensure_private_configuration_directory
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.jarvis_service_runtime import (
    VerifiedJarvisServiceRuntime,
    resolve_jarvis_service_runtime_authorization,
)
from clio_relay.models import GatewaySession, GatewaySessionState, utc_now

_GATEWAY_TEARDOWN_LOCK_TIMEOUT_SECONDS = 60.0
_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS = 120.0


class _ServiceRuntimeCoreMixin:
    """Construction, transition locking, and shared SSH transport."""

    def __init__(
        self,
        *,
        settings: RelaySettings,
        queue: ClioCoreQueue,
        cluster: str,
        definition: ClusterDefinition,
        token: str,
        secret_key: str,
        runner: _types.CommandRunner | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.queue = queue
        self.cluster = cluster
        self.definition = definition
        self.token = token
        self.secret_key = secret_key
        self.runner = runner or _command_runner.SubprocessCommandRunner()
        self.sleep = sleep

    def _jarvis_runtime_authorization(
        self,
        verified: VerifiedJarvisServiceRuntime,
    ) -> str | None:
        """Resolve per operation; callers may stdin-transfer only to the owned memory proxy."""
        return resolve_jarvis_service_runtime_authorization(
            definition=self.definition,
            settings=self.settings,
            verified=verified,
        )

    def _validate_gateway_transition_session(self, session: GatewaySession) -> None:
        """Require one exact relay-owned session before and after lock acquisition."""
        if session.cluster != self.cluster:
            raise ConfigurationError(
                f"gateway session {session.session_id} belongs to cluster {session.cluster}, "
                f"not {self.cluster}"
            )
        if session.metadata.get("owner") != "clio-relay":
            raise ConfigurationError(
                f"gateway session {session.session_id} is not an owned clio-relay runtime"
            )

    def _gateway_transition_lock_path(self, session_id: str) -> Path:
        """Return a private lock path keyed by the exact cluster and gateway session."""
        directory = self.queue.root / ".gateway-transition-locks"
        try:
            ensure_private_configuration_directory(directory)
        except (ConfigurationError, OSError) as exc:
            raise RelayError(
                "could not prepare the trusted gateway transition lock directory"
            ) from exc
        identity = hashlib.sha256(f"{self.cluster}\0{session_id}".encode()).hexdigest()
        return directory / f"{identity}.lock"

    def _acquire_gateway_transition_lock(self, session_id: str) -> FileLock:
        """Acquire and return the exact bounded cross-process transition lock."""
        lock_path = self._gateway_transition_lock_path(session_id)
        lock = FileLock(
            str(internal_filesystem_path(lock_path, force_extended=True)),
            timeout=_GATEWAY_TEARDOWN_LOCK_TIMEOUT_SECONDS,
        )
        try:
            lock.acquire()
        except FileLockTimeout as exc:
            raise RelayError("timed out acquiring the gateway transition lock") from exc
        except OSError as exc:
            raise RelayError("could not acquire the gateway transition lock") from exc
        return cast(FileLock, lock)

    @contextmanager
    def _gateway_transition_lock(self, session_id: str) -> Generator[None, None, None]:
        """Hold the bounded cross-process lock for one gateway state transition."""
        lock = self._acquire_gateway_transition_lock(session_id)
        try:
            yield
        finally:
            lock.release()

    def _runtime_start_session_after_lock(self, session_id: str) -> GatewaySession:
        """Reread and admit one newly created gateway before any runtime side effect."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if session.state is not GatewaySessionState.CREATED:
            raise ConfigurationError(
                f"gateway session {session_id} changed before runtime start acquired its lock"
            )
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot start"
            )
        return session

    def _set_ownership_intent(
        self,
        session: GatewaySession,
        role: str,
        intent: dict[str, object],
    ) -> GatewaySession:
        """Durably record one resource intent before or after its side effect."""
        gateway = self._gateway_with_ownership_intent(session, role, intent)
        return self._update(session, gateway=gateway)

    def _gateway_with_ownership_intent(
        self,
        session: GatewaySession,
        role: str,
        intent: dict[str, object],
        **gateway_updates: object,
    ) -> dict[str, object]:
        """Return a gateway payload containing an atomically paired intent update."""
        gateway = dict(session.gateway)
        intents = _primitives._object(gateway.get("ownership_intents", {}))
        intents[role] = intent
        gateway["ownership_intents"] = intents
        gateway.update(gateway_updates)
        return gateway

    def _update(
        self,
        session: GatewaySession,
        *,
        state: GatewaySessionState | None = None,
        metadata: dict[str, object] | None = None,
        **updates: object,
    ) -> GatewaySession:
        return self.queue.update_gateway_session(
            session.session_id,
            state=state,
            metadata=metadata,
            expected_updated_at=session.updated_at,
            **updates,
        )

    def _record_runtime_start_failure(
        self,
        *,
        session_id: str,
        error: BaseException,
        cleanup_errors: Sequence[str],
    ) -> None:
        """Persist a start failure against the latest post-cleanup session revision."""

        last_conflict: QueueConflictError | None = None
        for _attempt in range(3):
            current = self.queue.get_gateway_session(session_id)
            if current.state is GatewaySessionState.READY:
                return
            target_state = (
                GatewaySessionState.CLOSED
                if current.state is GatewaySessionState.CLOSED
                else GatewaySessionState.FAILED
            )
            try:
                self.queue.update_gateway_session(
                    session_id,
                    state=target_state,
                    expected_updated_at=current.updated_at,
                    metadata={
                        "failed_at": utc_now().isoformat(),
                        "last_error": str(error),
                        "cleanup_error": ("; ".join(dict.fromkeys(cleanup_errors)) or None),
                    },
                )
                return
            except QueueConflictError as exc:
                last_conflict = exc
        if last_conflict is not None:
            raise last_conflict

    def _record_attach_failure(
        self,
        *,
        session_id: str,
        error: BaseException,
        cleanup_error: str | None,
    ) -> None:
        """Record an attach failure only while the same gateway remains mutable."""

        if isinstance(error, QueueConflictError):
            return
        current = self.queue.get_gateway_session(session_id)
        if (
            current.state in {GatewaySessionState.READY, GatewaySessionState.CLOSED}
            or current.gateway.get("teardown_intent") is not None
        ):
            return
        try:
            self.queue.update_gateway_session(
                session_id,
                state=GatewaySessionState.DEGRADED,
                expected_updated_at=current.updated_at,
                metadata={
                    "attach_failed_at": utc_now().isoformat(),
                    "attach_error": str(error),
                    "attach_cleanup_error": cleanup_error,
                },
            )
        except QueueConflictError:
            return

    def _ssh(self, script: str) -> str:
        try:
            result = self.runner.run(
                ["ssh", self.definition.ssh_host, "bash", "-s"],
                input_text=script,
                timeout_seconds=_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise _types._AmbiguousRemoteSideEffectError(
                "remote service runtime command timed out after "
                f"{_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS:g} seconds"
            ) from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            if result.returncode == 255:
                raise _types._AmbiguousRemoteSideEffectError(
                    f"remote service runtime transport failed: {detail}"
                )
            raise RelayError(f"remote service runtime command failed: {detail}")
        return result.stdout
