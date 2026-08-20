"""Desktop-connector process stop for ``ServiceRuntimeSupervisor``.

Extracted from ``service_runtime.py`` (#231 rework slice 18b, class-mixin
split, kept as a separate seam from the rest of the observation cluster
since crossing 800 lines there): ``_stop_local_connector`` -- the shared
desktop-connector termination primitive called from start rollback, stop,
detach, attach rollback, and browser-proxy revocation -- and its
``_remove_unpublished_local_connector_files`` cleanup for a connector that
failed before durable publication.

This is a mixin, not a standalone class: it depends on attributes set by
``_ServiceRuntimeCoreMixin.__init__`` (``self.settings``). Python's MRO
resolves every cross-mixin call through whichever mixin defines it
regardless of call origin, so no cross-mixin qualification is used. The
class docstring in ``service_runtime.py`` records the full mixin
composition.
"""

from __future__ import annotations

from pathlib import Path

from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_primitives as _primitives
from clio_relay.errors import RelayError
from clio_relay.session_wire_models import CleanupResource


class _ServiceRuntimeLocalConnectorMixin:
    """Stop the desktop connector process and clean up its unpublished files."""

    def _stop_local_connector(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
        require_record: bool = False,
        absence_verified: bool = False,
    ) -> tuple[int | None, CleanupResource]:
        pid = _primitives._optional_int(connector.get("pid"))
        config_path = _primitives._optional_str(connector.get("config_path"))
        expected_directory = (
            self.settings.core_dir.parent / "runtime-sessions" / session_id
        ).resolve()
        config_owned = False
        if config_path is not None:
            try:
                config_owned = Path(config_path).resolve().parent == expected_directory
            except OSError:
                config_owned = False
        owned = (
            connector.get("owner") == "clio-relay"
            and connector.get("session_id") == session_id
            and config_owned
        )
        resource_id = str(pid) if pid is not None else session_id
        identity_status, identity_detail = _connector_identity._local_connector_identity_status(
            connector
        )
        if pid is None:
            residual = require_record and not absence_verified
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=absence_verified,
                outcome="refused" if residual else "missing",
                verified_after_operation=absence_verified,
                residual=residual,
                detail=(
                    "owned desktop connector record is missing"
                    if residual
                    else "no desktop connector was recorded"
                ),
            )
        if identity_status in {"missing", "replaced"}:
            try:
                no_group_members = not _connector_identity._local_connector_group_members(connector)
            except RelayError as exc:
                return None, CleanupResource(
                    kind="desktop_connector",
                    resource_id=resource_id,
                    location="desktop",
                    action="stop",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=str(exc),
                )
            durable_identity = (
                owned
                and _primitives._optional_str(connector.get("owner_token")) is not None
                and _primitives._optional_int(connector.get("process_group_id")) is not None
                and _primitives._optional_str(connector.get("process_start_marker")) is not None
                and no_group_members
            )
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=durable_identity,
                outcome="missing" if durable_identity else "refused",
                verified_after_operation=durable_identity,
                residual=not durable_identity,
                detail=identity_detail,
            )
        if not owned or identity_status != "owned":
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=False,
                outcome="refused",
                residual=True,
                detail=identity_detail
                or "connector process does not match the owned session record",
            )
        try:
            stopped = _connector_identity._terminate_local_connector(connector)
            residual = bool(_connector_identity._local_connector_group_members(connector))
        except RelayError as exc:
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=False,
                outcome="failed",
                residual=True,
                detail=str(exc),
            )
        return stopped, CleanupResource(
            kind="desktop_connector",
            resource_id=resource_id,
            location="desktop",
            action="stop",
            ownership_verified=True,
            outcome="failed" if residual else "stopped",
            verified_after_operation=not residual,
            residual=residual,
            detail="connector still running after termination" if residual else None,
        )

    def _remove_unpublished_local_connector_files(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> None:
        """Remove private files for a connector that failed before durable publication."""

        expected_directory = (
            self.settings.core_dir.parent / "runtime-sessions" / session_id
        ).resolve()
        paths: list[Path] = []
        for field in ("config_path", "stdout_path", "stderr_path", "metadata_path"):
            raw_path = _primitives._optional_str(connector.get(field))
            if raw_path is None:
                raise RelayError(f"unpublished desktop connector omitted {field}")
            path = Path(raw_path).resolve()
            if path.parent != expected_directory:
                raise RelayError("unpublished desktop connector path escaped its runtime directory")
            paths.append(path)
        try:
            for path in paths:
                path.unlink(missing_ok=True)
        except OSError as exc:
            raise RelayError("could not remove unpublished desktop connector files") from exc
