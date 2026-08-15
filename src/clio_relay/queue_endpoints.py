"""Durable endpoint registration and heartbeat ownership."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_order_index,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import EndpointRegistration, utc_now


class _ReadinessOwner(Protocol):
    def readiness_info(self) -> dict[str, object]:
        """Return the facade's sealed queue-readiness evidence."""
        ...


def _endpoint_fresh_bucket(value: datetime) -> int:
    """Return the UTC minute bucket used by the live endpoint index."""
    observed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return int(observed.timestamp()) // queue_layout.ENDPOINT_FRESH_BUCKET_SECONDS


class QueueEndpointsMixin(queue_order_index.QueueOrderIndexMixin):
    """Own endpoint registration, heartbeat, and bounded discovery behavior."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol

    def register_endpoint(self, endpoint: EndpointRegistration) -> EndpointRegistration:
        """Create or refresh an endpoint registration with exact identity continuity."""
        queue_layout.QueueLayout.require_durable_record_id(
            endpoint.endpoint_id,
            field="endpoint_id",
        )
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            existing = queue_store_read.read_optional(
                self._storage_root,
                self._storage_root / "endpoints" / f"{endpoint.endpoint_id}.json",
                EndpointRegistration,
            )
            if existing is not None:
                existing_identity = (
                    existing.role,
                    existing.cluster,
                    existing.hostname,
                    existing.pid,
                    existing.registered_at,
                )
                requested_identity = (
                    endpoint.role,
                    endpoint.cluster,
                    endpoint.hostname,
                    endpoint.pid,
                    endpoint.registered_at,
                )
                if existing_identity != requested_identity:
                    raise QueueConflictError(
                        "endpoint identity or registration generation changed before heartbeat: "
                        f"{endpoint.endpoint_id}"
                    )
                endpoint = existing.model_copy(
                    update={"last_seen_at": utc_now(), "metadata": endpoint.metadata}
                )
            self._ensure_global_order_entry_unlocked("endpoints", endpoint.endpoint_id)
            queue_store_write.write_model(
                self._storage_root,
                self._storage_root / "endpoints" / f"{endpoint.endpoint_id}.json",
                endpoint,
            )
            self._index_fresh_endpoint_unlocked(endpoint)
        return endpoint

    def list_endpoints(self, cluster: str | None = None) -> list[EndpointRegistration]:
        """Return registered endpoints, optionally filtered by cluster."""
        self._store_adapter.initialize()
        endpoints = list(
            queue_store_read.read_many(
                self._storage_root / "endpoints",
                EndpointRegistration,
                identity_field="endpoint_id",
            )
        )
        if cluster is not None:
            endpoints = [endpoint for endpoint in endpoints if endpoint.cluster == cluster]
        return sorted(endpoints, key=lambda endpoint: endpoint.registered_at)

    def list_endpoints_page(
        self,
        *,
        cursor: int = 1,
        limit: int = 100,
        cluster: str | None = None,
    ) -> tuple[list[EndpointRegistration], int | None, int]:
        """Read one global endpoint source window with an in-window cluster filter."""

        def matches(endpoint: EndpointRegistration) -> bool:
            return cluster is None or endpoint.cluster == cluster

        return self._read_global_order_page(
            family="endpoints",
            model=EndpointRegistration,
            identity_field="endpoint_id",
            cursor=cursor,
            limit=limit,
            predicate=matches,
        )

    def scan_endpoints(
        self,
        *,
        limit: int,
        cluster: str | None = None,
    ) -> tuple[list[EndpointRegistration], bool]:
        """Read a bounded endpoint snapshot."""
        endpoints, truncated = queue_store_read.scan_many(
            self._storage_root / "endpoints",
            EndpointRegistration,
            limit=limit,
            identity_field="endpoint_id",
        )
        if cluster is not None:
            endpoints = [endpoint for endpoint in endpoints if endpoint.cluster == cluster]
        return sorted(endpoints, key=lambda endpoint: endpoint.registered_at), truncated

    def scan_fresh_endpoints(
        self,
        *,
        limit: int,
        fresh_seconds: int,
        cluster: str | None = None,
        now: datetime | None = None,
    ) -> tuple[list[EndpointRegistration], bool]:
        """Read only recent endpoint buckets, independent of endpoint history size."""
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        return self._scan_fresh_endpoint_index(
            limit=limit,
            fresh_seconds=fresh_seconds,
            cluster=cluster,
            now=now,
        )

    def scan_fresh_endpoints_read_only(
        self,
        *,
        limit: int,
        fresh_seconds: int,
        cluster: str,
        now: datetime | None = None,
    ) -> tuple[list[EndpointRegistration], bool]:
        """Read one cluster's sealed fresh-endpoint index without initialization.

        This path is for bootstrap/readiness probes. It proves the fixed queue
        layout and audit seal first, then reads only the requested cluster's
        bounded recent time buckets. It never creates, repairs, or migrates
        queue state and never scans the historical endpoint family.
        """
        readiness = cast(_ReadinessOwner, self).readiness_info()
        if readiness.get("complete") is not True or readiness.get("sealed") is not True:
            raise QueueConflictError("fresh endpoint readiness requires a sealed indexed queue")
        return self._scan_fresh_endpoint_index(
            limit=limit,
            fresh_seconds=fresh_seconds,
            cluster=cluster,
            now=now,
        )

    def _scan_fresh_endpoint_index(
        self,
        *,
        limit: int,
        fresh_seconds: int,
        cluster: str | None,
        now: datetime | None,
    ) -> tuple[list[EndpointRegistration], bool]:
        """Read bounded fresh endpoint buckets after the caller proves readiness."""
        if limit < 1 or limit > queue_layout.MAX_BOUNDED_SCAN_RECORDS:
            raise ValueError(
                f"endpoint scan limit must be between 1 and {queue_layout.MAX_BOUNDED_SCAN_RECORDS}"
            )
        if fresh_seconds < 1 or fresh_seconds > queue_layout.MAX_ENDPOINT_FRESH_SECONDS:
            raise ValueError(
                f"fresh_seconds must be between 1 and {queue_layout.MAX_ENDPOINT_FRESH_SECONDS}"
            )
        observed_at = now or utc_now()
        cutoff = observed_at - timedelta(seconds=fresh_seconds)
        first_bucket = _endpoint_fresh_bucket(cutoff)
        last_bucket = _endpoint_fresh_bucket(observed_at)
        roots: list[Path]
        overflow = False
        if cluster is not None:
            roots = [
                self._storage_root
                / "endpoints_fresh"
                / queue_order_index._stable_ref_token(cluster)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            ]
        else:
            roots = []
            with os.scandir(self._storage_root / "endpoints_fresh") as entries:
                for entry in entries:
                    if not entry.is_dir(follow_symlinks=False):
                        raise QueueConflictError(
                            f"fresh endpoint index contains an unsafe root: {entry.path}"
                        )
                    if len(roots) >= queue_layout.MAX_ENDPOINT_FRESH_CLUSTER_ROOTS:
                        overflow = True
                        break
                    roots.append(Path(entry.path))
            roots.sort(key=lambda path: path.name)
        by_id: dict[str, EndpointRegistration] = {}
        for cluster_root in roots:
            for bucket in range(last_bucket, first_bucket - 1, -1):
                remaining = limit - len(by_id)
                if remaining <= 0:
                    overflow = True
                    break
                bucket_root = cluster_root / f"{bucket:020d}"
                if not bucket_root.is_dir():
                    continue
                bucket_endpoints, truncated = queue_store_read.scan_many(
                    bucket_root,
                    EndpointRegistration,
                    limit=remaining,
                )
                overflow = overflow or truncated
                for indexed_endpoint in bucket_endpoints:
                    endpoint = self.get_endpoint(indexed_endpoint.endpoint_id)
                    if endpoint is None:
                        continue
                    if endpoint.last_seen_at < cutoff:
                        continue
                    if (
                        endpoint.last_seen_at > observed_at
                        and indexed_endpoint.last_seen_at > observed_at
                    ):
                        continue
                    if cluster is not None and endpoint.cluster != cluster:
                        raise QueueConflictError(
                            f"fresh endpoint cluster index mismatch: {endpoint.endpoint_id}"
                        )
                    previous = by_id.get(endpoint.endpoint_id)
                    if previous is None or previous.last_seen_at < endpoint.last_seen_at:
                        by_id[endpoint.endpoint_id] = endpoint
            if len(by_id) >= limit:
                break
        endpoints = sorted(by_id.values(), key=lambda endpoint: endpoint.registered_at)
        return endpoints, overflow

    def get_endpoint(self, endpoint_id: str) -> EndpointRegistration | None:
        """Return one exact endpoint registration when present."""
        endpoint_id = queue_layout.QueueLayout.require_durable_record_id(
            endpoint_id,
            field="endpoint_id",
        )
        endpoint = queue_store_read.read_optional(
            self._storage_root,
            self._storage_root / "endpoints" / f"{endpoint_id}.json",
            EndpointRegistration,
        )
        if endpoint is not None and endpoint.endpoint_id != endpoint_id:
            raise QueueConflictError(f"canonical endpoint identity mismatch: {endpoint_id}")
        return endpoint

    def _index_fresh_endpoint_unlocked(self, endpoint: EndpointRegistration) -> None:
        """Move one endpoint's mutable presence record into its current time bucket."""
        cluster_identity = endpoint.cluster or "__desktop__"
        cluster_token = queue_order_index._stable_ref_token(cluster_identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        bucket = _endpoint_fresh_bucket(endpoint.last_seen_at)
        mapping_path = (
            self._storage_root
            / "endpoints_fresh_by_id"
            / f"{queue_order_index._stable_ref_token(endpoint.endpoint_id)}.json"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        )
        previous: dict[str, object] | None = None
        try:
            raw_previous = queue_store_read.read_json_document(mapping_path)
        except FileNotFoundError:
            raw_previous = None
        if raw_previous is not None:
            if not isinstance(raw_previous, dict):
                raise QueueConflictError(f"fresh endpoint mapping is invalid: {mapping_path}")
            previous = cast(dict[str, object], raw_previous)
            if (
                previous.get("schema_version") != "clio-relay.endpoint-fresh-index.v1"
                or previous.get("endpoint_id") != endpoint.endpoint_id
                or not isinstance(previous.get("cluster_token"), str)
                or isinstance(previous.get("bucket"), bool)
                or not isinstance(previous.get("bucket"), int)
            ):
                raise QueueConflictError(
                    f"fresh endpoint mapping identity mismatch: {mapping_path}"
                )
        target = (
            self._storage_root
            / "endpoints_fresh"
            / cluster_token
            / f"{bucket:020d}"
            / f"{endpoint.endpoint_id}.json"
        )
        if previous is not None:
            previous_target = (
                self._storage_root
                / "endpoints_fresh"
                / cast(str, previous["cluster_token"])
                / f"{cast(int, previous['bucket']):020d}"
                / f"{endpoint.endpoint_id}.json"
            )
            if previous_target != target:
                queue_store_write.unlink_durable_path(previous_target, missing_ok=True)
        queue_store_write.write_model(self._storage_root, target, endpoint)
        queue_store_write.write_json(
            self._storage_root,
            mapping_path,
            {
                "schema_version": "clio-relay.endpoint-fresh-index.v1",
                "endpoint_id": endpoint.endpoint_id,
                "cluster_token": cluster_token,
                "bucket": bucket,
                "last_seen_at": endpoint.last_seen_at.isoformat(),
            },
        )
