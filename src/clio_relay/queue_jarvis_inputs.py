"""Durable JARVIS input-contract, binding, lineage, and manifest ownership.

CQ20-JI-01 (dissolved CQ1 deviation): CQ1's original zero-inbound peel used
instance composition (``ClioCoreQueue.__init__`` held a private
``QueueJarvisInputs`` helper object) rather than mixin inheritance, since it
landed before ``QueueStoreProtocol``/``_QueueStoreAdapter`` had any other
owner to model the pattern on. Every public method below was still a
facade-resident delegator (``return self._jarvis_inputs.get_x(...)``) at the
top of the CQ1-CQ19 split. CQ20 dissolves that gap now that the pattern is
established everywhere else: this module is a real ``QueueJarvisInputsMixin``
composed directly into ``ClioCoreQueue`` like every CQ3+ owner, and each
method reads ``self._store_adapter`` -- the same private store adapter every
other owner already depends on -- instead of a separately held reference.
The eight public methods keep their exact prior names and signatures; the
facade no longer defines any of them.
"""

from __future__ import annotations

from clio_relay import queue_context
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    ArtifactUse,
    JarvisPackageInputContractRecord,
    JarvisPackageInputRoute,
    JarvisPipelineInputBinding,
    JarvisPipelineInputBindings,
    JarvisPipelineInputLineage,
    JarvisPipelineInputRoute,
    JarvisRunInputManifest,
    utc_now,
)

MAX_JARVIS_PACKAGE_INPUT_CONTRACT_RECORDS = 10_000
MAX_JARVIS_PIPELINE_INPUT_BINDING_RECORDS = 10_000
MAX_JARVIS_PIPELINE_INPUT_LINEAGE_RECORDS = 10_000
MAX_JARVIS_RUN_INPUT_MANIFEST_RECORDS = 10_000


class QueueJarvisInputsMixin:
    """Own durable JARVIS input records through the composed private store adapter."""

    _store_adapter: queue_context.QueueStoreProtocol

    def get_jarvis_package_input_contract(
        self,
        route: JarvisPackageInputRoute,
    ) -> JarvisPackageInputContractRecord | None:
        """Load one exact checksum-bound package input contract record."""
        self._store_adapter.initialize()
        route_sha256 = route.identity_sha256()
        path = (
            self._store_adapter.storage_root
            / "jarvis_package_input_contracts"
            / f"{route_sha256}.json"
        )
        with self._store_adapter.lock:
            record = self._store_adapter.read_optional(path, JarvisPackageInputContractRecord)
            if record is None:
                return None
            if record.route != route or record.route_sha256 != route_sha256:
                raise QueueConflictError(
                    "durable JARVIS package input contract route identity changed"
                )
            return record

    def put_jarvis_package_input_contract(
        self,
        record: JarvisPackageInputContractRecord,
    ) -> JarvisPackageInputContractRecord:
        """Persist immutable package semantics for one exact registered route."""
        self._store_adapter.initialize()
        route_sha256 = record.route.identity_sha256()
        if record.route_sha256 != route_sha256:
            raise ValueError("package input contract route checksum changed before persistence")
        directory = self._store_adapter.storage_root / "jarvis_package_input_contracts"
        path = directory / f"{route_sha256}.json"
        with self._store_adapter.lock:
            existing = self._store_adapter.read_optional(path, JarvisPackageInputContractRecord)
            if existing is not None:
                if (
                    existing.route != record.route
                    or existing.package_names != record.package_names
                    or existing.local_file_settings != record.local_file_settings
                    or existing.settings_sha256 != record.settings_sha256
                ):
                    raise QueueConflictError(
                        "immutable JARVIS package input contract changed on the same route"
                    )
                return existing
            count, over_capacity = self._store_adapter.bounded_regular_json_count(
                directory,
                limit=MAX_JARVIS_PACKAGE_INPUT_CONTRACT_RECORDS,
                label="JARVIS package input contract",
            )
            if over_capacity or count >= MAX_JARVIS_PACKAGE_INPUT_CONTRACT_RECORDS:
                raise QueueConflictError(
                    "JARVIS package input contracts reached their bounded record capacity"
                )
            self._store_adapter.write(path, record)
            return record

    def get_jarvis_pipeline_input_lineage(
        self,
        route: JarvisPipelineInputRoute,
    ) -> JarvisPipelineInputLineage | None:
        """Load one exact checksum-bound pipeline input lineage record."""
        self._store_adapter.initialize()
        route_sha256 = route.identity_sha256()
        path = (
            self._store_adapter.storage_root
            / "jarvis_pipeline_input_lineage"
            / f"{route_sha256}.json"
        )
        with self._store_adapter.lock:
            record = self._store_adapter.read_optional(path, JarvisPipelineInputLineage)
            if record is None:
                return None
            if record.route != route or record.route_sha256 != route_sha256:
                raise QueueConflictError(
                    "durable JARVIS pipeline input lineage route identity changed"
                )
            return record

    def get_jarvis_pipeline_input_bindings(
        self,
        route: JarvisPipelineInputRoute,
    ) -> JarvisPipelineInputBindings | None:
        """Load current local-file bindings for one exact registered pipeline route."""
        self._store_adapter.initialize()
        route_sha256 = route.identity_sha256()
        path = (
            self._store_adapter.storage_root
            / "jarvis_pipeline_input_bindings"
            / f"{route_sha256}.json"
        )
        with self._store_adapter.lock:
            record = self._store_adapter.read_optional(path, JarvisPipelineInputBindings)
            if record is None:
                return None
            if record.route != route or record.route_sha256 != route_sha256:
                raise QueueConflictError(
                    "durable JARVIS pipeline input bindings route identity changed"
                )
            return record

    def update_jarvis_pipeline_input_bindings(
        self,
        route: JarvisPipelineInputRoute,
        *,
        upserts: tuple[JarvisPipelineInputBinding, ...] = (),
        remove: tuple[tuple[str, str], ...] = (),
    ) -> JarvisPipelineInputBindings:
        """Atomically update exact step/setting bindings for one pipeline route."""
        self._store_adapter.initialize()
        route_sha256 = route.identity_sha256()
        directory = self._store_adapter.storage_root / "jarvis_pipeline_input_bindings"
        path = directory / f"{route_sha256}.json"
        remove_set = set(remove)
        if len(remove_set) != len(remove):
            raise ValueError("pipeline input binding removals must be unique")
        upsert_identities = [item.identity() for item in upserts]
        if len(upsert_identities) != len(set(upsert_identities)):
            raise ValueError("pipeline input binding upserts must be unique")
        if remove_set.intersection(upsert_identities):
            raise ValueError("pipeline input binding cannot be removed and upserted together")
        with self._store_adapter.lock:
            existing = self._store_adapter.read_optional(path, JarvisPipelineInputBindings)
            mutation_at = utc_now()
            if existing is None:
                count, over_capacity = self._store_adapter.bounded_regular_json_count(
                    directory,
                    limit=MAX_JARVIS_PIPELINE_INPUT_BINDING_RECORDS,
                    label="JARVIS pipeline input bindings",
                )
                if over_capacity or count >= MAX_JARVIS_PIPELINE_INPUT_BINDING_RECORDS:
                    raise QueueConflictError(
                        "JARVIS pipeline input bindings reached their bounded record capacity"
                    )
                by_identity: dict[tuple[str, str], JarvisPipelineInputBinding] = {}
                created_at = mutation_at
            else:
                if existing.route != route or existing.route_sha256 != route_sha256:
                    raise QueueConflictError(
                        "durable JARVIS pipeline input bindings route identity changed"
                    )
                by_identity = {item.identity(): item for item in existing.bindings}
                created_at = existing.created_at
            for identity in remove_set:
                by_identity.pop(identity, None)
            for item in upserts:
                by_identity[item.identity()] = item
            record = JarvisPipelineInputBindings.create(
                route=route,
                bindings=tuple(by_identity.values()),
                created_at=created_at,
                updated_at=mutation_at,
            )
            self._store_adapter.write(path, record)
            return record

    def get_jarvis_run_input_manifest(
        self,
        route: JarvisPipelineInputRoute,
        *,
        idempotency_key: str,
    ) -> JarvisRunInputManifest | None:
        """Load an immutable input manifest for one exact jarvis_run admission."""
        self._store_adapter.initialize()
        identity_sha256 = JarvisRunInputManifest.storage_identity_sha256(
            route=route,
            idempotency_key=idempotency_key,
        )
        path = (
            self._store_adapter.storage_root
            / "jarvis_run_input_manifests"
            / f"{identity_sha256}.json"
        )
        with self._store_adapter.lock:
            record = self._store_adapter.read_optional(path, JarvisRunInputManifest)
            if record is None:
                return None
            if (
                record.route != route
                or record.idempotency_key != idempotency_key
                or record.identity_sha256() != identity_sha256
            ):
                raise QueueConflictError("durable JARVIS run input manifest identity changed")
            return record

    def put_jarvis_run_input_manifest(
        self,
        record: JarvisRunInputManifest,
    ) -> JarvisRunInputManifest:
        """Persist the first exact input manifest admitted for one run key."""
        self._store_adapter.initialize()
        identity_sha256 = record.identity_sha256()
        directory = self._store_adapter.storage_root / "jarvis_run_input_manifests"
        path = directory / f"{identity_sha256}.json"
        with self._store_adapter.lock:
            existing = self._store_adapter.read_optional(path, JarvisRunInputManifest)
            if existing is not None:
                if (
                    existing.route != record.route
                    or existing.idempotency_key != record.idempotency_key
                    or existing.identity_sha256() != identity_sha256
                ):
                    raise QueueConflictError("durable JARVIS run input manifest identity changed")
                return existing
            count, over_capacity = self._store_adapter.bounded_regular_json_count(
                directory,
                limit=MAX_JARVIS_RUN_INPUT_MANIFEST_RECORDS,
                label="JARVIS run input manifest",
            )
            if over_capacity or count >= MAX_JARVIS_RUN_INPUT_MANIFEST_RECORDS:
                raise QueueConflictError(
                    "JARVIS run input manifests reached their bounded record capacity"
                )
            self._store_adapter.write(path, record)
            return record

    def merge_jarvis_pipeline_input_lineage(
        self,
        route: JarvisPipelineInputRoute,
        artifact_uses: tuple[ArtifactUse, ...],
        *,
        manifest_sha256: str,
    ) -> JarvisPipelineInputLineage:
        """Atomically merge staged inputs for one exact registered pipeline route."""
        if not artifact_uses:
            raise ValueError("pipeline input lineage requires at least one artifact use")
        if len(manifest_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in manifest_sha256
        ):
            raise ValueError("pipeline input lineage manifest must be a canonical SHA-256")
        self._store_adapter.initialize()
        route_sha256 = route.identity_sha256()
        directory = self._store_adapter.storage_root / "jarvis_pipeline_input_lineage"
        path = directory / f"{route_sha256}.json"
        with self._store_adapter.lock:
            existing = self._store_adapter.read_optional(path, JarvisPipelineInputLineage)
            mutation_at = utc_now()
            if existing is None:
                count, over_capacity = self._store_adapter.bounded_regular_json_count(
                    directory,
                    limit=MAX_JARVIS_PIPELINE_INPUT_LINEAGE_RECORDS,
                    label="JARVIS pipeline input lineage",
                )
                if over_capacity or count >= MAX_JARVIS_PIPELINE_INPUT_LINEAGE_RECORDS:
                    raise QueueConflictError(
                        "JARVIS pipeline input lineage reached its bounded record capacity"
                    )
                merged_uses = artifact_uses
                manifests = (manifest_sha256,)
                created_at = mutation_at
            else:
                if existing.route != route or existing.route_sha256 != route_sha256:
                    raise QueueConflictError(
                        "durable JARVIS pipeline input lineage route identity changed"
                    )
                by_artifact = {item.artifact_id: item for item in existing.artifact_uses}
                for item in artifact_uses:
                    previous = by_artifact.get(item.artifact_id)
                    if previous is not None and previous != item:
                        raise QueueConflictError(
                            f"pipeline input artifact identity changed: {item.artifact_id}"
                        )
                    by_artifact[item.artifact_id] = item
                merged_uses = tuple(
                    sorted(by_artifact.values(), key=lambda item: (item.artifact_id, item.sha256))
                )
                manifests = (*existing.manifest_sha256s, manifest_sha256)
                created_at = existing.created_at
            record = JarvisPipelineInputLineage.create(
                route=route,
                artifact_uses=merged_uses,
                manifest_sha256s=manifests,
                created_at=created_at,
                updated_at=mutation_at,
            )
            self._store_adapter.write(path, record)
            return record
