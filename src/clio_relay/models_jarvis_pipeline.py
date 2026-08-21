"""Checksum-bound JARVIS pipeline staged-input lineage, bindings, and run manifests.

Per-pipeline-instance (owner-session-scoped) staged local-file input state:
the tamper-evident historical lineage (:class:`JarvisPipelineInputLineage`),
the current binding set (:class:`JarvisPipelineInputBindings`), and the
immutable per-admission resolved manifest a ``jarvis_run`` execution pins
(:class:`JarvisRunInputManifest`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_artifact_provenance import (
    ArtifactUse,
    artifact_use_payload,
    validate_artifact_use_collection,
)
from clio_relay.models_shared import (
    MAX_JARVIS_PIPELINE_INPUT_BINDINGS_BYTES,
    MAX_JARVIS_RUN_INPUT_MANIFEST_BYTES,
    _canonical_json_sha256,
    _require_canonical_json_size,
    utc_now,
)


class JarvisPipelineInputRoute(BaseModel):
    """Exact registered route and owner generation for staged pipeline inputs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-pipeline-input-route.v1"] = (
        "clio-relay.jarvis-pipeline-input-route.v1"
    )
    cluster: str = Field(min_length=1, max_length=256)
    server_name: str = Field(min_length=1, max_length=256)
    contract: Literal["clio-kit-jarvis-user-v3.7.2"] = "clio-kit-jarvis-user-v3.7.2"
    cluster_route_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    registration_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_server_artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    pipeline_id: str = Field(min_length=1, max_length=512)
    owner_session_id: str = Field(min_length=1, max_length=256)
    owner_session_generation_id: DurableRecordId

    def identity_sha256(self) -> str:
        """Return the canonical content identity used as the durable record key."""
        return _canonical_json_sha256(self.model_dump(mode="json"))


class JarvisPipelineInputLineage(BaseModel):
    """Tamper-evident staged-input lineage for one exact JARVIS pipeline route."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-pipeline-input-lineage.v1"] = (
        "clio-relay.jarvis-pipeline-input-lineage.v1"
    )
    route: JarvisPipelineInputRoute
    route_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifact_uses: tuple[ArtifactUse, ...] = Field(max_length=1_000)
    manifest_sha256s: tuple[str, ...] = Field(max_length=1_000)
    created_at: datetime
    updated_at: datetime
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("artifact_uses")
    @classmethod
    def artifact_uses_must_be_unique_and_canonical(
        cls,
        value: tuple[ArtifactUse, ...],
    ) -> tuple[ArtifactUse, ...]:
        """Forbid ambiguous duplicate artifacts and non-canonical record order."""
        canonical = tuple(sorted(value, key=lambda item: (item.artifact_id, item.sha256)))
        if value != canonical:
            raise ValueError("pipeline input artifact uses must be canonically ordered")
        identities = [item.artifact_id for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("pipeline input artifact uses must have unique artifact IDs")
        validate_artifact_use_collection(value)
        return value

    @field_validator("manifest_sha256s")
    @classmethod
    def manifests_must_be_unique_canonical_sha256s(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        """Require a sorted set of canonical staging-manifest identities."""
        if value != tuple(sorted(set(value))):
            raise ValueError("pipeline input manifests must be unique and canonically ordered")
        if any(
            len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
            for item in value
        ):
            raise ValueError("pipeline input manifests must be canonical SHA-256 digests")
        return value

    @model_validator(mode="after")
    def checksums_must_match_exact_document(self) -> JarvisPipelineInputLineage:
        """Detect route substitution or record mutation before lineage is reused."""
        if self.route_sha256 != self.route.identity_sha256():
            raise ValueError("pipeline input route checksum does not match its identity")
        if self.updated_at < self.created_at:
            raise ValueError("pipeline input lineage updated_at predates created_at")
        if self.document_sha256 != _jarvis_pipeline_input_lineage_sha256(self):
            raise ValueError("pipeline input lineage checksum does not match its document")
        return self

    @classmethod
    def create(
        cls,
        *,
        route: JarvisPipelineInputRoute,
        artifact_uses: tuple[ArtifactUse, ...],
        manifest_sha256s: tuple[str, ...],
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> JarvisPipelineInputLineage:
        """Create one validated checksum-bound durable lineage record."""
        created = created_at or utc_now()
        updated = updated_at or created
        provisional = cls.model_construct(
            route=route,
            route_sha256=route.identity_sha256(),
            artifact_uses=tuple(
                sorted(artifact_uses, key=lambda item: (item.artifact_id, item.sha256))
            ),
            manifest_sha256s=tuple(sorted(set(manifest_sha256s))),
            created_at=created,
            updated_at=updated,
            document_sha256="0" * 64,
        )
        return cls.model_validate(
            {
                **provisional.model_dump(mode="python"),
                "document_sha256": _jarvis_pipeline_input_lineage_sha256(provisional),
            }
        )


class JarvisPipelineInputBinding(BaseModel):
    """One stable Host-workspace reference bound to an exact pipeline step setting."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    step_id: str = Field(min_length=1, max_length=512)
    canonical_setting: str = Field(min_length=1, max_length=512)
    accepted_names: tuple[Annotated[str, Field(min_length=1, max_length=512)], ...] = Field(
        min_length=1,
        max_length=64,
    )
    workspace_relative_path: str = Field(min_length=1, max_length=4096)
    logical_name: str = Field(min_length=1, max_length=512)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    remote_path: str = Field(min_length=1, max_length=4096)
    artifact_use: ArtifactUse

    @model_validator(mode="after")
    def identity_and_paths_are_exact(self) -> JarvisPipelineInputBinding:
        """Reject aliases, paths, or artifact evidence that can change meaning."""
        if self.accepted_names[0] != self.canonical_setting:
            raise ValueError("pipeline input accepted names must start with the canonical setting")
        if len(self.accepted_names) != len(set(self.accepted_names)):
            raise ValueError("pipeline input accepted names must be unique")
        if (
            "\\" in self.workspace_relative_path
            or self.workspace_relative_path.startswith("/")
            or any(
                component in {"", ".", ".."}
                for component in self.workspace_relative_path.split("/")
            )
        ):
            raise ValueError("pipeline input workspace path must be normalized and relative")
        if (
            not self.remote_path.startswith("/")
            or self.remote_path.startswith("//")
            or any(ord(character) < 32 or ord(character) == 127 for character in self.remote_path)
        ):
            raise ValueError("pipeline input remote path must be an absolute control-free path")
        if self.artifact_use.sha256 != self.sha256:
            raise ValueError("pipeline input artifact digest does not match its local snapshot")
        provenance = self.artifact_use.provenance
        if provenance is None or provenance.arg != self.canonical_setting:
            raise ValueError("pipeline input artifact provenance must name its canonical setting")
        return self

    def identity(self) -> tuple[str, str]:
        """Return the exact step-and-setting identity within one pipeline route."""
        return (self.step_id, self.canonical_setting)


class JarvisPipelineInputBindings(BaseModel):
    """Checksum-bound current local-file bindings for one exact pipeline route."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-pipeline-input-bindings.v1"] = (
        "clio-relay.jarvis-pipeline-input-bindings.v1"
    )
    route: JarvisPipelineInputRoute
    route_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bindings: tuple[JarvisPipelineInputBinding, ...] = Field(max_length=1_000)
    created_at: datetime
    updated_at: datetime
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def identity_and_document_are_exact(self) -> JarvisPipelineInputBindings:
        """Reject route substitution, duplicate settings, and record mutation."""
        if self.route_sha256 != self.route.identity_sha256():
            raise ValueError("pipeline input bindings route checksum does not match its identity")
        canonical = tuple(sorted(self.bindings, key=lambda item: item.identity()))
        if self.bindings != canonical:
            raise ValueError("pipeline input bindings must be canonically ordered")
        identities = [item.identity() for item in self.bindings]
        if len(identities) != len(set(identities)):
            raise ValueError("pipeline input bindings must have unique step and setting identities")
        if self.updated_at < self.created_at:
            raise ValueError("pipeline input bindings updated_at predates created_at")
        if self.document_sha256 != _jarvis_pipeline_input_bindings_sha256(self):
            raise ValueError("pipeline input bindings checksum does not match its document")
        _require_canonical_json_size(
            self.model_dump(mode="json"),
            label="JARVIS pipeline input bindings",
            maximum=MAX_JARVIS_PIPELINE_INPUT_BINDINGS_BYTES,
        )
        return self

    @classmethod
    def create(
        cls,
        *,
        route: JarvisPipelineInputRoute,
        bindings: tuple[JarvisPipelineInputBinding, ...],
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> JarvisPipelineInputBindings:
        """Create one validated current-binding document."""
        created = created_at or utc_now()
        updated = updated_at or created
        provisional = cls.model_construct(
            route=route,
            route_sha256=route.identity_sha256(),
            bindings=tuple(sorted(bindings, key=lambda item: item.identity())),
            created_at=created,
            updated_at=updated,
            document_sha256="0" * 64,
        )
        return cls.model_validate(
            {
                **provisional.model_dump(mode="python"),
                "document_sha256": _jarvis_pipeline_input_bindings_sha256(provisional),
            }
        )


class JarvisRunInputResolution(BaseModel):
    """One exact input selected for a newly admitted JARVIS execution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    binding: JarvisPipelineInputBinding
    disposition: Literal["reused", "updated"]
    previous_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def disposition_matches_content(self) -> JarvisRunInputResolution:
        """Bind reuse/update evidence to the before and after content digests."""
        unchanged = self.previous_sha256 == self.binding.sha256
        if unchanged != (self.disposition == "reused"):
            raise ValueError("JARVIS run input disposition does not match its content digest")
        return self


class JarvisRunInputManifest(BaseModel):
    """Immutable resolved local-input manifest for one jarvis_run admission key."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-run-input-manifest.v1"] = (
        "clio-relay.jarvis-run-input-manifest.v1"
    )
    route: JarvisPipelineInputRoute
    route_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    idempotency_key: str = Field(min_length=1, max_length=4096)
    resolutions: tuple[JarvisRunInputResolution, ...] = Field(min_length=1, max_length=1_000)
    artifact_uses: tuple[ArtifactUse, ...] = Field(min_length=1, max_length=1_000)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @staticmethod
    def storage_identity_sha256(
        *,
        route: JarvisPipelineInputRoute,
        idempotency_key: str,
    ) -> str:
        """Return the exact route-and-admission storage identity."""
        return _canonical_json_sha256(
            {
                "schema_version": "clio-relay.jarvis-run-input-manifest-identity.v1",
                "route_sha256": route.identity_sha256(),
                "idempotency_key": idempotency_key,
            }
        )

    def identity_sha256(self) -> str:
        """Return this manifest's exact route-and-admission storage identity."""
        return self.storage_identity_sha256(
            route=self.route,
            idempotency_key=self.idempotency_key,
        )

    @model_validator(mode="after")
    def identity_and_document_are_exact(self) -> JarvisRunInputManifest:
        """Reject admission, binding, artifact, or checksum substitution."""
        if self.route_sha256 != self.route.identity_sha256():
            raise ValueError("JARVIS run input route checksum does not match its identity")
        canonical_resolutions = tuple(
            sorted(self.resolutions, key=lambda item: item.binding.identity())
        )
        if self.resolutions != canonical_resolutions:
            raise ValueError("JARVIS run input resolutions must be canonically ordered")
        identities = [item.binding.identity() for item in self.resolutions]
        if len(identities) != len(set(identities)):
            raise ValueError("JARVIS run input resolutions must have unique binding identities")
        expected_uses = tuple(
            sorted(
                (item.binding.artifact_use for item in self.resolutions),
                key=lambda item: (item.artifact_id, item.sha256),
            )
        )
        if self.artifact_uses != expected_uses:
            raise ValueError("JARVIS run input artifact uses do not match its resolutions")
        validate_artifact_use_collection(self.artifact_uses)
        if self.manifest_sha256 != _jarvis_run_input_manifest_payload_sha256(self):
            raise ValueError("JARVIS run input manifest checksum does not match its resolutions")
        if self.document_sha256 != _jarvis_run_input_manifest_document_sha256(self):
            raise ValueError("JARVIS run input manifest checksum does not match its document")
        _require_canonical_json_size(
            self.model_dump(mode="json"),
            label="JARVIS run input manifest",
            maximum=MAX_JARVIS_RUN_INPUT_MANIFEST_BYTES,
        )
        return self

    @classmethod
    def create(
        cls,
        *,
        route: JarvisPipelineInputRoute,
        idempotency_key: str,
        resolutions: tuple[JarvisRunInputResolution, ...],
        created_at: datetime | None = None,
    ) -> JarvisRunInputManifest:
        """Create one exact immutable per-admission input manifest."""
        canonical_resolutions = tuple(sorted(resolutions, key=lambda item: item.binding.identity()))
        artifact_uses = tuple(
            sorted(
                (item.binding.artifact_use for item in canonical_resolutions),
                key=lambda item: (item.artifact_id, item.sha256),
            )
        )
        provisional = cls.model_construct(
            route=route,
            route_sha256=route.identity_sha256(),
            idempotency_key=idempotency_key,
            resolutions=canonical_resolutions,
            artifact_uses=artifact_uses,
            manifest_sha256="0" * 64,
            created_at=created_at or utc_now(),
            document_sha256="0" * 64,
        )
        with_manifest = provisional.model_copy(
            update={
                "manifest_sha256": _jarvis_run_input_manifest_payload_sha256(provisional),
            }
        )
        return cls.model_validate(
            {
                **with_manifest.model_dump(mode="python"),
                "document_sha256": _jarvis_run_input_manifest_document_sha256(with_manifest),
            }
        )


def _jarvis_pipeline_input_lineage_sha256(record: JarvisPipelineInputLineage) -> str:
    """Hash every durable lineage field except the checksum itself."""
    payload = record.model_dump(mode="json", exclude={"document_sha256"})
    payload["artifact_uses"] = [artifact_use_payload(item) for item in record.artifact_uses]
    return _canonical_json_sha256(payload)


def _jarvis_pipeline_input_bindings_sha256(record: JarvisPipelineInputBindings) -> str:
    """Hash every current-binding field except the checksum itself."""
    return _canonical_json_sha256(record.model_dump(mode="json", exclude={"document_sha256"}))


def _jarvis_run_input_manifest_payload_sha256(record: JarvisRunInputManifest) -> str:
    """Hash the exact resolved input selection independently from record metadata."""
    return _canonical_json_sha256(
        {
            "route_sha256": record.route_sha256,
            "idempotency_key": record.idempotency_key,
            "resolutions": [item.model_dump(mode="json") for item in record.resolutions],
            "artifact_uses": [artifact_use_payload(item) for item in record.artifact_uses],
        }
    )


def _jarvis_run_input_manifest_document_sha256(record: JarvisRunInputManifest) -> str:
    """Hash every immutable run-manifest field except the document checksum."""
    return _canonical_json_sha256(record.model_dump(mode="json", exclude={"document_sha256"}))
