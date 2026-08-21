"""Checksum-bound JARVIS package input contracts.

The registered, package-level (not per-pipeline-instance) description of
which local-file settings one JARVIS-CD package accepts
(:class:`JarvisPackageInputContractRecord`), keyed by an exact immutable
registered route (:class:`JarvisPackageInputRoute`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.models_shared import (
    MAX_JARVIS_PACKAGE_INPUT_CONTRACT_BYTES,
    _canonical_json_sha256,
    _require_canonical_json_size,
    utc_now,
)


class JarvisPackageInputRoute(BaseModel):
    """Exact immutable registered route used to describe one JARVIS package."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-package-input-route.v1"] = (
        "clio-relay.jarvis-package-input-route.v1"
    )
    cluster: str = Field(min_length=1, max_length=256)
    server_name: str = Field(min_length=1, max_length=256)
    contract: Literal["clio-kit-jarvis-user-v3.7.2"] = "clio-kit-jarvis-user-v3.7.2"
    cluster_route_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    registration_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_server_artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_name: str = Field(min_length=1, max_length=512)

    def identity_sha256(self) -> str:
        """Return the canonical route-and-package storage identity."""
        return _canonical_json_sha256(self.model_dump(mode="json"))


class JarvisPackageLocalFileInput(BaseModel):
    """Closed local-file setting names learned from a package description."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_name: str = Field(min_length=1, max_length=512)
    accepted_names: tuple[Annotated[str, Field(min_length=1, max_length=512)], ...] = Field(
        min_length=1,
        max_length=64,
    )

    @model_validator(mode="after")
    def accepted_names_are_unique_and_canonical(self) -> Self:
        """Require the canonical spelling first and reject ambiguous aliases."""
        if self.accepted_names[0] != self.canonical_name:
            raise ValueError("package local-file accepted names must start with the canonical name")
        if len(self.accepted_names) != len(set(self.accepted_names)):
            raise ValueError("package local-file accepted names must be unique")
        return self


class JarvisPackageInputContractRecord(BaseModel):
    """Checksum-bound package input semantics for one exact immutable route."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-package-input-contract.v1"] = (
        "clio-relay.jarvis-package-input-contract.v1"
    )
    route: JarvisPackageInputRoute
    route_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_names: tuple[Annotated[str, Field(min_length=1, max_length=512)], ...] = Field(
        min_length=1, max_length=64
    )
    local_file_settings: tuple[JarvisPackageLocalFileInput, ...] = Field(max_length=1_000)
    settings_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def identity_and_document_are_exact(self) -> Self:
        """Reject route substitution, ambiguous settings, and record mutation."""
        if self.route_sha256 != self.route.identity_sha256():
            raise ValueError("package input route checksum does not match its identity")
        if self.route.package_name not in self.package_names:
            raise ValueError("package input route name is absent from the described package names")
        if len(self.package_names) != len(set(self.package_names)):
            raise ValueError("described package names must be unique")
        accepted_names: set[str] = set()
        for setting in self.local_file_settings:
            overlap = accepted_names.intersection(setting.accepted_names)
            if overlap:
                raise ValueError("package local-file setting names and aliases must be unique")
            accepted_names.update(setting.accepted_names)
        if self.document_sha256 != _jarvis_package_input_contract_sha256(self):
            raise ValueError("package input contract checksum does not match its document")
        _require_canonical_json_size(
            self.model_dump(mode="json"),
            label="JARVIS package input contract",
            maximum=MAX_JARVIS_PACKAGE_INPUT_CONTRACT_BYTES,
        )
        return self

    @classmethod
    def create(
        cls,
        *,
        route: JarvisPackageInputRoute,
        package_names: tuple[str, ...],
        local_file_settings: tuple[JarvisPackageLocalFileInput, ...],
        settings_sha256: str,
        created_at: datetime | None = None,
    ) -> JarvisPackageInputContractRecord:
        """Create one validated immutable package-input contract record."""
        provisional = cls.model_construct(
            route=route,
            route_sha256=route.identity_sha256(),
            package_names=package_names,
            local_file_settings=local_file_settings,
            settings_sha256=settings_sha256,
            created_at=created_at or utc_now(),
            document_sha256="0" * 64,
        )
        return cls.model_validate(
            {
                **provisional.model_dump(mode="python"),
                "document_sha256": _jarvis_package_input_contract_sha256(provisional),
            }
        )


def _jarvis_package_input_contract_sha256(record: JarvisPackageInputContractRecord) -> str:
    """Hash every durable package-input field except the checksum itself."""
    return _canonical_json_sha256(record.model_dump(mode="json", exclude={"document_sha256"}))
