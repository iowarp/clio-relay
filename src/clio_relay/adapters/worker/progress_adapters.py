"""Plugin registry for package-owned progress providers."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Protocol, cast

import yaml

from clio_relay.errors import ConfigurationError

PACKAGE_PROGRESS_ENTRYPOINT_GROUP = "clio_relay.package_progress_adapters"


class PackageProgressAdapter(Protocol):
    """Application-specific adapter implemented outside relay core."""

    package_name: str
    package_version: str
    run_id: str
    adapter_name: str
    application_profile: str | None

    def observe_jarvis_stdout(self, text: str) -> list[dict[str, object]]:
        """Extract candidate package progress from JARVIS-scoped stdout."""
        ...

    def observe_stdout(self, text: str) -> list[dict[str, object]]:
        """Extract candidate progress from already trusted package stdout."""
        ...

    def finalize_jarvis_stdout(self) -> list[dict[str, object]]:
        """Flush candidates buffered until JARVIS stdout reaches EOF."""
        ...

    def finalize_stdout(self) -> list[dict[str, object]]:
        """Flush candidates buffered until package-owned log streams reach EOF."""
        ...

    def reset_stdout(self) -> None:
        """Reset package-log parser state after source replacement or truncation."""
        ...

    def progress_log_paths(self) -> list[Path]:
        """Return package-owned log paths that may emit live progress."""
        ...

    def package_load_probe_python(self) -> str | None:
        """Return optional Python used to verify the package implementation."""
        ...

    def acceptance_progress_valid(self, metadata: dict[str, Any]) -> bool:
        """Validate application-specific acceptance metadata."""
        ...


AdapterFactory = Callable[[dict[str, Any]], PackageProgressAdapter | None]


@dataclass(frozen=True)
class PackageProgressProviderIdentity:
    """Immutable identity of the distribution and adapter selected for a package."""

    entry_point_name: str
    entry_point_value: str
    distribution_name: str
    distribution_version: str
    adapter_name: str
    package_name: str
    package_version: str
    application_profile: str | None

    def as_metadata(self) -> dict[str, object]:
        """Return provider identity using the durable progress metadata schema."""
        metadata: dict[str, object] = {
            "adapter": self.adapter_name,
            "package_name": self.package_name,
            "package_version": self.package_version,
            "provider_entry_point": self.entry_point_name,
            "provider_entry_point_value": self.entry_point_value,
            "provider_distribution": self.distribution_name,
            "provider_distribution_version": self.distribution_version,
        }
        if self.application_profile is not None:
            metadata["application_profile"] = self.application_profile
        return metadata


@dataclass
class PackageProgressProvider:
    """Relay-owned binding around one externally implemented progress adapter."""

    identity: PackageProgressProviderIdentity
    adapter: PackageProgressAdapter

    @property
    def package_name(self) -> str:
        """Return the package declaration bound to this provider."""
        return self.identity.package_name

    @property
    def package_version(self) -> str:
        """Return the package version reported by the provider."""
        return self.identity.package_version

    @property
    def adapter_name(self) -> str:
        """Return the public adapter identity."""
        return self.identity.adapter_name

    @property
    def application_profile(self) -> str | None:
        """Return the optional site/application profile identity."""
        return self.identity.application_profile

    @property
    def run_id(self) -> str:
        """Return the execution identity currently assigned to the provider."""
        return self.adapter.run_id

    @run_id.setter
    def run_id(self, value: str) -> None:
        """Assign the execution identity to the external adapter."""
        self.adapter.run_id = value

    def observe_jarvis_stdout(self, text: str) -> list[dict[str, object]]:
        """Return candidates extracted by the external adapter."""
        return self.adapter.observe_jarvis_stdout(text)

    def observe_stdout(self, text: str) -> list[dict[str, object]]:
        """Return candidates extracted from a package-owned log."""
        return self.adapter.observe_stdout(text)

    def finalize_jarvis_stdout(self) -> list[dict[str, object]]:
        """Flush candidates buffered by the external JARVIS stdout parser."""
        return self.adapter.finalize_jarvis_stdout()

    def finalize_stdout(self) -> list[dict[str, object]]:
        """Flush candidates buffered by the external package log parser."""
        return self.adapter.finalize_stdout()

    def reset_stdout(self) -> None:
        """Reset external package-log state after the selected source restarts."""
        self.adapter.reset_stdout()

    def progress_log_paths(self) -> list[Path]:
        """Return package-owned log paths declared by the external adapter."""
        return self.adapter.progress_log_paths()

    def package_load_probe_python(self) -> str | None:
        """Return the opaque package probe declared by the external adapter."""
        return self.adapter.package_load_probe_python()

    def acceptance_progress_valid(self, metadata: dict[str, Any]) -> bool:
        """Delegate candidate validation to the owning external provider."""
        return self.adapter.acceptance_progress_valid(dict(metadata))


@dataclass(frozen=True)
class _AdapterFactoryBinding:
    entry_point_name: str
    entry_point_value: str
    distribution_name: str
    distribution_version: str
    factory: AdapterFactory


@dataclass(frozen=True)
class _DisabledAdapter:
    """Sentinel that keeps an explicit ``adapter: none`` distinct from omission."""


_DISABLED_ADAPTER = _DisabledAdapter()


def package_progress_adapter_from_pipeline(
    pipeline_yaml: str,
) -> PackageProgressProvider | None:
    """Bind one package declaration to an installed external progress provider."""
    loaded = cast(object, yaml.safe_load(pipeline_yaml))
    if not isinstance(loaded, dict):
        return None
    typed_document = cast(dict[str, object], loaded)
    packages = typed_document.get("pkgs")
    if not isinstance(packages, list):
        return None
    typed_packages = cast(list[object], packages)
    declared_packages = [item for item in typed_packages if _package_declares_progress(item)]
    if len(declared_packages) > 1:
        raise ConfigurationError(
            "multiple pipeline packages declare progress; select exactly one package-owned "
            "progress source"
        )
    if declared_packages:
        selected_package = declared_packages[0]
    elif len(typed_packages) == 1:
        selected_package = typed_packages[0]
    else:
        # Automatic provider discovery is safe only for an unambiguous one-package pipeline.
        # Multi-package pipelines select their progress source explicitly on the owning package.
        return None
    if not isinstance(selected_package, dict):
        return None
    package = _provider_package_context(
        typed_document,
        cast(dict[str, Any], selected_package),
    )
    declared_adapter = _declared_adapter_name(package)
    if declared_adapter is _DISABLED_ADAPTER:
        return None
    matches: list[PackageProgressProvider] = []
    package_providers: list[PackageProgressProvider] = []
    for binding in _adapter_factories():
        adapter = binding.factory(package)
        if adapter is None:
            continue
        provider = _bind_provider(package, binding=binding, adapter=adapter)
        package_providers.append(provider)
        if declared_adapter is None or provider.adapter_name == declared_adapter:
            matches.append(provider)
    if len(matches) > 1:
        names = sorted(provider.identity.entry_point_name for provider in matches)
        raise ConfigurationError(f"multiple package progress providers matched: {names}")
    if matches:
        return matches[0]
    if isinstance(declared_adapter, str):
        available = sorted(provider.adapter_name for provider in package_providers)
        raise ConfigurationError(
            f"declared package progress adapter {declared_adapter!r} is not provided; "
            f"available={available}"
        )
    return None


def package_progress_acceptance_valid(
    *,
    adapter_name: str,
    package_name: str,
    metadata: dict[str, Any],
) -> bool:
    """Run an installed provider's validator for compatibility callers."""
    package = {
        "pkg_type": package_name,
        "progress": {"adapter": adapter_name},
    }
    document = yaml.safe_dump({"pkgs": [package]}, sort_keys=True)
    provider = package_progress_adapter_from_pipeline(document)
    return provider is not None and provider.acceptance_progress_valid(metadata)


def _adapter_factories() -> Iterable[_AdapterFactoryBinding]:
    selected = entry_points().select(group=PACKAGE_PROGRESS_ENTRYPOINT_GROUP)
    for entry_point in sorted(selected, key=lambda item: item.name):
        distribution = entry_point.dist
        if distribution is None:
            raise ConfigurationError(
                f"package progress adapter has no owning distribution: {entry_point.name}"
            )
        distribution_name = distribution.name
        distribution_version = distribution.version
        if not distribution_name or not distribution_version:
            raise ConfigurationError(
                f"package progress adapter distribution identity is incomplete: {entry_point.name}"
            )
        try:
            factory = entry_point.load()
        except (ImportError, AttributeError) as exc:
            raise ConfigurationError(
                f"failed to load package progress adapter {entry_point.name}: {exc}"
            ) from exc
        if not callable(factory):
            raise ConfigurationError(
                f"package progress adapter entry point is not callable: {entry_point.name}"
            )
        yield _AdapterFactoryBinding(
            entry_point_name=entry_point.name,
            entry_point_value=entry_point.value,
            distribution_name=distribution_name,
            distribution_version=distribution_version,
            factory=cast(AdapterFactory, factory),
        )


def _bind_provider(
    package: dict[str, Any],
    *,
    binding: _AdapterFactoryBinding,
    adapter: PackageProgressAdapter,
) -> PackageProgressProvider:
    package_type = package.get("pkg_type")
    if not isinstance(package_type, str) or not package_type:
        raise ConfigurationError("package progress provider requires a non-empty pkg_type")
    if adapter.package_name != package_type:
        raise ConfigurationError(
            "package progress provider package identity does not match its declaration: "
            f"{adapter.package_name} != {package_type}"
        )
    values = {
        "adapter_name": adapter.adapter_name,
        "package_version": adapter.package_version,
    }
    for field_name, value in values.items():
        if not value:
            raise ConfigurationError(f"package progress provider {field_name} must be non-empty")
    profile = adapter.application_profile
    if profile is not None and not profile:
        raise ConfigurationError("package progress provider application_profile must be non-empty")
    for method_name in (
        "observe_jarvis_stdout",
        "observe_stdout",
        "finalize_jarvis_stdout",
        "finalize_stdout",
        "reset_stdout",
        "progress_log_paths",
        "package_load_probe_python",
        "acceptance_progress_valid",
    ):
        if not callable(getattr(adapter, method_name, None)):
            raise ConfigurationError(
                f"package progress provider method is missing or not callable: {method_name}"
            )
    return PackageProgressProvider(
        identity=PackageProgressProviderIdentity(
            entry_point_name=binding.entry_point_name,
            entry_point_value=binding.entry_point_value,
            distribution_name=binding.distribution_name,
            distribution_version=binding.distribution_version,
            adapter_name=adapter.adapter_name,
            package_name=package_type,
            package_version=adapter.package_version,
            application_profile=profile,
        ),
        adapter=adapter,
    )


def _declared_adapter_name(package: dict[str, Any]) -> str | _DisabledAdapter | None:
    progress = package.get("progress")
    if not isinstance(progress, dict):
        return None
    value = cast(dict[str, object], progress).get("adapter")
    if value is None:
        return None
    if value == "none":
        return _DISABLED_ADAPTER
    if not isinstance(value, str) or not value:
        raise ConfigurationError("package progress.adapter must be a non-empty string")
    return value


def _package_declares_progress(value: object) -> bool:
    """Return whether one package explicitly requests a progress provider."""
    if not isinstance(value, dict):
        return False
    declaration = _declared_adapter_name(cast(dict[str, Any], value))
    return isinstance(declaration, str)


def _provider_package_context(
    pipeline: dict[str, object],
    package: dict[str, Any],
) -> dict[str, Any]:
    """Add effective generic JARVIS deployment context before provider selection."""
    context = dict(package)
    package_mode = package.get("deploy_mode")
    base_mode = pipeline.get("base_deploy_mode")
    legacy_base_mode = pipeline.get("install_manager")
    if base_mode is not None and legacy_base_mode is not None and base_mode != legacy_base_mode:
        raise ConfigurationError(
            "pipeline base_deploy_mode and deprecated install_manager must agree"
        )
    if base_mode is None:
        base_mode = legacy_base_mode
    for field_name, value in (
        ("deploy_mode", package_mode),
        ("base_deploy_mode", base_mode),
    ):
        if value is not None and (not isinstance(value, str) or not value):
            raise ConfigurationError(f"pipeline {field_name} must be a non-empty string")
    effective_mode = package_mode or base_mode
    if isinstance(effective_mode, str):
        context["effective_deploy_mode"] = effective_mode
        context.setdefault("deploy_mode", effective_mode)
    return context
