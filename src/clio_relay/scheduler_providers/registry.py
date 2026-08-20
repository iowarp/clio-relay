"""Scheduler provider registry: factory lookup and typed capability resolution."""

from __future__ import annotations

from collections.abc import Callable

from clio_relay.errors import ConfigurationError

from .external import ExternalSchedulerProvider
from .protocols import (
    SchedulerAllocationConnectorProvider,
    SchedulerProvider,
    SchedulerReconciliationProvider,
    SchedulerValidationProvider,
)
from .slurm_provider import SlurmSchedulerProvider
from .validation import _normalize_provider_name

SchedulerProviderFactory = Callable[[], SchedulerProvider]
_PROVIDER_FACTORIES: dict[str, SchedulerProviderFactory] = {
    "external": ExternalSchedulerProvider,
    "slurm": SlurmSchedulerProvider,
}


def register_scheduler_provider(
    name: str,
    factory: SchedulerProviderFactory,
    *,
    replace: bool = False,
) -> None:
    """Register an additional scheduler provider factory."""
    normalized = _normalize_provider_name(name)
    if normalized in _PROVIDER_FACTORIES and not replace:
        raise ConfigurationError(f"scheduler provider is already registered: {normalized}")
    _PROVIDER_FACTORIES[normalized] = factory


def provider_for_scheduler(name: str | None) -> SchedulerProvider:
    """Return an explicitly selected scheduler provider."""
    if name is None or name.strip() == "":
        raise ConfigurationError(
            "scheduler provider must be explicit; configure external or a scheduler provider"
        )
    normalized = _normalize_provider_name(name)
    if normalized in {"external", "none", "unmanaged"}:
        normalized = "external"
    factory = _PROVIDER_FACTORIES.get(normalized)
    if factory is None:
        raise ConfigurationError(f"unsupported scheduler provider: {name}")
    provider = factory()
    if _normalize_provider_name(provider.name) != normalized:
        raise ConfigurationError(
            f"scheduler provider factory {normalized} returned provider {provider.name}"
        )
    return provider


def validation_provider_for_scheduler(name: str | None) -> SchedulerValidationProvider:
    """Return a provider that implements deterministic lifecycle validation operations."""
    provider = provider_for_scheduler(name)
    if not isinstance(provider, SchedulerValidationProvider):
        raise ConfigurationError(
            f"scheduler provider does not support lifecycle validation: {provider.name}"
        )
    return provider


def allocation_connector_provider_for_scheduler(
    name: str | None,
) -> SchedulerAllocationConnectorProvider:
    """Return a provider that can prove and enter one exact allocation placement."""
    provider = provider_for_scheduler(name)
    if not isinstance(provider, SchedulerAllocationConnectorProvider):
        raise ConfigurationError(
            f"scheduler provider does not support allocation connectors: {provider.name}"
        )
    return provider


def reconciliation_provider_for_scheduler(
    name: str | None,
) -> SchedulerReconciliationProvider:
    """Return a provider that can prove one interrupted submission by exact marker."""
    provider = provider_for_scheduler(name)
    if not isinstance(provider, SchedulerReconciliationProvider):
        raise ConfigurationError(
            f"scheduler provider does not support exact submission reconciliation: {provider.name}"
        )
    return provider
