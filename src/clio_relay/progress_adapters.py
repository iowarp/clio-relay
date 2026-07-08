"""Registry for package-owned progress adapters."""

from __future__ import annotations

from typing import Any, Protocol, cast

import yaml

from clio_relay.package_adapters.lammps import adapter_from_package


class PackageProgressAdapter(Protocol):
    """Progress adapter owned by a declared package/application boundary."""

    package_name: str
    package_version: str
    run_id: str

    def observe_jarvis_stdout(self, text: str) -> list[dict[str, object]]:
        """Extract trusted package progress from JARVIS-scoped stdout."""
        ...

    def observe_stdout(self, text: str) -> list[dict[str, object]]:
        """Extract package progress from already trusted package stdout."""
        ...


def package_progress_adapter_from_pipeline(
    pipeline_yaml: str,
) -> PackageProgressAdapter | None:
    """Return a package-owned progress adapter for a declared JARVIS pipeline."""
    loaded = cast(object, yaml.safe_load(pipeline_yaml))
    if not isinstance(loaded, dict):
        return None
    typed_document = cast(dict[str, object], loaded)
    packages = typed_document.get("pkgs")
    if not isinstance(packages, list):
        return None
    typed_packages = cast(list[object], packages)
    if len(typed_packages) != 1:
        return None
    for package in typed_packages:
        if not isinstance(package, dict):
            continue
        typed_package = cast(dict[str, Any], package)
        adapter = adapter_from_package(typed_package)
        if adapter is not None:
            return adapter
    return None
