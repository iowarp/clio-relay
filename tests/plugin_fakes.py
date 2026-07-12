"""Test-only external application and progress plugins."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from pytest import MonkeyPatch

from clio_relay.progress_provenance import package_progress_metadata


@dataclass(frozen=True)
class FakeDistribution:
    """Minimal owning-distribution identity for a fake entry point."""

    name: str
    version: str


class FakeEntryPoint:
    """Small importlib entry-point stand-in used only by tests."""

    def __init__(
        self,
        *,
        name: str,
        group: str,
        loaded: object,
        distribution_name: str = "site-progress-plugin",
        distribution_version: str = "3.4.5",
        value: str = "tests.plugin_fakes:site_progress_adapter_from_package",
    ) -> None:
        self.name = name
        self.group = group
        self.value = value
        self._loaded = loaded
        self.dist = FakeDistribution(distribution_name, distribution_version)

    def load(self) -> object:
        """Return the configured plugin object."""
        return self._loaded


class FakeEntryPoints(list[FakeEntryPoint]):
    """Entry-point collection with the Python 3.12 select contract."""

    def select(self, **parameters: str) -> FakeEntryPoints:
        """Return entries matching all supplied metadata fields."""
        return FakeEntryPoints(
            [
                item
                for item in self
                if all(getattr(item, key, None) == value for key, value in parameters.items())
            ]
        )


@dataclass
class SiteSimulationProgressAdapter:
    """Generic external progress adapter used to exercise relay plugin wiring."""

    package_name: str = "site.simulation"
    package_version: str = "test-plugin"
    run_id: str = ""
    adapter_name: str = "site-progress"
    application_profile: str | None = "site-stack"
    output_dir: Path | None = None
    active_package_stdout: bool = False

    def observe_jarvis_stdout(self, text: str) -> list[dict[str, object]]:
        """Parse records only inside the external package's JARVIS scope."""
        records: list[dict[str, object]] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped == f"[{self.package_name}] [START] BEGIN":
                self.active_package_stdout = True
                continue
            if stripped == f"[{self.package_name}] [START] END":
                self.active_package_stdout = False
                continue
            if self.active_package_stdout:
                record = self._record(stripped)
                if record is not None:
                    records.append(record)
        return records

    def observe_stdout(self, text: str) -> list[dict[str, object]]:
        """Parse progress records from a package-owned log."""
        return [record for line in text.splitlines() if (record := self._record(line)) is not None]

    def finalize_jarvis_stdout(self) -> list[dict[str, object]]:
        """Flush no records because the basic fake parser does not buffer fragments."""
        return []

    def finalize_stdout(self) -> list[dict[str, object]]:
        """Flush no records because the basic fake log parser does not buffer fragments."""
        return []

    def reset_stdout(self) -> None:
        """Reset no state because the basic fake log parser has no fragment buffer."""
        return None

    def progress_log_paths(self) -> list[Path]:
        """Return the external plugin's optional progress log path."""
        return [] if self.output_dir is None else [self.output_dir / "progress.log"]

    def package_load_probe_python(self) -> str | None:
        """Return an opaque external package probe owned by the plugin."""
        return "print('/opt/site/plugins/site_simulation.py')"

    def acceptance_progress_valid(self, metadata: dict[str, Any]) -> bool:
        """Accept only the external plugin's observed progress contract."""
        return (
            metadata.get("prediction_status") == "observed"
            and isinstance(metadata.get("eta_seconds"), int | float)
            and float(metadata["eta_seconds"]) >= 0
        )

    def _record(self, line: str) -> dict[str, object] | None:
        fields = line.strip().split()
        if len(fields) not in {2, 3} or fields[0] != "PROGRESS":
            return None
        try:
            current = float(fields[1])
            total = float(fields[2]) if len(fields) == 3 else None
        except ValueError:
            return None
        metadata = package_progress_metadata(
            {
                "adapter": self.adapter_name,
                "prediction_status": "observed",
                "eta_seconds": max(0.0, (total or current) - current),
            },
            package_name=self.package_name,
            package_version=self.package_version,
            run_id=self.run_id,
        )
        return {
            "label": "iteration",
            "current": current,
            "total": total,
            "unit": "step",
            "metadata": metadata,
        }


def site_progress_adapter_from_package(
    package: dict[str, Any],
) -> SiteSimulationProgressAdapter | None:
    """Create the test plugin only for its external package declaration."""
    if package.get("pkg_type") != "site.simulation":
        return None
    output = package.get("out")
    progress_value = package.get("progress")
    progress = cast(dict[str, object], progress_value) if isinstance(progress_value, dict) else {}
    log_visibility = progress.get("log_visibility")
    return SiteSimulationProgressAdapter(
        package_version=str(package.get("pkg_version") or "test-plugin"),
        output_dir=(
            Path(output)
            if isinstance(output, str)
            and output
            and package.get("effective_deploy_mode") != "container"
            and log_visibility == "shared"
            else None
        ),
    )


def install_site_progress_plugin(monkeypatch: MonkeyPatch) -> None:
    """Expose the generic test adapter through the production entry-point boundary."""
    entries = FakeEntryPoints(
        [
            FakeEntryPoint(
                name="site-progress",
                group="clio_relay.package_progress_adapters",
                loaded=site_progress_adapter_from_package,
            )
        ]
    )
    monkeypatch.setattr("clio_relay.progress_adapters.entry_points", lambda: entries)
