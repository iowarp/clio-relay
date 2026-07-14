"""Tests for the generic package-progress plugin boundary."""

from __future__ import annotations

from pathlib import Path

import pytest
from pytest import MonkeyPatch

from clio_relay.errors import ConfigurationError
from clio_relay.progress_adapters import (
    package_progress_acceptance_valid,
    package_progress_adapter_from_pipeline,
)
from tests.plugin_fakes import (
    FakeEntryPoints,
    SiteSimulationProgressAdapter,
    install_site_progress_plugin,
)


def test_pipeline_loads_external_progress_adapter(monkeypatch: MonkeyPatch) -> None:
    install_site_progress_plugin(monkeypatch)
    pipeline = (
        "name: external\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  pkg_version: '2.1'\n"
        "  out: /runtime/site-output\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "    log_visibility: shared\n"
    )

    adapter = package_progress_adapter_from_pipeline(pipeline)

    assert adapter is not None
    assert isinstance(adapter.adapter, SiteSimulationProgressAdapter)
    assert adapter.package_name == "site.simulation"
    assert adapter.package_version == "2.1"
    assert adapter.progress_log_paths() == [Path("/runtime/site-output/progress.log")]
    assert adapter.identity.entry_point_name == "site-progress"
    assert adapter.identity.distribution_name == "site-progress-plugin"
    assert adapter.identity.distribution_version == "3.4.5"
    assert adapter.identity.adapter_name == "site-progress"
    assert adapter.identity.application_profile == "site-stack"


def test_external_adapter_observes_only_its_jarvis_scope(monkeypatch: MonkeyPatch) -> None:
    install_site_progress_plugin(monkeypatch)
    adapter = package_progress_adapter_from_pipeline(
        "name: external\npkgs:\n- pkg_type: site.simulation\n"
    )
    assert adapter is not None
    adapter.run_id = "job_test"

    ignored = adapter.observe_jarvis_stdout(
        "[unrelated.package] [START] BEGIN\nPROGRESS 3 10\n[unrelated.package] [START] END\n"
    )
    observed = adapter.observe_jarvis_stdout(
        "[site.simulation] [START] BEGIN\nPROGRESS 3 10\n[site.simulation] [START] END\n"
    )

    assert ignored == []
    assert [item["current"] for item in observed] == [3.0]
    metadata = observed[0]["metadata"]
    assert isinstance(metadata, dict)
    assert metadata["adapter"] == "site-progress"
    assert metadata["package_name"] == "site.simulation"
    assert metadata["run_id"] == "job_test"


def test_pipeline_requires_exactly_one_external_package(monkeypatch: MonkeyPatch) -> None:
    install_site_progress_plugin(monkeypatch)
    unrelated = "name: generic\npkgs:\n- pkg_type: unrelated.package\n"
    mixed = "name: mixed\npkgs:\n- pkg_type: site.simulation\n- pkg_type: unrelated.package\n"

    assert package_progress_adapter_from_pipeline(unrelated) is None
    assert package_progress_adapter_from_pipeline(mixed) is None


def test_acceptance_delegates_to_external_plugin(monkeypatch: MonkeyPatch) -> None:
    install_site_progress_plugin(monkeypatch)

    assert package_progress_acceptance_valid(
        adapter_name="site-progress",
        package_name="site.simulation",
        metadata={"prediction_status": "observed", "eta_seconds": 2.5},
    )
    assert not package_progress_acceptance_valid(
        adapter_name="site-progress",
        package_name="site.simulation",
        metadata={"prediction_status": "claimed", "eta_seconds": 2.5},
    )


def test_no_external_plugin_means_no_application_inference() -> None:
    pipeline = "name: external\npkgs:\n- pkg_type: site.simulation\n"

    assert package_progress_adapter_from_pipeline(pipeline) is None


def test_explicit_none_disables_an_installed_provider(monkeypatch: MonkeyPatch) -> None:
    install_site_progress_plugin(monkeypatch)
    pipeline = (
        "name: disabled\npkgs:\n- pkg_type: site.simulation\n  progress:\n    adapter: none\n"
    )

    assert package_progress_adapter_from_pipeline(pipeline) is None


def test_explicit_named_provider_fails_when_no_provider_is_installed(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "clio_relay.progress_adapters.entry_points",
        lambda: FakeEntryPoints(),
    )
    pipeline = (
        "name: missing\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
    )

    with pytest.raises(ConfigurationError, match=r"is not provided; available=\[\]"):
        package_progress_adapter_from_pipeline(pipeline)


def test_explicit_multi_package_progress_selects_owning_package(
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    pipeline = (
        "name: supported-multi\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "- pkg_type: unrelated.package\n"
    )

    provider = package_progress_adapter_from_pipeline(pipeline)

    assert provider is not None
    assert provider.package_name == "site.simulation"
    assert provider.adapter_name == "site-progress"


def test_multiple_explicit_progress_packages_fail_closed(monkeypatch: MonkeyPatch) -> None:
    install_site_progress_plugin(monkeypatch)
    pipeline = (
        "name: ambiguous-multi\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "- pkg_type: another.simulation\n"
        "  progress:\n"
        "    adapter: site-progress\n"
    )

    with pytest.raises(ConfigurationError, match="multiple pipeline packages declare progress"):
        package_progress_adapter_from_pipeline(pipeline)


def test_provider_receives_effective_inherited_container_deploy_mode(
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    provider = package_progress_adapter_from_pipeline(
        "name: inherited-container\n"
        "base_deploy_mode: container\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  out: host-looking-output\n"
        "  progress:\n"
        "    adapter: site-progress\n"
    )

    assert provider is not None
    assert provider.progress_log_paths() == []


def test_provider_normalizes_legacy_inherited_container_deploy_mode(
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)
    provider = package_progress_adapter_from_pipeline(
        "name: inherited-container\n"
        "install_manager: container\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  out: host-looking-output\n"
        "  progress:\n"
        "    adapter: site-progress\n"
    )

    assert provider is not None
    assert provider.progress_log_paths() == []


def test_conflicting_legacy_and_current_deploy_modes_fail_closed(
    monkeypatch: MonkeyPatch,
) -> None:
    install_site_progress_plugin(monkeypatch)

    with pytest.raises(ConfigurationError, match="must agree"):
        package_progress_adapter_from_pipeline(
            "name: conflict\n"
            "base_deploy_mode: default\n"
            "install_manager: container\n"
            "pkgs:\n"
            "- pkg_type: site.simulation\n"
        )
