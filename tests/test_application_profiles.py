"""Tests for external application-profile ownership."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from pytest import MonkeyPatch

from clio_relay.application_profiles import (
    install_cluster_app_over_ssh,
    load_application_profile,
)
from clio_relay.errors import ConfigurationError
from clio_relay.progress_adapters import package_progress_adapter_from_pipeline
from tests.plugin_fakes import (
    FakeEntryPoint,
    FakeEntryPoints,
    install_site_progress_plugin,
)


@dataclass(frozen=True)
class SiteStackProfile:
    """Test-only site profile standing in for an external distribution."""

    name: str = "site-stack"

    def render_install_script(self) -> str:
        """Return an opaque site-owned installation script."""
        return "set -euo pipefail\nprintf 'site_stack=ready\\n'\n"


def _install_site_profile(monkeypatch: MonkeyPatch) -> None:
    entries = FakeEntryPoints(
        [
            FakeEntryPoint(
                name="site-stack",
                group="clio_relay.application_profiles",
                loaded=SiteStackProfile(),
            )
        ]
    )
    monkeypatch.setattr("clio_relay.application_profiles.entry_points", lambda: entries)


def test_relay_distribution_contains_no_built_in_application_plugins() -> None:
    source_root = Path(__file__).parents[1] / "src" / "clio_relay"
    assert [path.name for path in (source_root / "app_profiles").glob("*.py")] == ["__init__.py"]
    assert [path.name for path in (source_root / "package_adapters").glob("*.py")] == [
        "__init__.py"
    ]
    for relative_path in (
        "bootstrap.py",
        "endpoint.py",
        "progress_adapters.py",
        "live_acceptance.py",
    ):
        source = (source_root / relative_path).read_text(encoding="utf-8")
        assert "spack install" not in source.lower()
        assert "from clio_relay.app_profiles" not in source
        assert "from clio_relay.package_adapters" not in source


def test_application_and_progress_behavior_load_through_external_plugins(
    monkeypatch: MonkeyPatch,
) -> None:
    _install_site_profile(monkeypatch)
    install_site_progress_plugin(monkeypatch)

    profile = load_application_profile("site-stack")
    adapter = package_progress_adapter_from_pipeline(
        "name: external\n"
        "pkgs:\n"
        "- pkg_type: site.simulation\n"
        "  out: /runtime/output\n"
        "  progress:\n"
        "    adapter: site-progress\n"
        "    log_visibility: shared\n"
    )

    assert profile.name == "site-stack"
    assert "site_stack=ready" in profile.render_install_script()
    assert adapter is not None
    assert adapter.adapter_name == "site-progress"
    assert adapter.application_profile == "site-stack"
    assert adapter.progress_log_paths() == [Path("/runtime/output/progress.log")]


def test_missing_external_application_profile_fails_closed() -> None:
    with pytest.raises(ConfigurationError, match="unsupported cluster app profile"):
        load_application_profile("missing-site-stack")


@pytest.mark.parametrize(
    "ssh_host",
    [
        "-oProxyCommand=malicious-command",
        "host with-space",
        "host\nsecond-command",
        "\x7fhost",
        "",
    ],
)
def test_cluster_app_install_rejects_unsafe_ssh_destination(ssh_host: str) -> None:
    with pytest.raises(ConfigurationError, match="ssh host must be one non-option"):
        install_cluster_app_over_ssh(ssh_host=ssh_host, app_name="site-stack")
