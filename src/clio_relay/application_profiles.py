"""Plugin boundary for explicit application and site setup profiles."""

from __future__ import annotations

import shutil
import subprocess
from importlib.metadata import entry_points
from typing import Protocol, cast

from clio_relay.errors import ConfigurationError, RelayError

APPLICATION_PROFILE_ENTRYPOINT_GROUP = "clio_relay.application_profiles"


class ApplicationProfile(Protocol):
    """Operator-selected application setup behavior."""

    name: str

    def render_install_script(self) -> str:
        """Render a noninteractive remote application installation script."""
        ...


def load_application_profile(name: str) -> ApplicationProfile:
    """Load an explicitly selected application profile by entry-point name."""
    normalized = name.strip().lower()
    for entry_point in entry_points().select(group=APPLICATION_PROFILE_ENTRYPOINT_GROUP):
        if entry_point.name.lower() != normalized:
            continue
        try:
            profile = entry_point.load()
        except (ImportError, AttributeError) as exc:
            raise ConfigurationError(
                f"failed to load cluster app profile {entry_point.name}: {exc}"
            ) from exc
        if not hasattr(profile, "render_install_script") or not hasattr(profile, "name"):
            raise ConfigurationError(
                f"cluster app profile does not implement the profile contract: {entry_point.name}"
            )
        return cast(ApplicationProfile, profile)
    raise ConfigurationError(f"unsupported cluster app profile: {name}")


def render_cluster_app_install_script(*, app_name: str) -> str:
    """Render an operator-selected application profile's installer."""
    return load_application_profile(app_name).render_install_script()


def install_cluster_app_over_ssh(*, ssh_host: str, app_name: str) -> list[str]:
    """Install an explicit application profile on a cluster over SSH."""
    if shutil.which("ssh") is None:
        raise ConfigurationError("ssh is required for remote app installation")
    script = render_cluster_app_install_script(app_name=app_name)
    result = subprocess.run(
        ["ssh", ssh_host, "bash", "-s"],
        input=script.encode("utf-8"),
        capture_output=True,
        check=False,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    if result.returncode != 0:
        raise RelayError(
            f"cluster app profile installation failed on {ssh_host}: "
            f"{stderr.strip() or stdout.strip()}"
        )
    return stdout.splitlines()
