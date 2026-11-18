"""Where relay keeps its configuration and state.

``CLIO_RELAY_CONFIG_DIR`` holds ``sites.yaml``. ``CLIO_RELAY_STATE_DIR`` holds
operation and ask records, the content store and each site's adapter state.
Both default to the platform's usual places. ``CLIO_RELAY_GRANT_FILE_<PROVIDER>``
names a grant file a host keeps current (HS4a).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

APP = "clio-relay"


def _default_config_dir(env: Mapping[str, str]) -> Path:
    if sys.platform == "win32" and env.get("APPDATA"):
        return Path(env["APPDATA"]) / APP
    base = env.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / APP


def _default_state_dir(env: Mapping[str, str]) -> Path:
    if sys.platform == "win32" and env.get("LOCALAPPDATA"):
        return Path(env["LOCALAPPDATA"]) / APP / "state"
    base = env.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(base) / APP


@dataclass(frozen=True)
class RelayPaths:
    """Resolved relay directories and the grant files a host provides."""

    config_dir: Path
    state_dir: Path
    grant_files: dict[str, Path] = field(default_factory=dict[str, Path])

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RelayPaths:
        """Read the directories from the environment."""
        values = os.environ if env is None else env
        config = values.get("CLIO_RELAY_CONFIG_DIR")
        state = values.get("CLIO_RELAY_STATE_DIR")
        prefix = "CLIO_RELAY_GRANT_FILE_"
        grants = {
            key[len(prefix) :].lower(): Path(value)
            for key, value in values.items()
            if key.startswith(prefix) and value
        }
        return cls(
            config_dir=Path(config) if config else _default_config_dir(values),
            state_dir=Path(state) if state else _default_state_dir(values),
            grant_files=grants,
        )

    @property
    def sites_file(self) -> Path:
        """Return the path of ``sites.yaml``."""
        return self.config_dir / "sites.yaml"

    @property
    def operations_dir(self) -> Path:
        """Return where operation records live."""
        return self.state_dir / "operations"

    @property
    def asks_dir(self) -> Path:
        """Return where ask records live."""
        return self.state_dir / "asks"

    @property
    def content_dir(self) -> Path:
        """Return the content store's root."""
        return self.state_dir / "content"

    def adapter_dir(self, site_id: str, adapter: str) -> Path:
        """Return one site's state directory for one adapter."""
        return self.state_dir / "sites" / site_id / adapter
