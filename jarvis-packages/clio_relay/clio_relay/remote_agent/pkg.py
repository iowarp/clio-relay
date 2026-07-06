"""JARVIS-CD package for remote agent tasks."""

from __future__ import annotations

from typing import Any

from jarvis_cd.core.pkg import Application

from clio_relay.remote_agent.runner import run_remote_agent_from_params


class RemoteAgent(Application):
    """Run a configured agent binary against a prompt."""

    def _init(self) -> None:
        """Initialize package state."""

    def _configure_menu(self) -> list[dict[str, Any]]:
        """Return JARVIS configurator options."""
        return []

    def _configure(self, **kwargs: Any) -> None:
        """Store configuration provided by the pipeline YAML."""
        self.config.update(kwargs)

    def start(self) -> None:
        """Run the configured agent binary."""
        return_code = run_remote_agent_from_params(dict(self.config))
        if return_code != 0:
            raise RuntimeError(f"agent failed with exit code {return_code}")

    def stop(self) -> None:
        """Stop hook for remote agent tasks."""

    def clean(self) -> None:
        """Clean hook for remote agent tasks."""
