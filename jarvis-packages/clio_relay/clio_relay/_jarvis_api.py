"""Typed runtime boundary for the JARVIS-CD package API.

The package sources are copied into a JARVIS repository and imported there, so
JARVIS-CD is deliberately not a dependency of the relay wheel itself.  Static
checks still need the small part of the API these packages use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:

    class Application:
        """Static shape of the JARVIS-CD application base class used here."""

        config: dict[str, Any]

    class ConfigurationInputBinding:
        """Static shape of the JARVIS-CD configuration input binding used here."""

        def __init__(self, *, kind: str, structure: str) -> None: ...

        def to_dict(self) -> dict[str, Any]:
            """Return the serialized binding a package menu entry publishes."""
            ...

else:
    from jarvis_cd.core.pkg import Application
    from jarvis_cd.deployment import ConfigurationInputBinding

__all__ = ["Application", "ConfigurationInputBinding"]
