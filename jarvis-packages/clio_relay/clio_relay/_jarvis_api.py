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

else:
    from jarvis_cd.core.pkg import Application

__all__ = ["Application"]
