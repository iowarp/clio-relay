"""Suite-wide pytest configuration."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

_FIXTURES_ENV = Path(
    os.environ.get(
        "CLIO_RELAY_FIXTURES_ENV",
        str(Path.home() / "clio-relay-rework" / "state" / "w2-fixtures" / "fixtures.env"),
    )
)

# The Ares fixture smoke tests need the sites `scripts/ares/fixtures.sh up` started.
# Without its env file the module is not collected, so other hosts report no skip.
collect_ignore: list[str] = [] if _FIXTURES_ENV.is_file() else ["test_fixtures_smoke.py"]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Deselect tests marked ``release_platform`` for the other platform.

    Deselection, not a skip: a POSIX-only test is not part of a Windows run.
    """
    here = "windows" if os.name == "nt" else "posix"
    kept: list[pytest.Item] = []
    for item in items:
        markers = list(item.iter_markers(name="release_platform"))
        if len(markers) > 1 or any(m.kwargs or m.args not in (("posix",), ("windows",)) for m in markers):
            raise pytest.UsageError(f"{item.nodeid}: use one release_platform('posix'|'windows')")
        if not markers or markers[0].args[0] == here:
            kept.append(item)
    items[:] = kept
