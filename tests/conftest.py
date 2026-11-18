"""Suite-wide pytest configuration."""

from __future__ import annotations

import os
from pathlib import Path

_FIXTURES_ENV = Path(
    os.environ.get(
        "CLIO_RELAY_FIXTURES_ENV",
        str(Path.home() / "clio-relay-rework" / "state" / "w2-fixtures" / "fixtures.env"),
    )
)

# The Ares fixture smoke tests need the sites `scripts/ares/fixtures.sh up` started.
# Without its env file the module is not collected, so other hosts report no skip.
collect_ignore: list[str] = [] if _FIXTURES_ENV.is_file() else ["test_fixtures_smoke.py"]
