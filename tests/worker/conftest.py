"""Worker adapter tests.

The live conformance module needs a site to place the worker on and a
rendezvous; without RELAY_TEST_WORKER_CONFIG it is not collected.
"""

from __future__ import annotations

import os

collect_ignore = (
    [] if os.environ.get("RELAY_TEST_WORKER_CONFIG") else ["test_conformance_worker.py"]
)
