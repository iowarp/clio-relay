"""Bounded-payload budgets for validation evidence (#231; see bounded_payload.py).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). Mirrors the shape of
:mod:`clio_relay.bounded_payload`'s T1/T2/T3 catalog: every byte/count/depth
budget the validation-report family enforces lives here as a single owner, so
a change to one bound touches one file instead of the several concern modules
(schema validators, transport-probe evidence parsing, install-source
detection, artifact-identity verification, the durable validation directory)
that each read one of these constants. Nothing here has runtime behavior of
its own -- it is a data catalog other owner modules import.
"""

from __future__ import annotations

import re

# Structured transport-probe cleanup evidence (schema + recorder).
TRANSPORT_PROBE_EVIDENCE_KEY = "transport.probe_evidence"
MAX_TRANSPORT_PROBE_EVIDENCE_BYTES = 256 * 1024
MAX_TRANSPORT_PROBE_RESOURCES = 128
MAX_TRANSPORT_PROBE_JSON_DEPTH = 16
MAX_TRANSPORT_PROBE_JSON_NODES = 4096

# Install-source detection (launcher receipts, uv-tool receipts, pyvenv.cfg).
MAX_LAUNCHER_PROCESS_ANCESTORS = 64
MAX_PYVENV_CONFIG_BYTES = 64 * 1024
MAX_UV_TOOL_RECEIPT_BYTES = 256 * 1024

# Artifact-identity verification (wheel bytes fetched to compare against a
# pinned digest).
MAX_DISTRIBUTION_WHEEL_BYTES = 128 * 1024 * 1024

# Durable validation directory (atomic report writes + stale-pending sweep).
MAX_VALIDATION_REPORT_WRITE_BYTES = 64 * 1024 * 1024
MAX_VALIDATION_PENDING_FILES = 16
VALIDATION_PENDING_PATTERN = re.compile(r"^\.clio-validation-[0-9a-f]{32}\.pending$")
