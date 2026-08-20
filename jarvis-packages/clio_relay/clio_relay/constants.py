"""Shared bounds, schema-version strings, and static sets for the mcp_call runner.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). This
module has no dependencies on any other ``mcp_call`` module -- every owner module
that needs one of these values, including the ``runner`` facade, imports it from
here directly. The JARVIS-CD release pin (``JARVIS_CD_VERSION`` and friends) lives
in :mod:`clio_relay.jarvis_cd_lock_binding` instead, since it is owned by
that verification concern, not by the generic bounds catalog.
"""

from __future__ import annotations

import re

TOOLS_LIST_MAX_PAGES = 64
TOOLS_LIST_MAX_TOOLS = 10_000
TOOLS_LIST_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MCP_CALL_DEFAULT_TIMEOUT_SECONDS = 300
MCP_SERVER_TERMINATION_TIMEOUT_SECONDS = 2.0
MCP_INITIALIZE_MAX_RESPONSE_BYTES = 1024 * 1024
MCP_CALL_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MCP_SESSION_MAX_STDOUT_BYTES = 32 * 1024 * 1024
MCP_SESSION_MAX_STDERR_BYTES = 4 * 1024 * 1024
MCP_PACKAGE_PROGRESS_SCHEMA = "clio-kit.jarvis-package-progress.v1"
MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA = "clio-relay.mcp-package-progress-bridge.v1"
MCP_JARVIS_RUNTIME_SCHEMA = "jarvis.runtime.v1"
MCP_JARVIS_EXECUTION_HANDLE_SCHEMA = "jarvis.execution.handle.v1"
MCP_JARVIS_EXECUTION_RECORD_SCHEMA = "jarvis.execution.record.v1"
MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA = "jarvis.execution.progress.v1"
MCP_JARVIS_PROGRESS_EVENT_SCHEMA = "jarvis.progress.v1"
MCP_JARVIS_EXECUTION_QUERY_SCHEMA = "clio-kit.jarvis-execution.v2"
MCP_JARVIS_EXECUTION_ARTIFACTS_SCHEMA = "jarvis.execution.artifacts.v1"
MCP_JARVIS_ARTIFACT_SCHEMA = "jarvis.artifact.v1"
MCP_JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA = "jarvis.execution.service-runtimes.v1"
MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA = "clio-relay.mcp-jarvis-progress-bridge.v1"
_QUERY_CONTRACTS = ("clio-kit-jarvis-user-v3.6", "clio-kit-jarvis-user-v3.7.1")
MCP_REQUEST_MAX_BYTES = 16 * 1024 * 1024
MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES = 64 * 1024
MCP_PACKAGE_PROGRESS_MAX_NOTIFICATIONS = 10_000
MCP_PACKAGE_PROGRESS_MAX_TOTAL_BYTES = 4 * 1024 * 1024
PROGRESS_SIDECAR_RECORD_SCHEMA = "clio-relay.progress-sidecar-record.v1"
_JARVIS_EXECUTION_STATES = frozenset(
    {
        "preparing",
        "scripted",
        "submitting",
        "submitted",
        "running",
        "completed",
        "failed",
        "canceled",
        "unknown",
    }
)
_JARVIS_TERMINAL_STATES = frozenset({"scripted", "completed", "failed", "canceled"})
_JARVIS_PROGRESS_STATES = frozenset(
    {"pending", "starting", "running", "ready", "completed", "failed", "canceled"}
)
_JARVIS_ARTIFACT_ROLES = frozenset(
    {"intermediate", "output", "log", "checkpoint", "provenance", "validation"}
)
_JARVIS_ARTIFACT_STATES = frozenset({"producing", "available", "finalized", "incomplete", "failed"})
_JARVIS_ARTIFACT_STRUCTURES = frozenset({"file", "directory", "collection", "stream"})
_JARVIS_ARTIFACT_OWNERSHIP = frozenset({"execution", "external", "shared"})
_JARVIS_ARTIFACT_LOCATION_KINDS = frozenset({"execution_path", "cluster_path", "external_uri"})
_JARVIS_ARTIFACT_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "package_name",
        "package_id",
        "execution_id",
        "artifact_id",
        "logical_name",
        "kind",
        "role",
        "structure",
        "ownership",
        "state",
        "revision",
        "sequence",
        "observed_at_epoch",
        "metadata",
    }
)
_JARVIS_ARTIFACT_OPTIONAL_FIELDS = frozenset(
    {
        "location",
        "media_type",
        "format",
        "size_bytes",
        "checksum",
        "message",
        "content",
        "content_error",
        "content_truncated",
        "content_bytes_read",
    }
)
_JARVIS_ARTIFACT_ID = re.compile(r"^art_[A-Za-z0-9_-]{22,86}$")
_JARVIS_ARTIFACT_CHECKSUM = re.compile(r"^[a-z0-9][a-z0-9_-]*:[A-Fa-f0-9]{16,256}$")
_JARVIS_ARTIFACT_MEDIA_TYPE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*$"
)
_JARVIS_ARTIFACT_CURSOR = re.compile(r"^[A-Za-z0-9_-]+$")
_JARVIS_ARTIFACT_URI_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*$")
_JARVIS_ARTIFACT_UNSAFE_URI_SCHEMES = frozenset({"data", "file", "javascript"})
_JARVIS_ARTIFACT_MAX_PAGE_SIZE = 100
_JARVIS_ARTIFACT_DEFAULT_PAGE_SIZE = 50
_JARVIS_ARTIFACT_MAX_CURSOR_LENGTH = 1024
_JARVIS_ARTIFACT_MAX_EVENT_BYTES = 256 * 1024
_JARVIS_ARTIFACT_MAX_METADATA_BYTES = 64 * 1024
_WINDOWS_RESERVED_COMPONENTS = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "CLOCK$",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
)
_JARVIS_REACHABLE_STATES: dict[str, frozenset[str]] = {
    "preparing": _JARVIS_EXECUTION_STATES - {"preparing"},
    "scripted": frozenset({"running", "completed", "failed", "canceled", "unknown"}),
    "submitting": frozenset({"submitted", "running", "completed", "failed", "canceled", "unknown"}),
    "submitted": frozenset({"running", "completed", "failed", "canceled", "unknown"}),
    "running": frozenset({"completed", "failed", "canceled", "unknown"}),
    "completed": frozenset(),
    "failed": frozenset(),
    "canceled": frozenset(),
    "unknown": frozenset({"submitted", "running", "completed", "failed", "canceled"}),
}
FILE_HASH_CHUNK_BYTES = 1024 * 1024
CLIO_KIT_WHEEL_MAX_FILES = 10_000
CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES = 1024 * 1024
CLIO_KIT_LOCK_MAX_BYTES = 16 * 1024 * 1024
CLIO_KIT_WHEEL_MAX_PROJECT_FILES = 20_000
CLIO_KIT_WHEEL_MAX_PROJECT_BYTES = 512 * 1024 * 1024
PYTHON_DISTRIBUTION_MAX_DISTRIBUTIONS = 10_000
PYTHON_DISTRIBUTION_MAX_ENTRY_POINTS = 100_000
PYTHON_DISTRIBUTION_MAX_FILES = 100_000
PYTHON_DISTRIBUTION_MAX_BYTES = 4 * 1024 * 1024 * 1024
PYTHON_TOOL_IDENTITY_MAX_BYTES = 8 * 1024 * 1024
PYTHON_TOOL_IDENTITY_TIMEOUT_SECONDS = 30
_STREAM_READ_CHARS = 64 * 1024
_TOOLS_LIST_PAGINATION_KEY = "_clioRelayPagination"
_CLIO_KIT_LOCKED_SERVER_SCHEMA = "clio-kit.locked-server.v4"
_CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY = "uv-run:materialized:frozen:no-editable:no-dev:v3"
_CLIO_KIT_CACHE_EVENT_SCHEMA = "clio-kit.cache-event.v1"
_CLIO_KIT_POST_BUILD_EVENTS = frozenset(
    {
        "uv_cache_prune",
        "cache_maintenance_failed",
        "cache_maintenance_skipped",
    }
)
_CLIO_KIT_REQUEST_ENV_OVERRIDES = {
    # A cache-wide uv prune waits behind other live clio-kit servers. It must not
    # become part of one bounded relay request's startup path; explicit cache GC
    # remains available outside the served MCP session.
    "CLIO_KIT_UV_CACHE_PRUNE": "0",
}
# The relay composes one site runtime identity for a JARVIS run from the Spack
# executable its cluster registered, and publishes it to this runner under a
# relay-owned name. The JARVIS MCP server reads JARVIS_MCP_SPACK_COMMAND before
# it searches PATH, SPACK_ROOT/bin, ~/.local/spack or /opt/spack, so mapping the
# composed value onto that variable makes `spack load` resolve the registered
# executable rather than whichever one a search happens to reach.
_RELAY_JARVIS_SPACK_COMMAND_ENV = "CLIO_RELAY_JARVIS_SPACK_COMMAND"
_JARVIS_MCP_SPACK_COMMAND_CHILD_ENV = "JARVIS_MCP_SPACK_COMMAND"
_CLIO_KIT_RUNTIME_PROJECT_EXCLUDED_NAMES = frozenset(
    {
        ".git",
        ".coverage",
        ".DS_Store",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        ".virtualenv-app-data",
        "__pycache__",
        "dist",
        "coverage.xml",
        "htmlcov",
        "junit.xml",
        "tests",
    }
)
_RELAY_CREDENTIAL_ENV_NAMES = frozenset(
    {
        "CLIO_RELAY_API_TOKEN",
        "CLIO_RELAY_FRP_TOKEN",
        "CLIO_RELAY_PROGRESS_TOKEN",
        "CLIO_RELAY_RUNTIME_METADATA_TOKEN",
        "CLIO_RELAY_STCP_SECRET",
    }
)
_BASE_CHILD_ENV_NAMES = frozenset(
    {
        "APPDATA",
        "COMSPEC",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LOCALAPPDATA",
        "LOGNAME",
        "NoDefaultCurrentDirectoryInExePath",
        "PATH",
        "PATHEXT",
        "PYTHONIOENCODING",
        "PYTHONUTF8",
        "SHELL",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USER",
        "USERPROFILE",
        "UV_CACHE_DIR",
        "UV_PYTHON_INSTALL_DIR",
        "UV_TOOL_DIR",
        "WINDIR",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
    }
)
