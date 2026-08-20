"""Shared process-tree ownership for relay and embedded JARVIS runners.

This module is a thin facade (iowarp/clio-relay#231): the implementation
lives in the `process_containment_*` owner modules below, split by concern
(shared types, the secret-memory/child-environment gate, the broker script,
POSIX/Windows/systemd platform primitives, broker lifecycle, spawn
orchestration, and termination). Every name that used to live directly in
this file -- public and private alike -- is re-exported here verbatim so
that no other file's imports change, and so that `unittest.mock`/pytest
`monkeypatch` targeting `clio_relay.process_containment.<name>` (as the
existing test suite extensively does) keeps working: the owner modules call
back into this facade for any name the test suite is known to replace,
resolving it fresh at call time exactly as a bare name would have resolved
against this module's own globals before the split.

The jarvis-packages copy of this file
(`jarvis-packages/clio_relay/clio_relay/process_containment.py`) is a
deliberately vendored, byte-identical mirror used by the isolated JARVIS
runtime, policed by
`tests/test_process_containment.py::test_embedded_containment_source_is_an_exact_isolated_runtime_mirror`.
Every `process_containment_*` owner module is mirrored there too, under the
same filenames, so the isolated runtime's own `clio_relay` package resolves
the same split.
"""

from __future__ import annotations

# ruff: noqa: F401 -- every import below is an intentional facade re-export,
# including the private (`_`-prefixed) names the existing test suite reaches
# via `monkeypatch.setattr(process_containment, "_name", ...)`. See __all__
# below for the subset of this surface meant for external consumers.
#
# os/shutil/subprocess/sys are held here (unused by this file's own code)
# because the test suite patches attributes on them via
# `monkeypatch.setattr(process_containment.os, "write", ...)` etc. -- these
# are the same shared stdlib module objects every owner module imports, so
# holding a reference here is enough for a patch made through the facade to
# be visible wherever an owner module later reads e.g. `os.write`.
import os
import shutil
import subprocess
import sys

from clio_relay.process_containment_broker import (
    _await_broker_readiness,
    _is_bounded_broker_integer,
    _parse_broker_startup_record,
    _precreate_broker_readiness,
    _release_broker,
    _remove_broker_readiness,
    _spawn_broker,
    _validate_broker_credential_payload,
    _validate_broker_target_environment,
)
from clio_relay.process_containment_broker_script import _BROKER_SCRIPT
from clio_relay.process_containment_environment import (
    broker_child_environment_payload,
    consume_broker_child_environment,
    enforce_linux_secret_memory_gate,
)
from clio_relay.process_containment_popen import (
    inherited_relay_containment,
    nested_popen_kwargs,
    owner_environment,
    owner_popen_kwargs,
)
from clio_relay.process_containment_posix import (
    _current_posix_group,
    _posix_descendant_process_ids,
    _posix_process_group_ids,
    _posix_process_snapshot,
    _process_exists,
    _residual_process_ids,
    _signal_posix_tree,
    _wait_for_exit,
)
from clio_relay.process_containment_recorded import (
    _terminate_recorded_windows_process_tree,
    process_start_identity,
    terminate_recorded_process_tree,
)
from clio_relay.process_containment_registry import (
    _OWNED_PROCESSES,
    _OWNED_PROCESSES_LOCK,
    _OWNED_PROCESSES_RELEASING,
    _register_owned_process,
)
from clio_relay.process_containment_spawn import (
    _cleanup_failed_owned_spawn,
    _close_failed_broker_streams,
    _notify_containment_ready,
    containment_capability,
    owned_process_metadata,
    spawn_owned_process,
)
from clio_relay.process_containment_systemd_core import (
    _linux_cgroup_process_ids,
    _parse_systemd_properties,
    _remaining_deadline_seconds,
    _validated_recorded_systemd_scope_path,
    _validated_systemd_cgroup_path,
    _wait_for_linux_cgroup_empty,
)
from clio_relay.process_containment_systemd_query import (
    adopt_linux_systemd_scope_identity,
    recorded_linux_systemd_scope_process_ids,
    terminate_recorded_linux_systemd_scope,
)
from clio_relay.process_containment_systemd_scope import (
    _cleanup_failed_linux_systemd_spawn,
    _probe_linux_systemd_scope_capability,
    _release_linux_systemd_scope,
    _spawn_linux_systemd_scope,
    _systemctl_user,
    _terminate_linux_systemd_scope,
    _wait_for_systemd_scope_identity,
)
from clio_relay.process_containment_termination import (
    ensure_owned_process_tree_empty,
    release_owned_process,
    terminate_nested_process,
    terminate_owned_process,
    terminate_process_tree,
)
from clio_relay.process_containment_types import (
    _BROKER_EXCEPTION_TYPE_PATTERN,
    _BROKER_STARTUP_STAGE_CODES,
    _CGROUP_ROOT,
    BROKER_CHILD_ENVIRONMENT_SCHEMA,
    BROKER_CREDENTIAL_FD_ENV,
    BROKER_HANDSHAKE_TIMEOUT_SECONDS,
    BROKER_PROTOCOL_MAX_BYTES,
    BROKER_READY_FD_ENV,
    BROKER_READY_TIMEOUT_SECONDS,
    BROKER_SETUP_MAX_BYTES,
    BROKER_STARTUP_RECORD_MAX_BYTES,
    BROKER_STARTUP_RECORD_SCHEMA,
    BROKER_STDIN_MAX_BYTES,
    CONTAINMENT_ENV,
    CONTAINMENT_VALUE,
    DISCOVERY_ROUNDS,
    DISCOVERY_TIMEOUT_SECONDS,
    POLL_SECONDS,
    SYSTEMCTL_OUTPUT_MAX_BYTES,
    TERMINATION_TIMEOUT_SECONDS,
    OwnedProcessSpawnError,
    _BrokerReadiness,
    _BrokerStartupDiagnostic,
    _BrokerStartupRecord,
    _OwnedProcessState,
    _reject_broker_duplicate_keys,
    _ResourceModule,
)
from clio_relay.process_containment_windows import (
    _assign_windows_job,
    _close_windows_handle,
    _create_windows_job,
    _terminate_windows_job,
    _terminate_windows_tree,
    _windows_job_active_processes,
    _windows_process_start_identity,
)

__all__ = [
    "BROKER_CHILD_ENVIRONMENT_SCHEMA",
    "BROKER_CREDENTIAL_FD_ENV",
    "BROKER_HANDSHAKE_TIMEOUT_SECONDS",
    "BROKER_PROTOCOL_MAX_BYTES",
    "BROKER_READY_FD_ENV",
    "BROKER_READY_TIMEOUT_SECONDS",
    "BROKER_SETUP_MAX_BYTES",
    "BROKER_STARTUP_RECORD_MAX_BYTES",
    "BROKER_STARTUP_RECORD_SCHEMA",
    "BROKER_STDIN_MAX_BYTES",
    "CONTAINMENT_ENV",
    "CONTAINMENT_VALUE",
    "DISCOVERY_ROUNDS",
    "DISCOVERY_TIMEOUT_SECONDS",
    "OwnedProcessSpawnError",
    "POLL_SECONDS",
    "SYSTEMCTL_OUTPUT_MAX_BYTES",
    "TERMINATION_TIMEOUT_SECONDS",
    "adopt_linux_systemd_scope_identity",
    "broker_child_environment_payload",
    "consume_broker_child_environment",
    "containment_capability",
    "enforce_linux_secret_memory_gate",
    "ensure_owned_process_tree_empty",
    "inherited_relay_containment",
    "nested_popen_kwargs",
    "owned_process_metadata",
    "owner_environment",
    "owner_popen_kwargs",
    "process_start_identity",
    "recorded_linux_systemd_scope_process_ids",
    "release_owned_process",
    "spawn_owned_process",
    "terminate_nested_process",
    "terminate_owned_process",
    "terminate_process_tree",
    "terminate_recorded_linux_systemd_scope",
    "terminate_recorded_process_tree",
]
