"""Command-line interface for clio-relay."""

from __future__ import annotations

import base64
import binascii
import ctypes
import hashlib
import hmac
import json
import math
import os
import re
import shlex
import socket
import stat
import subprocess
import sys
from collections import Counter
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from importlib import import_module
from json import JSONDecodeError
from pathlib import Path, PurePosixPath
from time import monotonic, sleep
from typing import Annotated, Any, Literal, cast
from uuid import uuid4

import typer
import uvicorn
import yaml
from filelock import FileLock
from filelock import Timeout as FileLockTimeout
from pydantic import ValidationError

from clio_relay.application_profiles import install_cluster_app_over_ssh
from clio_relay.bootstrap import (
    bootstrap_cluster_over_ssh,
    install_local_frp,
    package_source_root,
)
from clio_relay.bootstrap_acceptance import bootstrap_reuse_acceptance_evidence
from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    bootstrap_invocation_lock,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)
from clio_relay.bounded_process import BoundedProcessError, run_bounded_process
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    ClusterRegistry,
    ClusterTargetIdentity,
    DirectTransportConfig,
    FrpTransportConfig,
    RemoteMcpContract,
    RemoteMcpProfile,
    RemoteMcpServerConfig,
    WorkerCapacityPolicy,
    acquire_private_configuration_windows_parent_guard,
    default_registry_path,
    ensure_private_configuration_windows_handle,
    open_private_atomic_file,
    open_private_configuration_windows_descriptor,
    release_private_configuration_windows_parent_guard,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.deployment import (
    install_endpoint_user_service_over_ssh,
    render_endpoint_user_service,
    restart_endpoint_user_service_over_ssh,
    write_endpoint_user_service,
)
from clio_relay.doctor import run_cluster_doctor, run_doctor
from clio_relay.endpoint import EndpointWorker
from clio_relay.endpoint_service_status import endpoint_service_readiness_over_ssh
from clio_relay.errors import (
    ConfigurationError,
    NotFoundError,
    ObservationTimeoutError,
    RelayError,
)
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.frp_check import run_frpc_connection_check
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.installation import (
    INSTALL_RECEIPT_PATH_ENV,
    InstallReceipt,
    attach_verified_worker_identity,
    default_install_receipt_path,
    installation_info,
    verify_remote_worker_info,
    worker_runtime_info,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    JARVIS_MCP_CACHE_SERVER_NAME,
    is_virtual_jarvis_control_query,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_artifact_binding_from_entry,
    jarvis_mcp_env_from,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
    require_handle_first_jarvis_run_schema,
)
from clio_relay.jarvis_mcp_validation import build_jarvis_mcp_validation_report
from clio_relay.jarvis_service_runtime import (
    private_jarvis_service_runtime_authority_document,
    resolve_local_jarvis_service_runtime_authority,
)
from clio_relay.live_acceptance import LiveAcceptanceOptions, run_live_acceptance
from clio_relay.mcp_server import (
    load_registered_remote_mcp_catalog,
    render_agent_mcp_profile,
    serve_stdio,
    static_mcp_tool_names,
)
from clio_relay.mcp_stdio_validation import PackagedMcpStdioSession, run_packaged_mcp_stdio_session
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    ArtifactUse,
    Cursor,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    JobWaitResult,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    MonitorRule,
    MonitorRuleAction,
    OwnerSessionClosure,
    ProgressRecord,
    RelayJob,
    RemoteAgentTaskSpec,
    SchedulerPhase,
    ServiceRuntimeSpec,
    TaskEventStatus,
    TaskTimelineEvent,
    artifact_use_payload,
    validate_artifact_use_collection,
)
from clio_relay.owner_session_admission import (
    assert_no_unscoped_desktop_admission_state as _assert_no_unscoped_desktop_admission_state,
)
from clio_relay.owner_session_admission import (
    desktop_owner_session_admission_id as _desktop_owner_session_admission_id,
)
from clio_relay.owner_session_admission import (
    owner_session_admission_status,
    owner_session_gateway_admission,
    owner_session_transition_lock,
)
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    MAX_RESPONSE_PAGE_RECORDS,
)
from clio_relay.process_containment import consume_broker_child_environment
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.public_records import public_gateway_session
from clio_relay.queue_management import (
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_STALE_SCAN_LIMIT,
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)
from clio_relay.queue_validation import run_queue_management_validation
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpsConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
    render_frps_config,
)
from clio_relay.relay_ops import (
    cancel_job as request_cancel_job,
)
from clio_relay.relay_ops import (
    evaluate_monitor_rules,
    job_wait_result,
    monitor_job,
    observe_until_terminal,
    read_artifact_bytes,
    read_job_log,
    wait_for_terminal,
)
from clio_relay.relay_ops import (
    job_status as get_job_status,
)
from clio_relay.release_validation import (
    LocalReleaseValidationOptions,
    run_local_release_validation,
)
from clio_relay.remote_cli import (
    remote_command_timeout,
    remove_remote_file,
    run_remote_clio,
    run_remote_shell,
    should_execute_on_cluster,
    stage_jarvis_yaml,
    write_remote_file,
)
from clio_relay.remote_mcp import (
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS,
    MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
    RemoteMcpAcceptanceReport,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    RemoteMcpSpackConfigurationObservation,
    RemoteMcpStructuredResultExpectation,
    VirtualRemoteMcpCatalog,
    build_remote_mcp_acceptance_report,
    build_remote_mcp_spack_fresh_install_transition_report,
    cache_entry_from_discovery_artifact,
    default_remote_mcp_cache_path,
    remote_mcp_execution_fingerprint,
    resolve_pinned_mcp_admission,
    resolve_registered_remote_mcp_admission,
)
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.runtime_metadata import RUNTIME_METADATA_SCHEMA, native_execution_documents
from clio_relay.scheduler_providers import (
    allocation_connector_provider_for_scheduler,
    provider_for_scheduler,
    validation_provider_for_scheduler,
)
from clio_relay.scheduler_validation import run_scheduler_lifecycle_validation
from clio_relay.service_runtime import (
    ServiceRuntimePendingResult,
    ServiceRuntimeSupervisor,
)
from clio_relay.session_api import (
    OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS,
    submit_owned_session_job,
)
from clio_relay.session_lifecycle import (
    MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES,
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
    CleanupResource,
    OwnedSessionCleanupFinalizeRequest,
    OwnedSessionCleanupReportReadRequest,
    OwnedSessionIdentityChallengeRequest,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartRejection,
    OwnedSessionStartRequest,
    OwnedSessionTeardownRequest,
    SessionApiReleaseIdentity,
    SessionLifecycleReport,
    cleanup_connectors_cover_gateways,
    detach_remote_session,
    execute_owned_session_cleanup_finalize,
    execute_owned_session_cleanup_report_read,
    execute_owned_session_identity_challenge,
    execute_owned_session_start,
    execute_owned_session_teardown,
    finalize_remote_session_cleanup_report,
    inspect_owned_session_recovery_status,
    inspect_owned_session_start_status,
    open_owned_session_transaction,
    plan_remote_session_start,
    publish_owned_session_api_startup_receipt,
    query_remote_session_start,
    read_remote_session_cleanup_report,
    session_lifecycle_report_bytes,
    session_lifecycle_report_sha256,
    start_remote_session,
    start_remote_session_durable,
    status_remote_session,
    teardown_remote_session,
    watch_remote_session_start,
)
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    StorageManagedQueue,
    storage_managed_queue,
)
from clio_relay.transport_probe import (
    run_frp_direct_http_probe,
    run_frp_http_probe,
    run_ssh_forward_http_probe,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    SoftwareIdentity,
    ValidationCheck,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    default_report_path,
    durably_ensure_validation_directory,
    evaluate_release_gate,
    load_release_gate_policy,
    load_validation_report,
    new_live_validation_report,
    redact_sensitive_values,
    sha256_file,
    write_release_gate_result,
    write_validation_report,
)
from clio_relay.worker_concurrency import parse_kind_concurrency_options

MAX_INTERNAL_COLLECTION_RECORDS = 10_000
MAX_OWNER_GATEWAY_CLEANUP_PASSES = 4
DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS = 30.0
DEFAULT_RELAY_CANCEL_POLL_SECONDS = 0.25
MAX_RELAY_CANCEL_TIMEOUT_SECONDS = 3_600.0
REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS = 120.0
REMOTE_CLEANUP_WORKER_INFO_TIMEOUT_SECONDS = 20.0
REMOTE_JOB_WAIT_STATUS_TIMEOUT_SECONDS = 30.0
MAX_FINALIZED_CLEANUP_RETRY_OUTPUT_BYTES = 1024 * 1024
MAX_CLEANUP_VALIDATION_REPORT_BYTES = 8 * 1024 * 1024
MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES = 8 * 1024 * 1024
MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES = 64 * 1024
MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_ENTRIES = 11
MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_STORED_BYTES = 2 * (
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES
)
_LOCAL_CLEANUP_REPORT_ARTIFACT_DIRECTORY_NAME = "cleanup-evidence-v1"
_LOCAL_CLEANUP_REPORT_ARTIFACT_PATTERN = re.compile(r"^r-[0-9a-f]{64}\.(?:p[0-9]{4}|manifest)$")
_LOCAL_CLEANUP_REPORT_PENDING_PATTERN = re.compile(
    r"^\.r-[0-9a-f]{64}\.(?:p[0-9]{4}|manifest)\.pending$"
)


def _cleanup_evidence_state_parent() -> Path:
    """Return the one user-scoped parent for all local cleanup evidence."""
    receipt_path = default_install_receipt_path().expanduser()
    if not receipt_path.is_absolute():
        raise ConfigurationError(
            f"{INSTALL_RECEIPT_PATH_ENV} must be an absolute path when cleanup evidence "
            "is persisted"
        )
    return receipt_path.parent


@dataclass(frozen=True, slots=True)
class _LocalCleanupReportChunk:
    """One immutable bounded chunk of a locally retained cleanup report."""

    path: Path
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _LocalCleanupReportArtifact:
    """Manifest and chunks retaining one exact coordinator cleanup report."""

    manifest_path: Path
    manifest_sha256: str
    report_sha256: str
    report_size: int
    chunks: tuple[_LocalCleanupReportChunk, ...]


@dataclass(frozen=True, slots=True)
class _WindowsPinnedDirectory:
    """Windows directory handle held without delete sharing to block path swaps."""

    path: Path
    status: os.stat_result
    handle: ctypes.c_void_p


class _WindowsCleanupFileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _WindowsCleanupFileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", ctypes.c_uint32),
        ("creation_time", _WindowsCleanupFileTime),
        ("last_access_time", _WindowsCleanupFileTime),
        ("last_write_time", _WindowsCleanupFileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


@dataclass(frozen=True, slots=True)
class _CleanupEvidenceLock:
    """One process-owned lock serializing a local cleanup evidence store."""

    path: Path
    parent_fd: int | None = None
    descriptor: int | None = None
    windows_handle: ctypes.c_void_p | None = None
    windows_parent: _WindowsPinnedDirectory | None = None


def _optional_runtime_descriptor(descriptor: int | None) -> int | None:
    """Preserve an OS-selected descriptor as optional across nested helpers."""
    return descriptor


def _windows_parent_guard_names(
    guard: tuple[Path, ctypes.c_void_p] | None,
) -> frozenset[str]:
    """Return the one exact internal guard name ignored by bounded enumeration."""
    return frozenset() if guard is None else frozenset({guard[0].name})


def _open_windows_pinned_directory(
    path: Path,
    *,
    expected: os.stat_result,
    acl_write: bool = False,
) -> _WindowsPinnedDirectory:
    """Open and verify one non-reparse Windows directory without delete sharing."""
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows directory pinning is unavailable")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_attributes = kernel32.GetFileAttributesW
    get_attributes.argtypes = [ctypes.c_wchar_p]
    get_attributes.restype = ctypes.c_uint32
    invalid_attributes = 0xFFFFFFFF
    file_attribute_directory = 0x00000010
    file_attribute_reparse_point = 0x00000400
    storage_path = internal_filesystem_path(path, force_extended=True)
    attributes = int(get_attributes(str(storage_path)))
    if (
        attributes == invalid_attributes
        or not attributes & file_attribute_directory
        or attributes & file_attribute_reparse_point
    ):
        raise RelayError("local cleanup report artifact directory is a Windows reparse point")
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    raw_handle = create_file(
        str(storage_path),
        0x00000080 | (0x00020000 | 0x00040000 | 0x00080000 if acl_write else 0),
        0x00000001 | 0x00000002,
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        error_number = ctypes.get_last_error()
        raise RelayError(
            "local cleanup report artifact directory cannot be pinned: "
            f"{ctypes.FormatError(error_number)}"
        )
    handle = ctypes.c_void_p(raw_handle)
    try:
        observed = os.stat(storage_path, follow_symlinks=False)
        attributes = int(get_attributes(str(storage_path)))
        if not (
            os.path.samestat(expected, observed)
            and stat.S_ISDIR(observed.st_mode)
            and attributes != invalid_attributes
            and attributes & file_attribute_directory
            and not attributes & file_attribute_reparse_point
        ):
            raise RelayError("local cleanup report artifact directory changed while pinning")
        return _WindowsPinnedDirectory(path=path, status=observed, handle=handle)
    except BaseException:
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        close_handle(handle)
        raise


def _verify_windows_pinned_directory(anchor: _WindowsPinnedDirectory) -> None:
    """Revalidate the named directory while its no-delete-share handle remains open."""
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows directory verification is unavailable")
    storage_path = internal_filesystem_path(anchor.path, force_extended=True)
    observed = os.stat(storage_path, follow_symlinks=False)
    if not os.path.samestat(anchor.status, observed):
        raise RelayError("local cleanup report artifact directory identity changed")
    get_attributes = ctypes.WinDLL("kernel32", use_last_error=True).GetFileAttributesW
    get_attributes.argtypes = [ctypes.c_wchar_p]
    get_attributes.restype = ctypes.c_uint32
    attributes = int(get_attributes(str(storage_path)))
    if attributes == 0xFFFFFFFF or attributes & 0x00000400:
        raise RelayError("local cleanup report artifact directory became a reparse point")


def _close_windows_pinned_directory(anchor: _WindowsPinnedDirectory | None) -> None:
    """Close one Windows directory anchor."""
    if anchor is None:
        return
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows directory handles cannot be closed on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    if not close_handle(anchor.handle):
        error_number = ctypes.get_last_error()
        raise RelayError(
            "local cleanup report artifact directory handle could not be closed: "
            f"{ctypes.FormatError(error_number)}"
        )


def _windows_cleanup_file_information(
    handle: ctypes.c_void_p,
    *,
    path: Path,
) -> _WindowsCleanupFileInformation:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows cleanup handles cannot be inspected on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsCleanupFileInformation),
    ]
    get_information.restype = ctypes.c_int
    information = _WindowsCleanupFileInformation()
    if not get_information(handle, ctypes.byref(information)):
        error_number = ctypes.get_last_error()
        raise RelayError(
            f"cleanup evidence lock cannot be inspected: {ctypes.FormatError(error_number)}"
        )
    if (
        information.attributes & 0x00000010
        or information.attributes & 0x00000400
        or information.number_of_links != 1
    ):
        raise RelayError(f"cleanup evidence lock is not one private regular file: {path}")
    return information


def _acquire_cleanup_evidence_lock() -> _CleanupEvidenceLock:
    """Acquire the crash-released lock shared by cleanup artifacts and validation output."""
    requested_parent = _cleanup_evidence_state_parent()
    durably_ensure_validation_directory(requested_parent)
    parent_directory = requested_parent.resolve(strict=True)
    if os.path.normcase(str(parent_directory)) != os.path.normcase(str(requested_parent)):
        raise RelayError("cleanup evidence lock parent traverses a symlink or reparse point")
    parent_status = os.lstat(parent_directory)
    if not stat.S_ISDIR(parent_status.st_mode) or stat.S_ISLNK(parent_status.st_mode):
        raise RelayError("cleanup evidence lock parent is not a real directory")
    lock_path = parent_directory / ".clio-cleanup-evidence-v1.lock"
    if os.name == "posix":
        parent_fd = os.open(
            parent_directory,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        descriptor: int | None = None
        try:
            if not os.path.samestat(parent_status, os.fstat(parent_fd)):
                raise RelayError("cleanup evidence lock parent changed while opening")
            try:
                descriptor = os.open(
                    lock_path.name,
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=parent_fd,
                )
                os.fchmod(descriptor, 0o600)
                os.fsync(descriptor)
                os.fsync(parent_fd)
            except FileExistsError:
                descriptor = os.open(
                    lock_path.name,
                    os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=parent_fd,
                )
            opened = os.fstat(descriptor)
            linked = os.stat(lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(linked.st_mode)
                and opened.st_nlink == 1
                and linked.st_nlink == 1
                and opened.st_uid == os.geteuid()
                and linked.st_uid == os.geteuid()
                and stat.S_IMODE(opened.st_mode) == 0o600
                and stat.S_IMODE(linked.st_mode) == 0o600
                and os.path.samestat(opened, linked)
            ):
                raise RelayError("cleanup evidence lock is not one owner-private regular file")
            flock = import_module("fcntl").flock
            try:
                flock(descriptor, 2 | 4)
            except BlockingIOError:
                raise RelayError("another cleanup is writing evidence in this directory") from None
            confirmed = os.stat(lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not os.path.samestat(opened, confirmed):
                raise RelayError("cleanup evidence lock changed during acquisition")
            return _CleanupEvidenceLock(
                path=lock_path,
                parent_fd=parent_fd,
                descriptor=descriptor,
            )
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            os.close(parent_fd)
            raise

    windows_parent: _WindowsPinnedDirectory | None = None
    windows_handle: ctypes.c_void_p | None = None
    windows_parent_guard: tuple[Path, ctypes.c_void_p] | None = None
    try:
        windows_parent_guard = acquire_private_configuration_windows_parent_guard(parent_directory)
        windows_parent = _open_windows_pinned_directory(
            parent_directory,
            expected=parent_status,
        )
        storage_lock_path = internal_filesystem_path(lock_path, force_extended=True)
        try:
            lock_status = os.lstat(storage_lock_path)
        except FileNotFoundError:
            try:
                with open_private_atomic_file(storage_lock_path) as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
            except FileExistsError:
                pass
            lock_status = os.lstat(storage_lock_path)
        if not (
            stat.S_ISREG(lock_status.st_mode)
            and not stat.S_ISLNK(lock_status.st_mode)
            and lock_status.st_nlink == 1
            and not getattr(lock_status, "st_file_attributes", 0) & 0x00000400
        ):
            raise RelayError("cleanup evidence lock is not one private regular file")
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        create_file.restype = ctypes.c_void_p
        raw_handle = create_file(
            str(storage_lock_path),
            0x80000000 | 0x00020000 | 0x00040000 | 0x00080000,
            0,
            None,
            3,
            0x00200000,
            None,
        )
        if raw_handle in (None, ctypes.c_void_p(-1).value):
            error_number = ctypes.get_last_error()
            if error_number in {5, 32, 33}:
                raise RelayError("another cleanup is writing evidence in this directory") from None
            raise RelayError(
                f"cleanup evidence lock cannot be opened: {ctypes.FormatError(error_number)}"
            )
        windows_handle = ctypes.c_void_p(raw_handle)
        information = _windows_cleanup_file_information(
            windows_handle,
            path=lock_path,
        )
        observed = os.lstat(storage_lock_path)
        file_index = (int(information.file_index_high) << 32) | int(information.file_index_low)
        if not (
            os.path.samestat(lock_status, observed)
            and observed.st_nlink == 1
            and observed.st_ino == file_index
        ):
            raise RelayError("cleanup evidence lock changed during acquisition")
        ensure_private_configuration_windows_handle(
            storage_lock_path,
            handle=windows_handle,
            directory=False,
        )
        _verify_windows_pinned_directory(windows_parent)
        result = _CleanupEvidenceLock(
            path=lock_path,
            windows_handle=windows_handle,
            windows_parent=windows_parent,
        )
        acquired_parent_guard = windows_parent_guard
        windows_parent_guard = None
        release_private_configuration_windows_parent_guard(acquired_parent_guard)
        return result
    except BaseException:
        if windows_handle is not None:
            close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
            close_handle.argtypes = [ctypes.c_void_p]
            close_handle.restype = ctypes.c_int
            close_handle(windows_handle)
        _close_windows_pinned_directory(windows_parent)
        release_private_configuration_windows_parent_guard(windows_parent_guard)
        raise


def _release_cleanup_evidence_lock(lock: _CleanupEvidenceLock | None) -> None:
    """Release one cleanup evidence lock without removing its private stable inode."""
    if lock is None:
        return
    release_error: BaseException | None = None
    if lock.descriptor is not None:
        try:
            import_module("fcntl").flock(lock.descriptor, 8)
        except BaseException as exc:  # pragma: no cover - OS release failure
            release_error = exc
        try:
            os.close(lock.descriptor)
        except BaseException as exc:  # pragma: no cover - OS release failure
            release_error = release_error or exc
    if lock.parent_fd is not None:
        try:
            os.close(lock.parent_fd)
        except BaseException as exc:  # pragma: no cover - OS release failure
            release_error = release_error or exc
    if lock.windows_handle is not None:
        if os.name != "nt":  # pragma: no cover - corrupt cross-platform state
            raise RelayError("Windows cleanup evidence handle exists on a non-Windows platform")
        close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        if not close_handle(lock.windows_handle):
            error_number = ctypes.get_last_error()
            release_error = release_error or OSError(
                error_number,
                ctypes.FormatError(error_number),
                str(lock.path),
            )
    try:
        _close_windows_pinned_directory(lock.windows_parent)
    except BaseException as exc:  # pragma: no cover - OS release failure
        release_error = release_error or exc
    if release_error is not None:
        raise RelayError(f"cleanup evidence lock could not be released: {release_error}")


def _verify_cleanup_evidence_lock(
    lock: _CleanupEvidenceLock,
    *,
    expected_parent: Path,
) -> None:
    """Verify that the retained cleanup lock still pins the named evidence parent."""
    resolved_parent = expected_parent.absolute().resolve(strict=True)
    if os.path.normcase(str(resolved_parent)) != os.path.normcase(str(lock.path.parent)):
        raise RelayError("cleanup evidence lock does not cover the validation parent")
    if lock.parent_fd is not None and lock.descriptor is not None:
        parent_linked = os.lstat(resolved_parent)
        lock_linked = os.stat(
            lock.path.name,
            dir_fd=lock.parent_fd,
            follow_symlinks=False,
        )
        if not (
            os.path.samestat(os.fstat(lock.parent_fd), parent_linked)
            and os.path.samestat(os.fstat(lock.descriptor), lock_linked)
            and lock_linked.st_nlink == 1
        ):
            raise RelayError("cleanup evidence lock identity changed")
        return
    if lock.windows_parent is None or lock.windows_handle is None:
        raise RelayError("cleanup evidence lock has no platform ownership handle")
    _verify_windows_pinned_directory(lock.windows_parent)
    information = _windows_cleanup_file_information(lock.windows_handle, path=lock.path)
    lock_linked = os.lstat(internal_filesystem_path(lock.path, force_extended=True))
    file_index = (int(information.file_index_high) << 32) | int(information.file_index_low)
    if lock_linked.st_ino != file_index or lock_linked.st_nlink != 1:
        raise RelayError("cleanup evidence lock identity changed")


OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS = 90.0
SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS = 60.0
MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES = 128 * 1024
MAX_SPACK_CONFIGURATION_TREE_ENTRIES = 1_024


SCHEDULER_SENTINEL_ACTIVE_PHASES = frozenset({"submitted", "pending", "allocated", "running"})
SCHEDULER_SENTINEL_PRESERVED_PHASES = SCHEDULER_SENTINEL_ACTIVE_PHASES | {"completed"}
BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS = 24.0
BOOTSTRAP_REPAIR_DEADLINE_SECONDS = 55.0
_ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE = "__clio_relay_acceptance_report_command__"


def _acceptance_report_command[CommandCallback: Callable[..., Any]](
    callback: CommandCallback,
) -> CommandCallback:
    """Mark a CLI callback as a canonical acceptance-report producer."""
    setattr(callback, _ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE, True)
    return callback


app = typer.Typer(no_args_is_help=True)
endpoint_app = typer.Typer(no_args_is_help=True)
relay_host_app = typer.Typer(no_args_is_help=True)
job_app = typer.Typer(no_args_is_help=True)
cluster_app = typer.Typer(no_args_is_help=True)
agent_app = typer.Typer(no_args_is_help=True)
monitor_app = typer.Typer(no_args_is_help=True)
api_app = typer.Typer(no_args_is_help=True)
session_app = typer.Typer(no_args_is_help=True)
gateway_app = typer.Typer(no_args_is_help=True)
queue_app = typer.Typer(no_args_is_help=True)
worker_app = typer.Typer(no_args_is_help=True)
scheduler_app = typer.Typer(no_args_is_help=True)
remote_mcp_app = typer.Typer(no_args_is_help=True)
release_app = typer.Typer(no_args_is_help=True)
storage_app = typer.Typer(no_args_is_help=True)

app.add_typer(endpoint_app, name="endpoint")
app.add_typer(relay_host_app, name="relay-host")
app.add_typer(job_app, name="job")
app.add_typer(cluster_app, name="cluster")
app.add_typer(agent_app, name="agent")
app.add_typer(monitor_app, name="monitor")
app.add_typer(api_app, name="api")
app.add_typer(session_app, name="session")
app.add_typer(gateway_app, name="gateway")
app.add_typer(queue_app, name="queue")
app.add_typer(worker_app, name="worker")
app.add_typer(scheduler_app, name="scheduler")
app.add_typer(remote_mcp_app, name="remote-mcp")
app.add_typer(release_app, name="release")
app.add_typer(storage_app, name="storage")


@app.callback()
def main() -> None:
    """Run clio-relay commands."""
    consume_broker_child_environment()


@app.command("jarvis-runtime-authority", hidden=True)
def jarvis_runtime_authority(
    execution_id: str,
    pipeline_id: Annotated[str, typer.Option(help="Exact JARVIS pipeline identity.")],
    package_id: Annotated[str, typer.Option(help="Exact JARVIS package identity.")],
    service_instance_id: Annotated[
        str,
        typer.Option(help="Exact JARVIS service instance identity."),
    ],
    revision: Annotated[int, typer.Option(help="Exact current service revision.")],
    token_sha256: Annotated[
        str,
        typer.Option(help="Public SHA-256 identity of the private bearer capability."),
    ],
) -> None:
    """Resolve one private JARVIS authority for relay-internal transport."""

    def action() -> None:
        settings = RelaySettings.from_env()
        authority = resolve_local_jarvis_service_runtime_authority(
            jarvis_bin=settings.jarvis_bin,
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            package_id=package_id,
            service_instance_id=service_instance_id,
            revision=revision,
            token_sha256=token_sha256,
        )
        typer.echo(
            json.dumps(
                private_jarvis_service_runtime_authority_document(authority),
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    _run_or_exit(action)


@storage_app.command("status")
def storage_status() -> None:
    """Return machine-readable storage admission readiness."""
    queue = storage_managed_queue(RelaySettings.from_env())
    typer.echo(json.dumps(queue.storage_runtime.status(), indent=2))


@app.command()
def init(
    migrate_legacy_output: Annotated[
        bool,
        typer.Option(
            help=(
                "Authorize migration of exact oversized v0.9 output events after every "
                "queue writer has been stopped and verified inactive."
            )
        ),
    ] = False,
) -> None:
    """Initialize local queue, spool, and cluster registry files."""
    settings = RelaySettings.from_env()
    storage_managed_queue(settings, migrate_legacy_output=migrate_legacy_output)
    registry = ClusterRegistry.load(default_registry_path())
    typer.echo(
        f"initialized core={settings.core_dir} spool={settings.spool_dir} "
        f"clusters={','.join(sorted(registry.clusters))}"
    )


@release_app.command("validate-local")
@_acceptance_report_command
def release_validate_local(
    project_root: Annotated[
        Path,
        typer.Option(help="Clean source checkout to validate."),
    ] = Path("."),
    report: Annotated[
        Path | None,
        typer.Option(help="JSON report path. Defaults under .clio-relay/validation-reports."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering."),
    ] = None,
    artifact_dir: Annotated[
        Path | None,
        typer.Option(help="Optional empty output directory for wheel and sdist artifacts."),
    ] = None,
    prebuilt_artifact_dir: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Reuse an exact build-once wheel, sdist, and SHA256SUMS directory; "
                "never build artifacts in this validation run."
            )
        ),
    ] = None,
) -> None:
    """Run the complete local release gate and persist evidence on failure."""
    report_path = report or default_report_path("local")
    seed_report = new_live_validation_report(
        scenario="local-release",
        cluster="local",
    )
    write_validation_report(seed_report, report_path)

    def _run() -> None:
        try:
            result = run_local_release_validation(
                LocalReleaseValidationOptions(
                    project_root=project_root,
                    report_path=report_path,
                    markdown_report_path=markdown_report,
                    artifact_dir=artifact_dir,
                    prebuilt_artifact_dir=prebuilt_artifact_dir,
                    report_id=seed_report.report_id,
                )
            )
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            if current_report is None or result.report_id != seed_report.report_id:
                raise RelayError(
                    "local release validation did not persist the current invocation report"
                )
        except BaseException as exc:
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            _write_failed_acceptance_report(
                path=report_path,
                scenario="local-release",
                cluster="local",
                check_id="local-release.completed",
                summary="complete local release gate",
                error=exc,
                launcher=None,
                install_source=None,
                artifact=None,
                partial_report=current_report or seed_report,
            )
            typer.echo(f"validation.report={report_path.resolve()}")
            raise
        typer.echo(f"validation.status={result.status.value}")
        typer.echo(f"validation.report={report_path.resolve()}")

    _run_or_exit(_run)


@release_app.command("gate")
def release_gate(
    policy: Annotated[Path, typer.Option(help="Machine-readable 1.0 release policy.")],
    report: Annotated[
        list[Path] | None,
        typer.Option(help="Validation JSON report. Repeat for multiple reports."),
    ] = None,
    report_dir: Annotated[
        Path | None,
        typer.Option(help="Directory containing validation JSON reports."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(help="Optional JSON path for the gate decision."),
    ] = None,
    expected_artifact_sha256: Annotated[
        str | None,
        typer.Option(
            help=(
                "SHA-256 independently computed from the immutable candidate wheel. "
                "Every non-local report used by the gate must match it."
            )
        ),
    ] = None,
) -> None:
    """Reject a release unless every policy requirement has released-artifact evidence."""

    def _run() -> None:
        report_paths = list(report or [])
        if report_dir is not None:
            report_paths.extend(sorted(report_dir.glob("*.json")))
        unique_paths = list(dict.fromkeys(path.resolve() for path in report_paths))
        if not unique_paths:
            raise ConfigurationError("release gate requires --report or --report-dir")
        gate_policy = load_release_gate_policy(policy)
        reports = [load_validation_report(path) for path in unique_paths]
        result = evaluate_release_gate(
            gate_policy,
            reports,
            expected_artifact_sha256=expected_artifact_sha256,
        )
        if output is not None:
            write_release_gate_result(result, output)
        typer.echo(result.model_dump_json(indent=2))
        if not result.passed:
            raise typer.Exit(code=1)

    _run_or_exit(_run)


@relay_host_app.command("render-frps-config")
def render_frps(
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to CLIO_RELAY_FRP_TOKEN."),
    ] = None,
    bind_port: Annotated[int, typer.Option(help="frps bind port.")] = 7000,
    transport_protocol: Annotated[
        FrpTransportProtocol,
        typer.Option(help="frpc-to-frps transport protocol."),
    ] = FrpTransportProtocol.WSS,
    dashboard_port: Annotated[
        int | None,
        typer.Option(help="Optional frps dashboard port."),
    ] = None,
) -> None:
    """Render an frps config with no relay application state."""
    _run_or_exit(
        lambda: typer.echo(
            render_frps_config(
                FrpsConfig(
                    bind_port=bind_port,
                    token=_resolve_env_secret(token, "CLIO_RELAY_FRP_TOKEN", "frp token"),
                    transport_protocol=transport_protocol,
                    dashboard_port=dashboard_port,
                )
            )
        )
    )


@relay_host_app.command("render-frpc-config")
def render_frpc(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_port: Annotated[int, typer.Option(help="Local relay endpoint port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy name.")] = "relay-stcp",
) -> None:
    """Render an frpc config using the cluster's configured frp transport."""

    def action() -> None:
        definition = _require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = _require_frp_server_addr(transport.server_addr, cluster)
        typer.echo(
            render_frpc_config(
                FrpcConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=_resolve_env_secret(token, transport.token_env, "frp token"),
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    proxy_name=proxy_name,
                    local_port=local_port,
                    secret_key=_resolve_env_secret(
                        secret_key,
                        transport.stcp_secret_env,
                        "stcp secret",
                    ),
                )
            )
        )

    _run_or_exit(action)


@relay_host_app.command("test-frpc-connection")
@_acceptance_report_command
def test_frpc(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_port: Annotated[int, typer.Option(help="Local relay endpoint port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy name.")] = "relay-stcp-live-check",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds frpc must stay connected before success."),
    ] = 10.0,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical frpc connection validation JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run a live frpc login check and persist canonical success or failure evidence."""

    canonical_report_path = validation_report or default_report_path(cluster)

    try:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = _require_frp_server_addr(transport.server_addr, cluster)
        config = FrpcConfig(
            server_addr=server_addr,
            server_port=transport.server_port,
            token=_resolve_env_secret(token, transport.token_env, "frp token"),
            transport_protocol=FrpTransportProtocol(transport.protocol),
            proxy_name=proxy_name,
            local_port=local_port,
            secret_key=_resolve_env_secret(
                secret_key,
                transport.stcp_secret_env,
                "stcp secret",
            ),
        )
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.frpc-connection.preflight",
            summary="validate frpc connection acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        _echo_lines(
            _run_frpc_connection_validation(
                cluster=cluster,
                proxy_name=proxy_name,
                frpc_bin=settings.frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
            )
        )

    _run_or_exit(action)


@relay_host_app.command("render-frpc-visitor-config")
def render_frpc_visitor(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    server_name: Annotated[str, typer.Option(help="Cluster-side stcp proxy name.")] = "relay-stcp",
    visitor_name: Annotated[
        str,
        typer.Option(help="Desktop-side stcp visitor name."),
    ] = "relay-stcp-visitor",
    bind_addr: Annotated[
        str,
        typer.Option(help="Local desktop visitor bind address."),
    ] = "127.0.0.1",
) -> None:
    """Render a desktop-side frpc STCP visitor config."""

    def action() -> None:
        definition = _require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = _require_frp_server_addr(transport.server_addr, cluster)
        typer.echo(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=_resolve_env_secret(token, transport.token_env, "frp token"),
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    visitor_name=visitor_name,
                    server_name=server_name,
                    bind_addr=bind_addr,
                    bind_port=bind_port,
                    secret_key=_resolve_env_secret(
                        secret_key,
                        transport.stcp_secret_env,
                        "stcp secret",
                    ),
                )
            )
        )

    _run_or_exit(action)


@relay_host_app.command("test-http-transport")
@_acceptance_report_command
def test_http_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy/server name.")] = "relay-http",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through the transport."),
    ] = 30.0,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through frp STCP."""
    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate HTTP transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    _run_or_exit(
        lambda: _echo_lines(
            _run_transport_validation(
                cluster=cluster,
                transport_mode="frp-relay",
                resource_id=proxy_name,
                resource_role="frp_stcp_probe",
                retain_remote_session=False,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: run_frp_http_probe(
                    cluster=cluster,
                    definition=definition,
                    frpc_bin=settings.frpc_bin,
                    token=_resolve_env_secret(
                        token,
                        definition.frp_transport.token_env,
                        "frp token",
                    ),
                    secret_key=_resolve_env_secret(
                        secret_key,
                        definition.frp_transport.stcp_secret_env,
                        "stcp secret",
                    ),
                    local_bind_port=local_bind_port,
                    remote_api_port=remote_api_port,
                    proxy_name=proxy_name,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                ),
            )
        )
    )


@relay_host_app.command("test-direct-transport")
@_acceptance_report_command
def test_direct_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp/xtcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    proxy_name: Annotated[
        str,
        typer.Option(help="xtcp proxy/server name."),
    ] = "relay-http-direct",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through direct transport."),
    ] = 30.0,
    allow_stcp_fallback: Annotated[
        bool,
        typer.Option(
            "--allow-stcp-fallback/--no-allow-stcp-fallback",
            help="Allow fallback to STCP if XTCP fails.",
        ),
    ] = False,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through frp XTCP direct transport."""
    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate direct transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    _run_or_exit(
        lambda: _echo_lines(
            _run_transport_validation(
                cluster=cluster,
                transport_mode="frp-direct",
                resource_id=proxy_name,
                resource_role="frp_xtcp_probe",
                retain_remote_session=False,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: run_frp_direct_http_probe(
                    cluster=cluster,
                    definition=definition,
                    frpc_bin=settings.frpc_bin,
                    token=_resolve_env_secret(
                        token,
                        definition.frp_transport.token_env,
                        "frp token",
                    ),
                    secret_key=_resolve_env_secret(
                        secret_key,
                        definition.frp_transport.stcp_secret_env,
                        "stcp/xtcp secret",
                    ),
                    local_bind_port=local_bind_port,
                    remote_api_port=remote_api_port,
                    proxy_name=proxy_name,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    allow_stcp_fallback=allow_stcp_fallback,
                ),
            )
        )
    )


@relay_host_app.command("test-ssh-transport")
@_acceptance_report_command
def test_ssh_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop SSH-forward bind port.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    session_id: Annotated[
        str,
        typer.Option(help="Owned remote relay session id for this probe."),
    ] = "relay-ssh-forward-test",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through the SSH forward."),
    ] = 30.0,
    detach_remote: Annotated[
        bool,
        typer.Option(
            "--detach-remote/--teardown-remote",
            help="Leave the remote API session running after the local SSH probe exits.",
        ),
    ] = False,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through SSH local port forwarding."""
    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate SSH transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    _run_or_exit(
        lambda: _echo_lines(
            _run_transport_validation(
                cluster=cluster,
                transport_mode="ssh-forward",
                resource_id=session_id,
                resource_role="ssh_forward_probe",
                retain_remote_session=detach_remote,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: run_ssh_forward_http_probe(
                    cluster=cluster,
                    definition=definition,
                    local_bind_port=local_bind_port,
                    remote_api_port=remote_api_port,
                    session_id=session_id,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    detach_remote=detach_remote,
                ),
            )
        )
    )


@endpoint_app.command("start")
def endpoint_start(
    role: Annotated[EndpointRole, typer.Option(help="Endpoint role.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster name for worker endpoints."),
    ] = None,
    once: Annotated[bool, typer.Option(help="Run one worker iteration and exit.")] = False,
    concurrency: Annotated[
        int,
        typer.Option(help="Number of in-process worker slots for worker endpoints."),
    ] = 1,
    control_query_concurrency: Annotated[
        int,
        typer.Option(help="Slots carved out of total capacity for control-class MCP queries."),
    ] = 0,
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    scheduler_provider: Annotated[
        str | None,
        typer.Option(help="Explicit scheduler provider for worker observation and cancellation."),
    ] = None,
) -> None:
    """Start a desktop or worker endpoint."""
    if concurrency < 1:
        raise typer.BadParameter("--concurrency must be at least 1")
    if control_query_concurrency < 0:
        raise typer.BadParameter("--control-query-concurrency must not be negative")
    if control_query_concurrency >= concurrency:
        raise typer.BadParameter("--control-query-concurrency must be less than --concurrency")
    kind_limits = _kind_concurrency_options(kind_concurrency)
    settings = RelaySettings.from_env()
    definition: ClusterDefinition | None = None
    if role == EndpointRole.WORKER:
        if cluster is None:
            raise typer.BadParameter("--cluster is required for worker endpoints")
        if scheduler_provider is None:
            definition = _require_cluster(cluster)
    selected_scheduler = scheduler_provider
    if selected_scheduler is None and definition is not None:
        selected_scheduler = definition.scheduler_provider
    worker = EndpointWorker(
        role=role,
        settings=settings,
        cluster=cluster or "local",
        concurrency=concurrency,
        control_query_concurrency=control_query_concurrency,
        kind_concurrency=kind_limits,
        scheduler_provider=(
            provider_for_scheduler(selected_scheduler) if role == EndpointRole.WORKER else None
        ),
    )
    try:
        worker.register()
        if once:
            worker.run_once()
            return
        worker.serve_forever()
    finally:
        worker.close()


@endpoint_app.command("status")
def endpoint_status(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional endpoint cluster filter."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global endpoint source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum endpoint source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """Show one stable source window of durable endpoint registrations."""
    settings = RelaySettings.from_env()
    queue = ClioCoreQueue(settings.core_dir)
    queue.initialize()
    endpoints, next_cursor, total = queue.list_endpoints_page(
        cursor=cursor,
        limit=limit,
        cluster=cluster,
    )
    typer.echo(
        _public_json(
            {
                "endpoints": [endpoint.model_dump(mode="json") for endpoint in endpoints],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_endpoint_sequence_high_water",
                "filters_apply_within_source_window": True,
                "core_dir": str(settings.core_dir),
                "spool_dir": str(settings.spool_dir),
            }
        )
    )


@endpoint_app.command("render-user-service")
def endpoint_render_user_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    output: Annotated[
        Path | None,
        typer.Option(help="Optional path to write the systemd user service."),
    ] = None,
    concurrency: Annotated[
        int | None,
        typer.Option(help="Number of in-process worker slots for the user service."),
    ] = None,
    control_query_concurrency: Annotated[
        int | None,
        typer.Option(help="Slots reserved within total capacity for live control queries."),
    ] = None,
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    clear_kind_concurrency: Annotated[
        bool,
        typer.Option(help="Clear every persisted per-kind override in the rendered unit."),
    ] = False,
) -> None:
    """Render a sudo-less systemd user service for a worker endpoint."""
    definition = _require_cluster(cluster)
    capacity = _resolved_worker_capacity_policy(
        definition,
        concurrency=concurrency,
        control_query_concurrency=control_query_concurrency,
        kind_concurrency=kind_concurrency,
        clear_kind_concurrency=clear_kind_concurrency,
    )
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=capacity.concurrency,
        control_query_concurrency=capacity.control_query_concurrency,
        kind_concurrency=capacity.kind_concurrency,
    )
    if output is None:
        typer.echo(service_text)
        return
    typer.echo(write_endpoint_user_service(output, service_text))


@endpoint_app.command("worker-info")
def endpoint_worker_info(
    cluster: Annotated[str, typer.Option(help="Configured worker cluster name.")],
    freshness_seconds: Annotated[
        float,
        typer.Option(help="Maximum acceptable durable worker heartbeat age."),
    ] = 120.0,
    readiness_only: Annotated[
        bool,
        typer.Option(help="Return bounded readiness flags without detailed installation records."),
    ] = False,
) -> None:
    """Report fresh process-bound identity for the active cluster worker."""
    _run_or_exit(
        lambda: typer.echo(
            json.dumps(
                worker_runtime_info(
                    cluster=cluster,
                    freshness_seconds=freshness_seconds,
                    readiness_only=readiness_only,
                ),
                indent=2,
            )
        )
    )


@endpoint_app.command("target-info", hidden=True)
def endpoint_target_info(
    scheduler_provider: Annotated[
        str,
        typer.Option(help="Configured scheduler provider to attest."),
    ] = "external",
) -> None:
    """Report physical host and scheduler identity from the cluster process context."""

    def action() -> None:
        provider = provider_for_scheduler(scheduler_provider)
        scheduler_cluster_name = provider.scheduler_cluster_name()
        typer.echo(
            json.dumps(
                {
                    "schema_version": "clio-relay.cluster-target-info.v1",
                    "hostname": socket.gethostname(),
                    "fqdn": socket.getfqdn(),
                    "site_marker_sha256": _physical_site_marker_sha256(Path("/etc/machine-id")),
                    "scheduler_provider": provider.name,
                    "scheduler_cluster_name": scheduler_cluster_name,
                },
                indent=2,
            )
        )

    _run_or_exit(action)


def _physical_site_marker_sha256(path: Path) -> str:
    """Hash the exact physical-site marker bytes used by operator pinning tools."""
    try:
        marker = path.read_bytes()
    except OSError as exc:
        raise ConfigurationError(f"could not read physical site marker: {exc}") from exc
    if not marker.strip():
        raise ConfigurationError("physical site marker is empty")
    return hashlib.sha256(marker).hexdigest()


@cluster_app.command("list")
def cluster_list() -> None:
    """List configured clusters."""
    registry = ClusterRegistry.load(default_registry_path())
    for name, definition in sorted(registry.clusters.items()):
        capacity = definition.worker_capacity
        typer.echo(
            f"{name} ssh={definition.ssh_host} profile={definition.bootstrap_profile} "
            f"worker_concurrency={capacity.concurrency} "
            f"control_query_concurrency={capacity.control_query_concurrency}"
        )


@cluster_app.command("add")
def cluster_add(
    name: Annotated[str, typer.Option(help="Cluster name used by relay jobs.")],
    ssh_host: Annotated[str, typer.Option(help="SSH host or alias for the cluster.")],
    bootstrap_profile: Annotated[
        str,
        typer.Option(help="Bootstrap profile for this cluster."),
    ] = "linux-user",
    core_dir: Annotated[
        str,
        typer.Option(help="Remote clio-core directory."),
    ] = "$HOME/.local/share/clio-relay/core",
    spool_dir: Annotated[
        str,
        typer.Option(help="Remote spool directory."),
    ] = "$HOME/.local/share/clio-relay/spool",
    jarvis_bin: Annotated[
        str | None,
        typer.Option(help="Remote JARVIS-CD executable path."),
    ] = None,
    jarvis_resource_graph_profile: Annotated[
        str | None,
        typer.Option(
            help=(
                "Exact JARVIS builtin resource-graph profile selected by the operator; "
                "relay never derives this from the cluster name."
            )
        ),
    ] = None,
    allow_jarvis_resource_graph_build: Annotated[
        bool,
        typer.Option(
            "--allow-jarvis-resource-graph-build/--no-allow-jarvis-resource-graph-build",
            help=(
                "Allow one benchmark-free first-install graph build only after JARVIS "
                "returns structured unavailable for the selected builtin profile."
            ),
        ),
    ] = False,
    spack_executable: Annotated[
        str | None,
        typer.Option(help="Absolute remote Spack executable used by the cluster-side JARVIS MCP."),
    ] = None,
    frpc_bin: Annotated[
        str | None,
        typer.Option(help="Remote frpc executable path."),
    ] = None,
    agent_bin: Annotated[
        str | None,
        typer.Option(help="Remote agent executable path."),
    ] = None,
    agent_adapter: Annotated[
        str,
        typer.Option(help="Remote agent adapter name."),
    ] = "exec",
    scheduler_provider: Annotated[
        str,
        typer.Option(
            help="Registered scheduler provider for relay-owned status/cancel operations."
        ),
    ] = "external",
    worker_concurrency: Annotated[
        int,
        typer.Option(help="Total slot capacity for the managed cluster worker service."),
    ] = 3,
    worker_control_query_concurrency: Annotated[
        int,
        typer.Option(help="Slots reserved within total worker capacity for live control queries."),
    ] = 1,
    worker_kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--worker-kind-concurrency",
            help="Per-kind managed-worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    agent_npm_package: Annotated[
        str | None,
        typer.Option(help="Optional npm package used to install the agent."),
    ] = None,
    agent_npm_bin: Annotated[
        str | None,
        typer.Option(help="Agent binary name provided by npm or PATH."),
    ] = None,
    frp_server_addr: Annotated[
        str,
        typer.Option(help="frps server address for this cluster transport."),
    ] = "",
    frp_server_port: Annotated[
        int,
        typer.Option(help="frps server port for this cluster transport."),
    ] = 443,
    frp_protocol: Annotated[
        str,
        typer.Option(help="frpc-to-frps transport protocol."),
    ] = "wss",
    frp_token_env: Annotated[
        str,
        typer.Option(help="Environment/local-secret key for the frp token."),
    ] = "CLIO_RELAY_FRP_TOKEN",
    stcp_secret_env: Annotated[
        str,
        typer.Option(help="Environment/local-secret key for the stcp secret."),
    ] = "CLIO_RELAY_STCP_SECRET",
    direct_transport: Annotated[
        bool,
        typer.Option(
            "--direct-transport/--no-direct-transport",
            help="Enable optional NAT-punching direct transport optimization.",
        ),
    ] = False,
    direct_transport_mode: Annotated[
        str,
        typer.Option(help="Direct transport mode. Currently only xtcp is supported."),
    ] = "xtcp",
    direct_transport_fallback: Annotated[
        str,
        typer.Option(help="Comma-separated direct transport fallback order ending in queue."),
    ] = "frp_stcp,queue",
    target_hostname: Annotated[
        list[str] | None,
        typer.Option(
            "--target-hostname",
            help="Expected remote hostname; repeat for accepted aliases.",
        ),
    ] = None,
    ssh_host_key_sha256: Annotated[
        list[str] | None,
        typer.Option(
            "--ssh-host-key-sha256",
            help="Expected SSH host-key SHA256 fingerprint; repeat for rotations.",
        ),
    ] = None,
    scheduler_cluster_name: Annotated[
        str | None,
        typer.Option(help="Expected scheduler-native cluster name, such as SLURM ClusterName."),
    ] = None,
    site_marker_sha256: Annotated[
        str | None,
        typer.Option(help="Expected SHA-256 of the remote /etc/machine-id site marker."),
    ] = None,
) -> None:
    """Add or update a local cluster definition."""
    if (target_hostname is None) != (ssh_host_key_sha256 is None):
        raise typer.BadParameter(
            "--target-hostname and --ssh-host-key-sha256 must be provided together"
        )
    try:
        definition = ClusterDefinition(
            name=name,
            ssh_host=ssh_host,
            bootstrap_profile=bootstrap_profile,
            core_dir=core_dir,
            spool_dir=spool_dir,
            jarvis_bin=jarvis_bin,
            jarvis_resource_graph_profile=_none_if_blank(jarvis_resource_graph_profile),
            allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
            spack_executable=_none_if_blank(spack_executable),
            frpc_bin=frpc_bin,
            agent_bin=_none_if_blank(agent_bin),
            agent_adapter=agent_adapter,
            scheduler_provider=scheduler_provider,
            worker_capacity=WorkerCapacityPolicy(
                concurrency=worker_concurrency,
                control_query_concurrency=worker_control_query_concurrency,
                kind_concurrency=_kind_concurrency_options(
                    worker_kind_concurrency,
                    param_hint="--worker-kind-concurrency",
                ),
            ),
            target_identity=(
                ClusterTargetIdentity(
                    hostnames=target_hostname,
                    ssh_host_key_sha256=ssh_host_key_sha256,
                    scheduler_cluster_name=_none_if_blank(scheduler_cluster_name),
                    site_marker_sha256=_none_if_blank(site_marker_sha256),
                )
                if target_hostname is not None and ssh_host_key_sha256 is not None
                else None
            ),
            agent_npm_package=_none_if_blank(agent_npm_package),
            agent_npm_bin=_none_if_blank(agent_npm_bin),
            frp_transport=FrpTransportConfig(
                protocol=frp_protocol,
                server_addr=frp_server_addr,
                server_port=frp_server_port,
                token_env=frp_token_env,
                stcp_secret_env=stcp_secret_env,
                direct=DirectTransportConfig(
                    enabled=direct_transport,
                    mode=direct_transport_mode,
                    fallback_order=_split_csv(direct_transport_fallback),
                ),
            ),
        )
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    ClusterRegistry.mutate(
        default_registry_path(),
        lambda registry: registry.clusters.__setitem__(name, definition),
    )
    typer.echo(f"{name} ssh={ssh_host} profile={bootstrap_profile}")


@cluster_app.command("pin-target")
def cluster_pin_target(
    cluster: Annotated[str, typer.Option(help="Existing configured cluster name.")],
    target_hostname: Annotated[
        list[str] | None,
        typer.Option(
            "--target-hostname",
            help="Expected remote hostname; repeat for accepted aliases.",
        ),
    ] = None,
    ssh_host_key_sha256: Annotated[
        list[str] | None,
        typer.Option(
            "--ssh-host-key-sha256",
            help="Expected SSH host-key SHA256 fingerprint; repeat for key rotations.",
        ),
    ] = None,
    scheduler_cluster_name: Annotated[
        str | None,
        typer.Option(help="Expected scheduler-native cluster name."),
    ] = None,
    site_marker_sha256: Annotated[
        str | None,
        typer.Option(help="Expected SHA-256 of the remote physical site marker."),
    ] = None,
    clear: Annotated[
        bool,
        typer.Option(help="Remove only the existing physical target identity pin."),
    ] = False,
) -> None:
    """Pin or clear one cluster's physical target identity without replacing its config."""
    identity_arguments_present = any(
        value is not None
        for value in (
            target_hostname,
            ssh_host_key_sha256,
            scheduler_cluster_name,
            site_marker_sha256,
        )
    )
    if clear and identity_arguments_present:
        raise typer.BadParameter("--clear cannot be combined with target identity values")
    if not clear and (target_hostname is None or ssh_host_key_sha256 is None):
        raise typer.BadParameter(
            "--target-hostname and --ssh-host-key-sha256 are required unless --clear is used"
        )
    target_identity: ClusterTargetIdentity | None = None
    if not clear:
        assert target_hostname is not None
        assert ssh_host_key_sha256 is not None
        try:
            target_identity = ClusterTargetIdentity(
                hostnames=target_hostname,
                ssh_host_key_sha256=ssh_host_key_sha256,
                scheduler_cluster_name=_none_if_blank(scheduler_cluster_name),
                site_marker_sha256=_none_if_blank(site_marker_sha256),
            )
        except ValidationError as exc:
            raise typer.BadParameter(str(exc)) from exc

    def update_target_identity(registry: ClusterRegistry) -> None:
        registry.require(cluster).target_identity = target_identity

    registry = ClusterRegistry.mutate(default_registry_path(), update_target_identity)
    definition = registry.require(cluster)
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "ssh_host": definition.ssh_host,
                "target_identity": (
                    definition.target_identity.model_dump(mode="json")
                    if definition.target_identity is not None
                    else None
                ),
            },
            indent=2,
        )
    )


@remote_mcp_app.command("register")
def remote_mcp_register(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Stable server registration name.")],
    command: Annotated[str, typer.Option(help="Remote stdio MCP executable.")],
    arg: Annotated[
        list[str] | None,
        typer.Option(help="Remote MCP command argument. Repeatable and passed without a shell."),
    ] = None,
    env_from: Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Child=SOURCE environment reference. Repeatable; values are resolved only "
                "by the endpoint worker."
            )
        ),
    ] = None,
    allow_tool: Annotated[
        list[str] | None,
        typer.Option(
            help="Exact remote tool name to virtualize. Repeatable; '*' explicitly allows all."
        ),
    ] = None,
    profile: Annotated[
        list[str] | None,
        typer.Option(help="Local MCP profile allowed to expose tools: user, admin, or operator."),
    ] = None,
    namespace: Annotated[
        str | None,
        typer.Option(help="Optional stable namespace used in generated local aliases."),
    ] = None,
    contract: Annotated[
        str | None,
        typer.Option(
            help=(
                "Optional audited semantic contract. Supported: clio-kit-spack-user-v2.1 "
                "(current), clio-kit-spack-user-v2 (compatibility), "
                "clio-kit-scientific-catalog-user-v1.1 (current), "
                "clio-kit-scientific-catalog-user-v1 (compatibility)."
            )
        ),
    ] = None,
    schema_cache_ttl_seconds: Annotated[
        int,
        typer.Option(help="Maximum age of a discovered schema before tools are hidden.", min=1),
    ] = 86_400,
    call_timeout_seconds: Annotated[
        int,
        typer.Option(
            help="Maximum duration of each virtual tools/call execution.",
            min=1,
            max=86_400,
        ),
    ] = 300,
    enabled: Annotated[
        bool,
        typer.Option("--enabled/--disabled", help="Enable this remote MCP registration."),
    ] = True,
    replace: Annotated[
        bool,
        typer.Option(help="Replace an existing registration with the same cluster and name."),
    ] = False,
) -> None:
    """Register an allowlisted remote MCP server for one cluster."""
    registry_path = default_registry_path()
    try:
        registration = RemoteMcpServerConfig(
            command=command,
            args=arg or [],
            env_from=_environment_references(env_from),
            namespace=namespace,
            contract=cast(RemoteMcpContract | None, contract),
            allow_tools=allow_tool or [],
            profiles=cast(list[RemoteMcpProfile], profile or ["admin"]),
            schema_cache_ttl_seconds=schema_cache_ttl_seconds,
            call_timeout_seconds=call_timeout_seconds,
            enabled=enabled,
        )
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc

    def update_registry(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if name in definition.remote_mcp_servers and not replace:
            raise typer.BadParameter(
                f"remote MCP server is already registered for {cluster}: {name}; use --replace"
            )
        definition.remote_mcp_servers[name] = registration

    ClusterRegistry.mutate(registry_path, update_registry)
    cache = RemoteMcpSchemaCache.load(default_remote_mcp_cache_path(registry_path=registry_path))
    cached = cache.entry_for(cluster, name)
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "server_name": name,
                "registration": registration.model_dump(mode="json"),
                "execution_fingerprint": remote_mcp_execution_fingerprint(registration),
                "cache_reusable": (
                    cached is not None
                    and cached.execution_fingerprint
                    == remote_mcp_execution_fingerprint(registration)
                ),
                "reload_semantics": (
                    "configuration is read on the next local MCP tools/list; run remote-mcp "
                    "refresh before exposure when the cache is missing, stale, or command-changed"
                ),
            },
            indent=2,
        )
    )


@remote_mcp_app.command("unregister")
def remote_mcp_unregister(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
) -> None:
    """Remove a remote MCP registration and its local schema cache entry."""
    registry_path = default_registry_path()

    def update_registry(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if name not in definition.remote_mcp_servers:
            raise typer.BadParameter(f"remote MCP server is not registered for {cluster}: {name}")
        del definition.remote_mcp_servers[name]

    ClusterRegistry.mutate(registry_path, update_registry)
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
    RemoteMcpSchemaCache.remove_entry(cache_path, cluster, name)
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "server_name": name,
                "registered": False,
                "cache_removed": True,
            },
            indent=2,
        )
    )


@remote_mcp_app.command("list")
def remote_mcp_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional configured cluster filter."),
    ] = None,
) -> None:
    """List registrations and cache freshness/provenance as JSON."""
    registry_path = default_registry_path()
    registry = ClusterRegistry.load(registry_path)
    if cluster is not None:
        registry.require(cluster)
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
    cache = RemoteMcpSchemaCache.load(cache_path)
    registrations: list[dict[str, object]] = []
    for cluster_name, definition in sorted(registry.clusters.items()):
        if cluster is not None and cluster_name != cluster:
            continue
        for server_name, registration in sorted(definition.remote_mcp_servers.items()):
            entry = cache.entry_for(cluster_name, server_name)
            registrations.append(
                {
                    "cluster": cluster_name,
                    "server_name": server_name,
                    "registration": registration.model_dump(mode="json"),
                    "cache": _remote_mcp_cache_status(registration, entry),
                }
            )
    typer.echo(
        json.dumps(
            {
                "registry_path": str(registry_path),
                "cache_path": str(cache_path),
                "registrations": registrations,
            },
            indent=2,
        )
    )


@remote_mcp_app.command("reload")
def remote_mcp_reload(
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile to render: user, admin, operator, or all."),
    ] = "user",
) -> None:
    """Reload local config/cache and report the exact next tools/list catalog."""
    if profile not in {"user", "admin", "operator", "all"}:
        raise typer.BadParameter("--profile must be user, admin, operator, or all")
    catalog = load_registered_remote_mcp_catalog(profile)
    typer.echo(
        json.dumps(
            {
                "profile": profile,
                "catalog_revision": catalog.revision,
                "tools": catalog.tool_definitions(),
                "issues": [issue.model_dump(mode="json") for issue in catalog.issues],
                "remote_discovery_performed": False,
                "mcp_server_restart_required": False,
                "client_action": "request tools/list again to observe this catalog revision",
            },
            indent=2,
        )
    )


@remote_mcp_app.command("refresh")
def remote_mcp_refresh(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote MCP protocol session.", min=1),
    ] = 120,
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time to wait for the durable discovery job.", min=1),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable discovery job polling interval.", min=0.05),
    ] = 2,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Optional discovery submission idempotency key."),
    ] = None,
) -> None:
    """Discover a registered server through a durable MCP tools/list relay job."""
    registry_path = default_registry_path()
    registry = ClusterRegistry.load(registry_path)
    definition = registry.require(cluster)
    try:
        registration = definition.remote_mcp_servers[name]
    except KeyError as exc:
        raise typer.BadParameter(
            f"remote MCP server is not registered for {cluster}: {name}"
        ) from exc
    if not registration.enabled:
        raise typer.BadParameter(f"remote MCP server is disabled for {cluster}: {name}")
    key = idempotency_key or f"remote-mcp-discovery:{cluster}:{name}:{uuid4().hex}"

    def action() -> None:
        if should_execute_on_cluster(definition):
            remote_args = [
                "mcp-call",
                "--cluster",
                cluster,
                "--server",
                registration.command,
                "--operation",
                McpOperation.TOOLS_LIST.value,
                "--idempotency-key",
                key,
            ]
            if timeout_seconds is not None:
                remote_args.extend(["--timeout-seconds", str(timeout_seconds)])
            for item in registration.args:
                remote_args.extend(["--server-arg", item])
            for child_name, source_name in sorted(registration.env_from.items()):
                remote_args.extend(["--env-from", f"{child_name}={source_name}"])
            job_id = _last_nonempty_line(run_remote_clio(definition, remote_args))
            wait_result = _json_output(
                run_remote_clio(
                    definition,
                    [
                        "job",
                        "wait",
                        job_id,
                        "--timeout-seconds",
                        str(wait_timeout_seconds),
                        "--poll-seconds",
                        str(poll_seconds),
                    ],
                ),
                "remote discovery wait",
            )
            _require_discovery_success(wait_result, job_id)
            artifact, artifact_payload = _read_remote_mcp_result_artifact(
                definition,
                job_id,
            )
        else:
            queue = _managed_queue_from_env()
            admission_class, admission_authority = resolve_registered_remote_mcp_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                server=registration.command,
                server_args=registration.args,
                env_from=registration.env_from,
                operation=McpOperation.TOOLS_LIST,
                tool=None,
                expected_server_artifact_digest=None,
                evidence=None,
                timeout_seconds=timeout_seconds,
            )
            metadata = (
                {}
                if admission_authority is None
                else {
                    MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(
                        mode="json"
                    )
                }
            )
            job = queue.submit_job(
                RelayJob(
                    cluster=cluster,
                    kind=JobKind.MCP_CALL,
                    spec=McpCallSpec(
                        server=registration.command,
                        server_args=registration.args,
                        env_from=registration.env_from,
                        admission_class=admission_class,
                        operation=McpOperation.TOOLS_LIST,
                        timeout_seconds=timeout_seconds,
                    ),
                    idempotency_key=key,
                    metadata=metadata,
                )
            )
            terminal = wait_for_terminal(
                queue,
                job.job_id,
                timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            _require_discovery_success(terminal.model_dump(mode="json"), job.job_id)
            artifact, artifact_payload = _read_local_mcp_result_artifact(queue, job.job_id)
            job_id = job.job_id
        entry = cache_entry_from_discovery_artifact(
            cluster=cluster,
            server_name=name,
            registration=registration,
            discovery_job_id=job_id,
            artifact_id=str(artifact["artifact_id"]),
            artifact_sha256=cast(str | None, artifact.get("sha256")),
            artifact_payload=artifact_payload,
        )
        cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
        RemoteMcpSchemaCache.update_entry(cache_path, entry)
        catalogs = {
            profile_name: load_registered_remote_mcp_catalog(profile_name)
            for profile_name in registration.profiles
        }
        typer.echo(
            json.dumps(
                {
                    "cluster": cluster,
                    "server_name": name,
                    "discovery_job_id": job_id,
                    "cache_path": str(cache_path),
                    "cache_entry": entry.model_dump(mode="json"),
                    "profiles": {
                        profile_name: {
                            "catalog_revision": catalog.revision,
                            "virtual_tools": sorted(catalog.tools),
                            "registration_virtual_tools": sorted(
                                alias
                                for alias, tool in catalog.tools.items()
                                if (route := tool.routes.get(cluster)) is not None
                                and route.server_name == name
                            ),
                        }
                        for profile_name, catalog in catalogs.items()
                    },
                    "mcp_server_restart_required": False,
                    "client_action": "request tools/list again to load the refreshed schemas",
                },
                indent=2,
                default=str,
            )
        )

    _run_or_exit(action)


@dataclass(frozen=True)
class _RemoteMcpValidationRoute:
    """One preflight-resolved virtual alias and its argument wrapping mode."""

    alias: str
    arguments_wrapped: bool


@dataclass(frozen=True)
class _RemoteMcpValidationPreflight:
    """Inputs and immutable routes resolved before any validation dispatch."""

    registry_path: Path
    registry: ClusterRegistry
    definition: ClusterDefinition
    remote_arguments: dict[str, Any]
    routes: dict[str, _RemoteMcpValidationRoute]
    result_expectation: RemoteMcpStructuredResultExpectation | None

    @property
    def fresh_spack_transition(self) -> bool:
        """Return whether this run requests disposable-store install proof."""
        return (
            self.result_expectation is not None
            and self.result_expectation.fresh_install_store_root is not None
        )


@dataclass(frozen=True)
class _RemoteMcpValidationCall:
    """One completed ordinary remote-MCP acceptance call and its protocol result."""

    report: RemoteMcpAcceptanceReport
    protocol_result: dict[str, Any] | None
    stdio_session: PackagedMcpStdioSession


def _resolve_remote_mcp_validation_route(
    *,
    catalog: VirtualRemoteMcpCatalog,
    cluster: str,
    server_name: str,
    remote_tool_name: str,
) -> _RemoteMcpValidationRoute:
    """Resolve exactly one fresh virtual alias before any MCP call is dispatched."""
    aliases = [
        alias
        for alias, virtual in catalog.tools.items()
        if virtual.remote_tool.name == remote_tool_name
        and cluster in virtual.routes
        and virtual.routes[cluster].server_name == server_name
    ]
    if len(aliases) != 1:
        raise typer.BadParameter(
            f"expected one fresh virtual alias for {cluster}/{server_name}/{remote_tool_name}, "
            f"found {len(aliases)}; run remote-mcp refresh and reload"
        )
    virtual = catalog.tools[aliases[0]]
    return _RemoteMcpValidationRoute(
        alias=aliases[0],
        arguments_wrapped=virtual.arguments_wrapped,
    )


@remote_mcp_app.command("validate")
@_acceptance_report_command
def remote_mcp_validate(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
    tool: Annotated[str, typer.Option(help="Allowlisted remote MCP tool name to call.")],
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the remote tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the remote tool."),
    ] = None,
    result_expectation_json: Annotated[
        str,
        typer.Option(
            help=("Optional JSON object describing semantic expectations for structuredContent.")
        ),
    ] = "{}",
    result_expectation_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a structured-result expectation JSON object."),
    ] = None,
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile used for tools/list and the virtual call."),
    ] = "user",
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time to wait for the durable virtual call.", min=1),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable call polling interval.", min=0.05),
    ] = 2,
    output_json: Annotated[
        Path | None,
        typer.Option(help="Optional path for the machine-readable acceptance report."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical release-evidence JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in canonical evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Call one virtual tool and emit canonical durable acceptance evidence."""
    canonical_report_path = validation_report or default_report_path(cluster)
    canonical_written = [False]

    def preflight() -> _RemoteMcpValidationPreflight:
        if profile not in {"user", "admin", "operator", "all"}:
            raise typer.BadParameter("--profile must be user, admin, operator, or all")
        arguments_source = _json_text_from_option(arguments_json, arguments_json_file)
        remote_arguments = _json_object(arguments_source)
        result_expectation: RemoteMcpStructuredResultExpectation | None = None
        if result_expectation_json_file is not None or result_expectation_json != "{}":
            expectation_source = _json_text_from_option(
                result_expectation_json,
                result_expectation_json_file,
            )
            try:
                result_expectation = RemoteMcpStructuredResultExpectation.model_validate(
                    _json_object(expectation_source)
                )
            except ValidationError as exc:
                raise typer.BadParameter(
                    f"structured-result expectation is invalid: {exc.errors()[0]['msg']}"
                ) from exc
        registry_path = default_registry_path()
        registry = ClusterRegistry.load(registry_path)
        definition = registry.require(cluster)
        if name not in definition.remote_mcp_servers:
            raise typer.BadParameter(f"remote MCP server is not registered for {cluster}: {name}")
        registration = definition.remote_mcp_servers[name]
        if result_expectation is not None:
            if result_expectation.tool != tool:
                raise typer.BadParameter("structured-result expectation tool must match --tool")
            if registration.contract != result_expectation.contract:
                raise typer.BadParameter(
                    "structured-result expectation contract must match the registered contract"
                )
        catalog = load_registered_remote_mcp_catalog(profile)
        fresh_transition = (
            result_expectation is not None
            and result_expectation.fresh_install_store_root is not None
        )
        if fresh_transition:
            if result_expectation is None:
                raise typer.BadParameter("fresh Spack expectation is unavailable")
            if (
                remote_arguments.get("spec") != result_expectation.requested_spec
                or remote_arguments.get("reuse") is not False
            ):
                raise typer.BadParameter(
                    "fresh Spack validation arguments must submit the expected spec "
                    "with reuse=false"
                )
        required_tools = (
            ("spack_find", "spack_install", "spack_locate") if fresh_transition else (tool,)
        )
        routes = {
            remote_tool_name: _resolve_remote_mcp_validation_route(
                catalog=catalog,
                cluster=cluster,
                server_name=name,
                remote_tool_name=remote_tool_name,
            )
            for remote_tool_name in required_tools
        }
        requested_route = routes[tool]
        if not requested_route.arguments_wrapped and "cluster" in remote_arguments:
            raise typer.BadParameter(
                "flat remote tool arguments must not contain reserved key 'cluster'"
            )
        return _RemoteMcpValidationPreflight(
            registry_path=registry_path,
            registry=registry,
            definition=definition,
            remote_arguments=remote_arguments,
            routes=routes,
            result_expectation=result_expectation,
        )

    try:
        prepared = preflight()
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="remote-mcp",
            cluster=cluster,
            check_id="remote-mcp.preflight",
            summary="validate virtual remote MCP acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        settings = RelaySettings.from_env()
        queue = storage_managed_queue(settings)
        queue.initialize()
        execute_remotely = should_execute_on_cluster(prepared.definition)
        remote_install_info = _remote_worker_info(prepared.definition) if execute_remotely else None
        cache = RemoteMcpSchemaCache.load(
            default_remote_mcp_cache_path(registry_path=prepared.registry_path)
        )
        reserved_names = static_mcp_tool_names()
        if prepared.fresh_spack_transition:
            expectation = prepared.result_expectation
            if expectation is None or expectation.requested_spec is None:
                raise RelayError("fresh Spack transition expectation became unavailable")
            preinstall_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_find",
                route=prepared.routes["spack_find"],
                remote_arguments={"query": expectation.requested_spec},
                result_expectation=None,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            _require_passing_remote_mcp_call(preinstall_call, phase="preinstall find")
            _require_spack_preinstall_absent(
                preinstall_call.protocol_result,
                requested_spec=expectation.requested_spec,
            )
            preinstall_configuration = _collect_spack_configuration_observation(
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                expectation=expectation,
                phase="preinstall",
            )
            install_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_install",
                route=prepared.routes["spack_install"],
                remote_arguments=prepared.remote_arguments,
                result_expectation=expectation,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            _require_passing_remote_mcp_call(install_call, phase="fresh install")
            postinstall_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name="spack_locate",
                route=prepared.routes["spack_locate"],
                remote_arguments={"spec": f"/{expectation.dag_hash}"},
                result_expectation=None,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            postinstall_configuration = _collect_spack_configuration_observation(
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                expectation=expectation,
                phase="postinstall",
            )
            report = build_remote_mcp_spack_fresh_install_transition_report(
                preinstall_report=preinstall_call.report,
                install_report=install_call.report,
                postinstall_report=postinstall_call.report,
                preinstall_protocol_result=preinstall_call.protocol_result,
                install_protocol_result=install_call.protocol_result,
                postinstall_protocol_result=postinstall_call.protocol_result,
                install_expectation=expectation,
                preinstall_configuration=preinstall_configuration,
                postinstall_configuration=postinstall_configuration,
            )
        else:
            requested_call = _execute_remote_mcp_validation_call(
                queue=queue,
                definition=prepared.definition,
                execute_remotely=execute_remotely,
                registry=prepared.registry,
                cache=cache,
                cluster=cluster,
                server_name=name,
                profile=profile,
                remote_tool_name=tool,
                route=prepared.routes[tool],
                remote_arguments=prepared.remote_arguments,
                result_expectation=prepared.result_expectation,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
                reserved_names=reserved_names,
            )
            report = requested_call.report
        canonical_report = report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        if remote_install_info is not None:
            attach_verified_worker_identity(canonical_report, remote_install_info)
        write_validation_report(canonical_report, canonical_report_path)
        canonical_written[0] = True
        rendered = report.model_dump_json(indent=2)
        if output_json is not None:
            output_json.parent.mkdir(parents=True, exist_ok=True)
            output_json.write_text(rendered + "\n", encoding="utf-8")
        typer.echo(rendered)
        if not report.passed:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            if not canonical_written[0]:
                failed_report = new_live_validation_report(
                    scenario="remote-mcp",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "remote-mcp.completed", "complete virtual remote MCP acceptance", exc
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    _run_or_exit(guarded_action)


def _execute_remote_mcp_validation_call(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    execute_remotely: bool,
    registry: ClusterRegistry,
    cache: RemoteMcpSchemaCache,
    cluster: str,
    server_name: str,
    profile: str,
    remote_tool_name: str,
    route: _RemoteMcpValidationRoute,
    remote_arguments: dict[str, Any],
    result_expectation: RemoteMcpStructuredResultExpectation | None,
    wait_timeout_seconds: float,
    poll_seconds: float,
    reserved_names: set[str],
) -> _RemoteMcpValidationCall:
    """Run one virtual alias and build its ordinary durable acceptance report."""
    stdio_session = run_packaged_mcp_stdio_session(
        profile=profile,
        tool=route.alias,
        arguments=(
            {"cluster": cluster, "arguments": remote_arguments}
            if route.arguments_wrapped
            else {"cluster": cluster, **remote_arguments}
        ),
    )
    job_id = _mcp_response_job_id(stdio_session.tools_call_response)
    if execute_remotely:
        run_remote_clio(
            definition,
            [
                "job",
                "wait",
                job_id,
                "--timeout-seconds",
                str(wait_timeout_seconds),
                "--poll-seconds",
                str(poll_seconds),
            ],
        )
        call_status = _json_output(
            run_remote_clio(definition, ["job", "status", job_id]),
            "remote MCP validation job status",
        )
        artifacts = _remote_artifact_records(definition, job_id)
        mcp_result = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
        )
        provenance = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
        )
    else:
        wait_for_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        call_status = get_job_status(queue, job_id)
        artifacts = _complete_local_artifact_records(queue, job_id)
        mcp_result = _read_local_json_artifact_kind(
            queue,
            artifacts,
            kind="mcp_result",
        )
        provenance = _read_local_json_artifact_kind(
            queue,
            artifacts,
            kind="provenance",
        )
    protocol_result = (
        cast(dict[str, Any], mcp_result["protocol_result"])
        if mcp_result is not None and isinstance(mcp_result.get("protocol_result"), dict)
        else None
    )
    report = build_remote_mcp_acceptance_report(
        registry=registry,
        cache=cache,
        cluster=cluster,
        server_name=server_name,
        remote_tool_name=remote_tool_name,
        profile=profile,
        call_job_id=job_id,
        call_status=call_status,
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
        result_expectation=result_expectation,
        reserved_names=reserved_names,
        mcp_stdio_evidence=stdio_session.evidence(),
    )
    return _RemoteMcpValidationCall(
        report=report,
        protocol_result=protocol_result,
        stdio_session=stdio_session,
    )


def _require_passing_remote_mcp_call(
    call: _RemoteMcpValidationCall,
    *,
    phase: str,
) -> None:
    """Stop a transition before its next mutation when an earlier call failed."""
    if not call.report.passed:
        failed = [check.name for check in call.report.checks if not check.passed]
        raise RelayError(f"{phase} acceptance failed before next dispatch: {failed}")


def _require_spack_preinstall_absent(
    protocol_result: dict[str, Any] | None,
    *,
    requested_spec: str,
) -> None:
    """Require exact structured absence before dispatching the mutating install call."""
    structured = (
        cast(dict[str, Any], protocol_result.get("structuredContent"))
        if protocol_result is not None
        and isinstance(protocol_result.get("structuredContent"), dict)
        else None
    )
    if (
        protocol_result is None
        or protocol_result.get("isError") is True
        or structured is None
        or structured.get("schema_version") != "spack.mcp.result.v1"
        or structured.get("operation") != "find"
        or structured.get("query") != requested_spec
        or structured.get("count") != 0
        or isinstance(structured.get("count"), bool)
        or structured.get("packages") != []
    ):
        raise RelayError(
            "fresh Spack preinstall call did not prove count=0 and packages=[] "
            "for the exact requested spec"
        )


def _collect_spack_configuration_observation(
    *,
    definition: ClusterDefinition,
    execute_remotely: bool,
    expectation: RemoteMcpStructuredResultExpectation,
    phase: Literal["preinstall", "postinstall"],
) -> RemoteMcpSpackConfigurationObservation:
    """Collect one real, bounded wrapper/configuration manifest observation."""
    manifest_path = expectation.fresh_install_configuration_manifest_path
    expected_sha256 = expectation.fresh_install_configuration_sha256
    if manifest_path is None or expected_sha256 is None:
        raise RelayError("fresh Spack configuration expectation is incomplete")
    if execute_remotely:
        observation = _collect_remote_spack_configuration_observation(
            definition=definition,
            phase=phase,
            manifest_path=manifest_path,
            expected_sha256=expected_sha256,
        )
    else:
        observation = _collect_local_spack_configuration_observation(
            phase=phase,
            manifest_path=manifest_path,
            expected_sha256=expected_sha256,
        )
    if (
        observation.phase != phase
        or observation.manifest_path != manifest_path
        or observation.manifest_sha256 != expected_sha256
    ):
        raise RelayError("fresh Spack configuration observation does not match expectation")
    return observation


def _collect_remote_spack_configuration_observation(
    *,
    definition: ClusterDefinition,
    phase: Literal["preinstall", "postinstall"],
    manifest_path: str,
    expected_sha256: str,
) -> RemoteMcpSpackConfigurationObservation:
    """Collect a configuration observation through one bounded Bash/SSH command."""
    script = _remote_spack_configuration_observer_script()
    command = " ".join(
        (
            "python3",
            "-c",
            shlex.quote(script),
            shlex.quote(phase),
            shlex.quote(manifest_path),
            shlex.quote(expected_sha256),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS),
            str(MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES),
            str(MAX_SPACK_CONFIGURATION_TREE_ENTRIES),
        )
    )
    with remote_command_timeout(SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS):
        output = run_remote_shell(definition, command)
    if len(output.encode("utf-8")) > MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES:
        raise RelayError("remote Spack configuration observation output exceeded its bound")
    payload = _json_output(output, f"{phase} Spack configuration observation")
    try:
        return RemoteMcpSpackConfigurationObservation.model_validate(payload)
    except ValidationError as exc:
        raise RelayError(
            f"remote Spack configuration observation is invalid: {exc.errors()[0]['msg']}"
        ) from exc


def _collect_local_spack_configuration_observation(
    *,
    phase: Literal["preinstall", "postinstall"],
    manifest_path: str,
    expected_sha256: str,
) -> RemoteMcpSpackConfigurationObservation:
    """Collect the same evidence locally using POSIX no-follow file operations."""
    if os.name == "nt":
        raise RelayError(
            "local fresh Spack configuration observation requires a POSIX host; "
            "use the configured SSH target from Windows"
        )
    manifest = Path(manifest_path)
    base = manifest.parent
    _require_regular_nonsymlink_directory(base, label="configuration manifest directory")
    manifest_bytes, manifest_size = _read_bounded_regular_nonsymlink_file(
        manifest,
        maximum_bytes=MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
        label="configuration manifest",
        require_nonempty=True,
    )
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha256 != expected_sha256:
        raise RelayError("configuration manifest SHA-256 does not match the expectation")
    declarations = _parse_spack_configuration_manifest(manifest_bytes)
    _require_exact_spack_configuration_component_set(base, declarations)
    components: list[dict[str, object]] = []
    for declared_sha256, relative_path in declarations:
        component_path = _safe_spack_configuration_component_path(base, relative_path)
        component_bytes, component_size = _read_bounded_regular_nonsymlink_file(
            component_path,
            maximum_bytes=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
            label=f"configuration component {relative_path}",
            require_nonempty=False,
        )
        observed_sha256 = hashlib.sha256(component_bytes).hexdigest()
        if observed_sha256 != declared_sha256:
            raise RelayError(f"configuration component SHA-256 changed: {relative_path}")
        components.append(
            {
                "relative_path": relative_path,
                "sha256": observed_sha256,
                "size_bytes": component_size,
                "regular_file": True,
            }
        )
    return RemoteMcpSpackConfigurationObservation.model_validate(
        {
            "phase": phase,
            "manifest_path": manifest_path,
            "manifest_sha256": manifest_sha256,
            "manifest_size_bytes": manifest_size,
            "manifest_regular_file": True,
            "components": components,
        }
    )


_SPACK_MANIFEST_LINE = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)$")


def _parse_spack_configuration_manifest(payload: bytes) -> list[tuple[str, str]]:
    """Parse one strict, sorted GNU sha256sum manifest within fixed limits."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RelayError("configuration manifest must be UTF-8") from exc
    if not text.endswith("\n") or "\x00" in text:
        raise RelayError("configuration manifest must be newline-terminated text")
    declarations: list[tuple[str, str]] = []
    for line in text.splitlines():
        match = _SPACK_MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise RelayError("configuration manifest contains an invalid sha256sum line")
        relative_path = match.group(2)
        if not _is_canonical_spack_component_relative_path(relative_path):
            raise RelayError("configuration manifest contains an unsafe component path")
        declarations.append((match.group(1), relative_path))
    paths = [relative_path for _digest, relative_path in declarations]
    if not 1 <= len(paths) <= MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS:
        raise RelayError("configuration manifest component count is outside its bound")
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise RelayError("configuration manifest component paths must be unique and sorted")
    return declarations


def _is_canonical_spack_component_relative_path(value: str) -> bool:
    """Return whether a manifest component is canonical and safely relative."""
    if (
        not value
        or len(value) > 1_024
        or value.startswith("/")
        or value == "."
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _safe_spack_configuration_component_path(base: Path, relative_path: str) -> Path:
    """Resolve a validated component while rejecting symlinked in-root parents."""
    if not _is_canonical_spack_component_relative_path(relative_path):
        raise RelayError("configuration component path is unsafe")
    current = base
    parts = PurePosixPath(relative_path).parts
    for part in parts[:-1]:
        current /= part
        _require_regular_nonsymlink_directory(
            current,
            label=f"configuration component parent {relative_path}",
        )
    return base.joinpath(*parts)


def _require_exact_spack_configuration_component_set(
    base: Path,
    declarations: list[tuple[str, str]],
) -> None:
    """Reject unmanifested files or symlinks in every covered configuration tree."""
    declared_paths = {relative_path for _digest, relative_path in declarations}
    covered_directories = sorted(
        {PurePosixPath(path).parts[0] for path in declared_paths if "/" in path}
    )
    observed_paths: set[str] = set()
    observed_entries = 0
    for relative_directory in covered_directories:
        directory = base / relative_directory
        _require_regular_nonsymlink_directory(
            directory,
            label=f"configuration tree {relative_directory}",
        )
        observed_entries += 1
        if observed_entries > MAX_SPACK_CONFIGURATION_TREE_ENTRIES:
            raise RelayError("configuration tree entry count exceeded its bound")
        pending = [directory]
        while pending:
            current = pending.pop()
            try:
                entries = os.scandir(current)
            except OSError as exc:
                raise RelayError(f"configuration tree is unavailable: {current}") from exc
            with entries:
                for entry in entries:
                    observed_entries += 1
                    if observed_entries > MAX_SPACK_CONFIGURATION_TREE_ENTRIES:
                        raise RelayError("configuration tree entry count exceeded its bound")
                    candidate = Path(entry.path)
                    relative_path = candidate.relative_to(base).as_posix()
                    if not _is_canonical_spack_component_relative_path(relative_path):
                        raise RelayError(
                            f"configuration tree entry has an unsafe path: {candidate}"
                        )
                    try:
                        metadata = candidate.lstat()
                    except OSError as exc:
                        raise RelayError(
                            f"configuration tree entry is unavailable: {candidate}"
                        ) from exc
                    if stat.S_ISLNK(metadata.st_mode):
                        raise RelayError(
                            f"configuration tree entry must not be a symbolic link: {candidate}"
                        )
                    if stat.S_ISDIR(metadata.st_mode):
                        pending.append(candidate)
                        continue
                    if not stat.S_ISREG(metadata.st_mode):
                        raise RelayError(
                            f"configuration tree entry must be a regular file: {candidate}"
                        )
                    observed_paths.add(relative_path)
    expected_covered_paths = {
        path for path in declared_paths if PurePosixPath(path).parts[0] in covered_directories
    }
    if observed_paths != expected_covered_paths:
        raise RelayError(
            "configuration tree files do not exactly match the bounded manifest: "
            f"missing={sorted(expected_covered_paths - observed_paths)} "
            f"unexpected={sorted(observed_paths - expected_covered_paths)}"
        )


def _require_regular_nonsymlink_directory(path: Path, *, label: str) -> None:
    """Require one existing directory without following its final path entry."""
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RelayError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RelayError(f"{label} must be a non-symlink directory: {path}")


def _read_bounded_regular_nonsymlink_file(
    path: Path,
    *,
    maximum_bytes: int,
    label: str,
    require_nonempty: bool,
) -> tuple[bytes, int]:
    """Read one stable regular file through a no-follow descriptor within a byte cap."""
    nofollow = cast(int | None, getattr(os, "O_NOFOLLOW", None))
    if nofollow is None:
        raise RelayError(f"{label} cannot be verified without O_NOFOLLOW support")
    flags = os.O_RDONLY | nofollow | cast(int, getattr(os, "O_CLOEXEC", 0))
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RelayError(f"{label} is unavailable or is a symbolic link: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RelayError(f"{label} must be a regular file: {path}")
        if before.st_size > maximum_bytes or (require_nonempty and before.st_size < 1):
            raise RelayError(f"{label} size is outside its bound: {path}")
        chunks: list[bytes] = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum_bytes:
                raise RelayError(f"{label} exceeded its byte bound while reading: {path}")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        stable_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) == (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if not stable_identity or observed != before.st_size:
            raise RelayError(f"{label} changed while it was observed: {path}")
        return b"".join(chunks), observed
    finally:
        os.close(descriptor)


def _remote_spack_configuration_observer_script() -> str:
    """Return the bounded POSIX observer executed by remote ``bash -lc``."""
    return r"""
import hashlib
import json
import os
import posixpath
import re
import stat
import sys

phase, manifest_path, expected_sha = sys.argv[1:4]
max_manifest, max_components, max_component, max_tree_entries = map(int, sys.argv[4:8])
line_pattern = re.compile(r"^([0-9a-f]{64})  ([^\r\n]+)$")

def safe_relative(value):
    return (
        bool(value)
        and len(value) <= 1024
        and not value.startswith("/")
        and value != "."
        and ".." not in value.split("/")
        and posixpath.normpath(value) == value
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )

def require_directory(path):
    metadata = os.lstat(path)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"not a non-symlink directory: {path}")

def read_regular(path, maximum, nonempty, retain_bytes=False):
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise RuntimeError("O_NOFOLLOW is unavailable")
    descriptor = os.open(path, os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0))
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError(f"not a regular file: {path}")
        if before.st_size > maximum or (nonempty and before.st_size < 1):
            raise RuntimeError(f"file size outside bound: {path}")
        digest = hashlib.sha256()
        chunks = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum:
                raise RuntimeError(f"file exceeded bound while reading: {path}")
            digest.update(chunk)
            if retain_bytes:
                chunks.append(chunk)
        after = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or observed != before.st_size
        ):
            raise RuntimeError(f"file changed while observed: {path}")
        return digest.hexdigest(), observed, b"".join(chunks)
    finally:
        os.close(descriptor)

base = posixpath.dirname(manifest_path)
require_directory(base)
manifest_sha, manifest_size, manifest_bytes = read_regular(
    manifest_path, max_manifest, True, retain_bytes=True
)
if manifest_sha != expected_sha:
    raise RuntimeError("configuration manifest SHA-256 mismatch")
text = manifest_bytes.decode("utf-8")
if not text.endswith("\n") or "\x00" in text:
    raise RuntimeError("configuration manifest is not newline-terminated text")
declarations = []
for line in text.splitlines():
    match = line_pattern.fullmatch(line)
    if match is None or not safe_relative(match.group(2)):
        raise RuntimeError("configuration manifest line is invalid")
    declarations.append((match.group(1), match.group(2)))
paths = [relative_path for _digest, relative_path in declarations]
if not 1 <= len(paths) <= max_components:
    raise RuntimeError("configuration component count is outside its bound")
if paths != sorted(paths) or len(paths) != len(set(paths)):
    raise RuntimeError("configuration component paths must be unique and sorted")
declared_paths = set(paths)
covered_directories = sorted({path.split("/", 1)[0] for path in paths if "/" in path})
observed_paths = set()
observed_entries = 0
for relative_directory in covered_directories:
    directory = posixpath.join(base, relative_directory)
    require_directory(directory)
    observed_entries += 1
    if observed_entries > max_tree_entries:
        raise RuntimeError("configuration tree entry count exceeded its bound")
    pending = [directory]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                observed_entries += 1
                if observed_entries > max_tree_entries:
                    raise RuntimeError("configuration tree entry count exceeded its bound")
                candidate = entry.path
                relative_path = posixpath.relpath(candidate, base)
                if not safe_relative(relative_path):
                    raise RuntimeError(f"configuration tree entry has an unsafe path: {candidate}")
                metadata = os.lstat(candidate)
                if stat.S_ISLNK(metadata.st_mode):
                    raise RuntimeError(
                        f"configuration tree entry must not be a symbolic link: {candidate}"
                    )
                if stat.S_ISDIR(metadata.st_mode):
                    pending.append(candidate)
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    raise RuntimeError(
                        f"configuration tree entry must be a regular file: {candidate}"
                    )
                observed_paths.add(relative_path)
expected_covered_paths = {
    path for path in declared_paths if path.split("/", 1)[0] in covered_directories
}
if observed_paths != expected_covered_paths:
    missing = sorted(expected_covered_paths - observed_paths)
    unexpected = sorted(observed_paths - expected_covered_paths)
    raise RuntimeError(
        "configuration tree files do not exactly match the bounded manifest: "
        f"missing={missing} unexpected={unexpected}"
    )
components = []
for declared_sha, relative_path in declarations:
    current = base
    parts = relative_path.split("/")
    for part in parts[:-1]:
        current = posixpath.join(current, part)
        require_directory(current)
    component_path = posixpath.join(base, *parts)
    observed_sha, observed_size, _unused = read_regular(component_path, max_component, False)
    if observed_sha != declared_sha:
        raise RuntimeError(f"configuration component SHA-256 mismatch: {relative_path}")
    components.append({
        "relative_path": relative_path,
        "sha256": observed_sha,
        "size_bytes": observed_size,
        "regular_file": True,
    })
print(json.dumps({
    "schema_version": "clio-relay.spack-configuration-observation.v1",
    "phase": phase,
    "manifest_path": manifest_path,
    "manifest_sha256": manifest_sha,
    "manifest_size_bytes": manifest_size,
    "manifest_regular_file": True,
    "components": components,
}, sort_keys=True, separators=(",", ":")))
""".strip()


@cluster_app.command("bootstrap")
@_acceptance_report_command
def cluster_bootstrap(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    relay_wheel: Annotated[
        Path | None,
        typer.Option(
            "--relay-wheel",
            help="Local clio-relay wheel to include in the bootstrap archive.",
        ),
    ] = None,
    relay_artifact_sha256: Annotated[
        str | None,
        typer.Option(
            help=(
                "Expected lowercase SHA-256 of the exact clio-relay wheel. Required "
                "for release bootstrap, with or without --relay-wheel, so repeated "
                "offline bootstrap has an artifact-distinct identity."
            ),
        ),
    ] = None,
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical cluster-bootstrap JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
) -> None:
    """Bootstrap a configured cluster's tools, relay package, and endpoint directories."""
    report_path = report or default_report_path(cluster)
    try:
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=report_path,
            scenario="cluster-bootstrap",
            cluster=cluster,
            check_id="cluster.bootstrap.preflight",
            summary="validate cluster bootstrap acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=relay_wheel,
        )
        raise

    def action() -> None:
        action_started = monotonic()
        expected_artifact_sha256 = relay_artifact_sha256
        if expected_artifact_sha256 is not None and (
            re.fullmatch(r"[0-9a-f]{64}", expected_artifact_sha256) is None
        ):
            raise ConfigurationError("relay artifact SHA-256 must be lowercase hex")
        if relay_wheel is not None and expected_artifact_sha256 is None:
            raise ConfigurationError(
                "--relay-wheel requires --relay-artifact-sha256 so preflight never reads "
                "payload bytes before deciding whether transfer is needed"
            )
        validation = new_live_validation_report(
            scenario="cluster-bootstrap",
            cluster=cluster,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=expected_artifact_sha256,
        )
        recorder = ValidationRecorder(validation)
        try:
            with recorder.check(
                "cluster.bootstrap",
                "execute the real cluster bootstrap and retrieve its durable receipt",
            ) as evidence:
                lines = bootstrap_cluster_over_ssh(
                    bootstrap_profile=definition.bootstrap_profile,
                    ssh_host=ssh_host or definition.ssh_host,
                    source_root=package_source_root(),
                    cluster=definition.name,
                    core_dir=definition.core_dir,
                    spool_dir=definition.spool_dir,
                    relay_wheel=relay_wheel,
                    relay_artifact_sha256=expected_artifact_sha256,
                    agent_adapter=definition.agent_adapter,
                    agent_npm_package=definition.agent_npm_package,
                    agent_npm_bin=definition.agent_npm_bin,
                    agent_args=definition.agent_args,
                    jarvis_resource_graph_profile=(definition.jarvis_resource_graph_profile),
                    allow_jarvis_resource_graph_build=(
                        definition.allow_jarvis_resource_graph_build
                    ),
                )
                receipt_lines = [
                    line for line in lines if line.startswith("bootstrap_receipt_json=")
                ]
                if len(receipt_lines) != 1:
                    raise RelayError(
                        "bootstrap did not return exactly one durable invocation receipt"
                    )
                receipt_references = [
                    line.partition("=")[2]
                    for line in lines
                    if line.startswith("bootstrap_receipt=")
                ]
                if len(receipt_references) != 1 or not receipt_references[0]:
                    raise RelayError(
                        "bootstrap did not return exactly one durable receipt reference"
                    )
                receipt = _json_output(
                    receipt_lines[0].partition("=")[2],
                    "bootstrap invocation receipt",
                )
                invocation_id = receipt.get("invocation_id")
                if not isinstance(invocation_id, str) or not invocation_id.startswith("bootstrap_"):
                    raise RelayError("bootstrap receipt omitted its unique invocation identity")
                evidence.append(
                    EvidenceReference(
                        kind="bootstrap_receipt",
                        reference=receipt_references[0],
                        metadata=receipt,
                    )
                )
                recorder.add_resource(
                    ValidationResource(
                        kind="bootstrap_invocation",
                        resource_id=invocation_id,
                        role="cluster_bootstrap",
                        cluster=cluster,
                        state="succeeded",
                        references=receipt_references,
                        metadata={
                            **receipt,
                            "ssh_host": ssh_host or definition.ssh_host,
                            "bootstrap_profile": definition.bootstrap_profile,
                            "output_sha256": hashlib.sha256("\n".join(lines).encode()).hexdigest(),
                        },
                    )
                )
            with recorder.check(
                "worker.target-identity",
                "verify the bootstrapped physical cluster against the operator pin",
            ) as target_evidence:
                target_definition = (
                    definition.model_copy(update={"ssh_host": ssh_host})
                    if ssh_host is not None
                    else definition
                )
                target_identity = _remote_target_identity(target_definition)
                target_evidence.append(
                    EvidenceReference(
                        kind="cluster_target",
                        reference=f"ssh-target:{target_definition.ssh_host}",
                        metadata=target_identity,
                    )
                )
            recorder.add_resource(
                ValidationResource(
                    kind="cluster_target",
                    resource_id=f"target:{cluster}",
                    role="physical_cluster_target",
                    cluster=cluster,
                    state="verified",
                    provider=definition.scheduler_provider,
                    metadata=target_identity,
                )
            )
            if receipt.get("outcome") in {"noop_verified", "repaired"}:
                with recorder.check(
                    "cluster.bootstrap.reuse-slo",
                    "enforce the bounded payload-free bootstrap reuse contract",
                ) as reuse_evidence:
                    reuse_acceptance = bootstrap_reuse_acceptance_evidence(
                        receipt,
                        elapsed_seconds=monotonic() - action_started,
                    )
                    if reuse_acceptance is None:
                        raise RelayError(
                            "bootstrap reuse receipt did not produce acceptance evidence"
                        )
                    reuse_evidence.append(
                        EvidenceReference(
                            kind="bootstrap_reuse_acceptance",
                            reference=f"bootstrap-reuse:{invocation_id}",
                            metadata=reuse_acceptance,
                        )
                    )
        except BaseException as exc:
            recorder.finish(exc)
            recorder.write(report_path)
            raise
        recorder.finish()
        recorder.write(report_path)
        lines.append(f"validation.report={report_path.resolve()}")
        _echo_lines(lines)

    _run_or_exit(action)


@cluster_app.command("install-app")
def cluster_install_app(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    app_name: Annotated[
        str,
        typer.Option("--app", help="Application runtime to install on the cluster."),
    ],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
) -> None:
    """Install an explicit application runtime on a configured cluster."""
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            install_cluster_app_over_ssh(
                ssh_host=ssh_host or definition.ssh_host,
                app_name=app_name,
            )
        )
    )


@cluster_app.command("install-endpoint-service")
def cluster_install_endpoint_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    start: Annotated[bool, typer.Option(help="Restart the service after installing.")] = True,
    enable: Annotated[bool, typer.Option(help="Enable the user service.")] = True,
    concurrency: Annotated[
        int | None,
        typer.Option(
            help="Override and persist total worker slots; defaults to the cluster policy."
        ),
    ] = None,
    control_query_concurrency: Annotated[
        int | None,
        typer.Option(
            help=(
                "Override and persist slots reserved within total capacity for live "
                "control queries."
            )
        ),
    ] = None,
    kind_concurrency: Annotated[
        list[str] | None,
        typer.Option(
            "--kind-concurrency",
            help="Per-kind worker limit as KIND=LIMIT; repeat for multiple kinds.",
        ),
    ] = None,
    clear_kind_concurrency: Annotated[
        bool,
        typer.Option(help="Clear and persist every per-kind worker capacity override."),
    ] = False,
    require_persistent: Annotated[
        bool,
        typer.Option(
            "--require-persistent/--allow-login-scoped",
            help=(
                "Require systemd user lingering so the enabled worker survives all logouts. "
                "The login-scoped opt-out is diagnostic and not release-gate eligible."
            ),
        ),
    ] = True,
) -> None:
    """Install a worker service from its persisted cluster capacity policy."""
    definition = _require_cluster(cluster)
    capacity = _resolved_worker_capacity_policy(
        definition,
        concurrency=concurrency,
        control_query_concurrency=control_query_concurrency,
        kind_concurrency=kind_concurrency,
        clear_kind_concurrency=clear_kind_concurrency,
    )
    if capacity != definition.worker_capacity:
        ClusterRegistry.mutate(
            default_registry_path(),
            lambda registry: registry.clusters.__setitem__(
                cluster,
                registry.require(cluster).model_copy(update={"worker_capacity": capacity}),
            ),
        )
        definition = definition.model_copy(update={"worker_capacity": capacity})
    service_text = render_endpoint_user_service(
        cluster=cluster,
        definition=definition,
        concurrency=capacity.concurrency,
        control_query_concurrency=capacity.control_query_concurrency,
        kind_concurrency=capacity.kind_concurrency,
    )
    _run_or_exit(
        lambda: _echo_lines(
            install_endpoint_user_service_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
                service_text=service_text,
                start=start,
                enable=enable,
                require_persistent=require_persistent,
            )
        )
    )


@cluster_app.command("restart-endpoint-service")
def cluster_restart_endpoint_service(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    require_persistent: Annotated[
        bool,
        typer.Option(
            "--require-persistent/--allow-login-scoped",
            help=(
                "Require systemd user lingering so the enabled worker survives all logouts. "
                "The login-scoped opt-out is diagnostic and not release-gate eligible."
            ),
        ),
    ] = True,
) -> None:
    """Verify persisted capacity, then restart without rewriting the installed unit."""
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: _echo_lines(
            restart_endpoint_user_service_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
                expected_capacity=definition.worker_capacity,
                require_persistent=require_persistent,
            )
        )
    )


@cluster_app.command("endpoint-service-status")
def cluster_endpoint_service_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this read-only inspection."),
    ] = None,
) -> None:
    """Return machine-readable endpoint persistence and recovery readiness."""
    definition = _require_cluster(cluster)

    def _status() -> None:
        evidence = endpoint_service_readiness_over_ssh(
            cluster=cluster,
            ssh_host=ssh_host or definition.ssh_host,
        )
        typer.echo(evidence.model_dump_json(indent=2))

    _run_or_exit(_status)


@session_app.command("plan-start")
def session_plan_start(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Plan replacement of an existing API."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Plan a token-protected remote API."),
    ] = True,
    start_operation_id: Annotated[
        str | None,
        typer.Option(help="Reuse an existing exact operation id; omitted mints one."),
    ] = None,
) -> None:
    """Emit a read-only exact plan that can survive loss of the start client."""
    definition = _require_cluster(cluster)

    def action() -> None:
        release_identity = _verify_session_start_worker_compatibility(definition)
        typer.echo(
            plan_remote_session_start(
                cluster=cluster,
                definition=definition,
                session_id=session_id,
                remote_api_port=remote_api_port,
                replace=replace,
                require_token=require_token,
                start_operation_id=start_operation_id,
                expected_api_release_identity_sha256=release_identity.sha256(),
            ).model_dump_json(indent=2)
        )

    _run_or_exit(action)


@session_app.command("start")
def session_start(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Replace an existing session API process."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Require CLIO_RELAY_API_TOKEN on the remote API."),
    ] = True,
    start_operation_id: Annotated[
        str | None,
        typer.Option(help="Exact id from session plan-start; omitted mints a fresh operation."),
    ] = None,
    expected_cluster_route_revision: Annotated[
        str | None,
        typer.Option(help="Fail before mutation if the planned cluster route changed."),
    ] = None,
    expected_api_release_identity_sha256: Annotated[
        str | None,
        typer.Option(help="Exact release digest from session plan-start."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json/--text", help="Emit the stable start-result JSON contract."),
    ] = False,
) -> None:
    """Start an owned API; exit 0 means ready and exit 2 means handle-only."""
    settings = RelaySettings.from_env()
    if require_token and settings.api_token is None:
        raise typer.BadParameter(
            "CLIO_RELAY_API_TOKEN is required unless --no-require-token is explicit"
        )
    if json_output and (
        start_operation_id is None
        or expected_cluster_route_revision is None
        or expected_api_release_identity_sha256 is None
    ):
        raise typer.BadParameter(
            "--json requires persisted operation, route, and release selectors from "
            "session plan-start"
        )
    definition = _require_cluster(cluster)

    def action() -> None:
        preliminary_plan = plan_remote_session_start(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            replace=replace,
            require_token=require_token,
            start_operation_id=start_operation_id,
            expected_cluster_route_revision=expected_cluster_route_revision,
            expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        )
        with _session_transition_lock(cluster=cluster, session_id=session_id):
            api_release_identity = _verify_session_start_worker_compatibility(definition)
            if (
                expected_api_release_identity_sha256 is not None
                and api_release_identity.sha256() != expected_api_release_identity_sha256
            ):
                raise RelayError("session API release identity changed after planning")
            plan = plan_remote_session_start(
                cluster=cluster,
                definition=definition,
                session_id=session_id,
                remote_api_port=remote_api_port,
                replace=replace,
                require_token=require_token,
                start_operation_id=preliminary_plan.start_operation_id,
                expected_cluster_route_revision=preliminary_plan.cluster_route_revision,
                expected_api_release_identity_sha256=api_release_identity.sha256(),
            )
            _finalize_completed_cleanup_receipt_before_start(
                definition=definition,
                cluster=cluster,
                session_id=session_id,
            )
            result = start_remote_session_durable(
                definition=definition,
                plan=plan,
                api_token=settings.api_token if require_token else None,
                expected_api_release_identity=api_release_identity,
                starter=start_remote_session,
            )
            if json_output:
                typer.echo(result.model_dump_json(indent=2))
            else:
                _echo_lines(result.compatibility_lines)
            if result.state in {"failed", "not_current"}:
                raise typer.Exit(code=1)
            if not result.usable:
                # A durable operation handle is useful for status/retry/cleanup,
                # but must never look like a successfully attached API session
                # to integrations that key off the process exit status.
                raise typer.Exit(code=2)

    _run_or_exit(action)


def _finalize_completed_cleanup_receipt_before_start(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
) -> None:
    """Finish the exact teardown commit if reconnect observes its completed receipt."""
    raw_status = status_remote_session(
        definition=definition,
        session_id=session_id,
        pre_start_cleanup_probe=True,
    )
    try:
        status = OwnedSessionRecoveryStatus.model_validate(raw_status)
    except ValidationError:
        return
    if not status.cleanup_receipt:
        return
    report = read_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        status=status,
    )
    report = _verified_finalized_cleanup_report(
        status,
        report=report,
        cluster=cluster,
        session_id=session_id,
    )
    generation_id = cast(str, report.session_generation_id)
    operation_id = report.cleanup_operation_id
    if operation_id is None:
        raise RelayError("completed cleanup receipt omitted its operation identity")
    admission = status.admission_status
    if not isinstance(admission, dict):
        raise RelayError("completed cleanup receipt omitted authoritative admission evidence")
    queue = _managed_queue_from_env()
    local_admission_session_id = _desktop_owner_session_admission_id(
        cluster=cluster,
        session_id=session_id,
    )
    local_status = queue.owner_session_generation_status(
        local_admission_session_id,
        session_generation_id=generation_id,
    )
    remote_closed = admission.get("closed") is True
    local_closing = bool(
        local_status.get("closing") is True
        and local_status.get("closing_generation_id") == generation_id
    )
    if not remote_closed and not local_closing:
        raise RelayError(
            "completed remote cleanup receipt has no exact desktop closing mirror; "
            "automatic reconnect recovery was refused before mutation"
        )
    _mark_owner_session_closed(
        queue=queue,
        definition=definition,
        cluster=cluster,
        remote_execution=should_execute_on_cluster(definition),
        session_id=session_id,
        local_admission_session_id=local_admission_session_id,
        session_generation_id=generation_id,
        legacy_unversioned_job_ids=[],
        finalized_recovery=status,
        finalized_report=report,
    )
    refreshed = OwnedSessionRecoveryStatus.model_validate(
        status_remote_session(definition=definition, session_id=session_id)
    )
    if not (
        refreshed.recovery_verified
        and refreshed.cleanup_receipt
        and refreshed.cleanup_paths_pending is False
        and refreshed.coordinator_report_bound
        and isinstance(refreshed.admission_status, dict)
        and refreshed.admission_status.get("closed") is True
    ):
        raise RelayError(
            "owned session cleanup closure was not authoritative after reconnect recovery"
        )
    if refreshed.coordinator_report_ref != status.coordinator_report_ref:
        raise RelayError("coordinator cleanup report reference changed during reconnect closure")
    _verified_finalized_cleanup_report(
        refreshed,
        report=report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=generation_id,
        expected_cleanup_operation_id=operation_id,
        expected_cleanup_policy=report.cleanup_policy,
    )


def _verified_finalized_cleanup_report(
    status: OwnedSessionRecoveryStatus,
    *,
    report: SessionLifecycleReport,
    cluster: str,
    session_id: str,
    expected_generation_id: str | None = None,
    expected_cleanup_operation_id: str | None = None,
    expected_cleanup_policy: dict[str, bool] | None = None,
) -> SessionLifecycleReport:
    """Return only a fully bound and semantically verified coordinator report."""
    generation_id = _verified_recovered_owner_session_generation(
        status,
        cluster=cluster,
        session_id=session_id,
    )
    if status.cleanup_paths_pending is not False:
        raise RelayError(
            "owned session cleanup receipt still has pending file deletion; "
            "retry teardown before reconnect"
        )
    if not (
        status.coordinator_report_bound
        and status.coordinator_report is None
        and status.coordinator_report_ref is not None
        and status.coordinator_report_sha256 is not None
    ):
        raise RelayError(
            "owned session cleanup has only cluster-local evidence; retry teardown to "
            "finalize desktop, connector, gateway, relay, and scheduler dispositions"
        )
    report_payload = session_lifecycle_report_bytes(report)
    report_sha256 = hashlib.sha256(report_payload).hexdigest()
    if not (
        len(report_payload) == status.coordinator_report_ref.size
        and report_sha256 == status.coordinator_report_sha256
        and report_sha256 == status.coordinator_report_ref.sha256
    ):
        raise RelayError("coordinator cleanup report size or digest did not match its receipt")
    policy = report.cleanup_policy
    if set(policy) != {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}:
        raise RelayError("coordinator cleanup report policy is incomplete")
    if policy["cancel_scheduler_jobs"] and not policy["cancel_jobs"]:
        raise RelayError("coordinator cleanup report has an invalid cancellation policy")
    admission = status.admission_status
    raw_intent = admission.get("cleanup_intent") if isinstance(admission, dict) else None
    intent = cast(dict[str, object], raw_intent) if isinstance(raw_intent, dict) else None
    if not (
        intent is not None
        and report.cleanup_operation_id == intent.get("operation_id")
        and {
            key: intent.get(key) for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        == policy
    ):
        raise RelayError("coordinator cleanup report does not match immutable cleanup intent")
    if expected_generation_id is not None and generation_id != expected_generation_id:
        raise RelayError("finalized cleanup retry changed its generation identity")
    if (
        expected_cleanup_operation_id is not None
        and report.cleanup_operation_id != expected_cleanup_operation_id
    ):
        raise RelayError("finalized cleanup retry changed its operation identity")
    if expected_cleanup_policy is not None and policy != expected_cleanup_policy:
        raise RelayError("finalized cleanup retry changed its immutable policy")
    _verify_owner_session_teardown(
        report,
        session_id=session_id,
        session_generation_id=generation_id,
        stop_worker=policy["stop_worker"],
    )
    return report


def _persist_local_cleanup_report_artifact(
    report: SessionLifecycleReport,
    *,
    validation_report_path: Path,
    evidence_lock: _CleanupEvidenceLock | None = None,
) -> _LocalCleanupReportArtifact:
    """Persist one exact report in a private, report-owned bounded artifact directory."""
    payload = session_lifecycle_report_bytes(report)
    digest = hashlib.sha256(payload).hexdigest()
    chunk_specs: list[tuple[str, bytes, str]] = []
    for index, offset in enumerate(range(0, len(payload), MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES)):
        chunk = payload[offset : offset + MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES]
        chunk_specs.append(
            (
                f"r-{digest}.p{index:04d}",
                chunk,
                hashlib.sha256(chunk).hexdigest(),
            )
        )
    manifest_payload = json.dumps(
        {
            "schema_version": "clio-relay.local-cleanup-report-artifact.v1",
            "encoding": "canonical-json-chunks",
            "report_sha256": digest,
            "report_size": len(payload),
            "chunk_size_limit": MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES,
            "chunks": [
                {"name": name, "size": len(chunk), "sha256": sha256}
                for name, chunk, sha256 in chunk_specs
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(manifest_payload) > MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES:
        raise RelayError("local cleanup report artifact manifest exceeds its byte limit")
    manifest_sha256 = hashlib.sha256(manifest_payload).hexdigest()
    manifest_name = f"r-{digest}.manifest"
    expected_names = {manifest_name, *(name for name, _chunk, _sha256 in chunk_specs)}
    expected_names.update(f".{name}.pending" for name in tuple(expected_names))

    if not validation_report_path.name or validation_report_path.name in {".", ".."}:
        raise RelayError("validation report path has no safe artifact identity")
    requested_parent = _cleanup_evidence_state_parent()
    if evidence_lock is not None:
        _verify_cleanup_evidence_lock(
            evidence_lock,
            expected_parent=requested_parent,
        )
    locked_posix_parent_fd = (
        evidence_lock.parent_fd if evidence_lock is not None and os.name == "posix" else None
    )
    if os.name == "posix" and evidence_lock is not None and locked_posix_parent_fd is None:
        raise RelayError("cleanup evidence lock omitted its pinned POSIX parent")
    if locked_posix_parent_fd is not None:
        if evidence_lock is None:  # pragma: no cover - narrowed by descriptor selection
            raise RelayError("cleanup evidence lock disappeared while binding its parent")
        parent_directory = evidence_lock.path.parent
        parent_linked = os.fstat(locked_posix_parent_fd)
    else:
        durably_ensure_validation_directory(requested_parent)
        requested_parent_status = os.lstat(requested_parent)
        if stat.S_ISLNK(requested_parent_status.st_mode):
            raise RelayError("local cleanup report artifact parent cannot be a symlink")
        if os.name == "nt":
            requested_anchor = _open_windows_pinned_directory(
                requested_parent,
                expected=requested_parent_status,
            )
            _close_windows_pinned_directory(requested_anchor)
        parent_directory = requested_parent.resolve(strict=True)
        if os.path.normcase(str(parent_directory)) != os.path.normcase(str(requested_parent)):
            raise RelayError("local cleanup report artifact parent cannot traverse a reparse point")
        parent_linked = os.lstat(parent_directory)
    if not stat.S_ISDIR(parent_linked.st_mode) or stat.S_ISLNK(parent_linked.st_mode):
        raise RelayError("local cleanup report artifact parent is not a real directory")
    if os.name == "posix" and not (
        (parent_linked.st_uid == os.geteuid() and stat.S_IMODE(parent_linked.st_mode) & 0o022 == 0)
        or (parent_linked.st_uid == 0 and stat.S_IMODE(parent_linked.st_mode) & stat.S_ISVTX != 0)
    ):
        raise RelayError("local cleanup report artifact parent is not rename-safe")
    artifact_directory_name = _LOCAL_CLEANUP_REPORT_ARTIFACT_DIRECTORY_NAME
    artifact_directory = parent_directory / artifact_directory_name
    parent_fd: int | None = None
    directory_fd: int | None = None
    parent_windows_anchor: _WindowsPinnedDirectory | None = None
    directory_windows_anchor: _WindowsPinnedDirectory | None = None
    directory_windows_guard: tuple[Path, ctypes.c_void_p] | None = None
    if os.name == "posix":
        try:
            parent_fd = (
                os.dup(locked_posix_parent_fd)
                if locked_posix_parent_fd is not None
                else os.open(
                    parent_directory,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                )
            )
            if not os.path.samestat(parent_linked, os.fstat(parent_fd)):
                raise RelayError("local cleanup report artifact parent changed while opening")
            created = False
            try:
                os.mkdir(artifact_directory_name, 0o700, dir_fd=parent_fd)
                created = True
            except FileExistsError:
                pass
            if created:
                os.fsync(parent_fd)
            directory_linked = os.stat(
                artifact_directory_name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            directory_fd = os.open(
                artifact_directory_name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
        except OSError as exc:
            if directory_fd is not None:
                os.close(directory_fd)
            if parent_fd is not None:
                os.close(parent_fd)
            raise RelayError(
                f"local cleanup report artifact directory cannot be pinned: {exc}"
            ) from exc
        directory_opened = os.fstat(directory_fd)
        if evidence_lock is not None and (
            evidence_lock.parent_fd is None
            or not os.path.samestat(os.fstat(evidence_lock.parent_fd), os.fstat(parent_fd))
        ):
            os.close(directory_fd)
            os.close(parent_fd)
            raise RelayError("local cleanup report artifact parent differs from its evidence lock")
        if not (
            stat.S_ISDIR(directory_linked.st_mode)
            and not stat.S_ISLNK(directory_linked.st_mode)
            and directory_linked.st_uid == os.geteuid()
            and stat.S_IMODE(directory_linked.st_mode) & 0o077 == 0
            and os.path.samestat(directory_linked, directory_opened)
        ):
            os.close(directory_fd)
            os.close(parent_fd)
            raise RelayError("local cleanup report artifact directory changed while opening")
    else:
        try:
            parent_windows_anchor = _open_windows_pinned_directory(
                parent_directory,
                expected=parent_linked,
            )
            durably_ensure_validation_directory(artifact_directory)
            directory_linked = os.lstat(artifact_directory)
            if not stat.S_ISDIR(directory_linked.st_mode) or stat.S_ISLNK(directory_linked.st_mode):
                raise RelayError("local cleanup report artifact directory is not a real directory")
            directory_windows_anchor = _open_windows_pinned_directory(
                artifact_directory,
                expected=directory_linked,
                acl_write=True,
            )
            ensure_private_configuration_windows_handle(
                internal_filesystem_path(artifact_directory, force_extended=True),
                handle=directory_windows_anchor.handle,
                directory=True,
            )
            directory_windows_guard = acquire_private_configuration_windows_parent_guard(
                artifact_directory
            )
            _verify_windows_pinned_directory(directory_windows_anchor)
            if evidence_lock is not None:
                _verify_cleanup_evidence_lock(
                    evidence_lock,
                    expected_parent=parent_directory,
                )
                if evidence_lock.windows_parent is None or not os.path.samestat(
                    evidence_lock.windows_parent.status,
                    parent_windows_anchor.status,
                ):
                    raise RelayError(
                        "local cleanup report artifact parent differs from its evidence lock"
                    )
        except BaseException:
            try:
                _close_windows_pinned_directory(directory_windows_anchor)
            finally:
                try:
                    _close_windows_pinned_directory(parent_windows_anchor)
                finally:
                    release_private_configuration_windows_parent_guard(directory_windows_guard)
            raise

    parent_fd = _optional_runtime_descriptor(parent_fd)
    directory_fd = _optional_runtime_descriptor(directory_fd)
    ignored_internal_names = _windows_parent_guard_names(directory_windows_guard)

    def verify_directory() -> None:
        try:
            observed_parent = os.lstat(parent_directory)
            observed = (
                os.stat(
                    artifact_directory_name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                if parent_fd is not None
                else os.lstat(artifact_directory)
            )
        except OSError as exc:
            raise RelayError("local cleanup report artifact directory disappeared") from exc
        if not (
            os.path.samestat(parent_linked, observed_parent)
            and os.path.samestat(directory_linked, observed)
        ):
            raise RelayError("local cleanup report artifact directory identity changed")
        if os.name == "nt":
            _verify_windows_pinned_directory(parent_windows_anchor)
            _verify_windows_pinned_directory(directory_windows_anchor)

    def stat_name(name: str) -> os.stat_result | None:
        if Path(name).name != name:
            raise RelayError("local cleanup report artifact name is unsafe")
        try:
            if directory_fd is not None:
                return os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            return os.lstat(artifact_directory / name)
        except FileNotFoundError:
            return None

    def fsync_directory() -> None:
        if directory_fd is not None:
            os.fsync(directory_fd)
        verify_directory()

    def unlink_name(name: str) -> None:
        if directory_fd is not None:
            os.unlink(name, dir_fd=directory_fd)
        else:
            os.unlink(artifact_directory / name)
        fsync_directory()

    def read_exact(
        name: str,
        *,
        expected_size: int,
        required: bool,
        expected_nlink: int = 1,
    ) -> bytes | None:
        descriptor: int | None = None
        try:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0)
            )
            if directory_fd is not None:
                descriptor = os.open(name, flags, dir_fd=directory_fd)
            elif os.name == "nt":
                descriptor = open_private_configuration_windows_descriptor(
                    internal_filesystem_path(
                        artifact_directory / name,
                        force_extended=True,
                    ),
                    expected_nlink=expected_nlink,
                )
            else:
                descriptor = os.open(artifact_directory / name, flags)
        except FileNotFoundError:
            if required:
                raise RelayError("local cleanup report artifact disappeared") from None
            return None
        except OSError as exc:
            raise RelayError(
                f"local cleanup report artifact cannot be opened safely: {exc}"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            linked = stat_name(name)
            if linked is None:  # pragma: no cover - opened descriptor exists
                raise RelayError("local cleanup report artifact pathname disappeared")
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(linked.st_mode)
                and opened.st_nlink == expected_nlink
                and linked.st_nlink == expected_nlink
                and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
                and opened.st_size == expected_size
            ):
                raise RelayError("local cleanup report artifact is not one exact regular file")
            if os.name == "posix" and not (
                opened.st_uid == os.geteuid()
                and linked.st_uid == os.geteuid()
                and stat.S_IMODE(opened.st_mode) == 0o600
                and stat.S_IMODE(linked.st_mode) == 0o600
            ):
                raise RelayError("local cleanup report artifact is not owner-private")
            value = bytearray()
            while len(value) <= expected_size:
                chunk = os.read(
                    descriptor,
                    min(64 * 1024, expected_size + 1 - len(value)),
                )
                if not chunk:
                    break
                value.extend(chunk)
            final_opened = os.fstat(descriptor)
            final_linked = stat_name(name)
            initial_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
                opened.st_nlink,
            )
            final_identity = (
                final_opened.st_dev,
                final_opened.st_ino,
                final_opened.st_size,
                final_opened.st_mtime_ns,
                final_opened.st_ctime_ns,
                final_opened.st_nlink,
            )
            if (
                len(value) != expected_size
                or final_linked is None
                or final_identity != initial_identity
                or (final_linked.st_dev, final_linked.st_ino, final_linked.st_nlink)
                != (final_opened.st_dev, final_opened.st_ino, expected_nlink)
            ):
                raise RelayError("local cleanup report artifact changed while it was read")
            verify_directory()
            return bytes(value)
        finally:
            os.close(descriptor)

    def candidate_byte_limit(name: str) -> int:
        return (
            MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES
            if ".manifest" in name
            else MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
        )

    def verify_candidate_status(
        name: str,
        observed: os.stat_result,
        *,
        expected_nlink: int,
    ) -> None:
        if not (
            stat.S_ISREG(observed.st_mode)
            and observed.st_nlink == expected_nlink
            and 0 <= observed.st_size <= candidate_byte_limit(name)
        ):
            raise RelayError("local cleanup report artifact candidate is unsafe")
        if os.name == "posix" and not (
            observed.st_uid == os.geteuid() and stat.S_IMODE(observed.st_mode) == 0o600
        ):
            raise RelayError("local cleanup report artifact candidate is not owner-private")

    def unlink_verified_candidate(
        name: str,
        observed: os.stat_result,
        *,
        expected_nlink: int,
    ) -> None:
        verify_candidate_status(name, observed, expected_nlink=expected_nlink)
        current = stat_name(name)
        if current is None or (
            current.st_dev,
            current.st_ino,
            current.st_size,
            current.st_nlink,
        ) != (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            expected_nlink,
        ):
            raise RelayError("local cleanup report artifact changed before deletion")
        unlink_name(name)

    def scan_candidates() -> dict[str, os.stat_result]:
        candidates: dict[str, os.stat_result] = {}
        stored_bytes = 0
        observed_inodes: set[tuple[int, int]] = set()
        verify_directory()
        try:
            with os.scandir(directory_fd if directory_fd is not None else artifact_directory) as it:
                for entry in it:
                    name = entry.name
                    if name in ignored_internal_names:
                        continue
                    if not (
                        _LOCAL_CLEANUP_REPORT_ARTIFACT_PATTERN.fullmatch(name)
                        or _LOCAL_CLEANUP_REPORT_PENDING_PATTERN.fullmatch(name)
                    ):
                        raise RelayError(
                            "local cleanup report artifact directory contains an invalid entry"
                        )
                    if len(candidates) >= MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_ENTRIES:
                        raise RelayError(
                            "local cleanup report artifact directory exceeds its entry limit"
                        )
                    observed = stat_name(name)
                    if observed is None:
                        raise RelayError(
                            "local cleanup report artifact disappeared during enumeration"
                        )
                    inode_identity = (observed.st_dev, observed.st_ino)
                    if inode_identity not in observed_inodes:
                        stored_bytes += observed.st_size
                        observed_inodes.add(inode_identity)
                    if stored_bytes > MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_STORED_BYTES:
                        raise RelayError(
                            "local cleanup report artifact directory exceeds its byte limit"
                        )
                    candidates[name] = observed
        except OSError as exc:
            raise RelayError(
                f"local cleanup report artifact directory cannot be enumerated: {exc}"
            ) from exc
        verify_directory()
        return candidates

    def prune_unreferenced_candidates(*, preserve_names: set[str]) -> None:
        candidates = scan_candidates()
        remaining = set(candidates)
        for pending_name in sorted(
            name
            for name in remaining
            if _LOCAL_CLEANUP_REPORT_PENDING_PATTERN.fullmatch(name) and name not in preserve_names
        ):
            if pending_name not in remaining:
                continue
            final_name = pending_name[1 : -len(".pending")]
            pending_status = candidates[pending_name]
            if final_name in remaining and final_name not in preserve_names:
                final_status = candidates[final_name]
                if not (
                    (pending_status.st_dev, pending_status.st_ino)
                    == (final_status.st_dev, final_status.st_ino)
                    and pending_status.st_nlink == 2
                    and final_status.st_nlink == 2
                ):
                    raise RelayError(
                        "local cleanup report artifact pruning found an ambiguous link pair"
                    )
                verify_candidate_status(pending_name, pending_status, expected_nlink=2)
                verify_candidate_status(final_name, final_status, expected_nlink=2)
                unlink_verified_candidate(pending_name, pending_status, expected_nlink=2)
                refreshed_final = stat_name(final_name)
                if refreshed_final is None:
                    raise RelayError("local cleanup report artifact disappeared while pruning")
                unlink_verified_candidate(final_name, refreshed_final, expected_nlink=1)
                remaining.remove(pending_name)
                remaining.remove(final_name)
                continue
            unlink_verified_candidate(pending_name, pending_status, expected_nlink=1)
            remaining.remove(pending_name)
        for name in sorted(remaining - preserve_names):
            unlink_verified_candidate(name, candidates[name], expected_nlink=1)
            remaining.remove(name)
        observed = set(scan_candidates())
        if not observed.issubset(preserve_names):
            raise RelayError("local cleanup report artifact pruning was not exact")

    def complete_report_names(
        candidates: dict[str, os.stat_result],
        *,
        candidate_digest: str,
    ) -> set[str]:
        manifest_candidate_name = f"r-{candidate_digest}.manifest"
        manifest_status = candidates.get(manifest_candidate_name)
        if manifest_status is None:
            raise RelayError("retained cleanup report artifact has no manifest")
        verify_candidate_status(
            manifest_candidate_name,
            manifest_status,
            expected_nlink=1,
        )
        manifest_bytes = read_exact(
            manifest_candidate_name,
            expected_size=manifest_status.st_size,
            required=True,
        )
        try:
            manifest_value = json.loads((manifest_bytes or b"").decode("utf-8"))
        except (UnicodeDecodeError, JSONDecodeError) as exc:
            raise RelayError("retained cleanup report artifact manifest is invalid") from exc
        if not isinstance(manifest_value, dict):
            raise RelayError("retained cleanup report artifact manifest is not an object")
        manifest = cast(dict[str, object], manifest_value)
        raw_report_size = manifest.get("report_size")
        raw_chunks = manifest.get("chunks")
        if not (
            manifest.get("schema_version") == "clio-relay.local-cleanup-report-artifact.v1"
            and manifest.get("encoding") == "canonical-json-chunks"
            and manifest.get("report_sha256") == candidate_digest
            and manifest.get("chunk_size_limit") == MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
            and isinstance(raw_report_size, int)
            and not isinstance(raw_report_size, bool)
            and 0 < raw_report_size <= MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES
            and isinstance(raw_chunks, list)
            and 0
            < len(cast(list[object], raw_chunks))
            <= (MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES - 1)
            // MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
        ):
            raise RelayError("retained cleanup report artifact manifest is inconsistent")
        retained = {manifest_candidate_name}
        observed_report_size = 0
        report_hasher = hashlib.sha256()
        for index, raw_chunk in enumerate(cast(list[object], raw_chunks)):
            if not isinstance(raw_chunk, dict):
                raise RelayError("retained cleanup report artifact chunk is invalid")
            chunk = cast(dict[str, object], raw_chunk)
            chunk_name = f"r-{candidate_digest}.p{index:04d}"
            chunk_size = chunk.get("size")
            chunk_sha256 = chunk.get("sha256")
            if not (
                chunk.get("name") == chunk_name
                and isinstance(chunk_size, int)
                and not isinstance(chunk_size, bool)
                and 0 < chunk_size <= MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
                and isinstance(chunk_sha256, str)
                and re.fullmatch(r"[0-9a-f]{64}", chunk_sha256)
            ):
                raise RelayError("retained cleanup report artifact chunk metadata is invalid")
            chunk_status = candidates.get(chunk_name)
            if chunk_status is None or chunk_status.st_size != chunk_size:
                raise RelayError("retained cleanup report artifact chunk is missing")
            verify_candidate_status(chunk_name, chunk_status, expected_nlink=1)
            chunk_bytes = read_exact(
                chunk_name,
                expected_size=chunk_size,
                required=True,
            )
            if chunk_bytes is None or hashlib.sha256(chunk_bytes).hexdigest() != chunk_sha256:
                raise RelayError("retained cleanup report artifact chunk digest is invalid")
            observed_report_size += chunk_size
            report_hasher.update(chunk_bytes)
            retained.add(chunk_name)
        if observed_report_size != raw_report_size or report_hasher.hexdigest() != candidate_digest:
            raise RelayError("retained cleanup report artifact size or digest is inconsistent")
        return retained

    def newest_previous_complete_report_names(
        candidates: dict[str, os.stat_result],
    ) -> set[str]:
        manifests: list[tuple[int, str]] = []
        for name, observed in candidates.items():
            match = re.fullmatch(r"r-([0-9a-f]{64})\.manifest", name)
            if match is not None and match.group(1) != digest:
                manifests.append((observed.st_mtime_ns, match.group(1)))
        if not manifests:
            return set()
        _mtime_ns, previous_digest = max(manifests)
        return complete_report_names(
            candidates,
            candidate_digest=previous_digest,
        )

    def publish_exact(name: str, content: bytes, *, expected_sha256: str) -> None:
        pending_name = f".{name}.pending"
        final_status = stat_name(name)
        pending_status = stat_name(pending_name)
        if final_status is not None and pending_status is not None:
            safe_link_window = bool(
                stat.S_ISREG(final_status.st_mode)
                and stat.S_ISREG(pending_status.st_mode)
                and final_status.st_nlink == 2
                and pending_status.st_nlink == 2
                and (final_status.st_dev, final_status.st_ino)
                == (pending_status.st_dev, pending_status.st_ino)
            )
            if os.name == "posix":
                safe_link_window = bool(
                    safe_link_window
                    and final_status.st_uid == os.geteuid()
                    and pending_status.st_uid == os.geteuid()
                    and stat.S_IMODE(final_status.st_mode) == 0o600
                    and stat.S_IMODE(pending_status.st_mode) == 0o600
                )
            if not safe_link_window:
                raise RelayError("local cleanup report artifact publication is ambiguous")
            linked = read_exact(
                pending_name,
                expected_size=len(content),
                required=True,
                expected_nlink=2,
            )
            if linked is None or not hmac.compare_digest(linked, content):
                raise RelayError("local cleanup report artifact linked file differs")
            unlink_name(pending_name)
            final_status = stat_name(name)
            pending_status = None
        existing = read_exact(name, expected_size=len(content), required=False)
        if existing is not None:
            if hashlib.sha256(existing).hexdigest() != expected_sha256:
                raise RelayError("local cleanup report artifact digest did not match its name")
            return
        if final_status is not None:
            raise RelayError("local cleanup report artifact final path was not readable")
        descriptor: int | None = None
        try:
            if pending_status is not None:
                verify_candidate_status(pending_name, pending_status, expected_nlink=1)
                staged = (
                    read_exact(
                        pending_name,
                        expected_size=len(content),
                        required=True,
                    )
                    if pending_status.st_size == len(content)
                    else None
                )
                if staged is None or not hmac.compare_digest(staged, content):
                    # Pending-only content is unreferenced staging.  A proven
                    # owner-private single link may be removed and restaged
                    # after an interrupted write.
                    unlink_verified_candidate(
                        pending_name,
                        pending_status,
                        expected_nlink=1,
                    )
                    pending_status = None
            if pending_status is None:
                if os.name == "nt":
                    pending_path = internal_filesystem_path(
                        artifact_directory / pending_name,
                        force_extended=True,
                    )
                    with open_private_atomic_file(pending_path) as stream:
                        view = memoryview(content)
                        while view:
                            written = stream.write(view)
                            if written <= 0:
                                raise RelayError(
                                    "local cleanup report artifact write made no progress"
                                )
                            view = view[written:]
                        stream.flush()
                        os.fsync(stream.fileno())
                else:
                    flags = (
                        os.O_WRONLY
                        | os.O_CREAT
                        | os.O_EXCL
                        | getattr(os, "O_BINARY", 0)
                        | getattr(os, "O_NOFOLLOW", 0)
                        | getattr(os, "O_CLOEXEC", 0)
                    )
                    descriptor = (
                        os.open(pending_name, flags, 0o600, dir_fd=directory_fd)
                        if directory_fd is not None
                        else os.open(artifact_directory / pending_name, flags, 0o600)
                    )
                    if os.name == "posix":
                        os.fchmod(descriptor, 0o600)
                    view = memoryview(content)
                    while view:
                        written = os.write(descriptor, view)
                        if written <= 0:
                            raise RelayError("local cleanup report artifact write made no progress")
                        view = view[written:]
                    os.fsync(descriptor)
                    os.close(descriptor)
                    descriptor = None
                fsync_directory()
            staged = read_exact(pending_name, expected_size=len(content), required=True)
            if staged is None or not hmac.compare_digest(staged, content):
                raise RelayError("local cleanup report artifact pending file differs")
            if os.name == "nt":
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                move_file_ex = kernel32.MoveFileExW
                move_file_ex.argtypes = [
                    ctypes.c_wchar_p,
                    ctypes.c_wchar_p,
                    ctypes.c_uint32,
                ]
                move_file_ex.restype = ctypes.c_int
                pending_path = internal_filesystem_path(
                    artifact_directory / pending_name,
                    force_extended=True,
                )
                final_path = internal_filesystem_path(
                    artifact_directory / name,
                    force_extended=True,
                )
                if not move_file_ex(str(pending_path), str(final_path), 0x00000008):
                    error_number = ctypes.get_last_error()
                    raise OSError(
                        error_number,
                        ctypes.FormatError(error_number),
                        str(final_path),
                    )
                fsync_directory()
                committed = read_exact(name, expected_size=len(content), required=True)
                if committed is None or not hmac.compare_digest(committed, content):
                    raise RelayError(
                        "local cleanup report artifact changed after durable publication"
                    )
                return
            publication_complete = False
            try:
                if directory_fd is not None:
                    os.link(
                        pending_name,
                        name,
                        src_dir_fd=directory_fd,
                        dst_dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                else:
                    os.link(
                        artifact_directory / pending_name,
                        artifact_directory / name,
                        follow_symlinks=False,
                    )
                fsync_directory()
                final_linked = stat_name(name)
                pending_linked = stat_name(pending_name)
                if not (
                    final_linked is not None
                    and pending_linked is not None
                    and (final_linked.st_dev, final_linked.st_ino)
                    == (pending_linked.st_dev, pending_linked.st_ino)
                    and final_linked.st_nlink == 2
                    and pending_linked.st_nlink == 2
                ):
                    raise RelayError("local cleanup report artifact link publication was not exact")
                linked = read_exact(
                    name,
                    expected_size=len(content),
                    required=True,
                    expected_nlink=2,
                )
                if linked is None or not hmac.compare_digest(linked, content):
                    raise RelayError("local cleanup report artifact linked file differs")
                publication_complete = True
            except FileExistsError:
                raise RelayError(
                    "local cleanup report artifact concurrent publication is ambiguous"
                ) from None
            if publication_complete:
                unlink_name(pending_name)
        except OSError as exc:
            raise RelayError(
                f"local cleanup report artifact cannot be published safely: {exc}"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
        committed = read_exact(name, expected_size=len(content), required=True)
        if committed is None or hashlib.sha256(committed).hexdigest() != expected_sha256:
            raise RelayError("local cleanup report artifact changed after publication")

    try:
        previous_names = newest_previous_complete_report_names(scan_candidates())
        preserved_names = expected_names | previous_names
        prune_unreferenced_candidates(preserve_names=preserved_names)
        chunks: list[_LocalCleanupReportChunk] = []
        for chunk_name, chunk, chunk_sha256 in chunk_specs:
            publish_exact(chunk_name, chunk, expected_sha256=chunk_sha256)
            chunks.append(
                _LocalCleanupReportChunk(
                    path=artifact_directory / chunk_name,
                    size=len(chunk),
                    sha256=chunk_sha256,
                )
            )
        publish_exact(manifest_name, manifest_payload, expected_sha256=manifest_sha256)
        retained = scan_candidates()
        final_names = {name for name in expected_names if not name.startswith(".")}
        if set(retained) != final_names | previous_names:
            raise RelayError("local cleanup report artifact retention was not exact")
        retained_size = sum(item.st_size for item in retained.values())
        if (
            len(retained) > MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_ENTRIES - 1
            or retained_size > MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_STORED_BYTES
        ):
            raise RelayError("local cleanup report artifact retention exceeded its bound")
        verify_directory()
        return _LocalCleanupReportArtifact(
            manifest_path=artifact_directory / manifest_name,
            manifest_sha256=manifest_sha256,
            report_sha256=digest,
            report_size=len(payload),
            chunks=tuple(chunks),
        )
    finally:
        if directory_fd is not None:
            os.close(directory_fd)
        if parent_fd is not None:
            os.close(parent_fd)
        try:
            _close_windows_pinned_directory(directory_windows_anchor)
        finally:
            try:
                _close_windows_pinned_directory(parent_windows_anchor)
            finally:
                release_private_configuration_windows_parent_guard(directory_windows_guard)


def _persist_verified_cleanup_report_before_closure(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    report: SessionLifecycleReport,
) -> tuple[SessionLifecycleReport, OwnedSessionRecoveryStatus]:
    """Persist, re-read, and verify the immutable full cleanup report."""
    cleanup_operation_id = report.cleanup_operation_id
    if cleanup_operation_id is None:
        raise RelayError("coordinator cleanup report omitted its operation id")
    finalized_status = finalize_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        cleanup_policy=report.cleanup_policy,
        report=report,
    )
    retrieved_report = read_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        status=finalized_status,
    )
    finalized_report = _verified_finalized_cleanup_report(
        finalized_status,
        report=retrieved_report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=session_generation_id,
        expected_cleanup_operation_id=cleanup_operation_id,
        expected_cleanup_policy=report.cleanup_policy,
    )
    if session_lifecycle_report_sha256(finalized_report) != session_lifecycle_report_sha256(report):
        raise RelayError("re-read coordinator cleanup report changed before closure")
    return finalized_report, finalized_status


def _verify_session_start_worker_compatibility(
    definition: ClusterDefinition,
) -> SessionApiReleaseIdentity:
    """Require one exact live worker/install identity before session mutation."""
    local_identity = _session_api_release_identity_from_installation(
        installation_info(),
        label="local clio-relay",
    )
    remote_receipt = verify_remote_worker_info(
        _remote_worker_info(definition),
        expected_cluster=definition.name,
        expected_version=local_identity.distribution_version,
        expected_software=local_identity.software,
        expected_artifact_sha256=local_identity.artifact_sha256,
        expected_source=None,
    )
    artifact_sha256 = remote_receipt.artifact_sha256
    if artifact_sha256 is None:
        raise ConfigurationError("remote worker receipt omitted its artifact SHA-256")
    return SessionApiReleaseIdentity(
        distribution_version=remote_receipt.distribution_version,
        artifact_sha256=artifact_sha256,
        software=remote_receipt.software,
    )


def _session_api_release_identity_from_installation(
    info: dict[str, object],
    *,
    label: str,
) -> SessionApiReleaseIdentity:
    """Validate installation evidence and return its session-API identity."""
    if info.get("receipt_matches_install") is not True:
        raise ConfigurationError(f"{label} installation receipt does not match the running package")
    try:
        receipt = InstallReceipt.model_validate(info.get("receipt"))
        software = SoftwareIdentity.model_validate(info.get("software"))
    except ValidationError as exc:
        raise ConfigurationError(f"{label} installation identity is invalid: {exc}") from exc
    version = info.get("distribution_version")
    artifact_sha256 = receipt.artifact_sha256
    if (
        not isinstance(version, str)
        or receipt.distribution_version != version
        or receipt.software != software
        or artifact_sha256 is None
    ):
        raise ConfigurationError(f"{label} installation receipt does not match the running package")
    return SessionApiReleaseIdentity(
        distribution_version=version,
        artifact_sha256=artifact_sha256,
        software=software,
    )


def _require_process_bound_session_api_release() -> None:
    """Require the API process to match its release marker when one is present."""
    expected_sha256 = os.environ.get("CLIO_RELAY_API_RELEASE_IDENTITY_SHA256")
    if expected_sha256 is None:
        return
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ConfigurationError("session API release identity marker is invalid")
    observed = _session_api_release_identity_from_installation(
        installation_info(),
        label="session API",
    )
    if observed.sha256() != expected_sha256:
        raise ConfigurationError("session API release identity does not match running package")


@session_app.command("status")
def session_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
) -> None:
    """Inspect an owned remote relay API session."""
    definition = _require_cluster(cluster)
    _run_or_exit(
        lambda: typer.echo(
            json.dumps(
                status_remote_session(definition=definition, session_id=session_id), indent=2
            )
        )
    )


@session_app.command("start-status")
def session_start_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    start_operation_id: Annotated[str, typer.Option(help="Exact planned start operation id.")],
    cluster_route_revision: Annotated[
        str,
        typer.Option(help="Exact route revision from session plan-start."),
    ],
    remote_api_port: Annotated[int, typer.Option(help="Planned remote API port.")],
    expected_api_release_identity_sha256: Annotated[
        str,
        typer.Option(help="Exact release digest from session plan-start."),
    ],
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Planned replacement policy."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Planned API token policy."),
    ] = True,
) -> None:
    """Query one exact start once without imposing an aggregate wait deadline."""
    definition = _require_cluster(cluster)

    def action() -> None:
        plan = plan_remote_session_start(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            replace=replace,
            require_token=require_token,
            start_operation_id=start_operation_id,
            expected_cluster_route_revision=cluster_route_revision,
            expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        )
        typer.echo(
            query_remote_session_start(definition=definition, plan=plan).model_dump_json(indent=2)
        )

    _run_or_exit(action)


@session_app.command("start-watch")
def session_start_watch(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    start_operation_id: Annotated[str, typer.Option(help="Exact planned start operation id.")],
    cluster_route_revision: Annotated[
        str,
        typer.Option(help="Exact route revision from session plan-start."),
    ],
    remote_api_port: Annotated[int, typer.Option(help="Planned remote API port.")],
    expected_api_release_identity_sha256: Annotated[
        str,
        typer.Option(help="Exact release digest from session plan-start."),
    ],
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Planned replacement policy."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Planned API token policy."),
    ] = True,
    timeout_seconds: Annotated[
        float,
        typer.Option(min=0.1, max=3600.0, help="Bounded aggregate watch duration."),
    ] = 120.0,
    poll_seconds: Annotated[
        float,
        typer.Option(min=0.05, max=60.0, help="Delay between exact status observations."),
    ] = 0.5,
) -> None:
    """Watch a durable handle; exit 0 is ready, 1 failed, and 2 detached."""
    definition = _require_cluster(cluster)

    def action() -> None:
        plan = plan_remote_session_start(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            replace=replace,
            require_token=require_token,
            start_operation_id=start_operation_id,
            expected_cluster_route_revision=cluster_route_revision,
            expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        )
        result = watch_remote_session_start(
            definition=definition,
            plan=plan,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        typer.echo(result.model_dump_json(indent=2))
        if result.state in {"failed", "not_current"}:
            raise typer.Exit(code=1)
        if not result.usable:
            raise typer.Exit(code=2)

    _run_or_exit(action)


@session_app.command("submit-jarvis")
def session_submit_jarvis(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
    pipeline_yaml_file: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Local JARVIS pipeline YAML file.",
        ),
    ],
    idempotency_key: Annotated[str, typer.Option(help="Durable submission identity.")],
    timeout_seconds: Annotated[
        float,
        typer.Option(min=1, max=300, help="Bounded session API transport timeout."),
    ] = 30,
) -> None:
    """Submit JARVIS through the identity-proven exact-generation session API."""
    settings = RelaySettings.from_env().model_copy(
        update={
            "owner_session_id": session_id,
            "owner_session_generation_id": session_generation_id,
            "owner_session_cluster": cluster,
        }
    )
    definition = _require_cluster(cluster)

    def action() -> None:
        job = submit_owned_session_job(
            definition=definition,
            settings=settings,
            path="/jobs/jarvis",
            payload={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml_file.read_text(encoding="utf-8"),
                "idempotency_key": idempotency_key,
            },
            timeout_seconds=timeout_seconds,
        )
        typer.echo(json.dumps(job.model_dump(mode="json"), indent=2))

    _run_or_exit(action)


@session_app.command("quiesce-intake", hidden=True)
def session_quiesce_intake(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
    cleanup_operation_id: Annotated[
        str | None,
        typer.Option(help="Exact cleanup operation id selected by the desktop coordinator."),
    ] = None,
    cleanup_stop_worker: Annotated[
        bool,
        typer.Option(help="Persist worker-stop scope in the immutable cleanup intent."),
    ] = False,
    cleanup_cancel_jobs: Annotated[
        bool,
        typer.Option(help="Persist relay cancellation scope in the immutable cleanup intent."),
    ] = False,
    cleanup_cancel_scheduler_jobs: Annotated[
        bool,
        typer.Option(help="Persist scheduler cancellation scope in the cleanup intent."),
    ] = False,
) -> None:
    """Durably stop one owned API session from accepting new work."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        cleanup_intent = queue.set_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
            operation_id=cleanup_operation_id,
            stop_worker=cleanup_stop_worker,
            cancel_jobs=cleanup_cancel_jobs,
            cancel_scheduler_jobs=cleanup_cancel_scheduler_jobs,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                    "intake": "quiesced",
                    "cleanup_intent": cleanup_intent,
                }
            )
        )

    _run_or_exit(action)


@session_app.command("admission-status", hidden=True)
def session_admission_status(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
) -> None:
    """Return machine-readable intake state for one exact session generation."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        typer.echo(
            json.dumps(
                queue.owner_session_generation_status(
                    session_id,
                    session_generation_id=session_generation_id,
                )
            )
        )

    _run_or_exit(action)


@session_app.command("recovery-status", hidden=True)
def session_recovery_status(
    cluster: Annotated[str, typer.Option(help="Exact cluster recorded by the owned session.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    pre_start_cleanup_probe: Annotated[
        bool,
        typer.Option(
            help=(
                "Return a structured unverified observation when no transition exists yet; "
                "reserved for the read-only pre-start cleanup probe."
            )
        ),
    ] = False,
) -> None:
    """Return fail-closed recovery evidence for an ambiguous or dead session start."""

    def action() -> None:
        settings_core_dir = RelaySettings.from_env().core_dir
        status = (
            _inspect_owned_session_recovery_before_start(
                cluster=cluster,
                session_id=session_id,
                core_dir=settings_core_dir,
            )
            if pre_start_cleanup_probe
            else _inspect_owned_session_recovery_after_transition(
                cluster=cluster,
                session_id=session_id,
                core_dir=settings_core_dir,
            )
        )
        typer.echo(status.model_dump_json(indent=2))

    _run_or_exit(action)


@session_app.command("start-status-owned", hidden=True)
def session_start_status_owned(
    cluster: Annotated[str, typer.Option(help="Exact cluster selected by the start plan.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    start_operation_id: Annotated[str, typer.Option(help="Exact start operation id.")],
    cluster_route_revision: Annotated[
        str,
        typer.Option(help="Exact cluster route revision selected by the start plan."),
    ],
) -> None:
    """Return one nonblocking cluster-local start observation."""

    def action() -> None:
        status = inspect_owned_session_start_status(
            cluster=cluster,
            session_id=session_id,
            start_operation_id=start_operation_id,
            cluster_route_revision=cluster_route_revision,
            core_dir=RelaySettings.from_env().core_dir,
        )
        typer.echo(status.model_dump_json(indent=2))

    _run_or_exit(action)


@session_app.command("start-owned", hidden=True)
def session_start_owned() -> None:
    """Execute a bounded stdin-carried owned-session start on the cluster."""

    def action() -> None:
        maximum_bytes = MAX_CLUSTER_REGISTRY_BYTES + 128 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session start request exceeds its byte limit")
        try:
            request = OwnedSessionStartRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session start request is invalid: {exc}") from exc
        try:
            for line in execute_owned_session_start(request):
                typer.echo(line)
        except RelayError as exc:
            typer.echo(
                OwnedSessionStartRejection(
                    cluster=request.cluster,
                    session_id=request.session_id,
                    start_operation_id=request.start_operation_id,
                    cluster_route_revision=request.cluster_route_revision,
                    error=str(exc)[:8192] or "owned-session start was rejected",
                ).model_dump_json()
            )
            raise typer.Exit(code=1) from exc

    _run_or_exit(action)


@session_app.command("teardown-owned", hidden=True)
def session_teardown_owned() -> None:
    """Execute a bounded stdin-carried owned-session teardown on the cluster."""

    def action() -> None:
        maximum_bytes = 128 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session teardown request exceeds its byte limit")
        try:
            request = OwnedSessionTeardownRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session teardown request is invalid: {exc}") from exc
        typer.echo(execute_owned_session_teardown(request).model_dump_json())

    _run_or_exit(action)


@session_app.command("challenge-owned", hidden=True)
def session_challenge_owned() -> None:
    """Answer a bounded stdin-carried owned-session identity challenge."""

    def action() -> None:
        maximum_bytes = 64 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session challenge request exceeds its byte limit")
        try:
            request = OwnedSessionIdentityChallengeRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session challenge request is invalid: {exc}") from exc
        typer.echo(json.dumps(execute_owned_session_identity_challenge(request)))

    _run_or_exit(action)


@session_app.command("finalize-cleanup-owned", hidden=True)
def session_finalize_cleanup_owned() -> None:
    """Bind a bounded coordinator-verified report to one cleanup receipt."""

    def action() -> None:
        maximum_bytes = MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session cleanup finalization exceeds its byte limit")
        try:
            request = OwnedSessionCleanupFinalizeRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session cleanup finalization is invalid: {exc}") from exc
        typer.echo(execute_owned_session_cleanup_finalize(request).model_dump_json())

    _run_or_exit(action)


@session_app.command("read-cleanup-report-owned", hidden=True)
def session_read_cleanup_report_owned() -> None:
    """Read one finalized cleanup report through its exact sidecar reference."""

    def action() -> None:
        maximum_bytes = 256 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session cleanup report read exceeds its byte limit")
        try:
            request = OwnedSessionCleanupReportReadRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session cleanup report read is invalid: {exc}") from exc
        typer.echo(execute_owned_session_cleanup_report_read(request).model_dump_json())

    _run_or_exit(action)


def _inspect_owned_session_recovery_after_transition(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    home: Path | None = None,
    timeout_seconds: float = OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS,
) -> OwnedSessionRecoveryStatus:
    """Wait for an ambiguous start transition, then inspect its exact durable identity."""
    if re.fullmatch(r"[A-Za-z0-9_-]+", session_id) is None:
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    selected_home = home or Path.home()
    session_dir = selected_home / ".local" / "share" / "clio-relay" / "sessions" / session_id
    transition_path = session_dir / "transition.lock"
    deadline = monotonic() + timeout_seconds
    transition_status: os.stat_result | None = None
    while transition_status is None:
        try:
            transition_status = transition_path.lstat()
        except FileNotFoundError:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise RelayError(
                    "owned session transition lock did not materialize during the bounded "
                    "recovery wait; a delayed remote start cannot be ruled out"
                ) from None
            sleep(min(0.05, remaining))
    if not stat.S_ISREG(transition_status.st_mode):
        raise RelayError("owned session transition lock is not a regular file")
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise RelayError(
            "owned session start transition could not be inspected during the bounded recovery wait"
        )
    if os.name == "posix" and getattr(os, "O_NOFOLLOW", 0) and getattr(os, "O_DIRECTORY", 0):
        with open_owned_session_transaction(
            session_id=session_id,
            create=False,
            timeout_seconds=remaining,
            home=selected_home,
        ) as transaction:
            locked_status = os.fstat(transaction.lock_fd)
            if (locked_status.st_dev, locked_status.st_ino) != (
                transition_status.st_dev,
                transition_status.st_ino,
            ):
                raise RelayError("owned session transition lock changed during recovery")
            return inspect_owned_session_recovery_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                home=selected_home,
                transaction=transaction,
            )
    try:
        with FileLock(
            str(transition_path),
            timeout=remaining,
            mode=0o600,
        ):
            locked_status = transition_path.lstat()
            lock_identity_changed = os.name == "posix" and (
                locked_status.st_dev,
                locked_status.st_ino,
            ) != (transition_status.st_dev, transition_status.st_ino)
            if not stat.S_ISREG(locked_status.st_mode) or lock_identity_changed:
                raise RelayError("owned session transition lock changed during recovery")
            return inspect_owned_session_recovery_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                home=selected_home,
            )
    except FileLockTimeout as exc:
        raise RelayError(
            "owned session start is still in progress after the bounded recovery wait"
        ) from exc


def _inspect_owned_session_recovery_before_start(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    home: Path | None = None,
    timeout_seconds: float = OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS,
) -> OwnedSessionRecoveryStatus:
    """Observe cleanup state without requiring a fresh session transition to exist."""
    if re.fullmatch(r"[A-Za-z0-9_-]+", session_id) is None:
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if not cluster:
        raise RelayError("cluster must not be empty")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    selected_home = home or Path.home()
    transition_path = (
        selected_home
        / ".local"
        / "share"
        / "clio-relay"
        / "sessions"
        / session_id
        / "transition.lock"
    )
    try:
        transition_path.lstat()
    except FileNotFoundError:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            cleanup_receipt=False,
            ownership_verified=False,
            recovery_verified=False,
            errors=[
                "owned session transition is not currently observable; "
                "start-owned remains the mutation authority"
            ],
        )
    return _inspect_owned_session_recovery_after_transition(
        cluster=cluster,
        session_id=session_id,
        core_dir=core_dir,
        home=selected_home,
        timeout_seconds=timeout_seconds,
    )


@session_app.command("prepare-start", hidden=True)
def session_prepare_start(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    candidate_generation_id: Annotated[
        str,
        typer.Option(help="Fresh candidate generation for an initial start or verified reopen."),
    ],
    recorded_generation_id: Annotated[
        str | None,
        typer.Option(help="Generation from verified durable API-session metadata, if present."),
    ] = None,
) -> None:
    """Atomically select the authoritative generation for an owned API start."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        generation_id = queue.prepare_owner_session_start(
            session_id,
            recorded_generation_id=recorded_generation_id,
            candidate_generation_id=candidate_generation_id,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": generation_id,
                }
            )
        )

    _run_or_exit(action)


@session_app.command("resume-intake", hidden=True)
def session_resume_intake(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact new or reopened relay session generation id."),
    ],
) -> None:
    """Clear durable intake quiescence for a new owned API generation."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        queue.clear_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                    "intake": "open",
                }
            )
        )

    _run_or_exit(action)


@session_app.command("mark-closed", hidden=True)
def session_mark_closed(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact verified relay session generation id."),
    ],
    legacy_unversioned_job_id: Annotated[
        list[str] | None,
        typer.Option(help="Exact verified legacy job id covered by this first upgraded teardown."),
    ] = None,
) -> None:
    """Durably close one verified, already-quiesced owner session generation."""

    def action() -> None:
        queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
        closure = queue.set_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
            legacy_unversioned_job_ids=legacy_unversioned_job_id or [],
        )
        payload = closure.model_dump(mode="json")
        if legacy_unversioned_job_id:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure was not persisted")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
        typer.echo(json.dumps(payload))

    _run_or_exit(action)


@session_app.command("detach")
@_acceptance_report_command
def session_detach(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical cleanup validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Close the desktop attachment while retaining remote work and session processes."""
    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = _new_cleanup_acceptance_report(
        scenario="cleanup",
        cluster=cluster,
        mode="detach",
        resource_kind="owner_session",
        resource_id=session_id,
        action="detach",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=False,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    write_validation_report(seed_report, canonical_report_path)
    try:
        definition = _require_cluster(cluster)
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="cleanup",
            cluster=cluster,
            check_id="session.detach.preflight",
            summary="validate owned session detach inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
            partial_report=canonical_report[0],
        )
        raise

    def action() -> None:
        remote_execution = should_execute_on_cluster(definition)
        queue = _managed_queue_from_env()
        cleanup_worker_info, cleanup_worker_error = _observe_worker_before_cleanup(definition)
        pre_detach_report = detach_remote_session(
            definition=definition,
            session_id=session_id,
            cluster=cluster,
        )
        pre_detach_canonical = pre_detach_report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical_report[0] = pre_detach_canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        session_generation_id = _verified_owner_session_detach(
            pre_detach_report,
            session_id=session_id,
        )
        if remote_execution:
            owned_jobs = _list_remote_owned_active_cluster_jobs(
                definition,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
        else:
            owned_jobs = _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
            )
        gateway_reports = _cleanup_owned_runtime_sessions(
            cluster=cluster,
            definition=definition,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            mode="detach",
            cancel_scheduler_jobs=False,
        )
        if remote_execution:
            post_operation_jobs = _list_remote_owned_active_cluster_jobs(
                definition,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
        else:
            post_operation_jobs = _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
            )
        report = detach_remote_session(
            definition=definition,
            session_id=session_id,
            cluster=cluster,
        )
        try:
            _verified_owner_session_detach(
                report,
                session_id=session_id,
                expected_session_generation_id=session_generation_id,
            )
        except RelayError as exc:
            detail = str(exc)
            if detail not in report.errors:
                report.errors.append(detail)
        report.resources.extend(
            _owned_job_cleanup_resources(
                owned_jobs,
                definition=definition,
                location=definition.ssh_host,
                cancel_jobs=False,
                cancel_scheduler_jobs=False,
                post_operation_jobs=post_operation_jobs,
            )
        )
        _merge_gateway_cleanup_resources(report, gateway_reports)
        payload = report.json_payload()
        payload["gateway_sessions"] = gateway_reports
        canonical = report.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        provenance_warning = _write_cleanup_validation_report(
            canonical,
            definition,
            canonical_report_path,
            observed_worker_info=cleanup_worker_info,
            worker_observation_error=cleanup_worker_error,
        )
        payload["validation_report"] = str(canonical_report_path.resolve())
        payload["validation_status"] = canonical.status.value
        payload["validation_provenance_warning"] = provenance_warning
        typer.echo(_public_json(payload))
        canonical_ok = canonical.status is ValidationStatus.PASSED
        if payload.get("ok") is not True or (not canonical_ok and not provenance_warning):
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.detach",
                summary="detach owned desktop session resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    def locked_action() -> None:
        with (
            remote_command_timeout(REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS),
            _session_transition_lock(cluster=cluster, session_id=session_id),
        ):
            guarded_action()

    _run_or_exit(locked_action)


@session_app.command("teardown")
@_acceptance_report_command
def session_teardown(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    stop_worker: Annotated[
        bool,
        typer.Option(help="Also stop the persistent cluster worker service for this cluster."),
    ] = False,
    cancel_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-jobs/--keep-jobs",
            help="Cancel active relay jobs. The safe default leaves all jobs running.",
        ),
    ] = False,
    cancel_scheduler_jobs: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-jobs/--keep-scheduler-jobs",
            help="Also request scheduler cancellation for canceled relay jobs.",
        ),
    ] = False,
    preserve_scheduler_job_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--preserve-scheduler-job-id",
            help=(
                "Unrelated active scheduler job id that must remain uncanceled; repeat for "
                "multiple live-gate sentinels. Requires --cancel-jobs and "
                "--cancel-scheduler-jobs."
            ),
        ),
    ] = None,
    relay_cancel_timeout_seconds: Annotated[
        float,
        typer.Option(
            help="Maximum wait for worker-acknowledged relay cancellation cleanup.",
            min=0.01,
            max=MAX_RELAY_CANCEL_TIMEOUT_SECONDS,
        ),
    ] = DEFAULT_RELAY_CANCEL_TIMEOUT_SECONDS,
    relay_cancel_poll_seconds: Annotated[
        float,
        typer.Option(
            help="Polling interval while awaiting relay cancellation acknowledgment.",
            min=0.01,
            max=60.0,
        ),
    ] = DEFAULT_RELAY_CANCEL_POLL_SECONDS,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical cleanup validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop owned remote relay session processes, optionally stopping the worker service."""
    canonical_report_path = validation_report or default_report_path(cluster)
    evidence_lock: _CleanupEvidenceLock | None = None
    try:
        evidence_lock = _acquire_cleanup_evidence_lock()
        seed_report = _new_cleanup_acceptance_report(
            scenario="cleanup",
            cluster=cluster,
            mode="teardown",
            resource_kind="owner_session",
            resource_id=session_id,
            action="teardown",
            cancel_relay_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            stop_worker=stop_worker,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        canonical_report: list[LiveValidationReport | None] = [seed_report]
        write_validation_report(seed_report, canonical_report_path)
    except BaseException:
        _release_cleanup_evidence_lock(evidence_lock)
        raise
    active_evidence_lock = evidence_lock
    try:
        definition = _require_cluster(cluster)
        scheduler_sentinel_ids = _normalize_scheduler_sentinel_ids(preserve_scheduler_job_ids or [])
        if cancel_scheduler_jobs and not cancel_jobs:
            raise typer.BadParameter(
                "--cancel-scheduler-jobs requires the separate --cancel-jobs flag"
            )
        if scheduler_sentinel_ids and not (cancel_jobs and cancel_scheduler_jobs):
            raise typer.BadParameter(
                "--preserve-scheduler-job-id requires both --cancel-jobs and "
                "--cancel-scheduler-jobs"
            )
    except BaseException as exc:
        try:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.teardown.preflight",
                summary="validate owned session teardown inputs",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
        finally:
            _release_cleanup_evidence_lock(evidence_lock)
        raise

    def action() -> None:
        remote_execution = should_execute_on_cluster(definition)
        queue = _managed_queue_from_env()
        cleanup_worker_info, cleanup_worker_error = _observe_worker_before_cleanup(definition)

        def checkpoint_finalized_cleanup_artifact(
            report: SessionLifecycleReport,
            *,
            recovery: OwnedSessionRecoveryStatus,
            local_artifact: _LocalCleanupReportArtifact,
        ) -> None:
            """Durably reference exact local evidence before authoritative closure."""
            _verify_cleanup_evidence_lock(
                active_evidence_lock,
                expected_parent=_cleanup_evidence_state_parent(),
            )
            reference = recovery.coordinator_report_ref
            generation_id = report.session_generation_id
            operation_id = report.cleanup_operation_id
            if not (
                reference is not None
                and generation_id is not None
                and operation_id is not None
                and reference.sha256 == local_artifact.report_sha256
                and reference.size == local_artifact.report_size
            ):
                raise RelayError("cleanup report artifact does not match its finalized reference")
            report_metadata: dict[str, object] = {
                "sha256": reference.sha256,
                "size": reference.size,
                "local_manifest": str(local_artifact.manifest_path.resolve()),
                "local_manifest_sha256": local_artifact.manifest_sha256,
                "local_chunk_count": len(local_artifact.chunks),
                "session_generation_id": generation_id,
                "cleanup_operation_id": operation_id,
                "cleanup_policy": report.cleanup_policy,
            }
            pending = seed_report.model_copy(
                deep=True,
                update={
                    "checks": [],
                    "resources": [],
                    "artifacts": [],
                    "completed_at": None,
                    "status": ValidationStatus.FAILED,
                    "error": "authoritative owner-session closure pending",
                    "cleanup": CleanupEvidence(
                        requested=True,
                        mode="teardown",
                        operation_id=operation_id,
                        cancel_relay_jobs=report.cleanup_policy["cancel_jobs"],
                        cancel_scheduler_jobs=report.cleanup_policy["cancel_scheduler_jobs"],
                        stop_worker=report.cleanup_policy["stop_worker"],
                        actions=[
                            {
                                "kind": "cleanup_report",
                                "resource_id": reference.name,
                                "action": "verify",
                                "outcome": "verified",
                                "verified_after_operation": True,
                                "residual": False,
                            },
                            {
                                "kind": "owner_session",
                                "resource_id": f"{session_id}:{generation_id}",
                                "action": "close",
                                "outcome": "pending",
                                "verified_after_operation": False,
                                "residual": True,
                            },
                        ],
                        remaining_resources=[
                            ValidationResource(
                                kind="owner_session",
                                resource_id=f"{session_id}:{generation_id}",
                                role="cleanup_closure",
                                cluster=cluster,
                                state="pending",
                                metadata={"cleanup_operation_id": operation_id},
                            )
                        ],
                    ),
                },
            )
            recorder = ValidationRecorder(pending)
            manifest_evidence = EvidenceReference(
                kind="cleanup_report_manifest",
                reference=str(local_artifact.manifest_path.resolve()),
                sha256=local_artifact.manifest_sha256,
                metadata=report_metadata,
            )
            with recorder.check(
                "session.teardown.cleanup-report-retained",
                "retain exact finalized cleanup report before authoritative closure",
            ) as evidence:
                evidence.append(manifest_evidence)
            pending.artifacts.append(manifest_evidence)
            pending.artifacts.extend(
                EvidenceReference(
                    kind="cleanup_report_chunk",
                    reference=str(chunk.path.resolve()),
                    sha256=chunk.sha256,
                    metadata={"size": chunk.size},
                )
                for chunk in local_artifact.chunks
            )
            recorder.add_resource(
                ValidationResource(
                    kind="cleanup_report",
                    resource_id=reference.name,
                    role="finalized_cleanup_report",
                    cluster=cluster,
                    state="verified",
                    references=[str(local_artifact.manifest_path.resolve())],
                    metadata=report_metadata,
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="owner_session",
                    resource_id=f"{session_id}:{generation_id}",
                    role="cleanup_closure",
                    cluster=cluster,
                    state="pending",
                    metadata={"cleanup_operation_id": operation_id},
                )
            )
            write_validation_report(pending, canonical_report_path)
            _verify_cleanup_evidence_lock(
                active_evidence_lock,
                expected_parent=_cleanup_evidence_state_parent(),
            )
            reread = load_validation_report(canonical_report_path)
            _verify_cleanup_evidence_lock(
                active_evidence_lock,
                expected_parent=_cleanup_evidence_state_parent(),
            )
            expected_checkpoint = LiveValidationReport.model_validate(
                redact_sensitive_values(pending.model_dump(mode="json"))
            )
            if reread.model_dump(mode="json") != expected_checkpoint.model_dump(mode="json"):
                raise RelayError(
                    "cleanup report artifact checkpoint changed during durable re-read"
                )
            canonical_report[0] = reread

        def emit_completed_report(
            report: SessionLifecycleReport,
            *,
            canceled_job_ids: list[str],
            gateway_reports: list[dict[str, object]],
            recovery: OwnedSessionRecoveryStatus,
            local_artifact: _LocalCleanupReportArtifact,
            legacy_recovery: OwnedSessionRecoveryStatus | None,
        ) -> None:
            """Keep the bounded legacy projection, falling back to compact evidence."""
            generation_id = report.session_generation_id
            operation_id = report.cleanup_operation_id
            reference = recovery.coordinator_report_ref
            if generation_id is None or operation_id is None or reference is None:
                raise RelayError("finalized cleanup omitted its durable identity")
            projection = report.model_copy(deep=True)
            projection.resources.append(
                CleanupResource(
                    kind="owner_session",
                    resource_id=f"{session_id}:{generation_id}",
                    location=definition.ssh_host if remote_execution else str(queue.root),
                    action="close",
                    ownership_verified=True,
                    outcome="closed",
                    verified_after_operation=True,
                    metadata={
                        "session_generation_id": generation_id,
                        "cleanup_operation_id": operation_id,
                        "cleanup_policy": report.cleanup_policy,
                        "covered_legacy_job_ids": [],
                    },
                )
            )
            payload = projection.json_payload()
            payload["cleanup_evidence"] = projection.to_cleanup_evidence(
                stop_worker=stop_worker
            ).model_dump(mode="json")
            payload["relay_jobs"] = {
                "cancel_requested": cancel_jobs,
                "scheduler_cancel_requested": cancel_jobs and cancel_scheduler_jobs,
                "canceled_job_ids": canceled_job_ids,
            }
            payload["gateway_sessions"] = gateway_reports
            if legacy_recovery is not None:
                payload["recovery_evidence"] = legacy_recovery.model_dump(mode="json")
            payload.update(
                {
                    "validation_report": str(canonical_report_path.resolve()),
                    "validation_status": ValidationStatus.PASSED.value,
                    "validation_provenance_warning": False,
                }
            )
            preliminary = _bounded_cleanup_public_json(payload)
            if preliminary is not None:
                canonical = projection.to_live_validation_report(
                    stop_worker=stop_worker,
                    cancel_jobs=cancel_jobs,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                ).model_copy(
                    update={
                        "report_id": seed_report.report_id,
                        "started_at": seed_report.started_at,
                    }
                )
                report_metadata: dict[str, object] = {
                    "sha256": reference.sha256,
                    "size": reference.size,
                    "local_manifest": str(local_artifact.manifest_path.resolve()),
                    "local_manifest_sha256": local_artifact.manifest_sha256,
                    "local_chunk_count": len(local_artifact.chunks),
                    "session_generation_id": generation_id,
                    "cleanup_operation_id": operation_id,
                    "cleanup_policy": report.cleanup_policy,
                }
                manifest_evidence = EvidenceReference(
                    kind="cleanup_report_manifest",
                    reference=str(local_artifact.manifest_path.resolve()),
                    sha256=local_artifact.manifest_sha256,
                    metadata=report_metadata,
                )
                canonical.artifacts.append(manifest_evidence)
                canonical.artifacts.extend(
                    EvidenceReference(
                        kind="cleanup_report_chunk",
                        reference=str(chunk.path.resolve()),
                        sha256=chunk.sha256,
                        metadata={"size": chunk.size},
                    )
                    for chunk in local_artifact.chunks
                )
                canonical.resources.append(
                    ValidationResource(
                        kind="cleanup_report",
                        resource_id=reference.name,
                        role="finalized_cleanup_report",
                        cluster=cluster,
                        state="verified",
                        references=[str(local_artifact.manifest_path.resolve())],
                        metadata=report_metadata,
                    )
                )
                projected_validation = (
                    json.dumps(
                        redact_sensitive_values(canonical.model_dump(mode="json")),
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n"
                )
                if len(projected_validation.encode("utf-8")) < MAX_CLEANUP_VALIDATION_REPORT_BYTES:
                    canonical_report[0] = canonical
                    provenance_warning = _write_cleanup_validation_report(
                        canonical,
                        definition,
                        canonical_report_path,
                        observed_worker_info=cleanup_worker_info,
                        worker_observation_error=cleanup_worker_error,
                    )
                    payload["validation_status"] = canonical.status.value
                    payload["validation_provenance_warning"] = provenance_warning
                    serialized = _bounded_cleanup_public_json(payload)
                    if (
                        serialized is not None
                        and canonical_report_path.stat().st_size
                        < MAX_CLEANUP_VALIDATION_REPORT_BYTES
                    ):
                        typer.echo(serialized)
                        canonical_ok = canonical.status is ValidationStatus.PASSED
                        if payload.get("ok") is not True or (
                            not canonical_ok and not provenance_warning
                        ):
                            raise typer.Exit(code=1)
                        return
            emit_finalized_retry_report(
                report,
                recovery=recovery,
                local_artifact=local_artifact,
                retry=False,
            )

        def emit_finalized_retry_report(
            report: SessionLifecycleReport,
            *,
            recovery: OwnedSessionRecoveryStatus,
            local_artifact: _LocalCleanupReportArtifact,
            retry: bool = True,
        ) -> None:
            """Emit bounded evidence for a finalized report without re-inlining it."""
            reference = recovery.coordinator_report_ref
            generation_id = report.session_generation_id
            operation_id = report.cleanup_operation_id
            admission = recovery.admission_status
            if not (
                reference is not None
                and generation_id is not None
                and operation_id is not None
                and isinstance(admission, dict)
                and admission.get("closed") is True
                and recovery.process_state == "already_closed"
            ):
                raise RelayError("finalized cleanup retry omitted authoritative closure evidence")

            resource_summary: dict[str, object] = {
                "total": len(report.resources),
                "by_kind": dict(sorted(Counter(item.kind for item in report.resources).items())),
                "by_action": dict(
                    sorted(Counter(item.action for item in report.resources).items())
                ),
                "by_outcome": dict(
                    sorted(Counter(item.outcome for item in report.resources).items())
                ),
                "residual_count": len(report.residual_resources),
                "error_count": len(report.errors),
            }
            canceled_relay_count = sum(
                1
                for resource in report.resources
                if resource.kind == "relay_job"
                and resource.action == "cancel"
                and resource.outcome == "canceled"
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            )
            gateway_resource_count = sum(
                1
                for resource in report.resources
                if resource.kind in {"desktop_connector", "remote_connector", "gateway_record"}
            )
            report_metadata: dict[str, object] = {
                "sha256": reference.sha256,
                "size": reference.size,
                "local_manifest": str(local_artifact.manifest_path.resolve()),
                "local_manifest_sha256": local_artifact.manifest_sha256,
                "local_chunk_count": len(local_artifact.chunks),
                "session_generation_id": generation_id,
                "cleanup_operation_id": operation_id,
                "cleanup_policy": report.cleanup_policy,
                "resource_summary": resource_summary,
            }
            canonical = seed_report.model_copy(
                deep=True,
                update={
                    "checks": [],
                    "resources": [],
                    "artifacts": [],
                    "completed_at": None,
                    "status": ValidationStatus.FAILED,
                    "error": None,
                    "cleanup": CleanupEvidence(
                        requested=True,
                        mode="teardown",
                        operation_id=operation_id,
                        cancel_relay_jobs=report.cleanup_policy["cancel_jobs"],
                        cancel_scheduler_jobs=report.cleanup_policy["cancel_scheduler_jobs"],
                        stop_worker=report.cleanup_policy["stop_worker"],
                        actions=[
                            {
                                "kind": "cleanup_report",
                                "resource_id": reference.name,
                                "action": "verify",
                                "outcome": "verified",
                                "verified_after_operation": True,
                                "residual": False,
                            },
                            {
                                "kind": "owner_session",
                                "resource_id": f"{session_id}:{generation_id}",
                                "action": "close",
                                "outcome": "closed",
                                "verified_after_operation": True,
                                "residual": False,
                            },
                        ],
                        remaining_resources=[],
                    ),
                },
            )
            recorder = ValidationRecorder(canonical)
            manifest_evidence = EvidenceReference(
                kind="cleanup_report_manifest",
                reference=str(local_artifact.manifest_path.resolve()),
                sha256=local_artifact.manifest_sha256,
                metadata=report_metadata,
            )
            with recorder.check(
                ("session.teardown.finalized-retry" if retry else "session.teardown.finalized"),
                "verify finalized cleanup report and authoritative session closure",
            ) as evidence:
                evidence.append(manifest_evidence)
            canonical.artifacts.append(manifest_evidence)
            canonical.artifacts.extend(
                EvidenceReference(
                    kind="cleanup_report_chunk",
                    reference=str(chunk.path.resolve()),
                    sha256=chunk.sha256,
                    metadata={"size": chunk.size},
                )
                for chunk in local_artifact.chunks
            )
            recorder.add_resource(
                ValidationResource(
                    kind="cleanup_report",
                    resource_id=reference.name,
                    role="finalized_cleanup_report",
                    cluster=cluster,
                    state="verified",
                    references=[str(local_artifact.manifest_path.resolve())],
                    metadata=report_metadata,
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="owner_session",
                    resource_id=f"{session_id}:{generation_id}",
                    role="cleanup_closure",
                    cluster=cluster,
                    state="closed",
                    metadata={
                        "cleanup_operation_id": operation_id,
                        "coordinator_report_sha256": reference.sha256,
                    },
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="owner_session_recovery",
                    resource_id=f"{session_id}:{generation_id}",
                    role="post_cleanup_recovery",
                    cluster=cluster,
                    state="verified",
                    metadata={
                        "process_state": recovery.process_state,
                        "cleanup_receipt": recovery.cleanup_receipt,
                        "cleanup_paths_pending": recovery.cleanup_paths_pending,
                        "coordinator_report_bound": recovery.coordinator_report_bound,
                        "ownership_verified": recovery.ownership_verified,
                        "recovery_verified": recovery.recovery_verified,
                        "closed": True,
                    },
                )
            )
            recorder.finish()
            canonical_report[0] = canonical
            provenance_warning = _write_cleanup_validation_report(
                canonical,
                definition,
                canonical_report_path,
                observed_worker_info=cleanup_worker_info,
                worker_observation_error=cleanup_worker_error,
            )
            if canonical_report_path.stat().st_size >= MAX_CLEANUP_VALIDATION_REPORT_BYTES:
                raise RelayError("finalized cleanup validation report exceeded its byte limit")
            payload: dict[str, object] = {
                "schema_version": (
                    "clio-relay.finalized-cleanup-retry.v1"
                    if retry
                    else "clio-relay.finalized-cleanup.v1"
                ),
                "cluster": cluster,
                "session_id": session_id,
                "session_generation_id": generation_id,
                "mode": report.mode,
                "cleanup_operation_id": operation_id,
                "cleanup_policy": report.cleanup_policy,
                "coordinator_report_ref": reference.model_dump(mode="json"),
                "coordinator_report_sha256": reference.sha256,
                "report_inline": False,
                "cleanup_report_artifact": {
                    "manifest": str(local_artifact.manifest_path.resolve()),
                    "manifest_sha256": local_artifact.manifest_sha256,
                    "report_sha256": local_artifact.report_sha256,
                    "report_size": local_artifact.report_size,
                    "chunk_count": len(local_artifact.chunks),
                    "chunk_size_limit": MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES,
                },
                "resource_summary": resource_summary,
                "relay_jobs": {
                    "cancel_requested": report.cleanup_policy["cancel_jobs"],
                    "scheduler_cancel_requested": report.cleanup_policy["cancel_scheduler_jobs"],
                    "canceled_count": canceled_relay_count,
                },
                "gateway_sessions": {"resource_count": gateway_resource_count},
                "authoritative_closure": True,
                "recovery_evidence": {
                    "process_state": recovery.process_state,
                    "closed": True,
                    "cleanup_receipt": recovery.cleanup_receipt,
                    "cleanup_paths_pending": recovery.cleanup_paths_pending,
                    "coordinator_report_bound": recovery.coordinator_report_bound,
                    "coordinator_report_ref": reference.model_dump(mode="json"),
                    "ownership_verified": recovery.ownership_verified,
                    "recovery_verified": recovery.recovery_verified,
                },
                "validation_report": str(canonical_report_path.resolve()),
                "validation_status": canonical.status.value,
                "validation_provenance_warning": provenance_warning,
                "ok": True,
            }
            serialized = _bounded_cleanup_public_json(payload)
            if serialized is None:
                raise RelayError("finalized cleanup output exceeded its byte limit")
            typer.echo(serialized)
            if canonical.status is not ValidationStatus.PASSED and not provenance_warning:
                raise typer.Exit(code=1)

        initial_status_error: str | None = None
        try:
            pre_teardown_status = status_remote_session(
                definition=definition,
                session_id=session_id,
            )
        except (JSONDecodeError, RelayError) as exc:
            initial_status_error = f"{type(exc).__name__}: {exc}"
            pre_teardown_status = {}
        recovery_status: OwnedSessionRecoveryStatus | None = None
        recovery_resource: ValidationResource | None = None
        try:
            session_generation_id = _verified_owner_session_generation(
                pre_teardown_status,
                session_id=session_id,
            )
        except RelayError:
            session_generation_id = ""
        if not session_generation_id or pre_teardown_status.get("running") is not True:
            recovery_status = _owned_session_recovery_status(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                cluster=cluster,
                session_id=session_id,
            )
            recovery_resource = _owner_session_recovery_validation_resource(recovery_status)
            if initial_status_error is not None:
                recovery_resource.metadata["initial_status_error"] = initial_status_error
            seed_report.resources.append(recovery_resource)
            canonical_report[0] = seed_report
            write_validation_report(seed_report, canonical_report_path)
            session_generation_id = _verified_recovered_owner_session_generation(
                recovery_status,
                cluster=cluster,
                session_id=session_id,
            )
            pre_teardown_status = {
                "owner": recovery_status.owner,
                "session_id": recovery_status.session_id,
                "session_generation_id": recovery_status.session_generation_id,
                "api_pid": recovery_status.api_pid,
                "process_start_ticks": recovery_status.process_start_marker,
                "running": recovery_status.running,
                "ownership_verified": recovery_status.ownership_verified,
                "process_absence_verified": recovery_status.process_absence_verified,
                "process_state": recovery_status.process_state,
            }
        requested_policy = {
            "stop_worker": stop_worker,
            "cancel_jobs": cancel_jobs,
            "cancel_scheduler_jobs": cancel_scheduler_jobs,
        }
        finalized_retry_report: SessionLifecycleReport | None = None
        finalized_retry_reference = None
        if (
            recovery_status is not None
            and recovery_status.cleanup_receipt
            and recovery_status.coordinator_report_bound
        ):
            retrieved_report = read_remote_session_cleanup_report(
                definition=definition,
                cluster=cluster,
                session_id=session_id,
                status=recovery_status,
            )
            finalized_retry_report = _verified_finalized_cleanup_report(
                recovery_status,
                report=retrieved_report,
                cluster=cluster,
                session_id=session_id,
                expected_generation_id=session_generation_id,
                expected_cleanup_policy=requested_policy,
            )
            finalized_retry_reference = recovery_status.coordinator_report_ref
        local_admission_session_id = _desktop_owner_session_admission_id(
            cluster=cluster,
            session_id=session_id,
        )
        if remote_execution:
            _assert_no_unscoped_desktop_admission_state(
                queue,
                cluster=cluster,
                session_id=session_id,
                session_generation_id=session_generation_id,
            )
        authoritative_admission = _owner_session_admission_status(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            session_generation_id=session_generation_id,
        )
        local_cleanup_intent = queue.get_owner_session_cleanup_intent(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
        cleanup_operation_id = _select_owner_session_cleanup_operation(
            authoritative_status=authoritative_admission,
            local_intent=local_cleanup_intent,
            session_id=session_id,
            session_generation_id=session_generation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        if finalized_retry_report is not None:
            if recovery_status is None or finalized_retry_reference is None:
                raise RelayError("finalized cleanup retry lost its exact report reference")
            finalized_retry_report = _verified_finalized_cleanup_report(
                recovery_status,
                report=finalized_retry_report,
                cluster=cluster,
                session_id=session_id,
                expected_generation_id=session_generation_id,
                expected_cleanup_operation_id=cleanup_operation_id,
                expected_cleanup_policy=requested_policy,
            )
            local_cleanup_artifact = _persist_local_cleanup_report_artifact(
                finalized_retry_report,
                validation_report_path=canonical_report_path,
                evidence_lock=active_evidence_lock,
            )
            checkpoint_finalized_cleanup_artifact(
                finalized_retry_report,
                recovery=recovery_status,
                local_artifact=local_cleanup_artifact,
            )
            _verify_cleanup_evidence_lock(
                active_evidence_lock,
                expected_parent=_cleanup_evidence_state_parent(),
            )
            _mark_owner_session_closed(
                queue=queue,
                definition=definition,
                cluster=cluster,
                remote_execution=remote_execution,
                session_id=session_id,
                local_admission_session_id=local_admission_session_id,
                session_generation_id=session_generation_id,
                legacy_unversioned_job_ids=[],
                finalized_recovery=recovery_status,
                finalized_report=finalized_retry_report,
            )
            closed_recovery = _owned_session_recovery_status(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                cluster=cluster,
                session_id=session_id,
            )
            if not (
                closed_recovery.recovery_verified
                and closed_recovery.cleanup_receipt
                and closed_recovery.cleanup_paths_pending is False
                and closed_recovery.coordinator_report_bound
                and closed_recovery.session_generation_id == session_generation_id
                and closed_recovery.process_state == "already_closed"
                and isinstance(closed_recovery.admission_status, dict)
                and closed_recovery.admission_status.get("closed") is True
            ):
                raise RelayError(
                    "finalized cleanup retry was not authoritatively closed after commit"
                )
            if closed_recovery.coordinator_report_ref != finalized_retry_reference:
                raise RelayError("finalized cleanup report reference changed during closure")
            closed_report = _verified_finalized_cleanup_report(
                closed_recovery,
                report=finalized_retry_report,
                cluster=cluster,
                session_id=session_id,
                expected_generation_id=session_generation_id,
                expected_cleanup_operation_id=cleanup_operation_id,
                expected_cleanup_policy=requested_policy,
            )
            if session_lifecycle_report_sha256(closed_report) != session_lifecycle_report_sha256(
                finalized_retry_report
            ):
                raise RelayError("finalized cleanup report reference changed during closure")
            recovery_status = closed_recovery
            recovery_resource = _owner_session_recovery_validation_resource(closed_recovery)
            emit_finalized_retry_report(
                finalized_retry_report,
                recovery=recovery_status,
                local_artifact=local_cleanup_artifact,
            )
            return
        partial = seed_report
        partial.cleanup = CleanupEvidence(
            requested=True,
            mode="teardown",
            operation_id=cleanup_operation_id,
            cancel_relay_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_jobs and cancel_scheduler_jobs,
            stop_worker=stop_worker,
            actions=[
                {
                    "kind": "owner_session_admission",
                    "resource_id": f"{session_id}:{session_generation_id}",
                    "action": "quiesce",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                },
                {
                    "kind": "remote_relay_api",
                    "resource_id": session_id,
                    "action": "stop",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                },
            ],
        )
        admission_resource = ValidationResource(
            kind="owner_session_admission",
            resource_id=f"{session_id}:{session_generation_id}",
            role="cleanup_admission",
            cluster=cluster,
            state="pending",
            metadata={
                "operation_id": cleanup_operation_id,
                "local_admission_session_id": local_admission_session_id,
                "remote_execution": remote_execution,
            },
        )
        api_resource = ValidationResource(
            kind="remote_relay_api",
            resource_id=session_id,
            role="cleanup_target",
            cluster=cluster,
            state="running" if pre_teardown_status.get("running") is True else "stopped",
            metadata={
                "session_generation_id": session_generation_id,
                "ownership_verified": pre_teardown_status.get("ownership_verified") is True,
                "cleanup_operation_id": cleanup_operation_id,
            },
        )
        admission_resource_index = len(partial.resources)
        partial.resources.extend([admission_resource, api_resource])
        partial.cleanup.remaining_resources.extend([admission_resource, api_resource])
        canonical_report[0] = partial
        write_validation_report(partial, canonical_report_path)
        cleanup_intent = _quiesce_owner_session_intake(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            session_id=session_id,
            local_admission_session_id=local_admission_session_id,
            session_generation_id=session_generation_id,
            cleanup_operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        partial.resources[admission_resource_index] = partial.resources[
            admission_resource_index
        ].model_copy(update={"state": "quiesced"})
        partial.cleanup.remaining_resources[0] = partial.resources[admission_resource_index]
        partial.cleanup.actions[0].update(
            {
                "outcome": "quiesced",
                "verified_after_operation": True,
            }
        )
        write_validation_report(partial, canonical_report_path)

        def list_owned_jobs(*, include_terminal: bool = False) -> list[_OwnedRelayJob]:
            if remote_execution:
                return _list_remote_owned_active_cluster_jobs(
                    definition,
                    cluster,
                    owner_session_id=session_id,
                    owner_session_generation_id=session_generation_id,
                    include_terminal=include_terminal,
                )
            return _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
                scheduler_provider=definition.scheduler_provider,
                include_terminal=include_terminal,
            )

        def list_legacy_jobs() -> list[_OwnedRelayJob]:
            """Discover unversioned records without treating them as this generation's jobs."""
            if remote_execution:
                return _list_remote_owned_active_cluster_jobs(
                    definition,
                    cluster,
                    owner_session_id=session_id,
                    owner_session_generation_id=None,
                    include_terminal=True,
                )
            return _list_owned_active_cluster_jobs(
                queue,
                cluster,
                owner_session_id=session_id,
                owner_session_generation_id=None,
                scheduler_provider=definition.scheduler_provider,
                include_terminal=True,
            )

        def read_owned_job(job_id: str) -> _OwnedRelayJob:
            return _read_owned_relay_job(
                queue=queue,
                definition=definition,
                remote_execution=remote_execution,
                cluster=cluster,
                job_id=job_id,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )

        legacy_jobs = list_legacy_jobs()
        if legacy_jobs:
            for legacy_job in legacy_jobs:
                resource = ValidationResource(
                    kind="relay_job",
                    resource_id=legacy_job.job_id,
                    role="ambiguous_legacy_owner_session",
                    cluster=cluster,
                    state=legacy_job.relay_state.value,
                    provider=legacy_job.scheduler_provider,
                    metadata={
                        "ownership_verified": False,
                        "expected_owner_session_generation_id": session_generation_id,
                        "observed_owner_session_generation_id": None,
                        "mutation_refused": True,
                    },
                )
                partial.resources.append(resource)
                partial.cleanup.remaining_resources.append(resource)
            write_validation_report(partial, canonical_report_path)
            raise RelayError(
                "owner-session cleanup found unversioned legacy jobs whose generation cannot be "
                "proven; no relay or scheduler cancellation was attempted: "
                + ", ".join(sorted(job.job_id for job in legacy_jobs))
            )

        owned_jobs = list_owned_jobs()
        if cancel_jobs:
            for job in owned_jobs:
                resource = ValidationResource(
                    kind="relay_job",
                    resource_id=job.job_id,
                    role="cleanup_cancel_target",
                    cluster=cluster,
                    state=job.relay_state.value,
                    provider=job.scheduler_provider,
                    metadata={
                        "action": "cancel",
                        "ownership_verified": True,
                        "owner_session_generation_id": session_generation_id,
                        "cleanup_operation_id": cleanup_operation_id,
                    },
                )
                partial.resources.append(resource)
                partial.cleanup.remaining_resources.append(resource)
                partial.cleanup.actions.append(
                    {
                        "kind": "relay_job",
                        "resource_id": job.job_id,
                        "action": "cancel",
                        "outcome": "pending",
                        "verified_after_operation": False,
                        "residual": True,
                    }
                )
            write_validation_report(partial, canonical_report_path)
        gateway_scheduler_job_ids = (
            _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        for scheduler_job_id in gateway_scheduler_job_ids:
            scheduler_resource = ValidationResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                role="gateway_cleanup_target",
                cluster=cluster,
                state="discovered",
                provider=definition.scheduler_provider,
                metadata={
                    "action": "cancel" if cancel_scheduler_jobs else "retain",
                    "ownership_verified": True,
                    "owner_session_generation_id": session_generation_id,
                    "cleanup_operation_id": cleanup_operation_id,
                },
            )
            partial.resources.append(scheduler_resource)
            partial.cleanup.remaining_resources.append(scheduler_resource)
            partial.cleanup.actions.append(
                {
                    "kind": "scheduler_job",
                    "resource_id": scheduler_job_id,
                    "action": "cancel" if cancel_scheduler_jobs else "retain",
                    "outcome": "pending",
                    "verified_after_operation": False,
                    "residual": True,
                    "source": "gateway",
                }
            )
        if gateway_scheduler_job_ids:
            write_validation_report(partial, canonical_report_path)
        scheduler_sentinel_pre_phases = _preflight_scheduler_sentinels(
            definition,
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )
        canceled: list[str] = []
        if cancel_jobs:
            try:
                cancellation_targets = (
                    _cancel_remote_owned_jobs(definition, cluster, owned_jobs)
                    if remote_execution
                    else _cancel_local_owned_jobs(queue, owned_jobs)
                )
                canceled.extend(
                    _wait_for_owned_relay_cancellations(
                        cancellation_targets,
                        read_owned_job=read_owned_job,
                        timeout_seconds=relay_cancel_timeout_seconds,
                        poll_seconds=relay_cancel_poll_seconds,
                    )
                )
            except BaseException as exc:
                for action_evidence in partial.cleanup.actions:
                    if action_evidence.get("kind") == "relay_job":
                        action_evidence.update(
                            {
                                "outcome": "failed",
                                "verified_after_operation": False,
                                "residual": True,
                                "detail": str(exc),
                            }
                        )
                write_validation_report(partial, canonical_report_path)
                raise
            canceled_ids = set(canceled)
            for index, resource in enumerate(partial.resources):
                if resource.kind == "relay_job" and resource.resource_id in canceled_ids:
                    partial.resources[index] = resource.model_copy(update={"state": "canceled"})
            partial.cleanup.remaining_resources = [
                resource
                for resource in partial.cleanup.remaining_resources
                if not (resource.kind == "relay_job" and resource.resource_id in canceled_ids)
            ]
            for action_evidence in partial.cleanup.actions:
                if (
                    action_evidence.get("kind") == "relay_job"
                    and action_evidence.get("resource_id") in canceled_ids
                ):
                    action_evidence.update(
                        {
                            "outcome": "canceled",
                            "verified_after_operation": True,
                            "residual": False,
                        }
                    )
            write_validation_report(partial, canonical_report_path)
        gateway_reports = _cleanup_owned_runtime_sessions(
            cluster=cluster,
            definition=definition,
            owner_session_id=session_id,
            owner_session_generation_id=session_generation_id,
            mode="teardown",
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            scheduler_sentinel_ids=scheduler_sentinel_ids,
            owned_jobs=owned_jobs,
        )
        report = teardown_remote_session(
            definition=definition,
            session_id=session_id,
            expected_session_generation_id=session_generation_id,
            expected_cleanup_operation_id=cast(str, cleanup_intent["operation_id"]),
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            cluster=cluster,
        )
        report.cleanup_operation_id = cast(str, cleanup_intent["operation_id"])
        report.cleanup_policy = {
            key: cast(bool, cleanup_intent[key])
            for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        report.relay_cancel_requested = cancel_jobs
        report.scheduler_cancel_requested = cancel_jobs and cancel_scheduler_jobs
        partial = report.to_live_validation_report(
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        partial = partial.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        if recovery_resource is not None:
            partial.resources.append(recovery_resource)
        canonical_report[0] = partial
        post_api_jobs = list_owned_jobs(include_terminal=True)
        initial_job_ids = {job.job_id for job in owned_jobs}
        late_jobs = [job for job in post_api_jobs if job.job_id not in initial_job_ids]
        if cancel_jobs and late_jobs:
            late_targets = (
                _cancel_remote_owned_jobs(definition, cluster, late_jobs)
                if remote_execution
                else _cancel_local_owned_jobs(queue, late_jobs)
            )
            canceled.extend(
                _wait_for_owned_relay_cancellations(
                    late_targets,
                    read_owned_job=read_owned_job,
                    timeout_seconds=relay_cancel_timeout_seconds,
                    poll_seconds=relay_cancel_poll_seconds,
                )
            )
            owned_jobs.extend(late_jobs)

        gateway_scheduler_job_ids = (
            _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        _assert_scheduler_sentinels_unrelated(
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )

        scheduler_jobs = list_owned_jobs(include_terminal=True)
        by_job_id: dict[str, _OwnedRelayJob] = {}
        for job in [*owned_jobs, *scheduler_jobs]:
            by_job_id.setdefault(job.job_id, job)
        owned_jobs = list(by_job_id.values())
        gateway_scheduler_job_ids = (
            _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=session_id,
                owner_session_generation_id=session_generation_id,
            )
            if scheduler_sentinel_ids
            else ()
        )
        _assert_scheduler_sentinels_unrelated(
            scheduler_sentinel_ids,
            owned_jobs,
            gateway_scheduler_job_ids=gateway_scheduler_job_ids,
        )
        report.resources.extend(
            _owned_job_cleanup_resources(
                owned_jobs,
                definition=definition,
                location=definition.ssh_host,
                cancel_jobs=cancel_jobs,
                cancel_scheduler_jobs=cancel_scheduler_jobs,
                post_operation_jobs=scheduler_jobs,
            )
        )
        if cancel_jobs and cancel_scheduler_jobs:
            scheduler_resources, scheduler_errors = _cancel_owned_scheduler_jobs(
                definition,
                owned_jobs,
            )
            report.resources.extend(scheduler_resources)
            report.errors.extend(scheduler_errors)
        sentinel_resources, sentinel_errors = _scheduler_sentinel_preservation_resources(
            definition,
            scheduler_sentinel_pre_phases,
        )
        report.resources.extend(sentinel_resources)
        report.errors.extend(sentinel_errors)
        final_jobs = list_owned_jobs(include_terminal=True)
        if cancel_jobs:
            uncanceled = [
                job.job_id
                for job in final_jobs
                if job.relay_state in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
                or (
                    job.relay_state is JobState.CANCELED and not job.relay_cancellation_acknowledged
                )
            ]
            if uncanceled:
                report.errors.append(
                    "owned relay jobs remained active after final rescan: "
                    + ", ".join(sorted(uncanceled))
                )
        _merge_gateway_cleanup_resources(report, gateway_reports)
        _verify_owner_session_teardown(
            report,
            session_id=session_id,
            session_generation_id=session_generation_id,
            stop_worker=stop_worker,
        )
        report, finalized_recovery = _persist_verified_cleanup_report_before_closure(
            definition=definition,
            cluster=cluster,
            session_id=session_id,
            session_generation_id=session_generation_id,
            report=report,
        )
        finalized_reference = finalized_recovery.coordinator_report_ref
        if finalized_reference is None:
            raise RelayError("finalized cleanup omitted its exact report reference")
        local_cleanup_artifact = _persist_local_cleanup_report_artifact(
            report,
            validation_report_path=canonical_report_path,
            evidence_lock=active_evidence_lock,
        )
        checkpoint_finalized_cleanup_artifact(
            report,
            recovery=finalized_recovery,
            local_artifact=local_cleanup_artifact,
        )
        _verify_cleanup_evidence_lock(
            active_evidence_lock,
            expected_parent=_cleanup_evidence_state_parent(),
        )
        legacy_recovery = recovery_status
        legacy_unversioned_job_ids: list[str] = []
        _mark_owner_session_closed(
            queue=queue,
            definition=definition,
            cluster=cluster,
            remote_execution=remote_execution,
            session_id=session_id,
            local_admission_session_id=local_admission_session_id,
            session_generation_id=session_generation_id,
            legacy_unversioned_job_ids=legacy_unversioned_job_ids,
            finalized_recovery=finalized_recovery,
            finalized_report=report,
        )
        closed_recovery = _owned_session_recovery_status(
            queue=queue,
            definition=definition,
            remote_execution=remote_execution,
            cluster=cluster,
            session_id=session_id,
        )
        if not (
            closed_recovery.recovery_verified
            and closed_recovery.cleanup_receipt
            and closed_recovery.cleanup_paths_pending is False
            and closed_recovery.coordinator_report_bound
            and closed_recovery.session_generation_id == session_generation_id
            and closed_recovery.process_state == "already_closed"
            and isinstance(closed_recovery.admission_status, dict)
            and closed_recovery.admission_status.get("closed") is True
            and closed_recovery.coordinator_report_ref == finalized_reference
        ):
            raise RelayError("cleanup was not authoritatively closed after commit")
        closed_report = _verified_finalized_cleanup_report(
            closed_recovery,
            report=report,
            cluster=cluster,
            session_id=session_id,
            expected_generation_id=session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
            expected_cleanup_policy=requested_policy,
        )
        if session_lifecycle_report_sha256(closed_report) != local_cleanup_artifact.report_sha256:
            raise RelayError("finalized cleanup report changed during authoritative closure")
        recovery_status = closed_recovery
        recovery_resource = _owner_session_recovery_validation_resource(closed_recovery)
        emit_completed_report(
            report,
            canceled_job_ids=canceled,
            gateway_reports=gateway_reports,
            recovery=closed_recovery,
            local_artifact=local_cleanup_artifact,
            legacy_recovery=legacy_recovery,
        )

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="cleanup",
                cluster=cluster,
                check_id="session.teardown",
                summary="teardown owned desktop session resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    def locked_action() -> None:
        with (
            remote_command_timeout(REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS),
            _session_transition_lock(cluster=cluster, session_id=session_id),
        ):
            guarded_action()

    try:
        _run_or_exit(locked_action)
    finally:
        _release_cleanup_evidence_lock(evidence_lock)


@app.command("install-frp")
def install_frp(
    destination: Annotated[
        Path,
        typer.Option(help="Directory for frpc/frps binaries."),
    ] = Path(".tools/frp/bin"),
) -> None:
    """Download and install frp for the local desktop."""
    _run_or_exit(lambda: typer.echo(f"frpc={install_local_frp(destination)}"))


@job_app.command("submit")
def job_submit(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    jarvis_yaml: Annotated[Path, typer.Option(help="Path to JARVIS YAML.")],
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
    exclusive: Annotated[
        bool,
        typer.Option("--exclusive/--shared", help="Request exclusive scheduler allocation."),
    ] = False,
) -> None:
    """Submit a JARVIS pipeline job."""
    definition = _require_cluster(cluster)
    yaml_text = jarvis_yaml.read_text(encoding="utf-8")
    if exclusive:
        yaml_text = _with_exclusive_scheduler(yaml_text, definition.scheduler_provider)
    artifact_uses = _artifact_use_refs(used_artifact)
    key = idempotency_key or (
        _file_idempotency_key(jarvis_yaml, yaml_text)
        + _artifact_use_idempotency_suffix(artifact_uses)
    )
    if should_execute_on_cluster(definition):
        remote_yaml = stage_jarvis_yaml(
            definition,
            jarvis_yaml=jarvis_yaml,
            pipeline_yaml_text=yaml_text,
            idempotency_key=key,
        )
        remote_command = [
            "job",
            "submit",
            "--cluster",
            cluster,
            "--jarvis-yaml",
            remote_yaml,
            "--idempotency-key",
            key,
            "--exclusive" if exclusive else "--shared",
        ]
        for ref in _artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", _artifact_use_cli_value(ref)])
        _run_remote_or_exit(
            definition,
            remote_command,
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_yaml=yaml_text),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@job_app.command("submit-pipeline")
def job_submit_pipeline(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    pipeline_name: Annotated[str, typer.Option(help="Existing JARVIS pipeline name.")],
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
) -> None:
    """Submit an existing JARVIS pipeline by name on the target cluster."""
    definition = _require_cluster(cluster)
    artifact_uses = _artifact_use_refs(used_artifact)
    key = idempotency_key or (
        f"jarvis-pipeline:{cluster}:{pipeline_name}"
        + _artifact_use_idempotency_suffix(artifact_uses)
    )
    if should_execute_on_cluster(definition):
        remote_command = [
            "job",
            "submit-pipeline",
            "--cluster",
            cluster,
            "--pipeline-name",
            pipeline_name,
            "--idempotency-key",
            key,
        ]
        for ref in _artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", _artifact_use_cli_value(ref)])
        _run_remote_or_exit(
            definition,
            remote_command,
        )
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(pipeline_name=pipeline_name),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@job_app.command("watch")
def job_watch(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job events from a cursor."""
    cursor = _job_event_cursor(cursor)
    if _try_remote_cluster_passthrough(
        cluster,
        ["job", "watch", job_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    events, next_cursor = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
    for event in events:
        typer.echo(f"{event.seq} {event.created_at.isoformat()} {event.event_type} {event.message}")
    typer.echo(f"next_cursor={next_cursor.next_seq}")


@job_app.command("monitor")
def job_monitor(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[int, typer.Option(help="First event sequence to read.")] = 1,
    limit: Annotated[int, typer.Option(help="Maximum events to read.")] = 100,
) -> None:
    """Read job state and event stream data from a cursor as JSON."""
    cursor = _job_event_cursor(cursor)
    if _try_remote_cluster_passthrough(
        cluster,
        ["job", "monitor", job_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    result = monitor_job(
        ClioCoreQueue(RelaySettings.from_env().core_dir),
        job_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(json.dumps(result, indent=2))


@job_app.command("status")
def job_status(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read job, relay queue, and scheduler status as JSON."""
    if _try_remote_cluster_passthrough(cluster, ["job", "status", job_id]):
        return
    result = get_job_status(ClioCoreQueue(RelaySettings.from_env().core_dir), job_id)
    typer.echo(json.dumps(result, indent=2))


@job_app.command("tasks")
def job_tasks(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based task record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum task records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of durable task records for a job as JSON."""
    args = [
        "job",
        "tasks",
        job_id,
        "--cursor",
        str(cursor),
        "--limit",
        str(limit),
    ]
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    tasks, next_cursor, total = queue.list_tasks_page(
        job_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(
        json.dumps(
            _record_page_payload(
                "tasks",
                [task.model_dump(mode="json") for task in tasks],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


@job_app.command("task-events")
def job_task_events(
    task_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="First task event sequence to read.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(help="Maximum task events to read.", min=1),
    ] = 100,
) -> None:
    """Read structured task timeline events from a cursor as JSON."""
    if _try_remote_cluster_passthrough(
        cluster,
        ["job", "task-events", task_id, "--cursor", str(cursor), "--limit", str(limit)],
    ):
        return
    events, next_cursor = ClioCoreQueue(RelaySettings.from_env().core_dir).drain_task_events(
        task_id,
        cursor=cursor,
        limit=limit,
    )
    typer.echo(
        json.dumps(
            {
                "events": [event.model_dump(mode="json") for event in events],
                "next_cursor": next_cursor,
            },
            indent=2,
        )
    )


@job_app.command("record-task-event")
def job_record_task_event(
    task_id: str,
    event_type: Annotated[str, typer.Option(help="Structured task event type.")],
    label: Annotated[str, typer.Option(help="Short UI step label.")],
    summary: Annotated[str, typer.Option(help="Short event summary.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to record the event over SSH."),
    ] = None,
    status: Annotated[
        TaskEventStatus,
        typer.Option(help="Task step status."),
    ] = TaskEventStatus.RUNNING,
    detail: Annotated[str | None, typer.Option(help="Optional detail text.")] = None,
    path_ref: Annotated[
        list[str] | None,
        typer.Option(help="Path reference; repeat for multiple paths."),
    ] = None,
    artifact_ref: Annotated[
        list[str] | None,
        typer.Option(help="Artifact reference; repeat for multiple artifacts."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this task event."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Record a structured task timeline event."""
    metadata_source = _json_text_from_option(metadata_json, metadata_json_file)
    remote_args = [
        "job",
        "record-task-event",
        task_id,
        "--event-type",
        event_type,
        "--label",
        label,
        "--summary",
        summary,
        "--status",
        status.value,
        "--metadata-json",
        metadata_source,
    ]
    if detail is not None:
        remote_args.extend(["--detail", detail])
    for value in path_ref or []:
        remote_args.extend(["--path-ref", value])
    for value in artifact_ref or []:
        remote_args.extend(["--artifact-ref", value])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    event = ClioCoreQueue(RelaySettings.from_env().core_dir).append_task_event(
        TaskTimelineEvent(
            task_id=task_id,
            event_type=event_type,
            label=label,
            status=status,
            summary=summary,
            detail=detail,
            path_refs=path_ref or [],
            artifact_refs=artifact_ref or [],
            metadata=_json_object(metadata_source),
        )
    )
    typer.echo(event.model_dump_json(indent=2))


@job_app.command("wait")
def job_wait(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds for this terminal-state observation."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Observe until terminal, returning current durable state when the bound expires."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise typer.BadParameter("timeout-seconds must be positive and finite")
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise typer.BadParameter("poll-seconds must be positive and finite")
    if _try_remote_job_wait_passthrough(
        cluster,
        job_id=job_id,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    ):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    job = observe_until_terminal(
        queue,
        job_id,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    typer.echo(job.model_dump_json(indent=2))


@job_app.command("read-log")
def job_read_log(
    job_id: str,
    stream: Annotated[str, typer.Option(help="stdout or stderr.")],
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    offset: Annotated[int, typer.Option(help="Byte offset.")] = 0,
    limit: Annotated[int, typer.Option(help="Maximum bytes.")] = 65536,
) -> None:
    """Read stdout or stderr from a job log by byte offset."""
    if _try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "read-log",
            job_id,
            "--stream",
            stream,
            "--offset",
            str(offset),
            "--limit",
            str(limit),
        ],
    ):
        return
    settings = RelaySettings.from_env()
    queue = ClioCoreQueue(settings.core_dir)
    if stream not in {"stdout", "stderr"}:
        raise typer.BadParameter("--stream must be stdout or stderr")
    result = read_job_log(
        settings,
        queue.get_job(job_id),
        stream_name="stdout" if stream == "stdout" else "stderr",
        offset=offset,
        limit=limit,
    )
    typer.echo(json.dumps(result, indent=2))


@job_app.command("read-artifact")
def job_read_artifact(
    artifact_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read an artifact payload as base64 JSON."""
    if _try_remote_cluster_passthrough(cluster, ["job", "read-artifact", artifact_id]):
        return
    result = read_artifact_bytes(ClioCoreQueue(RelaySettings.from_env().core_dir), artifact_id)
    typer.echo(json.dumps(result, indent=2))


@job_app.command("list-artifacts")
def job_list_artifacts(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based artifact record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum artifact records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of artifact references for a job as JSON."""
    if _try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "list-artifacts",
            job_id,
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
        ],
    ):
        return
    artifacts, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_artifacts_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            _record_page_payload(
                "artifacts",
                [artifact.model_dump(mode="json") for artifact in artifacts],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


@job_app.command("used-artifacts")
def job_used_artifacts(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        str | None,
        typer.Option(help="Artifact ID cursor returned by the previous page."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum used-artifact records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List content-pinned artifacts consumed by a job as JSON."""
    remote_args = ["job", "used-artifacts", job_id, "--limit", str(limit)]
    if cursor is not None:
        remote_args.extend(["--cursor", cursor])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    records, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_used_artifacts_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            {
                "used_artifacts": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            },
            indent=2,
        )
    )


@job_app.command("used-by")
def job_used_by(
    artifact_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        str | None,
        typer.Option(help="Opaque edge cursor returned by the previous page."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum consuming-job records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List jobs that consumed a content-pinned artifact as JSON."""
    remote_args = ["job", "used-by", artifact_id, "--limit", str(limit)]
    if cursor is not None:
        remote_args.extend(["--cursor", cursor])
    if _try_remote_cluster_passthrough(cluster, remote_args):
        return
    records, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_artifact_users_page(artifact_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            {
                "used_by": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            },
            indent=2,
        )
    )


@job_app.command("progress")
def job_progress(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based progress record cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum progress records returned.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable page of structured progress observations as JSON."""
    if _try_remote_cluster_passthrough(
        cluster,
        [
            "job",
            "progress",
            job_id,
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
        ],
    ):
        return
    progress, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_progress_page(job_id, cursor=cursor, limit=limit)
    typer.echo(
        json.dumps(
            _record_page_payload(
                "progress",
                [item.model_dump(mode="json") for item in progress],
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            ),
            indent=2,
        )
    )


@job_app.command("record-progress")
def job_record_progress(
    job_id: str,
    label: Annotated[str, typer.Option(help="Progress label.")] = "progress",
    current: Annotated[float | None, typer.Option(help="Current progress value.")] = None,
    total: Annotated[float | None, typer.Option(help="Total progress value.")] = None,
    unit: Annotated[str | None, typer.Option(help="Progress unit.")] = None,
    message: Annotated[str | None, typer.Option(help="Human-readable progress message.")] = None,
    source_event_seq: Annotated[
        int | None,
        typer.Option(help="Source event sequence for this progress observation."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this observation."),
    ] = "{}",
) -> None:
    """Record a structured progress observation for a job."""
    metadata = external_progress_metadata("external_cli", _json_object(metadata_json))
    progress = ClioCoreQueue(RelaySettings.from_env().core_dir).append_progress(
        ProgressRecord(
            job_id=job_id,
            label=label,
            current=current,
            total=total,
            unit=unit,
            message=message,
            source_event_seq=source_event_seq,
            metadata=metadata,
        )
    )
    typer.echo(progress.model_dump_json(indent=2))


@queue_app.command("list")
def queue_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    state: Annotated[
        JobState | None,
        typer.Option(help="Optional job state filter."),
    ] = None,
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    include_terminal: Annotated[
        bool,
        typer.Option(help="Include succeeded, failed, and canceled jobs."),
    ] = False,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global job source cursor.", min=1),
    ] = 1,
    limit: Annotated[int, typer.Option(help="Maximum jobs returned.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
) -> None:
    """List relay queue jobs."""
    args = ["queue", "list"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if state is not None:
        args.extend(["--state", state.value])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if include_terminal:
        args.append("--include-terminal")
    args.extend(
        [
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
        ]
    )
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = list_queue_jobs(
            queue,
            cluster=cluster,
            state=state,
            kind=kind,
            include_terminal=include_terminal,
            cursor=cursor,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("owner-jobs")
def queue_owner_jobs(
    owner_session_id: Annotated[
        str,
        typer.Option(help="Exact owner session id."),
    ],
    owner_session_generation_id: Annotated[
        str | None,
        typer.Option(help="Exact owner session generation; omit only for legacy membership."),
    ] = None,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    include_terminal: Annotated[
        bool,
        typer.Option(help="Include terminal generation members."),
    ] = False,
    cursor: Annotated[
        str | None,
        typer.Option(help="Opaque owner-session membership cursor."),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum source records returned.", min=1, max=500)] = (
        500
    ),
) -> None:
    """List one generation's durable job membership without global history."""
    args = [
        "queue",
        "owner-jobs",
        "--owner-session-id",
        owner_session_id,
        "--limit",
        str(limit),
    ]
    if owner_session_generation_id is not None:
        args.extend(["--owner-session-generation-id", owner_session_generation_id])
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if include_terminal:
        args.append("--include-terminal")
    if cursor is not None:
        args.extend(["--cursor", cursor])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        jobs, next_cursor, total, source_window_count = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=owner_session_generation_id,
            cursor=cursor,
            limit=limit,
            cluster=cluster,
            include_terminal=include_terminal,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(
        json.dumps(
            {
                "jobs": [job.model_dump(mode="json") for job in jobs],
                "owner_session_id": owner_session_id,
                "owner_session_generation_id": owner_session_generation_id,
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_window_count": source_window_count,
            },
            indent=2,
        )
    )


@queue_app.command("migrate-indexes")
def queue_migrate_indexes(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to migrate over SSH, or local storage."),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(
            help="Maximum flat records parsed in each crash-safe batch.", min=1, max=10_000
        ),
    ] = 500,
    all_batches: Annotated[
        bool,
        typer.Option("--all", help="Run bounded batches until migration completes."),
    ] = False,
) -> None:
    """Build v1 active and per-job indexes for an existing v0.9 queue."""
    args = ["queue", "migrate-indexes", "--batch-size", str(batch_size)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if all_batches:
        args.append("--all")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = queue.migrate_indexes_batch(batch_size=batch_size)
        while all_batches and result.get("complete") is not True:
            result = queue.migrate_indexes_batch(batch_size=batch_size)
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("migration-status")
def queue_migration_status(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local storage."),
    ] = None,
) -> None:
    """Read the crash-safe queue index migration checkpoint without mutation."""
    if _try_remote_cluster_passthrough(cluster, ["queue", "migration-status"]):
        return
    status_payload = ClioCoreQueue(RelaySettings.from_env().core_dir).index_migration_status()
    typer.echo(json.dumps(status_payload, indent=2))


@queue_app.command("readiness-info")
def queue_readiness_info(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local storage."),
    ] = None,
) -> None:
    """Verify the sealed fixed queue layout without initialization or repair."""
    if _try_remote_cluster_passthrough(cluster, ["queue", "readiness-info"]):
        return
    try:
        payload = ClioCoreQueue(RelaySettings.from_env().core_dir).readiness_info()
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(payload, indent=2))


@queue_app.command("repair-lease-indexes")
def queue_repair_lease_indexes(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to repair over SSH, or local storage."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum canonical leases rebuilt in the crash-safe repair.",
            min=1,
            max=10_000,
        ),
    ] = 10_000,
) -> None:
    """Rebuild and prune exact endpoint, kind, identity, and expiry lease indexes."""
    args = ["queue", "repair-lease-indexes", "--limit", str(limit)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = queue.repair_lease_operational_indexes(limit=limit)
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("audit-lease-capacity")
def queue_audit_lease_capacity(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to audit over SSH, or local storage."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum canonical leases and index records audited.",
            min=1,
            max=10_000,
        ),
    ] = 10_000,
) -> None:
    """Audit canonical leases, exact indexes, and the O(1) capacity aggregate."""
    args = ["queue", "audit-lease-capacity", "--limit", str(limit)]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    report = ClioCoreQueue(RelaySettings.from_env().core_dir).audit_lease_capacity(limit=limit)
    typer.echo(json.dumps(report, indent=2))
    if report.get("valid") is not True:
        raise typer.Exit(code=1)


@queue_app.command("diagnose")
def queue_diagnose(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
) -> None:
    """Explain why one exact relay job is not progressing."""
    args = [
        "queue",
        "diagnose",
        job_id,
        "--older-than",
        older_than,
        "--scan-limit",
        str(scan_limit),
    ]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        result = diagnose_job(
            queue,
            job_id,
            cluster=cluster,
            stale_after_seconds=_parse_age_seconds(older_than),
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("stale")
def queue_stale(
    cluster: Annotated[str, typer.Option(help="Cluster whose active jobs should be inspected.")],
    job_id: Annotated[
        str | None,
        typer.Option(help="Optional exact job to inspect without acting on neighboring jobs."),
    ] = None,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum jobs returned.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_STALE_SCAN_LIMIT,
) -> None:
    """Discover stale relay jobs without changing queue or scheduler state."""
    args = [
        "queue",
        "stale",
        "--cluster",
        cluster,
        "--older-than",
        older_than,
        "--limit",
        str(limit),
        "--scan-limit",
        str(scan_limit),
    ]
    if job_id is not None:
        args.extend(["--job-id", job_id])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    try:
        result = discover_stale_jobs(
            ClioCoreQueue(RelaySettings.from_env().core_dir),
            cluster=cluster,
            older_than_seconds=_parse_age_seconds(older_than),
            job_id=job_id,
            kind=kind,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("cancel")
def queue_cancel(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Request scheduler cancellation for already-submitted remote work.",
        ),
    ] = False,
) -> None:
    """Cancel a relay job with explicit scheduler policy."""
    args = ["queue", "cancel", job_id]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    args.append("--cancel-scheduler-job" if cancel_scheduler_job else "--keep-scheduler-job")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = _managed_queue_from_env()
    try:
        result = cancel_queue_job(
            queue,
            job_id,
            cluster=cluster,
            scheduler_policy="request-scheduler" if cancel_scheduler_job else "relay-only",
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("cleanup-stale")
def queue_cleanup_stale(
    cluster: Annotated[str, typer.Option(help="Cluster whose stale leases should be recovered.")],
    job_id: Annotated[
        str | None,
        typer.Option(
            help="Optional exact job; prevents neighboring stale jobs from being acted on."
        ),
    ] = None,
    max_attempts: Annotated[
        int,
        typer.Option(help="Maximum attempts before expired leased jobs fail instead of requeue."),
    ] = 3,
    older_than: Annotated[
        str,
        typer.Option(help="Stale activity threshold, for example 30m, 2h, or 1d."),
    ] = "2h",
    kind: Annotated[
        JobKind | None,
        typer.Option(help="Optional job kind filter."),
    ] = None,
    cancel_queued: Annotated[
        bool,
        typer.Option(help="Explicitly cancel queued jobs older than the threshold."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(help="Preview recoverable jobs without changing state."),
    ] = True,
    limit: Annotated[int, typer.Option(help="Maximum jobs acted on.", min=1, max=500)] = (
        DEFAULT_RESULT_LIMIT
    ),
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_STALE_SCAN_LIMIT,
) -> None:
    """Preview or recover stale jobs; queued cancellation is explicit and relay-only."""
    args = [
        "queue",
        "cleanup-stale",
        "--cluster",
        cluster,
        "--max-attempts",
        str(max_attempts),
        "--older-than",
        older_than,
        "--limit",
        str(limit),
        "--scan-limit",
        str(scan_limit),
    ]
    if job_id is not None:
        args.extend(["--job-id", job_id])
    if kind is not None:
        args.extend(["--kind", kind.value])
    if cancel_queued:
        args.append("--cancel-queued")
    args.append("--dry-run" if dry_run else "--no-dry-run")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = _managed_queue_from_env()
    try:
        result = cleanup_stale_jobs(
            queue,
            cluster=cluster,
            older_than_seconds=_parse_age_seconds(older_than),
            job_id=job_id,
            kind=kind,
            max_attempts=max_attempts,
            dry_run=dry_run,
            cancel_queued=cancel_queued,
            limit=limit,
            scan_limit=scan_limit,
        )
    except (RelayError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(result, indent=2))


@queue_app.command("retention-plan")
def queue_retention_plan(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    expected_updated_at: Annotated[
        str | None,
        typer.Option(help="Optional exact ISO-8601 job update timestamp assertion."),
    ] = None,
) -> None:
    """Build a read-only terminal-job retention plan."""
    args = ["queue", "retention-plan", job_id]
    if expected_updated_at is not None:
        args.extend(["--expected-updated-at", expected_updated_at])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    settings = RelaySettings.from_env()
    coordinator = TerminalRetentionCoordinator(
        ClioCoreQueue(settings.core_dir),
        settings.spool_dir,
    )
    plan = coordinator.plan(
        job_id,
        expected_updated_at=_optional_datetime(expected_updated_at),
    )
    typer.echo(
        json.dumps(
            {
                "plan": plan.model_dump(mode="json"),
                "scheduler_cancel_requested": False,
            },
            indent=2,
        )
    )


@queue_app.command("retention-status")
def queue_retention_status(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read the current crash-resumable retention phase without mutation."""
    if _try_remote_cluster_passthrough(cluster, ["queue", "retention-status", job_id]):
        return
    settings = RelaySettings.from_env()
    plan = TerminalRetentionCoordinator(
        ClioCoreQueue(settings.core_dir),
        settings.spool_dir,
    ).plan(job_id)
    typer.echo(
        json.dumps(
            {
                "job_id": job_id,
                "receipt_id": plan.receipt_id,
                "phase": None if plan.receipt_phase is None else plan.receipt_phase.value,
                "complete": plan.receipt_phase is not None
                and plan.receipt_phase.value == "complete",
                "eligible": plan.eligible,
                "protections": plan.protections,
                "scheduler_cancel_requested": False,
            },
            indent=2,
        )
    )


@queue_app.command("retention-collect")
def queue_retention_collect(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to collect over SSH."),
    ] = None,
    execute: Annotated[
        bool,
        typer.Option(
            "--execute/--dry-run",
            help="Advance retention; dry-run is the default and never mutates.",
        ),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(help="Maximum bounded retention actions.", min=1, max=100),
    ] = 100,
    expected_updated_at: Annotated[
        str | None,
        typer.Option(help="Optional exact ISO-8601 job update timestamp assertion."),
    ] = None,
) -> None:
    """Preview or advance terminal retention without scheduler cancellation."""
    args = [
        "queue",
        "retention-collect",
        job_id,
        "--execute" if execute else "--dry-run",
        "--batch-size",
        str(batch_size),
    ]
    if expected_updated_at is not None:
        args.extend(["--expected-updated-at", expected_updated_at])
    if _try_remote_cluster_passthrough(cluster, args):
        return

    def action() -> None:
        settings = RelaySettings.from_env()
        queue: ClioCoreQueue = (
            storage_managed_queue(settings) if execute else ClioCoreQueue(settings.core_dir)
        )
        result = TerminalRetentionCoordinator(queue, settings.spool_dir).collect(
            job_id,
            execute=execute,
            batch_size=batch_size,
            expected_updated_at=_optional_datetime(expected_updated_at),
        )
        typer.echo(result.model_dump_json(indent=2))

    _run_or_exit(action)


@queue_app.command("validate")
@_acceptance_report_command
def queue_validate(
    cluster: Annotated[str, typer.Option(help="Cluster containing the live worker service.")],
    job_id: Annotated[
        str | None,
        typer.Argument(help="Optional expendable queued compatibility anchor."),
    ] = None,
    kind: Annotated[
        JobKind,
        typer.Option(help="Controlled process kind; 1.0 live validation requires jarvis."),
    ] = JobKind.JARVIS,
    older_than: Annotated[
        str,
        typer.Option(help="Age that makes the queued test job stale, such as 1m or 2h."),
    ] = "2h",
    scan_limit: Annotated[
        int,
        typer.Option(help="Maximum durable job records scanned.", min=1, max=10_000),
    ] = DEFAULT_SCAN_LIMIT,
    provider: Annotated[
        str | None,
        typer.Option(
            "--scheduler-provider",
            help="Explicit provider for the bounded scheduler-preservation fixture.",
        ),
    ] = None,
    scheduler_run_seconds: Annotated[
        int,
        typer.Option(help="Bounded scheduler fixture runtime after release.", min=5, max=300),
    ] = 5,
    scheduler_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time for each scheduler fixture transition.", min=0.1, max=600),
    ] = 120.0,
    scheduler_poll_seconds: Annotated[
        float,
        typer.Option(help="Scheduler fixture polling interval.", min=0.01, max=10),
    ] = 1.0,
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical JSON report path."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Acceptance launcher identity, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Acceptance install source override, such as pypi."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional exact wheel whose SHA-256 binds the acceptance report.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_artifact_sha256: Annotated[
        str | None,
        typer.Option(hidden=True),
    ] = None,
    report_json_only: Annotated[
        bool,
        typer.Option(hidden=True),
    ] = False,
) -> None:
    """Validate real bounded queue admission, cleanup, and scheduler preservation."""
    resolved_report = report or default_report_path(cluster)
    artifact_sha256 = validation_artifact_sha256 or (
        sha256_file(validation_artifact) if validation_artifact is not None else None
    )

    def action() -> None:
        definition = _require_cluster(cluster)
        selected_provider = provider or definition.scheduler_provider
        if should_execute_on_cluster(definition):
            args = [
                "queue",
                "validate",
                "--cluster",
                cluster,
                "--kind",
                kind.value,
                "--older-than",
                older_than,
                "--scan-limit",
                str(scan_limit),
                "--scheduler-provider",
                selected_provider,
                "--scheduler-run-seconds",
                str(scheduler_run_seconds),
                "--scheduler-timeout-seconds",
                str(scheduler_timeout_seconds),
                "--scheduler-poll-seconds",
                str(scheduler_poll_seconds),
                "--report-json-only",
            ]
            if job_id is not None:
                args.insert(2, job_id)
            if validation_launcher is not None:
                args.extend(["--validation-launcher", validation_launcher])
            if validation_install_source is not None:
                args.extend(["--validation-install-source", validation_install_source])
            if artifact_sha256 is not None:
                args.extend(["--validation-artifact-sha256", artifact_sha256])
            canonical = LiveValidationReport.model_validate_json(
                run_remote_clio(definition, args).strip()
            )
            _write_remote_verified_report(canonical, definition, resolved_report)
            if markdown_report is not None:
                ValidationRecorder(canonical).write(resolved_report, markdown_report)
        else:
            canonical = run_queue_management_validation(
                _managed_queue_from_env(),
                job_id=job_id,
                cluster=cluster,
                kind=kind,
                older_than_seconds=_parse_age_seconds(older_than),
                scan_limit=scan_limit,
                scheduler_provider=validation_provider_for_scheduler(selected_provider),
                scheduler_run_seconds=scheduler_run_seconds,
                scheduler_timeout_seconds=scheduler_timeout_seconds,
                scheduler_poll_seconds=scheduler_poll_seconds,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact_sha256=artifact_sha256,
            )
            if not report_json_only:
                ValidationRecorder(canonical).write(resolved_report, markdown_report)
        if report_json_only:
            typer.echo(canonical.model_dump_json(indent=2))
            return
        typer.echo(f"validation.status={canonical.status.value}")
        typer.echo(f"validation.report={resolved_report.resolve()}")
        typer.echo(canonical.model_dump_json(indent=2))
        if canonical.status is ValidationStatus.FAILED:
            raise typer.Exit(code=1)

    try:
        action()
    except typer.Exit:
        raise
    except BaseException as exc:
        if not report_json_only:
            _write_failed_acceptance_report(
                path=resolved_report,
                scenario="queue-management",
                cluster=cluster,
                check_id="queue.completed",
                summary="complete queue-management acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
            )
        raise


@worker_app.command("status")
def worker_status_command(
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH, or local filter in local mode."),
    ] = None,
) -> None:
    """Show registered worker capacity and leases."""
    args = ["worker", "status"]
    if cluster is not None:
        args.extend(["--cluster", cluster])
    if _try_remote_cluster_passthrough(cluster, args):
        return
    queue = ClioCoreQueue(RelaySettings.from_env().core_dir)
    typer.echo(json.dumps(worker_status(queue, cluster=cluster), indent=2))


@scheduler_app.command("status")
def scheduler_status_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Read and normalize one scheduler job through the configured provider."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "status",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            provider_for_scheduler(selected).poll(scheduler_job_id).model_dump_json(indent=2)
        )
    )


@scheduler_app.command("cancel")
def scheduler_cancel_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Explicitly request cancellation of one scheduler job through its provider."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "cancel",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = provider_for_scheduler(selected).cancel(scheduler_job_id)
        payload = {
            "scheduler": selected,
            "scheduler_job_id": scheduler_job_id,
            "cancel_requested": True,
            "accepted": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        typer.echo(json.dumps(payload, indent=2))
        if result.returncode != 0:
            raise typer.Exit(code=1)

    _run_or_exit(action)


@scheduler_app.command("connector-placement", hidden=True)
def scheduler_connector_placement_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Resolve one provider-verified host for an allocation-scoped connector."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-placement",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            allocation_connector_provider_for_scheduler(selected)
            .connector_placement(scheduler_job_id)
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command(
    "connector-step-start",
    hidden=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def scheduler_connector_step_start_command(
    ctx: typer.Context,
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    step_marker: Annotated[
        str,
        typer.Option(help="Crash-reconciliation marker for the connector step."),
    ],
    output_path: Annotated[
        str,
        typer.Option(help="Absolute cluster-side connector output path."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Launch one asynchronous provider-owned connector step."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    connector_command = list(ctx.args)
    if connector_command and connector_command[0] == "--":
        connector_command = connector_command[1:]
    args = [
        "scheduler",
        "connector-step-start",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
        "--output-path",
        output_path,
        "--",
        *connector_command,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            allocation_connector_provider_for_scheduler(selected)
            .launch_connector_step(
                scheduler_job_id,
                placement_host=placement_host,
                step_marker=step_marker,
                command=connector_command,
                output_path=output_path,
            )
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command("connector-step-status", hidden=True)
def scheduler_connector_step_status_command(
    scheduler_step_id: str,
    scheduler_job_id: Annotated[str, typer.Option(help="Owning scheduler allocation id.")],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Observe one exact allocation connector step."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-status",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return
    _run_or_exit(
        lambda: typer.echo(
            allocation_connector_provider_for_scheduler(selected)
            .poll_connector_step(
                scheduler_job_id,
                scheduler_step_id=scheduler_step_id,
                placement_host=placement_host,
            )
            .model_dump_json(indent=2)
        )
    )


@scheduler_app.command("connector-step-cancel", hidden=True)
def scheduler_connector_step_cancel_command(
    scheduler_step_id: str,
    scheduler_job_id: Annotated[str, typer.Option(help="Owning scheduler allocation id.")],
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Cancel one exact connector step without canceling its allocation."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-cancel",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = allocation_connector_provider_for_scheduler(selected).cancel_connector_step(
            scheduler_job_id,
            scheduler_step_id=scheduler_step_id,
        )
        typer.echo(
            json.dumps(
                {
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "scheduler_step_id": scheduler_step_id,
                    "cancel_requested": True,
                    "accepted": result.returncode == 0,
                    "returncode": result.returncode,
                    "stdout": result.stdout.strip(),
                    "stderr": result.stderr.strip(),
                },
                indent=2,
            )
        )
        if result.returncode != 0:
            raise typer.Exit(code=1)

    _run_or_exit(action)


@scheduler_app.command("connector-step-reconcile", hidden=True)
def scheduler_connector_step_reconcile_command(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    placement_host: Annotated[
        str,
        typer.Option(help="Provider-verified allocation host."),
    ],
    step_marker: Annotated[
        str,
        typer.Option(help="Exact connector step reconciliation marker."),
    ],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Find an interrupted connector launch by exact provider marker."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "connector-step-reconcile",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        step = allocation_connector_provider_for_scheduler(selected).find_connector_step(
            scheduler_job_id,
            step_marker=step_marker,
            placement_host=placement_host,
        )
        typer.echo(
            json.dumps(
                {
                    "schema_version": "clio-relay.scheduler-connector-step-reconciliation.v1",
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "step_marker": step_marker,
                    "placement_host": placement_host,
                    "found": step is not None,
                    "step": step.model_dump(mode="json") if step is not None else None,
                },
                indent=2,
            )
        )

    _run_or_exit(action)


@scheduler_app.command("submit-held-validation", hidden=True)
def scheduler_submit_held_validation(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    job_name: Annotated[str, typer.Option(help="Unique bounded validation job name.")],
    run_seconds: Annotated[int, typer.Option(help="Bounded sleep duration.")] = 30,
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Submit one held provider-owned validation job."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "submit-held-validation",
        "--cluster",
        cluster,
        "--provider",
        selected,
        "--job-name",
        job_name,
        "--run-seconds",
        str(run_seconds),
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        scheduler_job_id = validation_provider_for_scheduler(selected).submit_held_validation_job(
            job_name=job_name, run_seconds=run_seconds
        )
        typer.echo(
            json.dumps(
                {
                    "scheduler": selected,
                    "scheduler_job_id": scheduler_job_id,
                    "held": True,
                    "owned_validation_job": True,
                },
                indent=2,
            )
        )

    _run_or_exit(action)


@scheduler_app.command("release-validation", hidden=True)
def scheduler_release_validation(
    scheduler_job_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
) -> None:
    """Release one exact held validation job."""
    definition = _require_cluster(cluster)
    selected = provider or definition.scheduler_provider
    args = [
        "scheduler",
        "release-validation",
        scheduler_job_id,
        "--cluster",
        cluster,
        "--provider",
        selected,
    ]
    if should_execute_on_cluster(definition):
        _run_remote_or_exit(definition, args)
        return

    def action() -> None:
        result = validation_provider_for_scheduler(selected).release_validation_job(
            scheduler_job_id
        )
        payload = {
            "scheduler": selected,
            "scheduler_job_id": scheduler_job_id,
            "release_requested": True,
            "accepted": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        typer.echo(json.dumps(payload, indent=2))
        if result.returncode != 0:
            raise typer.Exit(code=1)

    _run_or_exit(action)


@scheduler_app.command("validate-lifecycle")
@_acceptance_report_command
def scheduler_validate_lifecycle(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    provider: Annotated[
        str | None,
        typer.Option(help="Override the cluster's explicit scheduler provider."),
    ] = None,
    run_seconds: Annotated[
        int,
        typer.Option(help="Bounded validation job runtime in seconds."),
    ] = 30,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Timeout for each required lifecycle phase."),
    ] = 180.0,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Scheduler polling interval."),
    ] = 1.0,
    report_path: Annotated[
        Path | None,
        typer.Option("--report", help="Canonical scheduler lifecycle JSON path."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional Markdown rendering of the JSON report."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in scheduler evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Deterministically validate held-to-completed scheduler lifecycle semantics."""
    resolved_report = report_path or default_report_path(cluster)
    try:
        definition = _require_cluster(cluster)
        selected = provider or definition.scheduler_provider
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=resolved_report,
            scenario="scheduler-lifecycle",
            cluster=cluster,
            check_id="scheduler.preflight",
            summary="validate scheduler lifecycle acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    canonical_report: list[LiveValidationReport | None] = [None]

    def action() -> None:
        report = run_scheduler_lifecycle_validation(
            cluster=cluster,
            definition=definition,
            provider=selected,
            run_seconds=run_seconds,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical_report[0] = report
        if should_execute_on_cluster(definition):
            try:
                attach_verified_worker_identity(
                    report,
                    _remote_worker_info(definition),
                )
            except BaseException as exc:
                recorder = ValidationRecorder(report)
                recorder.record_failure(
                    "worker.identity",
                    "verify exact cluster worker artifact identity",
                    exc,
                )
                recorder.finish(exc)
                write_validation_report(report, resolved_report)
                raise
        write_validation_report(report, resolved_report)
        if markdown_report is not None:
            ValidationRecorder(report).write(resolved_report, markdown_report)
        typer.echo(f"validation.report={resolved_report.resolve()}")
        typer.echo(report.model_dump_json(indent=2))
        if report.status is ValidationStatus.FAILED:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=resolved_report,
                scenario="scheduler-lifecycle",
                cluster=cluster,
                check_id="scheduler.completed",
                summary="complete scheduler lifecycle acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    _run_or_exit(guarded_action)


@job_app.command("cancel")
def job_cancel(
    job_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Request scheduler cancellation for already-submitted remote work.",
        ),
    ] = False,
) -> None:
    """Cancel a queued or running job."""
    args = ["job", "cancel", job_id]
    if cancel_scheduler_job:
        args.append("--cancel-scheduler-job")
    if _try_remote_cluster_passthrough(cluster, args):
        return
    job = request_cancel_job(
        _managed_queue_from_env(),
        job_id,
        cancel_scheduler=cancel_scheduler_job,
    )
    typer.echo(f"{job.job_id} {job.state.value}")


_GENERIC_GATEWAY_RUNTIME_KEYS = frozenset(
    {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)
_GENERIC_GATEWAY_CONNECTOR_KEYS = frozenset(
    {
        "browser_proxy",
        "desktop_connector",
        "remote_connector",
    }
)
_GENERIC_GATEWAY_OWNER_METADATA_KEYS = frozenset(
    {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
        "runtime_kind",
        "binding_source",
        "source_relay_job_id",
        "source_relay_artifact_id",
        "jarvis_execution_id",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)


def _reject_generic_cli_gateway_runtime_fields(
    *,
    gateway: dict[str, object],
    metadata: dict[str, object],
) -> None:
    """Keep generic CLI gateway writes outside supervisor-owned runtime identity."""
    protected = [f"gateway.{key}" for key in sorted(_GENERIC_GATEWAY_RUNTIME_KEYS & gateway.keys())]
    transport = gateway.get("transport")
    if isinstance(transport, dict):
        protected.extend(
            f"gateway.transport.{key}"
            for key in sorted(_GENERIC_GATEWAY_CONNECTOR_KEYS & transport.keys())
        )
    protected.extend(
        f"metadata.{key}" for key in sorted(_GENERIC_GATEWAY_OWNER_METADATA_KEYS & metadata.keys())
    )
    if protected:
        raise typer.BadParameter(
            "generic gateway commands cannot write relay-managed runtime fields: "
            + ", ".join(protected)
        )


@gateway_app.command("create")
def gateway_create(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Human-readable session name.")],
    state: Annotated[
        GatewaySessionState,
        typer.Option(help="Initial gateway session state."),
    ] = GatewaySessionState.CREATED,
    queue_state: Annotated[str | None, typer.Option(help="Scheduler queue state.")] = None,
    node: Annotated[str | None, typer.Option(help="Allocated node or host.")] = None,
    stdout_uri: Annotated[str | None, typer.Option(help="Gateway stdout log URI.")] = None,
    stderr_uri: Annotated[str | None, typer.Option(help="Gateway stderr log URI.")] = None,
    log_uri: Annotated[
        list[str] | None,
        typer.Option(help="Additional log URI; repeat for multiple logs."),
    ] = None,
    artifact: Annotated[
        list[str] | None,
        typer.Option(help="Artifact URI or id; repeat for multiple artifacts."),
    ] = None,
    gateway_json: Annotated[
        str,
        typer.Option(help="JSON object with gateway endpoint metadata."),
    ] = "{}",
    gateway_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with gateway endpoint metadata."),
    ] = None,
    resources_json: Annotated[
        str,
        typer.Option(help="JSON object with requested resource metadata."),
    ] = "{}",
    resources_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with requested resource metadata."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this gateway session."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Create a durable scheduler-backed gateway service session."""
    gateway_source = _json_text_from_option(gateway_json, gateway_json_file)
    resources_source = _json_text_from_option(resources_json, resources_json_file)
    metadata_source = _json_text_from_option(metadata_json, metadata_json_file)
    gateway_payload = _json_object(gateway_source)
    metadata_payload = _json_object(metadata_source)
    _reject_generic_cli_gateway_runtime_fields(
        gateway=gateway_payload,
        metadata=metadata_payload,
    )
    remote_args = [
        "gateway",
        "create",
        "--cluster",
        cluster,
        "--name",
        name,
        "--state",
        state.value,
        "--gateway-json",
        gateway_source,
        "--resources-json",
        resources_source,
        "--metadata-json",
        metadata_source,
    ]
    if queue_state is not None:
        remote_args.extend(["--queue-state", queue_state])
    if node is not None:
        remote_args.extend(["--node", node])
    if stdout_uri is not None:
        remote_args.extend(["--stdout-uri", stdout_uri])
    if stderr_uri is not None:
        remote_args.extend(["--stderr-uri", stderr_uri])
    for value in log_uri or []:
        remote_args.extend(["--log-uri", value])
    for value in artifact or []:
        remote_args.extend(["--artifact", value])
    if _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    session = ClioCoreQueue(RelaySettings.from_env().core_dir).create_gateway_session(
        GatewaySession(
            cluster=cluster,
            name=name,
            state=state,
            queue_state=queue_state,
            node=node,
            stdout_uri=stdout_uri,
            stderr_uri=stderr_uri,
            log_uris=log_uri or [],
            gateway=gateway_payload,
            artifacts=artifact or [],
            requested_resources=_json_object(resources_source),
            metadata=metadata_payload,
        )
    )
    typer.echo(_public_json(public_gateway_session(session)))


def _local_gateway_session(
    session_id: str,
    *,
    cluster: str | None,
) -> GatewaySession | None:
    """Return a desktop-owned gateway record before considering remote passthrough."""
    queue = _local_gateway_queue()
    try:
        session = queue.get_gateway_session(session_id)
    except NotFoundError:
        return None
    if cluster is not None and session.cluster != cluster:
        return None
    return session


def _local_gateway_queue() -> ClioCoreQueue:
    """Open the desktop queue without resolving unrelated executable settings."""
    configured = os.getenv("CLIO_RELAY_CORE_DIR")
    if configured:
        core_dir = Path(configured).expanduser().resolve()
    else:
        bootstrap_dir = Path.home() / ".local" / "share" / "clio-relay" / "core"
        core_dir = bootstrap_dir.resolve() if bootstrap_dir.exists() else Path(".clio-relay/core")
    return ClioCoreQueue(core_dir)


@gateway_app.command("list")
def gateway_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional configured cluster filter."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global gateway source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum gateway source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
    desktop_cursor: Annotated[
        int | None,
        typer.Option(help="Optional desktop-owned gateway source cursor.", min=1),
    ] = None,
    cluster_cursor: Annotated[
        int | None,
        typer.Option(help="Optional cluster-owned gateway source cursor.", min=1),
    ] = None,
) -> None:
    """List bounded desktop and cluster gateway source windows."""

    def action() -> None:
        resolved_desktop_cursor = desktop_cursor or cursor
        resolved_cluster_cursor = cluster_cursor or cursor
        remote_args = [
            "gateway",
            "list",
            "--cursor",
            str(resolved_cluster_cursor),
            "--limit",
            str(limit),
        ]
        if cluster is not None:
            remote_args.extend(["--cluster", cluster])
        queue = _local_gateway_queue()
        desktop_sessions, desktop_next_cursor, desktop_total = queue.list_gateway_sessions_page(
            cursor=resolved_desktop_cursor,
            limit=limit,
            cluster=cluster,
        )
        cluster_sessions: list[GatewaySession] = []
        cluster_next_cursor: int | None = None
        cluster_total = 0
        query_remote = cluster is not None and _should_query_remote_cluster(cluster)
        if query_remote:
            assert cluster is not None
            definition = _require_cluster(cluster)
            cluster_sessions, cluster_next_cursor, cluster_total = _parse_gateway_page(
                run_remote_clio(definition, remote_args),
                limit=limit,
                expected_cluster=cluster,
            )
        combined = {session.session_id: session for session in cluster_sessions}
        combined.update({session.session_id: session for session in desktop_sessions})
        sessions = sorted(
            combined.values(),
            key=lambda session: (session.created_at, session.session_id),
        )
        typer.echo(
            _public_json(
                {
                    "gateway_sessions": [public_gateway_session(session) for session in sessions],
                    "source_cursor": cursor,
                    "source_limit": limit,
                    "source_next_cursor": (
                        (
                            desktop_next_cursor
                            if desktop_next_cursor == cluster_next_cursor
                            else None
                        )
                        if query_remote
                        else desktop_next_cursor
                    ),
                    "source_next_cursors": {
                        "desktop": desktop_next_cursor,
                        "cluster": cluster_next_cursor,
                    },
                    "source_cursors": {
                        "desktop": resolved_desktop_cursor,
                        "cluster": resolved_cluster_cursor,
                    },
                    "source_totals": {
                        "desktop": desktop_total,
                        "cluster": cluster_total,
                    },
                    "source_total": desktop_total + cluster_total,
                    "source_total_semantics": "sum_of_independent_gateway_source_high_waters",
                    "aggregate_record_limit": limit * 2,
                    "filters_apply_within_source_window": True,
                }
            )
        )

    _run_or_exit(action)


def _should_query_remote_cluster(cluster: str) -> bool:
    """Return whether a CLI read should include the configured remote store."""
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    return should_execute_on_cluster(_require_cluster(cluster))


def _parse_gateway_page(
    payload: str,
    *,
    limit: int,
    expected_cluster: str,
) -> tuple[list[GatewaySession], int | None, int]:
    """Validate a bounded current or legacy remote gateway-list response."""
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RelayError("remote gateway list did not return valid JSON") from exc
    if isinstance(decoded, list):
        raw_sessions = cast(list[object], decoded)
        next_cursor: int | None = None
        total = len(raw_sessions)
    elif isinstance(decoded, dict):
        page = cast(dict[str, object], decoded)
        raw = page.get("gateway_sessions")
        if not isinstance(raw, list):
            raise RelayError("remote gateway page omitted gateway_sessions")
        raw_sessions = cast(list[object], raw)
        raw_next_cursor = page.get("source_next_cursor")
        if raw_next_cursor is not None and not isinstance(raw_next_cursor, int):
            raise RelayError("remote gateway page has an invalid next cursor")
        next_cursor = raw_next_cursor
        raw_total = page.get("source_total")
        if not isinstance(raw_total, int) or raw_total < len(raw_sessions):
            raise RelayError("remote gateway page has an invalid source total")
        total = raw_total
    else:
        raise RelayError("remote gateway list must return an object or legacy array")
    if len(raw_sessions) > limit:
        raise RelayError(f"remote gateway page exceeds the requested {limit}-record limit")
    sessions = [GatewaySession.model_validate(item) for item in raw_sessions]
    if any(session.cluster != expected_cluster for session in sessions):
        raise RelayError("remote gateway page returned a different cluster")
    return sessions, next_cursor, total


@gateway_app.command("get")
def gateway_get(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read a gateway service session."""
    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is not None:
        typer.echo(_public_json(public_gateway_session(local_session)))
        return
    remote_args = ["gateway", "get", session_id]
    if _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    session = ClioCoreQueue(RelaySettings.from_env().core_dir).get_gateway_session(session_id)
    typer.echo(_public_json(public_gateway_session(session)))


@gateway_app.command("update")
def gateway_update(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to update over SSH."),
    ] = None,
    state: Annotated[
        GatewaySessionState | None,
        typer.Option(help="Updated gateway session state."),
    ] = None,
    queue_state: Annotated[str | None, typer.Option(help="Scheduler queue state.")] = None,
    node: Annotated[str | None, typer.Option(help="Allocated node or host.")] = None,
    stdout_uri: Annotated[str | None, typer.Option(help="Gateway stdout log URI.")] = None,
    stderr_uri: Annotated[str | None, typer.Option(help="Gateway stderr log URI.")] = None,
    log_uri: Annotated[
        list[str] | None,
        typer.Option(help="Additional log URI; repeat for multiple logs."),
    ] = None,
    artifact: Annotated[
        list[str] | None,
        typer.Option(help="Artifact URI or id; repeat for multiple artifacts."),
    ] = None,
    resources_json: Annotated[
        str | None,
        typer.Option(help="JSON object with requested resource metadata."),
    ] = None,
    resources_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with requested resource metadata."),
    ] = None,
    gateway_json: Annotated[
        str | None,
        typer.Option(help="JSON object with gateway endpoint metadata."),
    ] = None,
    gateway_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with gateway endpoint metadata."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata to merge into this session."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Update a gateway service session."""
    if gateway_json is not None and gateway_json_file is not None:
        raise typer.BadParameter("use either --gateway-json or --gateway-json-file, not both")
    if resources_json is not None and resources_json_file is not None:
        raise typer.BadParameter("use either --resources-json or --resources-json-file, not both")
    gateway_source = None
    if gateway_json is not None or gateway_json_file is not None:
        gateway_source = _json_text_from_option(gateway_json or "{}", gateway_json_file)
    resources_source = None
    if resources_json is not None or resources_json_file is not None:
        resources_source = _json_text_from_option(resources_json or "{}", resources_json_file)
    metadata_source = _json_text_from_option(metadata_json, metadata_json_file)
    gateway_payload = _json_object(gateway_source) if gateway_source is not None else None
    metadata_payload = _json_object(metadata_source)
    _reject_generic_cli_gateway_runtime_fields(
        gateway=gateway_payload or {},
        metadata=metadata_payload,
    )
    remote_args = ["gateway", "update", session_id]
    if state is not None:
        remote_args.extend(["--state", state.value])
    if queue_state is not None:
        remote_args.extend(["--queue-state", queue_state])
    if node is not None:
        remote_args.extend(["--node", node])
    if stdout_uri is not None:
        remote_args.extend(["--stdout-uri", stdout_uri])
    if stderr_uri is not None:
        remote_args.extend(["--stderr-uri", stderr_uri])
    for value in log_uri or []:
        remote_args.extend(["--log-uri", value])
    for value in artifact or []:
        remote_args.extend(["--artifact", value])
    if resources_source is not None:
        remote_args.extend(["--resources-json", resources_source])
    if gateway_source is not None:
        remote_args.extend(["--gateway-json", gateway_source])
    remote_args.extend(["--metadata-json", metadata_source])
    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is None and _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    updates: dict[str, object] = {}
    if queue_state is not None:
        updates["queue_state"] = queue_state
    if node is not None:
        updates["node"] = node
    if stdout_uri is not None:
        updates["stdout_uri"] = stdout_uri
    if stderr_uri is not None:
        updates["stderr_uri"] = stderr_uri
    if log_uri is not None:
        updates["log_uris"] = log_uri
    if artifact is not None:
        updates["artifacts"] = artifact
    if resources_source is not None:
        updates["requested_resources"] = _json_object(resources_source)
    if gateway_payload is not None:
        updates["gateway"] = gateway_payload
    _run_or_exit(
        lambda: typer.echo(
            _public_json(
                public_gateway_session(
                    ClioCoreQueue(RelaySettings.from_env().core_dir).update_gateway_session(
                        session_id,
                        state=state,
                        metadata=metadata_payload,
                        reject_relay_managed_fields=True,
                        **updates,
                    )
                )
            )
        )
    )


@gateway_app.command("close")
def gateway_close(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to update over SSH."),
    ] = None,
) -> None:
    """Mark a gateway service session closed."""
    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is None and _try_remote_gateway_session_passthrough(
        cluster, ["gateway", "close", session_id]
    ):
        return
    _run_or_exit(
        lambda: typer.echo(
            _public_json(
                public_gateway_session(
                    ClioCoreQueue(RelaySettings.from_env().core_dir).close_gateway_session(
                        session_id
                    )
                )
            )
        )
    )


@gateway_app.command("start-runtime")
@_acceptance_report_command
def gateway_start_runtime(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Human-readable runtime session name.")],
    runtime_json_file: Annotated[
        Path,
        typer.Option(help="Path to a generic ServiceRuntimeSpec JSON document."),
    ],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    owner_session_id: Annotated[
        str | None,
        typer.Option(help="Owned desktop relay session that controls this runtime."),
    ] = None,
    owner_session_generation_id: Annotated[
        str | None,
        typer.Option(help="Exact owned desktop relay session generation."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime validation JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in gateway evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Start and bind a scheduler-backed streaming service runtime."""
    canonical_report_path = validation_report or default_report_path(cluster)
    report_id: list[str | None] = [None]

    def action() -> None:
        definition = _require_cluster(cluster)
        if (owner_session_id is None) != (owner_session_generation_id is None):
            raise ConfigurationError(
                "--owner-session-id and --owner-session-generation-id must be provided together"
            )
        if not runtime_json_file.exists():
            raise ConfigurationError(f"runtime spec does not exist: {runtime_json_file}")
        spec = ServiceRuntimeSpec.model_validate_json(
            runtime_json_file.read_text(encoding="utf-8-sig")
        )
        settings = RelaySettings.from_env()
        queue = storage_managed_queue(settings)
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=queue,
            cluster=cluster,
            definition=definition,
            token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=_resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )

        if owner_session_id is None or owner_session_generation_id is None:
            result = supervisor.start(name=name, spec=spec)
        else:
            with owner_session_gateway_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                session_id=owner_session_id,
                session_generation_id=owner_session_generation_id,
                transition_lock_factory=_session_transition_lock,
                session_status_reader=status_remote_session,
                admission_status_reader=_owner_session_admission_status,
            ) as admission:
                result = supervisor.start(
                    name=name,
                    spec=spec,
                    owner_session_id=admission.owner_session_id,
                    owner_session_generation_id=admission.owner_session_generation_id,
                    owner_session_admission_id=admission.owner_session_admission_id,
                )
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        report_id[0] = canonical.report_id
        if isinstance(result, ServiceRuntimePendingResult):
            # A nonterminal report cannot satisfy the release gate. Persist and
            # return its exact retry selector without adding another fallible
            # remote observation that could hide the already-durable result.
            write_validation_report(canonical, canonical_report_path)
        else:
            _write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = public_gateway_session(result.session)
        if isinstance(result, ServiceRuntimePendingResult):
            payload["outcome"] = result.outcome
            payload["retry_selector"] = result.retry_selector()
            payload["scheduler_action"] = result.scheduler_action
            payload["relay_action"] = result.relay_action
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            report_already_written = False
            if report_id[0] is not None:
                with suppress(ConfigurationError):
                    report_already_written = (
                        load_validation_report(canonical_report_path).report_id == report_id[0]
                    )
            if not report_already_written:
                artifact_sha256: str | None = None
                if validation_artifact is not None:
                    with suppress(OSError):
                        artifact_sha256 = sha256_file(validation_artifact)
                failed_report = new_live_validation_report(
                    scenario="gateway-runtime",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=artifact_sha256,
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "gateway.start-runtime",
                    "start scheduler-backed gateway runtime",
                    exc,
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    _run_or_exit(guarded_action)


@gateway_app.command("resume-runtime")
@_acceptance_report_command
def gateway_resume_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime validation JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in gateway evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Advance one exact submitted runtime without creating another scheduler job."""
    canonical_report_path = validation_report or default_report_path(cluster)
    report_id: list[str | None] = [None]

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        queue = storage_managed_queue(settings)
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=queue,
            cluster=cluster,
            definition=definition,
            token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=_resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )
        session = queue.get_gateway_session(session_id)
        owner_session_id = session.metadata.get("owner_session_id")
        owner_generation_id = session.metadata.get("owner_session_generation_id")
        owner_admission_id = session.metadata.get("owner_session_admission_id")
        owner_values = (owner_session_id, owner_generation_id, owner_admission_id)
        if all(value is None for value in owner_values):
            result = supervisor.resume_start(session_id=session_id)
        else:
            if not all(isinstance(value, str) and value for value in owner_values):
                raise RelayError(
                    "owned gateway runtime omitted its exact owner-session admission identity"
                )
            typed_owner_session_id = cast(str, owner_session_id)
            typed_owner_generation_id = cast(str, owner_generation_id)
            typed_owner_admission_id = cast(str, owner_admission_id)
            expected_admission_id = _desktop_owner_session_admission_id(
                cluster=cluster,
                session_id=typed_owner_session_id,
            )
            if typed_owner_admission_id != expected_admission_id:
                raise RelayError("owned gateway runtime admission identity changed")
            with owner_session_gateway_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                session_id=typed_owner_session_id,
                session_generation_id=typed_owner_generation_id,
                transition_lock_factory=_session_transition_lock,
                session_status_reader=status_remote_session,
                admission_status_reader=_owner_session_admission_status,
            ) as admission:
                if admission.owner_session_admission_id != typed_owner_admission_id:
                    raise RelayError("owned gateway runtime admission identity changed")
                result = supervisor.resume_start(session_id=session_id)
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        report_id[0] = canonical.report_id
        if isinstance(result, ServiceRuntimePendingResult):
            # Pending is an operational checkpoint, not release evidence. Its
            # successful return must not depend on a second worker-provenance
            # observation after the exact runtime query already completed.
            write_validation_report(canonical, canonical_report_path)
        else:
            _write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = public_gateway_session(result.session)
        if isinstance(result, ServiceRuntimePendingResult):
            payload["outcome"] = result.outcome
            payload["retry_selector"] = result.retry_selector()
            payload["scheduler_action"] = result.scheduler_action
            payload["relay_action"] = result.relay_action
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            report_already_written = False
            if report_id[0] is not None:
                with suppress(ConfigurationError):
                    report_already_written = (
                        load_validation_report(canonical_report_path).report_id == report_id[0]
                    )
            if not report_already_written:
                artifact_sha256: str | None = None
                if validation_artifact is not None:
                    with suppress(OSError):
                        artifact_sha256 = sha256_file(validation_artifact)
                failed_report = new_live_validation_report(
                    scenario="gateway-runtime",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=artifact_sha256,
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "gateway.resume-runtime",
                    "resume exact scheduler-backed gateway runtime",
                    exc,
                )
                recorder.finish(exc)
                recorder.write(canonical_report_path)
            raise

    _run_or_exit(guarded_action)


@gateway_app.command("browser-attach", hidden=True)
def gateway_browser_attach(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ttl_seconds: Annotated[
        int,
        typer.Option(help="Short-lived browser capability lifetime in seconds."),
    ] = 1_800,
    bind_port: Annotated[
        int | None,
        typer.Option(help="Optional desktop loopback proxy port."),
    ] = None,
) -> None:
    """Issue one sandbox-browser attachment capability for a verified gateway."""

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        result = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        ).browser_attach(
            session_id=session_id,
            ttl_seconds=ttl_seconds,
            bind_port=bind_port,
        )
        # This is the sole one-time capability output. Do not route it through
        # routine gateway serialization or persist it in the gateway record.
        typer.echo(result.model_dump_json(indent=2))

    _run_or_exit(action)


@gateway_app.command("browser-detach", hidden=True)
def gateway_browser_detach(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    attachment_id: Annotated[
        str,
        typer.Option(help="Exact browser attachment identity to revoke."),
    ],
) -> None:
    """Revoke one exact browser capability and stop its owned proxy."""

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        result = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        ).browser_detach(session_id=session_id, attachment_id=attachment_id)
        typer.echo(_public_json(result.model_dump(mode="json")))

    _run_or_exit(action)


@gateway_app.command("detach-runtime")
@_acceptance_report_command
def gateway_detach_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime detach JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in gateway detach evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop the owned desktop connector while retaining the remote runtime and job."""
    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = _new_cleanup_acceptance_report(
        scenario="gateway-runtime",
        cluster=cluster,
        mode="detach",
        resource_kind="gateway_record",
        resource_id=session_id,
        action="retain",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=False,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    write_validation_report(seed_report, canonical_report_path)

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        )
        result = supervisor.detach(session_id=session_id)
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        _write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = result.json_payload()
        payload["session"] = public_gateway_session(result.session)
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))
        if (
            result.errors
            or result.residual_resources
            or canonical.status is not ValidationStatus.PASSED
        ):
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="gateway-runtime",
                cluster=cluster,
                check_id="gateway.detach-runtime",
                summary="detach owned gateway runtime resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    _run_or_exit(guarded_action)


@gateway_app.command("attach-runtime")
def gateway_attach_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
) -> None:
    """Recreate the desktop connector for a detached owned runtime."""

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token=_resolve_env_secret(token, definition.frp_transport.token_env, "frp token"),
            secret_key=_resolve_env_secret(
                secret_key,
                definition.frp_transport.stcp_secret_env,
                "stcp secret",
            ),
        )
        result = supervisor.attach(session_id=session_id)
        payload = public_gateway_session(result.session)
        if isinstance(result, ServiceRuntimePendingResult):
            gateway = cast(dict[str, object], payload.get("gateway", {}))
            for key in (
                "connect_url",
                "health_url",
                "stream_url",
                "events_url",
                "state_url",
                "command_url",
                "compatibility_urls",
            ):
                gateway.pop(key, None)
            payload["gateway"] = gateway
        payload.update(
            {
                "outcome": (
                    result.outcome if isinstance(result, ServiceRuntimePendingResult) else "ready"
                ),
                "retry_selector": (
                    result.retry_selector()
                    if isinstance(result, ServiceRuntimePendingResult)
                    else None
                ),
                "scheduler_action": "none",
                "relay_action": "none",
                "scheduler_cancel_requested": False,
            }
        )
        typer.echo(_public_json(payload))

    _run_or_exit(action)


@gateway_app.command("stop-runtime")
@_acceptance_report_command
def gateway_stop_runtime(
    session_id: str,
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    cancel_scheduler_job: Annotated[
        bool,
        typer.Option(
            "--cancel-scheduler-job/--keep-scheduler-job",
            help="Cancel the scheduler job after closing relay connectors.",
        ),
    ] = False,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical gateway-runtime cleanup JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in gateway cleanup evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Stop owned runtime relay connectors and optionally cancel scheduler work."""
    canonical_report_path = validation_report or default_report_path(cluster)
    seed_report = _new_cleanup_acceptance_report(
        scenario="gateway-runtime",
        cluster=cluster,
        mode="teardown",
        resource_kind="gateway_record",
        resource_id=session_id,
        action="close",
        cancel_relay_jobs=False,
        cancel_scheduler_jobs=cancel_scheduler_job,
        stop_worker=False,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact=validation_artifact,
    )
    canonical_report: list[LiveValidationReport | None] = [seed_report]
    write_validation_report(seed_report, canonical_report_path)

    def action() -> None:
        definition = _require_cluster(cluster)
        settings = RelaySettings.from_env()
        supervisor = ServiceRuntimeSupervisor(
            settings=settings,
            queue=storage_managed_queue(settings),
            cluster=cluster,
            definition=definition,
            token="",
            secret_key="",
        )
        result = supervisor.stop(
            session_id=session_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        canonical = result.to_live_validation_report(
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact_sha256=(
                sha256_file(validation_artifact) if validation_artifact is not None else None
            ),
        )
        canonical = canonical.model_copy(
            update={"report_id": seed_report.report_id, "started_at": seed_report.started_at}
        )
        canonical_report[0] = canonical
        _write_remote_verified_report(canonical, definition, canonical_report_path)
        payload = result.json_payload()
        payload["session"] = public_gateway_session(result.session)
        payload["validation_report"] = str(canonical_report_path.resolve())
        typer.echo(_public_json(payload))
        canonical_ok = canonical.status is ValidationStatus.PASSED
        if result.errors or result.residual_resources or not canonical_ok:
            raise typer.Exit(code=1)

    def guarded_action() -> None:
        try:
            action()
        except typer.Exit:
            raise
        except BaseException as exc:
            _write_failed_acceptance_report(
                path=canonical_report_path,
                scenario="gateway-runtime",
                cluster=cluster,
                check_id="gateway.stop-runtime",
                summary="stop owned gateway runtime resources",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=canonical_report[0],
            )
            raise

    _run_or_exit(guarded_action)


@monitor_app.command("add-regex")
def monitor_add_regex(
    job_id: str,
    pattern: Annotated[str, typer.Option(help="Python regular expression to match.")],
    action: Annotated[
        MonitorRuleAction,
        typer.Option(help="Action to take when the rule matches."),
    ] = MonitorRuleAction.EMIT_EVENT,
    event_type: Annotated[
        list[str] | None,
        typer.Option(help="Event type to inspect; repeat for multiple types."),
    ] = None,
    action_payload_json: Annotated[
        str,
        typer.Option(help="JSON object used by actions such as submit_agent."),
    ] = "{}",
) -> None:
    """Create a generic regex monitor rule over a job event stream."""
    action_payload = _json_object(action_payload_json)
    rule = ClioCoreQueue(RelaySettings.from_env().core_dir).append_monitor_rule(
        MonitorRule(
            job_id=job_id,
            pattern=pattern,
            action=action,
            event_types=event_type or [],
            action_payload=action_payload,
        )
    )
    typer.echo(rule.model_dump_json(indent=2))


@monitor_app.command("list")
def monitor_list(
    job_id: Annotated[
        str | None,
        typer.Option(help="Optional job id filter."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global monitor-rule source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum monitor-rule source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
) -> None:
    """List one stable source window of durable monitor rules as JSON."""
    rules, next_cursor, total = ClioCoreQueue(
        RelaySettings.from_env().core_dir
    ).list_monitor_rules_page(
        cursor=cursor,
        limit=limit,
        job_id=job_id,
    )
    typer.echo(
        json.dumps(
            {
                "rules": [rule.model_dump(mode="json") for rule in rules],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_monitor_rule_sequence_high_water",
                "filters_apply_within_source_window": True,
            },
            indent=2,
        )
    )


@monitor_app.command("run-once")
def monitor_run_once(
    limit: Annotated[int, typer.Option(help="Maximum events read per rule.")] = 100,
) -> None:
    """Evaluate enabled monitor rules once."""
    _run_or_exit(
        lambda: typer.echo(
            json.dumps(evaluate_monitor_rules(_managed_queue_from_env(), limit=limit), indent=2)
        )
    )


@agent_app.command("run")
def agent_run(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    prompt: Annotated[str, typer.Option(help="Prompt file path on the cluster.")],
    mcp_config: Annotated[
        str | None,
        typer.Option(help="Optional MCP config/profile path on the cluster."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
) -> None:
    """Submit a remote agent task on a configured cluster."""
    definition = _require_cluster(cluster)
    artifact_uses = _artifact_use_refs(used_artifact)
    key = idempotency_key or (
        f"agent:{cluster}:{prompt}:{mcp_config}" + _artifact_use_idempotency_suffix(artifact_uses)
    )
    if should_execute_on_cluster(definition):
        args = [
            "agent",
            "run",
            "--cluster",
            cluster,
            "--prompt",
            prompt,
            "--idempotency-key",
            key,
        ]
        if mcp_config is not None:
            args.extend(["--mcp-config", mcp_config])
        for ref in _artifact_use_refs(used_artifact):
            args.extend(["--used-artifact", _artifact_use_cli_value(ref)])
        _run_remote_or_exit(definition, args)
        return
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.REMOTE_AGENT,
        spec=RemoteAgentTaskSpec(prompt_path=prompt, mcp_config_path=mcp_config),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@app.command("mcp-call")
def mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    server: Annotated[str, typer.Option(help="Remote MCP server name.")],
    operation: Annotated[
        McpOperation,
        typer.Option(help="Remote MCP operation: tools/call or tools/list."),
    ] = McpOperation.TOOLS_CALL,
    tool: Annotated[
        str | None,
        typer.Option(help="Remote MCP tool name. Required for tools/call."),
    ] = None,
    server_arg: Annotated[
        list[str] | None,
        typer.Option(help="Additional remote MCP server argument. Repeatable."),
    ] = None,
    env_from: Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Child=SOURCE environment reference. Repeatable; values are resolved only "
                "by the endpoint worker."
            )
        ),
    ] = None,
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the remote MCP tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the remote MCP tool."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote MCP call."),
    ] = None,
    expected_server_artifact_digest: Annotated[
        str | None,
        typer.Option(help="Expected discovery-time MCP server artifact SHA-256 binding."),
    ] = None,
    expected_registered_contract: Annotated[
        str | None,
        typer.Option(
            help="Internal expected operator-registered semantic contract.",
            hidden=True,
        ),
    ] = None,
    control_query_evidence_json: Annotated[
        str | None,
        typer.Option(
            help="Internal discovery evidence offered for server-side admission validation.",
            hidden=True,
        ),
    ] = None,
) -> None:
    """Submit a durable remote MCP call or schema-discovery operation."""
    definition = _require_cluster(cluster)
    if operation == McpOperation.TOOLS_CALL and not tool:
        raise typer.BadParameter("--tool is required for tools/call")
    if operation == McpOperation.TOOLS_LIST and tool is not None:
        raise typer.BadParameter("--tool must be omitted for tools/list")
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = _json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    if operation == McpOperation.TOOLS_LIST and arguments:
        raise typer.BadParameter("tools/list does not accept arguments")
    try:
        control_query_evidence = (
            McpControlQueryEvidence.model_validate_json(control_query_evidence_json)
            if control_query_evidence_json is not None
            else None
        )
    except ValidationError as exc:
        raise typer.BadParameter("--control-query-evidence-json is invalid") from exc
    digest = hashlib.sha256(
        json.dumps(
            {"operation": operation.value, "tool": tool, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    server_args = server_arg or []
    environment_references = _environment_references(env_from)
    artifact_uses = _artifact_use_refs(used_artifact)
    if should_execute_on_cluster(definition):
        remote_arguments_path: str | None = None
        remote_command = [
            "mcp-call",
            "--cluster",
            cluster,
            "--server",
            server,
            "--operation",
            operation.value,
        ]
        if idempotency_key is not None:
            remote_command.extend(["--idempotency-key", idempotency_key])
        if control_query_evidence is not None:
            remote_command.extend(
                [
                    "--control-query-evidence-json",
                    json.dumps(
                        control_query_evidence.model_dump(mode="json"),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ]
            )
        if tool is not None:
            remote_arguments_path = (
                ".local/share/clio-relay/desktop-submissions/"
                f"mcp-{digest[:16]}-{uuid4().hex}/arguments.json"
            )
            remote_command.extend(["--tool", tool, "--arguments-json-file", remote_arguments_path])
        for child_name, source_name in sorted(environment_references.items()):
            remote_command.extend(["--env-from", f"{child_name}={source_name}"])
        if expected_server_artifact_digest is not None:
            remote_command.extend(
                [
                    "--expected-server-artifact-digest",
                    expected_server_artifact_digest,
                ]
            )
        if expected_registered_contract is not None:
            remote_command.extend(["--expected-registered-contract", expected_registered_contract])
        for ref in _artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", _artifact_use_cli_value(ref)])
        try:
            if remote_arguments_path is not None:
                write_remote_file(
                    definition,
                    remote_arguments_path,
                    json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                )
            _run_remote_or_exit(
                definition,
                remote_command
                + (
                    ["--timeout-seconds", str(timeout_seconds)]
                    if timeout_seconds is not None
                    else []
                )
                + [item for value in server_args for item in ("--server-arg", value)],
            )
        finally:
            if remote_arguments_path is not None:
                remove_remote_file(
                    definition,
                    remote_arguments_path,
                    remove_empty_parent=True,
                )
        return
    queue = _managed_queue_from_env()
    try:
        try:
            resolved_admission_class, admission_authority = resolve_registered_remote_mcp_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                server=server,
                server_args=server_args,
                env_from=environment_references,
                operation=operation,
                tool=tool,
                expected_server_artifact_digest=expected_server_artifact_digest,
                evidence=control_query_evidence,
                expected_registered_contract=expected_registered_contract,
                timeout_seconds=timeout_seconds,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        admission_identity: dict[str, object] = {
            "server": server,
            "args": server_args,
            "env_from": environment_references,
            "expected_server_artifact_digest": expected_server_artifact_digest,
        }
        if expected_registered_contract is not None:
            admission_identity["expected_registered_contract"] = expected_registered_contract
        if (
            resolved_admission_class is McpAdmissionClass.CONTROL_QUERY
            or admission_authority is not None
        ):
            admission_identity.update(
                {
                    "timeout_seconds": timeout_seconds,
                    "admission_class": resolved_admission_class.value,
                    "admission_authority": (
                        None
                        if admission_authority is None
                        else admission_authority.model_dump(mode="json")
                    ),
                }
            )
        server_digest = hashlib.sha256(
            json.dumps(
                admission_identity,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        key = idempotency_key or (
            f"mcp:{cluster}:{server_digest}:{operation.value}:{tool}:{digest}"
            + _artifact_use_idempotency_suffix(artifact_uses)
        )
        metadata = (
            {}
            if admission_authority is None
            else {MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(mode="json")}
        )
        job = RelayJob(
            cluster=cluster,
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=server,
                server_args=server_args,
                env_from=environment_references,
                expected_server_artifact_digest=expected_server_artifact_digest,
                expected_registered_contract=expected_registered_contract,
                admission_class=resolved_admission_class,
                operation=operation,
                tool=tool,
                arguments=arguments,
                timeout_seconds=timeout_seconds,
            ),
            idempotency_key=key,
            used_artifact_refs=artifact_uses,
            metadata=metadata,
        )
        try:
            saved = queue.submit_job(job)
        except StorageAdmissionError as exc:
            _echo_storage_admission_error(exc)
            raise typer.Exit(code=1) from exc
    finally:
        queue.close()
    typer.echo(saved.job_id)


@app.command("jarvis-mcp-call")
def jarvis_mcp_call(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    operation: Annotated[
        McpOperation,
        typer.Option(help="JARVIS MCP operation: tools/call or tools/list."),
    ] = McpOperation.TOOLS_CALL,
    tool: Annotated[
        str | None,
        typer.Option(help="JARVIS MCP tool name. Required for tools/call."),
    ] = None,
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the JARVIS MCP tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for the JARVIS MCP tool."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Submit/retry idempotency key."),
    ] = None,
    used_artifact: Annotated[
        list[str] | None,
        typer.Option(
            "--used-artifact",
            help="Dependency as ARTIFACT_ID=SHA256 or canonical JSON with provenance. Repeatable.",
        ),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote JARVIS MCP call."),
    ] = None,
    expected_server_artifact_digest: Annotated[
        str | None,
        typer.Option(help="Expected discovery-time JARVIS MCP artifact SHA-256 binding."),
    ] = None,
) -> None:
    """Submit a JARVIS MCP tool call that runs on the target cluster."""
    running_on_target = (
        os.getenv("CLIO_RELAY_CLI_MODE") == "local"
        and os.getenv("CLIO_RELAY_REMOTE_CLUSTER") == cluster
    )
    definition = None if running_on_target else _require_cluster(cluster)
    if operation == McpOperation.TOOLS_CALL and not tool:
        raise typer.BadParameter("--tool is required for tools/call")
    if operation == McpOperation.TOOLS_LIST and tool is not None:
        raise typer.BadParameter("--tool must be omitted for tools/list")
    if arguments_json_file is not None and arguments_json != "{}":
        raise typer.BadParameter("use either --arguments-json or --arguments-json-file, not both")
    arguments = _json_object(
        arguments_json_file.read_text(encoding="utf-8-sig")
        if arguments_json_file is not None
        else arguments_json
    )
    if operation == McpOperation.TOOLS_LIST and arguments:
        raise typer.BadParameter("tools/list does not accept arguments")
    try:
        resolved_admission_class, admission_authority = resolve_pinned_mcp_admission(
            operation=operation,
            tool=tool,
            expected_server_artifact_digest=expected_server_artifact_digest,
            pinned_control_query=(tool is not None and is_virtual_jarvis_control_query(tool)),
            timeout_seconds=timeout_seconds,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if resolved_admission_class is McpAdmissionClass.CONTROL_QUERY and timeout_seconds is None:
        timeout_seconds = MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
    digest = hashlib.sha256(
        json.dumps(
            {"operation": operation.value, "tool": tool, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    artifact_uses = _artifact_use_refs(used_artifact)
    legacy_key = (
        f"mcp:{cluster}:jarvis:{operation.value}:{tool}:{digest}:"
        f"{expected_server_artifact_digest or 'unbound'}"
        + _artifact_use_idempotency_suffix(artifact_uses)
    )
    key = idempotency_key or (
        legacy_key
        if resolved_admission_class is McpAdmissionClass.WORKLOAD
        else (
            f"{legacy_key}:{resolved_admission_class.value}:"
            f"{admission_authority.source if admission_authority is not None else 'none'}:"
            f"timeout={timeout_seconds}"
        )
    )
    if definition is not None and should_execute_on_cluster(definition):
        remote_args: str | None = None
        remote_command = [
            "jarvis-mcp-call",
            "--cluster",
            cluster,
            "--operation",
            operation.value,
            "--idempotency-key",
            key,
        ]
        if tool is not None:
            remote_args = (
                ".local/share/clio-relay/desktop-submissions/"
                f"jarvis-mcp-{digest[:16]}-{uuid4().hex}/arguments.json"
            )
            remote_command.extend(["--tool", tool, "--arguments-json-file", remote_args])
        if expected_server_artifact_digest is not None:
            remote_command.extend(
                [
                    "--expected-server-artifact-digest",
                    expected_server_artifact_digest,
                ]
            )
        for ref in _artifact_use_refs(used_artifact):
            remote_command.extend(["--used-artifact", _artifact_use_cli_value(ref)])
        try:
            if remote_args is not None:
                write_remote_file(
                    definition,
                    remote_args,
                    json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                )
            _run_remote_or_exit(
                definition,
                remote_command
                + (
                    ["--timeout-seconds", str(timeout_seconds)]
                    if timeout_seconds is not None
                    else []
                ),
            )
        finally:
            if remote_args is not None:
                remove_remote_file(definition, remote_args, remove_empty_parent=True)
        return
    server = jarvis_mcp_server()
    server_args = jarvis_mcp_server_args()
    metadata = (
        {}
        if admission_authority is None
        else {MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(mode="json")}
    )
    job = RelayJob(
        cluster=cluster,
        kind=JobKind.MCP_CALL,
        spec=McpCallSpec(
            server=server,
            server_args=server_args,
            env_from=jarvis_mcp_env_from(),
            expected_server_artifact_digest=expected_server_artifact_digest,
            expected_jarvis_cd_lock_binding=jarvis_cd_lock_binding_expectation(),
            admission_class=resolved_admission_class,
            operation=operation,
            tool=tool,
            arguments=arguments,
            timeout_seconds=timeout_seconds,
        ),
        idempotency_key=key,
        used_artifact_refs=artifact_uses,
        metadata=metadata,
    )
    saved = _submit_managed_job(job)
    typer.echo(saved.job_id)


@app.command("jarvis-mcp-refresh")
def jarvis_mcp_refresh(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for durable tools/list discovery."),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Discovery job polling interval."),
    ] = 2,
) -> None:
    """Refresh the verified JARVIS contract and pre-launch artifact binding."""
    definition = _require_cluster(cluster)

    def action() -> None:
        queue = _managed_queue_from_env()
        queue.initialize()
        job_id, result, artifacts, artifact_payload = _run_jarvis_remote_contract_discovery(
            cluster=cluster,
            definition=definition,
            queue=queue,
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        entry, binding = _persist_jarvis_remote_contract_discovery(
            cluster=cluster,
            discovery_job_id=job_id,
            result=result,
            artifacts=artifacts,
            artifact_payload=artifact_payload,
        )
        typer.echo(
            json.dumps(
                {
                    "cluster": cluster,
                    "discovery_job_id": job_id,
                    "schema_digest": entry.schema_digest,
                    "server_artifact_digest": binding,
                    "expires_at": entry.expires_at.isoformat(),
                    "tool_names": sorted(tool.name for tool in entry.tools),
                    "cache_path": str(
                        default_remote_mcp_cache_path(
                            registry_path=default_registry_path(),
                        )
                    ),
                },
                indent=2,
            )
        )

    _run_or_exit(action)


@app.command("jarvis-mcp-validate")
@_acceptance_report_command
def jarvis_mcp_validate(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    package_search_query: Annotated[
        str,
        typer.Option(
            help=("Non-blank application query used to prove bounded JARVIS package discovery."),
        ),
    ] = "",
    arguments_json: Annotated[
        str,
        typer.Option(help="JSON object arguments for the virtual jarvis_run tool."),
    ] = "{}",
    arguments_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object argument file for virtual jarvis_run."),
    ] = None,
    resume_report: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Pending report to resume its exact idempotent jarvis_run dispatch or "
                "JARVIS execution query; never creates a new workload identity."
            ),
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile used for tools/list and tools/call."),
    ] = "user",
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(
            help=(
                "Maximum observation window for durable JARVIS MCP calls, not the workload "
                "lifetime. Expiry after an idempotent intent, relay receipt, or execution "
                "identity writes a resumable pending checkpoint without cancellation."
            ),
            min=1,
        ),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable call polling interval.", min=0.05),
    ] = 2,
    report: Annotated[
        Path | None,
        typer.Option(help="Canonical release-evidence JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in canonical evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Exercise JARVIS run/query semantics and persist release acceptance evidence."""
    report_path = report or resume_report or default_report_path(cluster)
    failure_report_path = report_path
    if resume_report is not None and report_path.resolve() == resume_report.resolve():
        suffix = report_path.suffix or ".json"
        failure_report_path = report_path.with_name(
            f"{report_path.stem}.resume-failure-{uuid4().hex}{suffix}"
        )
    report_written = [False]

    def preflight() -> tuple[
        dict[str, Any],
        ClusterDefinition,
        str,
        dict[str, Any] | None,
    ]:
        if profile not in {"user", "admin", "operator", "all"}:
            raise typer.BadParameter("--profile must be user, admin, operator, or all")
        definition = _require_cluster(cluster)
        if resume_report is not None:
            if package_search_query or arguments_json != "{}" or arguments_json_file is not None:
                raise typer.BadParameter(
                    "--resume-report cannot be combined with run or package-search arguments"
                )
            checkpoint = _load_jarvis_validation_resume_checkpoint(
                resume_report,
                cluster=cluster,
            )
            return {}, definition, "", checkpoint
        normalized_package_search_query = " ".join(package_search_query.split())
        if not normalized_package_search_query:
            raise typer.BadParameter("--package-search-query must not be blank")
        if len(normalized_package_search_query) > 256:
            raise typer.BadParameter("--package-search-query must not exceed 256 characters")
        arguments_source = _json_text_from_option(arguments_json, arguments_json_file)
        arguments = _json_object(arguments_source)
        if redact_sensitive_values(arguments) != arguments:
            raise typer.BadParameter(
                "JARVIS validation arguments cannot contain credential-valued fields because "
                "durable resume reports are always credential-redacted"
            )
        if "cluster" in arguments:
            raise typer.BadParameter(
                "JARVIS tool arguments must not contain reserved key 'cluster'"
            )
        if "wait" in arguments:
            raise typer.BadParameter(
                "jarvis_run is handle-first and does not accept internal wait; remove 'wait' "
                "and let jarvis-mcp-validate observe workload lifecycle with "
                "jarvis_get_execution"
            )
        if not isinstance(arguments.get("pipeline_id"), str):
            raise typer.BadParameter("jarvis-mcp-validate requires a string pipeline_id argument")
        return arguments, definition, normalized_package_search_query, None

    try:
        (
            arguments,
            definition,
            normalized_package_search_query,
            resume_checkpoint,
        ) = preflight()
    except BaseException as exc:
        _write_failed_acceptance_report(
            path=failure_report_path,
            scenario="remote-mcp",
            cluster=cluster,
            check_id="jarvis-mcp.preflight",
            summary="validate virtual JARVIS MCP acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        settings = RelaySettings.from_env()
        queue = storage_managed_queue(settings)
        queue.initialize()

        def emit(validation: LiveValidationReport, *, attach_worker: bool = False) -> None:
            if attach_worker and should_execute_on_cluster(definition):
                attach_verified_worker_identity(validation, _remote_worker_info(definition))
            write_validation_report(validation, report_path)
            report_written[0] = True
            typer.echo(validation.model_dump_json(indent=2))
            if validation.status is ValidationStatus.FAILED:
                raise typer.Exit(code=1)

        def retain_existing_pending_report() -> None:
            if resume_report is None:  # pragma: no cover - guarded by resume_checkpoint
                raise RelayError("JARVIS validation resume source disappeared")
            emit(load_validation_report(resume_report))

        def finish_execution_query(
            *,
            builder_inputs: dict[str, Any],
            execution_query: _JarvisExecutionQueryAcceptance | _JarvisExecutionQueryPending,
            checkpoint_profile: str,
        ) -> None:
            selector = execution_query.retry_selector()
            builder_inputs = {
                **builder_inputs,
                "scheduler_cluster": selector["scheduler_cluster"],
            }
            observations = (
                []
                if isinstance(execution_query, _JarvisExecutionQueryPending)
                else list(execution_query.lifecycle_observations)
            )
            checkpoint = {
                "schema_version": _JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,
                "phase": _JARVIS_VALIDATION_PHASE_QUERY,
                "observation_state": "not_observed" if not observations else "observed",
                "profile": checkpoint_profile,
                "retry_selector": selector,
                "builder_inputs": builder_inputs,
                "lifecycle_observations": observations,
            }
            if isinstance(execution_query, _JarvisExecutionQueryPending):
                validation = _build_unobserved_jarvis_query_pending_report(
                    builder_inputs=builder_inputs,
                    execution_query=execution_query,
                    checkpoint=checkpoint,
                )
            else:
                validation = build_jarvis_mcp_validation_report(
                    **builder_inputs,
                    query_tools_list_response=execution_query.tools_list_response,
                    query_call_response=execution_query.call_response,
                    query_call_job_id=execution_query.call_job_id,
                    query_call_status=execution_query.call_status,
                    query_artifacts=execution_query.artifacts,
                    query_mcp_result=execution_query.mcp_result,
                    query_provenance=execution_query.provenance,
                    query_initialize_response=execution_query.initialize_response,
                    query_stdio_evidence=execution_query.stdio_evidence,
                    query_lifecycle_observations=observations,
                )
                if execution_query.outcome != "terminal":
                    validation = _mark_jarvis_validation_pending(
                        validation,
                        execution_query=execution_query,
                        resume_checkpoint=checkpoint,
                    )
            emit(validation, attach_worker=validation.status is not ValidationStatus.PENDING)

        checkpoint = resume_checkpoint
        checkpoint_profile = profile
        if checkpoint is not None:
            checkpoint_profile = cast(str, checkpoint["profile"])
            phase = checkpoint.get("phase", _JARVIS_VALIDATION_PHASE_QUERY)
            if phase == _JARVIS_VALIDATION_PHASE_QUERY:
                selector = cast(dict[str, Any], checkpoint["retry_selector"])
                query_selector: dict[str, object] = {
                    **cast(dict[str, object], selector),
                    "last_query_job_id": None,
                }
                execution_query = _run_post_run_jarvis_execution_query(
                    cluster=cluster,
                    definition=definition,
                    queue=queue,
                    profile=checkpoint_profile,
                    pipeline_id=cast(str, selector["pipeline_id"]),
                    execution_id=cast(str, selector["execution_id"]),
                    retry_selector=query_selector,
                    wait_timeout_seconds=wait_timeout_seconds,
                    poll_seconds=poll_seconds,
                )
                _require_same_jarvis_resume_identity(
                    expected=selector,
                    observed=execution_query.retry_selector(),
                )
                if isinstance(execution_query, _JarvisExecutionQueryPending):
                    retain_existing_pending_report()
                    return
                prior_observations = [
                    cast(dict[str, Any], observation)
                    for observation in cast(list[object], checkpoint["lifecycle_observations"])
                    if isinstance(observation, dict)
                ]
                execution_query = replace(
                    execution_query,
                    lifecycle_observations=_merge_jarvis_execution_query_observations(
                        prior_observations,
                        execution_query.lifecycle_observations,
                    ),
                )
                finish_execution_query(
                    builder_inputs=cast(dict[str, Any], checkpoint["builder_inputs"]),
                    execution_query=execution_query,
                    checkpoint_profile=checkpoint_profile,
                )
                return

        if checkpoint is None:
            (
                remote_discovery_job_id,
                remote_tools_list_result,
                remote_discovery_artifacts,
                remote_discovery_payload,
            ) = _run_jarvis_remote_contract_discovery(
                cluster=cluster,
                definition=definition,
                queue=queue,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            _persist_jarvis_remote_contract_discovery(
                cluster=cluster,
                discovery_job_id=remote_discovery_job_id,
                result=remote_tools_list_result,
                artifacts=remote_discovery_artifacts,
                artifact_payload=remote_discovery_payload,
            )
            package_search = _run_jarvis_package_search_query(
                cluster=cluster,
                definition=definition,
                queue=queue,
                profile=profile,
                query=normalized_package_search_query,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            validation_artifact_sha256 = (
                sha256_file(validation_artifact) if validation_artifact is not None else None
            )
            pre_dispatch_inputs: dict[str, Any] = {
                "cluster": cluster,
                "tool": "jarvis_run",
                "remote_tools_list_result": remote_tools_list_result,
                "remote_discovery_job_id": remote_discovery_job_id,
                "remote_discovery_artifacts": remote_discovery_artifacts,
                "package_search_query": normalized_package_search_query,
                "package_search_tools_list_response": package_search.tools_list_response,
                "package_search_call_response": package_search.call_response,
                "package_search_call_job_id": package_search.call_job_id,
                "package_search_call_status": package_search.call_status,
                "package_search_artifacts": package_search.artifacts,
                "package_search_mcp_result": package_search.mcp_result,
                "package_search_provenance": package_search.provenance,
                "package_search_initialize_response": package_search.initialize_response,
                "package_search_stdio_evidence": package_search.stdio_evidence,
                "launcher": validation_launcher,
                "install_source": validation_install_source,
                "artifact_sha256": validation_artifact_sha256,
            }
            idempotency_key = _new_jarvis_validation_idempotency_key(
                cluster=cluster,
                profile=profile,
                arguments=arguments,
            )
            execution_intent = _jarvis_run_execution_intent(
                cluster=cluster,
                profile=profile,
                arguments=arguments,
                idempotency_key=idempotency_key,
            )
            checkpoint = _new_jarvis_intent_resume_checkpoint(
                execution_intent=execution_intent,
                pre_dispatch_inputs=pre_dispatch_inputs,
            )
            # Persist the replayable identity before crossing the ambiguous stdio boundary.
            # A process or host failure can therefore resume with this exact key.
            write_validation_report(_new_jarvis_intent_pending_report(checkpoint), report_path)
        else:
            execution_intent = cast(dict[str, object], checkpoint["execution_intent"])
            pre_dispatch_inputs = cast(dict[str, Any], checkpoint["pre_dispatch_inputs"])

        if checkpoint["phase"] == _JARVIS_VALIDATION_PHASE_INTENT:
            try:
                stdio_session = run_packaged_mcp_stdio_session(
                    profile=checkpoint_profile,
                    tool="jarvis_run",
                    arguments=cast(dict[str, Any], execution_intent["arguments"]),
                    timeout_seconds=min(60.0, max(0.001, wait_timeout_seconds)),
                )
            except ObservationTimeoutError:
                if resume_checkpoint is not None:
                    retain_existing_pending_report()
                else:
                    emit(_new_jarvis_intent_pending_report(checkpoint))
                return
            call_response = stdio_session.tools_call_response
            job_id = _mcp_response_job_id(call_response)
            builder_inputs: dict[str, Any] = {
                **pre_dispatch_inputs,
                "scheduler_cluster": None,
                "tools_list_response": stdio_session.tools_list_response,
                "call_response": call_response,
                "call_job_id": job_id,
                "call_status": {},
                "artifacts": [],
                "mcp_result": None,
                "provenance": None,
                "runtime_metadata": None,
                "progress": [],
                "live_progress_observation": None,
                "initialize_response": stdio_session.initialize_response,
                "stdio_evidence": stdio_session.evidence(),
            }
            checkpoint = _promote_jarvis_intent_to_dispatch_checkpoint(
                checkpoint,
                job_id=job_id,
                builder_inputs=builder_inputs,
            )

        try:
            builder_inputs = _complete_jarvis_run_dispatch(
                definition=definition,
                queue=queue,
                checkpoint=checkpoint,
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
        except ObservationTimeoutError:
            emit(_build_jarvis_dispatch_pending_report(checkpoint))
            return
        raw_runtime_metadata = builder_inputs.get("runtime_metadata")
        runtime_metadata = (
            cast(dict[str, Any], raw_runtime_metadata)
            if isinstance(raw_runtime_metadata, dict)
            else None
        )
        if runtime_metadata is None:
            raise RelayError("JARVIS run metadata artifact is unavailable")
        pipeline_id = runtime_metadata.get("pipeline_id")
        execution_id = runtime_metadata.get("execution_id")
        if not isinstance(pipeline_id, str) or not pipeline_id:
            raise RelayError("JARVIS run metadata omitted the pipeline_id required for its query")
        if not isinstance(execution_id, str) or not execution_id:
            raise RelayError("JARVIS run metadata omitted the execution_id required for its query")
        retry_selector = _jarvis_execution_retry_selector_from_runtime_metadata(
            runtime_metadata,
            cluster=cluster,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
        )
        execution_query = _run_post_run_jarvis_execution_query(
            cluster=cluster,
            definition=definition,
            queue=queue,
            profile=checkpoint_profile,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            retry_selector=retry_selector,
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        finish_execution_query(
            builder_inputs=builder_inputs,
            execution_query=execution_query,
            checkpoint_profile=checkpoint_profile,
        )

    def guarded_action() -> None:
        try:
            action()
        except BaseException as exc:
            if not report_written[0]:
                failed_report = new_live_validation_report(
                    scenario="remote-mcp",
                    cluster=cluster,
                    launcher=validation_launcher,
                    install_source=validation_install_source,
                    artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                )
                recorder = ValidationRecorder(failed_report)
                recorder.record_failure(
                    "jarvis-mcp.completed", "complete virtual JARVIS MCP acceptance", exc
                )
                recorder.finish(exc)
                recorder.write(failure_report_path)
            raise

    _run_or_exit(guarded_action)


@app.command("mcp-server")
def mcp_server(
    profile: Annotated[
        str,
        typer.Option(help="MCP tool profile: user, admin, operator, or all."),
    ] = "user",
) -> None:
    """Serve relay job tools over stdio MCP."""
    serve_stdio(profile=profile)


@api_app.command("start")
def api_start(
    host: Annotated[str, typer.Option(help="HTTP bind address.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="HTTP bind port.")] = 8765,
    require_token: Annotated[
        bool,
        typer.Option(help="Fail if CLIO_RELAY_API_TOKEN is not configured."),
    ] = False,
) -> None:
    """Start the desktop-facing HTTP API."""
    if require_token and RelaySettings.from_env().api_token is None:
        raise typer.BadParameter("CLIO_RELAY_API_TOKEN is required with --require-token")
    try:
        _require_process_bound_session_api_release()
    except ConfigurationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    # Import the process-bound app while its one-time gated environment is intact.
    # The startup receipt then scrubs the owner token from the environment, while
    # the app retains the validated settings needed to prove this session's identity.
    from clio_relay.http_api import app as relay_http_app

    publish_owned_session_api_startup_receipt()
    uvicorn.run(relay_http_app, host=host, port=port)


@agent_app.command("render-mcp-config")
def agent_render_mcp_config(
    output: Annotated[
        Path | None,
        typer.Option(help="Optional path to write the agent MCP profile TOML."),
    ] = None,
) -> None:
    """Render an agent profile that exposes the relay MCP tools."""
    rendered = render_agent_mcp_profile(settings=RelaySettings.from_env())
    if output is None:
        typer.echo(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    typer.echo(output)


@app.command("installation-info")
def show_installation_info() -> None:
    """Print the current package identity and durable cluster install receipt."""
    _run_or_exit(lambda: typer.echo(json.dumps(installation_info(), indent=2, default=str)))


@app.command("bootstrap-inspect", hidden=True)
def bootstrap_inspect(
    invocation_id: Annotated[
        str,
        typer.Option(help="Unique bootstrap invocation identity."),
    ],
    repair: Annotated[
        bool,
        typer.Option(
            "--repair/--inspect-only",
            help="Apply only the typed payload-free repair returned by an inspect-only call.",
        ),
    ] = False,
) -> None:
    """Perform a bounded payload-free inspection or explicit typed repair."""

    def _inspect_locked() -> None:
        encoded = os.environ.get("CLIO_RELAY_BOOTSTRAP_DESIRED_STATE_BASE64", "")
        if not encoded or len(encoded) > 128 * 1024:
            raise ConfigurationError("bootstrap desired state environment is missing or oversized")
        if re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", invocation_id) is None:
            raise ConfigurationError("bootstrap invocation identity is invalid")
        try:
            raw = base64.b64decode(encoded, validate=True)
            if len(raw) > 64 * 1024:
                raise ConfigurationError("bootstrap desired state exceeds its decoded bound")
            desired = BootstrapDesiredState.model_validate_json(raw)
        except (binascii.Error, UnicodeError, ValidationError, ValueError) as exc:
            raise ConfigurationError("bootstrap desired state is invalid") from exc
        started_at = datetime.now(UTC)
        started = monotonic()
        deadline = started + (
            BOOTSTRAP_REPAIR_DEADLINE_SECONDS
            if repair
            else BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS
        )

        def run_systemctl(
            arguments: list[str], *, timeout_seconds: float
        ) -> subprocess.CompletedProcess[str]:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ConfigurationError("bootstrap inspection exceeded its total deadline")
            try:
                return run_bounded_process(
                    ["systemctl", "--user", *arguments],
                    timeout_seconds=min(timeout_seconds, remaining),
                    stdout_maximum_bytes=4096,
                    stderr_maximum_bytes=4096,
                )
            except (OSError, BoundedProcessError) as exc:
                raise ConfigurationError(
                    f"bounded systemd inspection failed: {arguments[0]}"
                ) from exc

        inspection_started = monotonic()
        current_installation = installation_info()
        service_active: bool | None = None
        service_enabled: bool | None = None
        if desired.worker_service is not None:
            active_result = run_systemctl(
                ["is-active", "--quiet", desired.worker_service],
                timeout_seconds=5,
            )
            enabled_result = run_systemctl(
                ["is-enabled", "--quiet", desired.worker_service],
                timeout_seconds=5,
            )
            service_active = active_result.returncode == 0
            service_enabled = enabled_result.returncode == 0
        queue_evidence = ClioCoreQueue(RelaySettings.from_env().core_dir).readiness_info()
        worker_evidence: dict[str, object] | None = None
        if service_active is True and desired.cluster is not None:
            try:
                worker_evidence = worker_runtime_info(
                    cluster=desired.cluster,
                    current_installation=current_installation,
                )
            except (RelayError, ValueError) as exc:
                worker_evidence = {
                    "schema_version": "clio-relay.worker-runtime-info.v1",
                    "cluster": desired.cluster,
                    "running": False,
                    "error": str(exc),
                }
        inspection = inspect_exact_bootstrap_noop(
            desired,
            service_was_active=service_active,
            service_was_enabled=service_enabled,
            queue_evidence=queue_evidence,
            worker_evidence=worker_evidence,
            installation_snapshot=current_installation,
        )
        initial_service_active = service_active
        initial_service_enabled = service_enabled
        initial_inspection_reasons = list(inspection.reasons)
        initial_jarvis_state = inspection.jarvis_state
        service_start_count = 0
        service_enable_count = 0
        service_restart_count = 0
        repair_attempted = False
        repairable_reasons = {
            "managed endpoint service is inactive",
            "managed endpoint service is disabled",
            "active endpoint worker readiness did not verify",
        }
        if (
            repair
            and desired.worker_service is not None
            and inspection.reasons
            and set(inspection.reasons).issubset(repairable_reasons)
        ):
            repair_attempted = True
            load_state = run_systemctl(
                [
                    "show",
                    "--property=LoadState",
                    "--value",
                    desired.worker_service,
                ],
                timeout_seconds=5,
            )
            if not (
                load_state.returncode == 0
                and len(load_state.stdout.encode()) <= 1024
                and load_state.stdout.strip() == "loaded"
            ):
                raise ConfigurationError(
                    "managed endpoint service is not installed; run "
                    "cluster install-endpoint-service before requesting readiness repair"
                )
            else:
                if service_enabled is not True:
                    enabled = run_systemctl(
                        ["enable", desired.worker_service],
                        timeout_seconds=15,
                    )
                    if enabled.returncode != 0:
                        raise ConfigurationError("managed endpoint service could not be enabled")
                    service_enable_count = 1
                if service_active is True:
                    started_service = run_systemctl(
                        ["restart", desired.worker_service],
                        timeout_seconds=20,
                    )
                    if started_service.returncode != 0:
                        raise ConfigurationError("managed endpoint service could not be restarted")
                    service_restart_count = 1
                else:
                    started_service = run_systemctl(
                        ["start", desired.worker_service],
                        timeout_seconds=20,
                    )
                    if started_service.returncode != 0:
                        raise ConfigurationError("managed endpoint service could not be started")
                    service_start_count = 1
                worker_deadline = min(deadline, monotonic() + 30)
                worker_evidence = None
                while monotonic() < worker_deadline:
                    try:
                        worker_evidence = worker_runtime_info(
                            cluster=desired.cluster or "",
                            current_installation=current_installation,
                        )
                    except (RelayError, ValueError):
                        sleep(0.25)
                        continue
                    if worker_evidence.get("running") is True:
                        break
                    sleep(0.25)
                service_active = True
                service_enabled = True
                inspection = inspect_exact_bootstrap_noop(
                    desired,
                    service_was_active=True,
                    service_was_enabled=True,
                    queue_evidence=queue_evidence,
                    worker_evidence=worker_evidence,
                    installation_snapshot=current_installation,
                )
        if repair_attempted and not inspection.exact_match:
            raise ConfigurationError(
                "payload-free bootstrap repair did not converge: " + "; ".join(inspection.reasons)
            )
        payload: dict[str, object] = {
            "schema_version": "clio-relay.bootstrap-preflight.v1",
            "exact_match": inspection.exact_match,
            "desired_fingerprint": desired.fingerprint,
            "reasons": inspection.reasons,
            "receipt": None,
        }
        if inspection.exact_match:
            inspection_duration = monotonic() - inspection_started
            outcome: Literal["noop_verified", "repaired"] = (
                "repaired"
                if service_start_count or service_enable_count or service_restart_count
                else "noop_verified"
            )
            receipt = make_bootstrap_receipt(
                invocation_id=invocation_id,
                desired=desired,
                outcome=outcome,
                inspection=inspection,
                started_at=started_at,
                transaction=None,
                previous_generation=inspection.active_generation,
                active_generation=inspection.active_generation,
                duration_seconds=monotonic() - started,
                inspection_duration_seconds=inspection_duration,
                service_start_count=service_start_count,
                service_enable_count=service_enable_count,
                service_restart_count=service_restart_count,
                initial_inspection_reasons=initial_inspection_reasons,
                jarvis_state_before=initial_jarvis_state,
                service_active_before=initial_service_active,
                service_enabled_before=initial_service_enabled,
                service_active_after=service_active,
                service_enabled_after=service_enabled,
            )
            write_bootstrap_receipt(
                Path.home() / ".local/share/clio-relay/bootstrap-receipt.json",
                receipt,
            )
            payload["receipt"] = receipt
            payload["action"] = outcome
        else:
            repairable = bool(inspection.reasons) and all(
                reason in repairable_reasons for reason in inspection.reasons
            )
            payload["action"] = (
                "repair_required" if not repair and repairable else "payload_required"
            )
            if payload["action"] == "repair_required":
                payload["repair_reasons"] = inspection.reasons
        typer.echo(
            "bootstrap_preflight_json=" + json.dumps(payload, sort_keys=True, separators=(",", ":"))
        )

    def _inspect() -> None:
        with bootstrap_invocation_lock(timeout_seconds=2):
            _inspect_locked()

    _run_or_exit(_inspect)


@app.command("doctor")
def doctor(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
) -> None:
    """Check local or live cluster configuration."""
    definition = _require_cluster(cluster)

    def _run() -> None:
        _echo_lines(
            run_doctor(
                RelaySettings.from_env(),
                live=True,
                frps_addr=definition.frp_transport.server_addr,
            )
        )
        _echo_lines(run_cluster_doctor(definition))

    _run_or_exit(_run)


@app.command("live-test")
@_acceptance_report_command
def live_test(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    jarvis_yaml: Annotated[
        Path | None,
        typer.Option(help="Configured acceptance JARVIS YAML. Overrides cluster config."),
    ] = None,
    monitor_pattern: Annotated[
        str | None,
        typer.Option(help="Regex expected to match stdout.delta during acceptance."),
    ] = None,
    progress_pattern: Annotated[
        str | None,
        typer.Option(help="Regex used to record structured progress from stdout.delta."),
    ] = None,
    progress_action_payload_json: Annotated[
        str,
        typer.Option(
            help="JSON object payload for progress monitor extraction, such as groups and units.",
        ),
    ] = "{}",
    agent_prompt: Annotated[
        str | None,
        typer.Option(help="Remote prompt path for optional agent acceptance."),
    ] = None,
    agent_mcp_config: Annotated[
        str | None,
        typer.Option(help="Remote MCP config path for optional agent acceptance."),
    ] = None,
    agent_child_jarvis_yaml: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Local JARVIS YAML the agent must submit through MCP. "
                "Generates a remote agent prompt with a fresh idempotency key."
            ),
        ),
    ] = None,
    require_agent_child_job: Annotated[
        bool | None,
        typer.Option(
            "--require-agent-child-job/--no-require-agent-child-job",
            help=(
                "Require optional agent acceptance to report and complete a child relay job. "
                "Defaults to enabled when --agent-mcp-config is set."
            ),
        ),
    ] = None,
    verify_transport: Annotated[
        bool | None,
        typer.Option(
            "--verify-transport/--no-verify-transport",
            help="Verify desktop-to-cluster HTTP reachability through configured frp transport.",
        ),
    ] = None,
    verify_direct_transport: Annotated[
        bool | None,
        typer.Option(
            "--verify-direct-transport/--no-verify-direct-transport",
            help="Verify desktop-to-cluster HTTP reachability through frp XTCP.",
        ),
    ] = None,
    verify_ssh_transport: Annotated[
        bool,
        typer.Option(
            "--verify-ssh-transport/--no-verify-ssh-transport",
            help="Verify an owned SSH-forward transport and teardown path.",
        ),
    ] = False,
    allow_direct_transport_fallback: Annotated[
        bool | None,
        typer.Option(
            "--allow-direct-transport-fallback/--no-allow-direct-transport-fallback",
            help="Allow live direct transport acceptance to fall back to STCP.",
        ),
    ] = None,
    transport_token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    transport_secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    transport_local_bind_port: Annotated[
        int | None,
        typer.Option(help="Local desktop visitor bind port for transport acceptance."),
    ] = None,
    transport_remote_api_port: Annotated[
        int | None,
        typer.Option(help="Remote cluster API port for transport acceptance."),
    ] = None,
    transport_proxy_name: Annotated[
        str | None,
        typer.Option(help="frp proxy/server name for transport acceptance."),
    ] = None,
    ssh_transport_local_bind_port: Annotated[
        int | None,
        typer.Option(help="Local bind port for SSH-forward acceptance."),
    ] = None,
    ssh_transport_remote_api_port: Annotated[
        int | None,
        typer.Option(help="Remote API port for SSH-forward acceptance."),
    ] = None,
    ssh_transport_session_id: Annotated[
        str | None,
        typer.Option(help="Owned remote session id for SSH-forward acceptance."),
    ] = None,
    report: Annotated[
        Path | None,
        typer.Option(help="JSON report path. Defaults under .clio-relay/validation-reports."),
    ] = None,
    markdown_report: Annotated[
        Path | None,
        typer.Option(help="Optional human-readable Markdown rendering of the JSON report."),
    ] = None,
    resume_report: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Resume the exact nonterminal workload recorded by a PENDING live-test report. "
                "The source checkpoint is never overwritten."
            ),
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(
            help="Launcher evidence, such as uv-tool. Can use the validation environment."
        ),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(
            help="Explicit kind:reference install evidence, such as pypi:clio-relay==1.0.0."
        ),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel artifact whose SHA-256 is recorded in the report.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    validation_scenario: Annotated[
        str,
        typer.Option(help="Release-policy scenario recorded in the JSON report."),
    ] = "live-test",
    verify_cluster_deployment: Annotated[
        bool,
        typer.Option(
            "--verify-cluster-deployment/--no-verify-cluster-deployment",
            help="Require the matching installed worker version and a live worker execution.",
        ),
    ] = False,
    require_structured_runtime_metadata: Annotated[
        bool,
        typer.Option(
            "--require-structured-runtime-metadata/--allow-legacy-runtime-metadata",
            help="Require JARVIS-owned structured runtime and scheduler metadata.",
        ),
    ] = True,
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum seconds to wait for acceptance jobs."),
    ] = 600,
    poll_seconds: Annotated[float, typer.Option(help="Polling interval.")] = 2,
) -> None:
    """Run configurable live acceptance checks for a cluster."""
    report_path = report or (
        _live_acceptance_resume_output_path(resume_report)
        if resume_report is not None
        else default_report_path(cluster)
    )
    if resume_report is not None and report_path.resolve() == resume_report.resolve():
        raise typer.BadParameter(
            "--report must differ from --resume-report so the checkpoint is preserved"
        )
    seed_report = new_live_validation_report(
        scenario=validation_scenario,
        cluster=cluster,
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    if resume_report is None:
        write_validation_report(seed_report, report_path)
    try:
        definition = _require_cluster(cluster)
        should_verify_transport = (
            definition.live_test.verify_transport if verify_transport is None else verify_transport
        )
        should_verify_direct_transport = (
            definition.live_test.verify_direct_transport
            if verify_direct_transport is None
            else verify_direct_transport
        )
        should_allow_direct_transport_fallback = (
            definition.live_test.allow_direct_transport_fallback
            if allow_direct_transport_fallback is None
            else allow_direct_transport_fallback
        )
        needs_transport_secrets = should_verify_transport or should_verify_direct_transport
    except BaseException as exc:
        current_report = _load_current_acceptance_report(
            report_path,
            expected_report_id=seed_report.report_id,
        )
        _write_failed_acceptance_report(
            path=report_path,
            scenario=validation_scenario,
            cluster=cluster,
            check_id="live.preflight",
            summary="validate live acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
            partial_report=current_report or seed_report,
        )
        raise

    def _run() -> None:
        settings = RelaySettings.from_env()
        try:
            lines = run_live_acceptance(
                LiveAcceptanceOptions(
                    cluster=cluster,
                    definition=definition,
                    jarvis_yaml=jarvis_yaml,
                    monitor_pattern=monitor_pattern,
                    progress_pattern=progress_pattern,
                    progress_action_payload=_json_object(progress_action_payload_json),
                    agent_prompt=agent_prompt,
                    agent_mcp_config=agent_mcp_config,
                    require_agent_child_job=require_agent_child_job,
                    agent_child_jarvis_yaml=agent_child_jarvis_yaml,
                    verify_transport=verify_transport,
                    verify_direct_transport=should_verify_direct_transport,
                    verify_ssh_transport=verify_ssh_transport,
                    allow_direct_transport_fallback=should_allow_direct_transport_fallback,
                    transport_token=(
                        _resolve_env_secret(
                            transport_token,
                            definition.frp_transport.token_env,
                            "frp token",
                        )
                        if needs_transport_secrets
                        else None
                    ),
                    transport_secret_key=(
                        _resolve_env_secret(
                            transport_secret_key,
                            definition.frp_transport.stcp_secret_env,
                            "stcp secret",
                        )
                        if needs_transport_secrets
                        else None
                    ),
                    transport_frpc_bin=settings.frpc_bin,
                    transport_local_bind_port=transport_local_bind_port,
                    transport_remote_api_port=transport_remote_api_port,
                    transport_proxy_name=transport_proxy_name,
                    ssh_transport_local_bind_port=ssh_transport_local_bind_port,
                    ssh_transport_remote_api_port=ssh_transport_remote_api_port,
                    ssh_transport_session_id=ssh_transport_session_id,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                    report_path=report_path,
                    markdown_report_path=markdown_report,
                    validation_launcher=validation_launcher,
                    validation_install_source=validation_install_source,
                    validation_artifact_sha256=(
                        sha256_file(validation_artifact)
                        if validation_artifact is not None
                        else None
                    ),
                    require_structured_runtime_metadata=require_structured_runtime_metadata,
                    validation_scenario=validation_scenario,
                    verify_cluster_deployment=verify_cluster_deployment,
                    report_id=seed_report.report_id,
                    resume_report_path=resume_report,
                )
            )
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            if current_report is None:
                raise RelayError("live acceptance did not persist the current invocation report")
            if current_report.status is ValidationStatus.PASSED and should_execute_on_cluster(
                definition
            ):
                _write_remote_verified_report(
                    current_report,
                    definition,
                    report_path,
                )
        except BaseException as exc:
            current_report = _load_current_acceptance_report(
                report_path,
                expected_report_id=seed_report.report_id,
            )
            _write_failed_acceptance_report(
                path=report_path,
                scenario=validation_scenario,
                cluster=cluster,
                check_id="live.completed",
                summary="complete live acceptance",
                error=exc,
                launcher=validation_launcher,
                install_source=validation_install_source,
                artifact=validation_artifact,
                partial_report=current_report or seed_report,
            )
            typer.echo(f"validation.report={report_path.resolve()}")
            raise
        _echo_lines(lines)

    _run_or_exit(_run)


def _file_idempotency_key(path: Path, text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"jarvis:{path.resolve()}:{digest}"


def _live_acceptance_resume_output_path(source: Path) -> Path:
    """Return a collision-resistant sibling without altering the source checkpoint."""
    return source.with_name(f"{source.stem}.resume-{uuid4().hex[:8]}{source.suffix}")


def _none_if_blank(value: str | None) -> str | None:
    if value is None or value.strip() == "":
        return None
    return value


def _split_csv(value: str) -> list[str]:
    return [entry.strip() for entry in value.split(",") if entry.strip()]


def _parse_age_seconds(value: str) -> int:
    """Parse a positive operator age threshold such as ``30m`` or ``2h``."""
    match = re.fullmatch(r"(?P<count>[1-9][0-9]*)(?P<unit>[smhd]?)", value.strip().lower())
    if match is None:
        raise typer.BadParameter("age threshold must be a positive integer with s, m, h, or d")
    multiplier = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86_400}[match.group("unit")]
    return int(match.group("count")) * multiplier


def _optional_datetime(value: str | None) -> datetime | None:
    """Parse an optional strict ISO-8601 timestamp for optimistic concurrency."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("expected timestamp must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise typer.BadParameter("expected timestamp must include a timezone")
    return parsed


def _public_json(value: object) -> str:
    """Serialize operator-facing JSON without exposing durable credentials."""
    return json.dumps(redact_sensitive_values(value), indent=2)


def _bounded_cleanup_public_json(value: object) -> str | None:
    """Return public JSON only below the cleanup stdout compatibility boundary."""
    serialized = _public_json(value)
    return (
        serialized
        if len(serialized.encode("utf-8")) < MAX_FINALIZED_CLEANUP_RETRY_OUTPUT_BYTES
        else None
    )


def _managed_queue_from_env() -> StorageManagedQueue:
    """Open the production queue with durable storage reconciliation enabled."""
    return storage_managed_queue(RelaySettings.from_env())


def _submit_managed_job(job: RelayJob) -> RelayJob:
    """Submit through storage admission and emit stable JSON on refusal."""
    try:
        return _managed_queue_from_env().submit_job(job)
    except StorageAdmissionError as exc:
        _echo_storage_admission_error(exc)
        raise typer.Exit(code=1) from exc


def _echo_storage_admission_error(error: StorageAdmissionError) -> None:
    """Write the stable CLI storage refusal envelope to stderr."""
    typer.echo(
        json.dumps(
            {
                "error": "storage_admission_denied",
                "storage_decision": error.decision.to_dict(),
            },
            sort_keys=True,
        ),
        err=True,
    )


def _record_page_payload(
    record_key: str,
    records: list[dict[str, object]],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> dict[str, object]:
    """Build the shared one-based collection response used by CLI surfaces."""
    return {
        record_key: records,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


def _json_object(value: str) -> dict[str, object]:
    source = Path(value[1:]).read_text(encoding="utf-8-sig") if value.startswith("@") else value
    try:
        loaded = cast(object, json.loads(source))
    except JSONDecodeError as exc:
        raise typer.BadParameter(f"value must be valid JSON: {exc.msg}") from exc
    if not isinstance(loaded, dict):
        raise typer.BadParameter("value must be a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], loaded).items()}


def _json_text_from_option(source: str, source_file: Path | None) -> str:
    if source_file is None:
        return source
    if source != "{}":
        raise typer.BadParameter("use either the JSON value option or the JSON file option")
    if not source_file.exists():
        raise typer.BadParameter(f"JSON file does not exist: {source_file}")
    return source_file.read_text(encoding="utf-8-sig")


def _with_exclusive_scheduler(pipeline_yaml: str, scheduler_provider: str) -> str:
    loaded = yaml.safe_load(pipeline_yaml)
    if not isinstance(loaded, dict):
        raise ConfigurationError("JARVIS YAML must be an object to request exclusive allocation")
    document = cast(dict[str, object], loaded)
    scheduler = document.get("scheduler")
    if scheduler is None:
        if scheduler_provider == "external":
            raise ConfigurationError(
                "--exclusive requires an explicit scheduler provider in the cluster definition"
            )
        scheduler = {"name": scheduler_provider}
    if not isinstance(scheduler, dict):
        raise ConfigurationError("scheduler must be an object to request exclusive allocation")
    typed_scheduler = cast(dict[str, object], scheduler)
    typed_scheduler.setdefault("name", scheduler_provider)
    typed_scheduler["exclusive"] = True
    document["scheduler"] = typed_scheduler
    return yaml.safe_dump(document, sort_keys=False)


@dataclass(frozen=True)
class _OwnedRelayJob:
    job_id: str
    relay_state: JobState
    scheduler_job_ids: tuple[str, ...]
    scheduler_provider: str
    owner_session_generation_id: str | None = None
    unowned_scheduler_job_ids: tuple[str, ...] = ()
    relay_cancellation_requested: bool = False
    relay_cancellation_acknowledged: bool = False
    relay_cancellation_scheduler_requested: bool | None = None


def _quiesce_owner_session_intake(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    local_admission_session_id: str,
    session_generation_id: str,
    cleanup_operation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
) -> dict[str, object]:
    """Quiesce desktop and authoritative intake under one immutable operation id."""
    existing_local_intent = queue.get_owner_session_cleanup_intent(
        local_admission_session_id,
        session_generation_id=session_generation_id,
    )
    if existing_local_intent is None:
        queue.mirror_owner_session_generation_open(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
    local_intent = queue.set_owner_session_closing(
        local_admission_session_id,
        session_generation_id=session_generation_id,
        operation_id=cleanup_operation_id,
        stop_worker=stop_worker,
        cancel_jobs=cancel_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
    )
    if not remote_execution:
        authoritative_intent = queue.set_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
            operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
        )
        _require_matching_cleanup_intents(
            authoritative_intent,
            local_intent,
            cleanup_operation_id=cleanup_operation_id,
        )
        return authoritative_intent
    command = [
        "session",
        "quiesce-intake",
        "--session-id",
        session_id,
        "--session-generation-id",
        session_generation_id,
        "--cleanup-operation-id",
        cleanup_operation_id,
    ]
    if stop_worker:
        command.append("--cleanup-stop-worker")
    if cancel_jobs:
        command.append("--cleanup-cancel-jobs")
    if cancel_scheduler_jobs:
        command.append("--cleanup-cancel-scheduler-jobs")
    raw_result = cast(
        object,
        json.loads(
            run_remote_clio(
                definition,
                command,
            )
        ),
    )
    if not isinstance(raw_result, dict):
        raise RelayError("remote owner-session intake quiescence returned no evidence")
    result = cast(dict[str, object], raw_result)
    if (
        result.get("session_id") != session_id
        or result.get("session_generation_id") != session_generation_id
        or result.get("intake") != "quiesced"
    ):
        raise RelayError("remote owner-session intake quiescence identity did not match")
    raw_intent = result.get("cleanup_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError("remote owner-session intake quiescence omitted cleanup intent")
    intent = {str(key): value for key, value in cast(dict[object, object], raw_intent).items()}
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    if (
        intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
        or intent.get("owner_session_id") != session_id
        or intent.get("session_generation_id") != session_generation_id
        or intent.get("operation_id") != cleanup_operation_id
        or any(intent.get(key) is not value for key, value in expected_policy.items())
    ):
        raise RelayError("remote owner-session cleanup intent did not match requested policy")
    _require_matching_cleanup_intents(
        intent,
        local_intent,
        cleanup_operation_id=cleanup_operation_id,
    )
    return intent


def _require_matching_cleanup_intents(
    authoritative: dict[str, object],
    local: dict[str, object],
    *,
    cleanup_operation_id: str,
) -> None:
    """Require identical operation and policy across authoritative and desktop records."""
    keys = (
        "operation_id",
        "session_generation_id",
        "stop_worker",
        "cancel_jobs",
        "cancel_scheduler_jobs",
    )
    if (
        authoritative.get("operation_id") != cleanup_operation_id
        or local.get("operation_id") != cleanup_operation_id
        or any(authoritative.get(key) != local.get(key) for key in keys)
    ):
        raise RelayError("desktop and authoritative owner-session cleanup intents did not match")


def _owner_session_admission_status(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    session_id: str,
    session_generation_id: str,
) -> dict[str, object]:
    """Read owner-session intake through the CLI's injectable remote runner."""
    return owner_session_admission_status(
        queue=queue,
        definition=definition,
        remote_execution=remote_execution,
        session_id=session_id,
        session_generation_id=session_generation_id,
        remote_cli_runner=run_remote_clio,
    )


def _select_owner_session_cleanup_operation(
    *,
    authoritative_status: dict[str, object],
    local_intent: dict[str, object] | None,
    session_id: str,
    session_generation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
) -> str:
    """Reuse a retry operation or choose one id before the first cleanup mutation."""
    _require_durable_session_identity(
        session_generation_id,
        field="session_generation_id",
    )
    if not (
        authoritative_status.get("owner_session_id") == session_id
        and authoritative_status.get("session_generation_id") == session_generation_id
    ):
        raise RelayError("owner-session cleanup admission identity changed")
    if not (
        authoritative_status.get("open") is True or authoritative_status.get("closing") is True
    ):
        raise RelayError("owner-session generation is neither open nor a resumable cleanup")
    raw_authoritative_intent = authoritative_status.get("cleanup_intent")
    authoritative_intent = (
        cast(dict[str, object], raw_authoritative_intent)
        if isinstance(raw_authoritative_intent, dict)
        else None
    )
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    operation_ids: set[str] = set()
    for intent in (authoritative_intent, local_intent):
        if intent is None:
            continue
        if intent.get("session_generation_id") != session_generation_id or any(
            intent.get(key) is not value for key, value in expected_policy.items()
        ):
            raise RelayError("owner-session cleanup retry changed generation or policy")
        operation_id = intent.get("operation_id")
        if not isinstance(operation_id, str):
            raise RelayError("owner-session cleanup retry omitted its operation id")
        _require_durable_session_identity(operation_id, field="operation_id")
        operation_ids.add(operation_id)
    if len(operation_ids) > 1:
        raise RelayError("desktop and authoritative cleanup operation ids disagree")
    return next(iter(operation_ids), f"cleanup_{uuid4().hex}")


def _list_owned_active_cluster_jobs(
    queue: ClioCoreQueue,
    cluster: str,
    *,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    scheduler_provider: str,
    include_terminal: bool = False,
) -> list[_OwnedRelayJob]:
    owned: list[_OwnedRelayJob] = []
    membership_generations = [owner_session_generation_id]
    for membership_generation in membership_generations:
        cursor: str | None = None
        expected_total: int | None = None
        processed_source = 0
        while True:
            jobs, next_cursor, total, source_window_count = queue.list_owner_session_jobs_page(
                owner_session_id,
                session_generation_id=membership_generation,
                cursor=cursor,
                limit=MAX_RESPONSE_PAGE_RECORDS,
                cluster=cluster,
                include_terminal=include_terminal,
            )
            if expected_total is not None and total != expected_total:
                raise RelayError("owner-session membership changed during local discovery")
            expected_total = total
            processed_source += source_window_count
            for job in jobs:
                job_document = job.model_dump(mode="json")
                tasks, tasks_truncated = queue.scan_job_tasks(job.job_id, limit=1_000)
                if tasks_truncated:
                    raise RelayError(f"owner-session task discovery was truncated: {job.job_id}")
                task_documents = [task.model_dump(mode="json") for task in tasks]
                candidate = _owned_relay_job(
                    job_document,
                    task_documents,
                    scheduler_provider=scheduler_provider,
                )
                if include_terminal or _relay_job_needs_cleanup(candidate):
                    owned.append(candidate)
            if next_cursor is None:
                if processed_source != total:
                    raise RelayError("owner-session membership ended before its declared total")
                break
            if cursor is not None and next_cursor <= cursor:
                raise RelayError("owner-session membership cursor did not advance")
            cursor = next_cursor
    return owned


def _list_remote_owned_active_cluster_jobs(
    definition: ClusterDefinition,
    cluster: str,
    *,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    include_terminal: bool = False,
) -> list[_OwnedRelayJob]:
    owned: list[_OwnedRelayJob] = []
    membership_generations = [owner_session_generation_id]
    for membership_generation in membership_generations:
        cursor: str | None = None
        expected_total: int | None = None
        processed_source = 0
        while True:
            command = [
                "queue",
                "owner-jobs",
                "--cluster",
                cluster,
                "--owner-session-id",
                owner_session_id,
                "--limit",
                str(MAX_RESPONSE_PAGE_RECORDS),
            ]
            if membership_generation is not None:
                command.extend(["--owner-session-generation-id", membership_generation])
            if include_terminal:
                command.append("--include-terminal")
            if cursor is not None:
                command.extend(["--cursor", cursor])
            payload = _json_output(
                run_remote_clio(definition, command),
                f"remote owner-session jobs for {cluster}",
            )
            raw_jobs = payload.get("jobs")
            if not isinstance(raw_jobs, list):
                raise RelayError("remote owner-session membership returned no jobs array")
            total = payload.get("source_total")
            if isinstance(total, bool) or not isinstance(total, int) or total < 0:
                raise RelayError("remote owner-session membership returned an invalid total")
            if total > MAX_INTERNAL_COLLECTION_RECORDS:
                raise RelayError(
                    "remote owner-session membership exceeds the bounded source limit "
                    f"{MAX_INTERNAL_COLLECTION_RECORDS}"
                )
            if expected_total is not None and total != expected_total:
                raise RelayError("remote owner-session membership changed during discovery")
            expected_total = total
            source_window_count = payload.get("source_window_count")
            if (
                isinstance(source_window_count, bool)
                or not isinstance(source_window_count, int)
                or source_window_count < 0
                or source_window_count > MAX_RESPONSE_PAGE_RECORDS
            ):
                raise RelayError("remote owner-session membership returned an invalid source count")
            processed_source += source_window_count
            for raw_job in cast(list[object], raw_jobs):
                if not isinstance(raw_job, dict):
                    raise RelayError("remote owner-session membership returned a non-object job")
                job_document = {
                    str(key): value for key, value in cast(dict[object, object], raw_job).items()
                }
                if not _job_is_owned_by_session(
                    job_document,
                    owner_session_id,
                    owner_session_generation_id=owner_session_generation_id,
                ):
                    raise RelayError("remote owner-session membership target identity mismatch")
                job_id = job_document.get("job_id")
                if not isinstance(job_id, str):
                    raise RelayError("remote owner-session membership omitted job_id")
                task_documents = _complete_remote_collection(
                    definition,
                    ["job", "tasks", job_id],
                    record_key="tasks",
                    label=f"remote owner-session tasks for {job_id}",
                )
                candidate = _owned_relay_job(
                    job_document,
                    task_documents,
                    scheduler_provider=definition.scheduler_provider,
                )
                if include_terminal or _relay_job_needs_cleanup(candidate):
                    owned.append(candidate)
            next_cursor = payload.get("source_next_cursor")
            if next_cursor is None:
                if processed_source != total:
                    raise RelayError(
                        "remote owner-session membership ended before its declared total"
                    )
                break
            if not isinstance(next_cursor, str) or (cursor is not None and next_cursor <= cursor):
                raise RelayError("remote owner-session membership returned an invalid cursor")
            cursor = next_cursor
    return owned


def _cancel_local_owned_jobs(
    queue: ClioCoreQueue,
    jobs: list[_OwnedRelayJob],
) -> list[str]:
    requested: list[str] = []
    for job in jobs:
        if job.relay_state not in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}:
            continue
        canceled_job = request_cancel_job(
            queue,
            job.job_id,
            cancel_scheduler=False,
        )
        observed = _owned_relay_job(
            canceled_job.model_dump(mode="json"),
            [],
            scheduler_provider=job.scheduler_provider,
        )
        if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
            continue
        _require_durable_relay_cancellation(observed)
        requested.append(canceled_job.job_id)
    return requested


def _cancel_remote_owned_jobs(
    definition: ClusterDefinition,
    cluster: str,
    jobs: list[_OwnedRelayJob],
) -> list[str]:
    requested: list[str] = []
    for job in jobs:
        if job.relay_state not in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}:
            continue
        raw_result = cast(
            object,
            json.loads(
                run_remote_clio(
                    definition,
                    [
                        "queue",
                        "cancel",
                        job.job_id,
                        "--cluster",
                        cluster,
                        "--keep-scheduler-job",
                    ],
                )
            ),
        )
        if not isinstance(raw_result, dict):
            raise RelayError(f"owned relay cancellation returned no result: {job.job_id}")
        result = cast(dict[str, object], raw_result)
        if not isinstance(result.get("cancellation_requested"), bool):
            raise RelayError(f"owned relay cancellation omitted request evidence: {job.job_id}")
        raw_job = result.get("job")
        if not isinstance(raw_job, dict):
            raise RelayError(f"owned relay cancellation omitted its job: {job.job_id}")
        observed = _owned_relay_job(
            {str(key): value for key, value in cast(dict[object, object], raw_job).items()},
            [],
            scheduler_provider=job.scheduler_provider,
        )
        if observed.job_id != job.job_id:
            raise RelayError(f"owned relay cancellation returned a different job: {job.job_id}")
        if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
            continue
        _require_durable_relay_cancellation(observed)
        requested.append(job.job_id)
    return requested


def _require_durable_relay_cancellation(job: _OwnedRelayJob) -> None:
    """Require the exact relay-only request and any terminal cleanup acknowledgment."""
    if (
        not job.relay_cancellation_requested
        or job.relay_cancellation_scheduler_requested is not False
    ):
        raise RelayError(f"owned relay job cancellation was not durable: {job.job_id}")
    if job.relay_state is JobState.CANCELED and not job.relay_cancellation_acknowledged:
        raise RelayError(
            f"owned relay job was canceled without worker cleanup acknowledgment: {job.job_id}"
        )


def _read_owned_relay_job(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    cluster: str,
    job_id: str,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> _OwnedRelayJob:
    """Read one exact cancellation target and reverify its owner-session identity."""
    if remote_execution:
        raw_status = cast(
            object,
            json.loads(run_remote_clio(definition, ["job", "status", job_id])),
        )
        if not isinstance(raw_status, dict):
            raise RelayError(f"remote relay cancellation status was not an object: {job_id}")
        raw_job = cast(dict[str, object], raw_status).get("job")
        if not isinstance(raw_job, dict):
            raise RelayError(f"remote relay cancellation status omitted its job: {job_id}")
        document = {str(key): value for key, value in cast(dict[object, object], raw_job).items()}
    else:
        document = queue.get_job(job_id).model_dump(mode="json")
    if document.get("job_id") != job_id or document.get("cluster") != cluster:
        raise RelayError(f"relay cancellation target identity changed: {job_id}")
    if not _job_is_owned_by_session(
        document,
        owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    ):
        raise RelayError(f"relay cancellation target ownership changed: {job_id}")
    return _owned_relay_job(
        document,
        [],
        scheduler_provider=definition.scheduler_provider,
    )


def _wait_for_owned_relay_cancellations(
    job_ids: list[str],
    *,
    read_owned_job: Callable[[str], _OwnedRelayJob],
    timeout_seconds: float,
    poll_seconds: float,
) -> list[str]:
    """Wait boundedly for worker cleanup to acknowledge exact durable cancel requests."""
    if timeout_seconds <= 0:
        raise ValueError("relay cancellation timeout must be positive")
    if poll_seconds <= 0:
        raise ValueError("relay cancellation polling interval must be positive")
    pending = dict.fromkeys(job_ids)
    if len(pending) != len(job_ids):
        raise RelayError("relay cancellation targets must be unique")
    deadline = monotonic() + timeout_seconds
    last_states: dict[str, str] = {}
    while pending:
        for job_id in list(pending):
            remaining = deadline - monotonic()
            if remaining <= 0:
                detail = ", ".join(
                    f"{pending_id}={last_states.get(pending_id, 'missing')}"
                    for pending_id in sorted(pending)
                )
                raise RelayError(
                    "timed out waiting for worker-acknowledged relay cancellation: " + detail
                )
            with remote_command_timeout(min(REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS, remaining)):
                observed = read_owned_job(job_id)
            last_states[job_id] = observed.relay_state.value
            _require_durable_relay_cancellation(observed)
            if observed.relay_state is JobState.CANCELED:
                if not observed.relay_cancellation_acknowledged:
                    raise RelayError(
                        "owned relay cancellation reached CANCELED without cleanup evidence: "
                        f"{job_id}"
                    )
                pending.pop(job_id)
                continue
            if observed.relay_state in {JobState.SUCCEEDED, JobState.FAILED}:
                raise RelayError(
                    "owned relay cancellation became terminal without acknowledged cleanup: "
                    f"{job_id} ({observed.relay_state.value})"
                )
        if not pending:
            break
        remaining = deadline - monotonic()
        if remaining <= 0:
            detail = ", ".join(
                f"{job_id}={last_states.get(job_id, 'missing')}" for job_id in sorted(pending)
            )
            raise RelayError(
                "timed out waiting for worker-acknowledged relay cancellation: " + detail
            )
        sleep(min(poll_seconds, remaining))
    return list(job_ids)


def _job_is_owned_by_session(
    job: dict[str, object],
    owner_session_id: str,
    *,
    owner_session_generation_id: str | None = None,
) -> bool:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return False
    typed_metadata = cast(dict[str, object], metadata)
    if (
        typed_metadata.get("owner") != "clio-relay"
        or typed_metadata.get("owner_session_id") != owner_session_id
    ):
        return False
    recorded_generation = typed_metadata.get("owner_session_generation_id")
    return recorded_generation == owner_session_generation_id


def _relay_cancellation_evidence(
    job_id: str,
    metadata: dict[str, object],
) -> tuple[bool, bool, bool | None]:
    """Parse the durable cancellation request and cleanup acknowledgment contract."""
    raw_request = metadata.get("cancellation_request")
    if raw_request is None:
        return False, False, None
    if not isinstance(raw_request, dict):
        raise RelayError(f"owned relay job has invalid cancellation evidence: {job_id}")
    request = cast(dict[str, object], raw_request)
    requested_at = request.get("requested_at")
    previous_state = request.get("previous_state")
    cancel_scheduler = request.get("cancel_scheduler")
    if (
        request.get("schema_version") != "clio-relay.cancellation-request.v1"
        or not isinstance(requested_at, str)
        or previous_state
        not in {
            JobState.QUEUED.value,
            JobState.LEASED.value,
            JobState.RUNNING.value,
        }
        or not isinstance(cancel_scheduler, bool)
    ):
        raise RelayError(f"owned relay job has invalid cancellation evidence: {job_id}")
    try:
        parsed_requested_at = datetime.fromisoformat(requested_at)
    except ValueError as exc:
        raise RelayError(
            f"owned relay job has invalid cancellation request time: {job_id}"
        ) from exc
    if parsed_requested_at.tzinfo is None:
        raise RelayError(f"owned relay job cancellation request time is naive: {job_id}")
    acknowledged = request.get("cleanup_acknowledged") is True
    acknowledged_at = request.get("acknowledged_at")
    if acknowledged:
        if not isinstance(acknowledged_at, str):
            raise RelayError(
                f"owned relay job cancellation acknowledgment omitted its time: {job_id}"
            )
        try:
            parsed_acknowledged_at = datetime.fromisoformat(acknowledged_at)
        except ValueError as exc:
            raise RelayError(
                f"owned relay job has invalid cancellation acknowledgment time: {job_id}"
            ) from exc
        if parsed_acknowledged_at.tzinfo is None:
            raise RelayError(f"owned relay job cancellation acknowledgment time is naive: {job_id}")
    elif acknowledged_at is not None:
        raise RelayError(
            f"owned relay job has an acknowledgment time without cleanup proof: {job_id}"
        )
    return True, acknowledged, cancel_scheduler


def _owned_relay_job(
    job: dict[str, object],
    tasks: list[dict[str, object]],
    *,
    scheduler_provider: str,
) -> _OwnedRelayJob:
    job_id = job.get("job_id")
    if not isinstance(job_id, str):
        raise RelayError("owned relay job is missing a job id")
    raw_state = job.get("state")
    if not isinstance(raw_state, str):
        raise RelayError(f"owned relay job is missing its state: {job_id}")
    try:
        relay_state = JobState(raw_state)
    except ValueError as exc:
        raise RelayError(f"owned relay job has an invalid state: {job_id}: {raw_state}") from exc
    job_metadata = job.get("metadata")
    if not isinstance(job_metadata, dict):
        raise RelayError(f"owned relay job is missing metadata: {job_id}")
    typed_job_metadata = cast(dict[str, object], job_metadata)
    raw_generation_id = typed_job_metadata.get("owner_session_generation_id")
    if raw_generation_id is not None and not isinstance(raw_generation_id, str):
        raise RelayError(f"owned relay job has an invalid session generation: {job_id}")
    (
        cancellation_requested,
        cancellation_acknowledged,
        cancellation_scheduler_requested,
    ) = _relay_cancellation_evidence(job_id, typed_job_metadata)
    documents = [job, *tasks]
    observed_scheduler_job_ids: list[str] = []
    owned_scheduler_job_ids: list[str] = []
    provider = _normalized_scheduler_provider(scheduler_provider)
    task_ids = {
        task_id for task in tasks if isinstance((task_id := task.get("task_id")), str) and task_id
    }
    for document in documents:
        metadata = document.get("metadata")
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        runtime = typed_metadata.get("runtime_metadata")
        if isinstance(runtime, dict):
            typed_runtime = cast(dict[str, object], runtime)
            _append_scheduler_job_id(
                observed_scheduler_job_ids,
                typed_runtime.get("scheduler_job_id"),
            )
        _append_scheduler_job_id(
            observed_scheduler_job_ids,
            typed_metadata.get("scheduler_job_id"),
        )
        stored_ids = typed_metadata.get("scheduler_job_ids")
        if isinstance(stored_ids, list):
            for stored_id in cast(list[object], stored_ids):
                _append_scheduler_job_id(observed_scheduler_job_ids, stored_id)
        scheduler_status = typed_metadata.get("scheduler_status")
        if isinstance(scheduler_status, dict):
            typed_status = cast(dict[str, object], scheduler_status)
            _append_scheduler_job_id(
                observed_scheduler_job_ids,
                typed_status.get("scheduler_job_id"),
            )
        ownership_records = typed_metadata.get("scheduler_job_ownership")
        if not isinstance(ownership_records, list):
            continue
        document_task_id = document.get("task_id")
        for raw_record in cast(list[object], ownership_records):
            if not isinstance(raw_record, dict):
                continue
            record = cast(dict[str, object], raw_record)
            scheduler_job_id = record.get("scheduler_job_id")
            _append_scheduler_job_id(observed_scheduler_job_ids, scheduler_job_id)
            if not isinstance(scheduler_job_id, str) or not scheduler_job_id:
                continue
            record_task_id = record.get("task_id")
            record_provider = record.get("scheduler_provider")
            record_execution_id = record.get("execution_id")
            source = record.get("runtime_metadata_source")
            expected_proofs = {
                "jarvis_mcp": {"owned_jarvis_run_mcp_result"},
                "jarvis_sidecar": {
                    "authenticated_runtime_sidecar",
                    "exact_scheduler_marker_reconciliation",
                },
                "relay_reconciliation": {"exact_scheduler_marker_reconciliation"},
            }.get(source if isinstance(source, str) else "", set())
            if (
                record.get("ownership_verified") is not True
                or record.get("relay_job_id") != job_id
                or not isinstance(document_task_id, str)
                or not isinstance(record_task_id, str)
                or record_task_id not in task_ids
                or document_task_id != record_task_id
                or not isinstance(record_provider, str)
                or _normalized_scheduler_provider(record_provider) != provider
                or not isinstance(record_execution_id, str)
                or not record_execution_id
                or not expected_proofs
                or typed_metadata.get("runtime_metadata_source") != source
                or record.get("proof") not in expected_proofs
            ):
                continue
            _append_scheduler_job_id(owned_scheduler_job_ids, scheduler_job_id)
    unowned_scheduler_job_ids = [
        scheduler_job_id
        for scheduler_job_id in observed_scheduler_job_ids
        if scheduler_job_id not in owned_scheduler_job_ids
    ]
    return _OwnedRelayJob(
        job_id=job_id,
        relay_state=relay_state,
        scheduler_job_ids=tuple(owned_scheduler_job_ids),
        scheduler_provider=provider,
        owner_session_generation_id=raw_generation_id,
        unowned_scheduler_job_ids=tuple(unowned_scheduler_job_ids),
        relay_cancellation_requested=cancellation_requested,
        relay_cancellation_acknowledged=cancellation_acknowledged,
        relay_cancellation_scheduler_requested=cancellation_scheduler_requested,
    )


def _relay_job_needs_cleanup(job: _OwnedRelayJob) -> bool:
    return (
        job.relay_state in {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
        or bool(job.scheduler_job_ids)
        or bool(job.unowned_scheduler_job_ids)
    )


def _normalized_scheduler_provider(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _append_scheduler_job_id(target: list[str], value: object) -> None:
    if isinstance(value, str) and value and value not in target:
        target.append(value)


def _normalize_scheduler_sentinel_ids(values: list[str]) -> tuple[str, ...]:
    """Validate and de-duplicate scheduler preservation sentinel ids."""
    normalized: list[str] = []
    for value in values:
        scheduler_job_id = value.strip()
        if not scheduler_job_id:
            raise typer.BadParameter("--preserve-scheduler-job-id cannot be empty")
        if scheduler_job_id not in normalized:
            normalized.append(scheduler_job_id)
    return tuple(normalized)


def _owned_gateway_scheduler_job_ids(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    owner_session_id: str,
    owner_session_generation_id: str,
) -> tuple[str, ...]:
    """Discover every exact-generation gateway scheduler allocation without mutation."""
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway scheduler discovery exceeds the bounded source limit; "
            "no scheduler cancellation was attempted"
        )
    documents = [gateway.model_dump(mode="json") for gateway in local_gateways]
    if should_execute_on_cluster(definition):
        documents.extend(
            _complete_remote_source_collection(
                definition,
                ["gateway", "list", "--cluster", cluster],
                record_key="gateway_sessions",
                label=f"remote gateway scheduler discovery for {cluster}",
            )
        )
    ids_by_gateway: dict[str, set[str]] = {}
    for gateway in documents:
        session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if not isinstance(session_id, str) or not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if (
            typed_metadata.get("owner") != "clio-relay"
            or typed_metadata.get("owner_session_id") != owner_session_id
            or typed_metadata.get("owner_session_generation_id") != owner_session_generation_id
        ):
            continue
        exact_ids = ids_by_gateway.setdefault(session_id, set())
        scheduler_job_id = gateway.get("scheduler_job_id")
        if isinstance(scheduler_job_id, str) and scheduler_job_id:
            exact_ids.add(scheduler_job_id)
        raw_gateway = gateway.get("gateway")
        if not isinstance(raw_gateway, dict):
            continue
        ownership_intents = cast(dict[str, object], raw_gateway).get("ownership_intents")
        if not isinstance(ownership_intents, dict):
            continue
        raw_scheduler_intent = cast(dict[str, object], ownership_intents).get(
            "scheduler_submission"
        )
        if not isinstance(raw_scheduler_intent, dict):
            continue
        scheduler_intent = cast(dict[str, object], raw_scheduler_intent)
        intent_state = scheduler_intent.get("state")
        intent_scheduler_job_id = scheduler_intent.get("scheduler_job_id")
        if isinstance(intent_scheduler_job_id, str) and intent_scheduler_job_id:
            exact_ids.add(intent_scheduler_job_id)
        if intent_state in {"starting", "recorded"} and not exact_ids:
            raise RelayError(
                "owned gateway has an unresolved scheduler submission; no scheduler "
                f"cancellation was attempted: {session_id}"
            )
        if len(exact_ids) > 1:
            raise RelayError(
                "owned gateway scheduler identity disagrees across durable evidence; no "
                f"scheduler cancellation was attempted: {session_id}"
            )
    return tuple(sorted({job_id for ids in ids_by_gateway.values() for job_id in ids}))


def _assert_scheduler_sentinels_unrelated(
    scheduler_sentinel_ids: tuple[str, ...],
    jobs: list[_OwnedRelayJob],
    *,
    gateway_scheduler_job_ids: tuple[str, ...] = (),
) -> None:
    """Fail closed if a preservation sentinel appears in session-owned job evidence."""
    session_scheduler_ids = {
        scheduler_job_id
        for job in jobs
        for scheduler_job_id in (*job.scheduler_job_ids, *job.unowned_scheduler_job_ids)
    }
    session_scheduler_ids.update(gateway_scheduler_job_ids)
    conflicts = sorted(set(scheduler_sentinel_ids) & session_scheduler_ids)
    if conflicts:
        raise RelayError(
            "scheduler preservation sentinel ids appeared in owned or unowned scheduler "
            "evidence for the target session generation; no scheduler cancellation was "
            "attempted: " + ", ".join(conflicts)
        )


def _preflight_scheduler_sentinels(
    definition: ClusterDefinition,
    scheduler_sentinel_ids: tuple[str, ...],
    jobs: list[_OwnedRelayJob],
    *,
    gateway_scheduler_job_ids: tuple[str, ...] = (),
) -> dict[str, str]:
    """Prove unrelated scheduler sentinels are active before cleanup mutation."""
    _assert_scheduler_sentinels_unrelated(
        scheduler_sentinel_ids,
        jobs,
        gateway_scheduler_job_ids=gateway_scheduler_job_ids,
    )
    provider = definition.scheduler_provider
    observed_phases: dict[str, str] = {}
    errors: list[str] = []
    for scheduler_job_id in scheduler_sentinel_ids:
        phase, error = _scheduler_phase_after_operation(
            definition,
            scheduler_job_id,
            provider=provider,
        )
        normalized_phase = phase.strip().lower() if phase is not None else "unknown"
        if error is not None or normalized_phase not in SCHEDULER_SENTINEL_ACTIVE_PHASES:
            errors.append(
                f"{scheduler_job_id} phase={normalized_phase}"
                + (f" error={error}" if error is not None else "")
            )
            continue
        observed_phases[scheduler_job_id] = normalized_phase
    if errors:
        raise RelayError(
            "scheduler preservation sentinels must be unrelated active jobs before "
            "cancellation; " + "; ".join(errors)
        )
    return observed_phases


def _scheduler_sentinel_preservation_resources(
    definition: ClusterDefinition,
    pre_phases: dict[str, str],
) -> tuple[list[CleanupResource], list[str]]:
    """Re-poll scheduler sentinels and emit canonical preservation evidence."""
    provider = definition.scheduler_provider
    resources: list[CleanupResource] = []
    errors: list[str] = []
    for scheduler_job_id, pre_phase in pre_phases.items():
        phase, poll_error = _scheduler_phase_after_operation(
            definition,
            scheduler_job_id,
            provider=provider,
        )
        post_phase = phase.strip().lower() if phase is not None else "unknown"
        preserved = poll_error is None and post_phase in SCHEDULER_SENTINEL_PRESERVED_PHASES
        detail = (
            "unrelated scheduler sentinel remained active after owned cancellation"
            if preserved and post_phase != "completed"
            else "unrelated scheduler sentinel completed naturally during owned cancellation"
            if preserved
            else "unrelated scheduler sentinel preservation was not proven"
            + (f": {poll_error}" if poll_error is not None else f": phase={post_phase}")
        )
        resource = CleanupResource(
            kind="scheduler_sentinel",
            resource_id=scheduler_job_id,
            location=definition.ssh_host,
            action="retain",
            ownership_verified=False,
            outcome="retained" if preserved else "failed",
            provider=provider,
            verified_after_operation=preserved,
            observed_state=post_phase,
            residual=not preserved,
            detail=detail,
            metadata={
                "unowned_sentinel": True,
                "active_before_operation": True,
                "preservation_verified": preserved,
                "pre_phase": pre_phase,
                "post_phase": post_phase,
            },
        )
        resources.append(resource)
        if not preserved:
            errors.append(f"scheduler sentinel {scheduler_job_id} was not preserved: {detail}")
    return resources, errors


def _owned_job_cleanup_resources(
    jobs: list[_OwnedRelayJob],
    *,
    definition: ClusterDefinition,
    location: str,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
    post_operation_jobs: list[_OwnedRelayJob] | None = None,
) -> list[CleanupResource]:
    resources: list[CleanupResource] = []
    post_by_id = {
        job.job_id: job for job in (post_operation_jobs if post_operation_jobs is not None else [])
    }
    for job in jobs:
        relay_active = job.relay_state in {
            JobState.QUEUED,
            JobState.LEASED,
            JobState.RUNNING,
        }
        post_job = post_by_id.get(job.job_id)
        canceled_with_cleanup = (
            post_job is not None
            and post_job.relay_state is JobState.CANCELED
            and post_job.relay_cancellation_requested
            and post_job.relay_cancellation_acknowledged
            and post_job.relay_cancellation_scheduler_requested is False
        )
        completed_before_request = (
            post_job is not None
            and post_job.relay_state in {JobState.SUCCEEDED, JobState.FAILED}
            and not post_job.relay_cancellation_requested
        )
        relay_verified = (
            canceled_with_cleanup or completed_before_request
            if cancel_jobs and relay_active
            else post_job is not None
        )
        if not relay_active:
            relay_action: Literal["retain", "stop", "close", "cancel"] = "retain"
            relay_outcome: Literal[
                "retained",
                "stopped",
                "closed",
                "canceled",
                "terminal",
                "missing",
                "refused",
                "failed",
            ] = "terminal"
            relay_verified = True
            relay_detail = (
                f"relay job was already terminal ({job.relay_state.value}); "
                "owned scheduler resources were evaluated independently"
            )
        else:
            relay_action = "cancel" if cancel_jobs else "retain"
            if cancel_jobs and canceled_with_cleanup:
                relay_outcome = "canceled"
                relay_detail = (
                    "worker cleanup acknowledged the durable relay-only cancellation request"
                )
            elif cancel_jobs and completed_before_request:
                relay_outcome = "terminal"
                relay_detail = "relay job completed before the cancellation request won the race"
            elif not cancel_jobs and relay_verified:
                relay_outcome = "retained"
                relay_detail = "relay job ownership matched and retention was verified"
            else:
                relay_outcome = "failed"
                relay_detail = "owned relay job cancellation or retention was not verified"
        resources.append(
            CleanupResource(
                kind="relay_job",
                resource_id=job.job_id,
                location=location,
                action=relay_action,
                ownership_verified=True,
                outcome=relay_outcome,
                verified_after_operation=relay_verified,
                residual=not relay_verified,
                detail=relay_detail,
                metadata={"scheduler_job_ids": list(job.scheduler_job_ids)},
            )
        )
        for scheduler_job_id in job.scheduler_job_ids:
            if cancel_jobs and cancel_scheduler_jobs:
                continue
            scheduler_verified = False
            phase: str | None = None
            status_error: str | None = None
            if not cancel_scheduler_jobs:
                phase, status_error = _scheduler_phase_after_operation(
                    definition,
                    scheduler_job_id,
                    provider=job.scheduler_provider,
                )
                scheduler_verified = phase in {
                    "submitted",
                    "pending",
                    "allocated",
                    "running",
                    "completed",
                    "failed",
                    "canceled",
                    "missing",
                }
            scheduler_terminal = phase in {"completed", "failed", "canceled", "missing"}
            resources.append(
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=location,
                    action="retain",
                    ownership_verified=True,
                    outcome=(
                        "missing"
                        if phase == "missing"
                        else "terminal"
                        if scheduler_verified and scheduler_terminal
                        else "retained"
                        if scheduler_verified
                        else "failed"
                    ),
                    provider=job.scheduler_provider,
                    verified_after_operation=scheduler_verified,
                    observed_state=phase,
                    residual=not scheduler_verified,
                    detail=(
                        "scheduler cancellation was not requested; no active scheduler record "
                        "remained after the operation"
                        if phase == "missing"
                        else (
                            "scheduler cancellation was not requested; "
                            f"post-operation phase={phase}"
                        )
                        if scheduler_verified
                        else "scheduler preservation was not verified"
                        + (f": {status_error}" if status_error else "")
                    ),
                    metadata={"relay_job_id": job.job_id},
                )
            )
        for scheduler_job_id in job.unowned_scheduler_job_ids:
            resources.append(
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=location,
                    action=("cancel" if cancel_jobs and cancel_scheduler_jobs else "retain"),
                    ownership_verified=False,
                    outcome="refused",
                    provider=job.scheduler_provider,
                    verified_after_operation=False,
                    residual=True,
                    detail=(
                        "scheduler identity was observed but no ownership record bound it "
                        "to this relay job and task with an authenticated JARVIS proof"
                    ),
                )
            )
    return resources


def _scheduler_phase_after_operation(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    provider: str,
) -> tuple[str | None, str | None]:
    try:
        if should_execute_on_cluster(definition):
            raw_status = cast(
                object,
                json.loads(
                    run_remote_clio(
                        definition,
                        [
                            "scheduler",
                            "status",
                            scheduler_job_id,
                            "--cluster",
                            definition.name,
                            "--provider",
                            provider,
                        ],
                    )
                ),
            )
            if not isinstance(raw_status, dict):
                raise RelayError("scheduler status did not return a JSON object")
            phase = cast(dict[str, object], raw_status).get("phase")
            active_record_found = cast(dict[str, object], raw_status).get("active_record_found")
            if phase == SchedulerPhase.UNKNOWN.value and active_record_found is False:
                return "missing", None
            return (str(phase), None) if isinstance(phase, str) else (None, None)
        status = provider_for_scheduler(provider).poll(scheduler_job_id)
        if status.phase is SchedulerPhase.UNKNOWN and status.active_record_found is False:
            return "missing", None
        return status.phase.value, None
    except (RelayError, json.JSONDecodeError) as exc:
        return None, str(exc)


def _cancel_owned_scheduler_jobs(
    definition: ClusterDefinition,
    jobs: list[_OwnedRelayJob],
    *,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.5,
) -> tuple[list[CleanupResource], list[str]]:
    resources: list[CleanupResource] = []
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for job in jobs:
        for scheduler_job_id in job.scheduler_job_ids:
            identity = (job.scheduler_provider, scheduler_job_id)
            if identity in seen:
                continue
            seen.add(identity)
            resource, error = _cancel_owned_scheduler_job(
                definition,
                scheduler_job_id,
                relay_job_id=job.job_id,
                provider=job.scheduler_provider,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
            resources.append(resource)
            if error is not None:
                errors.append(error)
    return resources, errors


def _cancel_owned_scheduler_job(
    definition: ClusterDefinition,
    scheduler_job_id: str,
    *,
    relay_job_id: str,
    provider: str,
    timeout_seconds: float,
    poll_seconds: float,
) -> tuple[CleanupResource, str | None]:
    deadline = monotonic() + timeout_seconds
    accepted = False
    cancel_detail: str | None = None
    try:
        if should_execute_on_cluster(definition):
            with remote_command_timeout(
                min(
                    REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS,
                    max(0.01, deadline - monotonic()),
                )
            ):
                raw_cancel = cast(
                    object,
                    json.loads(
                        run_remote_clio(
                            definition,
                            [
                                "scheduler",
                                "cancel",
                                scheduler_job_id,
                                "--cluster",
                                definition.name,
                                "--provider",
                                provider,
                            ],
                        )
                    ),
                )
            accepted = (
                isinstance(raw_cancel, dict)
                and cast(dict[str, object], raw_cancel).get("accepted") is True
            )
        else:
            result = provider_for_scheduler(provider).cancel(scheduler_job_id)
            accepted = result.returncode == 0
            cancel_detail = result.stderr.strip() or result.stdout.strip() or None
    except (RelayError, json.JSONDecodeError) as exc:
        cancel_detail = str(exc)

    last_phase = "unknown"
    while monotonic() < deadline:
        try:
            if should_execute_on_cluster(definition):
                with remote_command_timeout(
                    min(
                        REMOTE_CLEANUP_COMMAND_TIMEOUT_SECONDS,
                        max(0.01, deadline - monotonic()),
                    )
                ):
                    raw_status = cast(
                        object,
                        json.loads(
                            run_remote_clio(
                                definition,
                                [
                                    "scheduler",
                                    "status",
                                    scheduler_job_id,
                                    "--cluster",
                                    definition.name,
                                    "--provider",
                                    provider,
                                ],
                            )
                        ),
                    )
                if not isinstance(raw_status, dict):
                    raise RelayError("scheduler status did not return a JSON object")
                phase = cast(dict[str, object], raw_status).get("phase")
                last_phase = str(phase) if phase is not None else "unknown"
            else:
                last_phase = provider_for_scheduler(provider).poll(scheduler_job_id).phase.value
        except (RelayError, json.JSONDecodeError) as exc:
            cancel_detail = str(exc)
        if last_phase == "canceled":
            return (
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=definition.ssh_host,
                    action="cancel",
                    ownership_verified=True,
                    outcome="canceled",
                    provider=provider,
                    verified_after_operation=True,
                    observed_state=last_phase,
                    detail="scheduler reported the canceled terminal phase",
                    metadata={"relay_job_id": relay_job_id},
                ),
                None,
            )
        if last_phase in {"completed", "failed"}:
            return (
                CleanupResource(
                    kind="scheduler_job",
                    resource_id=scheduler_job_id,
                    location=definition.ssh_host,
                    action="cancel",
                    ownership_verified=True,
                    outcome="terminal",
                    provider=provider,
                    verified_after_operation=True,
                    observed_state=last_phase,
                    detail=(
                        "scheduler reached a terminal phase during the cancellation race; "
                        f"cancellation is not claimed: accepted={accepted}, phase={last_phase}"
                        + (f", detail={cancel_detail}" if cancel_detail else "")
                    ),
                    metadata={"relay_job_id": relay_job_id},
                ),
                None,
            )
        sleep(poll_seconds)

    detail = (
        f"scheduler cancellation was not confirmed: accepted={accepted}, phase={last_phase}"
        + (f", detail={cancel_detail}" if cancel_detail else "")
    )
    return (
        CleanupResource(
            kind="scheduler_job",
            resource_id=scheduler_job_id,
            location=definition.ssh_host,
            action="cancel",
            ownership_verified=True,
            outcome="failed",
            provider=provider,
            residual=True,
            detail=detail,
            metadata={"relay_job_id": relay_job_id},
        ),
        detail,
    )


def _cleanup_owned_runtime_sessions(
    *,
    cluster: str,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    mode: Literal["detach", "teardown"],
    cancel_scheduler_jobs: bool,
    scheduler_sentinel_ids: tuple[str, ...] = (),
    owned_jobs: list[_OwnedRelayJob] | None = None,
) -> list[dict[str, object]]:
    """Clean exact owned gateways and rescan boundedly until admission is stable."""
    queue = storage_managed_queue(RelaySettings.from_env())
    reports: list[dict[str, object]] = []
    if mode == "detach":
        target_ids = _owned_runtime_gateway_ids_needing_cleanup(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )
        return _cleanup_owned_runtime_sessions_once(
            cluster=cluster,
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            mode=mode,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            target_session_ids=target_ids,
        )
    for _pass in range(MAX_OWNER_GATEWAY_CLEANUP_PASSES):
        target_ids = _owned_runtime_gateway_ids_needing_cleanup(
            queue=queue,
            definition=definition,
            cluster=cluster,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )
        if not target_ids:
            return reports
        if owner_session_generation_id is not None and scheduler_sentinel_ids:
            gateway_scheduler_job_ids = _owned_gateway_scheduler_job_ids(
                queue=queue,
                definition=definition,
                cluster=cluster,
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_session_generation_id,
            )
            _assert_scheduler_sentinels_unrelated(
                scheduler_sentinel_ids,
                owned_jobs or [],
                gateway_scheduler_job_ids=gateway_scheduler_job_ids,
            )
        pass_reports = _cleanup_owned_runtime_sessions_once(
            cluster=cluster,
            definition=definition,
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            mode=mode,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            target_session_ids=target_ids,
        )
        reports.extend(pass_reports)
        if any(
            report.get("ok") is False or bool(report.get("residual_resources"))
            for report in pass_reports
        ):
            return reports
    residual_ids = _owned_runtime_gateway_ids_needing_cleanup(
        queue=queue,
        definition=definition,
        cluster=cluster,
        owner_session_id=owner_session_id,
        owner_session_generation_id=owner_session_generation_id,
    )
    if residual_ids:
        raise RelayError(
            "owned gateway cleanup did not converge after bounded rescans: "
            + ", ".join(sorted(residual_ids))
        )
    return reports


def _owned_runtime_gateway_ids_needing_cleanup(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    owner_session_id: str,
    owner_session_generation_id: str | None,
) -> set[str]:
    """Return the current non-closed owned gateway ids from local and remote stores."""
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway cleanup discovery exceeds the bounded source limit; "
            "no gateway cleanup was attempted"
        )
    documents = [gateway.model_dump(mode="json") for gateway in local_gateways]
    if should_execute_on_cluster(definition):
        documents.extend(
            _complete_remote_source_collection(
                definition,
                ["gateway", "list", "--cluster", cluster],
                record_key="gateway_sessions",
                label=f"remote gateway cleanup discovery for {cluster}",
            )
        )
    targets: set[str] = set()
    for gateway in documents:
        session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if (
            not isinstance(session_id, str)
            or gateway.get("state") == GatewaySessionState.CLOSED.value
            or not isinstance(metadata, dict)
        ):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if (
            typed_metadata.get("owner") != "clio-relay"
            or typed_metadata.get("owner_session_id") != owner_session_id
        ):
            continue
        observed_generation = typed_metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None and observed_generation not in {
            None,
            owner_session_generation_id,
        }:
            continue
        targets.add(session_id)
    return targets


def _cleanup_owned_runtime_sessions_once(
    *,
    cluster: str,
    definition: ClusterDefinition,
    owner_session_id: str,
    owner_session_generation_id: str | None = None,
    mode: Literal["detach", "teardown"],
    cancel_scheduler_jobs: bool,
    target_session_ids: set[str],
) -> list[dict[str, object]]:
    settings = RelaySettings.from_env()
    queue = storage_managed_queue(settings)
    queue.initialize()
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster=cluster,
        definition=definition,
        token="",
        secret_key="",
    )
    reports: list[dict[str, object]] = []
    seen_session_ids: set[str] = set()
    local_gateways, local_truncated = queue.scan_gateway_sessions(
        limit=MAX_INTERNAL_COLLECTION_RECORDS,
        cluster=cluster,
    )
    if local_truncated:
        raise RelayError(
            "local gateway cleanup discovery exceeds the bounded source limit "
            f"{MAX_INTERNAL_COLLECTION_RECORDS}; no gateway cleanup was attempted"
        )
    remote_gateways: list[dict[str, Any]] = []
    if should_execute_on_cluster(definition):
        remote_gateways = _complete_remote_source_collection(
            definition,
            ["gateway", "list", "--cluster", cluster],
            record_key="gateway_sessions",
            label=f"remote gateway cleanup discovery for {cluster}",
        )

    for gateway in local_gateways:
        if gateway.session_id not in target_session_ids:
            continue
        if gateway.state == GatewaySessionState.CLOSED and mode == "detach":
            continue
        if gateway.metadata.get("owner") != "clio-relay":
            continue
        if gateway.metadata.get("owner_session_id") != owner_session_id:
            continue
        gateway_generation = gateway.metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None:
            if not isinstance(gateway_generation, str) or not gateway_generation:
                reports.append(
                    _unverified_gateway_generation_report(
                        gateway_session_id=gateway.session_id,
                        location=str(settings.core_dir),
                        mode=mode,
                        expected_generation_id=owner_session_generation_id,
                        observed_generation_id=gateway_generation,
                    )
                )
                continue
            if gateway_generation != owner_session_generation_id:
                continue
        if mode == "detach":
            result = supervisor.detach(session_id=gateway.session_id)
        else:
            result = supervisor.stop(
                session_id=gateway.session_id,
                cancel_scheduler_job=cancel_scheduler_jobs,
            )
        reports.append(result.json_payload())
        seen_session_ids.add(gateway.session_id)
    for gateway in remote_gateways:
        remote_session_id = gateway.get("session_id")
        metadata = gateway.get("metadata")
        if (
            not isinstance(remote_session_id, str)
            or remote_session_id not in target_session_ids
            or remote_session_id in seen_session_ids
        ):
            continue
        if gateway.get("state") == GatewaySessionState.CLOSED.value and mode == "detach":
            continue
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, object], metadata)
        if typed_metadata.get("owner") != "clio-relay":
            continue
        if typed_metadata.get("owner_session_id") != owner_session_id:
            continue
        gateway_generation = typed_metadata.get("owner_session_generation_id")
        if owner_session_generation_id is not None:
            if not isinstance(gateway_generation, str) or not gateway_generation:
                reports.append(
                    _unverified_gateway_generation_report(
                        gateway_session_id=remote_session_id,
                        location=definition.ssh_host,
                        mode=mode,
                        expected_generation_id=owner_session_generation_id,
                        observed_generation_id=gateway_generation,
                    )
                )
                continue
            if gateway_generation != owner_session_generation_id:
                continue
        if mode == "detach":
            args = [
                "gateway",
                "detach-runtime",
                remote_session_id,
                "--cluster",
                cluster,
            ]
        else:
            args = [
                "gateway",
                "stop-runtime",
                remote_session_id,
                "--cluster",
                cluster,
                ("--cancel-scheduler-job" if cancel_scheduler_jobs else "--keep-scheduler-job"),
            ]
        remote_report = cast(object, json.loads(run_remote_clio(definition, args)))
        if not isinstance(remote_report, dict):
            raise RelayError(
                f"remote gateway cleanup did not return a JSON object: {remote_session_id}"
            )
        reports.append(
            {str(key): value for key, value in cast(dict[object, object], remote_report).items()}
        )
        seen_session_ids.add(remote_session_id)
    return reports


def _unverified_gateway_generation_report(
    *,
    gateway_session_id: str,
    location: str,
    mode: Literal["detach", "teardown"],
    expected_generation_id: str,
    observed_generation_id: object,
) -> dict[str, object]:
    """Return fail-closed evidence for an owner-session gateway without a generation."""
    detail = (
        "owned gateway record has no exact session generation; cleanup was refused: "
        f"gateway={gateway_session_id} expected={expected_generation_id} "
        f"observed={observed_generation_id!r}"
    )
    resource = CleanupResource(
        kind="gateway_record",
        resource_id=gateway_session_id,
        location=location,
        action="retain" if mode == "detach" else "close",
        ownership_verified=False,
        outcome="refused",
        verified_after_operation=False,
        residual=True,
        detail=detail,
        metadata={
            "expected_owner_session_generation_id": expected_generation_id,
            "observed_owner_session_generation_id": observed_generation_id,
        },
    )
    return {
        "resources": [resource.model_dump(mode="json")],
        "residual_resources": [resource.model_dump(mode="json")],
        "errors": [detail],
        "ok": False,
    }


def _merge_gateway_cleanup_resources(
    report: SessionLifecycleReport,
    gateway_reports: list[dict[str, object]],
) -> None:
    """Merge gateway connector cleanup into the owning desktop-session report."""
    for gateway_report in gateway_reports:
        raw_errors = gateway_report.get("errors")
        if isinstance(raw_errors, list):
            for raw_error in cast(list[object], raw_errors):
                if isinstance(raw_error, str) and raw_error not in report.errors:
                    report.errors.append(raw_error)
        raw_resources = gateway_report.get("resources")
        if not isinstance(raw_resources, list):
            report.errors.append("gateway cleanup report did not contain resource evidence")
            continue
        for raw_resource in cast(list[object], raw_resources):
            resource = CleanupResource.model_validate(raw_resource)
            if any(
                existing.kind == resource.kind
                and existing.resource_id == resource.resource_id
                and existing.action == resource.action
                for existing in report.resources
            ):
                continue
            report.resources.append(resource)


def _verified_owner_session_generation(
    status: dict[str, object],
    *,
    session_id: str,
) -> str:
    """Return the exact durable generation for a session teardown attempt."""
    if status.get("session_id") != session_id or status.get("owner") != "clio-relay":
        raise RelayError("remote session status did not prove the requested owned session")
    generation_id = status.get("session_generation_id")
    if not isinstance(generation_id, str) or not generation_id:
        raise RelayError("remote session status did not contain an owned generation id")
    _require_durable_session_identity(generation_id, field="session_generation_id")
    if status.get("running") is True and status.get("ownership_verified") is not True:
        raise RelayError("running remote session failed process ownership verification")
    return generation_id


def _owned_session_recovery_status(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    cluster: str,
    session_id: str,
) -> OwnedSessionRecoveryStatus:
    """Read exact dead-session recovery evidence at the authoritative boundary."""
    if remote_execution:
        raw_status = cast(
            object,
            json.loads(
                run_remote_clio(
                    definition,
                    [
                        "session",
                        "recovery-status",
                        "--cluster",
                        cluster,
                        "--session-id",
                        session_id,
                    ],
                )
            ),
        )
        return OwnedSessionRecoveryStatus.model_validate(raw_status)
    return _inspect_owned_session_recovery_after_transition(
        cluster=cluster,
        session_id=session_id,
        core_dir=queue.root,
    )


def _verified_recovered_owner_session_generation(
    status: OwnedSessionRecoveryStatus,
    *,
    cluster: str,
    session_id: str,
) -> str:
    """Return an exact generation only from complete recovery evidence."""
    generation_id = status.session_generation_id
    committed_identity = status.metadata_verified
    pre_metadata_identity = bool(
        not status.metadata_verified
        and not status.cleanup_receipt
        and status.start_attempt_verified
        and status.start_state in {"starting", "failed"}
        and status.start_phase is not None
    )
    if not (
        status.cluster == cluster
        and status.session_id == session_id
        and status.owner == "clio-relay"
        and (committed_identity or pre_metadata_identity)
        and status.cluster_registry_verified
        and status.durable_generation_verified
        and status.ownership_verified
        and status.recovery_verified
        and not status.errors
        and generation_id is not None
    ):
        detail = "; ".join(status.errors) or "recovery proof was incomplete"
        raise RelayError(f"owned session recovery was refused: {detail}")
    _require_durable_session_identity(generation_id, field="session_generation_id")
    if status.running and status.process_state != "owned_running":
        raise RelayError("owned session recovery did not prove the running process identity")
    if not status.running and status.process_state not in {
        "absent",
        "owned_terminal",
        "cleanup_pending",
        "already_closed",
    }:
        raise RelayError("owned session recovery did not prove the recorded process stopped")
    return generation_id


def _owner_session_recovery_validation_resource(
    status: OwnedSessionRecoveryStatus,
) -> ValidationResource:
    """Project recovery evidence into the canonical machine-readable report."""
    generation_id = status.session_generation_id or "generation-unverified"
    return ValidationResource(
        kind="owner_session_recovery",
        resource_id=f"{status.session_id}:{generation_id}",
        role="cleanup_identity_recovery",
        cluster=status.cluster,
        state="verified" if status.recovery_verified else "refused",
        metadata={
            "session_generation_id": status.session_generation_id,
            "api_pid": status.api_pid,
            "remote_api_port": status.remote_api_port,
            "process_start_marker": status.process_start_marker,
            "leader_process_state": status.leader_process_state,
            "process_state": status.process_state,
            "running": status.running,
            "process_absence_verified": status.process_absence_verified,
            "generation_process_pids": status.generation_process_pids,
            "generation_process_absence_verified": (status.generation_process_absence_verified),
            "metadata_verified": status.metadata_verified,
            "cluster_registry_verified": status.cluster_registry_verified,
            "durable_generation_verified": status.durable_generation_verified,
            "cleanup_receipt": status.cleanup_receipt,
            "cleanup_paths_pending": status.cleanup_paths_pending,
            "api_release_identity_verified": status.api_release_identity_verified,
            "ownership_token_present": status.ownership_token_present,
            "ownership_verified": status.ownership_verified,
            "recovery_verified": status.recovery_verified,
            "errors": status.errors,
            "admission_status": status.admission_status,
        },
    )


def _verified_owner_session_detach(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    expected_session_generation_id: str | None = None,
) -> str:
    """Return the exact generation only when detach retained its owned API."""
    if report.mode != "detach" or report.session_id != session_id:
        raise RelayError("session detach report identity did not match the requested session")
    generation_id = report.session_generation_id
    if not isinstance(generation_id, str) or not generation_id:
        raise RelayError("session detach did not prove an owned session generation")
    _require_durable_session_identity(generation_id, field="session_generation_id")
    if (
        expected_session_generation_id is not None
        and generation_id != expected_session_generation_id
    ):
        raise RelayError("owned session generation changed during desktop detach")
    if report.errors or report.residual_resources:
        raise RelayError("session detach did not prove remote session retention")
    api_resources = [
        resource for resource in report.resources if resource.kind == "remote_relay_api"
    ]
    if len(api_resources) != 1:
        raise RelayError("session detach must contain exactly one remote relay API result")
    api_resource = api_resources[0]
    if not (
        api_resource.action == "retain"
        and api_resource.outcome == "retained"
        and api_resource.ownership_verified
        and api_resource.verified_after_operation
        and not api_resource.residual
    ):
        raise RelayError("session detach did not verify remote relay API retention")
    return generation_id


def _verify_owner_session_teardown(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    session_generation_id: str,
    stop_worker: bool,
) -> None:
    """Reject closure unless all requested owner-session cleanup is verified."""
    if report.mode != "teardown" or report.session_id != session_id:
        raise RelayError("session teardown report identity did not match the requested session")
    if report.session_generation_id != session_generation_id:
        raise RelayError("session teardown report generation did not match the quiesced generation")
    if report.errors:
        raise RelayError("session teardown reported errors: " + "; ".join(report.errors))
    if report.residual_resources:
        residual_ids = sorted(resource.resource_id for resource in report.residual_resources)
        raise RelayError(
            "session teardown left requested residual resources: " + ", ".join(residual_ids)
        )

    policy = report.cleanup_policy
    expected_policy_keys = {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}
    if set(policy) != expected_policy_keys or any(type(policy[key]) is not bool for key in policy):
        raise RelayError("session teardown cleanup policy is incomplete or invalid")
    cancel_jobs = policy["cancel_jobs"]
    cancel_scheduler_jobs = policy["cancel_scheduler_jobs"]
    if policy["stop_worker"] is not stop_worker:
        raise RelayError("session teardown worker policy did not match the requested cleanup")
    if cancel_scheduler_jobs and not cancel_jobs:
        raise RelayError("session teardown scheduler cancellation requires relay cancellation")
    if report.relay_cancel_requested is not cancel_jobs:
        raise RelayError("session teardown relay-job disposition did not match cleanup policy")
    if report.scheduler_cancel_requested is not cancel_scheduler_jobs:
        raise RelayError("session teardown scheduler disposition did not match cleanup policy")

    allowed_resource_kinds = {
        "browser_proxy",
        "desktop_connector",
        "gateway_record",
        "relay_job",
        "remote_connector",
        "remote_relay_api",
        "remote_session_files",
        "scheduler_job",
        "scheduler_sentinel",
        "worker_service",
    }
    unknown_kinds = sorted(
        {resource.kind for resource in report.resources} - allowed_resource_kinds
    )
    if unknown_kinds:
        raise RelayError(
            "session teardown reported unknown cleanup resource kinds: " + ", ".join(unknown_kinds)
        )
    resource_keys = [(resource.kind, resource.resource_id) for resource in report.resources]
    duplicate_resource_keys = sorted(
        f"{kind}:{resource_id}"
        for (kind, resource_id), count in Counter(resource_keys).items()
        if count != 1
    )
    if duplicate_resource_keys:
        raise RelayError(
            "session teardown reported duplicate cleanup resources: "
            + ", ".join(duplicate_resource_keys)
        )

    prior_status = report.prior_session_status
    post_status = report.post_session_status
    if (
        prior_status is None
        or prior_status.session_generation_id != session_generation_id
        or not prior_status.ownership_verified
    ):
        raise RelayError("session teardown did not prove prior generation ownership")
    if (
        post_status is None
        or post_status.session_generation_id != session_generation_id
        or post_status.running
        or not post_status.ownership_verified
    ):
        raise RelayError("session teardown did not prove the owned API generation stopped")

    api_resources = [
        resource for resource in report.resources if resource.kind == "remote_relay_api"
    ]
    if len(api_resources) != 1:
        raise RelayError("session teardown must contain exactly one remote relay API result")
    api_resource = api_resources[0]
    if not (
        api_resource.action == "stop"
        and api_resource.outcome in {"stopped", "missing"}
        and api_resource.ownership_verified
        and api_resource.verified_after_operation
        and not api_resource.residual
    ):
        raise RelayError("session teardown did not verify remote relay API cleanup")

    session_file_resources = [
        resource for resource in report.resources if resource.kind == "remote_session_files"
    ]
    if len(session_file_resources) != 1:
        raise RelayError("session teardown must contain exactly one remote session-file result")
    session_files = session_file_resources[0]
    if not (
        session_files.resource_id == f"{session_id}:{session_generation_id}"
        and session_files.action == "close"
        and session_files.outcome == "closed"
        and session_files.ownership_verified
        and session_files.verified_after_operation
        and not session_files.residual
    ):
        raise RelayError("session teardown did not verify remote session-file cleanup")

    gateway_resources = [
        resource for resource in report.resources if resource.kind == "gateway_record"
    ]
    relay_resource_ids = {
        resource.resource_id for resource in report.resources if resource.kind == "relay_job"
    }
    gateway_resource_ids = {resource.resource_id for resource in gateway_resources}
    connector_resources = [
        resource
        for resource in report.resources
        if resource.kind in {"desktop_connector", "remote_connector"}
    ]
    if (gateway_resources or connector_resources) and not cleanup_connectors_cover_gateways(
        connector_resources,
        gateway_resources,
        mode="teardown",
    ):
        raise RelayError(
            "session teardown connector evidence did not cover each owned gateway exactly"
        )

    browser_resources = [
        resource for resource in report.resources if resource.kind == "browser_proxy"
    ]
    for resource in browser_resources:
        linked_gateway_id = resource.metadata.get("gateway_session_id")
        if not (
            isinstance(linked_gateway_id, str)
            and linked_gateway_id in gateway_resource_ids
            and resource.action == "stop"
            and resource.outcome in {"stopped", "missing"}
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify browser proxy cleanup: {resource.resource_id}"
            )

    for resource in report.resources:
        if resource.kind in {"desktop_connector", "remote_connector"} and not (
            resource.action == "stop"
            and resource.outcome in {"stopped", "missing"}
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify connector cleanup: {resource.resource_id}"
            )
        if resource.kind == "gateway_record" and not (
            resource.action == "close"
            and resource.outcome == "closed"
            and resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            raise RelayError(
                f"session teardown did not verify gateway closure: {resource.resource_id}"
            )
        if resource.kind == "relay_job":
            retained = resource.action == "retain" and resource.outcome in {
                "retained",
                "terminal",
            }
            canceled = resource.action == "cancel" and resource.outcome in {
                "canceled",
                "terminal",
            }
            disposition_matches_policy = (retained and not cancel_jobs) or (
                cancel_jobs
                and (canceled or (resource.action == "retain" and resource.outcome == "terminal"))
            )
            if not (
                disposition_matches_policy
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            ):
                raise RelayError(
                    f"session teardown relay-job disposition contradicted cleanup policy: "
                    f"{resource.resource_id}"
                )
        if resource.kind == "scheduler_job":
            linked_relay_id = resource.metadata.get("relay_job_id")
            linked_gateway_id = resource.metadata.get("gateway_session_id")
            linked = (
                isinstance(linked_relay_id, str) and linked_relay_id in relay_resource_ids
            ) or (isinstance(linked_gateway_id, str) and linked_gateway_id in gateway_resource_ids)
            retained = resource.action == "retain" and (
                (
                    resource.outcome == "retained"
                    and resource.observed_state in {"submitted", "pending", "allocated", "running"}
                )
                or (
                    resource.outcome == "terminal"
                    and resource.observed_state in {"completed", "failed", "canceled"}
                )
                or (resource.outcome == "missing" and resource.observed_state == "missing")
            )
            canceled = resource.action == "cancel" and (
                (resource.outcome == "canceled" and resource.observed_state == "canceled")
                or (
                    resource.outcome == "terminal"
                    and resource.observed_state in {"completed", "failed"}
                )
            )
            if not (
                linked
                and resource.provider is not None
                and (
                    (retained and not cancel_scheduler_jobs) or (canceled and cancel_scheduler_jobs)
                )
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            ):
                raise RelayError(
                    f"session teardown did not verify scheduler disposition: {resource.resource_id}"
                )

        if resource.kind == "scheduler_sentinel" and not (
            cancel_jobs
            and cancel_scheduler_jobs
            and resource.action == "retain"
            and resource.outcome == "retained"
            and not resource.ownership_verified
            and resource.verified_after_operation
            and resource.observed_state in SCHEDULER_SENTINEL_PRESERVED_PHASES
            and not resource.residual
            and resource.metadata.get("unowned_sentinel") is True
            and resource.metadata.get("preservation_verified") is True
        ):
            raise RelayError(
                "session teardown did not verify scheduler sentinel preservation: "
                f"{resource.resource_id}"
            )

    worker_resources = [
        resource for resource in report.resources if resource.kind == "worker_service"
    ]
    if stop_worker:
        if len(worker_resources) != 1:
            raise RelayError("session teardown must contain exactly one worker service result")
        worker = worker_resources[0]
        if not (
            worker.action == "stop"
            and worker.outcome in {"stopped", "missing"}
            and worker.ownership_verified
            and worker.verified_after_operation
            and worker.observed_state in {"inactive", "not-found"}
            and not worker.residual
        ):
            raise RelayError("session teardown did not verify worker service inactivity")
    elif worker_resources:
        raise RelayError("session teardown reported worker cleanup when it was not requested")


def _require_exact_owner_session_admission(
    status: dict[str, object],
    *,
    owner_session_id: str,
    session_generation_id: str,
    cleanup_operation_id: str,
    cleanup_policy: dict[str, bool],
    closed: bool,
    label: str,
) -> dict[str, object] | None:
    """Require one exact closing or closed admission record before closure handling."""
    raw_intent = status.get("cleanup_intent")
    intent = cast(dict[str, object], raw_intent) if isinstance(raw_intent, dict) else None
    expected_policy = {
        key: cleanup_policy[key] for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
    }
    raw_closure = status.get("closure")
    closure: dict[str, object] | None = None
    closure_matches = False
    if closed:
        try:
            closure_model = OwnerSessionClosure.model_validate(raw_closure)
        except ValidationError as exc:
            raise RelayError(f"{label} admission closure evidence was invalid") from exc
        closure_matches = (
            closure_model.schema_version == "clio-relay.owner-session-closure.v1"
            and closure_model.owner_session_id == owner_session_id
            and closure_model.session_generation_id == session_generation_id
            and closure_model.covered_by_session_generation_id is None
            and closure_model.covered_legacy_job_ids == []
            and closure_model.residual_resource_ids == []
        )
        closure = cast(dict[str, object], closure_model.model_dump(mode="json"))
    if not (
        status.get("schema_version") == "clio-relay.owner-session-admission-status.v1"
        and status.get("owner_session_id") == owner_session_id
        and status.get("session_generation_id") == session_generation_id
        and status.get("active_generation_id") == (None if closed else session_generation_id)
        and status.get("closing_generation_id") == session_generation_id
        and status.get("active") is (not closed)
        and status.get("closing") is True
        and status.get("closed") is closed
        and status.get("open") is False
        and intent is not None
        and intent.get("schema_version") == "clio-relay.owner-session-cleanup-intent.v1"
        and intent.get("owner_session_id") == owner_session_id
        and intent.get("session_generation_id") == session_generation_id
        and intent.get("operation_id") == cleanup_operation_id
        and {
            key: intent.get(key) for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        == expected_policy
        and (closure_matches if closed else raw_closure is None)
    ):
        raise RelayError(f"{label} admission evidence was incomplete or inconsistent")
    return closure if closed else None


def _mark_owner_session_closed(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    remote_execution: bool,
    session_id: str,
    local_admission_session_id: str,
    session_generation_id: str,
    legacy_unversioned_job_ids: list[str],
    finalized_recovery: OwnedSessionRecoveryStatus,
    finalized_report: SessionLifecycleReport,
) -> None:
    """Close the authoritative generation, then its cluster-scoped desktop mirror."""
    if definition.name != cluster:
        raise RelayError("owner-session closure cluster identity changed")
    verified_report = _verified_finalized_cleanup_report(
        finalized_recovery,
        report=finalized_report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=session_generation_id,
        expected_cleanup_operation_id=finalized_report.cleanup_operation_id,
        expected_cleanup_policy=finalized_report.cleanup_policy,
    )
    cleanup_operation_id = verified_report.cleanup_operation_id
    if cleanup_operation_id is None:
        raise RelayError("finalized owner-session closure omitted its operation identity")
    raw_authoritative_admission = finalized_recovery.admission_status
    if not isinstance(raw_authoritative_admission, dict):
        raise RelayError("finalized owner-session closure omitted authoritative admission evidence")
    authoritative_admission = raw_authoritative_admission
    authoritative_already_closed = authoritative_admission.get("closed") is True
    authoritative_closure_evidence = _require_exact_owner_session_admission(
        authoritative_admission,
        owner_session_id=session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        cleanup_policy=verified_report.cleanup_policy,
        closed=authoritative_already_closed,
        label="authoritative owner-session",
    )
    if authoritative_already_closed:
        if finalized_recovery.process_state != "already_closed":
            raise RelayError("authoritative closure evidence disagreed with recovery process state")
        if remote_execution and legacy_unversioned_job_ids:
            raise RelayError(
                "authoritative closure retry cannot infer legacy job coverage from admission status"
            )

    payload: dict[str, object]
    if remote_execution and not authoritative_already_closed:
        args = [
            "session",
            "mark-closed",
            "--session-id",
            session_id,
            "--session-generation-id",
            session_generation_id,
        ]
        for job_id in legacy_unversioned_job_ids:
            args.extend(["--legacy-unversioned-job-id", job_id])
        raw_payload = cast(
            object,
            json.loads(run_remote_clio(definition, args)),
        )
        if not isinstance(raw_payload, dict):
            raise RelayError("remote owner-session closure did not return a JSON object")
        payload = cast(dict[str, object], raw_payload)
    elif remote_execution:
        if authoritative_closure_evidence is None:  # pragma: no cover - verifier invariant
            raise RelayError("authoritative owner-session closure evidence disappeared")
        payload = authoritative_closure_evidence
    elif authoritative_already_closed:
        closure = queue.get_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
        )
        if closure is None:
            raise RelayError("authoritative owner-session closure disappeared after admission read")
        payload = cast(dict[str, object], closure.model_dump(mode="json"))
        if legacy_unversioned_job_ids:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure disappeared after admission read")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
    else:
        closure = queue.set_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
            legacy_unversioned_job_ids=legacy_unversioned_job_ids,
        )
        payload = cast(dict[str, object], closure.model_dump(mode="json"))
        if legacy_unversioned_job_ids:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure was not persisted")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
    if (
        payload.get("owner_session_id") != session_id
        or payload.get("session_generation_id") != session_generation_id
        or payload.get("residual_resource_ids") != []
    ):
        raise RelayError("owner-session closure did not match the verified teardown generation")
    if legacy_unversioned_job_ids:
        raw_legacy_closure = payload.get("legacy_closure")
        if not isinstance(raw_legacy_closure, dict):
            raise RelayError("owner-session closure omitted legacy job coverage")
        legacy_closure = cast(dict[str, object], raw_legacy_closure)
        if (
            legacy_closure.get("session_generation_id") is not None
            or legacy_closure.get("covered_by_session_generation_id") != session_generation_id
            or legacy_closure.get("covered_legacy_job_ids")
            != sorted(set(legacy_unversioned_job_ids))
        ):
            raise RelayError("owner-session legacy coverage did not match verified job ids")
    local_status = queue.owner_session_generation_status(
        local_admission_session_id,
        session_generation_id=session_generation_id,
    )
    local_already_closed = local_status.get("closed") is True
    local_closure_evidence = _require_exact_owner_session_admission(
        local_status,
        owner_session_id=local_admission_session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        cleanup_policy=verified_report.cleanup_policy,
        closed=local_already_closed,
        label="desktop owner-session mirror",
    )
    if local_already_closed:
        local_closure = queue.get_owner_session_closed(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
        if local_closure is None:
            raise RelayError("desktop owner-session closure disappeared after admission read")
        if local_closure.model_dump(mode="json") != local_closure_evidence:
            raise RelayError("desktop owner-session closure changed after admission read")
    else:
        local_closure = queue.set_owner_session_closed(
            local_admission_session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
        )
    if (
        local_closure.owner_session_id != local_admission_session_id
        or local_closure.session_generation_id != session_generation_id
        or local_closure.residual_resource_ids
    ):
        raise RelayError("desktop owner-session admission mirror did not close exactly")


def _remote_mcp_cache_status(
    registration: RemoteMcpServerConfig,
    entry: RemoteMcpSchemaCacheEntry | None,
) -> dict[str, object]:
    if entry is None:
        return {"state": "missing", "fresh": False}
    execution_matches = entry.execution_fingerprint == remote_mcp_execution_fingerprint(
        registration
    )
    fresh = entry.is_fresh()
    if fresh and execution_matches:
        state = "fresh"
    elif not execution_matches:
        state = "command_changed"
    else:
        state = "stale"
    return {
        "state": state,
        "fresh": fresh,
        "execution_matches": execution_matches,
        "discovered_at": entry.discovered_at.isoformat(),
        "expires_at": entry.expires_at.isoformat(),
        "schema_digest": entry.schema_digest,
        "tool_names": sorted(tool.name for tool in entry.tools),
        "provenance": entry.provenance.model_dump(mode="json"),
    }


def _require_discovery_success(result: dict[str, object], job_id: str) -> None:
    state = result.get("state")
    if state != JobState.SUCCEEDED.value:
        error = result.get("error")
        detail = f": {error}" if isinstance(error, str) and error else ""
        raise RelayError(f"remote MCP discovery job {job_id} ended in state {state}{detail}")


def _run_jarvis_remote_contract_discovery(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> tuple[str, dict[str, Any], list[dict[str, Any]], bytes]:
    """Discover the actual cluster-side JARVIS MCP before accepting its virtual route."""
    idempotency_key = f"mcp:jarvis-contract:{cluster}:{uuid4().hex}"
    if should_execute_on_cluster(definition):
        remote_args = [
            "jarvis-mcp-call",
            "--cluster",
            cluster,
            "--operation",
            "tools/list",
            "--idempotency-key",
            idempotency_key,
            "--timeout-seconds",
            str(
                min(
                    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
                    max(1, math.ceil(wait_timeout_seconds)),
                )
            ),
        ]
        job_id = _last_nonempty_line(run_remote_clio(definition, remote_args))
        terminal = _json_output(
            run_remote_clio(
                definition,
                [
                    "job",
                    "wait",
                    job_id,
                    "--timeout-seconds",
                    str(wait_timeout_seconds),
                    "--poll-seconds",
                    str(poll_seconds),
                ],
            ),
            "JARVIS MCP discovery wait",
        )
        _require_discovery_success(terminal, job_id)
        artifacts = _remote_artifact_records(definition, job_id)
        artifact_payload = _read_remote_artifact_kind_bytes(
            definition,
            artifacts,
            kind="mcp_result",
        )
    else:
        server = jarvis_mcp_server()
        server_args = jarvis_mcp_server_args()
        admission_class, admission_authority = resolve_pinned_mcp_admission(
            operation=McpOperation.TOOLS_LIST,
            tool=None,
            expected_server_artifact_digest=None,
            pinned_control_query=False,
            timeout_seconds=MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
        )
        assert admission_authority is not None
        submitted = queue.submit_job(
            RelayJob(
                cluster=cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=server,
                    server_args=server_args,
                    env_from=jarvis_mcp_env_from(),
                    expected_jarvis_cd_lock_binding=jarvis_cd_lock_binding_expectation(),
                    admission_class=admission_class,
                    operation=McpOperation.TOOLS_LIST,
                    timeout_seconds=MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
                ),
                idempotency_key=idempotency_key,
                metadata={
                    MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(
                        mode="json"
                    )
                },
            )
        )
        job_id = submitted.job_id
        terminal_job = wait_for_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _require_discovery_success(terminal_job.model_dump(mode="json"), job_id)
        artifacts = _complete_local_artifact_records(queue, job_id)
        artifact_payload = _read_local_artifact_kind_bytes(
            queue,
            artifacts,
            kind="mcp_result",
        )
    if artifact_payload is None:
        raise RelayError("JARVIS MCP discovery did not produce an mcp_result artifact")
    result = _decode_json_artifact(artifact_payload, kind="mcp_result")
    return job_id, result, artifacts, artifact_payload


def _persist_jarvis_remote_contract_discovery(
    *,
    cluster: str,
    discovery_job_id: str,
    result: dict[str, Any],
    artifacts: list[dict[str, Any]],
    artifact_payload: bytes,
) -> tuple[RemoteMcpSchemaCacheEntry, str]:
    """Persist and verify the exact discovery identity used by built-in JARVIS calls."""
    durable_result = _decode_json_artifact(artifact_payload, kind="mcp_result")
    if durable_result != result:
        raise RelayError(
            "JARVIS MCP discovery result did not match its durable mcp_result artifact"
        )
    result = durable_result
    expected_jarvis_cd_lock_binding = jarvis_cd_lock_binding_expectation()
    if result.get("expected_jarvis_cd_lock_binding") != expected_jarvis_cd_lock_binding:
        raise RelayError("JARVIS MCP discovery did not enforce the relay JARVIS-CD lock pin")
    server = result.get("server")
    raw_server_args = result.get("server_args")
    raw_env_from = result.get("env_from", {})
    if not isinstance(server, str) or not server:
        raise RelayError("JARVIS MCP discovery result has no server command")
    if not isinstance(raw_server_args, list) or not all(
        isinstance(item, str) for item in cast(list[object], raw_server_args)
    ):
        raise RelayError("JARVIS MCP discovery result has invalid server arguments")
    if not isinstance(raw_env_from, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in cast(dict[object, object], raw_env_from).items()
    ):
        raise RelayError("JARVIS MCP discovery result has invalid environment references")
    artifact = _artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError("JARVIS MCP discovery has no durable result artifact")
    artifact_id = artifact.get("artifact_id")
    artifact_sha256 = artifact.get("sha256")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("JARVIS MCP discovery result artifact has no artifact_id")
    if artifact_sha256 is not None and not isinstance(artifact_sha256, str):
        raise RelayError("JARVIS MCP discovery result artifact has invalid SHA-256")
    registration = RemoteMcpServerConfig(
        command=server,
        args=cast(list[str], raw_server_args),
        env_from=cast(dict[str, str], raw_env_from),
        allow_tools=[
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_add_step",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        ],
        profiles=["user"],
    )
    entry = cache_entry_from_discovery_artifact(
        cluster=cluster,
        server_name=JARVIS_MCP_CACHE_SERVER_NAME,
        registration=registration,
        discovery_job_id=discovery_job_id,
        artifact_id=artifact_id,
        artifact_sha256=artifact_sha256,
        artifact_payload=artifact_payload,
    )
    run_tool = next((tool for tool in entry.tools if tool.name == "jarvis_run"), None)
    if run_tool is None:
        raise RelayError("JARVIS MCP discovery contract omitted jarvis_run")
    try:
        require_handle_first_jarvis_run_schema(run_tool.input_schema)
    except ValueError as exc:
        raise RelayError(str(exc)) from exc
    if entry.schema_digest != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise RelayError(
            f"JARVIS MCP discovery contract does not match clio-kit {CLIO_KIT_JARVIS_MCP_VERSION}"
        )
    try:
        binding = jarvis_mcp_artifact_binding_from_entry(entry)
    except ValueError as exc:
        raise RelayError(str(exc)) from exc
    cache_path = default_remote_mcp_cache_path(registry_path=default_registry_path())
    RemoteMcpSchemaCache.update_entry(cache_path, entry)
    return entry, binding


def _read_remote_mcp_result_artifact(
    definition: ClusterDefinition,
    job_id: str,
) -> tuple[dict[str, object], bytes]:
    artifacts = _remote_artifact_records(definition, job_id)
    artifact = _artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError(f"remote MCP discovery job has no mcp_result artifact: {job_id}")
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("remote MCP result artifact has no artifact_id")
    envelope = _json_output(
        run_remote_clio(definition, ["job", "read-artifact", artifact_id]),
        "remote discovery artifact payload",
    )
    return artifact, _decode_artifact_envelope(envelope)


def _remote_artifact_records(
    definition: ClusterDefinition,
    job_id: str,
    *,
    deadline: float | None = None,
) -> list[dict[str, Any]]:
    return _complete_remote_collection(
        definition,
        ["job", "list-artifacts", job_id],
        record_key="artifacts",
        label=f"remote artifacts for {job_id}",
        deadline=deadline,
    )


def _artifact_record(
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> dict[str, Any] | None:
    matches = [artifact for artifact in artifacts if artifact.get("kind") == kind]
    if len(matches) > 1:
        raise RelayError(
            f"durable artifact authority is ambiguous: found {len(matches)} {kind} artifacts"
        )
    return matches[0] if matches else None


def _read_remote_json_artifact_kind(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
    deadline: float | None = None,
) -> dict[str, Any] | None:
    payload = _read_remote_artifact_kind_bytes(
        definition,
        artifacts,
        kind=kind,
        deadline=deadline,
    )
    return _decode_json_artifact(payload, kind=kind) if payload is not None else None


def _read_remote_artifact_kind_bytes(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
    deadline: float | None = None,
) -> bytes | None:
    """Read the exact remote artifact bytes recorded by the durable queue."""
    artifact = _artifact_record(artifacts, kind=kind)
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError(f"remote {kind} artifact has no artifact_id")
    envelope = _json_output(
        _run_remote_clio_before_deadline(
            definition,
            ["job", "read-artifact", artifact_id],
            deadline=deadline,
        ),
        f"remote {kind} artifact payload",
    )
    return _decode_artifact_envelope(envelope)


def _read_local_json_artifact_kind(
    queue: ClioCoreQueue,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> dict[str, Any] | None:
    payload = _read_local_artifact_kind_bytes(queue, artifacts, kind=kind)
    return _decode_json_artifact(payload, kind=kind) if payload is not None else None


def _read_local_artifact_kind_bytes(
    queue: ClioCoreQueue,
    artifacts: list[dict[str, Any]],
    *,
    kind: str,
) -> bytes | None:
    """Read the exact local artifact bytes recorded by the durable queue."""
    artifact = _artifact_record(artifacts, kind=kind)
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError(f"local {kind} artifact has no artifact_id")
    envelope = read_artifact_bytes(queue, artifact_id)
    return _decode_artifact_envelope(envelope)


def _decode_json_artifact(payload: bytes, *, kind: str) -> dict[str, Any]:
    try:
        decoded = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, JSONDecodeError) as exc:
        raise RelayError(f"{kind} artifact must contain UTF-8 JSON") from exc
    if not isinstance(decoded, dict):
        raise RelayError(f"{kind} artifact must contain a JSON object")
    typed = cast(dict[object, object], decoded)
    return {str(key): value for key, value in typed.items()}


def _mcp_response_job_id(response: dict[str, Any] | None) -> str:
    if response is None:
        raise RelayError("virtual remote MCP call returned no JSON-RPC response")
    error = response.get("error")
    if isinstance(error, dict):
        typed_error = cast(dict[object, object], error)
        raise RelayError(f"virtual remote MCP call failed: {typed_error.get('message')}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RelayError("virtual remote MCP call returned no result object")
    structured = cast(dict[object, object], result).get("structuredContent")
    if not isinstance(structured, dict):
        raise RelayError("virtual remote MCP call returned no structuredContent")
    job_id = cast(dict[object, object], structured).get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise RelayError("virtual remote MCP call returned no durable job_id")
    return job_id


def _read_local_mcp_result_artifact(
    queue: ClioCoreQueue,
    job_id: str,
) -> tuple[dict[str, object], bytes]:
    artifacts = _complete_local_artifact_records(queue, job_id)
    artifact = _artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError(f"remote MCP discovery job has no mcp_result artifact: {job_id}")
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("local MCP result artifact has no artifact_id")
    envelope = read_artifact_bytes(queue, artifact_id)
    return cast(dict[str, object], artifact), _decode_artifact_envelope(envelope)


def _decode_artifact_envelope(envelope: dict[str, object]) -> bytes:
    if envelope.get("encoding") != "base64":
        raise RelayError("remote MCP result artifact must use base64 encoding")
    encoded = envelope.get("data")
    if not isinstance(encoded, str):
        raise RelayError("remote MCP result artifact data must be a base64 string")
    try:
        return base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RelayError("remote MCP result artifact contains invalid base64") from exc


@dataclass(frozen=True)
class _JarvisPackageSearchAcceptance:
    """Durable evidence from one bounded JARVIS package-discovery query."""

    tools_list_response: dict[str, Any]
    call_response: dict[str, Any]
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None
    initialize_response: dict[str, Any]
    stdio_evidence: dict[str, Any]


def _run_jarvis_package_search_query(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    profile: str,
    query: str,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> _JarvisPackageSearchAcceptance:
    """Exercise bounded package discovery through the local virtual MCP surface."""
    session = run_packaged_mcp_stdio_session(
        profile=profile,
        tool="jarvis_describe",
        arguments={
            "cluster": cluster,
            "target": "package_search",
            "query": query,
            "page_size": 5,
        },
    )
    call_job_id = _mcp_response_job_id(session.tools_call_response)
    if should_execute_on_cluster(definition):
        call_status = _wait_for_remote_job_terminal(
            definition,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = _remote_artifact_records(definition, call_job_id)
        mcp_result = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
        )
        provenance = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
        )
    else:
        call_status = _wait_for_local_job_terminal(
            queue,
            call_job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = _complete_local_artifact_records(queue, call_job_id)
        mcp_result = _read_local_json_artifact_kind(queue, artifacts, kind="mcp_result")
        provenance = _read_local_json_artifact_kind(queue, artifacts, kind="provenance")
    return _JarvisPackageSearchAcceptance(
        tools_list_response=session.tools_list_response,
        call_response=session.tools_call_response,
        call_job_id=call_job_id,
        call_status=cast(dict[str, Any], call_status),
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
        initialize_response=session.initialize_response,
        stdio_evidence=session.evidence(),
    )


@dataclass(frozen=True)
class _JarvisExecutionQueryAcceptance:
    """Durable evidence from one post-run unified JARVIS execution query."""

    cluster: str
    pipeline_id: str
    execution_id: str
    outcome: Literal["terminal", "observation_unknown", "terminal_artifacts_pending"]
    tools_list_response: dict[str, Any]
    call_response: dict[str, Any]
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None
    initialize_response: dict[str, Any]
    stdio_evidence: dict[str, Any]
    lifecycle_observations: list[dict[str, Any]]
    scheduler_action: Literal["none"] = "none"
    relay_action: Literal["none"] = "none"

    def retry_selector(self) -> dict[str, object]:
        """Return the exact execution identity for a later query-only observation."""
        if not self.lifecycle_observations:
            raise RelayError("JARVIS execution observation omitted durable lifecycle evidence")
        latest = self.lifecycle_observations[-1]
        handle = latest.get("execution_handle")
        if not isinstance(handle, dict):
            raise RelayError("JARVIS execution observation omitted its durable handle")
        typed_handle = cast(dict[str, object], handle)
        scheduler_cluster = typed_handle.get("cluster")
        if scheduler_cluster is not None and (
            not isinstance(scheduler_cluster, str) or not scheduler_cluster
        ):
            raise RelayError("JARVIS execution observation returned an invalid scheduler cluster")
        return {
            "cluster": self.cluster,
            "scheduler_cluster": scheduler_cluster,
            "pipeline_id": self.pipeline_id,
            "execution_id": self.execution_id,
            "scheduler_provider": typed_handle.get("scheduler_provider"),
            "scheduler_native_id": typed_handle.get("scheduler_native_id"),
            "last_query_job_id": self.call_job_id,
        }


@dataclass(frozen=True)
class _JarvisExecutionQueryPending:
    """Exact query-only resume identity before the first execution snapshot arrives."""

    cluster: str
    pipeline_id: str
    execution_id: str
    selector: dict[str, object]
    outcome: Literal["observation_pending"] = "observation_pending"
    lifecycle_observations: tuple[()] = ()
    scheduler_action: Literal["none"] = "none"
    relay_action: Literal["retain"] = "retain"

    def retry_selector(self) -> dict[str, object]:
        """Return the exact execution identity without inventing query evidence."""
        if (
            self.selector.get("cluster") != self.cluster
            or self.selector.get("pipeline_id") != self.pipeline_id
            or self.selector.get("execution_id") != self.execution_id
            or self.selector.get("last_query_job_id") is not None
        ):
            raise RelayError("unobserved JARVIS execution selector is inconsistent")
        return dict(self.selector)


@dataclass(frozen=True)
class _JarvisExecutionQueryAttempt:
    """One durable ``jarvis_get_execution`` query and its local transport evidence."""

    session: PackagedMcpStdioSession
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None


_JARVIS_NONTERMINAL_VALIDATION_CHECKS = frozenset(
    {
        "remote-mcp.jarvis-live-progress",
        "remote-mcp.jarvis-execution-query",
    }
)
_JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA_V1 = "clio-relay.jarvis-mcp-validation-resume.v1"
_JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA = "clio-relay.jarvis-mcp-validation-resume.v2"
_JARVIS_VALIDATION_PHASE_INTENT = "jarvis_run_intent"
_JARVIS_VALIDATION_PHASE_DISPATCH = "jarvis_run_dispatch"
_JARVIS_VALIDATION_PHASE_QUERY = "execution_query"


def _canonical_jarvis_validation_digest(value: object) -> str:
    """Hash one finite JSON value using the checkpoint's canonical encoding."""
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("JARVIS validation checkpoint evidence must be finite JSON") from exc
    return hashlib.sha256(payload).hexdigest()


def _new_jarvis_validation_idempotency_key(
    *,
    cluster: str,
    profile: str,
    arguments: dict[str, Any],
) -> str:
    """Create a run-specific stable key before crossing the stdio dispatch boundary."""
    intent_digest = _canonical_jarvis_validation_digest(
        {
            "cluster": cluster,
            "profile": profile,
            "tool": "jarvis_run",
            "arguments": arguments,
        }
    )
    return f"validation:jarvis-run:{cluster}:{intent_digest}:{uuid4().hex}"


def _jarvis_run_execution_intent(
    *,
    cluster: str,
    profile: str,
    arguments: dict[str, Any],
    idempotency_key: str,
) -> dict[str, object]:
    """Return the exact replayable virtual-tool request, including relay idempotency."""
    return {
        "cluster": cluster,
        "profile": profile,
        "tool": "jarvis_run",
        "arguments": {
            "cluster": cluster,
            **arguments,
            "idempotency_key": idempotency_key,
        },
    }


def _new_jarvis_intent_resume_checkpoint(
    *,
    execution_intent: dict[str, object],
    pre_dispatch_inputs: dict[str, Any],
) -> dict[str, Any]:
    """Persist an idempotent intent before a relay receipt is observable."""
    cluster = cast(str, execution_intent["cluster"])
    profile = cast(str, execution_intent["profile"])
    arguments = cast(dict[str, object], execution_intent["arguments"])
    idempotency_key = cast(str, arguments["idempotency_key"])
    pipeline_id = cast(str, arguments["pipeline_id"])
    selector: dict[str, object] = {
        "cluster": cluster,
        "pipeline_id": pipeline_id,
        "relay_job_id": None,
        "idempotency_key": idempotency_key,
        "idempotency_key_sha256": hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest(),
        "execution_intent_sha256": _canonical_jarvis_validation_digest(execution_intent),
        "pre_dispatch_inputs_sha256": _canonical_jarvis_validation_digest(pre_dispatch_inputs),
        "call_response_sha256": None,
        "dispatch_evidence_sha256": None,
    }
    return {
        "schema_version": _JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,
        "phase": _JARVIS_VALIDATION_PHASE_INTENT,
        "profile": profile,
        "retry_selector": selector,
        "execution_intent": execution_intent,
        "pre_dispatch_inputs": pre_dispatch_inputs,
    }


def _promote_jarvis_intent_to_dispatch_checkpoint(
    intent_checkpoint: dict[str, Any],
    *,
    job_id: str,
    builder_inputs: dict[str, Any],
) -> dict[str, Any]:
    """Bind a pre-dispatch intent to the one durable relay receipt it returned."""
    checkpoint = dict(intent_checkpoint)
    selector = dict(cast(dict[str, object], checkpoint["retry_selector"]))
    call_response = builder_inputs.get("call_response")
    typed_call_response = (
        cast(dict[str, Any], call_response) if isinstance(call_response, dict) else None
    )
    if typed_call_response is None or _mcp_response_job_id(typed_call_response) != job_id:
        raise RelayError("JARVIS validation dispatch response changed its relay job identity")
    selector.update(
        {
            "relay_job_id": job_id,
            "call_response_sha256": _canonical_jarvis_validation_digest(typed_call_response),
            "dispatch_evidence_sha256": _canonical_jarvis_validation_digest(builder_inputs),
        }
    )
    checkpoint.update(
        {
            "phase": _JARVIS_VALIDATION_PHASE_DISPATCH,
            "retry_selector": selector,
            "builder_inputs": builder_inputs,
        }
    )
    return checkpoint


def _new_jarvis_intent_pending_report(
    checkpoint: dict[str, Any],
) -> LiveValidationReport:
    """Represent an ambiguous stdio response as replayable intent, never workload failure."""
    selector = cast(dict[str, object], checkpoint["retry_selector"])
    inputs = cast(dict[str, object], checkpoint["pre_dispatch_inputs"])
    report = new_live_validation_report(
        scenario="remote-mcp",
        cluster=cast(str, selector["cluster"]),
        launcher=cast(str | None, inputs.get("launcher")),
        install_source=cast(str | None, inputs.get("install_source")),
        artifact_sha256=cast(str | None, inputs.get("artifact_sha256")),
    )
    now = datetime.now(UTC)
    report.completed_at = now
    report.status = ValidationStatus.PENDING
    report.checks = [
        ValidationCheck(
            check_id="remote-mcp.jarvis-run-intent",
            summary="idempotent jarvis_run dispatch response remains observable",
            status=ValidationStatus.PENDING,
            started_at=report.started_at,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="jarvis_run_intent_resume_selector",
                    excerpt=json.dumps(selector, sort_keys=True),
                    metadata={
                        **selector,
                        "scheduler_action": "none",
                        "relay_action": "replay_same_idempotency_key",
                    },
                )
            ],
        )
    ]
    report.resources = [
        ValidationResource(
            kind="jarvis_dispatch_intent",
            resource_id=cast(str, selector["execution_intent_sha256"]),
            role="resumable_jarvis_run_intent",
            cluster=cast(str, selector["cluster"]),
            state="response_unobserved",
            metadata={
                "retry_selector": selector,
                "outcome": "observation_pending",
                "scheduler_action": "none",
                "relay_action": "replay_same_idempotency_key",
                "resume_checkpoint": checkpoint,
            },
        )
    ]
    return report


def _convert_jarvis_checks_to_pending(
    report: LiveValidationReport,
    *,
    pending_check_ids: frozenset[str],
    resource: ValidationResource,
) -> LiveValidationReport:
    """Downgrade only checks whose evidence is unavailable within this observation window."""
    failed_ids = {
        check.check_id for check in report.checks if check.status is ValidationStatus.FAILED
    }
    if not failed_ids or failed_ids - pending_check_ids:
        return report
    updated_checks = [
        check.model_copy(update={"status": ValidationStatus.PENDING, "error": None})
        if check.status is ValidationStatus.FAILED and check.check_id in pending_check_ids
        else check
        for check in report.checks
    ]
    return report.model_copy(
        update={
            "status": ValidationStatus.PENDING,
            "error": None,
            "checks": updated_checks,
            "resources": [*report.resources, resource],
        }
    )


def _build_jarvis_dispatch_pending_report(
    checkpoint: dict[str, Any],
) -> LiveValidationReport:
    """Retain one accepted relay job while its terminal result remains unobserved."""
    builder_inputs = cast(dict[str, Any], checkpoint["builder_inputs"])
    selector = cast(dict[str, object], checkpoint["retry_selector"])
    report = build_jarvis_mcp_validation_report(
        **builder_inputs,
        query_tools_list_response=None,
        query_call_response=None,
        query_call_job_id="",
        query_call_status={},
        query_artifacts=[],
        query_mcp_result=None,
        query_provenance=None,
        query_initialize_response=None,
        query_stdio_evidence=None,
        query_lifecycle_observations=[],
    )
    resource = ValidationResource(
        kind="relay_job",
        resource_id=cast(str, selector["relay_job_id"]),
        role="resumable_jarvis_run_dispatch",
        cluster=cast(str, selector["cluster"]),
        state="observation_pending",
        metadata={
            "retry_selector": selector,
            "outcome": "observation_pending",
            "scheduler_action": "none",
            "relay_action": "retain",
            "resume_checkpoint": checkpoint,
        },
    )
    return _convert_jarvis_checks_to_pending(
        report,
        pending_check_ids=frozenset(
            {
                "remote-mcp.jarvis-call",
                "remote-mcp.server-artifact",
                "remote-mcp.durable-result",
                "remote-mcp.jarvis-live-progress",
                "jarvis.spack-runtime-environment",
                "jarvis.structured-runtime-metadata",
                "remote-mcp.jarvis-execution-query",
            }
        ),
        resource=resource,
    )


def _build_unobserved_jarvis_query_pending_report(
    *,
    builder_inputs: dict[str, Any],
    execution_query: _JarvisExecutionQueryPending,
    checkpoint: dict[str, Any],
) -> LiveValidationReport:
    """Retain exact execution identity when no query result arrives in the window."""
    selector = execution_query.retry_selector()
    report = build_jarvis_mcp_validation_report(
        **builder_inputs,
        query_tools_list_response=None,
        query_call_response=None,
        query_call_job_id="",
        query_call_status={},
        query_artifacts=[],
        query_mcp_result=None,
        query_provenance=None,
        query_initialize_response=None,
        query_stdio_evidence=None,
        query_lifecycle_observations=[],
    )
    provider = selector.get("scheduler_provider")
    resource = ValidationResource(
        kind="jarvis_execution",
        resource_id=execution_query.execution_id,
        role="resumable_acceptance_workload",
        cluster=execution_query.cluster,
        provider=provider if isinstance(provider, str) else None,
        state="observation_pending",
        metadata={
            "retry_selector": selector,
            "outcome": execution_query.outcome,
            "scheduler_action": execution_query.scheduler_action,
            "relay_action": execution_query.relay_action,
            "resume_checkpoint": checkpoint,
        },
    )
    return _convert_jarvis_checks_to_pending(
        report,
        pending_check_ids=_JARVIS_NONTERMINAL_VALIDATION_CHECKS,
        resource=resource,
    )


def _mark_jarvis_validation_pending(
    report: LiveValidationReport,
    *,
    execution_query: _JarvisExecutionQueryAcceptance,
    resume_checkpoint: dict[str, Any] | None = None,
) -> LiveValidationReport:
    """Convert only terminal-dependent failures into honest resumable evidence."""
    failed_check_ids = {
        check.check_id for check in report.checks if check.status is ValidationStatus.FAILED
    }
    unexpected_failures = failed_check_ids - _JARVIS_NONTERMINAL_VALIDATION_CHECKS
    if unexpected_failures or not _jarvis_nonterminal_failures_are_resumable(
        report,
        execution_query=execution_query,
    ):
        return report
    selector = execution_query.retry_selector()
    latest = execution_query.lifecycle_observations[-1]
    updated_checks = [
        check.model_copy(
            update={
                "status": ValidationStatus.PENDING,
                "error": None,
                "evidence": [
                    *check.evidence,
                    EvidenceReference(
                        kind="jarvis_execution_resume_selector",
                        excerpt=json.dumps(selector, sort_keys=True),
                        metadata={
                            **selector,
                            "scheduler_action": execution_query.scheduler_action,
                            "relay_action": execution_query.relay_action,
                        },
                    ),
                ],
            }
        )
        if check.check_id in _JARVIS_NONTERMINAL_VALIDATION_CHECKS
        and check.status is ValidationStatus.FAILED
        else check
        for check in report.checks
    ]
    provider = selector.get("scheduler_provider")
    resource = ValidationResource(
        kind="jarvis_execution",
        resource_id=execution_query.execution_id,
        role="resumable_acceptance_workload",
        cluster=execution_query.cluster,
        provider=provider if isinstance(provider, str) else None,
        state=str(latest.get("state")) if latest.get("state") is not None else None,
        metadata={
            "retry_selector": selector,
            "outcome": execution_query.outcome,
            "scheduler_action": execution_query.scheduler_action,
            "relay_action": execution_query.relay_action,
            **({"resume_checkpoint": resume_checkpoint} if resume_checkpoint is not None else {}),
        },
    )
    return report.model_copy(
        update={
            "status": ValidationStatus.PENDING,
            "error": None,
            "checks": updated_checks,
            "resources": [*report.resources, resource],
        }
    )


def _jarvis_nonterminal_failures_are_resumable(
    report: LiveValidationReport,
    *,
    execution_query: _JarvisExecutionQueryAcceptance,
) -> bool:
    """Require all nonterminal integrity assertions before downgrading terminal checks."""
    required_assertions = {
        "remote-mcp.jarvis-live-progress": {
            "observation_count_bounded",
            "query_identities_coherent",
            "scheduler_identity_optional_coherent_and_stable",
            "lifecycle_prefix_coherent",
            "package_progress_nonregressing",
        },
        "remote-mcp.jarvis-execution-query": {
            "local_query_surface_verified",
            "server_artifact_binding_verified",
            "resumable_query_job_verified",
            "resumable_result_transport_verified",
            "resumable_result_envelope_verified",
            "resumable_identity_coherent",
            "resumable_lifecycle_coherent",
            "resumable_runner_semantic_validation_verified",
        },
    }
    if not execution_query.lifecycle_observations:
        return False
    latest = execution_query.lifecycle_observations[-1]
    terminal = latest.get("terminal")
    if execution_query.outcome == "observation_unknown" and terminal is not False:
        return False
    if execution_query.outcome == "terminal_artifacts_pending" and terminal is not True:
        return False
    for check in report.checks:
        required = required_assertions.get(check.check_id)
        if check.status is not ValidationStatus.FAILED or required is None:
            continue
        if len(check.evidence) != 1:
            return False
        assertions = check.evidence[0].metadata.get("assertions")
        if not isinstance(assertions, dict) or not all(
            cast(dict[str, object], assertions).get(name) is True for name in required
        ):
            return False
    return True


def _load_jarvis_validation_resume_checkpoint(
    path: Path,
    *,
    cluster: str,
) -> dict[str, Any]:
    """Load one exact pending acceptance checkpoint without trusting caller selectors."""
    report = load_validation_report(path)
    if report.scenario != "remote-mcp" or report.cluster != cluster:
        raise ConfigurationError(
            "JARVIS validation resume report does not match the requested cluster/scenario"
        )
    if report.status is not ValidationStatus.PENDING:
        raise ConfigurationError("JARVIS validation resume requires a pending report")
    candidates = [
        (resource, resource.metadata.get("resume_checkpoint"))
        for resource in report.resources
        if isinstance(resource.metadata.get("resume_checkpoint"), dict)
        and (
            (
                resource.kind == "jarvis_execution"
                and resource.role == "resumable_acceptance_workload"
            )
            or (resource.kind == "relay_job" and resource.role == "resumable_jarvis_run_dispatch")
            or (
                resource.kind == "jarvis_dispatch_intent"
                and resource.role == "resumable_jarvis_run_intent"
            )
        )
    ]
    if len(candidates) != 1:
        raise ConfigurationError(
            "pending JARVIS validation report must contain one resume checkpoint"
        )
    resource, raw_checkpoint = candidates[0]
    checkpoint = cast(dict[str, Any], raw_checkpoint)
    schema_version = checkpoint.get("schema_version")
    if schema_version == _JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA_V1:
        phase = _JARVIS_VALIDATION_PHASE_QUERY
    elif schema_version == _JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA:
        phase = checkpoint.get("phase")
    else:
        raise ConfigurationError("pending JARVIS validation resume checkpoint is invalid")
    if phase in {_JARVIS_VALIDATION_PHASE_INTENT, _JARVIS_VALIDATION_PHASE_DISPATCH}:
        return _validate_jarvis_dispatch_resume_checkpoint(
            checkpoint,
            resource=resource,
            cluster=cluster,
        )
    if phase != _JARVIS_VALIDATION_PHASE_QUERY:
        raise ConfigurationError("pending JARVIS validation resume checkpoint phase is invalid")
    selector = checkpoint.get("retry_selector")
    builder_inputs = checkpoint.get("builder_inputs")
    observations = checkpoint.get("lifecycle_observations")
    profile = checkpoint.get("profile")
    unobserved = (
        schema_version == _JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA
        and checkpoint.get("observation_state") == "not_observed"
    )
    if (
        not isinstance(selector, dict)
        or resource.kind != "jarvis_execution"
        or resource.role != "resumable_acceptance_workload"
        or cast(dict[str, object], selector).get("cluster") != cluster
        or not isinstance(cast(dict[str, object], selector).get("pipeline_id"), str)
        or not cast(str, cast(dict[str, object], selector).get("pipeline_id"))
        or not isinstance(cast(dict[str, object], selector).get("execution_id"), str)
        or not cast(str, cast(dict[str, object], selector).get("execution_id"))
        or not isinstance(builder_inputs, dict)
        or cast(dict[str, object], builder_inputs).get("cluster") != cluster
        or "scheduler_cluster" not in cast(dict[str, object], builder_inputs)
        or cast(dict[str, object], builder_inputs).get("tool") != "jarvis_run"
        or not isinstance(cast(dict[str, object], builder_inputs).get("runtime_metadata"), dict)
        or not isinstance(observations, list)
        or (not observations and not unobserved)
        or (bool(cast(list[object], observations)) and unobserved)
        or len(cast(list[object], observations)) > _MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS
        or profile not in {"user", "admin", "operator", "all"}
        or (
            schema_version == _JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA
            and checkpoint.get("observation_state") not in {"not_observed", "observed"}
        )
    ):
        raise ConfigurationError("pending JARVIS validation resume checkpoint is invalid")
    typed_selector = cast(dict[str, object], selector)
    pipeline_id = cast(str, typed_selector["pipeline_id"])
    execution_id = cast(str, typed_selector["execution_id"])
    scheduler_cluster = typed_selector.get("scheduler_cluster")
    scheduler_provider = typed_selector.get("scheduler_provider")
    scheduler_native_id = typed_selector.get("scheduler_native_id")
    last_query_job_id = typed_selector.get("last_query_job_id")
    builder_scheduler_cluster = cast(dict[str, object], builder_inputs).get("scheduler_cluster")
    expected_mode = "scheduler" if scheduler_provider is not None else "direct"
    if (
        "scheduler_cluster" not in typed_selector
        or builder_scheduler_cluster != scheduler_cluster
        or (
            scheduler_cluster is not None
            and (not isinstance(scheduler_cluster, str) or not scheduler_cluster)
        )
        or (
            scheduler_provider is not None
            and (not isinstance(scheduler_provider, str) or not scheduler_provider)
        )
        or (
            scheduler_native_id is not None
            and (not isinstance(scheduler_native_id, str) or not scheduler_native_id)
        )
        or (scheduler_provider is None and scheduler_native_id is not None)
        or (
            unobserved
            and (
                last_query_job_id is not None
                or resource.state != "observation_pending"
                or resource.metadata.get("outcome") != "observation_pending"
            )
        )
        or (not unobserved and (not isinstance(last_query_job_id, str) or not last_query_job_id))
        or resource.resource_id != execution_id
        or resource.cluster != cluster
        or resource.provider != scheduler_provider
        or resource.metadata.get("retry_selector") != selector
    ):
        raise ConfigurationError("pending JARVIS validation resume identity is invalid")
    typed_observations: list[dict[str, object]] = []
    validated_prefix: list[dict[str, Any]] = []
    scheduler_native_id_assigned = False
    scheduler_cluster_assigned = False
    for raw_observation in cast(list[object], observations):
        if not isinstance(raw_observation, dict):
            raise ConfigurationError("pending JARVIS validation observation is invalid")
        observation = {
            str(key): value for key, value in cast(dict[object, object], raw_observation).items()
        }
        handle = observation.get("execution_handle")
        if not isinstance(handle, dict):
            raise ConfigurationError("pending JARVIS validation observation is invalid")
        typed_handle = cast(dict[str, object], handle)
        observation_scheduler_cluster = typed_handle.get("cluster")
        observation_native_id = typed_handle.get("scheduler_native_id")
        if (
            observation.get("pipeline_id") != pipeline_id
            or observation.get("execution_id") != execution_id
            or not isinstance(observation.get("query_job_id"), str)
            or not observation.get("query_job_id")
            or typed_handle.get("pipeline_id") != pipeline_id
            or typed_handle.get("execution_id") != execution_id
            or typed_handle.get("mode") != expected_mode
            or typed_handle.get("scheduler_provider") != scheduler_provider
        ):
            raise ConfigurationError("pending JARVIS validation observation identity changed")
        if scheduler_native_id is None:
            if observation_native_id is not None:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_native_id is None:
            if scheduler_native_id_assigned:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_native_id != scheduler_native_id:
            raise ConfigurationError("pending JARVIS validation observation identity changed")
        else:
            scheduler_native_id_assigned = True
        if scheduler_cluster is None:
            if observation_scheduler_cluster is not None:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_scheduler_cluster is None:
            if scheduler_cluster_assigned:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_scheduler_cluster != scheduler_cluster:
            raise ConfigurationError("pending JARVIS validation observation identity changed")
        else:
            scheduler_cluster_assigned = True
        if observation.get(_JARVIS_QUERY_INTEGRITY_KEY) is not None:
            raise ConfigurationError("pending JARVIS validation observation integrity failed")
        gap_marker = observation.get(_JARVIS_VERIFIED_GAP_KEY)
        crossed_verified_gap = gap_marker is not None
        if crossed_verified_gap and (
            not validated_prefix
            or not _valid_jarvis_verified_gap_marker(
                gap_marker,
                previous=validated_prefix[-1],
                current=cast(dict[str, Any], observation),
            )
        ):
            raise ConfigurationError("pending JARVIS validation observation gap is invalid")
        integrity_violation = _jarvis_query_integrity_violation(
            validated_prefix,
            cast(dict[str, Any], observation),
            crossed_verified_gap=crossed_verified_gap,
        )
        if integrity_violation is not None:
            raise ConfigurationError(
                "pending JARVIS validation observation integrity failed: "
                f"{integrity_violation['reason']}"
            )
        validated_prefix.append(cast(dict[str, Any], observation))
        typed_observations.append(observation)
    if typed_observations:
        latest = typed_observations[-1]
        latest_handle = cast(dict[str, object], latest["execution_handle"])
        if (
            latest.get("query_job_id") != last_query_job_id
            or resource.state != latest.get("state")
            or latest_handle.get("cluster") != scheduler_cluster
            or latest_handle.get("scheduler_native_id") != scheduler_native_id
        ):
            raise ConfigurationError("pending JARVIS validation latest observation changed")
    typed_runtime = cast(
        dict[str, Any],
        cast(dict[str, object], builder_inputs)["runtime_metadata"],
    )
    runtime_scheduler_job_id = typed_runtime.get("scheduler_job_id")
    runtime_details = typed_runtime.get("details")
    runtime_native_execution = (
        cast(dict[str, object], runtime_details).get("native_execution")
        if isinstance(runtime_details, dict)
        else None
    )
    if (
        typed_runtime.get("schema_version") != RUNTIME_METADATA_SCHEMA
        or typed_runtime.get("source") != "jarvis_mcp"
        or typed_runtime.get("pipeline_id") != pipeline_id
        or typed_runtime.get("execution_id") != execution_id
        or typed_runtime.get("scheduler_provider") != scheduler_provider
        or (
            runtime_scheduler_job_id is not None and runtime_scheduler_job_id != scheduler_native_id
        )
    ):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    if not isinstance(runtime_native_execution, dict):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    try:
        native_documents = native_execution_documents(
            cast(dict[str, Any], runtime_native_execution)
        )
    except (ValidationError, ValueError) as exc:
        raise ConfigurationError("pending JARVIS validation runtime identity changed") from exc
    if native_documents is None:
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    runtime_handle = native_documents.execution_handle
    runtime_record = native_documents.execution_record
    runtime_progress = native_documents.progress
    runtime_terminal = typed_runtime.get("terminal")
    if not isinstance(runtime_terminal, dict):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    typed_runtime_terminal = cast(dict[str, object], runtime_terminal)
    first_state = typed_observations[0].get("state") if typed_observations else runtime_record.state
    runtime_rank = _JARVIS_EXECUTION_STATE_RANK.get(runtime_record.state)
    first_rank = (
        _JARVIS_EXECUTION_STATE_RANK.get(first_state) if isinstance(first_state, str) else None
    )
    if (
        runtime_handle.pipeline_id != pipeline_id
        or runtime_handle.execution_id != execution_id
        or runtime_handle.mode != expected_mode
        or runtime_handle.scheduler_provider != scheduler_provider
        or runtime_handle.scheduler_native_id != runtime_scheduler_job_id
        or (
            runtime_handle.scheduler_native_id is not None
            and runtime_handle.scheduler_native_id != scheduler_native_id
        )
        or (runtime_handle.cluster is not None and runtime_handle.cluster != scheduler_cluster)
        or typed_runtime_terminal.get("state") != runtime_record.state
        or typed_runtime_terminal.get("terminal") is not runtime_record.terminal
        or typed_runtime_terminal.get("returncode") != runtime_record.return_code
        or typed_runtime_terminal.get("reason") != runtime_record.error
        or runtime_progress.pipeline_id != pipeline_id
        or runtime_progress.execution_id != execution_id
        or (runtime_rank is not None and first_rank is not None and first_rank < runtime_rank)
        or (
            typed_observations
            and runtime_record.terminal
            and (
                typed_observations[0].get("terminal") is not True
                or first_state != runtime_record.state
            )
        )
    ):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    return checkpoint


def _validate_jarvis_dispatch_resume_checkpoint(
    checkpoint: dict[str, Any],
    *,
    resource: ValidationResource,
    cluster: str,
) -> dict[str, Any]:
    """Fail closed on any change to a pre-query JARVIS dispatch identity."""
    phase = checkpoint.get("phase")
    profile = checkpoint.get("profile")
    selector = checkpoint.get("retry_selector")
    intent = checkpoint.get("execution_intent")
    pre_dispatch_inputs = checkpoint.get("pre_dispatch_inputs")
    if (
        checkpoint.get("schema_version") != _JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA
        or phase not in {_JARVIS_VALIDATION_PHASE_INTENT, _JARVIS_VALIDATION_PHASE_DISPATCH}
        or profile not in {"user", "admin", "operator", "all"}
        or not isinstance(selector, dict)
        or not isinstance(intent, dict)
        or not isinstance(pre_dispatch_inputs, dict)
    ):
        raise ConfigurationError("pending JARVIS dispatch checkpoint is invalid")
    typed_selector = cast(dict[str, object], selector)
    typed_intent = cast(dict[str, object], intent)
    typed_pre_dispatch_inputs = cast(dict[str, Any], pre_dispatch_inputs)
    raw_arguments = typed_intent.get("arguments")
    if not isinstance(raw_arguments, dict):
        raise ConfigurationError("pending JARVIS dispatch intent is invalid")
    arguments = cast(dict[str, object], raw_arguments)
    idempotency_key = arguments.get("idempotency_key")
    pipeline_id = arguments.get("pipeline_id")
    expected_selector_fields = {
        "cluster",
        "pipeline_id",
        "relay_job_id",
        "idempotency_key",
        "idempotency_key_sha256",
        "execution_intent_sha256",
        "pre_dispatch_inputs_sha256",
        "call_response_sha256",
        "dispatch_evidence_sha256",
    }
    if (
        set(typed_selector) != expected_selector_fields
        or typed_intent.get("cluster") != cluster
        or typed_intent.get("profile") != profile
        or typed_intent.get("tool") != "jarvis_run"
        or arguments.get("cluster") != cluster
        or not isinstance(pipeline_id, str)
        or not pipeline_id
        or not isinstance(idempotency_key, str)
        or not idempotency_key
        or len(idempotency_key) > 512
        or typed_selector.get("cluster") != cluster
        or typed_selector.get("pipeline_id") != pipeline_id
        or typed_selector.get("idempotency_key") != idempotency_key
        or typed_selector.get("execution_intent_sha256")
        != _canonical_jarvis_validation_digest(typed_intent)
        or typed_selector.get("pre_dispatch_inputs_sha256")
        != _canonical_jarvis_validation_digest(typed_pre_dispatch_inputs)
        or typed_selector.get("idempotency_key_sha256")
        != hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        or typed_pre_dispatch_inputs.get("cluster") != cluster
        or typed_pre_dispatch_inputs.get("tool") != "jarvis_run"
        or not isinstance(typed_pre_dispatch_inputs.get("package_search_query"), str)
        or not typed_pre_dispatch_inputs.get("package_search_query")
    ):
        raise ConfigurationError("pending JARVIS dispatch identity changed")
    if (
        resource.cluster != cluster
        or resource.provider is not None
        or resource.metadata.get("retry_selector") != selector
        or resource.metadata.get("resume_checkpoint") != checkpoint
        or resource.metadata.get("outcome") != "observation_pending"
        or resource.metadata.get("scheduler_action") != "none"
    ):
        raise ConfigurationError("pending JARVIS dispatch resource identity changed")
    relay_job_id = typed_selector.get("relay_job_id")
    if phase == _JARVIS_VALIDATION_PHASE_INTENT:
        if (
            relay_job_id is not None
            or typed_selector.get("call_response_sha256") is not None
            or typed_selector.get("dispatch_evidence_sha256") is not None
            or "builder_inputs" in checkpoint
            or resource.kind != "jarvis_dispatch_intent"
            or resource.role != "resumable_jarvis_run_intent"
            or resource.resource_id != typed_selector.get("execution_intent_sha256")
            or resource.state != "response_unobserved"
            or resource.metadata.get("relay_action") != "replay_same_idempotency_key"
        ):
            raise ConfigurationError("pending JARVIS dispatch intent changed")
        return checkpoint
    builder_inputs = checkpoint.get("builder_inputs")
    if (
        not isinstance(builder_inputs, dict)
        or not isinstance(relay_job_id, str)
        or not relay_job_id
    ):
        raise ConfigurationError("pending JARVIS relay dispatch checkpoint is invalid")
    typed_builder = cast(dict[str, Any], builder_inputs)
    call_response = typed_builder.get("call_response")
    typed_call_response = (
        cast(dict[str, Any], call_response) if isinstance(call_response, dict) else None
    )
    try:
        response_job_id = (
            _mcp_response_job_id(typed_call_response) if typed_call_response is not None else None
        )
    except RelayError as exc:
        raise ConfigurationError("pending JARVIS relay dispatch response is invalid") from exc
    if (
        response_job_id != relay_job_id
        or typed_builder.get("cluster") != cluster
        or typed_builder.get("tool") != "jarvis_run"
        or typed_builder.get("call_job_id") != relay_job_id
        or typed_builder.get("scheduler_cluster") is not None
        or typed_builder.get("call_status") != {}
        or typed_builder.get("artifacts") != []
        or typed_builder.get("mcp_result") is not None
        or typed_builder.get("provenance") is not None
        or typed_builder.get("runtime_metadata") is not None
        or typed_builder.get("progress") != []
        or typed_builder.get("live_progress_observation") is not None
        or any(typed_builder.get(key) != value for key, value in typed_pre_dispatch_inputs.items())
        or typed_selector.get("call_response_sha256")
        != _canonical_jarvis_validation_digest(typed_call_response)
        or typed_selector.get("dispatch_evidence_sha256")
        != _canonical_jarvis_validation_digest(typed_builder)
        or resource.kind != "relay_job"
        or resource.role != "resumable_jarvis_run_dispatch"
        or resource.resource_id != relay_job_id
        or resource.state != "observation_pending"
        or resource.metadata.get("relay_action") != "retain"
    ):
        raise ConfigurationError("pending JARVIS relay dispatch identity changed")
    return checkpoint


def _require_same_jarvis_resume_identity(
    *,
    expected: dict[str, Any],
    observed: dict[str, object],
) -> None:
    """Reject a resume snapshot whose durable workload identity changed."""
    for field in ("cluster", "pipeline_id", "execution_id", "scheduler_provider"):
        if observed.get(field) != expected.get(field):
            raise RelayError(f"JARVIS validation resume changed {field}")
    expected_native_id = expected.get("scheduler_native_id")
    observed_native_id = observed.get("scheduler_native_id")
    if expected_native_id is not None and observed_native_id != expected_native_id:
        raise RelayError("JARVIS validation resume changed scheduler_native_id")
    if observed_native_id is not None and (
        not isinstance(observed_native_id, str) or not observed_native_id
    ):
        raise RelayError("JARVIS validation resume returned an invalid scheduler_native_id")
    expected_scheduler_cluster = expected.get("scheduler_cluster")
    observed_scheduler_cluster = observed.get("scheduler_cluster")
    if (
        expected_scheduler_cluster is not None
        and observed_scheduler_cluster != expected_scheduler_cluster
    ):
        raise RelayError("JARVIS validation resume changed scheduler_cluster")
    if observed_scheduler_cluster is not None and (
        not isinstance(observed_scheduler_cluster, str) or not observed_scheduler_cluster
    ):
        raise RelayError("JARVIS validation resume returned an invalid scheduler_cluster")


def _jarvis_execution_retry_selector_from_runtime_metadata(
    runtime_metadata: dict[str, Any],
    *,
    cluster: str,
    pipeline_id: str,
    execution_id: str,
) -> dict[str, object]:
    """Bind a query-only selector to JARVIS's structured native execution authority."""
    details = runtime_metadata.get("details")
    native_execution = (
        cast(dict[str, object], details).get("native_execution")
        if isinstance(details, dict)
        else None
    )
    if (
        runtime_metadata.get("schema_version") != RUNTIME_METADATA_SCHEMA
        or runtime_metadata.get("source") != "jarvis_mcp"
        or runtime_metadata.get("pipeline_id") != pipeline_id
        or runtime_metadata.get("execution_id") != execution_id
        or not isinstance(native_execution, dict)
    ):
        raise RelayError("JARVIS run metadata omitted its structured execution identity")
    try:
        documents = native_execution_documents(cast(dict[str, Any], native_execution))
    except (ValidationError, ValueError) as exc:
        raise RelayError(
            "JARVIS run metadata contains an invalid native execution identity"
        ) from exc
    if documents is None:
        raise RelayError("JARVIS run metadata omitted its native execution identity")
    handle = documents.execution_handle
    scheduler_provider = runtime_metadata.get("scheduler_provider")
    scheduler_native_id = runtime_metadata.get("scheduler_job_id")
    if (
        handle.pipeline_id != pipeline_id
        or handle.execution_id != execution_id
        or handle.scheduler_provider != scheduler_provider
        or handle.scheduler_native_id != scheduler_native_id
        or (scheduler_provider is None and scheduler_native_id is not None)
    ):
        raise RelayError("JARVIS run metadata contains inconsistent scheduler identity")
    return {
        "cluster": cluster,
        "scheduler_cluster": handle.cluster,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "scheduler_provider": scheduler_provider,
        "scheduler_native_id": scheduler_native_id,
        "last_query_job_id": None,
    }


def _require_jarvis_run_dispatch_job_identity(
    status: dict[str, object],
    *,
    cluster: str,
    job_id: str,
    pipeline_id: str,
    idempotency_key: str,
) -> None:
    """Verify that a resumed receipt still denotes the exact accepted jarvis_run call."""
    raw_job = status.get("job")
    job = cast(dict[str, object], raw_job) if isinstance(raw_job, dict) else {}
    raw_spec = job.get("spec")
    spec = cast(dict[str, object], raw_spec) if isinstance(raw_spec, dict) else {}
    raw_arguments = spec.get("arguments")
    arguments = cast(dict[str, object], raw_arguments) if isinstance(raw_arguments, dict) else {}
    if (
        status.get("terminal") is not True
        or job.get("job_id") != job_id
        or job.get("cluster") != cluster
        or job.get("kind") != "mcp_call"
        or job.get("idempotency_key") != idempotency_key
        or spec.get("operation") != "tools/call"
        or spec.get("tool") != "jarvis_run"
        or arguments.get("pipeline_id") != pipeline_id
    ):
        raise RelayError("resumed JARVIS relay job changed its dispatch identity")


def _complete_jarvis_run_dispatch(
    *,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    checkpoint: dict[str, Any],
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    """Wait and collect one exact relay dispatch without ever resubmitting it."""
    selector = cast(dict[str, object], checkpoint["retry_selector"])
    cluster = cast(str, selector["cluster"])
    job_id = cast(str, selector["relay_job_id"])
    pipeline_id = cast(str, selector["pipeline_id"])
    idempotency_key = cast(str, selector["idempotency_key"])
    if should_execute_on_cluster(definition):
        call_status = _wait_for_remote_job_terminal(
            definition,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _require_jarvis_run_dispatch_job_identity(
            call_status,
            cluster=cluster,
            job_id=job_id,
            pipeline_id=pipeline_id,
            idempotency_key=idempotency_key,
        )
        progress = _complete_remote_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"JARVIS MCP dispatch progress for {job_id}",
        )
        artifacts = _remote_artifact_records(definition, job_id)
        mcp_result = _read_remote_json_artifact_kind(definition, artifacts, kind="mcp_result")
        provenance = _read_remote_json_artifact_kind(definition, artifacts, kind="provenance")
        runtime_metadata = _read_remote_json_artifact_kind(
            definition, artifacts, kind="runtime_metadata"
        )
    else:
        call_status = _wait_for_local_job_terminal(
            queue,
            job_id,
            timeout_seconds=wait_timeout_seconds,
            poll_seconds=poll_seconds,
        )
        _require_jarvis_run_dispatch_job_identity(
            call_status,
            cluster=cluster,
            job_id=job_id,
            pipeline_id=pipeline_id,
            idempotency_key=idempotency_key,
        )
        progress = _complete_local_progress_records(queue, job_id)
        artifacts = _complete_local_artifact_records(queue, job_id)
        mcp_result = _read_local_json_artifact_kind(queue, artifacts, kind="mcp_result")
        provenance = _read_local_json_artifact_kind(queue, artifacts, kind="provenance")
        runtime_metadata = _read_local_json_artifact_kind(queue, artifacts, kind="runtime_metadata")
    return {
        **cast(dict[str, Any], checkpoint["builder_inputs"]),
        "call_status": call_status,
        "artifacts": artifacts,
        "mcp_result": mcp_result,
        "provenance": provenance,
        "runtime_metadata": runtime_metadata,
        "progress": progress,
        "live_progress_observation": None,
    }


def _merge_jarvis_execution_query_observations(
    prior: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge bounded query snapshots while preserving lifecycle order across retries."""
    merged: list[dict[str, Any]] = []
    for observation in [*prior, *current]:
        _append_bounded_jarvis_execution_query_observation(merged, observation)
    return merged


_MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS = 512
_JARVIS_QUERY_INTEGRITY_KEY = "relay_query_integrity"
_JARVIS_QUERY_INTEGRITY_SCHEMA = "clio-relay.jarvis-query-integrity.v1"
_JARVIS_VERIFIED_GAP_KEY = "relay_query_verified_gap"
_JARVIS_VERIFIED_GAP_SCHEMA = "clio-relay.jarvis-query-verified-gap.v1"
_JARVIS_EXECUTION_STATE_RANK = {
    "preparing": 0,
    "scripted": 1,
    "submitting": 2,
    "submitted": 3,
    "running": 4,
    "completed": 5,
    "failed": 5,
    "canceled": 5,
}
_JARVIS_PACKAGE_PROGRESS_STATES = frozenset(
    {"pending", "starting", "running", "ready", "completed", "failed", "canceled"}
)


def _append_bounded_jarvis_execution_query_observation(
    observations: list[dict[str, Any]],
    observation: dict[str, Any],
) -> None:
    """Retain ordered lifecycle evidence without failing a healthy long run."""
    prior_violation = any(
        _valid_jarvis_query_integrity_marker(item.get(_JARVIS_QUERY_INTEGRITY_KEY))
        for item in observations
    )
    incoming_marker = observation.get(_JARVIS_QUERY_INTEGRITY_KEY)
    incoming_gap = observation.get(_JARVIS_VERIFIED_GAP_KEY)
    gap_invalid = incoming_gap is not None and (
        not observations
        or not _valid_jarvis_verified_gap_marker(
            incoming_gap,
            previous=observations[-1],
            current=observation,
        )
    )
    if gap_invalid:
        observation = {
            **observation,
            _JARVIS_QUERY_INTEGRITY_KEY: _jarvis_query_integrity_summary(
                "verified_gap_invalid",
                observations[-1].get("state") if observations else None,
                observation.get("state"),
            ),
        }
    elif incoming_marker is not None and not _valid_jarvis_query_integrity_marker(incoming_marker):
        observation = {
            **observation,
            _JARVIS_QUERY_INTEGRITY_KEY: _jarvis_query_integrity_summary(
                "integrity_marker_invalid",
                observations[-1].get("state") if observations else None,
                observation.get("state"),
            ),
        }
    elif not prior_violation and incoming_marker is None:
        violation = _jarvis_query_integrity_violation(
            observations,
            observation,
            crossed_verified_gap=incoming_gap is not None,
        )
        if violation is not None:
            observation = {**observation, _JARVIS_QUERY_INTEGRITY_KEY: violation}
    observations.append(observation)
    if len(observations) <= _MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS:
        return

    protected_indexes = {0, len(observations) - 1}
    first_state_indexes: dict[tuple[object, object], int] = {}
    first_live_progress_index: int | None = None
    for index, item in enumerate(observations):
        raw_state = item.get("state")
        state = (
            raw_state
            if raw_state in {"unknown", "submitted", "running", "completed", "failed", "canceled"}
            else "invalid"
        )
        state_key = (state, item.get("terminal"))
        first_state_indexes.setdefault(state_key, index)
        if _valid_jarvis_query_integrity_marker(item.get(_JARVIS_QUERY_INTEGRITY_KEY)):
            protected_indexes.add(index)
        if first_live_progress_index is None and _has_live_jarvis_package_progress(item):
            first_live_progress_index = index
    protected_indexes.update(first_state_indexes.values())
    if first_live_progress_index is not None:
        protected_indexes.add(first_live_progress_index)

    target_size = _MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS // 2
    available_slots = max(0, target_size - len(protected_indexes))
    candidates = [index for index in range(len(observations)) if index not in protected_indexes]
    if available_slots and candidates:
        selected = {
            candidates[index * len(candidates) // available_slots]
            for index in range(available_slots)
        }
        protected_indexes.update(selected)
    selected_indexes = sorted(protected_indexes)
    compacted: list[dict[str, Any]] = []
    prior_selected_index: int | None = None
    for index in selected_indexes:
        item = observations[index]
        if prior_selected_index is not None and index > prior_selected_index + 1:
            discarded = observations[prior_selected_index + 1 : index]
            item = {
                **item,
                _JARVIS_VERIFIED_GAP_KEY: _jarvis_verified_gap_marker(
                    previous=observations[prior_selected_index],
                    current=item,
                    discarded=discarded,
                ),
            }
        compacted.append(item)
        prior_selected_index = index
    observations[:] = compacted


def _jarvis_verified_gap_marker(
    *,
    previous: dict[str, Any],
    current: dict[str, Any],
    discarded: list[dict[str, Any]],
) -> dict[str, object]:
    """Record a relay-trusted summary after checking every sampled transition."""
    nested_gap = current.get(_JARVIS_VERIFIED_GAP_KEY)
    nested_discarded_count = (
        cast(dict[str, Any], nested_gap).get("discarded_observation_count")
        if isinstance(nested_gap, dict)
        else 0
    )
    if (
        not isinstance(nested_discarded_count, int)
        or isinstance(nested_discarded_count, bool)
        or nested_discarded_count < 0
    ):
        nested_discarded_count = 0
    canonical = json.dumps(
        {"discarded": discarded, "nested_current_gap": nested_gap},
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "schema_version": _JARVIS_VERIFIED_GAP_SCHEMA,
        "verified": True,
        "discarded_observation_count": len(discarded) + nested_discarded_count,
        "discarded_observations_sha256": hashlib.sha256(canonical).hexdigest(),
        "previous_query_job_id": previous.get("query_job_id"),
        "current_query_job_id": current.get("query_job_id"),
    }


def _valid_jarvis_verified_gap_marker(
    value: object,
    *,
    previous: dict[str, Any],
    current: dict[str, Any],
) -> bool:
    """Accept one exact relay-trusted local summary bound to adjacent retained snapshots."""
    if not isinstance(value, dict):
        return False
    marker = cast(dict[str, object], value)
    digest = marker.get("discarded_observations_sha256")
    count = marker.get("discarded_observation_count")
    return bool(
        set(marker)
        == {
            "schema_version",
            "verified",
            "discarded_observation_count",
            "discarded_observations_sha256",
            "previous_query_job_id",
            "current_query_job_id",
        }
        and marker.get("schema_version") == _JARVIS_VERIFIED_GAP_SCHEMA
        and marker.get("verified") is True
        and isinstance(count, int)
        and not isinstance(count, bool)
        and count > 0
        and isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
        and marker.get("previous_query_job_id") == previous.get("query_job_id")
        and marker.get("current_query_job_id") == current.get("query_job_id")
    )


def _jarvis_query_integrity_violation(
    observations: list[dict[str, Any]],
    current: dict[str, Any],
    *,
    crossed_verified_gap: bool = False,
) -> dict[str, object] | None:
    """Return a sticky summary when compaction must not erase an integrity failure."""
    handle = current.get("execution_handle")
    record = current.get("execution_record")
    progress = current.get("progress")
    if (
        not isinstance(handle, dict)
        or not isinstance(record, dict)
        or not isinstance(progress, dict)
    ):
        return _jarvis_query_integrity_summary("native_document_missing", None, None)
    handle = cast(dict[str, Any], handle)
    record = cast(dict[str, Any], record)
    progress = cast(dict[str, Any], progress)
    expected_pipeline_id = current.get("pipeline_id")
    expected_execution_id = current.get("execution_id")
    if observations:
        expected_pipeline_id = observations[0].get("pipeline_id")
        expected_execution_id = observations[0].get("execution_id")
    if not (
        isinstance(current.get("query_job_id"), str)
        and bool(current.get("query_job_id"))
        and current.get("pipeline_id") == expected_pipeline_id
        and current.get("execution_id") == expected_execution_id
        and handle.get("pipeline_id") == expected_pipeline_id
        and handle.get("execution_id") == expected_execution_id
        and record.get("pipeline_id") == expected_pipeline_id
        and record.get("execution_id") == expected_execution_id
        and progress.get("pipeline_id") == expected_pipeline_id
        and progress.get("execution_id") == expected_execution_id
        and handle.get("schema_version") == "jarvis.execution.handle.v1"
        and record.get("schema_version") == "jarvis.execution.record.v1"
        and progress.get("schema_version") == "jarvis.execution.progress.v1"
        and record.get("state") == current.get("state")
        and record.get("terminal") is current.get("terminal")
        and progress.get("execution_state") == current.get("state")
        and progress.get("terminal") is current.get("terminal")
    ):
        return _jarvis_query_integrity_summary(
            "query_identity_changed",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )

    stable_fields = ("execution_id", "pipeline_id", "mode", "scheduler_provider")
    snapshot_fields = (*stable_fields, "scheduler_native_id", "cluster")
    if any(handle.get(field) != record.get(field) for field in snapshot_fields):
        return _jarvis_query_integrity_summary(
            "handle_record_identity_changed",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )
    if observations:
        first_handle = observations[0].get("execution_handle")
        if not isinstance(first_handle, dict):
            return _jarvis_query_integrity_summary(
                "durable_identity_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
        typed_first_handle = cast(dict[str, Any], first_handle)
        if any(handle.get(field) != typed_first_handle.get(field) for field in stable_fields):
            return _jarvis_query_integrity_summary(
                "durable_identity_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
    mode = handle.get("mode")
    provider = handle.get("scheduler_provider")
    native_id = handle.get("scheduler_native_id")
    scheduler_cluster = handle.get("cluster")
    if mode == "direct":
        if provider is not None or native_id is not None or scheduler_cluster is not None:
            return _jarvis_query_integrity_summary(
                "direct_scheduler_identity_present",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
    elif mode == "scheduler":
        if not isinstance(provider, str) or not provider:
            return _jarvis_query_integrity_summary(
                "scheduler_provider_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        assigned_native_id: object = None
        assigned_scheduler_cluster: object = None
        for item in observations:
            prior_handle = item.get("execution_handle")
            if not isinstance(prior_handle, dict):
                continue
            typed_prior_handle = cast(dict[str, Any], prior_handle)
            candidate_native_id = typed_prior_handle.get("scheduler_native_id")
            if candidate_native_id is not None:
                assigned_native_id = candidate_native_id
            candidate_scheduler_cluster = typed_prior_handle.get("cluster")
            if candidate_scheduler_cluster is not None:
                assigned_scheduler_cluster = candidate_scheduler_cluster
        if native_id is not None and (not isinstance(native_id, str) or not native_id):
            return _jarvis_query_integrity_summary(
                "scheduler_native_id_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        if assigned_native_id is not None and native_id != assigned_native_id:
            return _jarvis_query_integrity_summary(
                "scheduler_native_id_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
        if scheduler_cluster is not None and (
            not isinstance(scheduler_cluster, str) or not scheduler_cluster
        ):
            return _jarvis_query_integrity_summary(
                "scheduler_cluster_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        if (
            assigned_scheduler_cluster is not None
            and scheduler_cluster != assigned_scheduler_cluster
        ):
            return _jarvis_query_integrity_summary(
                "scheduler_cluster_changed",
                observations[-1].get("state"),
                current.get("state"),
            )
    else:
        return _jarvis_query_integrity_summary(
            "execution_mode_invalid",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )

    current_state = current.get("state")
    current_terminal = current.get("terminal")
    current_rank = (
        _JARVIS_EXECUTION_STATE_RANK.get(current_state) if isinstance(current_state, str) else None
    )
    if current_state != "unknown" and current_rank is None:
        return _jarvis_query_integrity_summary("invalid_state", None, current_state)
    if current_terminal is True:
        if current_state not in {"completed", "failed", "canceled"}:
            return _jarvis_query_integrity_summary(
                "terminal_state_invalid",
                observations[-1].get("state") if observations else None,
                current_state,
            )
    elif current_terminal is False:
        if (
            current_state in {"completed", "failed", "canceled"}
            or record.get("return_code") is not None
            or record.get("error") is not None
        ):
            return _jarvis_query_integrity_summary(
                "nonterminal_result_present",
                observations[-1].get("state") if observations else None,
                current_state,
            )
    else:
        return _jarvis_query_integrity_summary(
            "terminal_flag_invalid",
            observations[-1].get("state") if observations else None,
            current_state,
        )

    prior_known = next(
        (
            item
            for item in reversed(observations)
            if item.get("state") in _JARVIS_EXECUTION_STATE_RANK
        ),
        None,
    )
    if prior_known is not None and current_rank is not None:
        prior_state = cast(str, prior_known["state"])
        if current_rank < _JARVIS_EXECUTION_STATE_RANK[prior_state]:
            return _jarvis_query_integrity_summary(
                "state_regression",
                prior_state,
                current_state,
            )

    prior_terminal = next(
        (item for item in observations if item.get("terminal") is True),
        None,
    )
    if prior_terminal is None:
        return _jarvis_package_progress_integrity_violation(
            observations,
            current,
            crossed_verified_gap=crossed_verified_gap,
        )
    if current_terminal is not True:
        return _jarvis_query_integrity_summary(
            "terminal_regression",
            prior_terminal.get("state"),
            current_state,
        )
    prior_record = prior_terminal.get("execution_record")
    current_record = current.get("execution_record")
    if not isinstance(prior_record, dict) or not isinstance(current_record, dict):
        return _jarvis_query_integrity_summary(
            "terminal_record_missing",
            prior_terminal.get("state"),
            current_state,
        )
    typed_prior_record = cast(dict[str, Any], prior_record)
    typed_current_record = cast(dict[str, Any], current_record)
    prior_result = (
        prior_terminal.get("state"),
        typed_prior_record.get("return_code"),
        typed_prior_record.get("error"),
    )
    current_result = (
        current_state,
        typed_current_record.get("return_code"),
        typed_current_record.get("error"),
    )
    if current_result != prior_result:
        return _jarvis_query_integrity_summary(
            "terminal_snapshot_changed",
            prior_terminal.get("state"),
            current_state,
        )
    return _jarvis_package_progress_integrity_violation(
        observations,
        current,
        crossed_verified_gap=crossed_verified_gap,
    )


def _jarvis_package_progress_integrity_violation(
    observations: list[dict[str, Any]],
    current: dict[str, Any],
    *,
    crossed_verified_gap: bool,
) -> dict[str, object] | None:
    """Reject package-progress corruption before bounded observation sampling."""
    progress = cast(dict[str, Any], current["progress"])
    packages = progress.get("packages")
    if not isinstance(packages, list):
        return _jarvis_query_integrity_summary(
            "package_progress_invalid",
            observations[-1].get("state") if observations else None,
            current.get("state"),
        )
    prior_packages: dict[tuple[object, object], tuple[int, int, dict[str, Any] | None]] = {}
    for observation in observations:
        prior_progress = observation.get("progress")
        if not isinstance(prior_progress, dict):
            continue
        typed_prior_progress = cast(dict[str, Any], prior_progress)
        if not isinstance(typed_prior_progress.get("packages"), list):
            continue
        for raw_package in cast(list[object], typed_prior_progress["packages"]):
            if not isinstance(raw_package, dict):
                continue
            package = cast(dict[str, Any], raw_package)
            summary = _jarvis_package_progress_summary(
                package,
                expected_execution_id=observation.get("execution_id"),
            )
            if summary is not None:
                prior_packages[(package.get("package_id"), package.get("package_name"))] = summary
    for raw_package in cast(list[object], packages):
        if not isinstance(raw_package, dict):
            return _jarvis_query_integrity_summary(
                "package_progress_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        package = cast(dict[str, Any], raw_package)
        summary = _jarvis_package_progress_summary(
            package,
            expected_execution_id=current.get("execution_id"),
        )
        if summary is None:
            return _jarvis_query_integrity_summary(
                "package_progress_invalid",
                observations[-1].get("state") if observations else None,
                current.get("state"),
            )
        key = (package.get("package_id"), package.get("package_name"))
        prior = prior_packages.get(key)
        if prior is None:
            continue
        event_count, sequence, latest = summary
        prior_event_count, prior_sequence, prior_latest = prior
        if event_count < prior_event_count or sequence < prior_sequence:
            return _jarvis_query_integrity_summary(
                "package_progress_regressed",
                observations[-1].get("state"),
                current.get("state"),
            )
        if latest is not None and prior_latest is not None:
            current_signature = _jarvis_package_progress_signature(latest)
            prior_signature = _jarvis_package_progress_signature(prior_latest)
            if sequence == prior_sequence and (
                event_count != prior_event_count or current_signature != prior_signature
            ):
                return _jarvis_query_integrity_summary(
                    "package_progress_changed_without_sequence",
                    observations[-1].get("state"),
                    current.get("state"),
                )
            if sequence > prior_sequence and (
                event_count <= prior_event_count
                or (
                    not crossed_verified_gap
                    and not _jarvis_package_progress_transition_nonregressing(
                        prior_latest,
                        latest,
                    )
                )
            ):
                return _jarvis_query_integrity_summary(
                    "package_progress_regressed",
                    observations[-1].get("state"),
                    current.get("state"),
                )
    return None


def _jarvis_package_progress_summary(
    package: dict[str, Any],
    *,
    expected_execution_id: object,
) -> tuple[int, int, dict[str, Any] | None] | None:
    """Return validated counters used by the query-integrity accumulator."""
    package_id = package.get("package_id")
    package_name = package.get("package_name")
    event_count = package.get("event_count")
    latest = package.get("latest")
    if (
        not isinstance(package_id, str)
        or not package_id
        or not isinstance(package_name, str)
        or not package_name
        or not isinstance(event_count, int)
        or isinstance(event_count, bool)
        or event_count < 0
    ):
        return None
    if event_count == 0 and latest is None:
        return 0, -1, None
    if not isinstance(latest, dict):
        return None
    typed_latest = cast(dict[str, Any], latest)
    sequence = typed_latest.get("sequence")
    if (
        typed_latest.get("schema_version") != "jarvis.progress.v1"
        or typed_latest.get("execution_id") != expected_execution_id
        or typed_latest.get("package_id") != package_id
        or typed_latest.get("package_name") != package_name
        or not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or sequence < 0
        or not _jarvis_package_progress_semantics_valid(typed_latest)
    ):
        return None
    return event_count, sequence, typed_latest


def _jarvis_package_progress_semantics_valid(progress: dict[str, Any]) -> bool:
    """Validate package progress fields that could otherwise disappear during sampling."""
    state = progress.get("state")
    label = progress.get("label")
    if (
        state not in _JARVIS_PACKAGE_PROGRESS_STATES
        or not isinstance(label, str)
        or not label.strip()
        or len(label) > 256
    ):
        return False
    current = progress.get("current")
    total = progress.get("total")
    if current is not None and (
        isinstance(current, bool)
        or not isinstance(current, (int, float))
        or not math.isfinite(current)
        or current < 0
    ):
        return False
    if total is not None and (
        isinstance(total, bool)
        or not isinstance(total, (int, float))
        or not math.isfinite(total)
        or total <= 0
        or current is None
        or current > total
    ):
        return False
    unit = progress.get("unit")
    if unit is not None and (not isinstance(unit, str) or not unit.strip() or len(unit) > 256):
        return False
    determinate = progress.get("determinate")
    return isinstance(determinate, bool) and determinate is (
        current is not None and total is not None
    )


def _jarvis_package_progress_signature(progress: dict[str, Any]) -> tuple[object, ...]:
    """Return fields that cannot change without a new native progress sequence."""
    return tuple(
        progress.get(field)
        for field in ("state", "label", "determinate", "current", "total", "unit")
    )


def _jarvis_package_progress_transition_nonregressing(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> bool:
    """Allow quantitative reset only after an explicit package phase change."""
    if (previous.get("state"), previous.get("label")) != (
        current.get("state"),
        current.get("label"),
    ):
        return True
    if previous.get("unit") is not None and current.get("unit") != previous.get("unit"):
        return False
    if previous.get("total") is not None and current.get("total") != previous.get("total"):
        return False
    previous_value = previous.get("current")
    current_value = current.get("current")
    return previous_value is None or bool(
        current_value is not None and cast(float, current_value) >= cast(float, previous_value)
    )


def _jarvis_query_integrity_summary(
    reason: str,
    previous_state: object,
    current_state: object,
) -> dict[str, object]:
    """Create one bounded machine-readable integrity accumulator entry."""
    return {
        "schema_version": _JARVIS_QUERY_INTEGRITY_SCHEMA,
        "valid": False,
        "reason": reason,
        "previous_state": previous_state,
        "current_state": current_state,
    }


def _valid_jarvis_query_integrity_marker(value: object) -> bool:
    """Accept only relay-generated, fail-closed query-integrity summaries."""
    if not isinstance(value, dict):
        return False
    marker = cast(dict[str, object], value)
    return bool(
        set(marker) == {"schema_version", "valid", "reason", "previous_state", "current_state"}
        and marker.get("schema_version") == _JARVIS_QUERY_INTEGRITY_SCHEMA
        and marker.get("valid") is False
        and isinstance(marker.get("reason"), str)
        and marker.get("reason")
        and (marker.get("previous_state") is None or isinstance(marker.get("previous_state"), str))
        and (marker.get("current_state") is None or isinstance(marker.get("current_state"), str))
    )


def _has_live_jarvis_package_progress(observation: dict[str, Any]) -> bool:
    """Return whether an in-flight observation contains native package progress."""
    if observation.get("state") != "running" or observation.get("terminal") is not False:
        return False
    progress = observation.get("progress")
    if not isinstance(progress, dict):
        return False
    packages = cast(dict[str, object], progress).get("packages")
    if not isinstance(packages, list):
        return False
    for raw_package in cast(list[object], packages):
        if not isinstance(raw_package, dict):
            continue
        package = cast(dict[str, object], raw_package)
        event_count = package.get("event_count")
        if (
            isinstance(event_count, int)
            and not isinstance(event_count, bool)
            and event_count > 0
            and isinstance(package.get("latest"), dict)
        ):
            return True
    return False


def _run_post_run_jarvis_execution_query(
    *,
    cluster: str,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    profile: str,
    pipeline_id: str,
    execution_id: str,
    retry_selector: dict[str, object] | None = None,
    wait_timeout_seconds: float,
    poll_seconds: float,
) -> _JarvisExecutionQueryAcceptance | _JarvisExecutionQueryPending:
    """Observe one handle-first run without treating a bounded wait as execution failure."""
    _validate_progress_wait(timeout_seconds=wait_timeout_seconds, poll_seconds=poll_seconds)
    query_arguments: dict[str, Any] = {
        "cluster": cluster,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "include_progress": True,
    }
    deadline = monotonic() + wait_timeout_seconds
    lifecycle_observations: list[dict[str, Any]] = []
    latest_attempt: _JarvisExecutionQueryAttempt | None = None
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            if latest_attempt is not None:
                return _nonterminal_jarvis_execution_query_acceptance(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    attempt=latest_attempt,
                    lifecycle_observations=lifecycle_observations,
                )
            return _unobserved_jarvis_execution_query_pending(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                retry_selector=retry_selector,
            )
        try:
            attempt = _execute_jarvis_execution_query(
                definition=definition,
                queue=queue,
                profile=profile,
                arguments=query_arguments,
                deadline=deadline,
                poll_seconds=poll_seconds,
            )
        except ObservationTimeoutError:
            if latest_attempt is None:
                return _unobserved_jarvis_execution_query_pending(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    retry_selector=retry_selector,
                )
            return _nonterminal_jarvis_execution_query_acceptance(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                attempt=latest_attempt,
                lifecycle_observations=lifecycle_observations,
            )
        latest_attempt = attempt
        observation = _jarvis_execution_lifecycle_observation(
            attempt.mcp_result,
            query_job_id=attempt.call_job_id,
            expected_pipeline_id=pipeline_id,
            expected_execution_id=execution_id,
        )
        _append_bounded_jarvis_execution_query_observation(
            lifecycle_observations,
            observation,
        )
        if observation["terminal"] is True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                return _nonterminal_jarvis_execution_query_acceptance(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    lifecycle_observations=lifecycle_observations,
                    outcome="terminal_artifacts_pending",
                )
            try:
                terminal_attempt = _execute_jarvis_execution_query(
                    definition=definition,
                    queue=queue,
                    profile=profile,
                    arguments={**query_arguments, "artifacts": {"page_size": 25}},
                    deadline=deadline,
                    poll_seconds=poll_seconds,
                )
            except ObservationTimeoutError:
                return _nonterminal_jarvis_execution_query_acceptance(
                    cluster=cluster,
                    pipeline_id=pipeline_id,
                    execution_id=execution_id,
                    attempt=attempt,
                    lifecycle_observations=lifecycle_observations,
                    outcome="terminal_artifacts_pending",
                )
            terminal_observation = _jarvis_execution_lifecycle_observation(
                terminal_attempt.mcp_result,
                query_job_id=terminal_attempt.call_job_id,
                expected_pipeline_id=pipeline_id,
                expected_execution_id=execution_id,
            )
            if terminal_observation["terminal"] is not True:
                raise RelayError(
                    "JARVIS execution regressed from terminal during its artifact query"
                )
            _append_bounded_jarvis_execution_query_observation(
                lifecycle_observations,
                terminal_observation,
            )
            return _JarvisExecutionQueryAcceptance(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                outcome="terminal",
                tools_list_response=terminal_attempt.session.tools_list_response,
                call_response=terminal_attempt.session.tools_call_response,
                call_job_id=terminal_attempt.call_job_id,
                call_status=terminal_attempt.call_status,
                artifacts=terminal_attempt.artifacts,
                mcp_result=terminal_attempt.mcp_result,
                provenance=terminal_attempt.provenance,
                initialize_response=terminal_attempt.session.initialize_response,
                stdio_evidence=terminal_attempt.session.evidence(),
                lifecycle_observations=lifecycle_observations,
            )
        remaining = deadline - monotonic()
        if remaining <= 0:
            return _nonterminal_jarvis_execution_query_acceptance(
                cluster=cluster,
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                attempt=attempt,
                lifecycle_observations=lifecycle_observations,
            )
        sleep(min(poll_seconds, remaining))


def _unobserved_jarvis_execution_query_pending(
    *,
    cluster: str,
    pipeline_id: str,
    execution_id: str,
    retry_selector: dict[str, object] | None,
) -> _JarvisExecutionQueryPending:
    """Preserve an exact query selector when the first observation window expires."""
    selector: dict[str, object] = {
        "cluster": cluster,
        "scheduler_cluster": None,
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "scheduler_provider": None,
        "scheduler_native_id": None,
        "last_query_job_id": None,
    }
    if retry_selector is not None:
        selector.update(retry_selector)
    if (
        selector.get("cluster") != cluster
        or selector.get("pipeline_id") != pipeline_id
        or selector.get("execution_id") != execution_id
        or selector.get("last_query_job_id") is not None
    ):
        raise RelayError("JARVIS execution query retry selector changed its durable identity")
    return _JarvisExecutionQueryPending(
        cluster=cluster,
        pipeline_id=pipeline_id,
        execution_id=execution_id,
        selector=selector,
    )


def _nonterminal_jarvis_execution_query_acceptance(
    *,
    cluster: str,
    pipeline_id: str,
    execution_id: str,
    attempt: _JarvisExecutionQueryAttempt,
    lifecycle_observations: list[dict[str, Any]],
    outcome: Literal["observation_unknown", "terminal_artifacts_pending"] = "observation_unknown",
) -> _JarvisExecutionQueryAcceptance:
    """Return the last proven snapshot with a query-only retry selector."""
    return _JarvisExecutionQueryAcceptance(
        cluster=cluster,
        pipeline_id=pipeline_id,
        execution_id=execution_id,
        outcome=outcome,
        tools_list_response=attempt.session.tools_list_response,
        call_response=attempt.session.tools_call_response,
        call_job_id=attempt.call_job_id,
        call_status=attempt.call_status,
        artifacts=attempt.artifacts,
        mcp_result=attempt.mcp_result,
        provenance=attempt.provenance,
        initialize_response=attempt.session.initialize_response,
        stdio_evidence=attempt.session.evidence(),
        lifecycle_observations=lifecycle_observations,
    )


def _execute_jarvis_execution_query(
    *,
    definition: ClusterDefinition,
    queue: ClioCoreQueue,
    profile: str,
    arguments: dict[str, Any],
    deadline: float,
    poll_seconds: float,
) -> _JarvisExecutionQueryAttempt:
    """Execute one query with the workload deadline applied to every boundary."""
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise ObservationTimeoutError("JARVIS execution query deadline expired before MCP dispatch")
    timeout_seconds = min(60.0, max(0.001, remaining))
    session = run_packaged_mcp_stdio_session(
        profile=profile,
        tool="jarvis_get_execution",
        arguments=arguments,
        timeout_seconds=timeout_seconds,
    )
    call_job_id = _mcp_response_job_id(session.tools_call_response)
    timeout_seconds = deadline - monotonic()
    if timeout_seconds <= 0:
        raise ObservationTimeoutError(
            f"JARVIS execution query dispatch exceeded its deadline: {call_job_id}"
        )
    if should_execute_on_cluster(definition):
        call_status = _wait_for_remote_job_terminal(
            definition,
            call_job_id,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            deadline=deadline,
        )
        artifacts = _remote_artifact_records(
            definition,
            call_job_id,
            deadline=deadline,
        )
        mcp_result = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="mcp_result",
            deadline=deadline,
        )
        provenance = _read_remote_json_artifact_kind(
            definition,
            artifacts,
            kind="provenance",
            deadline=deadline,
        )
    else:
        call_status = _wait_for_local_job_terminal(
            queue,
            call_job_id,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        artifacts = _complete_local_artifact_records(queue, call_job_id)
        mcp_result = _read_local_json_artifact_kind(queue, artifacts, kind="mcp_result")
        provenance = _read_local_json_artifact_kind(queue, artifacts, kind="provenance")
    return _JarvisExecutionQueryAttempt(
        session=session,
        call_job_id=call_job_id,
        call_status=cast(dict[str, Any], call_status),
        artifacts=artifacts,
        mcp_result=mcp_result,
        provenance=provenance,
    )


def _jarvis_execution_lifecycle_observation(
    mcp_result: dict[str, Any] | None,
    *,
    query_job_id: str,
    expected_pipeline_id: str,
    expected_execution_id: str,
) -> dict[str, Any]:
    """Extract one identity-bound workload observation from a query result."""
    structured = (
        cast(dict[str, Any], mcp_result.get("structured_result"))
        if mcp_result is not None and isinstance(mcp_result.get("structured_result"), dict)
        else None
    )
    record = (
        cast(dict[str, Any], structured.get("execution_record"))
        if structured is not None and isinstance(structured.get("execution_record"), dict)
        else None
    )
    handle = (
        cast(dict[str, Any], structured.get("execution_handle"))
        if structured is not None and isinstance(structured.get("execution_handle"), dict)
        else None
    )
    progress = (
        cast(dict[str, Any], structured.get("progress"))
        if structured is not None and isinstance(structured.get("progress"), dict)
        else None
    )
    if structured is None or handle is None or record is None or progress is None:
        raise RelayError(
            f"jarvis_get_execution job {query_job_id} omitted its structured lifecycle result"
        )
    if (
        structured.get("pipeline_id") != expected_pipeline_id
        or structured.get("execution_id") != expected_execution_id
        or record.get("pipeline_id") != expected_pipeline_id
        or record.get("execution_id") != expected_execution_id
        or handle.get("pipeline_id") != expected_pipeline_id
        or handle.get("execution_id") != expected_execution_id
        or progress.get("pipeline_id") != expected_pipeline_id
        or progress.get("execution_id") != expected_execution_id
    ):
        raise RelayError(
            f"jarvis_get_execution job {query_job_id} returned a different execution identity"
        )
    state = record.get("state")
    terminal = record.get("terminal")
    if not isinstance(state, str) or not isinstance(terminal, bool):
        raise RelayError(
            f"jarvis_get_execution job {query_job_id} returned an invalid lifecycle state"
        )
    return {
        "query_job_id": query_job_id,
        "pipeline_id": expected_pipeline_id,
        "execution_id": expected_execution_id,
        "state": state,
        "terminal": terminal,
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
        "runtime_metadata": structured.get("runtime_metadata"),
    }


def _wait_for_remote_job_terminal(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    deadline: float | None = None,
) -> dict[str, object]:
    """Wait for one remote relay job without requiring progress observations."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    timeout_deadline = monotonic() + timeout_seconds
    effective_deadline = timeout_deadline if deadline is None else min(timeout_deadline, deadline)
    while True:
        status = _json_output(
            _run_remote_clio_before_deadline(
                definition,
                ["job", "status", job_id],
                deadline=effective_deadline,
            ),
            "JARVIS MCP execution-query job status",
        )
        if status.get("terminal") is True:
            return status
        remaining = effective_deadline - monotonic()
        if remaining <= 0:
            raise ObservationTimeoutError(
                f"job did not reach terminal state before timeout: {job_id}"
            )
        sleep(min(poll_seconds, remaining))


def _wait_for_local_job_terminal(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    """Wait for one local relay job without requiring progress observations."""
    _validate_progress_wait(timeout_seconds=timeout_seconds, poll_seconds=poll_seconds)
    deadline = monotonic() + timeout_seconds
    while True:
        status = get_job_status(queue, job_id)
        if status.get("terminal") is True:
            return status
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise ObservationTimeoutError(
                f"job did not reach terminal state before timeout: {job_id}"
            )
        sleep(min(poll_seconds, remaining))


def _validate_progress_wait(*, timeout_seconds: float, poll_seconds: float) -> None:
    if timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")
    if poll_seconds <= 0:
        raise ConfigurationError("poll_seconds must be positive")


def _json_value(value: str, label: str) -> object:
    try:
        return cast(object, json.loads(value))
    except JSONDecodeError as exc:
        raise RelayError(f"{label} did not return valid JSON: {exc.msg}") from exc


def _json_output(value: str, label: str) -> dict[str, object]:
    decoded = _json_value(value, label)
    if not isinstance(decoded, dict):
        raise RelayError(f"{label} did not return a JSON object")
    return {str(key): item for key, item in cast(dict[object, object], decoded).items()}


def _complete_local_artifact_records(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Read a complete bounded artifact snapshot or fail before using partial evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        page, next_cursor, total = queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_page(
            label=f"artifacts for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(item.model_dump(mode="json") for item in page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"artifacts for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_local_progress_records(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Read a complete bounded progress snapshot or fail before using partial evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        page, next_cursor, total = queue.list_progress_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_page(
            label=f"progress for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(item.model_dump(mode="json") for item in page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"progress for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    max_records: int = MAX_INTERNAL_COLLECTION_RECORDS,
    deadline: float | None = None,
) -> list[dict[str, Any]]:
    """Drain a remote paged CLI collection under an explicit completeness cap."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        payload = _json_output(
            _run_remote_clio_before_deadline(
                definition,
                [
                    *command,
                    "--cursor",
                    str(cursor),
                    "--limit",
                    str(MAX_RESPONSE_PAGE_RECORDS),
                ],
                deadline=deadline,
            ),
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        page: list[dict[str, Any]] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            page.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if next_cursor is not None and (
            isinstance(next_cursor, bool) or not isinstance(next_cursor, int)
        ):
            raise RelayError(f"{label} returned an invalid next_cursor")
        expected_total = _validate_complete_page(
            label=label,
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
            max_records=max_records,
        )
        records.extend(page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"{label} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_source_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    max_source_positions: int = MAX_INTERNAL_COLLECTION_RECORDS,
) -> list[dict[str, Any]]:
    """Drain filtered global source windows while bounding every durable position."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        payload = _json_output(
            run_remote_clio(
                definition,
                [
                    *command,
                    "--cursor",
                    str(cursor),
                    "--limit",
                    str(MAX_RESPONSE_PAGE_RECORDS),
                ],
            ),
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            records.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("source_total")
        returned_cursor = payload.get("source_cursor")
        returned_limit = payload.get("source_limit")
        next_cursor = payload.get("source_next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if total > max_source_positions:
            raise RelayError(f"{label} exceeds the bounded source limit {max_source_positions}")
        if expected_total is not None and total != expected_total:
            raise RelayError(f"{label} changed during bounded discovery")
        expected_total = total
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if next_cursor is None:
            return records
        if (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or next_cursor <= cursor
            or next_cursor > total
        ):
            raise RelayError(f"{label} returned an invalid next_cursor")
        cursor = next_cursor


def _validate_complete_page(
    *,
    label: str,
    cursor: int,
    page_count: int,
    next_cursor: int | None,
    total: int,
    expected_total: int | None,
    collected_count: int,
    max_records: int,
) -> int:
    """Validate a page chain before it can be treated as complete evidence."""
    if max_records < 1:
        raise ValueError("max_records must be positive")
    if total > max_records:
        raise RelayError(f"{label} exceeds the bounded completeness limit {max_records}")
    if expected_total is not None and total != expected_total:
        raise RelayError(f"{label} changed during bounded discovery")
    if collected_count + page_count > total:
        raise RelayError(f"{label} returned more records than its total")
    expected_next = cursor + page_count
    if next_cursor is not None and (
        page_count == 0 or next_cursor != expected_next or next_cursor > total
    ):
        raise RelayError(f"{label} returned a non-contiguous page cursor")
    if next_cursor is None and collected_count + page_count != total:
        raise RelayError(f"{label} ended before its declared total")
    return total


def _remote_worker_info(
    definition: ClusterDefinition,
    *,
    timeout_seconds: float | None = None,
) -> dict[str, object]:
    """Read fresh process-bound worker identity over one optional total deadline."""
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    deadline = None if timeout_seconds is None else monotonic() + timeout_seconds
    info = _json_output(
        _run_remote_clio_before_deadline(
            definition,
            ["endpoint", "worker-info", "--cluster", definition.name],
            deadline=deadline,
        ),
        "remote clio-relay worker runtime info",
    )
    actual_provider = info.get("scheduler_provider")
    if actual_provider != definition.scheduler_provider:
        raise ConfigurationError(
            "remote worker scheduler provider does not match the cluster definition: "
            f"{actual_provider!r} != {definition.scheduler_provider!r}"
        )
    info["target_identity"] = _remote_target_identity(definition, deadline=deadline)
    return info


def _run_remote_clio_before_deadline(
    definition: ClusterDefinition,
    args: list[str],
    *,
    deadline: float | None,
) -> str:
    """Run one remote observation without exceeding a shared monotonic deadline."""
    if deadline is None:
        return run_remote_clio(definition, args)
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise ObservationTimeoutError("remote worker identity observation timed out")
    with remote_command_timeout(remaining):
        return run_remote_clio(definition, args)


def _remote_target_identity(
    definition: ClusterDefinition,
    *,
    deadline: float | None = None,
) -> dict[str, object]:
    """Verify and return one operator-pinned physical cluster identity."""
    target = definition.target_identity
    if target is None:
        raise ConfigurationError(
            f"cluster {definition.name} has no operator-pinned target_identity"
        )
    remote_target = _json_output(
        _run_remote_clio_before_deadline(
            definition,
            [
                "endpoint",
                "target-info",
                "--scheduler-provider",
                definition.scheduler_provider,
            ],
            deadline=deadline,
        ),
        "remote physical cluster target info",
    )
    if remote_target.get("schema_version") != "clio-relay.cluster-target-info.v1":
        raise ConfigurationError("remote physical target identity schema does not match")
    if remote_target.get("scheduler_provider") != definition.scheduler_provider:
        raise ConfigurationError(
            "remote physical target scheduler provider does not match the cluster definition"
        )
    observed_hostnames = {
        value
        for key in ("hostname", "fqdn")
        if isinstance((value := remote_target.get(key)), str) and value
    }
    if not observed_hostnames.intersection(target.hostnames):
        raise ConfigurationError(
            "remote hostname does not match the operator-pinned cluster identity: "
            f"observed={sorted(observed_hostnames)!r} expected={target.hostnames!r}"
        )
    if (
        target.site_marker_sha256 is not None
        and remote_target.get("site_marker_sha256") != target.site_marker_sha256
    ):
        raise ConfigurationError("remote site marker does not match cluster target identity")
    if (
        target.scheduler_cluster_name is not None
        and remote_target.get("scheduler_cluster_name") != target.scheduler_cluster_name
    ):
        raise ConfigurationError("scheduler-native cluster name does not match target identity")
    fingerprints = (
        _ssh_host_key_fingerprints(definition.ssh_host)
        if deadline is None
        else _ssh_host_key_fingerprints(definition.ssh_host, deadline=deadline)
    )
    if not set(fingerprints).intersection(target.ssh_host_key_sha256):
        raise ConfigurationError(
            "live SSH host keys do not match the operator-pinned cluster target identity"
        )
    return {
        **remote_target,
        "ssh_host": definition.ssh_host,
        "ssh_host_key_sha256": fingerprints,
        "expected_hostnames": target.hostnames,
        "expected_ssh_host_key_sha256": target.ssh_host_key_sha256,
        "expected_scheduler_cluster_name": target.scheduler_cluster_name,
        "expected_site_marker_sha256": target.site_marker_sha256,
        "verified": True,
    }


def _ssh_host_key_fingerprints(
    ssh_host: str,
    *,
    deadline: float | None = None,
) -> list[str]:
    """Return trusted SHA-256 host-key fingerprints for a configured SSH target."""
    resolved_host = ssh_host
    resolved_port = "22"
    host_key_alias: str | None = None
    known_hosts_files: list[str] = []
    diagnostics: list[str] = []
    try:
        config = subprocess.run(
            ["ssh", "-G", ssh_host],
            capture_output=True,
            text=True,
            check=False,
            timeout=_remote_observation_subprocess_timeout(10, deadline=deadline),
        )
    except subprocess.TimeoutExpired:
        diagnostics.append("ssh -G timed out")
    except OSError as exc:
        diagnostics.append(f"ssh -G failed: {exc}")
    else:
        if config.returncode != 0:
            diagnostics.append(config.stderr.strip() or f"ssh -G exited {config.returncode}")
        else:
            for line in config.stdout.splitlines():
                fields = line.split(maxsplit=1)
                if len(fields) != 2:
                    continue
                key, value = fields[0].casefold(), fields[1].strip()
                if key == "hostname" and value:
                    resolved_host = value
                elif key == "port" and value:
                    resolved_port = value
                elif key == "hostkeyalias" and value:
                    host_key_alias = value
                elif key == "userknownhostsfile" and value:
                    known_hosts_files.extend(_split_ssh_config_values(value))

    lookup_host = host_key_alias or resolved_host
    if resolved_port != "22":
        lookup_host = f"[{lookup_host}]:{resolved_port}"
    fingerprints: set[str] = set()
    for configured_path in known_hosts_files:
        if configured_path.casefold() == "none":
            continue
        known_hosts_path = Path(os.path.expandvars(os.path.expanduser(configured_path)))
        try:
            found = subprocess.run(
                ["ssh-keygen", "-F", lookup_host, "-f", str(known_hosts_path)],
                capture_output=True,
                text=True,
                check=False,
                timeout=_remote_observation_subprocess_timeout(10, deadline=deadline),
            )
        except subprocess.TimeoutExpired:
            diagnostics.append(f"ssh-keygen timed out for {known_hosts_path}")
            continue
        except OSError as exc:
            diagnostics.append(f"ssh-keygen failed for {known_hosts_path}: {exc}")
            break
        fingerprints.update(_ssh_fingerprints_from_key_lines(found.stdout))
    if fingerprints:
        return sorted(fingerprints)

    try:
        scanned = subprocess.run(
            ["ssh-keyscan", "-T", "10", "-p", resolved_port, resolved_host],
            capture_output=True,
            text=True,
            check=False,
            timeout=_remote_observation_subprocess_timeout(15, deadline=deadline),
        )
    except subprocess.TimeoutExpired:
        diagnostics.append("ssh-keyscan timed out")
        scanned = None
    except OSError as exc:
        diagnostics.append(f"ssh-keyscan failed: {exc}")
        scanned = None
    if scanned is not None:
        fingerprints.update(_ssh_fingerprints_from_key_lines(scanned.stdout))
        if scanned.returncode != 0:
            diagnostics.append(scanned.stderr.strip() or f"ssh-keyscan exited {scanned.returncode}")
    if not fingerprints:
        detail = "; ".join(item for item in diagnostics if item) or "no host keys returned"
        raise ConfigurationError(f"could not observe SSH host keys for {ssh_host}: {detail}")
    return sorted(fingerprints)


def _remote_observation_subprocess_timeout(
    default_seconds: float,
    *,
    deadline: float | None,
) -> float:
    """Return a positive subprocess timeout inside one shared observation budget."""
    if deadline is None:
        return default_seconds
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise ConfigurationError("remote worker identity observation timed out")
    return min(default_seconds, remaining)


def _split_ssh_config_values(value: str) -> list[str]:
    """Split an ``ssh -G`` multi-value while preserving Windows path separators."""
    values: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(value):
        character = value[index]
        if quote is not None:
            if character == quote:
                quote = None
            elif character == "\\" and index + 1 < len(value) and value[index + 1] == quote:
                index += 1
                current.append(value[index])
            else:
                current.append(character)
        elif character in {'"', "'"}:
            quote = character
        elif (
            character == "\\"
            and index + 1 < len(value)
            and (value[index + 1].isspace() or value[index + 1] in {'"', "'"})
        ):
            index += 1
            current.append(value[index])
        elif character.isspace():
            if current:
                values.append("".join(current))
                current = []
        else:
            current.append(character)
        index += 1
    if current:
        values.append("".join(current))
    return values


def _ssh_fingerprints_from_key_lines(output: str) -> set[str]:
    """Decode public-key records emitted by ``ssh-keygen`` or ``ssh-keyscan``."""
    fingerprints: set[str] = set()
    for line in output.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        marker_offset = 1 if fields and fields[0].startswith("@") else 0
        if marker_offset and fields[0].casefold() == "@revoked":
            continue
        if len(fields) < marker_offset + 3:
            continue
        try:
            key_bytes = base64.b64decode(fields[marker_offset + 2], validate=True)
        except (binascii.Error, ValueError):
            continue
        digest = base64.b64encode(hashlib.sha256(key_bytes).digest()).decode().rstrip("=")
        fingerprints.add(f"SHA256:{digest}")
    return fingerprints


def _last_nonempty_line(value: str) -> str:
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    if not lines:
        raise RelayError("remote MCP discovery submission did not return a job id")
    return lines[-1]


def _run_transport_validation(
    *,
    cluster: str,
    transport_mode: str,
    resource_id: str,
    resource_role: str,
    retain_remote_session: bool,
    validation_report: Path | None,
    validation_launcher: str | None,
    validation_install_source: str | None,
    validation_artifact: Path | None,
    probe: Callable[[], list[str]],
) -> list[str]:
    """Run one transport probe and persist canonical success or failure evidence."""
    report_path = validation_report or default_report_path(cluster)
    connector = ValidationResource(
        kind="connector",
        resource_id=resource_id,
        role=resource_role,
        cluster=cluster,
        state="starting",
        metadata={"transport_mode": transport_mode},
    )
    report = new_live_validation_report(
        scenario="transport",
        cluster=cluster,
        transport_modes=[transport_mode],
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode=("transport_probe_detach" if retain_remote_session else "transport_probe_teardown"),
        cancel_scheduler_jobs=False,
    )
    recorder = ValidationRecorder(report)
    try:
        lines = probe()
    except BaseException as exc:
        failed_connector = connector.model_copy(
            update={
                "state": "unknown",
                "metadata": {
                    **connector.metadata,
                    "cleanup_verified": False,
                },
            }
        )
        recorder.add_resource(failed_connector)
        recorder.report.cleanup.actions.append(
            {
                "kind": "transport_probe",
                "resource_id": resource_id,
                "action": "detach" if retain_remote_session else "teardown",
                "outcome": "failed",
            }
        )
        recorder.report.cleanup.remaining_resources.append(failed_connector)
        recorder.record_failure("transport.completed", "complete transport probe", exc)
        recorder.finish(exc)
        recorder.write(report_path)
        raise

    for line in lines:
        recorder.observe_line(line)
    expected_cleanup_line = (
        "transport.cleanup=detached" if retain_remote_session else "transport.cleanup=passed"
    )
    cleanup_verified = expected_cleanup_line in lines and (
        not retain_remote_session or "transport.remote_session=retained" in lines
    )
    if not cleanup_verified:
        expected = (
            "verified active remote-session retention"
            if retain_remote_session
            else "verified transport teardown"
        )
        error = RelayError(f"transport probe returned without {expected} evidence")
        failed_connector = connector.model_copy(
            update={
                "state": "unknown",
                "metadata": {**connector.metadata, "cleanup_verified": False},
            }
        )
        recorder.add_resource(failed_connector)
        recorder.report.cleanup.remaining_resources.append(failed_connector)
        recorder.record_failure("transport.cleanup", "verify transport cleanup", error)
        recorder.finish(error)
        recorder.write(report_path)
        raise error
    recorder.add_resource(
        connector.model_copy(
            update={
                "state": "stopped",
                "metadata": {
                    **connector.metadata,
                    "cleanup_verified": True,
                    "remote_session_retained": retain_remote_session,
                },
            }
        )
    )
    action_outcome = "detached" if retain_remote_session else "stopped"
    recorder.report.cleanup.actions.append(
        {
            "kind": "transport_probe",
            "resource_id": resource_id,
            "action": "detach" if retain_remote_session else "teardown",
            "outcome": action_outcome,
        }
    )
    if retain_remote_session:
        retained_session = ValidationResource(
            kind="relay_session",
            resource_id=resource_id,
            role="transport_probe",
            cluster=cluster,
            state="retained",
            metadata={
                "ownership": "clio-relay",
                "ownership_verified": True,
                "verified_after_operation": True,
            },
        )
        recorder.add_resource(retained_session)
        recorder.report.cleanup.actions.append(
            {
                "kind": "relay_session",
                "resource_id": resource_id,
                "action": "retain",
                "outcome": "retained",
                "ownership_verified": True,
                "verified_after_operation": True,
            }
        )
    try:
        _attach_verified_remote_worker(recorder.report, _require_cluster(cluster))
    except BaseException as exc:
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            exc,
        )
        recorder.finish(exc)
        recorder.write(report_path)
        raise
    recorder.finish()
    recorder.write(report_path)
    lines.append(f"validation.report={report_path.resolve()}")
    return lines


def _run_frpc_connection_validation(
    *,
    cluster: str,
    proxy_name: str,
    frpc_bin: str,
    config: FrpcConfig,
    timeout_seconds: float,
    validation_report: Path,
    validation_launcher: str | None,
    validation_install_source: str | None,
    validation_artifact: Path | None,
) -> list[str]:
    """Run the bounded frpc process probe and persist canonical evidence."""
    report = new_live_validation_report(
        scenario="transport",
        cluster=cluster,
        transport_modes=[config.transport_protocol.value],
        launcher=validation_launcher,
        install_source=validation_install_source,
        artifact_sha256=(
            sha256_file(validation_artifact) if validation_artifact is not None else None
        ),
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode="frpc_connection_probe",
        cancel_scheduler_jobs=False,
    )
    recorder = ValidationRecorder(report)
    connector = ValidationResource(
        kind="connector",
        resource_id=proxy_name,
        role="frpc_connection_probe",
        cluster=cluster,
        state="starting",
        metadata={"transport_mode": config.transport_protocol.value},
    )

    def record_stopped_connector() -> None:
        recorder.add_resource(
            connector.model_copy(
                update={
                    "state": "stopped",
                    "metadata": {**connector.metadata, "cleanup_verified": True},
                }
            )
        )
        if not any(
            action.get("kind") == "connector" and action.get("resource_id") == proxy_name
            for action in recorder.report.cleanup.actions
        ):
            recorder.report.cleanup.actions.append(
                {
                    "kind": "connector",
                    "resource_id": proxy_name,
                    "action": "stop",
                    "outcome": "stopped",
                    "ownership_verified": True,
                    "verified_after_operation": True,
                }
            )

    try:
        with recorder.check(
            "transport.frpc-connection",
            "frpc stayed connected for the bounded probe interval",
        ) as evidence:
            lines = run_frpc_connection_check(
                frpc_bin=frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
            )
            output = "\n".join(lines)
            evidence.append(
                EvidenceReference(
                    kind="frpc_probe",
                    excerpt=lines[0] if lines else "frpc connection probe completed",
                    metadata={
                        "line_count": len(lines),
                        "output_sha256": hashlib.sha256(output.encode()).hexdigest(),
                        "timeout_seconds": timeout_seconds,
                    },
                )
            )
        record_stopped_connector()
        _attach_verified_remote_worker(recorder.report, _require_cluster(cluster))
    except BaseException as exc:
        if not recorder.report.checks:
            recorder.record_failure(
                "transport.frpc-connection",
                "frpc stayed connected for the bounded probe interval",
                exc,
            )
        elif all(check.status is ValidationStatus.PASSED for check in recorder.report.checks):
            recorder.record_failure(
                "worker.installation-info",
                "verify remote worker installation identity",
                exc,
            )
        record_stopped_connector()
        recorder.finish(exc)
        recorder.write(validation_report)
        raise
    recorder.finish()
    recorder.write(validation_report)
    lines.append(f"validation.report={validation_report.resolve()}")
    return lines


def _attach_verified_remote_worker(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    *,
    observed_worker_info: dict[str, object] | None = None,
) -> None:
    """Attach exact remote installation identity when the target executes over SSH."""
    if not should_execute_on_cluster(definition):
        return
    remote_info = (
        observed_worker_info
        if observed_worker_info is not None
        else _remote_worker_info(definition)
    )
    attach_verified_worker_identity(report, remote_info)


def _observe_worker_before_cleanup(
    definition: ClusterDefinition,
) -> tuple[dict[str, object] | None, Exception | None]:
    """Capture bounded worker evidence before cleanup can stop remote services."""
    if not should_execute_on_cluster(definition):
        return None, None
    try:
        return (
            _remote_worker_info(
                definition,
                timeout_seconds=REMOTE_CLEANUP_WORKER_INFO_TIMEOUT_SECONDS,
            ),
            None,
        )
    except Exception as exc:
        return None, exc


def _write_remote_verified_report(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    path: Path,
    *,
    observed_worker_info: dict[str, object] | None = None,
    worker_observation_error: Exception | None = None,
) -> None:
    """Persist a report only after recording remote installation verification."""
    if observed_worker_info is not None and worker_observation_error is not None:
        raise ValueError("worker observation cannot contain both info and an error")
    try:
        if worker_observation_error is not None:
            raise worker_observation_error
        _attach_verified_remote_worker(
            report,
            definition,
            observed_worker_info=observed_worker_info,
        )
        if observed_worker_info is not None:
            for resource in report.resources:
                if (
                    resource.kind == "relay_worker"
                    and resource.resource_id == f"worker:{definition.name}"
                ):
                    resource.metadata["observation_phase"] = "before_cleanup"
    except BaseException as exc:
        recorder = ValidationRecorder(report)
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            exc,
        )
        recorder.finish(exc)
        recorder.write(path)
        raise
    write_validation_report(report, path)


def _write_cleanup_validation_report(
    report: LiveValidationReport,
    definition: ClusterDefinition,
    path: Path,
    *,
    observed_worker_info: dict[str, object] | None = None,
    worker_observation_error: Exception | None = None,
) -> bool:
    """Persist operational cleanup without manufacturing release provenance.

    Ordinary detach and teardown commands do not require a candidate wheel.  When
    no independently computed artifact digest was supplied, the cleanup report
    remains an honest operational result with unverified artifact provenance and
    without verified-worker checks.  The release gate therefore cannot consume it
    as released-artifact evidence.  If a digest is supplied, remote worker
    verification remains strict and any mismatch still fails the acceptance run.

    A bounded pre-cleanup worker observation is optional operational metadata.  Its
    failure is recorded as failed provenance evidence, but it must not hide a
    completed cleanup receipt or change the cleanup command's operational result.
    Return ``True`` only when that optional provenance warning was recorded.
    """
    if report.install_source.artifact_sha256 is None:
        write_validation_report(report, path)
        return False
    if worker_observation_error is not None:
        recorder = ValidationRecorder(report)
        recorder.record_failure(
            "worker.installation-info",
            "verify remote worker installation identity",
            worker_observation_error,
        )
        recorder.finish(worker_observation_error)
        recorder.write(path)
        return True
    _write_remote_verified_report(
        report,
        definition,
        path,
        observed_worker_info=observed_worker_info,
        worker_observation_error=worker_observation_error,
    )
    return False


def _new_cleanup_acceptance_report(
    *,
    scenario: str,
    cluster: str,
    mode: str,
    resource_kind: str,
    resource_id: str,
    action: str,
    cancel_relay_jobs: bool,
    cancel_scheduler_jobs: bool,
    stop_worker: bool,
    launcher: str | None,
    install_source: str | None,
    artifact: Path | None,
) -> LiveValidationReport:
    """Seed requested cleanup policy before any fallible preflight or observation."""
    artifact_sha256: str | None = None
    if artifact is not None:
        with suppress(OSError):
            artifact_sha256 = sha256_file(artifact)
    report = new_live_validation_report(
        scenario=scenario,
        cluster=cluster,
        launcher=launcher,
        install_source=install_source,
        artifact_sha256=artifact_sha256,
    )
    report.cleanup = CleanupEvidence(
        requested=True,
        mode=mode,
        cancel_relay_jobs=cancel_relay_jobs,
        cancel_scheduler_jobs=cancel_scheduler_jobs,
        stop_worker=stop_worker,
        actions=[
            {
                "kind": resource_kind,
                "resource_id": resource_id,
                "action": action,
                "outcome": "pending",
                "verified_after_operation": False,
                "residual": True,
            }
        ],
    )
    return report


def _write_failed_acceptance_report(
    *,
    path: Path,
    scenario: str,
    cluster: str,
    check_id: str,
    summary: str,
    error: BaseException,
    launcher: str | None,
    install_source: str | None,
    artifact: Path | None,
    partial_report: LiveValidationReport | None = None,
) -> None:
    """Persist one canonical failed report without discarding partial evidence."""
    report = partial_report
    if partial_report is not None and path.exists():
        with suppress(OSError, ValidationError, ValueError):
            existing = load_validation_report(path)
            if existing.report_id == partial_report.report_id:
                expected_error = f"{type(error).__name__}: {error}"
                already_recorded = (
                    existing.status is ValidationStatus.FAILED
                    and existing.error == expected_error
                    and any(
                        check.check_id == check_id
                        and check.status is ValidationStatus.FAILED
                        and check.error == expected_error
                        for check in existing.checks
                    )
                )
                if already_recorded:
                    return
                # The caller's in-memory report may contain the latest observation that
                # failed before its next checkpoint write. The on-disk copy is used only
                # for idempotency here; replacing the partial would discard that evidence.
    artifact_sha256: str | None = None
    if artifact is not None:
        with suppress(OSError):
            artifact_sha256 = sha256_file(artifact)
    if report is None:
        report = new_live_validation_report(
            scenario=scenario,
            cluster=cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
    recorder = ValidationRecorder(report)
    recorder.record_failure(check_id, summary, error)
    recorder.finish(error)
    recorder.write(path)


def _load_current_acceptance_report(
    path: Path,
    *,
    expected_report_id: str,
) -> LiveValidationReport | None:
    """Load strict evidence only when it belongs to the current CLI invocation."""
    try:
        report = load_validation_report(path)
    except ConfigurationError:
        return None
    return report if report.report_id == expected_report_id else None


def _echo_lines(lines: list[str]) -> None:
    for line in lines:
        typer.echo(_console_safe_text(line))


def _job_event_cursor(cursor: int) -> int:
    """Normalize CLI event cursors while preserving the durable cursor contract."""
    if cursor < 0:
        raise typer.BadParameter("cursor must be greater than or equal to 0")
    return 1 if cursor == 0 else cursor


def _console_safe_text(value: str) -> str:
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return value.encode(encoding, errors="replace").decode(encoding, errors="replace")


def _try_remote_gateway_session_passthrough(cluster: str | None, args: list[str]) -> bool:
    """Render a validated remote gateway record through the local public projection."""
    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = _require_cluster(cluster)
    if not should_execute_on_cluster(definition):
        return False

    def action() -> None:
        payload = run_remote_clio(definition, args)
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RelayError("remote gateway command did not return valid JSON") from exc
        try:
            session = GatewaySession.model_validate(decoded)
        except ValidationError as exc:
            raise RelayError("remote gateway command returned an invalid session") from exc
        if session.cluster != cluster:
            raise RelayError("remote gateway command returned a different cluster")
        typer.echo(_public_json(public_gateway_session(session)))

    _run_or_exit(action)
    return True


def _try_remote_cluster_passthrough(cluster: str | None, args: list[str]) -> bool:
    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = _require_cluster(cluster)
    if not should_execute_on_cluster(definition):
        return False
    _run_remote_or_exit(definition, args)
    return True


def _try_remote_job_wait_passthrough(
    cluster: str | None,
    *,
    job_id: str,
    timeout_seconds: float,
    poll_seconds: float,
) -> bool:
    """Run one bounded remote wait and preserve its durable receipt on observation expiry."""
    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = _require_cluster(cluster)
    if not should_execute_on_cluster(definition):
        return False
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise typer.BadParameter("timeout-seconds must be positive and finite")
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise typer.BadParameter("poll-seconds must be positive and finite")

    def action() -> None:
        try:
            with remote_command_timeout(
                timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
            ):
                payload = run_remote_clio(
                    definition,
                    [
                        "job",
                        "wait",
                        job_id,
                        "--timeout-seconds",
                        str(timeout_seconds),
                        "--poll-seconds",
                        str(poll_seconds),
                    ],
                )
            document = _json_output(payload, "remote job wait")
            if "observation" in document:
                try:
                    result = JobWaitResult.model_validate(document)
                except ValidationError as exc:
                    raise RelayError("remote job wait returned an invalid result") from exc
            else:
                result = job_wait_result(
                    RelayJob.model_validate(document),
                    timeout_seconds=timeout_seconds,
                )
        except ObservationTimeoutError as observation_error:
            with remote_command_timeout(REMOTE_JOB_WAIT_STATUS_TIMEOUT_SECONDS):
                status = _json_output(
                    run_remote_clio(definition, ["job", "status", job_id]),
                    "remote job status after bounded wait",
                )
            job = RelayJob.model_validate(status.get("job"))
            terminal = job.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
            if status.get("terminal") is not terminal:
                raise RelayError(
                    "remote job status disagrees with its durable job state"
                ) from observation_error
            result = job_wait_result(
                job,
                timeout_seconds=timeout_seconds,
            )

        if result.job_id != job_id or result.cluster != cluster:
            raise RelayError("remote job wait returned a different durable receipt")
        typer.echo(result.model_dump_json(indent=2))

    _run_or_exit(action)
    return True


def _run_remote_or_exit(definition: ClusterDefinition, args: list[str]) -> None:
    _run_or_exit(
        lambda: typer.echo(_console_safe_text(run_remote_clio(definition, args)), nl=False)
    )


def _require_cluster(cluster: str) -> ClusterDefinition:
    return ClusterRegistry.load(default_registry_path()).require(cluster)


def _session_transition_lock(*, cluster: str, session_id: str) -> FileLock:
    """Return the shared cluster-scoped owner-session transition lock."""
    return owner_session_transition_lock(cluster=cluster, session_id=session_id)


def _require_durable_session_identity(value: str, *, field: str) -> str:
    """Validate a session identity before it reaches local or remote persistence."""
    try:
        return validate_durable_record_id(value)
    except ValueError as error:
        raise RelayError(f"invalid {field}: {error}") from error


def _kind_concurrency_options(
    items: list[str] | None,
    *,
    param_hint: str = "--kind-concurrency",
) -> dict[JobKind, int]:
    try:
        return parse_kind_concurrency_options(items)
    except ConfigurationError as exc:
        raise typer.BadParameter(
            str(exc),
            param_hint=param_hint,
        ) from exc


def _resolved_worker_capacity_policy(
    definition: ClusterDefinition,
    *,
    concurrency: int | None,
    control_query_concurrency: int | None,
    kind_concurrency: list[str] | None,
    clear_kind_concurrency: bool,
) -> WorkerCapacityPolicy:
    """Resolve optional CLI overrides against one persisted worker policy."""
    if clear_kind_concurrency and kind_concurrency is not None:
        raise typer.BadParameter(
            "--clear-kind-concurrency cannot be combined with --kind-concurrency"
        )
    current = definition.worker_capacity
    selected_kind_concurrency = (
        {}
        if clear_kind_concurrency
        else (
            current.kind_concurrency
            if kind_concurrency is None
            else _kind_concurrency_options(kind_concurrency)
        )
    )
    try:
        return WorkerCapacityPolicy(
            concurrency=current.concurrency if concurrency is None else concurrency,
            control_query_concurrency=(
                current.control_query_concurrency
                if control_query_concurrency is None
                else control_query_concurrency
            ),
            kind_concurrency=selected_kind_concurrency,
        )
    except ValidationError as exc:
        raise typer.BadParameter(
            str(exc),
            param_hint="--concurrency/--control-query-concurrency",
        ) from exc


def _require_frp_server_addr(server_addr: str, cluster: str) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(
        f"frp server address is not configured for cluster {cluster}; "
        "set it with `clio-relay cluster add --frp-server-addr ...`"
    )


def _resolve_env_secret(value: str | None, env_name: str, label: str) -> str:
    resolved = value or os.getenv(env_name) or _local_secret(env_name)
    if resolved:
        return resolved
    raise ConfigurationError(
        f"{label} is required; pass it explicitly, set {env_name}, "
        f"or add {env_name} to .clio-relay/secrets.json"
    )


def _local_secret(env_name: str) -> str | None:
    path = Path(".clio-relay/secrets.json")
    if not path.exists():
        return None
    loaded = cast(object, json.loads(path.read_text(encoding="utf-8-sig")))
    if not isinstance(loaded, dict):
        raise ConfigurationError(".clio-relay/secrets.json must contain a JSON object")
    secrets = cast(dict[object, object], loaded)
    value = secrets.get(env_name)
    if value is None:
        return None
    if not isinstance(value, str) or value == "":
        raise ConfigurationError(
            f".clio-relay/secrets.json field must be a non-empty string: {env_name}"
        )
    return value


def _environment_references(items: list[str] | None) -> dict[str, str]:
    """Parse repeatable CHILD=SOURCE environment references without reading values."""
    references: dict[str, str] = {}
    for item in items or []:
        child_name, separator, source_name = item.partition("=")
        if not separator or not child_name or not source_name:
            raise typer.BadParameter("--env-from entries must use CHILD=SOURCE")
        if child_name in references:
            raise typer.BadParameter(f"--env-from child name is repeated: {child_name}")
        references[child_name] = source_name
    return references


def _artifact_use_refs(items: list[str] | None) -> list[ArtifactUse]:
    """Parse legacy shorthand or canonical JSON artifact dependency bindings."""
    refs: list[ArtifactUse] = []
    for item in items or []:
        try:
            if item.lstrip().startswith("{"):
                refs.append(ArtifactUse.model_validate_json(item))
            else:
                artifact_id, separator, sha256 = item.partition("=")
                if not separator or not artifact_id or not sha256:
                    raise ValueError(
                        "dependency must use ARTIFACT_ID=SHA256 or a canonical JSON object"
                    )
                refs.append(ArtifactUse(artifact_id=artifact_id, sha256=sha256))
        except ValueError as exc:
            raise typer.BadParameter(
                str(exc),
                param_hint="--used-artifact",
            ) from exc
    artifact_ids = [ref.artifact_id for ref in refs]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise typer.BadParameter(
            "--used-artifact values must have unique artifact IDs",
            param_hint="--used-artifact",
        )
    canonical = sorted(refs, key=lambda ref: ref.artifact_id)
    try:
        validate_artifact_use_collection(canonical)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--used-artifact") from exc
    return canonical


def _artifact_use_cli_value(ref: ArtifactUse) -> str:
    """Render legacy shorthand or canonical JSON for one CLI dependency."""
    if ref.provenance is None:
        return f"{ref.artifact_id}={ref.sha256}"
    return json.dumps(
        artifact_use_payload(ref),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _artifact_use_idempotency_suffix(refs: list[ArtifactUse]) -> str:
    """Return a stable suffix only when a submission has artifact dependencies."""
    if not refs:
        return ""
    payload = [artifact_use_payload(ref) for ref in refs]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f":uses-{hashlib.sha256(encoded).hexdigest()}"


def _run_or_exit(action: Callable[[], None]) -> None:
    try:
        action()
    except StorageAdmissionError as exc:
        _echo_storage_admission_error(exc)
        raise typer.Exit(code=1) from exc
    except (ConfigurationError, RelayError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
