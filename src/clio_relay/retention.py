"""Crash-resumable terminal-job retention across spool and core state."""

from __future__ import annotations

import ctypes
import hashlib
import os
import stat
from collections.abc import Generator
from contextlib import contextmanager
from ctypes import wintypes
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from filelock import FileLock
from pydantic import BaseModel, ConfigDict, Field

from clio_relay.core_queue import ClioCoreQueue, purge_quarantined_tree_batch
from clio_relay.errors import QueueConflictError
from clio_relay.identifiers import DurableRecordId, validate_durable_record_id
from clio_relay.models import (
    TerminalJobGcPlan,
    TerminalJobGcResult,
    utc_now,
)
from clio_relay.pagination import validate_gc_batch_size
from clio_relay.spool import read_owned_regular_file_bytes

RETENTION_RECEIPT_SCHEMA = "clio-relay.spool-retention-receipt.v1"
RETENTION_PLAN_SCHEMA = "clio-relay.terminal-retention-plan.v1"
RETENTION_RESULT_SCHEMA = "clio-relay.terminal-retention-result.v1"
MAX_RETENTION_RECEIPT_BYTES = 65_536
DEFAULT_RETENTION_LOCK_TIMEOUT_SECONDS = 10.0


class SpoolRetentionPhase(StrEnum):
    """Durable phases for one outer spool/core retention transaction."""

    PREPARED = "prepared"
    QUARANTINED = "quarantined"
    CORE_COLLECTING = "core_collecting"
    CORE_COMPLETE = "core_complete"
    PURGING = "purging"
    COMPLETE = "complete"


class SpoolQuarantineReceipt(BaseModel):
    """Durable proof that a job spool was quarantined before core retirement."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = RETENTION_RECEIPT_SCHEMA
    receipt_id: DurableRecordId
    job_id: DurableRecordId
    expected_updated_at: datetime
    source_name: DurableRecordId
    quarantine_name: DurableRecordId
    source_existed_at_prepare: bool
    source_device: int | None = Field(default=None, ge=0)
    source_inode: int | None = Field(default=None, ge=0)
    phase: SpoolRetentionPhase = SpoolRetentionPhase.PREPARED
    prepared_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    quarantined_at: datetime | None = None
    core_completed_at: datetime | None = None
    completed_at: datetime | None = None
    purged_entries: int = Field(default=0, ge=0)


class TerminalRetentionPlan(BaseModel):
    """Read-only outer retention decision for one terminal relay job."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = RETENTION_PLAN_SCHEMA
    job_id: DurableRecordId
    core_plan: TerminalJobGcPlan
    eligible: bool
    protections: list[str] = Field(default_factory=list)
    spool_path: str
    receipt_id: DurableRecordId | None = None
    receipt_phase: SpoolRetentionPhase | None = None
    planned_at: datetime = Field(default_factory=utc_now)


class TerminalRetentionResult(BaseModel):
    """Machine-readable bounded progress for one outer retention call."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = RETENTION_RESULT_SCHEMA
    plan: TerminalRetentionPlan
    dry_run: bool = True
    actions: int = Field(default=0, ge=0, le=100)
    complete: bool = False
    scheduler_cancel_requested: bool = False
    receipt: SpoolQuarantineReceipt | None = None
    core_result: TerminalJobGcResult | None = None


class TerminalRetentionCoordinator:
    """Coordinate spool quarantine, core retirement, and bounded spool purge."""

    def __init__(
        self,
        queue: ClioCoreQueue,
        spool_root: Path,
        *,
        lock_timeout_seconds: float = DEFAULT_RETENTION_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        if lock_timeout_seconds <= 0:
            raise ValueError("lock_timeout_seconds must be positive")
        self.queue = queue
        self.spool_root = Path(os.path.abspath(os.fspath(spool_root)))
        self._lock_timeout_seconds = lock_timeout_seconds

    def plan(
        self,
        job_id: str,
        *,
        expected_updated_at: datetime | None = None,
    ) -> TerminalRetentionPlan:
        """Build a read-only retention plan without moving or deleting spool data."""
        validate_durable_record_id(job_id)
        core_plan = self.queue.plan_terminal_job_gc(job_id)
        protections = list(core_plan.protections)
        if not _safe_component(job_id):
            protections.append("spool_job_id_unsafe")
        receipt = self._read_receipt_optional(job_id) if _safe_component(job_id) else None
        tombstone = self.queue.get_job_tombstone(job_id)
        if tombstone is not None:
            if receipt is None:
                protections.append("spool_quarantine_receipt_missing")
            elif tombstone.external_quarantine_id != receipt.receipt_id:
                protections.append("spool_quarantine_receipt_mismatch")
        if receipt is not None and (
            receipt.job_id != job_id or receipt.expected_updated_at != core_plan.expected_updated_at
        ):
            protections.append("spool_quarantine_receipt_ambiguous")
        if expected_updated_at is not None and expected_updated_at != core_plan.expected_updated_at:
            protections.append("job_snapshot_changed")
        if _safe_component(job_id):
            try:
                self._spool_source_identity(job_id)
            except (OSError, QueueConflictError):
                protections.append("spool_source_unsafe")
        protections = list(dict.fromkeys(protections))
        return TerminalRetentionPlan(
            job_id=job_id,
            core_plan=core_plan,
            eligible=core_plan.eligible and not protections,
            protections=protections,
            spool_path=str(self.spool_root / job_id),
            receipt_id=None if receipt is None else receipt.receipt_id,
            receipt_phase=None if receipt is None else receipt.phase,
        )

    def collect(
        self,
        job_id: str,
        *,
        execute: bool = False,
        batch_size: int = 100,
        expected_updated_at: datetime | None = None,
    ) -> TerminalRetentionResult:
        """Dry-run or advance one bounded, crash-resumable retention transaction."""
        batch_size = validate_gc_batch_size(batch_size)
        plan = self.plan(job_id, expected_updated_at=expected_updated_at)
        if not execute or not plan.eligible:
            return TerminalRetentionResult(plan=plan)
        self._ensure_retention_layout()
        with FileLock(
            str(self._retention_root / ".lock"),
            timeout=self._lock_timeout_seconds,
        ):
            plan = self.plan(job_id, expected_updated_at=expected_updated_at)
            if not plan.eligible:
                return TerminalRetentionResult(plan=plan, dry_run=False)
            receipt = self._read_receipt_optional(job_id)
            if receipt is None:
                receipt = self._prepare_receipt(plan)
                self._write_receipt(receipt)
                self._after_retention_checkpoint(SpoolRetentionPhase.PREPARED)
            self._validate_receipt_for_plan(receipt, plan)
            if receipt.phase is SpoolRetentionPhase.COMPLETE:
                return TerminalRetentionResult(
                    plan=plan,
                    dry_run=False,
                    complete=True,
                    receipt=receipt,
                )
            if receipt.phase is SpoolRetentionPhase.PREPARED:
                receipt = self._quarantine_spool(receipt)
                self._write_receipt(receipt)
                self._after_retention_checkpoint(SpoolRetentionPhase.QUARANTINED)
            actions = 0
            core_result: TerminalJobGcResult | None = None
            if receipt.phase in {
                SpoolRetentionPhase.QUARANTINED,
                SpoolRetentionPhase.CORE_COLLECTING,
            }:
                self._verify_spool_stays_quarantined(receipt)
                core_result = self.queue.collect_terminal_job(
                    job_id,
                    execute=True,
                    batch_size=batch_size,
                    expected_updated_at=receipt.expected_updated_at,
                    external_quarantine_id=receipt.receipt_id,
                )
                actions += core_result.actions
                if not core_result.plan.eligible:
                    blocked_plan = plan.model_copy(
                        update={
                            "eligible": False,
                            "protections": core_result.plan.protections,
                        }
                    )
                    return TerminalRetentionResult(
                        plan=blocked_plan,
                        dry_run=False,
                        actions=actions,
                        receipt=receipt,
                        core_result=core_result,
                    )
                if core_result.complete:
                    receipt = receipt.model_copy(
                        update={
                            "phase": SpoolRetentionPhase.CORE_COMPLETE,
                            "core_completed_at": utc_now(),
                            "updated_at": utc_now(),
                        }
                    )
                    self._write_receipt(receipt)
                    self._after_retention_checkpoint(SpoolRetentionPhase.CORE_COMPLETE)
                elif receipt.phase is not SpoolRetentionPhase.CORE_COLLECTING:
                    receipt = receipt.model_copy(
                        update={
                            "phase": SpoolRetentionPhase.CORE_COLLECTING,
                            "updated_at": utc_now(),
                        }
                    )
                    self._write_receipt(receipt)
                    self._after_retention_checkpoint(SpoolRetentionPhase.CORE_COLLECTING)
                if actions >= batch_size or not core_result.complete:
                    return TerminalRetentionResult(
                        plan=plan,
                        dry_run=False,
                        actions=actions,
                        receipt=receipt,
                        core_result=core_result,
                    )
            if receipt.phase is SpoolRetentionPhase.CORE_COMPLETE:
                receipt = receipt.model_copy(
                    update={"phase": SpoolRetentionPhase.PURGING, "updated_at": utc_now()}
                )
                self._write_receipt(receipt)
                self._after_retention_checkpoint(SpoolRetentionPhase.PURGING)
            if receipt.phase is SpoolRetentionPhase.PURGING and actions < batch_size:
                removed, complete = purge_quarantined_tree_batch(
                    self._quarantine_path(receipt),
                    limit=batch_size - actions,
                )
                actions += removed
                updates: dict[str, object] = {
                    "purged_entries": receipt.purged_entries + removed,
                    "updated_at": utc_now(),
                }
                if complete:
                    updates.update(
                        {"phase": SpoolRetentionPhase.COMPLETE, "completed_at": utc_now()}
                    )
                receipt = receipt.model_copy(update=updates)
                self._write_receipt(receipt)
                if complete:
                    self._after_retention_checkpoint(SpoolRetentionPhase.COMPLETE)
            return TerminalRetentionResult(
                plan=plan,
                dry_run=False,
                actions=actions,
                complete=receipt.phase is SpoolRetentionPhase.COMPLETE,
                receipt=receipt,
                core_result=core_result,
            )

    @property
    def _retention_root(self) -> Path:
        return self.spool_root / ".retention"

    @property
    def _receipt_root(self) -> Path:
        return self._retention_root / "receipts"

    @property
    def _quarantine_root(self) -> Path:
        return self._retention_root / "quarantine"

    def _receipt_path(self, job_id: str) -> Path:
        digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
        return self._receipt_root / f"job_{digest}.json"

    def _quarantine_path(self, receipt: SpoolQuarantineReceipt) -> Path:
        return self._quarantine_root / receipt.quarantine_name

    def _prepare_receipt(self, plan: TerminalRetentionPlan) -> SpoolQuarantineReceipt:
        source_identity = self._spool_source_identity(plan.job_id)
        receipt_id = f"retention_{uuid4().hex}"
        return SpoolQuarantineReceipt(
            receipt_id=receipt_id,
            job_id=plan.job_id,
            expected_updated_at=plan.core_plan.expected_updated_at,
            source_name=plan.job_id,
            quarantine_name=receipt_id,
            source_existed_at_prepare=source_identity is not None,
            source_device=None if source_identity is None else source_identity[0],
            source_inode=None if source_identity is None else source_identity[1],
        )

    def _quarantine_spool(
        self,
        receipt: SpoolQuarantineReceipt,
    ) -> SpoolQuarantineReceipt:
        source_identity = self._spool_source_identity(receipt.source_name)
        quarantine_identity = _owned_child_identity(
            self._quarantine_root,
            receipt.quarantine_name,
            expect_directory=True,
        )
        expected_identity = _receipt_source_identity(receipt)
        if receipt.source_existed_at_prepare:
            if source_identity is not None and quarantine_identity is not None:
                raise QueueConflictError("spool source and quarantine destination both exist")
            if source_identity is not None:
                if source_identity != expected_identity:
                    raise QueueConflictError("spool source identity changed before quarantine")
                _rename_owned_child(
                    self.spool_root,
                    receipt.source_name,
                    self._quarantine_root,
                    receipt.quarantine_name,
                    expected_identity=expected_identity,
                    expect_directory=True,
                    replace=False,
                )
            elif quarantine_identity is not None:
                if quarantine_identity != expected_identity:
                    raise QueueConflictError("spool quarantine identity changed after rename")
            else:
                raise QueueConflictError("prepared spool disappeared before quarantine")
        elif source_identity is not None or quarantine_identity is not None:
            raise QueueConflictError("spool presence changed after retention preparation")
        self._after_spool_rename(receipt)
        now = utc_now()
        return receipt.model_copy(
            update={
                "phase": SpoolRetentionPhase.QUARANTINED,
                "quarantined_at": now,
                "updated_at": now,
            }
        )

    def _verify_spool_stays_quarantined(self, receipt: SpoolQuarantineReceipt) -> None:
        if self._spool_source_identity(receipt.source_name) is not None:
            raise QueueConflictError(
                f"active spool reappeared during retention: {self.spool_root / receipt.source_name}"
            )
        quarantine_identity = _owned_child_identity(
            self._quarantine_root,
            receipt.quarantine_name,
            expect_directory=True,
        )
        if receipt.source_existed_at_prepare:
            if quarantine_identity is None:
                raise QueueConflictError("quarantined spool disappeared before core retirement")
            if quarantine_identity != _receipt_source_identity(receipt):
                raise QueueConflictError(
                    "quarantined spool identity changed before core retirement"
                )
        elif quarantine_identity is not None:
            raise QueueConflictError("unexpected quarantine appeared for an absent spool")

    def _read_receipt_optional(self, job_id: str) -> SpoolQuarantineReceipt | None:
        path = self._receipt_path(job_id)
        if _path_lstat(self._receipt_root) is None:
            return None
        identity = _owned_child_identity(
            self._receipt_root,
            path.name,
            expect_directory=False,
        )
        if identity is None:
            return None
        snapshot = read_owned_regular_file_bytes(
            path,
            owned_root=self._receipt_root,
            max_bytes=MAX_RETENTION_RECEIPT_BYTES,
        )
        if snapshot.data is None:
            raise QueueConflictError(f"retention receipt could not be read: {path}")
        if (
            _owned_child_identity(
                self._receipt_root,
                path.name,
                expect_directory=False,
            )
            != identity
        ):
            raise QueueConflictError(f"retention receipt changed during read: {path}")
        try:
            return SpoolQuarantineReceipt.model_validate_json(snapshot.data)
        except ValueError as exc:
            raise QueueConflictError(f"invalid retention receipt {path}: {exc}") from exc

    def _write_receipt(self, receipt: SpoolQuarantineReceipt) -> None:
        self._ensure_retention_layout()
        payload = receipt.model_dump_json(indent=2).encode("utf-8")
        if len(payload) > MAX_RETENTION_RECEIPT_BYTES:
            raise QueueConflictError("retention receipt exceeds its byte limit")
        path = self._receipt_path(receipt.job_id)
        receipt_parent_identity = _owned_child_identity(
            path.parent.parent,
            path.parent.name,
            expect_directory=True,
        )
        if receipt_parent_identity is None:
            raise QueueConflictError("retention receipt directory disappeared")
        _atomic_write_owned_record(
            path.parent,
            path.name,
            payload,
            expected_parent_identity=receipt_parent_identity,
        )
        committed = self._read_receipt_optional(receipt.job_id)
        if committed != receipt:
            raise QueueConflictError("retention receipt changed during atomic commit")

    def _ensure_retention_layout(self) -> None:
        self.spool_root.mkdir(parents=True, exist_ok=True)
        for directory in (
            self.spool_root,
            self._retention_root,
            self._receipt_root,
            self._quarantine_root,
        ):
            directory.mkdir(parents=True, exist_ok=True)
            directory_stat = os.lstat(directory)
            if not stat.S_ISDIR(directory_stat.st_mode) or _is_reparse(directory_stat):
                raise QueueConflictError(f"retention path is not an owned directory: {directory}")

    def _spool_source_identity(self, job_id: str) -> tuple[int, int] | None:
        root_stat = _path_lstat(self.spool_root)
        if root_stat is None:
            return None
        if not stat.S_ISDIR(root_stat.st_mode) or _is_reparse(root_stat):
            raise QueueConflictError(f"spool root is not an owned directory: {self.spool_root}")
        return _owned_child_identity(
            self.spool_root,
            job_id,
            expect_directory=True,
        )

    @staticmethod
    def _after_retention_checkpoint(_phase: SpoolRetentionPhase) -> None:
        """Fault-injection seam after each durable outer-retention phase."""

    @staticmethod
    def _after_spool_rename(_receipt: SpoolQuarantineReceipt) -> None:
        """Fault-injection seam after rename and before the quarantine receipt update."""

    @staticmethod
    def _validate_receipt_for_plan(
        receipt: SpoolQuarantineReceipt,
        plan: TerminalRetentionPlan,
    ) -> None:
        if (
            receipt.schema_version != RETENTION_RECEIPT_SCHEMA
            or receipt.job_id != plan.job_id
            or receipt.source_name != plan.job_id
            or receipt.expected_updated_at != plan.core_plan.expected_updated_at
            or not _safe_component(receipt.quarantine_name)
        ):
            raise QueueConflictError("retention receipt does not match the current job plan")


def collect_terminal_job_retention(
    queue: ClioCoreQueue,
    spool_root: Path,
    job_id: str,
    *,
    execute: bool = False,
    batch_size: int = 100,
    expected_updated_at: datetime | None = None,
) -> TerminalRetentionResult:
    """Convenience entry point shared by administrative surfaces."""
    return TerminalRetentionCoordinator(queue, spool_root).collect(
        job_id,
        execute=execute,
        batch_size=batch_size,
        expected_updated_at=expected_updated_at,
    )


def _safe_component(value: str) -> bool:
    return (
        bool(value)
        and len(value) <= 256
        and value not in {".", ".."}
        and all(character.isalnum() or character in "-_." for character in value)
    )


class _WindowsFileInformation(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", wintypes.DWORD),
        ("ftCreationTime", wintypes.FILETIME),
        ("ftLastAccessTime", wintypes.FILETIME),
        ("ftLastWriteTime", wintypes.FILETIME),
        ("dwVolumeSerialNumber", wintypes.DWORD),
        ("nFileSizeHigh", wintypes.DWORD),
        ("nFileSizeLow", wintypes.DWORD),
        ("nNumberOfLinks", wintypes.DWORD),
        ("nFileIndexHigh", wintypes.DWORD),
        ("nFileIndexLow", wintypes.DWORD),
    ]


class _WindowsRenameInformation(ctypes.Structure):
    _fields_ = [
        ("ReplaceIfExists", wintypes.BOOL),
        ("RootDirectory", wintypes.HANDLE),
        ("FileNameLength", wintypes.DWORD),
        ("FileName", wintypes.WCHAR * 1),
    ]


@contextmanager
def _open_windows_owned_handle(
    path: Path,
    *,
    expect_directory: bool,
    delete_access: bool = False,
) -> Generator[int]:
    if os.name != "nt":
        raise RuntimeError("Windows owned handles require Windows")
    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    access = 0x80 | 0x00100000 | (0x00010000 if delete_access else 0)
    handle = create_file(
        str(path),
        access,
        0x1 | 0x2 | 0x4,
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    handle_value = cast(int | None, handle)
    if handle_value is None or handle_value == invalid_handle:
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)
    typed_handle = int(handle_value)
    try:
        information = _windows_file_information(kernel32, typed_handle)
        is_directory = bool(information.dwFileAttributes & 0x10)
        is_reparse = bool(information.dwFileAttributes & 0x400)
        if is_directory != expect_directory or is_reparse:
            raise QueueConflictError(f"owned Windows path has an unsafe type: {path}")
        if not expect_directory and information.nNumberOfLinks != 1:
            raise QueueConflictError(f"owned Windows file is hard linked: {path}")
        yield typed_handle
    finally:
        kernel32.CloseHandle(wintypes.HANDLE(typed_handle))


def _windows_file_information(kernel32: Any, handle: int) -> _WindowsFileInformation:
    if os.name != "nt":
        raise RuntimeError("Windows file information requires Windows")
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [wintypes.HANDLE, ctypes.POINTER(_WindowsFileInformation)]
    get_information.restype = wintypes.BOOL
    information = _WindowsFileInformation()
    if not get_information(wintypes.HANDLE(handle), ctypes.byref(information)):
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error))
    return information


def _windows_handle_identity(kernel32: Any, handle: int) -> tuple[int, int]:
    if os.name != "nt":
        raise RuntimeError("Windows handle identity requires Windows")
    information = _windows_file_information(kernel32, handle)
    inode = (int(information.nFileIndexHigh) << 32) | int(information.nFileIndexLow)
    return int(information.dwVolumeSerialNumber), inode


def _windows_final_path(kernel32: Any, handle: int) -> Path:
    if os.name != "nt":
        raise RuntimeError("Windows final-path inspection requires Windows")
    get_final_path = kernel32.GetFinalPathNameByHandleW
    get_final_path.argtypes = [
        wintypes.HANDLE,
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    ]
    get_final_path.restype = wintypes.DWORD
    required = get_final_path(wintypes.HANDLE(handle), None, 0, 0)
    if required == 0:
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error))
    buffer = ctypes.create_unicode_buffer(required + 1)
    written = get_final_path(wintypes.HANDLE(handle), buffer, len(buffer), 0)
    if written == 0 or written >= len(buffer):
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error))
    value = buffer.value
    if value.startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return Path(os.path.abspath(value))


def _windows_rename_handle(
    source_handle: int,
    destination_parent_handle: int,
    destination_parent: Path,
    destination_name: str,
    *,
    replace: bool,
) -> None:
    if os.name != "nt":
        raise RuntimeError("Windows handle rename requires Windows")
    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
    destination_parent_before = _windows_final_path(kernel32, destination_parent_handle)
    configured_parent = Path(os.path.abspath(destination_parent))
    if os.path.normcase(destination_parent_before) != os.path.normcase(configured_parent):
        raise QueueConflictError(
            f"Windows destination parent changed before rename: {destination_parent}"
        )
    destination_path = destination_parent_before / destination_name
    encoded_name = str(destination_path).encode("utf-16-le")
    filename_offset = _WindowsRenameInformation.FileName.offset
    buffer = ctypes.create_string_buffer(
        filename_offset + len(encoded_name) + ctypes.sizeof(wintypes.WCHAR)
    )
    information = ctypes.cast(
        buffer,
        ctypes.POINTER(_WindowsRenameInformation),
    ).contents
    information.ReplaceIfExists = replace
    information.RootDirectory = None
    information.FileNameLength = len(encoded_name)
    ctypes.memmove(ctypes.addressof(buffer) + filename_offset, encoded_name, len(encoded_name))
    set_information = kernel32.SetFileInformationByHandle
    set_information.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    set_information.restype = wintypes.BOOL
    if not set_information(
        wintypes.HANDLE(source_handle),
        3,
        buffer,
        len(buffer),
    ):
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error))
    destination_parent_after = _windows_final_path(kernel32, destination_parent_handle)
    if os.path.normcase(destination_parent_after) != os.path.normcase(destination_parent_before):
        raise QueueConflictError(
            f"Windows destination parent changed during rename: {destination_parent}"
        )
    final_path = _windows_final_path(kernel32, source_handle)
    expected_path = destination_path
    if os.path.normcase(final_path) != os.path.normcase(expected_path):
        raise QueueConflictError(
            f"Windows handle rename reached an unexpected destination: {final_path}"
        )


@contextmanager
def _open_posix_owned_directory(path: Path) -> Generator[int]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        current = os.lstat(path)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or _is_reparse(opened)
            or not os.path.samestat(opened, current)
        ):
            raise QueueConflictError(f"owned directory changed while opening: {path}")
        yield descriptor
    finally:
        os.close(descriptor)


def _owned_child_identity(
    parent: Path,
    name: str,
    *,
    expect_directory: bool,
) -> tuple[int, int] | None:
    if not _safe_component(name):
        raise QueueConflictError(f"unsafe owned path component: {name}")
    if os.name == "nt":
        try:
            with _open_windows_owned_handle(parent, expect_directory=True) as parent_handle:
                parent_final = _windows_final_path(
                    ctypes.WinDLL("kernel32", use_last_error=True),
                    parent_handle,
                )
                with _open_windows_owned_handle(
                    parent / name,
                    expect_directory=expect_directory,
                ) as child_handle:
                    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
                    child_final = _windows_final_path(kernel32, child_handle)
                    if os.path.normcase(child_final.parent) != os.path.normcase(parent_final):
                        raise QueueConflictError(
                            f"owned Windows child escaped its parent: {parent / name}"
                        )
                    return _windows_handle_identity(kernel32, child_handle)
        except FileNotFoundError:
            return None
        except OSError as exc:
            if getattr(exc, "winerror", None) in {2, 3} or exc.errno in {2, 3}:
                return None
            raise QueueConflictError(
                f"cannot inspect owned Windows child: {parent / name}"
            ) from exc
    with _open_posix_owned_directory(parent) as parent_descriptor:
        try:
            child_stat = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if (
            stat.S_ISDIR(child_stat.st_mode) != expect_directory
            or stat.S_ISLNK(child_stat.st_mode)
            or _is_reparse(child_stat)
            or (not expect_directory and child_stat.st_nlink != 1)
        ):
            raise QueueConflictError(f"owned child has an unsafe type: {parent / name}")
        return child_stat.st_dev, child_stat.st_ino


def _rename_owned_child(
    source_parent: Path,
    source_name: str,
    destination_parent: Path,
    destination_name: str,
    *,
    expected_identity: tuple[int, int],
    expect_directory: bool,
    replace: bool,
) -> None:
    if not _safe_component(source_name) or not _safe_component(destination_name):
        raise QueueConflictError("owned rename contains an unsafe path component")
    if os.name == "nt":
        with (
            _open_windows_owned_handle(
                source_parent,
                expect_directory=True,
            ) as source_parent_handle,
            _open_windows_owned_handle(
                destination_parent,
                expect_directory=True,
            ) as destination_parent_handle,
            _open_windows_owned_handle(
                source_parent / source_name,
                expect_directory=expect_directory,
                delete_access=True,
            ) as source_handle,
        ):
            kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
            source_parent_final = _windows_final_path(kernel32, source_parent_handle)
            source_final = _windows_final_path(kernel32, source_handle)
            if os.path.normcase(source_final.parent) != os.path.normcase(source_parent_final):
                raise QueueConflictError("owned Windows source escaped its parent")
            if _windows_handle_identity(kernel32, source_handle) != expected_identity:
                raise QueueConflictError("owned Windows source identity changed before rename")
            _windows_rename_handle(
                source_handle,
                destination_parent_handle,
                destination_parent,
                destination_name,
                replace=replace,
            )
            if _windows_handle_identity(kernel32, source_handle) != expected_identity:
                raise QueueConflictError("owned Windows source identity changed during rename")
        return
    with (
        _open_posix_owned_directory(source_parent) as source_parent_descriptor,
        _open_posix_owned_directory(destination_parent) as destination_parent_descriptor,
    ):
        source_stat = os.stat(
            source_name,
            dir_fd=source_parent_descriptor,
            follow_symlinks=False,
        )
        if (source_stat.st_dev, source_stat.st_ino) != expected_identity:
            raise QueueConflictError("owned POSIX source identity changed before rename")
        try:
            destination_stat = os.stat(
                destination_name,
                dir_fd=destination_parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            destination_stat = None
        if destination_stat is not None and not replace:
            raise QueueConflictError("owned rename destination already exists")
        rename = os.replace if replace else os.rename
        rename(
            source_name,
            destination_name,
            src_dir_fd=source_parent_descriptor,
            dst_dir_fd=destination_parent_descriptor,
        )
        moved_stat = os.stat(
            destination_name,
            dir_fd=destination_parent_descriptor,
            follow_symlinks=False,
        )
        if (moved_stat.st_dev, moved_stat.st_ino) != expected_identity:
            raise QueueConflictError("owned POSIX source identity changed during rename")
        os.fsync(source_parent_descriptor)
        os.fsync(destination_parent_descriptor)


def _atomic_write_owned_record(
    parent: Path,
    name: str,
    payload: bytes,
    *,
    expected_parent_identity: tuple[int, int] | None = None,
) -> None:
    if not _safe_component(name):
        raise QueueConflictError(f"unsafe owned record name: {name}")
    if (
        expected_parent_identity is not None
        and _owned_child_identity(
            parent.parent,
            parent.name,
            expect_directory=True,
        )
        != expected_parent_identity
    ):
        raise QueueConflictError("owned record parent identity changed before commit")
    temporary_name = f".{uuid4().hex}.tmp"
    temporary_path = parent / temporary_name
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    try:
        descriptor = os.open(temporary_path, flags, 0o600)
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
        temporary_stat = os.fstat(descriptor)
        if not stat.S_ISREG(temporary_stat.st_mode) or temporary_stat.st_nlink != 1:
            raise QueueConflictError(f"temporary retention record is unsafe: {temporary_path}")
        os.close(descriptor)
        descriptor = -1
        identity = _owned_child_identity(
            parent,
            temporary_name,
            expect_directory=False,
        )
        if identity is None:
            raise QueueConflictError("temporary retention record disappeared before commit")
        _rename_owned_child(
            parent,
            temporary_name,
            parent,
            name,
            expected_identity=identity,
            expect_directory=False,
            replace=True,
        )
        committed_identity = _owned_child_identity(parent, name, expect_directory=False)
        if committed_identity != identity:
            raise QueueConflictError("retention record identity changed during commit")
        if (
            expected_parent_identity is not None
            and _owned_child_identity(
                parent.parent,
                parent.name,
                expect_directory=True,
            )
            != expected_parent_identity
        ):
            raise QueueConflictError("owned record parent identity changed during commit")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _path_lstat(path: Path) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None


def _is_reparse(path_stat: os.stat_result) -> bool:
    attributes = getattr(path_stat, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _receipt_source_identity(
    receipt: SpoolQuarantineReceipt,
) -> tuple[int, int]:
    if receipt.source_device is None or receipt.source_inode is None:
        raise QueueConflictError("retention receipt has no prepared spool identity")
    return receipt.source_device, receipt.source_inode
