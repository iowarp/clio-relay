"""Core-scoped lifetime locking for relay worker and migration exclusion."""

from __future__ import annotations

import ctypes
import errno
import importlib
import os
import stat
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Protocol, cast

from clio_relay.cluster_config import (
    ensure_private_configuration_directory,
    ensure_private_configuration_path,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path

WORKER_LIFETIME_LOCK_NAME = ".clio-relay-worker-lifetime.lock"
WORKER_LIFETIME_GUARD_FD_ENV = "CLIO_RELAY_WORKER_LIFETIME_GUARD_FD"
WORKER_LIFETIME_GUARD_PID_ENV = "CLIO_RELAY_WORKER_LIFETIME_GUARD_PID"
WORKER_LIFETIME_GUARD_READY_ENV = "CLIO_RELAY_WORKER_LIFETIME_GUARD_READY"
_LOCK_RETRY_SECONDS = 0.05
DEFAULT_WORKER_LIFETIME_LOCK_TIMEOUT_SECONDS = 30.0
_LOCKED_CORE_AUTHORITY = object()


class _FcntlModule(Protocol):
    """Typed surface used from the platform-only ``fcntl`` module."""

    LOCK_EX: int
    LOCK_NB: int
    LOCK_SH: int
    LOCK_UN: int

    def flock(self, fd: int, operation: int) -> Any:
        """Apply an advisory lock operation to ``fd``."""


class _WindowsFunction(Protocol):
    """Typed callable surface for one dynamically loaded Win32 function."""

    argtypes: list[object]
    restype: object

    def __call__(self, *args: object) -> int:
        """Invoke the configured Win32 function."""
        ...


class _WindowsKernel32(Protocol):
    """Win32 lock functions loaded only on Windows."""

    LockFileEx: _WindowsFunction
    UnlockFileEx: _WindowsFunction


def _runtime_attribute(module: object, name: str) -> object:
    """Read an OS-specific module attribute without platform-stub narrowing."""
    try:
        return vars(module)[name]
    except KeyError as exc:
        raise RuntimeError(f"platform runtime attribute is unavailable: {name}") from exc


class _WindowsOverlapped(ctypes.Structure):
    """Portable declaration of the Windows ``OVERLAPPED`` structure."""

    _fields_ = [
        ("Internal", ctypes.c_size_t),
        ("InternalHigh", ctypes.c_size_t),
        ("Offset", ctypes.c_ulong),
        ("OffsetHigh", ctypes.c_ulong),
        ("hEvent", ctypes.c_void_p),
    ]


class WorkerLifetimeLockUnavailable(ConfigurationError):
    """The lifetime lock is healthy but incompatible ownership is active."""


@dataclass
class LockedCoreIdentity:
    """Canonical physical core pinned by one acquired lifetime lock."""

    root: Path
    device: int
    inode: int
    _authority: object = field(repr=False)
    _directory_fd: int | None = field(default=None, repr=False)
    _pinned_root: Path | None = field(default=None, repr=False)
    _active: bool = field(default=True, repr=False)

    def require_active(self) -> None:
        """Reject forged or already-released lifetime ownership."""
        if self._authority is not _LOCKED_CORE_AUTHORITY or not self._active:
            raise ConfigurationError("migration locked-core authority is not active")

    def deactivate(self) -> None:
        """End this identity's authorized lifetime scope."""
        descriptor = self._directory_fd
        self._directory_fd = None
        self._pinned_root = None
        self._active = False
        if descriptor is not None:
            with suppress(OSError):
                os.close(descriptor)

    @property
    def filesystem_root(self) -> Path:
        """Return the descriptor-pinned root used for authorized filesystem I/O."""
        self.require_active()
        descriptor = self._directory_fd
        pinned_root = self._pinned_root
        if descriptor is None or pinned_root is None:
            return self.root
        try:
            descriptor_stat = os.fstat(descriptor)
            pinned_stat = os.stat(pinned_root)
        except OSError as exc:
            raise ConfigurationError("migration pinned-core descriptor is unavailable") from exc
        expected = (self.device, self.inode)
        if (
            descriptor_stat.st_dev,
            descriptor_stat.st_ino,
        ) != expected or (pinned_stat.st_dev, pinned_stat.st_ino) != expected:
            raise ConfigurationError("migration pinned-core descriptor identity changed")
        return pinned_root

    @property
    def filesystem_root_descriptor(self) -> int | None:
        """Return the borrowed descriptor that pins authorized POSIX root I/O."""
        self.require_active()
        descriptor = self._directory_fd
        if descriptor is None:
            return None
        try:
            descriptor_stat = os.fstat(descriptor)
        except OSError as exc:
            raise ConfigurationError("migration pinned-core descriptor is unavailable") from exc
        if (descriptor_stat.st_dev, descriptor_stat.st_ino) != (self.device, self.inode):
            raise ConfigurationError("migration pinned-core descriptor identity changed")
        return descriptor


def require_active_locked_core(identity: LockedCoreIdentity) -> None:
    """Reject forged or out-of-scope migration lock identities."""
    identity.require_active()


def _locked_core_identity(
    root: Path,
    file_stat: os.stat_result,
    *,
    guard_fd: int,
) -> LockedCoreIdentity:
    directory_fd, pinned_root = _pin_locked_core_directory(
        root,
        expected_stat=file_stat,
        guard_fd=guard_fd,
    )
    return LockedCoreIdentity(
        root=root,
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
        _authority=_LOCKED_CORE_AUTHORITY,
        _directory_fd=directory_fd,
        _pinned_root=pinned_root,
    )


def _pin_locked_core_directory(
    root: Path,
    *,
    expected_stat: os.stat_result,
    guard_fd: int,
) -> tuple[int | None, Path | None]:
    """Pin the locked POSIX directory and bind it to the held lock inode."""
    if os.name != "posix":
        return None, None
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fd: int | None = None
    try:
        directory_fd = os.open(root, flags)
        os.set_inheritable(directory_fd, False)
        directory_stat = os.fstat(directory_fd)
        guard_stat = os.fstat(guard_fd)
        linked_guard_stat = os.stat(
            WORKER_LIFETIME_LOCK_NAME,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        expected_identity = (expected_stat.st_dev, expected_stat.st_ino)
        if (
            not stat.S_ISDIR(directory_stat.st_mode)
            or _is_reparse(directory_stat)
            or (directory_stat.st_dev, directory_stat.st_ino) != expected_identity
            or not stat.S_ISREG(linked_guard_stat.st_mode)
            or _is_reparse(linked_guard_stat)
            or linked_guard_stat.st_dev != guard_stat.st_dev
            or linked_guard_stat.st_ino != guard_stat.st_ino
        ):
            raise ConfigurationError(
                "migration core descriptor does not contain its acquired lifetime lock"
            )
        for candidate in (
            Path("/proc/self/fd") / str(directory_fd),
            Path("/dev/fd") / str(directory_fd),
        ):
            try:
                candidate_stat = os.stat(candidate)
            except OSError:
                continue
            if (candidate_stat.st_dev, candidate_stat.st_ino) == expected_identity:
                return directory_fd, candidate
        raise ConfigurationError("POSIX migration requires a descriptor filesystem alias")
    except BaseException:
        if directory_fd is not None:
            with suppress(OSError):
                os.close(directory_fd)
        raise


class WorkerLifetimeLock:
    """Hold shared worker or exclusive migration ownership for one core directory.

    Worker processes use ``shared`` mode for their complete lifetime. Managed
    bootstrap uses the same byte-range lock in ``exclusive`` mode through
    package replacement and migration, then releases it immediately before
    starting the newly installed worker.
    """

    def __init__(
        self,
        core_dir: Path,
        *,
        mode: Literal["shared", "exclusive"],
        timeout_seconds: float | None = None,
        lock_name: str = WORKER_LIFETIME_LOCK_NAME,
    ) -> None:
        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("worker lifetime lock timeout must be non-negative")
        self.core_dir = logical_filesystem_path(core_dir)
        self.mode = mode
        self.timeout_seconds = timeout_seconds
        if (
            not lock_name
            or lock_name in {".", ".."}
            or Path(lock_name).name != lock_name
            or any(character in lock_name for character in "\x00\r\n")
        ):
            raise ValueError("lifetime lock name must be one safe path component")
        self.lock_name = lock_name
        self.path = self.core_dir / self.lock_name
        self._fd: int | None = None
        self._windows_overlapped: _WindowsOverlapped | None = None

    @property
    def acquired(self) -> bool:
        """Return whether this instance currently owns its OS lock."""
        return self._fd is not None

    @property
    def descriptor(self) -> int | None:
        """Return the acquired guard descriptor for authenticated inheritance."""
        return self._fd

    def acquire(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> WorkerLifetimeLock:
        """Acquire the configured shared or exclusive OS lock."""
        if self._fd is not None:
            raise RuntimeError("worker lifetime lock is already acquired")
        effective_timeout = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        if effective_timeout is not None and effective_timeout < 0:
            raise ValueError("worker lifetime lock timeout must be non-negative")
        fd, canonical_core = _open_private_lock_file(
            self.core_dir,
            lock_name=self.lock_name,
        )
        try:
            if os.name == "nt":
                overlapped = _acquire_windows_lock(
                    fd,
                    exclusive=self.mode == "exclusive",
                    timeout_seconds=effective_timeout,
                )
                self._windows_overlapped = overlapped
            else:
                _acquire_posix_lock(
                    fd,
                    exclusive=self.mode == "exclusive",
                    timeout_seconds=effective_timeout,
                )
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd
        self.core_dir = logical_filesystem_path(canonical_core)
        self.path = self.core_dir / self.lock_name
        return self

    def release(self) -> None:
        """Release the OS lock and its non-inheritable file descriptor."""
        fd = self._fd
        if fd is None:
            return
        self._fd = None
        try:
            if os.name == "nt":
                _release_windows_lock(fd, self._windows_overlapped)
            else:
                _release_posix_lock(fd)
        finally:
            self._windows_overlapped = None
            os.close(fd)

    def __enter__(self) -> WorkerLifetimeLock:
        """Acquire this lock for a context-managed lifetime."""
        return self.acquire()

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Release this context-managed lifetime lock."""
        self.release()

    def __del__(self) -> None:
        with suppress(Exception):
            self.release()


def _open_private_lock_file(core_dir: Path, *, lock_name: str) -> tuple[int, Path]:
    internal_core = internal_filesystem_path(core_dir, force_extended=True)
    try:
        internal_core.mkdir(parents=True, exist_ok=True, mode=0o700)
        resolved_core = internal_core.resolve(strict=True)
        ensure_private_configuration_directory(resolved_core)
        directory_stat = os.lstat(resolved_core)
    except OSError as exc:
        raise ConfigurationError(f"cannot prepare worker lifetime lock directory: {exc}") from exc
    if not stat.S_ISDIR(directory_stat.st_mode) or _is_reparse(directory_stat):
        raise ConfigurationError("worker lifetime lock core must be a real directory")
    current_uid = _current_uid()
    if current_uid is not None:
        if directory_stat.st_uid != current_uid:
            raise ConfigurationError("worker lifetime lock core must be owned by the current user")
        if stat.S_IMODE(directory_stat.st_mode) & 0o022:
            raise ConfigurationError(
                "worker lifetime lock core must not be group- or world-writable"
            )

    lock_path = resolved_core / lock_name
    flags = os.O_RDWR | os.O_CREAT
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOINHERIT", 0)
    flags |= getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    created = False
    try:
        fd = os.open(lock_path, flags | os.O_EXCL, 0o600)
        created = True
    except FileExistsError:
        try:
            fd = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise ConfigurationError(f"cannot open private worker lifetime lock: {exc}") from exc
    except OSError as exc:
        raise ConfigurationError(f"cannot open private worker lifetime lock: {exc}") from exc
    if os.name == "nt" and created:
        # The Windows ACL verifier opens a metadata handle that conflicts with
        # the CRT descriptor's sharing mode. The containing directory is
        # already private, so close, pin the ACL, and reopen before identity
        # verification and locking.
        os.close(fd)
        ensure_private_configuration_path(lock_path, directory=False)
        try:
            fd = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise ConfigurationError(f"cannot reopen private worker lifetime lock: {exc}") from exc
    try:
        os.set_inheritable(fd, False)
        opened_stat = os.fstat(fd)
        path_stat = os.lstat(lock_path)
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or _is_reparse(opened_stat)
            or opened_stat.st_nlink != 1
            or path_stat.st_dev != opened_stat.st_dev
            or path_stat.st_ino != opened_stat.st_ino
        ):
            raise ConfigurationError("worker lifetime lock must be one owned regular file")
        if os.name != "nt":
            ensure_private_configuration_path(lock_path, directory=False)
        if current_uid is not None:
            if opened_stat.st_uid != current_uid:
                raise ConfigurationError("worker lifetime lock must be owned by the current user")
            if stat.S_IMODE(opened_stat.st_mode) & 0o077:
                raise ConfigurationError("worker lifetime lock permissions must be owner-private")
    except BaseException:
        os.close(fd)
        raise
    return fd, resolved_core


def _acquire_posix_lock(
    fd: int,
    *,
    exclusive: bool,
    timeout_seconds: float | None,
) -> None:
    try:
        fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))
    except ImportError as exc:
        raise ConfigurationError("POSIX worker lifetime locking requires fcntl") from exc
    operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    if timeout_seconds is None:
        fcntl.flock(fd, operation)
        return
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            fcntl.flock(fd, operation | fcntl.LOCK_NB)
            return
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise ConfigurationError(f"cannot acquire worker lifetime lock: {exc}") from exc
            if time.monotonic() >= deadline:
                raise WorkerLifetimeLockUnavailable(
                    "timed out acquiring worker lifetime lock"
                ) from exc
            time.sleep(min(_LOCK_RETRY_SECONDS, max(0.0, deadline - time.monotonic())))


def _release_posix_lock(fd: int) -> None:
    fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))

    fcntl.flock(fd, fcntl.LOCK_UN)


def _acquire_windows_lock(
    fd: int,
    *,
    exclusive: bool,
    timeout_seconds: float | None,
) -> _WindowsOverlapped:
    import msvcrt
    from ctypes import wintypes

    win_dll = cast(
        Callable[..., _WindowsKernel32],
        _runtime_attribute(ctypes, "WinDLL"),
    )
    get_last_error = cast(
        Callable[[], int],
        _runtime_attribute(ctypes, "get_last_error"),
    )
    get_osfhandle = cast(
        Callable[[int], int],
        _runtime_attribute(msvcrt, "get_osfhandle"),
    )
    kernel32 = win_dll("kernel32", use_last_error=True)
    lock_file_ex = kernel32.LockFileEx
    lock_file_ex.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(_WindowsOverlapped),
    ]
    lock_file_ex.restype = wintypes.BOOL
    handle = wintypes.HANDLE(get_osfhandle(fd))
    overlapped = _WindowsOverlapped()
    exclusive_flag = 0x00000002 if exclusive else 0
    if timeout_seconds is None:
        if not lock_file_ex(handle, exclusive_flag, 0, 1, 0, ctypes.byref(overlapped)):
            error = get_last_error()
            raise ConfigurationError(f"cannot acquire worker lifetime lock: WinError {error}")
        return overlapped

    deadline = time.monotonic() + timeout_seconds
    while True:
        flags = exclusive_flag | 0x00000001
        if lock_file_ex(handle, flags, 0, 1, 0, ctypes.byref(overlapped)):
            return overlapped
        error = get_last_error()
        if error != 33:
            raise ConfigurationError(f"cannot acquire worker lifetime lock: WinError {error}")
        if time.monotonic() >= deadline:
            raise WorkerLifetimeLockUnavailable("timed out acquiring worker lifetime lock")
        time.sleep(min(_LOCK_RETRY_SECONDS, max(0.0, deadline - time.monotonic())))


def _release_windows_lock(fd: int, overlapped: _WindowsOverlapped | None) -> None:
    if overlapped is None:
        raise RuntimeError("Windows worker lifetime lock has no OVERLAPPED state")
    import msvcrt
    from ctypes import wintypes

    win_dll = cast(
        Callable[..., _WindowsKernel32],
        _runtime_attribute(ctypes, "WinDLL"),
    )
    get_last_error = cast(
        Callable[[], int],
        _runtime_attribute(ctypes, "get_last_error"),
    )
    get_osfhandle = cast(
        Callable[[int], int],
        _runtime_attribute(msvcrt, "get_osfhandle"),
    )
    kernel32 = win_dll("kernel32", use_last_error=True)
    unlock_file_ex = kernel32.UnlockFileEx
    unlock_file_ex.restype = wintypes.BOOL
    handle = wintypes.HANDLE(get_osfhandle(fd))
    if not unlock_file_ex(handle, 0, 1, 0, ctypes.byref(overlapped)):
        error = get_last_error()
        raise OSError(error, f"cannot release worker lifetime lock: WinError {error}")


def _is_reparse(file_stat: os.stat_result) -> bool:
    attributes = getattr(file_stat, "st_file_attributes", 0) or 0
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _current_uid() -> int | None:
    """Return the current POSIX uid when the platform exposes one."""
    getuid = getattr(os, "getuid", None)
    if not callable(getuid):
        return None
    uid = getuid()
    if not isinstance(uid, int):
        raise ConfigurationError("operating system returned an invalid current uid")
    return uid


@contextmanager
def exclusive_migration_lifetime(
    core_dir: Path,
    *,
    timeout_seconds: float | None = None,
) -> Generator[LockedCoreIdentity, None, None]:
    """Hold or validate exclusive core ownership for an authorized migration."""
    effective_timeout = (
        DEFAULT_WORKER_LIFETIME_LOCK_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    )
    guard_fd = os.getenv(WORKER_LIFETIME_GUARD_FD_ENV)
    guard_pid = os.getenv(WORKER_LIFETIME_GUARD_PID_ENV)
    guard_ready = os.getenv(WORKER_LIFETIME_GUARD_READY_ENV)
    if guard_fd is None and guard_pid is None and guard_ready is None:
        with WorkerLifetimeLock(
            core_dir,
            mode="exclusive",
            timeout_seconds=effective_timeout,
        ) as lifetime_lock:
            locked_stat = os.stat(lifetime_lock.core_dir)
            descriptor = lifetime_lock.descriptor
            if descriptor is None:
                raise ConfigurationError("exclusive migration lifetime lock is not acquired")
            identity = _locked_core_identity(
                lifetime_lock.core_dir,
                locked_stat,
                guard_fd=descriptor,
            )
            try:
                yield identity
            finally:
                identity.deactivate()
        return
    if not guard_fd or guard_pid is not None or guard_ready is not None:
        raise ConfigurationError(
            "external worker lifetime ownership requires only one inherited guard fd"
        )
    locked_identity = _validate_and_acquire_inherited_migration_guard(
        core_dir,
        guard_fd=guard_fd,
        timeout_seconds=effective_timeout,
    )
    # Do not unlock or close the inherited descriptor here. The bootstrap shell
    # owns the same open-file description and deliberately retains EX across
    # package replacement and migration, until immediately before a service
    # start or its EXIT cleanup. This process's inherited copy independently
    # retains EX if that shell dies while migration is running.
    try:
        yield locked_identity
    finally:
        locked_identity.deactivate()


def _validate_and_acquire_inherited_migration_guard(
    core_dir: Path,
    *,
    guard_fd: str,
    timeout_seconds: float,
) -> LockedCoreIdentity:
    """Validate and acquire EX on bootstrap's exact inherited lock descriptor."""
    current_uid = _current_uid()
    if os.name != "posix" or current_uid is None:
        raise ConfigurationError("inherited worker lifetime guards require POSIX flock")
    try:
        descriptor = int(guard_fd)
    except ValueError as exc:
        raise ConfigurationError("inherited worker lifetime guard fd is invalid") from exc
    if descriptor < 3:
        raise ConfigurationError("inherited worker lifetime guard fd is invalid")
    try:
        opened_stat = os.fstat(descriptor)
    except OSError as exc:
        raise ConfigurationError("inherited worker lifetime guard fd is not open") from exc
    try:
        canonical_core = internal_filesystem_path(
            core_dir,
            force_extended=True,
        ).resolve(strict=True)
        ensure_private_configuration_directory(canonical_core)
        core_stat = os.lstat(canonical_core)
        lock_path = canonical_core / WORKER_LIFETIME_LOCK_NAME
        linked_stat = os.lstat(lock_path)
    except OSError as exc:
        raise ConfigurationError(
            f"inherited worker lifetime guard identity cannot be verified: {exc}"
        ) from exc
    if (
        not stat.S_ISDIR(core_stat.st_mode)
        or _is_reparse(core_stat)
        or core_stat.st_uid != current_uid
        or stat.S_IMODE(core_stat.st_mode) & 0o022
    ):
        raise ConfigurationError(
            "inherited worker lifetime core is not one private owned directory"
        )
    if (
        not stat.S_ISREG(opened_stat.st_mode)
        or _is_reparse(opened_stat)
        or opened_stat.st_nlink != 1
        or opened_stat.st_uid != current_uid
        or stat.S_IMODE(opened_stat.st_mode) & 0o077
        or opened_stat.st_dev != linked_stat.st_dev
        or opened_stat.st_ino != linked_stat.st_ino
    ):
        raise ConfigurationError(
            "inherited worker lifetime guard is not the core's private lock file"
        )
    _acquire_posix_lock(
        descriptor,
        exclusive=True,
        timeout_seconds=timeout_seconds,
    )
    return _locked_core_identity(
        logical_filesystem_path(canonical_core),
        core_stat,
        guard_fd=descriptor,
    )
