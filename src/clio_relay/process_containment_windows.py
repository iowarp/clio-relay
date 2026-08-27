"""Windows Job Object primitives used for kernel-enforced containment.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).
None of these names call each other, and only `_close_windows_handle` is
individually replaced by the test suite (from other owner modules, via the
facade) -- so this file needs no indirection of its own.
"""

from __future__ import annotations

import os
import subprocess


def _create_windows_job() -> int:
    if os.name != "nt":
        raise RuntimeError("Windows job objects require Windows")
    import ctypes
    from ctypes import wintypes

    class _BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            (name, ctypes.c_ulonglong)
            for name in (
                "ReadOperationCount",
                "WriteOperationCount",
                "OtherOperationCount",
                "ReadTransferCount",
                "WriteTransferCount",
                "OtherTransferCount",
            )
        ]

    class _ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimitInformation),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    handle = kernel32.CreateJobObjectW(None, None)
    if not handle:
        raise RuntimeError(f"CreateJobObjectW failed: {ctypes.get_last_error()}")
    information = _ExtendedLimitInformation()
    information.BasicLimitInformation.LimitFlags = 0x00002000
    if not kernel32.SetInformationJobObject(
        handle,
        9,
        ctypes.byref(information),
        ctypes.sizeof(information),
    ):
        error = ctypes.get_last_error()
        kernel32.CloseHandle(handle)
        raise RuntimeError(f"SetInformationJobObject failed: {error}")
    return int(handle)


def _windows_process_start_identity(process_id: int) -> str | None:
    if os.name != "nt":
        raise RuntimeError("Windows process identity inspection requires Windows")
    import ctypes
    from ctypes import wintypes

    error_invalid_parameter = 87
    error_access_denied = 5

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    handle = kernel32.OpenProcess(0x1000, False, process_id)
    if not handle:
        error = ctypes.get_last_error()
        if error == error_invalid_parameter:
            return None
        if error == error_access_denied:
            # ERROR_ACCESS_DENIED does not mean "gone": it can name a real,
            # inspection-restricted process (clio-relay#202 D1 -- the same
            # ambiguity `lock_holder_sidecar._windows_pid_alive` already
            # resolves this way). Returning None here would misreport a
            # possibly-live pid as exited to callers like
            # `terminate_recorded_process_tree` (skip cleanup) and
            # `_append_execution_start` (record identity for a job that
            # just started); raising unconditionally instead crashed the
            # unrelated caller with a raw OpenProcess error. Surface a
            # stable, distinguishable placeholder so recorded-identity
            # comparisons still fail closed (refuse cleanup on mismatch)
            # without an unhandled exception.
            return f"windows-access-denied:{process_id}"
        raise RuntimeError(f"OpenProcess failed for {process_id}: {error}")
    creation = wintypes.FILETIME()
    exit_time = wintypes.FILETIME()
    kernel = wintypes.FILETIME()
    user = wintypes.FILETIME()
    try:
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            raise RuntimeError(
                f"GetExitCodeProcess failed for {process_id}: {ctypes.get_last_error()}"
            )
        if exit_code.value != 259:
            return None
        if not kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            raise RuntimeError(
                f"GetProcessTimes failed for {process_id}: {ctypes.get_last_error()}"
            )
        value = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)
        return f"windows-filetime:{value}"
    finally:
        kernel32.CloseHandle(handle)


def _assign_windows_job(job_handle: int, process: subprocess.Popen[str]) -> None:
    if os.name != "nt":
        raise RuntimeError("Windows job assignment requires Windows")
    import ctypes
    from ctypes import wintypes

    raw_process_handle = getattr(process, "_handle", None)
    if raw_process_handle is None:
        raise RuntimeError("Popen did not expose a Windows process handle")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    if not kernel32.AssignProcessToJobObject(job_handle, int(raw_process_handle)):
        raise RuntimeError(f"AssignProcessToJobObject failed: {ctypes.get_last_error()}")


def _terminate_windows_job(job_handle: int) -> None:
    if os.name != "nt":
        raise RuntimeError("Windows job termination requires Windows")
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    if not kernel32.TerminateJobObject(job_handle, 1):
        raise RuntimeError(f"TerminateJobObject failed: {ctypes.get_last_error()}")


def _windows_job_active_processes(job_handle: int) -> int:
    if os.name != "nt":
        raise RuntimeError("Windows job inspection requires Windows")
    import ctypes
    from ctypes import wintypes

    class _AccountingInformation(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", wintypes.DWORD),
            ("TotalProcesses", wintypes.DWORD),
            ("ActiveProcesses", wintypes.DWORD),
            ("TotalTerminatedProcesses", wintypes.DWORD),
        ]

    information = _AccountingInformation()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if not kernel32.QueryInformationJobObject(
        job_handle,
        1,
        ctypes.byref(information),
        ctypes.sizeof(information),
        None,
    ):
        raise RuntimeError(f"QueryInformationJobObject failed: {ctypes.get_last_error()}")
    return int(information.ActiveProcesses)


def _close_windows_handle(job_handle: int) -> None:
    if os.name != "nt":
        raise RuntimeError("Windows handle cleanup requires Windows")
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if not kernel32.CloseHandle(job_handle):
        raise RuntimeError(f"CloseHandle failed: {ctypes.get_last_error()}")


def _terminate_windows_tree(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float,
) -> None:
    if process.poll() is not None:
        return
    taskkill_error: BaseException | None = None
    try:
        result = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        taskkill_error = exc
        result = subprocess.CompletedProcess(["taskkill", str(process.pid)], 1, "", str(exc))
    # The Popen handle is PID-reuse safe; taskkill diagnostics are localized.
    exited_before_fallback = process.poll() is not None
    taskkill_failed = taskkill_error is not None or result.returncode not in {0, 128}
    if taskkill_failed and not exited_before_fallback:
        process.kill()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_seconds)
    benign_exit_race = (
        taskkill_error is None and result.returncode not in {0, 128} and exited_before_fallback
    )
    if taskkill_failed and not benign_exit_race:
        raise RuntimeError(
            result.stderr.strip()
            or f"taskkill could not prove process-tree termination: {process.pid}"
        )
