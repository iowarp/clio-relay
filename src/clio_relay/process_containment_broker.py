"""Containment broker process spawn, release, and readiness handshake.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).

`_spawn_broker`, `_release_broker`, and `_remove_broker_readiness` are
individually replaced by the test suite via `monkeypatch.setattr` on the
facade module, as are the shared `_BROKER_SCRIPT`, `BROKER_READY_TIMEOUT_SECONDS`,
and `POLL_SECONDS` names this file reads. Calls to (and reads of) those names
go through the live facade module (`clio_relay.process_containment`) instead
of a plain import, so a monkeypatch applied to the facade after import is
observed here exactly as it was when all of this code lived in one file --
matching bare-name lookup through the module's own globals, which is what
`monkeypatch.setattr(module, name, ...)` replaces.
"""

from __future__ import annotations

import base64
import hmac
import json
import os
import stat as stat_module
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from clio_relay import process_containment as _pc
from clio_relay.process_containment_environment import broker_child_environment_payload
from clio_relay.process_containment_popen import owner_popen_kwargs
from clio_relay.process_containment_types import (
    _BROKER_EXCEPTION_TYPE_PATTERN,
    _BROKER_STARTUP_STAGE_CODES,
    BROKER_PROTOCOL_MAX_BYTES,
    BROKER_SETUP_MAX_BYTES,
    BROKER_STARTUP_RECORD_MAX_BYTES,
    BROKER_STARTUP_RECORD_SCHEMA,
    BROKER_STDIN_MAX_BYTES,
    DISCOVERY_TIMEOUT_SECONDS,
    TERMINATION_TIMEOUT_SECONDS,
    _BrokerReadiness,
    _BrokerStartupDiagnostic,
    _BrokerStartupRecord,
    _reject_broker_duplicate_keys,
)


def _spawn_broker(
    command: list[str],
    popen_kwargs: dict[str, Any],
) -> tuple[subprocess.Popen[str], _BrokerReadiness]:
    if "stdin" in popen_kwargs:
        raise RuntimeError("owned process launch reserves stdin for containment setup")
    readiness = _precreate_broker_readiness()
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-S",
                "-u",
                "-c",
                _pc._BROKER_SCRIPT,
                json.dumps(command),
                str(readiness.path),
                json.dumps(readiness.anchor(), separators=(",", ":")),
                str(Path(__file__).resolve().parent.parent),
            ],
            **popen_kwargs,
            stdin=subprocess.PIPE,
            **owner_popen_kwargs(),
        )
    except BaseException:
        _pc._remove_broker_readiness(readiness)
        raise
    return process, readiness


def _validate_broker_credential_payload(payload: str | None) -> None:
    """Reject secret broker transport where a POSIX pipe cannot be guaranteed."""
    if payload is None:
        return
    if os.name == "nt":
        raise RuntimeError("secure broker credential transport requires POSIX")
    if not payload or len(payload.encode("utf-8")) > BROKER_PROTOCOL_MAX_BYTES:
        raise RuntimeError("broker credential payload is empty or exceeds its byte limit")


def _validate_broker_target_environment(
    environment: Mapping[str, str] | None,
) -> dict[str, str] | None:
    """Validate Windows target-only values delivered after Job Object assignment."""
    if environment is None:
        return None
    if os.name != "nt":
        raise RuntimeError("broker target environment setup is restricted to Windows")
    payload = broker_child_environment_payload(environment)
    if len(payload.encode("utf-8")) > BROKER_PROTOCOL_MAX_BYTES:
        raise RuntimeError("broker target environment exceeded its byte limit")
    return dict(environment)


def _release_broker(
    process: subprocess.Popen[str],
    *,
    readiness: _BrokerReadiness,
    credential_payload: str | None = None,
    stdin_payload: bytes | None = None,
    interactive_stdin: bool = False,
    target_environment: Mapping[str, str] | None = None,
    startup_deadline: float,
) -> None:
    if process.stdin is None:
        raise RuntimeError("containment broker did not expose its setup channel")
    if stdin_payload is not None and len(stdin_payload) > BROKER_STDIN_MAX_BYTES:
        raise RuntimeError("containment broker stdin payload exceeded its byte limit")
    message = json.dumps(
        {
            "release": True,
            "credential": credential_payload,
            "readiness_token": readiness.token,
            "stdin_payload": (
                None if stdin_payload is None else base64.b64encode(stdin_payload).decode("ascii")
            ),
            "interactive_stdin": interactive_stdin,
            "target_environment": (
                None if target_environment is None else dict(target_environment)
            ),
        },
        separators=(",", ":"),
    )
    setup = (message + "\n").encode("utf-8")
    if len(setup) > BROKER_SETUP_MAX_BYTES:
        raise RuntimeError("containment broker setup message exceeded its byte limit")
    encoded = setup
    setup_channel = process.stdin
    completed = threading.Event()
    errors: list[BaseException] = []

    def write_setup() -> None:
        try:
            view = memoryview(encoded)
            while view:
                written = os.write(setup_channel.fileno(), view)
                if written <= 0:
                    raise RuntimeError("containment broker setup write made no progress")
                view = view[written:]
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()

    writer = threading.Thread(
        target=write_setup,
        name=f"clio-relay-broker-release-{process.pid}",
        daemon=True,
    )
    writer.start()
    while not completed.is_set():
        remaining = startup_deadline - time.monotonic()
        if remaining > 0:
            completed.wait(remaining)
            continue
        if process.poll() is None:
            process.kill()
        process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        writer.join(timeout=TERMINATION_TIMEOUT_SECONDS)
        if writer.is_alive():
            with suppress(OSError):
                setup_channel.close()
            writer.join(timeout=TERMINATION_TIMEOUT_SECONDS)
        process.stdin = None
        if writer.is_alive():
            raise RuntimeError("containment broker setup writer remained active after timeout")
        with suppress(OSError):
            setup_channel.close()
        raise RuntimeError("containment broker setup write timed out")
    writer.join()
    if errors:
        setup_channel.close()
        process.stdin = None
        raise RuntimeError(f"containment broker setup write failed: {type(errors[0]).__name__}")
    if not interactive_stdin:
        setup_channel.close()
        process.stdin = None
    _await_broker_readiness(process, readiness, startup_deadline=startup_deadline)


def _precreate_broker_readiness() -> _BrokerReadiness:
    """Create a private, pinned, bounded broker-readiness channel."""
    path = Path(tempfile.gettempdir()) / f".clio-relay-broker-{uuid4().hex}.ready"
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        os.set_inheritable(descriptor, False)
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        opened = os.fstat(descriptor)
        mode = stat_module.S_IMODE(opened.st_mode)
        if (
            not stat_module.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (os.name != "nt" and (opened.st_uid != os.getuid() or mode != 0o600))
        ):
            raise RuntimeError("broker readiness channel was not a private regular file")
        return _BrokerReadiness(
            path=path,
            descriptor=descriptor,
            token=uuid4().hex,
            device=int(opened.st_dev),
            inode=int(opened.st_ino),
            owner=int(opened.st_uid),
            link_count=int(opened.st_nlink),
            mode=mode,
        )
    except BaseException:
        os.close(descriptor)
        with suppress(OSError):
            path.unlink()
        raise


def _parse_broker_startup_record(
    payload: bytes,
    *,
    expected_token: str,
) -> _BrokerStartupRecord | None:
    """Parse one complete authenticated broker record without accepting free-form data."""
    if not payload:
        return None
    if len(payload) > BROKER_STARTUP_RECORD_MAX_BYTES:
        raise RuntimeError("containment broker readiness payload exceeded its bound")
    if not payload.endswith(b"\n"):
        return None
    if b"\n" in payload[:-1]:
        raise RuntimeError("containment broker readiness record was invalid")
    try:
        decoded = cast(
            object,
            json.loads(
                payload[:-1].decode("ascii"),
                object_pairs_hook=_reject_broker_duplicate_keys,
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
        raise RuntimeError("containment broker readiness record was invalid") from None
    if not isinstance(decoded, dict):
        raise RuntimeError("containment broker readiness record was invalid")
    raw_record = cast(dict[object, object], decoded)
    if set(raw_record) != {
        "schema_version",
        "token",
        "complete",
        "ready",
        "diagnostic",
    }:
        raise RuntimeError("containment broker readiness record was invalid")
    record = cast(dict[str, object], raw_record)
    token = record["token"]
    if (
        record["schema_version"] != BROKER_STARTUP_RECORD_SCHEMA
        or record["complete"] is not True
        or not isinstance(token, str)
        or not token.isascii()
        or not hmac.compare_digest(token, expected_token)
        or not isinstance(record["ready"], bool)
    ):
        raise RuntimeError("containment broker readiness record was invalid")
    if record["ready"] is True:
        if record["diagnostic"] is not None:
            raise RuntimeError("containment broker readiness record was invalid")
        return _BrokerStartupRecord(ready=True, diagnostic=None)
    raw_diagnostic = record["diagnostic"]
    if not isinstance(raw_diagnostic, dict):
        raise RuntimeError("containment broker readiness record was invalid")
    diagnostic_record = cast(dict[object, object], raw_diagnostic)
    if set(diagnostic_record) != {
        "stage",
        "code",
        "exception_type",
        "errno",
        "child_return_code",
    }:
        raise RuntimeError("containment broker readiness record was invalid")
    diagnostic_values = cast(dict[str, object], diagnostic_record)
    stage = diagnostic_values["stage"]
    code = diagnostic_values["code"]
    exception_type = diagnostic_values["exception_type"]
    error_number = diagnostic_values["errno"]
    child_return_code = diagnostic_values["child_return_code"]
    if (
        not isinstance(stage, str)
        or not isinstance(code, str)
        or stage not in _BROKER_STARTUP_STAGE_CODES
        or code not in _BROKER_STARTUP_STAGE_CODES[stage]
        or (
            exception_type is not None
            and (
                not isinstance(exception_type, str)
                or _BROKER_EXCEPTION_TYPE_PATTERN.fullmatch(exception_type) is None
            )
        )
        or not _is_bounded_broker_integer(error_number, minimum=0, maximum=65535)
        or not _is_bounded_broker_integer(
            child_return_code,
            minimum=-(2**31),
            maximum=2**31 - 1,
        )
    ):
        raise RuntimeError("containment broker readiness record was invalid")
    return _BrokerStartupRecord(
        ready=False,
        diagnostic=_BrokerStartupDiagnostic(
            stage=stage,
            code=code,
            exception_type=exception_type,
            error_number=cast(int | None, error_number),
            child_return_code=cast(int | None, child_return_code),
        ),
    )


def _is_bounded_broker_integer(value: object, *, minimum: int, maximum: int) -> bool:
    """Accept null or a non-boolean integer inside a fixed diagnostic range."""
    return value is None or (
        isinstance(value, int) and not isinstance(value, bool) and minimum <= value <= maximum
    )


def _await_broker_readiness(
    process: subprocess.Popen[str],
    readiness: _BrokerReadiness,
    *,
    startup_deadline: float | None = None,
) -> None:
    """Wait until the released child has consumed credentials or fail boundedly."""
    descriptor = readiness.descriptor
    if descriptor is None:
        raise RuntimeError("containment broker readiness channel was already closed")
    deadline = (
        startup_deadline
        if startup_deadline is not None
        else time.monotonic() + _pc.BROKER_READY_TIMEOUT_SECONDS
    )

    def read_record() -> _BrokerStartupRecord | None:
        try:
            opened = os.fstat(descriptor)
            if (
                int(opened.st_dev) != readiness.device
                or int(opened.st_ino) != readiness.inode
                or int(opened.st_uid) != readiness.owner
                or int(opened.st_nlink) != readiness.link_count
                or stat_module.S_IMODE(opened.st_mode) != readiness.mode
            ):
                raise RuntimeError("containment broker readiness identity changed")
            os.lseek(descriptor, 0, os.SEEK_SET)
            observed = os.read(descriptor, BROKER_STARTUP_RECORD_MAX_BYTES + 1)
        except OSError as exc:
            raise RuntimeError("containment broker readiness channel failed") from exc
        return _parse_broker_startup_record(observed, expected_token=readiness.token)

    def accept_record(record: _BrokerStartupRecord | None) -> bool:
        if record is None:
            return False
        if record.ready:
            return True
        diagnostic = record.diagnostic
        if diagnostic is None:
            raise RuntimeError("containment broker readiness record was invalid")
        raise RuntimeError(diagnostic.safe_message())

    try:
        while time.monotonic() < deadline:
            if accept_record(read_record()):
                return
            if process.poll() is not None:
                # The broker can publish its terminal record immediately before
                # exiting. Re-read after observing exit to close that race.
                if accept_record(read_record()):
                    return
                raise RuntimeError(
                    "containment broker exited before child readiness "
                    f"with return code {process.returncode}"
                )
            time.sleep(_pc.POLL_SECONDS)
        raise RuntimeError("containment broker child readiness timed out")
    finally:
        _pc._remove_broker_readiness(readiness)


def _remove_broker_readiness(readiness: _BrokerReadiness) -> None:
    descriptor = readiness.descriptor
    if descriptor is None:
        return
    readiness.descriptor = None
    deadline = time.monotonic() + DISCOVERY_TIMEOUT_SECONDS
    try:
        while True:
            try:
                path_stat = os.stat(readiness.path, follow_symlinks=False)
                if (
                    int(path_stat.st_dev) != readiness.device
                    or int(path_stat.st_ino) != readiness.inode
                    or int(path_stat.st_uid) != readiness.owner
                    or stat_module.S_IMODE(path_stat.st_mode) != readiness.mode
                ):
                    raise RuntimeError("refused to remove a replaced broker readiness path")
                if os.name == "nt" and descriptor >= 0:
                    os.close(descriptor)
                    descriptor = -1
                readiness.path.unlink()
                return
            except FileNotFoundError as exc:
                raise RuntimeError("broker readiness path disappeared before cleanup") from exc
            except PermissionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(_pc.POLL_SECONDS)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
