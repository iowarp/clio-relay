"""The stdlib-only source text executed inside each containment broker.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).
`_BROKER_SCRIPT` is handed to a fresh `python -I -S -u -c` invocation by
`process_containment_broker._spawn_broker` and
`process_containment_systemd_scope._spawn_linux_systemd_scope`; it must stay
importable with no dependency beyond the standard library, since it runs
before the broker process has any relay module on its path except the one
directory `sys.argv[4]` points at.
"""

from __future__ import annotations

_BROKER_SCRIPT = r"""
import base64
import binascii
import errno
import json
import os
import select
import stat
import subprocess
import sys
import time

MAX_CREDENTIAL_BYTES = 16 * 1024
MAX_STDIN_BYTES = 4 * 1024 * 1024
MAX_SETUP_BYTES = 6 * 1024 * 1024
MAX_STARTUP_RECORD_BYTES = 1024
HANDSHAKE_TIMEOUT_SECONDS = 5.0
FD_ENV = "CLIO_RELAY_BROKER_CREDENTIAL_FD"
READY_FD_ENV = "CLIO_RELAY_BROKER_READY_FD"
STARTUP_RECORD_SCHEMA = "clio-relay.broker-startup.v1"

# Import only the relay's exact stdlib-only containment module before reading
# the setup pipe. The module root is a non-secret parent-supplied path.
module_root = sys.argv[4]
if not os.path.isabs(module_root) or not os.path.isdir(module_root):
    raise SystemExit(125)
sys.path.insert(0, module_root)
try:
    from clio_relay.process_containment import enforce_linux_secret_memory_gate
except BaseException:
    raise SystemExit(125) from None
if sys.platform.startswith("linux"):
    try:
        enforce_linux_secret_memory_gate()
    except BaseException:
        raise SystemExit(125) from None


def publish_record(token, ready, diagnostic):
    record = {
        "schema_version": STARTUP_RECORD_SCHEMA,
        "token": token,
        "complete": True,
        "ready": ready,
        "diagnostic": diagnostic,
    }
    payload = (
        json.dumps(
            record,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        + b"\n"
    )
    if not token or len(token) > 128 or len(payload) > MAX_STARTUP_RECORD_BYTES:
        raise RuntimeError("broker readiness payload was invalid")
    flags = (
        os.O_WRONLY
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    descriptor = os.open(sys.argv[2], flags)
    try:
        opened = os.fstat(descriptor)
        expected = json.loads(sys.argv[3])
        observed = {
            "device": int(opened.st_dev),
            "inode": int(opened.st_ino),
            "owner": int(opened.st_uid),
            "link_count": int(opened.st_nlink),
            "mode": int(opened.st_mode & 0o7777),
        }
        if (
            not isinstance(expected, dict)
            or observed != expected
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
        ):
            raise RuntimeError("broker readiness file identity changed")
        os.ftruncate(descriptor, 0)
        if os.write(descriptor, payload) != len(payload):
            raise RuntimeError("broker readiness write was incomplete")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def safe_exception_type(error):
    name = type(error).__name__
    if (
        not name
        or len(name) > 64
        or not name[0].isalpha()
        or not all(character.isalnum() or character == "_" for character in name)
    ):
        return None
    return name


def safe_integer(value, *, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < minimum or value > maximum:
        return None
    return value


def publish_failure(token, stage, code, error, child_return_code):
    if token is None:
        return
    diagnostic = {
        "stage": stage,
        "code": code,
        "exception_type": safe_exception_type(error),
        "errno": safe_integer(getattr(error, "errno", None), minimum=0, maximum=65535),
        "child_return_code": safe_integer(
            child_return_code,
            minimum=-(2**31),
            maximum=2**31 - 1,
        ),
    }
    try:
        publish_record(token, False, diagnostic)
    except BaseException:
        pass


def close_fd(descriptor):
    if descriptor is None:
        return
    try:
        os.close(descriptor)
    except OSError:
        pass


readiness_token = None
startup_stage = "setup_parse"
startup_code = "internal_error"
try:
    raw_message = sys.stdin.buffer.readline(MAX_SETUP_BYTES + 1)
    if not raw_message.endswith(b"\n") or len(raw_message) > MAX_SETUP_BYTES:
        raise ValueError
    message = json.loads(raw_message)
    if not isinstance(message, dict) or message.get("release") is not True:
        raise ValueError
    candidate_token = message.get("readiness_token")
    if (
        not isinstance(candidate_token, str)
        or not candidate_token.isascii()
        or not candidate_token
        or len(candidate_token) > 128
    ):
        raise ValueError
    readiness_token = candidate_token
    command = json.loads(sys.argv[1])
    credential = message.get("credential")
    stdin_payload_encoded = message.get("stdin_payload")
    interactive_stdin = message.get("interactive_stdin")
    target_environment = message.get("target_environment")
    if credential is not None and (os.name == "nt" or not isinstance(credential, str)):
        raise ValueError
    if stdin_payload_encoded is not None and not isinstance(stdin_payload_encoded, str):
        raise ValueError
    if not isinstance(interactive_stdin, bool):
        raise ValueError
    if interactive_stdin and stdin_payload_encoded is not None:
        raise ValueError
    if target_environment is not None:
        if os.name != "nt" or credential is not None or not isinstance(target_environment, dict):
            raise ValueError
        if not target_environment:
            raise ValueError
        for environment_name, environment_value in target_environment.items():
            if (
                not isinstance(environment_name, str)
                or not environment_name
                or "=" in environment_name
                or "\x00" in environment_name
                or not isinstance(environment_value, str)
                or "\x00" in environment_value
            ):
                raise ValueError
    stdin_payload = (
        None
        if stdin_payload_encoded is None
        else base64.b64decode(stdin_payload_encoded.encode("ascii"), validate=True)
    )
    if stdin_payload is not None and len(stdin_payload) > MAX_STDIN_BYTES:
        raise ValueError
except BaseException as error:
    publish_failure(readiness_token, startup_stage, startup_code, error, None)
    raise SystemExit(125) from None

read_fd = None
write_fd = None
ready_read_fd = None
ready_write_fd = None
process = None
try:
    popen_kwargs = {}
    if target_environment is not None:
        child_env = os.environ.copy()
        child_env.update(target_environment)
        popen_kwargs["env"] = child_env
    if credential is not None:
        credential_bytes = credential.encode("utf-8")
        if len(credential_bytes) > MAX_CREDENTIAL_BYTES:
            raise RuntimeError("broker credential exceeded its byte limit")
        read_fd, write_fd = os.pipe()
        ready_read_fd, ready_write_fd = os.pipe()
        child_env = os.environ.copy()
        child_env[FD_ENV] = str(read_fd)
        child_env[READY_FD_ENV] = str(ready_write_fd)
        popen_kwargs = {
            "env": child_env,
            "pass_fds": (read_fd, ready_write_fd),
        }
    startup_stage = "child_spawn"
    startup_code = "internal_error"
    try:
        process = subprocess.Popen(
            command,
            **popen_kwargs,
            stdin=(
                subprocess.PIPE
                if stdin_payload is not None or interactive_stdin
                else subprocess.DEVNULL
            ),
        )
    except FileNotFoundError:
        startup_code = "executable_not_found"
        raise
    except PermissionError:
        startup_code = "executable_not_permitted"
        raise
    except OSError as error:
        if error.errno == errno.ENOEXEC or getattr(error, "winerror", None) == 193:
            startup_code = "executable_format_invalid"
        raise
    close_fd(read_fd)
    read_fd = None
    close_fd(ready_write_fd)
    ready_write_fd = None
    if write_fd is not None:
        startup_stage = "credential_write"
        startup_code = "internal_error"
        os.set_blocking(write_fd, False)
        view = memoryview(credential_bytes)
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        while view:
            if process.poll() is not None:
                startup_code = "child_exited"
                raise RuntimeError("credential consumer exited before broker readiness")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                startup_code = "credential_timeout"
                raise RuntimeError("broker credential write timed out")
            _, writable, _ = select.select([], [write_fd], [], remaining)
            if not writable:
                startup_code = "credential_timeout"
                raise RuntimeError("broker credential write timed out")
            try:
                written = os.write(write_fd, view)
            except BlockingIOError:
                continue
            if written <= 0:
                raise RuntimeError("broker credential write made no progress")
            view = view[written:]
        close_fd(write_fd)
        write_fd = None
        startup_stage = "credential_ack"
        startup_code = "internal_error"
        os.set_blocking(ready_read_fd, False)
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        while True:
            if process.poll() is not None:
                startup_code = "child_exited"
                raise RuntimeError("credential consumer exited before broker readiness")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                startup_code = "ack_timeout"
                raise RuntimeError("broker readiness acknowledgement timed out")
            readable, _, _ = select.select([ready_read_fd], [], [], remaining)
            if not readable:
                startup_code = "ack_timeout"
                raise RuntimeError("broker readiness acknowledgement timed out")
            try:
                acknowledgement = os.read(ready_read_fd, 2)
            except BlockingIOError:
                continue
            if acknowledgement != b"1":
                startup_code = "ack_mismatch"
                raise RuntimeError("broker readiness acknowledgement did not match")
            break
        close_fd(ready_read_fd)
        ready_read_fd = None
    if stdin_payload is not None:
        startup_stage = "stdin_forward"
        startup_code = "internal_error"
        if process.stdin is None:
            if process.poll() is not None:
                startup_code = "child_exited"
            raise RuntimeError("stdin consumer did not expose its input pipe")
        process.stdin.write(stdin_payload)
        process.stdin.close()
    elif interactive_stdin:
        startup_stage = "readiness_publish"
        startup_code = "internal_error"
        publish_record(readiness_token, True, None)
        startup_stage = "stdin_forward"
        if process.stdin is None:
            if process.poll() is not None:
                startup_code = "child_exited"
            raise RuntimeError("interactive stdin consumer did not expose its input pipe")
        while True:
            chunk = os.read(sys.stdin.fileno(), 64 * 1024)
            if not chunk:
                break
            process.stdin.write(chunk)
            process.stdin.flush()
        process.stdin.close()
    if not interactive_stdin:
        startup_stage = "readiness_publish"
        startup_code = "internal_error"
        publish_record(readiness_token, True, None)
except BaseException as error:
    close_fd(read_fd)
    close_fd(write_fd)
    close_fd(ready_read_fd)
    close_fd(ready_write_fd)
    if process is not None and process.poll() is None:
        try:
            process.kill()
            process.wait()
        except BaseException:
            pass
    child_return_code = None if process is None else process.poll()
    if startup_stage == "stdin_forward" and child_return_code is not None:
        startup_code = "child_exited"
    publish_failure(
        readiness_token,
        startup_stage,
        startup_code,
        error,
        child_return_code,
    )
    raise SystemExit(125) from None
try:
    return_code = process.wait()
except BaseException:
    raise SystemExit(125) from None
raise SystemExit(return_code)
"""
