"""Autonomous installation helpers for desktop and cluster targets."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from importlib import resources
from pathlib import Path, PurePosixPath
from typing import cast
from urllib.request import urlretrieve
from uuid import uuid4

from clio_relay import __version__
from clio_relay.deployment import endpoint_user_service_name
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)
from clio_relay.remote_values import render_remote_shell_path
from clio_relay.worker_lifetime_lock import (
    WORKER_LIFETIME_GUARD_FD_ENV,
    WORKER_LIFETIME_LOCK_NAME,
)

FRP_VERSION = "0.69.1"
FRP_WINDOWS_AMD64_SHA256 = "829ac915f8655d4d4e021b8db61b46c3445205ed80d32b04cda7fa89d87c46e0"
FRP_LINUX_AMD64_SHA256 = "7be257b72dbbc60bcb3e0e25a5afd1dfac7b63f897084864d3c956dd3d5674e1"
FRPC_LINUX_AMD64_SHA256 = "142f447f43fef286acc8da8a6852dda80631db631d604b2e63634b2db4d6848c"
FRPS_LINUX_AMD64_SHA256 = "68d2908bb73fe7a03c29d9227d2acc2104bff3fea6b1cece0b8388c1a0660442"
FRPC_WINDOWS_AMD64_SHA256 = "1d1c4f988b1808bb458a4ba38f00359052d14636023a504520e0afed127d636d"
FRPS_WINDOWS_AMD64_SHA256 = "bd463ef89370abc6973c86258256fa65776baa5f515ef91ebeabd6070b92e229"
UV_VERSION = "0.11.28"
UV_LINUX_AMD64_SHA256 = "e490a6464492183c5d4534a5527fb4440f7f2bb2f228162ad7e4afe076dc0224"
JARVIS_UTIL_COMMIT = "c91bfdc9bba802e4b03bfb1babe614ffa3e09644"
JARVIS_CD_VERSION = "1.3.4"
JARVIS_CD_WHEEL_FILENAME = f"jarvis_cd-{JARVIS_CD_VERSION}-py3-none-any.whl"
JARVIS_CD_WHEEL_URL = (
    "https://github.com/grc-iit/jarvis-cd/releases/download/"
    f"v{JARVIS_CD_VERSION}/{JARVIS_CD_WHEEL_FILENAME}"
)
JARVIS_CD_WHEEL_SHA256 = "960debefd73b7789a141b5d02e89776fa10317144c357d791e0b843d730e4275"
DEFAULT_REMOTE_CORE_DIR = "$HOME/.local/share/clio-relay/core"
DEFAULT_REMOTE_SPOOL_DIR = "$HOME/.local/share/clio-relay/spool"

_WORKER_WRITER_PROOF_PYTHON = r'''from __future__ import annotations

import errno
import json
import os
import posixpath
import socket
import stat
import sys
from pathlib import Path

MAX_PROC_ENTRIES = 1_000_000
MAX_OWNED_PROCESSES = 65_536
MAX_PROC_FILE_BYTES = 1_048_576
MAX_ENDPOINT_RECORDS = 10_000
MAX_ENDPOINT_TOTAL_BYTES = 64 * 1_048_576


def fail(message: str) -> "NoReturn":
    """Stop the bootstrap because writer exclusion could not be proved."""
    raise SystemExit(f"relay worker writer proof failed: {message}")


def vanished(error: OSError) -> bool:
    """Return whether a proc entry disappeared during inspection."""
    return error.errno in {errno.ENOENT, errno.ESRCH}


def read_bounded(path: Path) -> bytes | None:
    """Read one proc pseudo-file without accepting an unbounded value."""
    try:
        with path.open("rb") as stream:
            value = stream.read(MAX_PROC_FILE_BYTES + 1)
    except OSError as error:
        if vanished(error):
            return None
        fail(f"cannot inspect {path}: {error}")
    if len(value) > MAX_PROC_FILE_BYTES:
        fail(f"{path} exceeds the bounded inspection size")
    return value


def decode_nul_values(value: bytes) -> list[str]:
    """Decode an exact NUL-delimited proc value with filesystem semantics."""
    return [os.fsdecode(part) for part in value.split(b"\0") if part]


def relay_process_invocation(argv: list[str]) -> list[str] | None:
    """Return command arguments for one exact installed relay invocation."""
    for index, argument in enumerate(argv):
        if os.path.basename(argument) == "clio-relay":
            return argv[index + 1 :]
    return None


def option_value(arguments: list[str], name: str) -> str | None:
    """Return the last exact Click-style option value before an option terminator."""
    found: str | None = None
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == "--":
            break
        if argument == name:
            if index + 1 >= len(arguments):
                return None
            found = arguments[index + 1]
            index += 2
            continue
        prefix = f"{name}="
        if argument.startswith(prefix):
            found = argument[len(prefix) :]
        index += 1
    return found


def environment(value: bytes) -> dict[str, str]:
    """Parse the process environment without substring or shell matching."""
    parsed: dict[str, str] = {}
    for item in value.split(b"\0"):
        if b"=" not in item:
            continue
        key, raw_value = item.split(b"=", 1)
        parsed[os.fsdecode(key)] = os.fsdecode(raw_value)
    return parsed


def process_cwd(process: Path) -> str | None:
    """Read a process working directory, accounting for an ordinary exit race."""
    try:
        return os.readlink(process / "cwd")
    except OSError as error:
        if vanished(error):
            return None
        fail(f"cannot inspect {process / 'cwd'}: {error}")


def target_home(process_environment: dict[str, str], uid: int | None) -> str:
    """Resolve Path.home() as the inspected process would resolve it."""
    if "HOME" in process_environment:
        # posixpath.expanduser maps an explicitly empty HOME to the filesystem
        # root for both '~' and '~/...'; it does not fall back to passwd.
        return process_environment["HOME"].rstrip("/") or "/"
    if uid is None:
        fail("an inspected process has no HOME and no numeric uid")
    try:
        import pwd

        return pwd.getpwuid(uid).pw_dir
    except (ImportError, KeyError, OSError) as error:
        fail(f"cannot resolve the inspected process home directory: {error}")


def path_is_absolute(value: str) -> bool:
    """Recognize target Linux roots even when the proof is tested on Windows."""
    return value.startswith("/") or os.path.isabs(value)


def path_join(base: str, *parts: str) -> str:
    """Join target Linux paths without inheriting a Windows test host's flavor."""
    if base.startswith("/"):
        return posixpath.join(base, *parts)
    return os.path.join(base, *parts)


def expand_user(value: str, home: str) -> str:
    """Expand a user path with the inspected process's HOME semantics."""
    if value == "~":
        return home
    if value.startswith("~/"):
        return path_join(home, value[2:])
    if not value.startswith("~"):
        return value
    user, separator, suffix = value[1:].partition("/")
    try:
        import pwd

        user_home = pwd.getpwnam(user).pw_dir
    except (ImportError, KeyError, OSError) as error:
        fail(f"cannot expand inspected core directory {value!r}: {error}")
    return path_join(user_home, suffix) if separator else user_home


def canonical(value: str, *, cwd: str | None = None) -> str:
    """Return a non-strict canonical absolute path."""
    if not path_is_absolute(value):
        if cwd is None:
            fail(f"relative path {value!r} has no inspected working directory")
        value = path_join(cwd, value)
    if os.name == "nt" and value.startswith("/"):
        # The embedded proof runs on Linux in production.  Python 3.13 changed
        # ntpath.isabs('/core') to false, so use the target path flavor when
        # exercising the exact source in the Windows CI matrix.
        return posixpath.normpath(value)
    return os.path.realpath(os.path.abspath(value))


def process_core_candidates(
    process: Path,
    process_environment: dict[str, str],
    uid: int | None,
) -> set[str] | None:
    """Reconstruct every core path the live endpoint could have selected at startup."""
    home = target_home(process_environment, uid)
    configured = process_environment.get("CLIO_RELAY_CORE_DIR")
    if configured:
        expanded = expand_user(configured, home)
        cwd = None if path_is_absolute(expanded) else process_cwd(process)
        if cwd is None and not path_is_absolute(expanded):
            return None
        return {canonical(expanded, cwd=cwd)}

    cwd = process_cwd(process)
    if cwd is None:
        return None
    # RelaySettings selects the bootstrap directory when it exists, otherwise
    # its cwd-relative compatibility directory.  /proc cannot prove which one
    # existed at process startup, so both are safety-relevant candidates.
    return {
        canonical(path_join(home, ".local", "share", "clio-relay", "core")),
        canonical(path_join(".clio-relay", "core"), cwd=cwd),
    }


def endpoint_record_pids(
    expected_core: str,
) -> dict[int, list[tuple[str, dict[str, object] | None]]]:
    """Read bounded worker PID evidence from the exact core's endpoint records."""
    endpoint_directory = Path(expected_core) / "endpoints"
    try:
        directory_stat = os.lstat(endpoint_directory)
    except FileNotFoundError:
        return {}
    except OSError as error:
        fail(f"cannot inspect endpoint evidence directory {endpoint_directory}: {error}")
    if not stat.S_ISDIR(directory_stat.st_mode) or stat.S_ISLNK(directory_stat.st_mode):
        fail(f"endpoint evidence path is not a real directory: {endpoint_directory}")
    current_uid = os.getuid() if hasattr(os, "getuid") else None
    if current_uid is not None and directory_stat.st_uid != current_uid:
        fail(f"endpoint evidence directory has a foreign owner: {endpoint_directory}")

    records: dict[int, list[tuple[str, dict[str, object] | None]]] = {}
    record_count = 0
    total_bytes = 0
    try:
        entries = os.scandir(endpoint_directory)
    except OSError as error:
        fail(f"cannot enumerate endpoint evidence {endpoint_directory}: {error}")
    with entries:
        for entry in entries:
            if not entry.name.endswith(".json"):
                continue
            record_count += 1
            if record_count > MAX_ENDPOINT_RECORDS:
                fail("endpoint evidence exceeds the bounded record count")
            path = Path(entry.path)
            try:
                before = os.lstat(path)
            except OSError as error:
                if vanished(error):
                    fail(f"endpoint evidence changed during inspection: {path}")
                fail(f"cannot inspect endpoint evidence {path}: {error}")
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or (current_uid is not None and before.st_uid != current_uid)
            ):
                fail(f"endpoint evidence is not one owned regular file: {path}")
            value = read_bounded(path)
            if value is None:
                fail(f"endpoint evidence disappeared during inspection: {path}")
            total_bytes += len(value)
            if total_bytes > MAX_ENDPOINT_TOTAL_BYTES:
                fail("endpoint evidence exceeds the bounded aggregate size")
            try:
                after = os.lstat(path)
            except OSError as error:
                fail(f"endpoint evidence changed during inspection: {path}: {error}")
            if (
                before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
            ):
                fail(f"endpoint evidence changed during inspection: {path}")
            try:
                document = json.loads(value)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                fail(f"endpoint evidence is not valid JSON: {path}: {error}")
            if not isinstance(document, dict):
                fail(f"endpoint evidence is not an object: {path}")
            endpoint_id = document.get("endpoint_id")
            role = document.get("role")
            hostname = document.get("hostname")
            pid = document.get("pid")
            cluster = document.get("cluster")
            metadata = document.get("metadata")
            if not isinstance(endpoint_id, str) or path.stem != endpoint_id:
                fail(f"endpoint evidence identity does not match its filename: {path}")
            if role != "worker":
                continue
            if hostname != socket.gethostname():
                continue
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                fail(f"worker endpoint evidence has an invalid pid: {path}")
            if not isinstance(cluster, str) or not cluster:
                fail(f"worker endpoint evidence has no cluster: {path}")
            process_identity: dict[str, object] | None = None
            raw_identity = metadata.get("process_identity") if isinstance(metadata, dict) else None
            if isinstance(raw_identity, dict):
                identity_start_ticks = raw_identity.get("start_ticks")
                identity_uid = raw_identity.get("uid")
                identity_pid = raw_identity.get("pid")
                if (
                    raw_identity.get("schema_version") == "clio-relay.process-identity.v1"
                    and isinstance(raw_identity.get("boot_id"), str)
                    and bool(raw_identity["boot_id"])
                    and len(raw_identity["boot_id"]) <= 128
                    and isinstance(identity_start_ticks, int)
                    and not isinstance(identity_start_ticks, bool)
                    and identity_start_ticks > 0
                    and isinstance(identity_uid, int)
                    and not isinstance(identity_uid, bool)
                    and identity_uid >= 0
                    and isinstance(identity_pid, int)
                    and not isinstance(identity_pid, bool)
                    and identity_pid == pid
                ):
                    process_identity = raw_identity
                # A malformed identity is deliberately treated as legacy PID
                # evidence.  Only a complete exact identity may dismiss PID
                # reuse; malformed metadata can never weaken writer proof.
            records.setdefault(pid, []).append((cluster, process_identity))
    return records


def proc_boot_id(proc_root: Path) -> str:
    """Read the exact Linux boot identity used by new endpoint records."""
    value = read_bounded(proc_root / "sys" / "kernel" / "random" / "boot_id")
    if value is None:
        fail("cannot read Linux boot identity for endpoint evidence")
    try:
        boot_id = value.decode("ascii").strip()
    except UnicodeDecodeError as error:
        fail(f"Linux boot identity is invalid: {error}")
    if not boot_id or len(boot_id) > 128:
        fail("Linux boot identity is empty or oversized")
    return boot_id


def process_identity_matches(
    raw_stat: bytes,
    *,
    process_pid: int,
    process_uid: int,
    boot_id: str,
    identity: dict[str, object] | None,
) -> bool:
    """Match new exact identities; conservatively retain legacy PID evidence."""
    closing_parenthesis = raw_stat.rfind(b")")
    fields = raw_stat[closing_parenthesis + 1 :].split()
    if closing_parenthesis < 0 or len(fields) <= 19:
        fail("cannot parse live endpoint process generation")
    if fields[0] == b"Z":
        return False
    if identity is None:
        return True
    try:
        start_ticks = int(fields[19])
    except ValueError as error:
        fail(f"cannot parse live endpoint start ticks: {error}")
    return (
        identity.get("schema_version") == "clio-relay.process-identity.v1"
        and identity.get("boot_id") == boot_id
        and identity.get("start_ticks") == start_ticks
        and identity.get("uid") == process_uid
        and identity.get("pid") == process_pid
    )


def prove_no_writer(cluster: str, expected_core: str, proc_root: Path) -> None:
    """Fail if a same-user long-lived process can write the configured core queue."""
    expected = canonical(expected_core)
    current_uid = os.getuid() if hasattr(os, "getuid") else None
    endpoint_pids = endpoint_record_pids(expected)
    boot_id = proc_boot_id(proc_root) if endpoint_pids else ""
    total_entries = 0
    owned_processes = 0
    try:
        entries = os.scandir(proc_root)
    except OSError as error:
        fail(f"cannot enumerate {proc_root}: {error}")
    with entries:
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            total_entries += 1
            if total_entries > MAX_PROC_ENTRIES:
                fail(f"{proc_root} exceeds the bounded process-entry count")
            try:
                process_uid = entry.stat(follow_symlinks=False).st_uid
            except OSError as error:
                if vanished(error):
                    continue
                fail(f"cannot identify process owner for {entry.path}: {error}")
            if current_uid is not None and process_uid != current_uid:
                continue
            owned_processes += 1
            if owned_processes > MAX_OWNED_PROCESSES:
                fail("same-user process count exceeds the bounded inspection limit")
            process = Path(entry.path)
            endpoint_evidence = endpoint_pids.get(int(entry.name))
            if endpoint_evidence is not None:
                raw_stat = read_bounded(process / "stat")
                if raw_stat is None:
                    continue
                for endpoint_cluster, process_identity in endpoint_evidence:
                    if process_identity_matches(
                        raw_stat,
                        process_pid=int(entry.name),
                        process_uid=process_uid,
                        boot_id=boot_id,
                        identity=process_identity,
                    ):
                        fail(
                            f"live endpoint pid={entry.name} has exact-core record "
                            f"cluster={endpoint_cluster!r} while bootstrapping cluster={cluster!r}"
                        )
            raw_cmdline = read_bounded(process / "cmdline")
            if raw_cmdline is None:
                continue
            argv = decode_nul_values(raw_cmdline)
            command = relay_process_invocation(argv)
            if command is None:
                continue
            writer_kind = "clio-relay"
            options = command
            if command[:2] == ["endpoint", "start"]:
                writer_kind = "endpoint"
                options = command[2:]
            elif command[:2] == ["api", "start"]:
                writer_kind = "api"
                options = command[2:]
            elif command[:1] == ["mcp-server"]:
                writer_kind = "mcp-server"
                options = command[1:]
            process_cluster = option_value(options, "--cluster")
            raw_environment = read_bounded(process / "environ")
            if raw_environment is None:
                continue
            candidates = process_core_candidates(
                process,
                environment(raw_environment),
                process_uid,
            )
            if candidates is None:
                continue
            if expected in candidates:
                if (
                    writer_kind == "endpoint"
                    and option_value(options, "--role") == "worker"
                    and process_cluster is not None
                ):
                    fail(
                        f"live endpoint pid={entry.name} still owns "
                        f"cluster={process_cluster!r} core={expected!r} "
                        f"while bootstrapping cluster={cluster!r}"
                    )
                if writer_kind in {"api", "mcp-server"}:
                    fail(
                        f"live {writer_kind} writer pid={entry.name} still owns "
                        f"core={expected!r} while bootstrapping cluster={cluster!r}; "
                        "stop or detach it before bootstrap"
                    )
                fail(
                    f"live clio-relay process pid={entry.name} still owns "
                    f"core={expected!r} while bootstrapping cluster={cluster!r}; "
                    "wait for it to exit before bootstrap"
                )
    print("relay_worker_writer_proof=clear")


if len(sys.argv) != 4:
    fail("writer proof requires cluster, canonical core, and proc root")
prove_no_writer(sys.argv[1], sys.argv[2], Path(sys.argv[3]))
'''

_WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON = r'''from __future__ import annotations

import errno
import os
import stat
import sys
import time
from pathlib import Path


def fail(message: str) -> "NoReturn":
    """Fail inherited-FD validation with one operator-facing reason."""
    raise SystemExit(f"worker lifetime inherited-fd proof failed: {message}")


if len(sys.argv) != 4:
    fail("proof requires canonical core, inherited fd, and lock filename")
core_value, descriptor_value, lock_name = sys.argv[1:]
try:
    import fcntl

    descriptor = int(descriptor_value)
    if descriptor < 3:
        fail("inherited descriptor is invalid")
    core = Path(core_value)
    core = core.resolve(strict=True)
    core_stat = os.lstat(core)
    if not stat.S_ISDIR(core_stat.st_mode) or stat.S_ISLNK(core_stat.st_mode):
        fail("worker lifetime core is not a real directory")
    if core_stat.st_uid != os.getuid():
        fail("worker lifetime core has a foreign owner")
    if stat.S_IMODE(core_stat.st_mode) & 0o022:
        fail("worker lifetime core is writable by group or other users")

    lock_path = core / lock_name
    opened = os.fstat(descriptor)
    linked = os.lstat(lock_path)
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != os.getuid()
        or stat.S_IMODE(opened.st_mode) & 0o077
        or opened.st_dev != linked.st_dev
        or opened.st_ino != linked.st_ino
    ):
        fail("worker lifetime lock is not one owner-private regular file")

    deadline = time.monotonic() + 30.0
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError as lock_error:
            if lock_error.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            if time.monotonic() >= deadline:
                fail("timed out acquiring exclusive worker lifetime lock")
            time.sleep(0.05)
    print(f"relay_worker_lifetime_fd=exclusive:{descriptor}:{core}")
except Exception as error:
    fail(f"{type(error).__name__}: {error}")
'''


@dataclass(frozen=True)
class BootstrapArchive:
    """Remote bootstrap archive and relay install source."""

    archive: Path
    install_spec: str


def install_local_frp(destination: Path) -> Path:
    """Install frpc/frps for the local platform into a user-writable directory."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "windows" or machine not in {"amd64", "x86_64"}:
        raise ConfigurationError(f"local frp installer does not support {system}/{machine}")
    destination.mkdir(parents=True, exist_ok=True)
    frpc = destination / "frpc.exe"
    frps = destination / "frps.exe"
    if frpc.exists() and frps.exists():
        try:
            _assert_frp_pair(frpc, frps)
            return frpc
        except ConfigurationError:
            pass
    cleanup_errors = _remove_local_frp_pair(frpc, frps)
    if cleanup_errors:
        raise ConfigurationError(
            "could not remove an unverified existing frp installation: " + "; ".join(cleanup_errors)
        )
    try:
        with tempfile.TemporaryDirectory(
            prefix=".clio-relay-frp-",
            dir=destination.parent,
        ) as temporary_directory:
            staging = Path(temporary_directory) / "bin"
            staging.mkdir()
            _install_frp_from_release_archive(staging, FRP_VERSION)
            staged_frpc = staging / "frpc.exe"
            staged_frps = staging / "frps.exe"
            _assert_frp_pair(staged_frpc, staged_frps)
            shutil.copy2(staged_frpc, frpc)
            shutil.copy2(staged_frps, frps)
            _assert_frp_pair(frpc, frps)
        _assert_frp_pair(frpc, frps)
        return frpc
    except (ConfigurationError, OSError) as exc:
        cleanup_errors = _remove_local_frp_pair(frpc, frps)
        cleanup_detail = (
            ""
            if not cleanup_errors
            else "; unverified destination cleanup failed: " + "; ".join(cleanup_errors)
        )
        raise ConfigurationError(
            f"failed to install verified frp release: {exc}{cleanup_detail}"
        ) from exc


def _remove_local_frp_pair(frpc: Path, frps: Path) -> list[str]:
    """Remove both local frp executables and return any cleanup errors."""
    errors: list[str] = []
    for path in (frpc, frps):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            errors.append(f"{path}: {exc}")
    return errors


def _install_frp_from_release_archive(destination: Path, version: str) -> None:
    if version != FRP_VERSION:
        raise ConfigurationError(f"no pinned Windows checksum is registered for frp {version}")
    archive = destination.parent / f"frp_{version}_windows_amd64.zip"
    url = (
        "https://github.com/fatedier/frp/releases/download/"
        f"v{version}/frp_{version}_windows_amd64.zip"
    )
    urlretrieve(url, archive)
    observed = hashlib.sha256(archive.read_bytes()).hexdigest()
    if observed != FRP_WINDOWS_AMD64_SHA256:
        raise ConfigurationError(
            f"frp archive SHA-256 mismatch: {observed} != {FRP_WINDOWS_AMD64_SHA256}"
        )
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(destination.parent)
    extracted = destination.parent / f"frp_{version}_windows_amd64"
    shutil.copy2(extracted / "frpc.exe", destination / "frpc.exe")
    shutil.copy2(extracted / "frps.exe", destination / "frps.exe")


def bootstrap_cluster_over_ssh(
    *,
    bootstrap_profile: str,
    ssh_host: str,
    source_root: Path,
    cluster: str | None = None,
    core_dir: str = DEFAULT_REMOTE_CORE_DIR,
    spool_dir: str = DEFAULT_REMOTE_SPOOL_DIR,
    relay_wheel: Path | None = None,
    relay_artifact_sha256: str | None = None,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    frp_version: str = FRP_VERSION,
) -> list[str]:
    """Install relay dependencies and the current source tree on a cluster over SSH."""
    if bootstrap_profile != "linux-user":
        raise ConfigurationError(f"unsupported bootstrap profile: {bootstrap_profile}")
    if cluster is not None:
        endpoint_user_service_name(cluster)
    render_remote_shell_path(core_dir, field="core_dir")
    render_remote_shell_path(spool_dir, field="spool_dir")
    _validate_ssh_destination(ssh_host)
    if shutil.which("ssh") is None or shutil.which("scp") is None:
        raise ConfigurationError("ssh and scp are required for remote bootstrap")
    invocation_id = f"bootstrap_{uuid4().hex}"
    remote_root = f"/tmp/clio-relay-{invocation_id}"
    remote_archive = f"{remote_root}/clio-relay-head.tar"
    remote_script = f"{remote_root}/clio-relay-bootstrap.sh"
    remote_created = False
    primary_error: BaseException | None = None
    try:
        _run(
            [
                "ssh",
                ssh_host,
                "bash",
                "-c",
                f"umask 077; mkdir -- {shlex.quote(remote_root)}; "
                f"chmod 700 -- {shlex.quote(remote_root)}",
            ]
        )
        remote_created = True
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive = temp_path / "clio-relay-head.tar"
            script_path = temp_path / "clio-relay-bootstrap.sh"
            deployment = create_bootstrap_archive(
                source_root=source_root,
                archive=archive,
                relay_wheel=relay_wheel,
            )
            expected_relay_sha256 = relay_artifact_sha256
            if relay_wheel is not None:
                observed_relay_sha256 = hashlib.sha256(relay_wheel.read_bytes()).hexdigest()
                if (
                    expected_relay_sha256 is not None
                    and expected_relay_sha256 != observed_relay_sha256
                ):
                    raise ConfigurationError("relay bootstrap wheel SHA-256 does not match its pin")
                expected_relay_sha256 = observed_relay_sha256
            source_archive_sha256 = hashlib.sha256(deployment.archive.read_bytes()).hexdigest()
            _run(["scp", str(deployment.archive), f"{ssh_host}:{remote_archive}"])
            script_path.write_text(
                render_linux_user_bootstrap_script(
                    frp_version=frp_version,
                    cluster=cluster,
                    core_dir=core_dir,
                    spool_dir=spool_dir,
                    agent_adapter=agent_adapter,
                    agent_npm_package=agent_npm_package,
                    agent_npm_bin=agent_npm_bin,
                    agent_args=agent_args or [],
                    relay_install_spec=deployment.install_spec,
                    relay_artifact_sha256=expected_relay_sha256,
                    invocation_id=invocation_id,
                    source_archive=remote_archive,
                    source_archive_sha256=source_archive_sha256,
                ),
                encoding="utf-8",
                newline="\n",
            )
            _run(["scp", str(script_path), f"{ssh_host}:{remote_script}"])
        result = _run(["ssh", ssh_host, "bash", remote_script])
        receipt_result = _run(
            ["ssh", ssh_host, "cat", "$HOME/.local/share/clio-relay/bootstrap-receipt.json"]
        )
        try:
            raw_receipt = cast(object, json.loads(receipt_result.stdout))
        except json.JSONDecodeError as exc:
            raise RelayError(f"bootstrap receipt was not valid JSON: {exc}") from exc
        if not isinstance(raw_receipt, dict):
            raise RelayError("bootstrap receipt was not a JSON object")
        receipt = cast(dict[str, object], raw_receipt)
        if receipt.get("invocation_id") != invocation_id:
            raise RelayError("bootstrap receipt does not match the completed invocation")
        install_receipt_sha256 = receipt.get("install_receipt_sha256")
        worker_fence = receipt.get("worker_fence")
        expected_worker_service = (
            endpoint_user_service_name(cluster) if cluster is not None else None
        )
        managed_fence_valid = expected_worker_service is None
        if expected_worker_service is not None and isinstance(worker_fence, dict):
            typed_worker_fence = cast(dict[str, object], worker_fence)
            worker_was_active = typed_worker_fence.get("was_active")
            worker_restarted = typed_worker_fence.get("restarted")
            managed_fence_valid = (
                typed_worker_fence.get("managed") is True
                and typed_worker_fence.get("service_name") == expected_worker_service
                and typed_worker_fence.get("writer_proof") is True
                and typed_worker_fence.get("writer_recheck") is True
                and typed_worker_fence.get("lifetime_exclusive") is True
                and type(worker_was_active) is bool
                and type(worker_restarted) is bool
                and worker_restarted is worker_was_active
            )
        receipt_contract = {
            "schema_version": receipt.get("schema_version") == "clio-relay.bootstrap-receipt.v1",
            "bootstrap_profile": receipt.get("bootstrap_profile") == bootstrap_profile,
            "relay_install_spec": receipt.get("relay_install_spec") == deployment.install_spec,
            "install_receipt_sha256": isinstance(install_receipt_sha256, str)
            and len(install_receipt_sha256) == 64
            and all(character in "0123456789abcdef" for character in install_receipt_sha256),
            "completed_at": isinstance(receipt.get("completed_at"), str)
            and bool(receipt.get("completed_at")),
            "worker_fence": managed_fence_valid,
        }
        failed_contract = sorted(name for name, passed in receipt_contract.items() if not passed)
        if failed_contract:
            raise RelayError(f"bootstrap receipt contract failed: {failed_contract}")
        return [
            *result.stdout.splitlines(),
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
        ]
    except BaseException as error:
        primary_error = error
        raise
    finally:
        if remote_created:
            try:
                _run(["ssh", ssh_host, "rm", "-rf", "--", remote_root])
            except RelayError as cleanup_error:
                if primary_error is None:
                    raise
                primary_error.add_note(f"remote bootstrap staging cleanup failed: {cleanup_error}")


def package_source_root() -> Path:
    """Return the project root for editable installs, or the package root for wheels."""
    return Path(__file__).resolve().parents[2]


def _worker_writer_proof_shell(*, rendered_core_dir: str, success_variable: str) -> str:
    """Render one bounded legacy-writer proof against the configured core."""
    return "\n".join(
        [
            (
                'if ! python3 - "$WORKER_CLUSTER_NAME" '
                f"{rendered_core_dir} /proc <<'__CLIO_RELAY_WORKER_WRITER_PROOF__'"
            ),
            _WORKER_WRITER_PROOF_PYTHON.rstrip(),
            "__CLIO_RELAY_WORKER_WRITER_PROOF__",
            "then",
            ('  echo "cannot prove exclusive relay writer ownership for $WORKER_CLUSTER_NAME" >&2'),
            "  exit 1",
            "fi",
            f"{success_variable}=1",
        ]
    )


def _worker_upgrade_fence_script(
    cluster: str | None,
    *,
    rendered_core_dir: str,
) -> tuple[str, str, str, str]:
    """Render managed fencing, recheck, migration command, and restart step."""
    service_name = endpoint_user_service_name(cluster) if cluster is not None else ""
    declarations = "\n".join(
        [
            f"WORKER_SERVICE_NAME={shlex.quote(service_name)}",
            f"WORKER_CLUSTER_NAME={shlex.quote(cluster or '')}",
            "WORKER_WAS_ACTIVE=0",
            "WORKER_STOP_CONFIRMED=0",
            "WORKER_WRITER_PROOF=0",
            "WORKER_WRITER_RECHECK=0",
            "WORKER_LIFETIME_EXCLUSIVE=0",
            "WORKER_LIFETIME_GUARD_FD=",
            "WORKER_LIFETIME_LOCK_PATH=",
            "WORKER_RESTART_ATTEMPTED=0",
            "WORKER_RESTARTED=0",
        ]
    )
    if not service_name:
        return declarations, "", "clio-relay init", ""
    initial_proof = _worker_writer_proof_shell(
        rendered_core_dir=rendered_core_dir,
        success_variable="WORKER_WRITER_PROOF",
    )
    inherited_fd_check = "\n".join(
        [
            (
                f'python3 - {rendered_core_dir} "$WORKER_LIFETIME_GUARD_FD" '
                f"{shlex.quote(WORKER_LIFETIME_LOCK_NAME)} "
                "<<'__CLIO_RELAY_WORKER_LIFETIME_FD__'"
            ),
            _WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON.rstrip(),
            "__CLIO_RELAY_WORKER_LIFETIME_FD__",
        ]
    )
    recheck = "\n".join(
        [
            "bootstrap_require_worker_lifetime_guard",
            _worker_writer_proof_shell(
                rendered_core_dir=rendered_core_dir,
                success_variable="WORKER_WRITER_RECHECK",
            ),
        ]
    )
    fence = "\n".join(
        [
            declarations,
            "bootstrap_worker_fence_exit() {",
            "  status=$?",
            "  trap - EXIT",
            '  if [ -n "$WORKER_LIFETIME_GUARD_FD" ]; then',
            "    exec 8>&- || true",
            "  fi",
            (
                '  if [ "$status" -ne 0 ] && [ "$WORKER_WAS_ACTIVE" = "1" ]'
                ' && [ "$WORKER_RESTARTED" != "1" ]; then'
            ),
            '    if [ "$WORKER_RESTART_ATTEMPTED" = "1" ]; then',
            (
                '      if WORKER_EXIT_STATE="$(systemctl --user show '
                '"$WORKER_SERVICE_NAME" --property=ActiveState --value 2>/dev/null)"; then'
            ),
            '        case "$WORKER_EXIT_STATE" in',
            "          inactive|failed)",
            (
                '            echo "bootstrap failed; $WORKER_SERVICE_NAME is confirmed '
                '$WORKER_EXIT_STATE after restart attempt; operator action is required" >&2'
            ),
            "            ;;",
            "          *)",
            (
                '            echo "bootstrap failed after restart attempt; '
                "$WORKER_SERVICE_NAME state is $WORKER_EXIT_STATE and requires "
                'operator verification" >&2'
            ),
            "            ;;",
            "        esac",
            "      else",
            (
                '        echo "bootstrap failed after restart attempt; '
                '$WORKER_SERVICE_NAME state is unknown and requires operator verification" >&2'
            ),
            "      fi",
            '    elif [ "$WORKER_STOP_CONFIRMED" = "1" ]; then',
            (
                '      echo "bootstrap failed; $WORKER_SERVICE_NAME remains stopped; '
                'operator action is required" >&2'
            ),
            "    else",
            (
                '      echo "bootstrap failed while fencing $WORKER_SERVICE_NAME; '
                'worker state is unknown and requires operator verification" >&2'
            ),
            "    fi",
            "  fi",
            '  exit "$status"',
            "}",
            "trap bootstrap_worker_fence_exit EXIT",
            "command -v systemctl >/dev/null 2>&1 || {",
            '  echo "systemctl is required to fence the configured relay worker" >&2',
            "  exit 1",
            "}",
            (
                'if ! WORKER_LOAD_STATE="$(systemctl --user show "$WORKER_SERVICE_NAME" '
                '--property=LoadState --value)"; then'
            ),
            '  echo "cannot inspect relay worker unit: $WORKER_SERVICE_NAME" >&2',
            "  exit 1",
            "fi",
            (
                'if ! WORKER_ACTIVE_STATE="$(systemctl --user show "$WORKER_SERVICE_NAME" '
                '--property=ActiveState --value)"; then'
            ),
            '  echo "cannot inspect relay worker state: $WORKER_SERVICE_NAME" >&2',
            "  exit 1",
            "fi",
            'case "$WORKER_LOAD_STATE:$WORKER_ACTIVE_STATE" in',
            "  loaded:active|loaded:activating|loaded:reloading|loaded:deactivating)",
            "    WORKER_WAS_ACTIVE=1",
            '    systemctl --user stop "$WORKER_SERVICE_NAME"',
            (
                '    if ! WORKER_POST_STOP_STATE="$(systemctl --user show '
                '"$WORKER_SERVICE_NAME" --property=ActiveState --value)"; then'
            ),
            '      echo "cannot verify stopped relay worker: $WORKER_SERVICE_NAME" >&2',
            "      exit 1",
            "    fi",
            '    case "$WORKER_POST_STOP_STATE" in',
            "      inactive|failed) WORKER_STOP_CONFIRMED=1 ;;",
            "      *)",
            (
                '        echo "relay worker stop has unknown state '
                '$WORKER_POST_STOP_STATE: $WORKER_SERVICE_NAME" >&2'
            ),
            "        exit 1",
            "        ;;",
            "    esac",
            "    ;;",
            "  loaded:inactive|loaded:failed|masked:inactive|not-found:inactive) ;;",
            "  *)",
            (
                '    echo "refusing bootstrap with unknown relay worker state '
                '$WORKER_LOAD_STATE:$WORKER_ACTIVE_STATE: $WORKER_SERVICE_NAME" >&2'
            ),
            "    exit 1",
            "    ;;",
            "esac",
            initial_proof,
            f"mkdir -p -- {rendered_core_dir}",
            (
                f"WORKER_LIFETIME_LOCK_PATH={rendered_core_dir}/"
                f"{shlex.quote(WORKER_LIFETIME_LOCK_NAME)}"
            ),
            'exec 8<>"$WORKER_LIFETIME_LOCK_PATH"',
            "WORKER_LIFETIME_GUARD_FD=8",
            "bootstrap_require_worker_lifetime_guard() {",
            inherited_fd_check,
            "}",
            "bootstrap_require_worker_lifetime_guard",
            "WORKER_LIFETIME_EXCLUSIVE=1",
        ]
    )
    restart = "\n".join(
        [
            'if [ "$WORKER_WAS_ACTIVE" = "1" ]; then',
            "  bootstrap_require_worker_lifetime_guard",
            "  WORKER_RESTART_ATTEMPTED=1",
            '  systemctl --user start "$WORKER_SERVICE_NAME"',
            (
                '  if ! WORKER_POST_START_STATE="$(systemctl --user show '
                '"$WORKER_SERVICE_NAME" --property=ActiveState --value)"; then'
            ),
            '    echo "cannot verify restarted relay worker: $WORKER_SERVICE_NAME" >&2',
            "    exit 1",
            "  fi",
            '  if [ "$WORKER_POST_START_STATE" != "active" ]; then',
            (
                '    echo "relay worker did not become active '
                '($WORKER_POST_START_STATE): $WORKER_SERVICE_NAME" >&2'
            ),
            "    exit 1",
            "  fi",
            "  WORKER_RESTARTED=1",
            "fi",
        ]
    )
    return fence, recheck, "clio-relay init --migrate-legacy-output", restart


def render_linux_user_bootstrap_script(
    *,
    frp_version: str = FRP_VERSION,
    cluster: str | None = None,
    core_dir: str = DEFAULT_REMOTE_CORE_DIR,
    spool_dir: str = DEFAULT_REMOTE_SPOOL_DIR,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    relay_install_spec: str = "$DEST",
    relay_artifact_sha256: str | None = None,
    jarvis_mcp_install_spec: str | None = None,
    jarvis_mcp_artifact_sha256: str | None = None,
    invocation_id: str = "manual",
    source_archive: str = "/tmp/clio-relay-head.tar",
    source_archive_sha256: str | None = None,
) -> str:
    """Render the idempotent shell script used for the current Linux cluster bootstrap."""
    rendered_core_dir = render_remote_shell_path(core_dir, field="core_dir")
    rendered_spool_dir = render_remote_shell_path(spool_dir, field="spool_dir")
    worker_fence, worker_recheck, init_command, worker_restart = _worker_upgrade_fence_script(
        cluster,
        rendered_core_dir=rendered_core_dir,
    )
    rendered_agent_adapter = shlex.quote(agent_adapter)
    rendered_agent_args = shlex.quote(" ".join(agent_args or []))
    rendered_agent_npm_package = shlex.quote(agent_npm_package or "")
    rendered_agent_npm_bin = shlex.quote(agent_npm_bin or "")
    rendered_relay_install_spec = _render_relay_install_spec(relay_install_spec)
    source_archive_path = PurePosixPath(source_archive)
    if (
        not source_archive_path.is_absolute()
        or ".." in source_archive_path.parts
        or any(character in source_archive for character in "\x00\r\n")
    ):
        raise ConfigurationError("bootstrap source archive must be one safe absolute path")
    if source_archive_sha256 is not None and (
        len(source_archive_sha256) != 64
        or any(character not in "0123456789abcdef" for character in source_archive_sha256)
    ):
        raise ConfigurationError("bootstrap source archive SHA-256 must be lowercase hex")
    rendered_source_archive = shlex.quote(source_archive)
    rendered_source_archive_sha256 = shlex.quote(source_archive_sha256 or "")
    if relay_artifact_sha256 is not None and (
        len(relay_artifact_sha256) != 64
        or any(character not in "0123456789abcdef" for character in relay_artifact_sha256)
    ):
        raise ConfigurationError("relay bootstrap wheel SHA-256 must be lowercase hex")
    if relay_install_spec.endswith(".whl") and relay_artifact_sha256 is None:
        raise ConfigurationError("a relay bootstrap wheel requires its expected SHA-256")
    rendered_relay_artifact_sha256 = shlex.quote(relay_artifact_sha256 or "")
    if frp_version != FRP_VERSION:
        raise ConfigurationError(f"no pinned Linux checksum is registered for frp {frp_version}")
    resolved_jarvis_mcp_install_spec = jarvis_mcp_install_spec or os.environ.get(
        "CLIO_RELAY_JARVIS_MCP_INSTALL_SPEC",
        CLIO_KIT_JARVIS_MCP_WHEEL_URL,
    )
    resolved_jarvis_mcp_artifact_sha256 = (
        jarvis_mcp_artifact_sha256
        or os.environ.get("CLIO_RELAY_JARVIS_MCP_ARTIFACT_SHA256")
        or (
            CLIO_KIT_JARVIS_MCP_WHEEL_SHA256
            if resolved_jarvis_mcp_install_spec == CLIO_KIT_JARVIS_MCP_WHEEL_URL
            else None
        )
    )
    if resolved_jarvis_mcp_artifact_sha256 is None:
        raise ConfigurationError(
            "a custom clio-kit bootstrap source requires its expected wheel SHA-256"
        )
    if len(resolved_jarvis_mcp_artifact_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in resolved_jarvis_mcp_artifact_sha256
    ):
        raise ConfigurationError("clio-kit bootstrap wheel SHA-256 must be lowercase hex")
    rendered_jarvis_mcp_install_spec = shlex.quote(resolved_jarvis_mcp_install_spec)
    rendered_jarvis_mcp_artifact_sha256 = shlex.quote(resolved_jarvis_mcp_artifact_sha256)
    script = f"""set -euo pipefail
umask 077
export PATH="$HOME/.local/bin:$PATH"
export UV_TOOL_DIR="$HOME/.local/share/uv/tools"
export UV_TOOL_BIN_DIR="$HOME/.local/bin"
while IFS= read -r variable_name; do
  case "$variable_name" in
    UV_TOOL_DIR|UV_TOOL_BIN_DIR|UV_CACHE_DIR) ;;
    UV_*|PIP_*) unset "$variable_name" ;;
  esac
done < <(compgen -e)
mkdir -p "$HOME/.local/bin" "$HOME/.local/src" "$HOME/.local/share/clio-relay"
command -v flock >/dev/null 2>&1 || {{
  echo "flock is required to serialize clio-relay bootstrap" >&2
  exit 1
}}
exec 9>"$HOME/.local/share/clio-relay/bootstrap.lock"
if ! flock -n 9; then
  echo "another clio-relay bootstrap is already running" >&2
  exit 1
fi
{worker_fence}

cd "$HOME/.local/src"
FRP_VERSION="{frp_version}"
FRP_SHA256="{FRP_LINUX_AMD64_SHA256}"
FRPC_SHA256="{FRPC_LINUX_AMD64_SHA256}"
FRPS_SHA256="{FRPS_LINUX_AMD64_SHA256}"
ARCHIVE="frp_${{FRP_VERSION}}_linux_amd64.tar.gz"
if [ ! -x "$HOME/.local/bin/frpc" ] \
  || [ ! -x "$HOME/.local/bin/frps" ] \
  || ! echo "$FRPC_SHA256 *$HOME/.local/bin/frpc" | sha256sum --check --status - \
  || ! echo "$FRPS_SHA256 *$HOME/.local/bin/frps" | sha256sum --check --status -; then
  curl -L --fail --retry 3 -o "$ARCHIVE" \
    "https://github.com/fatedier/frp/releases/download/v${{FRP_VERSION}}/${{ARCHIVE}}"
  echo "$FRP_SHA256 *$ARCHIVE" | sha256sum --check --strict -
  tar -xzf "$ARCHIVE"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frpc" "$HOME/.local/bin/frpc"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frps" "$HOME/.local/bin/frps"
  echo "$FRPC_SHA256 *$HOME/.local/bin/frpc" | sha256sum --check --strict -
  echo "$FRPS_SHA256 *$HOME/.local/bin/frps" | sha256sum --check --strict -
fi

UV_VERSION="{UV_VERSION}"
UV_SHA256="{UV_LINUX_AMD64_SHA256}"
UV_ARCHIVE="uv-x86_64-unknown-linux-gnu.tar.gz"
if [ ! -x "$HOME/.local/bin/uv" ] \
  || [ "$("$HOME/.local/bin/uv" --version | awk '{{print $1 " " $2}}')" != "uv $UV_VERSION" ]; then
  curl -L --fail --retry 3 -o "$UV_ARCHIVE" \
    "https://github.com/astral-sh/uv/releases/download/$UV_VERSION/$UV_ARCHIVE"
  echo "$UV_SHA256 *$UV_ARCHIVE" | sha256sum --check --strict -
  tar -xzf "$UV_ARCHIVE"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uv" "$HOME/.local/bin/uv"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uvx" "$HOME/.local/bin/uvx"
fi
uv python install 3.12

AGENT_NPM_PACKAGE=${{CLIO_RELAY_AGENT_NPM_PACKAGE:-{rendered_agent_npm_package}}}
AGENT_NPM_BIN=${{CLIO_RELAY_AGENT_NPM_BIN:-{rendered_agent_npm_bin}}}
AGENT_BIN="${{CLIO_RELAY_AGENT_BIN:-}}"
if [ -z "$AGENT_BIN" ] && [ -n "$AGENT_NPM_BIN" ]; then
  AGENT_BIN="$HOME/.local/bin/$AGENT_NPM_BIN"
fi
if [ ! -x "$AGENT_BIN" ] && [ -n "$AGENT_NPM_PACKAGE" ] && command -v npm >/dev/null 2>&1; then
  npm install -g "$AGENT_NPM_PACKAGE"
fi

JARVIS_VENV="$HOME/.local/share/clio-relay/jarvis-venv"
uv venv --python 3.12 --seed --clear "$JARVIS_VENV"
. "$JARVIS_VENV/bin/activate"
JARVIS_UTIL_COMMIT="{JARVIS_UTIL_COMMIT}"
if [ ! -d "$HOME/.local/src/jarvis-util/.git" ]; then
  git clone --no-checkout https://github.com/grc-iit/jarvis-util.git \
    "$HOME/.local/src/jarvis-util"
fi
if [ -n "$(
  git -C "$HOME/.local/src/jarvis-util" status --porcelain=v1 --untracked-files=all
)" ]; then
  echo "refusing to replace modified jarvis-util checkout" >&2
  exit 1
fi
git -C "$HOME/.local/src/jarvis-util" fetch --depth 1 origin "$JARVIS_UTIL_COMMIT"
git -C "$HOME/.local/src/jarvis-util" checkout --detach "$JARVIS_UTIL_COMMIT"
test "$(git -C "$HOME/.local/src/jarvis-util" rev-parse HEAD)" = "$JARVIS_UTIL_COMMIT"
python -m pip install --isolated --index-url https://pypi.org/simple \\
  -r "$HOME/.local/src/jarvis-util/requirements.txt"
python -m pip install --isolated --no-deps "$HOME/.local/src/jarvis-util"
JARVIS_CD_VERSION="{JARVIS_CD_VERSION}"
JARVIS_CD_WHEEL_URL="{JARVIS_CD_WHEEL_URL}"
JARVIS_CD_WHEEL_SHA256="{JARVIS_CD_WHEEL_SHA256}"
JARVIS_CD_WHEEL_DIR="$HOME/.local/share/clio-relay/component-wheels/jarvis-cd"
JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL_DIR/{JARVIS_CD_WHEEL_FILENAME}"
rm -rf "$JARVIS_CD_WHEEL_DIR"
mkdir -p "$JARVIS_CD_WHEEL_DIR"
JARVIS_CD_STAGING="$(mktemp "${{JARVIS_CD_WHEEL}}.XXXXXX")"
curl -L --fail --retry 3 -o "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL_URL"
echo "$JARVIS_CD_WHEEL_SHA256 *$JARVIS_CD_STAGING" | sha256sum --check --strict -
mv "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL"
python -m pip install --isolated --index-url https://pypi.org/simple "$JARVIS_CD_WHEEL"
ln -sf "$JARVIS_VENV/bin/jarvis" "$HOME/.local/bin/jarvis"
JARVIS_MCP_INSTALL_SPEC={rendered_jarvis_mcp_install_spec}
JARVIS_MCP_ARTIFACT_SHA256={rendered_jarvis_mcp_artifact_sha256}
JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_INSTALL_SPEC"
JARVIS_MCP_ARTIFACT_PATH=""
JARVIS_MCP_REQUESTED_SOURCE="checkout"
JARVIS_MCP_VERSION=""
case "$JARVIS_MCP_INSTALL_SPEC" in
  "{CLIO_KIT_JARVIS_MCP_WHEEL_URL}")
    JARVIS_MCP_VERSION="{CLIO_KIT_JARVIS_MCP_VERSION}"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    rm -rf "$COMPONENT_DOWNLOAD_DIR"
    mkdir -p "$COMPONENT_DOWNLOAD_DIR"
    JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"
    COMPONENT_STAGING="$(mktemp "${{JARVIS_MCP_ARTIFACT_PATH}}.XXXXXX")"
    curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
      --retry 3 --retry-all-errors --retry-max-time 180 \
      --connect-timeout 20 --max-time 180 \
      --output "$COMPONENT_STAGING" "$JARVIS_MCP_INSTALL_SPEC"
    echo "$JARVIS_MCP_ARTIFACT_SHA256 *$COMPONENT_STAGING" | \
      sha256sum --check --strict -
    mv "$COMPONENT_STAGING" "$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="github_release"
    ;;
  clio-kit==*)
    JARVIS_MCP_VERSION="${{JARVIS_MCP_INSTALL_SPEC#clio-kit==}}"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    rm -rf "$COMPONENT_DOWNLOAD_DIR"
    mkdir -p "$COMPONENT_DOWNLOAD_DIR"
    python -m pip download --isolated --disable-pip-version-check --no-cache-dir \
      --index-url https://pypi.org/simple --no-deps --only-binary=:all: \
      --dest "$COMPONENT_DOWNLOAD_DIR" "$JARVIS_MCP_INSTALL_SPEC"
    mapfile -t JARVIS_MCP_WHEELS < <(
      find "$COMPONENT_DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_kit-*.whl' -print
    )
    if [ "${{#JARVIS_MCP_WHEELS[@]}}" -ne 1 ]; then
      echo "expected exactly one downloaded clio-kit wheel" >&2
      exit 1
    fi
    JARVIS_MCP_ARTIFACT_PATH="${{JARVIS_MCP_WHEELS[0]}}"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="pypi"
    ;;
  *.whl)
    test -f "$JARVIS_MCP_INSTALL_SPEC"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    mkdir -p "$(dirname "$COMPONENT_DOWNLOAD_DIR")"
    COMPONENT_STAGING="$(mktemp "${{COMPONENT_DOWNLOAD_DIR}}.XXXXXX.whl")"
    cp "$JARVIS_MCP_INSTALL_SPEC" "$COMPONENT_STAGING"
    rm -rf "$COMPONENT_DOWNLOAD_DIR"
    mkdir -p "$COMPONENT_DOWNLOAD_DIR"
    JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/$(basename "$JARVIS_MCP_INSTALL_SPEC")"
    mv "$COMPONENT_STAGING" "$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="wheel"
    ;;
  *)
    echo "clio-kit source must be the pinned URL, an exact version, or a local wheel" >&2
    exit 1
    ;;
esac
echo "$JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH" | \
  sha256sum --check --strict -
deactivate
uv tool install --force --python 3.12 --no-config \\
  --default-index https://pypi.org/simple "$JARVIS_MCP_INSTALL_TARGET"
JARVIS_MCP_UV_EXECUTABLE="$(command -v uv)"
test -x "$JARVIS_MCP_UV_EXECUTABLE"
JARVIS_MCP_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-kit"
test -x "$JARVIS_MCP_EXECUTABLE"
JARVIS_MCP_PROVIDER_PYTHON="$(sed -n '1{{s/^#!//;p;}}' "$JARVIS_MCP_EXECUTABLE")"
test -x "$JARVIS_MCP_PROVIDER_PYTHON"
JARVIS_MCP_INSTALLED_VERSION="$("$JARVIS_MCP_PROVIDER_PYTHON" -c \
  'from importlib.metadata import version; print(version("clio-kit"))')"
if [ -n "$JARVIS_MCP_VERSION" ] && \
   [ "$JARVIS_MCP_INSTALLED_VERSION" != "$JARVIS_MCP_VERSION" ]; then
  echo "installed clio-kit tool version does not match the release pin" >&2
  exit 1
fi
JARVIS_MCP_VERSION="$JARVIS_MCP_INSTALLED_VERSION"
"$JARVIS_MCP_EXECUTABLE" --help >/dev/null

DEST="$HOME/.local/src/clio-relay"
rm -rf "$DEST"
mkdir -p "$DEST"
SOURCE_ARCHIVE={rendered_source_archive}
SOURCE_ARCHIVE_SHA256={rendered_source_archive_sha256}
if [ -n "$SOURCE_ARCHIVE_SHA256" ]; then
  echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -
fi
tar -xf "$SOURCE_ARCHIVE" -C "$DEST"
RELAY_INSTALL_SPEC={rendered_relay_install_spec}
RELAY_ARTIFACT_SHA256={rendered_relay_artifact_sha256}
RELAY_INSTALL_TARGET="$RELAY_INSTALL_SPEC"
RELAY_ARTIFACT_PATH=""
case "$RELAY_INSTALL_SPEC" in
  clio-relay==*)
    DOWNLOAD_DIR="$DEST/downloaded-wheels"
    rm -rf "$DOWNLOAD_DIR"
    mkdir -p "$DOWNLOAD_DIR"
    "$JARVIS_VENV/bin/python" -m pip download --isolated \
      --disable-pip-version-check --no-cache-dir \
      --index-url https://pypi.org/simple --no-deps --only-binary=:all: \
      --dest "$DOWNLOAD_DIR" "$RELAY_INSTALL_SPEC"
    mapfile -t RELAY_WHEELS < <(
      find "$DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_relay-*.whl' -print
    )
    if [ "${{#RELAY_WHEELS[@]}}" -ne 1 ]; then
      echo "expected exactly one downloaded clio-relay wheel" >&2
      exit 1
    fi
    RELAY_ARTIFACT_PATH="${{RELAY_WHEELS[0]}}"
    RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
    if [ -z "$RELAY_ARTIFACT_SHA256" ]; then
      RELAY_VERSION="${{RELAY_INSTALL_SPEC#clio-relay==}}"
      RELAY_ARTIFACT_SHA256="$(
        "$JARVIS_VENV/bin/python" - "$RELAY_VERSION" "$(basename "$RELAY_ARTIFACT_PATH")" \
          <<'__CLIO_RELAY_PYPI_DIGEST__'
import json
import re
import sys
from urllib.parse import quote
from urllib.request import urlopen

version, filename = sys.argv[1:]
with urlopen(
    f"https://pypi.org/pypi/clio-relay/{{quote(version, safe='')}}/json",
    timeout=30,
) as response:
    content = response.read(4 * 1024 * 1024 + 1)
if len(content) > 4 * 1024 * 1024:
    raise SystemExit("PyPI clio-relay metadata exceeds the bounded response size")
document = json.loads(content)
matches = [
    item
    for item in document.get("urls", [])
    if item.get("filename") == filename and item.get("packagetype") == "bdist_wheel"
]
if len(matches) != 1:
    raise SystemExit("PyPI did not return one exact clio-relay wheel identity")
digest = matches[0].get("digests", {{}}).get("sha256")
if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{{64}}", digest) is None:
    raise SystemExit("PyPI clio-relay wheel identity omitted a valid SHA-256")
print(digest)
__CLIO_RELAY_PYPI_DIGEST__
      )"
    fi
    ;;
  *.whl)
    RELAY_ARTIFACT_PATH="$RELAY_INSTALL_SPEC"
    ;;
esac
if [ -n "$RELAY_ARTIFACT_PATH" ]; then
  test -n "$RELAY_ARTIFACT_SHA256"
  echo "$RELAY_ARTIFACT_SHA256 *$RELAY_ARTIFACT_PATH" | sha256sum --check --strict -
fi
uv tool install --force --python 3.12 --no-config \\
  --default-index https://pypi.org/simple \\
  --with "$JARVIS_CD_WHEEL" "$RELAY_INSTALL_TARGET"
RELAY_UV_EXECUTABLE="$(command -v uv)"
test -x "$RELAY_UV_EXECUTABLE"
RELAY_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-relay"
test -x "$RELAY_EXECUTABLE"
RELAY_PROVIDER_PYTHON="$(sed -n '1{{s/^#!//;p;}}' "$RELAY_EXECUTABLE")"
test -x "$RELAY_PROVIDER_PYTHON"
uv pip install --python "$JARVIS_VENV/bin/python" \\
  --default-index https://pypi.org/simple \\
  --refresh-package clio-relay "$RELAY_INSTALL_TARGET"
JARVIS_PACKAGE_PROBE='import clio_relay, jarvis_cd; '
JARVIS_PACKAGE_PROBE+='import clio_relay.bounded_command.pkg; '
JARVIS_PACKAGE_PROBE+='import clio_relay.mcp_call.pkg; '
JARVIS_PACKAGE_PROBE+='import clio_relay.remote_agent.pkg'
"$RELAY_PROVIDER_PYTHON" -c "$JARVIS_PACKAGE_PROBE"
"$JARVIS_VENV/bin/python" -c "$JARVIS_PACKAGE_PROBE"
verify_jarvis_cd_distribution() {{
  local interpreter="$1"
  "$interpreter" - \\
    "$JARVIS_CD_WHEEL" \\
    "$JARVIS_CD_WHEEL_SHA256" \\
    "$JARVIS_CD_VERSION" \\
    <<'__CLIO_RELAY_NATIVE_JARVIS_PROBE__'
import hashlib
import sys
from importlib.metadata import distribution
from pathlib import Path

from clio_relay.errors import ConfigurationError
from clio_relay.installation import verify_distribution_file_source

wheel = Path(sys.argv[1]).resolve()
expected_sha256 = sys.argv[2]
expected_version = sys.argv[3]
if hashlib.sha256(wheel.read_bytes()).hexdigest() != expected_sha256:
    raise SystemExit("JARVIS-CD release wheel digest changed after installation")
installed = distribution("jarvis_cd")
if installed.version != expected_version:
    raise SystemExit("JARVIS-CD installed version does not match the release pin")
try:
    verify_distribution_file_source(
        direct_url_text=installed.read_text("direct_url.json"),
        expected_artifact=wheel,
    )
except ConfigurationError as exc:
    raise SystemExit(
        f"JARVIS-CD was not installed from the verified release wheel: {{exc}}"
    ) from exc
print(f"jarvis_cd_distribution={{installed.name}}=={{installed.version}}")
__CLIO_RELAY_NATIVE_JARVIS_PROBE__
}}
verify_jarvis_cd_distribution "$RELAY_PROVIDER_PYTHON"
verify_jarvis_cd_distribution "$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC="$RELAY_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_ARTIFACT="$RELAY_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE="$RELAY_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON="$RELAY_PROVIDER_PYTHON"
export CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE="$RELAY_UV_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT="$JARVIS_UTIL_COMMIT"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION="$JARVIS_CD_VERSION"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL="$JARVIS_CD_WHEEL_URL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256="$JARVIS_CD_WHEEL_SHA256"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON="$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE="$HOME/.local/bin/jarvis"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC="$JARVIS_MCP_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT="$JARVIS_MCP_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT_SHA256="$JARVIS_MCP_ARTIFACT_SHA256"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE="$JARVIS_MCP_REQUESTED_SOURCE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION="$JARVIS_MCP_VERSION"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE="$JARVIS_MCP_EXECUTABLE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON="$JARVIS_MCP_PROVIDER_PYTHON"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE="$JARVIS_MCP_UV_EXECUTABLE"
"$RELAY_PROVIDER_PYTHON" - <<'__CLIO_RELAY_INSTALL_RECEIPT__'
import os
import sys
from importlib.metadata import distribution
from pathlib import Path

from clio_relay.installation import (
    ComponentArtifactIdentity,
    probe_persistent_uv_tool_identity,
    probe_clio_kit_native_execution_contract,
    probe_jarvis_native_execution_capability,
    write_install_receipt,
)
from clio_relay.validation_report import sha256_file

artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_ARTIFACT"]
relay_artifact = Path(artifact_value).resolve() if artifact_value else None
relay_distribution = distribution("clio-relay")
relay_persistent_tool = None
if relay_artifact is not None:
    relay_persistent_tool = probe_persistent_uv_tool_identity(
        uv_executable=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE"],
        tool_executable=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
        provider_interpreter=os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON"],
        source_artifact=relay_artifact,
        distribution="clio-relay",
        distribution_version=relay_distribution.version,
        entry_point="clio-relay",
    )
component_artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT"]
component_artifact = Path(component_artifact_value).resolve() if component_artifact_value else None
component_artifact_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT_SHA256"]
if component_artifact is None or sha256_file(component_artifact) != component_artifact_sha256:
    raise SystemExit("clio-kit wheel digest changed after persistent-tool installation")
component_version = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION"] or None
component_spec = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC"]
jarvis_cd_wheel = Path(os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL"]).resolve()
jarvis_cd_wheel_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256"]
if sha256_file(jarvis_cd_wheel) != jarvis_cd_wheel_sha256:
    raise SystemExit("jarvis-cd receipt wheel digest does not match bootstrap pin")
jarvis_cd_distribution = distribution("jarvis_cd")
if jarvis_cd_distribution.version != os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"]:
    raise SystemExit("jarvis-cd receipt version does not match the released wheel pin")
runtime_command = [
    os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
    "mcp-server",
    "jarvis",
]
if not runtime_command:
    raise SystemExit("clio-kit native JARVIS contract requires a persistent uv tool")
clio_kit_native_execution = probe_clio_kit_native_execution_contract(runtime_command)
persistent_clio_kit_tool = probe_persistent_uv_tool_identity(
    uv_executable=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE"],
    tool_executable=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
    provider_interpreter=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON"],
    source_artifact=component_artifact,
    distribution="clio-kit",
    distribution_version=component_version,
    entry_point="clio-kit",
)
jarvis_execution_native_execution = probe_jarvis_native_execution_capability(
    os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"]
)
receipt = write_install_receipt(
    install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"],
    artifact_path=Path(artifact_value) if artifact_value else None,
    components={{
        "clio-relay": relay_distribution.version,
        "clio-kit": component_version or component_spec,
        "jarvis-cd": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"],
        "jarvis-util": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT"],
    }},
    component_artifacts={{
        "clio-relay": ComponentArtifactIdentity(
            distribution=relay_distribution.name,
            distribution_version=relay_distribution.version,
            install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"],
            requested_source=(
                "pypi"
                if os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"].startswith("clio-relay==")
                else ("wheel" if relay_artifact is not None else "checkout")
            ),
            artifact_filename=(relay_artifact.name if relay_artifact is not None else None),
            artifact_sha256=(sha256_file(relay_artifact) if relay_artifact is not None else None),
            runtime_artifact_path=(str(relay_artifact) if relay_artifact is not None else None),
            runtime_command=[
                os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
                "installation-info",
            ],
            runtime_interpreters={{
                "provider": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_PROVIDER_PYTHON"],
            }},
            runtime_executables={{
                "clio-relay": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_EXECUTABLE"],
                "uv": os.environ["CLIO_RELAY_BOOTSTRAP_RELAY_UV_EXECUTABLE"],
            }},
            persistent_tool=relay_persistent_tool,
        ),
        "clio-kit": ComponentArtifactIdentity(
            distribution="clio-kit",
            distribution_version=component_version,
            install_spec=component_spec,
            requested_source=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE"],
            artifact_filename=(component_artifact.name if component_artifact else None),
            artifact_sha256=component_artifact_sha256,
            runtime_artifact_path=(str(component_artifact) if component_artifact else None),
            runtime_command=runtime_command,
            runtime_interpreters={{
                "provider": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_PROVIDER_PYTHON"],
            }},
            runtime_executables={{
                "clio-kit": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_EXECUTABLE"],
                "uv": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_UV_EXECUTABLE"],
            }},
            native_execution=clio_kit_native_execution,
            persistent_tool=persistent_clio_kit_tool,
        ),
        "jarvis-cd": ComponentArtifactIdentity(
            distribution=jarvis_cd_distribution.name,
            distribution_version=jarvis_cd_distribution.version,
            install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL"],
            requested_source="github_release",
            artifact_filename=jarvis_cd_wheel.name,
            artifact_sha256=jarvis_cd_wheel_sha256,
            runtime_artifact_path=str(jarvis_cd_wheel),
            runtime_command=[
                os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
                "--help",
            ],
            runtime_interpreters={{
                "provider": sys.executable,
                "execution": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"],
            }},
            runtime_executables={{
                "jarvis": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
            }},
            native_execution=jarvis_execution_native_execution,
        ),
    }},
)
print(f"relay_install_receipt={{receipt.schema_version}}")
print(f"relay_artifact_sha256={{receipt.artifact_sha256 or 'none'}}")
__CLIO_RELAY_INSTALL_RECEIPT__

mkdir -p \
  "$HOME/.local/share/clio-relay/jarvis-config" \
  "$HOME/.local/share/clio-relay/jarvis-private" \
  "$HOME/.local/share/clio-relay/jarvis-shared"
jarvis init \
  "$HOME/.local/share/clio-relay/jarvis-config" \
  "$HOME/.local/share/clio-relay/jarvis-private" \
  "$HOME/.local/share/clio-relay/jarvis-shared"
jarvis repo add "$DEST/jarvis-packages/clio_relay" --force true

{worker_recheck}
CLIO_RELAY_CORE_DIR={rendered_core_dir} \
CLIO_RELAY_SPOOL_DIR={rendered_spool_dir} \
CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
{WORKER_LIFETIME_GUARD_FD_ENV}="$WORKER_LIFETIME_GUARD_FD" \
{init_command}
{worker_restart}

export CLIO_RELAY_BOOTSTRAP_WORKER_SERVICE_NAME="$WORKER_SERVICE_NAME"
export CLIO_RELAY_BOOTSTRAP_WORKER_WAS_ACTIVE="$WORKER_WAS_ACTIVE"
export CLIO_RELAY_BOOTSTRAP_WORKER_WRITER_PROOF="$WORKER_WRITER_PROOF"
export CLIO_RELAY_BOOTSTRAP_WORKER_WRITER_RECHECK="$WORKER_WRITER_RECHECK"
export CLIO_RELAY_BOOTSTRAP_WORKER_LIFETIME_EXCLUSIVE="$WORKER_LIFETIME_EXCLUSIVE"
export CLIO_RELAY_BOOTSTRAP_WORKER_RESTARTED="$WORKER_RESTARTED"

python3 - <<'__CLIO_RELAY_BOOTSTRAP_RECEIPT__'
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

install_receipt = Path.home() / ".local/share/clio-relay/install-receipt.json"
install_receipt_sha256 = hashlib.sha256(install_receipt.read_bytes()).hexdigest()
worker_service_name = os.environ["CLIO_RELAY_BOOTSTRAP_WORKER_SERVICE_NAME"] or None
worker_was_active = os.environ["CLIO_RELAY_BOOTSTRAP_WORKER_WAS_ACTIVE"] == "1"
worker_writer_proof = os.environ["CLIO_RELAY_BOOTSTRAP_WORKER_WRITER_PROOF"] == "1"
worker_writer_recheck = os.environ["CLIO_RELAY_BOOTSTRAP_WORKER_WRITER_RECHECK"] == "1"
worker_lifetime_exclusive = (
    os.environ["CLIO_RELAY_BOOTSTRAP_WORKER_LIFETIME_EXCLUSIVE"] == "1"
)
worker_restarted = os.environ["CLIO_RELAY_BOOTSTRAP_WORKER_RESTARTED"] == "1"
receipt = {{
    "schema_version": "clio-relay.bootstrap-receipt.v1",
    "invocation_id": {invocation_id!r},
    "bootstrap_profile": "linux-user",
    "relay_install_spec": {relay_install_spec!r},
    "install_receipt_sha256": install_receipt_sha256,
    "worker_fence": {{
        "managed": worker_service_name is not None,
        "service_name": worker_service_name,
        "was_active": worker_was_active,
        "writer_proof": worker_writer_proof,
        "writer_recheck": worker_writer_recheck,
        "lifetime_exclusive": worker_lifetime_exclusive,
        "restarted": worker_restarted,
    }},
    "completed_at": datetime.now(timezone.utc).isoformat(),
}}
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
temporary = destination.with_name(f".{{destination.name}}.{{os.getpid()}}.tmp")
temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
os.chmod(temporary, 0o600)
os.replace(temporary, destination)
print(f"bootstrap_receipt={{destination}}")
print(f"bootstrap_invocation_id={{receipt['invocation_id']}}")
print(f"bootstrap_install_receipt_sha256={{install_receipt_sha256}}")
__CLIO_RELAY_BOOTSTRAP_RECEIPT__

echo "frpc=$("$HOME/.local/bin/frpc" --version)"
echo "frps=$("$HOME/.local/bin/frps" --version)"
if [ -x "$AGENT_BIN" ]; then
  echo "agent=$("$AGENT_BIN" --version)"
fi
echo "jarvis=$("$HOME/.local/bin/jarvis" --help | head -n 1)"
echo "relay=$(clio-relay --help | head -n 1)"
"""
    return script.replace("\r\n", "\n")


def _render_relay_install_spec(relay_install_spec: str) -> str:
    if relay_install_spec == "$DEST":
        return '"$DEST"'
    if relay_install_spec.startswith("$DEST/"):
        return '"$DEST"/' + shlex.quote(relay_install_spec.removeprefix("$DEST/"))
    return shlex.quote(relay_install_spec)


def create_bootstrap_archive(
    *,
    source_root: Path,
    archive: Path,
    relay_wheel: Path | None = None,
) -> BootstrapArchive:
    """Create the archive used by remote bootstrap.

    A clean git checkout deploys that exact committed tree. Installed-package
    runs deploy packaged JARVIS assets and install either the supplied candidate
    wheel or the exact package version, so bootstrap does not require a checkout.
    """
    if relay_wheel is not None:
        _write_packaged_bootstrap_archive(archive, relay_wheel=relay_wheel)
        return BootstrapArchive(
            archive=archive,
            install_spec=f"$DEST/wheels/{relay_wheel.name}",
        )
    if _is_clio_relay_git_checkout(source_root):
        assert_clean_git_checkout(source_root)
        _run(["git", "archive", "--format=tar", "-o", str(archive), "HEAD"], cwd=source_root)
        return BootstrapArchive(archive=archive, install_spec="$DEST")
    _write_packaged_bootstrap_archive(archive, relay_wheel=None)
    return BootstrapArchive(archive=archive, install_spec=f"clio-relay=={__version__}")


def _write_packaged_bootstrap_archive(archive: Path, *, relay_wheel: Path | None) -> None:
    if relay_wheel is not None and not relay_wheel.is_file():
        raise ConfigurationError(f"relay wheel does not exist: {relay_wheel}")
    assets = resources.files("clio_relay").joinpath("assets", "jarvis-packages")
    source_assets = Path(__file__).resolve().parents[2] / "jarvis-packages"
    with tarfile.open(archive, "w") as tar:
        if relay_wheel is not None:
            tar.add(relay_wheel, arcname=str(Path("wheels", relay_wheel.name)))
        if assets.is_dir():
            with resources.as_file(assets) as asset_path:
                _add_jarvis_assets_to_archive(tar=tar, asset_path=asset_path)
            return
        if source_assets.is_dir():
            _add_jarvis_assets_to_archive(tar=tar, asset_path=source_assets)
            return
    raise ConfigurationError("installed clio-relay package does not include jarvis package assets")


def _is_clio_relay_git_checkout(source_root: Path) -> bool:
    pyproject = source_root / "pyproject.toml"
    if not (source_root / ".git").exists() or not pyproject.exists():
        return False
    try:
        text = pyproject.read_text(encoding="utf-8")
    except OSError:
        return False
    return 'name = "clio-relay"' in text


def _add_jarvis_assets_to_archive(*, tar: tarfile.TarFile, asset_path: Path) -> None:
    for item in asset_path.rglob("*"):
        relative_parts = item.relative_to(asset_path).parts
        if "__pycache__" in relative_parts or item.name.endswith(".pyc"):
            continue
        tar.add(
            item,
            arcname=str(Path("jarvis-packages", *relative_parts)),
            recursive=False,
        )


def assert_clean_git_checkout(source_root: Path) -> None:
    """Raise if source_root has uncommitted changes that git archive would omit."""
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=source_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RelayError(f"failed to inspect git checkout before bootstrap: {detail}")
    if result.stdout.strip():
        raise ConfigurationError(
            "remote bootstrap deploys git HEAD; commit or stash local changes before bootstrap"
        )


def _assert_executable(path: Path) -> None:
    try:
        subprocess.run([str(path), "--version"], check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ConfigurationError(f"installed executable cannot run: {path}: {exc}") from exc


def _assert_frp_pair(frpc: Path, frps: Path) -> None:
    _assert_sha256(frpc, FRPC_WINDOWS_AMD64_SHA256)
    _assert_sha256(frps, FRPS_WINDOWS_AMD64_SHA256)
    _assert_executable(frpc)
    _assert_executable(frps)


def _assert_sha256(path: Path, expected: str) -> None:
    try:
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ConfigurationError(f"installed executable cannot be hashed: {path}: {exc}") from exc
    if observed != expected:
        raise ConfigurationError(f"installed executable SHA-256 mismatch: {path}: {observed}")


def _validate_ssh_destination(value: str) -> None:
    """Reject SSH destinations that could be parsed as client options."""
    if (
        not value
        or value != value.strip()
        or value.startswith("-")
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise ConfigurationError(
            "ssh host must be one non-option destination without whitespace or controls"
        )


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RelayError(f"command failed ({' '.join(command)}): {detail}")
    return result
