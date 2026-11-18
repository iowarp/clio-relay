"""Embedded worker-writer-proof and lifetime-guard programs.

Split from bootstrap.py (clio-relay#255). `_WORKER_WRITER_PROOF_PYTHON` proves
exclusive relay-writer ownership of the configured core against /proc;
`_WORKER_LIFETIME_EXCLUSIVE_GUARD_PYTHON` verifies an inherited lifetime-guard
file descriptor. Both are consumed by bootstrap_worker_fence_script.py.
"""

from __future__ import annotations

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


def read_capped(path: Path) -> bytes:
    """Read one proc pseudo-file's bytes, enforcing the bounded read-size invariant.

    Unlike read_bounded, a non-vanished OSError is raised to the caller
    un-translated.  Every caller except the writer-proof environ evidence
    treats any such error as immediately fatal (see read_bounded below); the
    writer proof instead treats an unreadable environ as best-effort evidence
    that a non-target candidate can still be dismissed without it.
    """
    with path.open("rb") as stream:
        value = stream.read(MAX_PROC_FILE_BYTES + 1)
    if len(value) > MAX_PROC_FILE_BYTES:
        fail(f"{path} exceeds the bounded inspection size")
    return value


def read_bounded(path: Path) -> bytes | None:
    """Read one proc pseudo-file without accepting an unbounded value."""
    try:
        return read_capped(path)
    except OSError as error:
        if vanished(error):
            return None
        fail(f"cannot inspect {path}: {error}")


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
            environ_path = process / "environ"
            try:
                raw_environment = read_capped(environ_path)
            except OSError as error:
                if vanished(error):
                    continue
                if process_cluster is not None and process_cluster != cluster:
                    # environ is best-effort evidence for writer-proof, not a
                    # mandatory one.  A candidate whose cmdline names a
                    # cluster other than the one being bootstrapped is
                    # provably not our concern even when its environment
                    # cannot be inspected -- e.g. a same-uid peer hardened by
                    # our own process_containment.enforce_linux_secret_memory_gate
                    # non-dumpable gate, whose environ is root-only.  A
                    # candidate that matches our cluster, or leaves it
                    # ambiguous (no --cluster in its cmdline), still cannot
                    # be dismissed without proof: keep failing closed.
                    continue
                fail(f"cannot inspect {environ_path}: {error}")
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
