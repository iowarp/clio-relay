"""Spool directory helpers for logs and artifact backing files."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Literal, NotRequired, TypedDict, cast

from filelock import FileLock

from clio_relay.models import ArtifactRef, RelayJob

MAX_LOG_READ_BYTES = 1_048_576
HASH_CHUNK_BYTES = 1_048_576
DEFAULT_MAX_LOG_BYTES_PER_STREAM = 64 * 1_048_576
DEFAULT_MAX_LOG_BYTES_PER_JOB = 2 * DEFAULT_MAX_LOG_BYTES_PER_STREAM
LOG_CAPTURE_SCHEMA = "clio-relay.log-capture.v1"
LOG_CAPTURE_LOCK_TIMEOUT_SECONDS = 10
ARTIFACT_OWNERSHIP_SCHEMA = "clio-relay.owned-artifact.v1"


class _StreamCaptureState(TypedDict):
    observed_bytes: int
    persisted_bytes: int
    dropped_bytes: int
    truncated: bool
    truncation_event_recorded: bool


class _LogCaptureState(TypedDict):
    schema_version: str
    max_bytes_per_stream: int
    max_bytes_per_job: int
    streams: dict[str, _StreamCaptureState]
    pending: NotRequired[_PendingCaptureState]


class _PendingCaptureState(TypedDict):
    stream: Literal["stdout", "stderr"]
    before_persisted_bytes: int
    observed_bytes: int
    accepted_bytes: int
    dropped_bytes: int


@dataclass(frozen=True, slots=True)
class LogAppendResult:
    """Describe the durable portion of one streamed output chunk."""

    stream: Literal["stdout", "stderr"]
    accepted_text: str
    observed_bytes: int
    accepted_bytes: int
    dropped_bytes: int
    persisted_stream_bytes: int
    persisted_job_bytes: int
    truncated: bool
    truncation_event_required: bool


@dataclass(frozen=True, slots=True)
class OwnedFileSnapshot:
    """A stable size and digest captured from one owned file descriptor."""

    size_bytes: int
    sha256: str
    data: bytes | None = None


class OwnedFileSizeLimitError(RuntimeError):
    """Raised when an owned regular file exceeds a bounded read limit."""

    def __init__(self, path: Path, limit: int) -> None:
        super().__init__(f"owned file exceeds the {limit}-byte read limit: {path}")
        self.path = path
        self.limit = limit


class JobSpool:
    """Per-job execution spool rooted on cluster-accessible storage."""

    def __init__(
        self,
        root: Path,
        job: RelayJob,
        *,
        max_log_bytes_per_stream: int = DEFAULT_MAX_LOG_BYTES_PER_STREAM,
        max_log_bytes_per_job: int = DEFAULT_MAX_LOG_BYTES_PER_JOB,
    ) -> None:
        if max_log_bytes_per_stream <= 0:
            raise ValueError("max_log_bytes_per_stream must be positive")
        if max_log_bytes_per_job <= 0:
            raise ValueError("max_log_bytes_per_job must be positive")
        self.root = root
        self.job = job
        self.path = root / job.job_id
        self.max_log_bytes_per_stream = max_log_bytes_per_stream
        self.max_log_bytes_per_job = max_log_bytes_per_job

    @property
    def log_capture_path(self) -> Path:
        """Return the durable log-capture accounting manifest path."""
        return self.path / "log-capture.json"

    def initialize(self) -> None:
        """Create metadata and log files for a job spool."""
        self.path.mkdir(parents=True, exist_ok=True)
        (self.path / "metadata.json").write_text(
            self.job.model_dump_json(indent=2),
            encoding="utf-8",
        )
        for name in ("events.jsonl", "stdout.log", "stderr.log", "artifacts.jsonl"):
            target = self.path / name
            if not target.exists():
                target.write_text("", encoding="utf-8")
        with self._capture_lock():
            state = self._load_capture_state_unlocked()
            self._write_capture_state_unlocked(state)

    def write_pipeline(self, yaml_text: str) -> Path:
        """Write the materialized JARVIS pipeline for this job."""
        target = self.path / "pipeline.yaml"
        target.write_text(yaml_text, encoding="utf-8")
        return target

    def write_provenance(self, provenance: dict[str, Any]) -> Path:
        """Write a relay execution provenance manifest."""
        target = self.path / "provenance.json"
        target.write_text(json.dumps(provenance, indent=2, sort_keys=True), encoding="utf-8")
        return target

    def write_runtime_metadata(self, metadata: dict[str, Any]) -> Path:
        """Write the normalized JARVIS runtime metadata manifest."""
        target = self.path / "runtime-metadata.json"
        target.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        return target

    def append_stdout(self, text: str) -> LogAppendResult:
        """Append standard output within the configured durable byte quotas."""
        return self.append_log("stdout", text)

    def append_stderr(self, text: str) -> LogAppendResult:
        """Append standard error within the configured durable byte quotas."""
        return self.append_log("stderr", text)

    def append_log(
        self,
        stream_name: Literal["stdout", "stderr"],
        text: str,
    ) -> LogAppendResult:
        """Append output atomically while enforcing stream and whole-job quotas."""
        encoded = text.encode("utf-8")
        with self._capture_lock():
            state = self._load_capture_state_unlocked()
            stream_state = state["streams"][stream_name]
            persisted_job_bytes = sum(item["persisted_bytes"] for item in state["streams"].values())
            stream_available = max(
                0,
                self.max_log_bytes_per_stream - stream_state["persisted_bytes"],
            )
            job_available = max(0, self.max_log_bytes_per_job - persisted_job_bytes)
            accepted_payload = _complete_utf8_prefix(encoded, min(stream_available, job_available))
            dropped_bytes = len(encoded) - len(accepted_payload)
            state["pending"] = {
                "stream": stream_name,
                "before_persisted_bytes": stream_state["persisted_bytes"],
                "observed_bytes": len(encoded),
                "accepted_bytes": len(accepted_payload),
                "dropped_bytes": dropped_bytes,
            }
            self._write_capture_state_unlocked(state)
            if accepted_payload:
                with _open_owned_log(self.path / f"{stream_name}.log", mode="ab") as stream:
                    stream.write(accepted_payload)
                    stream.flush()
                    os.fsync(stream.fileno())
            stream_state["observed_bytes"] += len(encoded)
            stream_state["persisted_bytes"] += len(accepted_payload)
            stream_state["dropped_bytes"] += dropped_bytes
            stream_state["truncated"] = stream_state["truncated"] or dropped_bytes > 0
            del state["pending"]
            self._write_capture_state_unlocked(state)
            persisted_job_bytes += len(accepted_payload)
            return LogAppendResult(
                stream=stream_name,
                accepted_text=accepted_payload.decode("utf-8"),
                observed_bytes=len(encoded),
                accepted_bytes=len(accepted_payload),
                dropped_bytes=dropped_bytes,
                persisted_stream_bytes=stream_state["persisted_bytes"],
                persisted_job_bytes=persisted_job_bytes,
                truncated=stream_state["truncated"],
                truncation_event_required=(
                    stream_state["truncated"] and not stream_state["truncation_event_recorded"]
                ),
            )

    def mark_truncation_event_recorded(
        self,
        stream_name: Literal["stdout", "stderr"],
    ) -> None:
        """Durably acknowledge the queue event describing a truncated stream."""
        with self._capture_lock():
            state = self._load_capture_state_unlocked()
            stream_state = state["streams"][stream_name]
            if not stream_state["truncated"]:
                raise ValueError(f"{stream_name} has not been truncated")
            stream_state["truncation_event_recorded"] = True
            self._write_capture_state_unlocked(state)

    def capture_summary(self) -> dict[str, Any]:
        """Return durable byte accounting for both captured output streams."""
        with self._capture_lock():
            state = self._load_capture_state_unlocked()
        stream_states = state["streams"]
        streams = {name: dict(stream_state) for name, stream_state in state["streams"].items()}
        return {
            "schema_version": state["schema_version"],
            "max_bytes_per_stream": state["max_bytes_per_stream"],
            "max_bytes_per_job": state["max_bytes_per_job"],
            "observed_bytes": sum(item["observed_bytes"] for item in stream_states.values()),
            "persisted_bytes": sum(item["persisted_bytes"] for item in stream_states.values()),
            "dropped_bytes": sum(item["dropped_bytes"] for item in stream_states.values()),
            "truncated": any(item["truncated"] for item in stream_states.values()),
            "streams": streams,
        }

    def read_log(
        self,
        stream_name: Literal["stdout", "stderr"],
        *,
        offset: int = 0,
        limit: int = 65536,
    ) -> tuple[str, int, bool]:
        """Read a byte range from a named log and return text, next offset, and EOF."""
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        if limit > MAX_LOG_READ_BYTES:
            raise ValueError(f"limit cannot exceed {MAX_LOG_READ_BYTES} bytes")
        path = self.path / f"{stream_name}.log"
        if not path.exists():
            return "", offset, True
        with _open_owned_log(path, mode="rb") as stream:
            size = os.fstat(stream.fileno()).st_size
            stream.seek(offset)
            chunk = stream.read(limit)
        next_offset = offset + len(chunk)
        return chunk.decode("utf-8", errors="replace"), next_offset, next_offset >= size

    def artifact_for(self, path: Path, *, kind: str) -> ArtifactRef:
        """Build an artifact reference for a spool-backed path."""
        owned_root = _absolute_path(self.path)
        artifact_path = _absolute_path(path)
        digest, size = _sha256_file(artifact_path, owned_root=owned_root)
        return ArtifactRef(
            job_id=self.job.job_id,
            uri=artifact_path.as_uri(),
            kind=kind,
            size_bytes=size,
            sha256=digest,
            metadata={
                "ownership_schema": ARTIFACT_OWNERSHIP_SCHEMA,
                "owned_root_uri": owned_root.as_uri(),
            },
        )

    def _capture_lock(self) -> FileLock:
        return FileLock(
            str(self.path / ".log-capture.lock"),
            timeout=LOG_CAPTURE_LOCK_TIMEOUT_SECONDS,
        )

    def _load_capture_state_unlocked(self) -> _LogCaptureState:
        if not self.log_capture_path.exists():
            streams: dict[str, _StreamCaptureState] = {}
            for name in ("stdout", "stderr"):
                path = self.path / f"{name}.log"
                persisted_bytes = _owned_log_size(path) if path.exists() else 0
                if persisted_bytes > self.max_log_bytes_per_stream:
                    raise RuntimeError(
                        f"existing {name} log exceeds the configured stream quota: {path}"
                    )
                streams[name] = {
                    "observed_bytes": persisted_bytes,
                    "persisted_bytes": persisted_bytes,
                    "dropped_bytes": 0,
                    "truncated": False,
                    "truncation_event_recorded": False,
                }
            if sum(item["persisted_bytes"] for item in streams.values()) > (
                self.max_log_bytes_per_job
            ):
                raise RuntimeError(
                    f"existing logs exceed the configured whole-job quota: {self.path}"
                )
            return {
                "schema_version": LOG_CAPTURE_SCHEMA,
                "max_bytes_per_stream": self.max_log_bytes_per_stream,
                "max_bytes_per_job": self.max_log_bytes_per_job,
                "streams": streams,
            }
        try:
            raw = json.loads(self.log_capture_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"invalid log capture state: {self.log_capture_path}: {exc}"
            ) from exc
        state = _validate_capture_state(raw, path=self.log_capture_path)
        if (
            state["max_bytes_per_stream"] != self.max_log_bytes_per_stream
            or state["max_bytes_per_job"] != self.max_log_bytes_per_job
        ):
            raise RuntimeError(
                "log capture quotas changed for an existing job spool; preserve the original "
                f"limits recorded in {self.log_capture_path}"
            )
        self._recover_pending_capture_unlocked(state)
        for name in ("stdout", "stderr"):
            path = self.path / f"{name}.log"
            actual_size = _owned_log_size(path) if path.exists() else 0
            if actual_size != state["streams"][name]["persisted_bytes"]:
                raise RuntimeError(
                    f"log capture state does not match {path}: expected "
                    f"{state['streams'][name]['persisted_bytes']} bytes, found {actual_size}"
                )
        return state

    def _recover_pending_capture_unlocked(self, state: _LogCaptureState) -> None:
        pending = state.get("pending")
        if pending is None:
            return
        stream_name = pending["stream"]
        stream_state = state["streams"][stream_name]
        before = pending["before_persisted_bytes"]
        if stream_state["persisted_bytes"] != before:
            raise RuntimeError(f"invalid pending log capture baseline: {self.log_capture_path}")
        path = self.path / f"{stream_name}.log"
        actual_size = _owned_log_size(path) if path.exists() else 0
        accepted = pending["accepted_bytes"]
        if actual_size == before + accepted:
            persisted = accepted
            dropped = pending["dropped_bytes"]
        elif actual_size == before:
            persisted = 0
            dropped = pending["observed_bytes"]
        else:
            raise RuntimeError(
                f"pending log capture cannot reconcile {path}: expected {before} or "
                f"{before + accepted} bytes, found {actual_size}"
            )
        stream_state["observed_bytes"] += pending["observed_bytes"]
        stream_state["persisted_bytes"] += persisted
        stream_state["dropped_bytes"] += dropped
        stream_state["truncated"] = stream_state["truncated"] or dropped > 0
        del state["pending"]
        self._write_capture_state_unlocked(state)

    def _write_capture_state_unlocked(self, state: _LogCaptureState) -> None:
        temporary = self.path / f".log-capture-{secrets.token_hex(8)}.tmp"
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as stream:
                json.dump(state, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.log_capture_path)
        finally:
            temporary.unlink(missing_ok=True)


@contextmanager
def _open_owned_log(
    path: Path,
    *,
    mode: Literal["rb", "ab"],
) -> Generator[BinaryIO]:
    try:
        before = os.lstat(path)
    except OSError as exc:
        raise RuntimeError(f"cannot inspect owned log {path}: {exc}") from exc
    _validate_owned_log_stat(before, path=path)
    flags = os.O_RDONLY if mode == "rb" else os.O_WRONLY | os.O_APPEND
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError(f"cannot open owned log {path}: {exc}") from exc
    try:
        opened = os.fstat(descriptor)
        _validate_owned_log_stat(opened, path=path)
        try:
            after = os.lstat(path)
        except OSError as exc:
            raise RuntimeError(f"owned log changed while opening {path}: {exc}") from exc
        _validate_owned_log_stat(after, path=path)
        if not os.path.samestat(before, opened) or not os.path.samestat(opened, after):
            raise RuntimeError(f"owned log changed while opening: {path}")
        with os.fdopen(descriptor, mode) as stream:
            descriptor = -1
            yield stream
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _validate_owned_log_stat(file_stat: os.stat_result, *, path: Path) -> None:
    if not stat.S_ISREG(file_stat.st_mode):
        raise RuntimeError(f"owned log is not a regular file: {path}")
    if file_stat.st_nlink != 1:
        raise RuntimeError(f"owned log must not be hard linked: {path}")


def _owned_log_size(path: Path) -> int:
    with _open_owned_log(path, mode="rb") as stream:
        return os.fstat(stream.fileno()).st_size


def _sha256_file(path: Path, *, owned_root: Path) -> tuple[str, int]:
    snapshot = snapshot_owned_regular_file(path, owned_root=owned_root)
    return snapshot.sha256, snapshot.size_bytes


def read_owned_regular_file_bytes(
    path: Path,
    *,
    owned_root: Path,
    max_bytes: int,
) -> OwnedFileSnapshot:
    """Read and hash one stable owned regular file within a strict byte limit."""
    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    return snapshot_owned_regular_file(
        path,
        owned_root=owned_root,
        max_bytes=max_bytes,
        capture_data=True,
    )


def snapshot_owned_regular_file(
    path: Path,
    *,
    owned_root: Path,
    max_bytes: int | None = None,
    capture_data: bool = False,
) -> OwnedFileSnapshot:
    """Hash a stable owned regular file using a single no-follow descriptor."""
    digest = hashlib.sha256()
    chunks: list[bytes] | None = [] if capture_data else None
    total = 0
    with _open_owned_regular_file(path, owned_root=owned_root) as stream:
        before = os.fstat(stream.fileno())
        if max_bytes is not None and before.st_size > max_bytes:
            raise OwnedFileSizeLimitError(path, max_bytes)
        while True:
            read_size = HASH_CHUNK_BYTES
            if max_bytes is not None:
                read_size = min(read_size, max_bytes + 1 - total)
                if read_size <= 0:
                    raise OwnedFileSizeLimitError(path, max_bytes)
            chunk = _read_owned_file_chunk(stream, read_size)
            if not chunk:
                break
            total += len(chunk)
            if max_bytes is not None and total > max_bytes:
                raise OwnedFileSizeLimitError(path, max_bytes)
            digest.update(chunk)
            if chunks is not None:
                chunks.append(chunk)
        after = os.fstat(stream.fileno())
        _validate_owned_regular_stat(after, path=path)
        if total != before.st_size or _stat_changed(before, after):
            raise RuntimeError(f"owned file changed while reading: {path}")
    return OwnedFileSnapshot(
        size_bytes=total,
        sha256=digest.hexdigest(),
        data=b"".join(chunks) if chunks is not None else None,
    )


def _read_owned_file_chunk(stream: BinaryIO, size: int) -> bytes:
    """Read one bounded artifact chunk; isolated for deterministic race testing."""
    return stream.read(size)


@contextmanager
def _open_owned_regular_file(
    path: Path,
    *,
    owned_root: Path,
) -> Generator[BinaryIO]:
    root = _absolute_path(owned_root)
    target = _absolute_path(path)
    try:
        relative = target.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(f"owned file is outside its root {root}: {target}") from exc
    if relative == Path(".") or not relative.parts:
        raise RuntimeError(f"owned file path names the root directory: {target}")
    if os.open in os.supports_dir_fd and os.stat in os.supports_dir_fd:
        with _open_owned_regular_file_dirfd(root, relative, target=target) as stream:
            yield stream
        return
    with _open_owned_regular_file_path(root, relative, target=target) as stream:
        yield stream


@contextmanager
def _open_owned_regular_file_dirfd(
    root: Path,
    relative: Path,
    *,
    target: Path,
) -> Generator[BinaryIO]:
    descriptors: list[int] = []
    directory_edges: list[tuple[int, str, Path, os.stat_result]] = []
    root_before = _lstat_owned_directory(root)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        root_descriptor = os.open(root, directory_flags)
        descriptors.append(root_descriptor)
        root_opened = os.fstat(root_descriptor)
        _validate_owned_directory_stat(root_opened, path=root)
        if not os.path.samestat(root_before, root_opened):
            raise RuntimeError(f"owned root changed while opening: {root}")
        current_descriptor = root_descriptor
        current_path = root
        for part in relative.parts[:-1]:
            before = _stat_at(current_descriptor, part, path=current_path / part)
            _validate_owned_directory_stat(before, path=current_path / part)
            opened_descriptor = os.open(part, directory_flags, dir_fd=current_descriptor)
            descriptors.append(opened_descriptor)
            opened = os.fstat(opened_descriptor)
            _validate_owned_directory_stat(opened, path=current_path / part)
            after = _stat_at(current_descriptor, part, path=current_path / part)
            _validate_owned_directory_stat(after, path=current_path / part)
            if not os.path.samestat(before, opened) or not os.path.samestat(opened, after):
                raise RuntimeError(f"owned directory changed while opening: {current_path / part}")
            directory_edges.append((current_descriptor, part, current_path / part, opened))
            current_descriptor = opened_descriptor
            current_path /= part
        filename = relative.parts[-1]
        before = _stat_at(current_descriptor, filename, path=target)
        _validate_owned_regular_stat(before, path=target)
        file_flags = (
            os.O_RDONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        descriptor = os.open(filename, file_flags, dir_fd=current_descriptor)
        descriptors.append(descriptor)
        opened = os.fstat(descriptor)
        _validate_owned_regular_stat(opened, path=target)
        after = _stat_at(current_descriptor, filename, path=target)
        _validate_owned_regular_stat(after, path=target)
        if not os.path.samestat(before, opened) or not os.path.samestat(opened, after):
            raise RuntimeError(f"owned file changed while opening: {target}")
        with os.fdopen(descriptor, "rb") as stream:
            descriptors.remove(descriptor)
            try:
                yield stream
            finally:
                final = _stat_at(current_descriptor, filename, path=target)
                _validate_owned_regular_stat(final, path=target)
                if not os.path.samestat(opened, final):
                    raise RuntimeError(f"owned file changed while reading: {target}")
        for parent_descriptor, part, directory_path, opened_directory in reversed(directory_edges):
            final_directory = _stat_at(
                parent_descriptor,
                part,
                path=directory_path,
            )
            _validate_owned_directory_stat(final_directory, path=directory_path)
            if not os.path.samestat(opened_directory, final_directory):
                raise RuntimeError(f"owned directory changed while reading: {target}")
        root_final = os.lstat(root)
        _validate_owned_directory_stat(root_final, path=root)
        if not os.path.samestat(root_opened, root_final):
            raise RuntimeError(f"owned root changed while reading: {root}")
    except OSError as exc:
        raise RuntimeError(f"cannot open owned file {target}: {exc}") from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


@contextmanager
def _open_owned_regular_file_path(
    root: Path,
    relative: Path,
    *,
    target: Path,
) -> Generator[BinaryIO]:
    inspected: list[tuple[Path, os.stat_result]] = []
    root_stat = _lstat_owned_directory(root)
    inspected.append((root, root_stat))
    current = root
    for part in relative.parts[:-1]:
        current /= part
        directory_stat = _lstat_owned_directory(current)
        inspected.append((current, directory_stat))
    try:
        before = os.lstat(target)
    except OSError as exc:
        raise RuntimeError(f"cannot inspect owned file {target}: {exc}") from exc
    _validate_owned_regular_stat(before, path=target)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = -1
    try:
        descriptor = os.open(target, flags)
        opened = os.fstat(descriptor)
        _validate_owned_regular_stat(opened, path=target)
        after = os.lstat(target)
        _validate_owned_regular_stat(after, path=target)
        if not os.path.samestat(before, opened) or not os.path.samestat(opened, after):
            raise RuntimeError(f"owned file changed while opening: {target}")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            try:
                yield stream
            finally:
                final = os.lstat(target)
                _validate_owned_regular_stat(final, path=target)
                if not os.path.samestat(opened, final):
                    raise RuntimeError(f"owned file changed while reading: {target}")
        for inspected_path, original in inspected:
            current_stat = os.lstat(inspected_path)
            _validate_owned_directory_stat(current_stat, path=inspected_path)
            if not os.path.samestat(original, current_stat):
                raise RuntimeError(f"owned directory changed while reading: {inspected_path}")
    except OSError as exc:
        raise RuntimeError(f"cannot open owned file {target}: {exc}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _lstat_owned_directory(path: Path) -> os.stat_result:
    try:
        result = os.lstat(path)
    except OSError as exc:
        raise RuntimeError(f"cannot inspect owned directory {path}: {exc}") from exc
    _validate_owned_directory_stat(result, path=path)
    return result


def _stat_at(directory_descriptor: int, name: str, *, path: Path) -> os.stat_result:
    try:
        return os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError(f"cannot inspect owned path {path}: {exc}") from exc


def _validate_owned_directory_stat(file_stat: os.stat_result, *, path: Path) -> None:
    if not stat.S_ISDIR(file_stat.st_mode) or _stat_is_reparse_point(file_stat):
        raise RuntimeError(f"owned path component is not a regular directory: {path}")


def _validate_owned_regular_stat(file_stat: os.stat_result, *, path: Path) -> None:
    if not stat.S_ISREG(file_stat.st_mode) or _stat_is_reparse_point(file_stat):
        raise RuntimeError(f"owned file is not a regular file: {path}")
    if file_stat.st_nlink != 1:
        raise RuntimeError(f"owned file must not be hard linked: {path}")


def _stat_is_reparse_point(file_stat: os.stat_result) -> bool:
    attributes = getattr(file_stat, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(attributes & reparse_flag)


def _stat_changed(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        not os.path.samestat(before, after)
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
        or before.st_nlink != after.st_nlink
    )


def _complete_utf8_prefix(payload: bytes, limit: int) -> bytes:
    if len(payload) <= limit:
        return payload
    prefix = payload[:limit]
    while prefix:
        try:
            prefix.decode("utf-8")
        except UnicodeDecodeError as exc:
            prefix = prefix[: exc.start]
            continue
        return prefix
    return b""


def _validate_capture_state(raw: object, *, path: Path) -> _LogCaptureState:
    if not isinstance(raw, dict):
        raise RuntimeError(f"log capture state was not an object: {path}")
    typed = cast(dict[str, object], raw)
    if typed.get("schema_version") != LOG_CAPTURE_SCHEMA:
        raise RuntimeError(f"unsupported log capture state schema: {path}")
    max_per_stream = typed.get("max_bytes_per_stream")
    max_per_job = typed.get("max_bytes_per_job")
    raw_streams = typed.get("streams")
    if (
        not isinstance(max_per_stream, int)
        or isinstance(max_per_stream, bool)
        or max_per_stream <= 0
        or not isinstance(max_per_job, int)
        or isinstance(max_per_job, bool)
        or max_per_job <= 0
        or not isinstance(raw_streams, dict)
    ):
        raise RuntimeError(f"invalid log capture quota state: {path}")
    if set(cast(dict[object, object], raw_streams)) != {"stdout", "stderr"}:
        raise RuntimeError(f"log capture state has unexpected streams: {path}")
    streams: dict[str, _StreamCaptureState] = {}
    for name in ("stdout", "stderr"):
        raw_stream = cast(dict[object, object], raw_streams).get(name)
        if not isinstance(raw_stream, dict):
            raise RuntimeError(f"missing {name} log capture state: {path}")
        stream = cast(dict[str, object], raw_stream)
        integers: dict[str, int] = {}
        for field in ("observed_bytes", "persisted_bytes", "dropped_bytes"):
            value = stream.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise RuntimeError(f"invalid {name}.{field} log capture state: {path}")
            integers[field] = value
        truncated = stream.get("truncated")
        event_recorded = stream.get("truncation_event_recorded")
        if not isinstance(truncated, bool) or not isinstance(event_recorded, bool):
            raise RuntimeError(f"invalid {name} truncation state: {path}")
        if integers["observed_bytes"] != (integers["persisted_bytes"] + integers["dropped_bytes"]):
            raise RuntimeError(f"inconsistent {name} byte accounting: {path}")
        if truncated != (integers["dropped_bytes"] > 0):
            raise RuntimeError(f"inconsistent {name} truncation accounting: {path}")
        if event_recorded and not truncated:
            raise RuntimeError(f"invalid {name} truncation event state: {path}")
        streams[name] = {
            "observed_bytes": integers["observed_bytes"],
            "persisted_bytes": integers["persisted_bytes"],
            "dropped_bytes": integers["dropped_bytes"],
            "truncated": truncated,
            "truncation_event_recorded": event_recorded,
        }
        if integers["persisted_bytes"] > max_per_stream:
            raise RuntimeError(f"{name} exceeds the recorded stream quota: {path}")
    if sum(item["persisted_bytes"] for item in streams.values()) > max_per_job:
        raise RuntimeError(f"persisted logs exceed the recorded whole-job quota: {path}")
    state: _LogCaptureState = {
        "schema_version": LOG_CAPTURE_SCHEMA,
        "max_bytes_per_stream": max_per_stream,
        "max_bytes_per_job": max_per_job,
        "streams": streams,
    }
    raw_pending = typed.get("pending")
    if raw_pending is None:
        return state
    if not isinstance(raw_pending, dict):
        raise RuntimeError(f"invalid pending log capture state: {path}")
    pending = cast(dict[str, object], raw_pending)
    stream_name = pending.get("stream")
    if stream_name not in {"stdout", "stderr"}:
        raise RuntimeError(f"invalid pending log capture stream: {path}")
    typed_stream = cast(Literal["stdout", "stderr"], stream_name)
    pending_integers: dict[str, int] = {}
    for field in (
        "before_persisted_bytes",
        "observed_bytes",
        "accepted_bytes",
        "dropped_bytes",
    ):
        value = pending.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RuntimeError(f"invalid pending {field}: {path}")
        pending_integers[field] = value
    if pending_integers["before_persisted_bytes"] != streams[typed_stream]["persisted_bytes"]:
        raise RuntimeError(f"pending log capture baseline differs from stream state: {path}")
    if pending_integers["observed_bytes"] != (
        pending_integers["accepted_bytes"] + pending_integers["dropped_bytes"]
    ):
        raise RuntimeError(f"inconsistent pending log byte accounting: {path}")
    if (
        streams[typed_stream]["persisted_bytes"] + pending_integers["accepted_bytes"]
        > max_per_stream
    ):
        raise RuntimeError(f"pending log capture exceeds the stream quota: {path}")
    if (
        sum(item["persisted_bytes"] for item in streams.values())
        + pending_integers["accepted_bytes"]
        > max_per_job
    ):
        raise RuntimeError(f"pending log capture exceeds the whole-job quota: {path}")
    state["pending"] = {
        "stream": typed_stream,
        "before_persisted_bytes": pending_integers["before_persisted_bytes"],
        "observed_bytes": pending_integers["observed_bytes"],
        "accepted_bytes": pending_integers["accepted_bytes"],
        "dropped_bytes": pending_integers["dropped_bytes"],
    }
    return state
