from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO, cast

import pytest

import clio_relay.relay_ops as relay_ops_module
import clio_relay.spool as spool_module
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import ArtifactRef, JarvisRunSpec, JobKind, JobState, RelayJob
from clio_relay.relay_ops import (
    MAX_ARTIFACT_CONTENT_BYTES,
    observe_until_terminal,
    read_artifact_bytes,
)
from clio_relay.spool import ARTIFACT_OWNERSHIP_SCHEMA, MAX_LOG_READ_BYTES, JobSpool


def _owned_artifact_metadata(root: Path) -> dict[str, str]:
    return {
        "ownership_schema": ARTIFACT_OWNERSHIP_SCHEMA,
        "owned_root_uri": root.absolute().as_uri(),
    }


def test_observe_until_terminal_returns_current_durable_job_on_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="long-queued-run"),
            idempotency_key="long-queued-run",
        )
    )

    def expire(
        _queue: ClioCoreQueue,
        _job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> RelayJob:
        assert timeout_seconds == 1
        assert poll_seconds == 0.1
        raise TimeoutError("observation expired")

    monkeypatch.setattr(relay_ops_module, "wait_for_terminal", expire)
    observed = observe_until_terminal(
        queue,
        job.job_id,
        timeout_seconds=1,
        poll_seconds=0.1,
    )

    assert observed.job_id == job.job_id
    assert observed.state is JobState.QUEUED
    assert observed.observation.outcome == "observation_unknown"
    assert observed.observation.scheduler_action == "none"
    assert queue.get_job(job.job_id).state is JobState.QUEUED


@pytest.mark.parametrize(
    ("timeout_seconds", "poll_seconds"),
    [(float("inf"), 0.1), (1.0, float("inf"))],
)
def test_observe_until_terminal_rejects_nonfinite_bounds(
    tmp_path: Path,
    timeout_seconds: float,
    poll_seconds: float,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    with pytest.raises(ConfigurationError, match="positive and finite"):
        observe_until_terminal(
            queue,
            "job_00000000000000000000000000000001",
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )


def test_read_artifact_rejects_tampered_backing_file(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="local",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="artifact-integrity",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    artifact_path = owned_root / "result.json"
    original = b'{"status":"ok"}\n'
    artifact_path.write_bytes(original)
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=artifact_path.as_uri(),
            kind="result",
            size_bytes=len(original),
            sha256=hashlib.sha256(original).hexdigest(),
            metadata=_owned_artifact_metadata(owned_root),
        )
    )

    artifact_path.write_bytes(b'{"status":"tampered"}\n')

    with pytest.raises(RelayError, match="artifact size does not match"):
        read_artifact_bytes(queue, artifact.artifact_id)


def test_read_artifact_verifies_and_returns_original_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="local",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="artifact-integrity-ok",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    artifact_path = owned_root / "result.json"
    data = b'{"status":"ok"}\n'
    artifact_path.write_bytes(data)
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=artifact_path.as_uri(),
            kind="result",
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
            metadata=_owned_artifact_metadata(owned_root),
        )
    )

    def forbidden_path_read_bytes(_path: Path) -> bytes:
        raise AssertionError("artifact reads must not use Path.read_bytes")

    monkeypatch.setattr(Path, "read_bytes", forbidden_path_read_bytes)
    payload = read_artifact_bytes(queue, artifact.artifact_id)

    assert payload["artifact"] == artifact.model_dump(mode="json")


def test_read_artifact_rejects_unbounded_json_transfer(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="local",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="artifact-transfer-bound",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    artifact_path = owned_root / "large.log"
    with artifact_path.open("wb") as stream:
        stream.truncate(MAX_ARTIFACT_CONTENT_BYTES + 1)
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=artifact_path.as_uri(),
            kind="stdout",
            size_bytes=MAX_ARTIFACT_CONTENT_BYTES + 1,
            metadata=_owned_artifact_metadata(owned_root),
        )
    )

    with pytest.raises(RelayError, match="transfer limit"):
        read_artifact_bytes(queue, artifact.artifact_id)


def test_read_artifact_rejects_paths_outside_root_hardlinks_and_symlinks(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="local",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="artifact-path-safety",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")

    outside_artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=outside.as_uri(),
            kind="outside",
            metadata=_owned_artifact_metadata(owned_root),
        )
    )
    with pytest.raises(RelayError, match="outside its root"):
        read_artifact_bytes(queue, outside_artifact.artifact_id)

    hardlink = owned_root / "hardlink.bin"
    os.link(outside, hardlink)
    hardlink_artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=hardlink.as_uri(),
            kind="hardlink",
            metadata=_owned_artifact_metadata(owned_root),
        )
    )
    with pytest.raises(RelayError, match="hard linked"):
        read_artifact_bytes(queue, hardlink_artifact.artifact_id)

    symlink = owned_root / "symlink.bin"
    try:
        symlink.symlink_to(outside)
    except OSError:
        assert os.name == "nt"
    else:
        symlink_artifact = queue.append_artifact(
            ArtifactRef(
                job_id=job.job_id,
                uri=symlink.as_uri(),
                kind="symlink",
                metadata=_owned_artifact_metadata(owned_root),
            )
        )
        with pytest.raises(RelayError, match="not a regular file|cannot open owned file"):
            read_artifact_bytes(queue, symlink_artifact.artifact_id)


def test_read_artifact_rejects_fifo_device_or_directory_without_blocking(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="local",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="artifact-special-file",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    if os.name == "posix":
        fifo = owned_root / "artifact.fifo"
        os.mkfifo(fifo)
        fifo_artifact = queue.append_artifact(
            ArtifactRef(
                job_id=job.job_id,
                uri=fifo.as_uri(),
                kind="fifo",
                metadata=_owned_artifact_metadata(owned_root),
            )
        )
        with pytest.raises(RelayError, match="not a regular file"):
            read_artifact_bytes(queue, fifo_artifact.artifact_id)

        zero = Path("/dev/zero")
        zero_artifact = queue.append_artifact(
            ArtifactRef(
                job_id=job.job_id,
                uri=zero.as_uri(),
                kind="device",
                metadata=_owned_artifact_metadata(zero.parent),
            )
        )
        with pytest.raises(RelayError, match="owned-root metadata does not name"):
            read_artifact_bytes(queue, zero_artifact.artifact_id)
    else:
        directory = owned_root / "artifact-directory"
        directory.mkdir()
        directory_artifact = queue.append_artifact(
            ArtifactRef(
                job_id=job.job_id,
                uri=directory.as_uri(),
                kind="directory",
                metadata=_owned_artifact_metadata(owned_root),
            )
        )
        with pytest.raises(RelayError, match="not a regular file"):
            read_artifact_bytes(queue, directory_artifact.artifact_id)


def test_read_artifact_growth_is_bounded_to_limit_plus_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="local",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="artifact-growth-bound",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    artifact_path = owned_root / "growing.bin"
    original = b"1234"
    artifact_path.write_bytes(original)
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=artifact_path.as_uri(),
            kind="growing",
            size_bytes=len(original),
            sha256=hashlib.sha256(original).hexdigest(),
            metadata=_owned_artifact_metadata(owned_root),
        )
    )
    original_read = cast(
        Callable[[BinaryIO, int], bytes],
        spool_module.__dict__["_read_owned_file_chunk"],
    )
    requested_sizes: list[int] = []
    grew = False

    def grow_after_read(stream: BinaryIO, size: int) -> bytes:
        nonlocal grew
        requested_sizes.append(size)
        chunk = original_read(stream, size)
        if not grew:
            grew = True
            with artifact_path.open("ab") as output:
                output.write(b"56")
        return chunk

    monkeypatch.setattr(relay_ops_module, "MAX_ARTIFACT_CONTENT_BYTES", 4)
    monkeypatch.setattr(spool_module, "_read_owned_file_chunk", grow_after_read)

    with pytest.raises(RelayError, match="4-byte transfer limit"):
        read_artifact_bytes(queue, artifact.artifact_id)
    assert requested_sizes == [5, 1]


def test_read_artifact_rejects_swap_between_inspection_and_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="local",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="artifact-open-swap",
        )
    )
    owned_root = tmp_path / "spool" / job.job_id
    owned_root.mkdir(parents=True)
    artifact_path = owned_root / "swapped.bin"
    original = b"original"
    artifact_path.write_bytes(original)
    replacement = owned_root / "replacement.bin"
    replacement.write_bytes(b"replacement")
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=artifact_path.as_uri(),
            kind="swapped",
            size_bytes=len(original),
            sha256=hashlib.sha256(original).hexdigest(),
            metadata=_owned_artifact_metadata(owned_root),
        )
    )
    original_open = spool_module.os.open
    swapped = False

    def swap_before_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        normalized_path = os.fsdecode(os.fspath(path))
        is_target = (
            normalized_path == os.fspath(artifact_path)
            if dir_fd is None
            else normalized_path == artifact_path.name
        )
        if is_target and not swapped:
            swapped = True
            artifact_path.unlink()
            replacement.replace(artifact_path)
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(spool_module.os, "open", swap_before_open)

    with pytest.raises(RelayError, match="changed while opening"):
        read_artifact_bytes(queue, artifact.artifact_id)


def test_job_spool_reads_only_requested_log_range(tmp_path: Path) -> None:
    job = RelayJob(
        cluster="local",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="bounded-log-read",
    )
    spool = JobSpool(tmp_path / "spool", job)
    spool.initialize()
    spool.append_stdout("0123456789" * 100_000)

    text, next_offset, eof = spool.read_log("stdout", offset=500_000, limit=7)

    assert text == "0123456"
    assert next_offset == 500_007
    assert eof is False
    with pytest.raises(ValueError, match="limit cannot exceed"):
        spool.read_log("stdout", limit=MAX_LOG_READ_BYTES + 1)
