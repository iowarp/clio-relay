from __future__ import annotations

import json
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import BinaryIO, cast

import pytest

import clio_relay.spool as spool_module
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.models import JarvisRunSpec, JobKind, RelayJob
from clio_relay.spool import (
    ARTIFACT_OWNERSHIP_SCHEMA,
    JobSpool,
    read_owned_regular_file_bytes,
)


def _job(key: str = "spool-test") -> RelayJob:
    return RelayJob(
        cluster="local",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
    )


def test_operator_configured_long_spool_root_preserves_artifact_provenance(
    tmp_path: Path,
) -> None:
    """Spool and artifact I/O may use extended paths without exposing them."""
    root = tmp_path.joinpath(*(f"operator-spool-{index}-{'x' * 72}" for index in range(3)))
    job = _job("long-spool-root")
    spool = JobSpool(root, job)
    spool.initialize()
    pipeline = spool.write_pipeline("name: long-path\n")
    spool.append_stdout("completed\n")

    artifact = spool.artifact_for(pipeline, kind="jarvis_pipeline")
    text, _, eof = spool.read_log("stdout")
    reopened = JobSpool(internal_filesystem_path(root, force_extended=True), job)

    assert spool.root == root
    assert reopened.root == root.absolute()
    assert pipeline == spool.path / "pipeline.yaml"
    assert artifact.uri == pipeline.absolute().as_uri()
    assert "\\\\?\\" not in artifact.uri
    assert artifact.metadata["owned_root_uri"] == spool.path.absolute().as_uri()
    assert text == "completed\n"
    assert eof is True
    assert internal_filesystem_path(pipeline).read_text(encoding="utf-8") == ("name: long-path\n")


def test_long_spool_errors_expose_only_logical_paths(tmp_path: Path) -> None:
    """Durable spool diagnostics never expose the private Windows namespace."""
    root = tmp_path.joinpath(*(f"error-spool-{index}-{'x' * 72}" for index in range(3)))
    spool = JobSpool(
        root,
        _job("long-spool-error"),
        max_log_bytes_per_stream=4,
        max_log_bytes_per_job=8,
    )
    internal_filesystem_path(spool.path, force_extended=True).mkdir(parents=True)
    internal_filesystem_path(spool.path / "stdout.log").write_bytes(b"oversized")

    with pytest.raises(RuntimeError, match="stream quota") as caught:
        spool.initialize()

    assert "\\\\?\\" not in str(caught.value)
    assert str(spool.path / "stdout.log") in str(caught.value)


def test_spool_enforces_stream_and_job_byte_quotas_without_splitting_utf8(
    tmp_path: Path,
) -> None:
    spool = JobSpool(
        tmp_path / "spool",
        _job(),
        max_log_bytes_per_stream=7,
        max_log_bytes_per_job=10,
    )
    spool.initialize()

    first = spool.append_stdout("éééé")
    assert first.accepted_text == "ééé"
    assert first.accepted_bytes == 6
    assert first.dropped_bytes == 2
    assert first.truncation_event_required is True

    spool.mark_truncation_event_recorded("stdout")
    second = spool.append_stdout("x")
    assert second.accepted_text == "x"
    assert second.truncation_event_required is False

    stderr = spool.append_stderr("abcd")
    assert stderr.accepted_text == "abc"
    assert stderr.dropped_bytes == 1
    assert stderr.persisted_job_bytes == 10

    assert (spool.path / "stdout.log").read_bytes() == "éééx".encode()
    assert (spool.path / "stderr.log").read_bytes() == b"abc"
    assert spool.capture_summary() == {
        "schema_version": "clio-relay.log-capture.v1",
        "max_bytes_per_stream": 7,
        "max_bytes_per_job": 10,
        "observed_bytes": 13,
        "persisted_bytes": 10,
        "dropped_bytes": 3,
        "truncated": True,
        "streams": {
            "stdout": {
                "observed_bytes": 9,
                "persisted_bytes": 7,
                "dropped_bytes": 2,
                "truncated": True,
                "truncation_event_recorded": True,
            },
            "stderr": {
                "observed_bytes": 4,
                "persisted_bytes": 3,
                "dropped_bytes": 1,
                "truncated": True,
                "truncation_event_recorded": False,
            },
        },
    }


def test_spool_capture_accounting_survives_reopen_and_rejects_quota_drift(
    tmp_path: Path,
) -> None:
    job = _job("spool-reopen")
    spool = JobSpool(
        tmp_path / "spool",
        job,
        max_log_bytes_per_stream=4,
        max_log_bytes_per_job=8,
    )
    spool.initialize()
    spool.append_stdout("abcdef")

    reopened = JobSpool(
        tmp_path / "spool",
        job,
        max_log_bytes_per_stream=4,
        max_log_bytes_per_job=8,
    )
    reopened.initialize()
    assert reopened.capture_summary()["dropped_bytes"] == 2

    drifted = JobSpool(
        tmp_path / "spool",
        job,
        max_log_bytes_per_stream=5,
        max_log_bytes_per_job=8,
    )
    with pytest.raises(RuntimeError, match="quotas changed"):
        drifted.initialize()


def test_spool_serializes_concurrent_stream_writers_against_whole_job_quota(
    tmp_path: Path,
) -> None:
    spool = JobSpool(
        tmp_path / "spool",
        _job("spool-concurrent"),
        max_log_bytes_per_stream=1_000,
        max_log_bytes_per_job=1_500,
    )
    spool.initialize()

    def write(stream: str) -> None:
        typed_stream = "stdout" if stream == "stdout" else "stderr"
        for _ in range(20):
            spool.append_log(typed_stream, "x" * 100)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(write, ["stdout", "stderr"]))

    summary = spool.capture_summary()
    assert summary["observed_bytes"] == 4_000
    assert summary["persisted_bytes"] == 1_500
    assert summary["dropped_bytes"] == 2_500
    assert (spool.path / "stdout.log").stat().st_size <= 1_000
    assert (spool.path / "stderr.log").stat().st_size <= 1_000
    assert (spool.path / "stdout.log").stat().st_size + (
        spool.path / "stderr.log"
    ).stat().st_size == 1_500


def test_spool_rejects_preexisting_logs_above_configured_quota(tmp_path: Path) -> None:
    job = _job("spool-preseeded-overflow")
    spool = JobSpool(
        tmp_path / "spool",
        job,
        max_log_bytes_per_stream=4,
        max_log_bytes_per_job=8,
    )
    spool.initialize()
    spool.log_capture_path.unlink()
    (spool.path / "stdout.log").write_bytes(b"overflow")

    with pytest.raises(RuntimeError, match="exceeds the configured stream quota"):
        spool.initialize()


def test_spool_rejects_forged_state_above_recorded_quotas(tmp_path: Path) -> None:
    job = _job("spool-forged-overflow")
    spool = JobSpool(
        tmp_path / "spool",
        job,
        max_log_bytes_per_stream=4,
        max_log_bytes_per_job=8,
    )
    spool.initialize()
    (spool.path / "stdout.log").write_bytes(b"overflow")
    state = json.loads(spool.log_capture_path.read_text(encoding="utf-8"))
    state["streams"]["stdout"].update(
        {
            "observed_bytes": 8,
            "persisted_bytes": 8,
            "dropped_bytes": 0,
        }
    )
    spool.log_capture_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(RuntimeError, match="exceeds the recorded stream quota"):
        spool.initialize()


def test_spool_rejects_redirected_log_file(tmp_path: Path) -> None:
    job = _job("spool-redirect")
    spool = JobSpool(tmp_path / "spool", job)
    spool.initialize()
    outside = tmp_path / "outside.log"
    outside.write_text("outside", encoding="utf-8")
    stdout = spool.path / "stdout.log"
    stdout.unlink()
    if os.name == "nt":
        os.link(outside, stdout)
        expected = "hard linked"
    else:
        stdout.symlink_to(outside)
        expected = "not a regular file|cannot open owned log"

    with pytest.raises(RuntimeError, match=expected):
        spool.append_stdout("must-not-escape")
    assert outside.read_text(encoding="utf-8") == "outside"


def test_spool_indexes_only_stable_regular_files_owned_by_the_job(tmp_path: Path) -> None:
    spool = JobSpool(tmp_path / "spool", _job("artifact-owned-file"))
    spool.initialize()
    artifact_path = spool.path / "result.json"
    artifact_path.write_bytes(b'{"ok":true}\n')

    artifact = spool.artifact_for(artifact_path, kind="result")

    assert artifact.size_bytes == 12
    assert artifact.sha256 is not None
    assert artifact.metadata == {
        "ownership_schema": ARTIFACT_OWNERSHIP_SCHEMA,
        "owned_root_uri": spool.path.absolute().as_uri(),
    }

    nested_path = spool.path / "nested" / "result.bin"
    nested_path.parent.mkdir()
    nested_path.write_bytes(b"nested")
    nested = spool.artifact_for(nested_path, kind="nested")
    assert nested.size_bytes == 6

    outside = tmp_path / "outside.json"
    outside.write_text("outside", encoding="utf-8")
    with pytest.raises(RuntimeError, match="outside its root"):
        spool.artifact_for(outside, kind="outside")

    hardlink = spool.path / "hardlink.json"
    os.link(outside, hardlink)
    with pytest.raises(RuntimeError, match="hard linked"):
        spool.artifact_for(hardlink, kind="hardlink")


def test_spool_rejects_symlink_reparse_and_nonregular_artifacts_without_blocking(
    tmp_path: Path,
) -> None:
    spool = JobSpool(tmp_path / "spool", _job("artifact-nonregular"))
    spool.initialize()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    redirected = spool.path / "redirected.bin"
    try:
        redirected.symlink_to(outside)
    except OSError:
        assert os.name == "nt"
    else:
        with pytest.raises(RuntimeError, match="not a regular file|cannot open owned file"):
            spool.artifact_for(redirected, kind="redirected")

    if os.name == "posix":
        fifo = spool.path / "artifact.fifo"
        os.mkfifo(fifo)
        with pytest.raises(RuntimeError, match="not a regular file"):
            spool.artifact_for(fifo, kind="fifo")
        with pytest.raises(RuntimeError, match="not a regular file"):
            read_owned_regular_file_bytes(
                Path("/dev/zero"),
                owned_root=Path("/dev"),
                max_bytes=1,
            )
    else:
        directory = spool.path / "artifact-directory"
        directory.mkdir()
        with pytest.raises(RuntimeError, match="not a regular file"):
            spool.artifact_for(directory, kind="directory")


def test_spool_rejects_file_growth_during_artifact_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = JobSpool(tmp_path / "spool", _job("artifact-growth"))
    spool.initialize()
    artifact_path = spool.path / "growing.bin"
    artifact_path.write_bytes(b"before")
    original_read = cast(
        Callable[[BinaryIO, int], bytes],
        spool_module.__dict__["_read_owned_file_chunk"],
    )
    grew = False

    def grow_after_read(stream: BinaryIO, size: int) -> bytes:
        nonlocal grew
        chunk = original_read(stream, size)
        if not grew:
            grew = True
            with artifact_path.open("ab") as output:
                output.write(b"after")
        return chunk

    monkeypatch.setattr(spool_module, "_read_owned_file_chunk", grow_after_read)

    with pytest.raises(RuntimeError, match="changed while reading"):
        spool.artifact_for(artifact_path, kind="growing")


def test_spool_rejects_path_swap_between_inspection_and_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = JobSpool(tmp_path / "spool", _job("artifact-swap"))
    spool.initialize()
    artifact_path = spool.path / "swapped.bin"
    artifact_path.write_bytes(b"original")
    replacement = spool.path / "replacement.bin"
    replacement.write_bytes(b"replacement")
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

    with pytest.raises(RuntimeError, match="changed while opening"):
        spool.artifact_for(artifact_path, kind="swapped")


@pytest.mark.parametrize("log_write_completed", [False, True])
def test_spool_recovers_write_ahead_capture_state_after_interrupted_append(
    tmp_path: Path,
    log_write_completed: bool,
) -> None:
    job = _job(f"spool-recover-{log_write_completed}")
    spool = JobSpool(
        tmp_path / "spool",
        job,
        max_log_bytes_per_stream=8,
        max_log_bytes_per_job=16,
    )
    spool.initialize()
    state = json.loads(spool.log_capture_path.read_text(encoding="utf-8"))
    state["pending"] = {
        "stream": "stdout",
        "before_persisted_bytes": 0,
        "observed_bytes": 3,
        "accepted_bytes": 2,
        "dropped_bytes": 1,
    }
    spool.log_capture_path.write_text(json.dumps(state), encoding="utf-8")
    if log_write_completed:
        (spool.path / "stdout.log").write_bytes(b"ok")

    reopened = JobSpool(
        tmp_path / "spool",
        job,
        max_log_bytes_per_stream=8,
        max_log_bytes_per_job=16,
    )
    reopened.initialize()

    summary = reopened.capture_summary()
    assert summary["observed_bytes"] == 3
    assert summary["persisted_bytes"] == (2 if log_write_completed else 0)
    assert summary["dropped_bytes"] == (1 if log_write_completed else 3)
    assert summary["truncated"] is True
    assert "pending" not in json.loads(reopened.log_capture_path.read_text(encoding="utf-8"))
