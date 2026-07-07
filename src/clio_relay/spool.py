"""Spool directory helpers for logs and artifact backing files."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

from clio_relay.models import ArtifactRef, RelayJob


class JobSpool:
    """Per-job execution spool rooted on cluster-accessible storage."""

    def __init__(self, root: Path, job: RelayJob) -> None:
        self.root = root
        self.job = job
        self.path = root / job.job_id

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

    def append_stdout(self, text: str) -> None:
        """Append captured standard output."""
        with (self.path / "stdout.log").open("a", encoding="utf-8") as stream:
            stream.write(text)

    def append_stderr(self, text: str) -> None:
        """Append captured standard error."""
        with (self.path / "stderr.log").open("a", encoding="utf-8") as stream:
            stream.write(text)

    def append_log(self, stream_name: Literal["stdout", "stderr"], text: str) -> None:
        """Append captured output to a named job log."""
        if stream_name == "stdout":
            self.append_stdout(text)
            return
        self.append_stderr(text)

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
        path = self.path / f"{stream_name}.log"
        if not path.exists():
            return "", offset, True
        data = path.read_bytes()
        chunk = data[offset : offset + limit]
        next_offset = offset + len(chunk)
        return chunk.decode("utf-8", errors="replace"), next_offset, next_offset >= len(data)

    def artifact_for(self, path: Path, *, kind: str) -> ArtifactRef:
        """Build an artifact reference for a spool-backed path."""
        digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
        size = path.stat().st_size if path.exists() else None
        return ArtifactRef(
            job_id=self.job.job_id,
            uri=path.resolve().as_uri(),
            kind=kind,
            size_bytes=size,
            sha256=digest,
        )
