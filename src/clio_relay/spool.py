"""Spool directory helpers for logs and artifact backing files."""

from __future__ import annotations

import hashlib
from pathlib import Path

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

    def append_stdout(self, text: str) -> None:
        """Append captured standard output."""
        with (self.path / "stdout.log").open("a", encoding="utf-8") as stream:
            stream.write(text)

    def append_stderr(self, text: str) -> None:
        """Append captured standard error."""
        with (self.path / "stderr.log").open("a", encoding="utf-8") as stream:
            stream.write(text)

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
