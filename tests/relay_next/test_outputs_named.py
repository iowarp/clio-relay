"""Outputs are named where they live, in the outputs directory, never through relay's link."""

from __future__ import annotations

from typing import Any

import pytest

from clio_relay.serve.report import listed_artifacts
from clio_relay.surface.package_progress import (
    _shared,  # pyright: ignore[reportPrivateUsage]
)


class _Files:
    def __init__(self, said: bytes | None) -> None:
        self.said = said
        self.read_paths: list[str] = []

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        self.read_paths.append(path)
        if self.said is None:
            raise FileNotFoundError(path)
        return self.said


@pytest.mark.asyncio
async def test_progress_reads_the_shared_directory_the_site_script_wrote() -> None:
    job = "/home/u/.clio-relay/jobs/op_x"
    files: Any = _Files(b"/home/u/clio-relay-runs/op_x/\n")
    assert await _shared(files, job) == "/home/u/clio-relay-runs/op_x"
    assert files.read_paths == [f"{job}/relay.shared"]
    for said in (None, b"", b"relative/path"):
        assert await _shared(_Files(said), job) == f"{job}/.relay-jarvis/shared"


def test_a_serve_artifact_with_an_absolute_name_keeps_its_path() -> None:
    plan: Any = type("Plan", (), {"job_dir": "/home/u/.clio-relay/jobs/op_s/"})()
    listing = b"5\tout.log\n7\t/home/u/clio-relay-runs/op_s/p/data.bp\n"
    found = listed_artifacts(plan, listing, set())
    assert [(item.name, item.path, item.size) for item in found] == [
        ("out.log", "/home/u/.clio-relay/jobs/op_s/out.log", 5),
        ("/home/u/clio-relay-runs/op_s/p/data.bp", "/home/u/clio-relay-runs/op_s/p/data.bp", 7),
    ]
