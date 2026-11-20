from __future__ import annotations

import gzip
import io
import os
import tarfile
from pathlib import Path

import pytest

from clio_relay.jarvis_site import (
    SOURCE_FILE,
    archive_package,
    inspect_archive,
    package_files,
    read_archive,
)
from clio_relay.jarvis_submit import (
    ARCHIVE_DIR,
    SITE_SCRIPT,
    direct_job,
    input_paths,
    pipeline_document,
)
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

from .conftest import make_repo


def _tar(members: list[tuple[tarfile.TarInfo, bytes]]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        for info, body in members:
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return gzip.compress(raw.getvalue())


def test_the_archive_holds_the_package_and_its_shared_code_only(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    paths = [path for path, _ in package_files(repo, "demo.hello")]
    assert paths == [
        "demo/README.md",
        "demo/demo/__init__.py",
        "demo/demo/_contract/__init__.py",
        "demo/demo/_contract/base.py",
        "demo/demo/hello/__init__.py",
        "demo/demo/hello/pkg.py",
        "schemas/package-contract.v1.json",
    ]


def test_the_same_files_give_the_same_bytes(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    first = archive_package(repo, "demo.hello")
    pkg = repo / "demo" / "hello" / "pkg.py"
    os.utime(pkg, (1_000_000, 1_000_000))
    assert archive_package(repo, "demo.hello") == first
    pkg.write_text(pkg.read_text() + "# changed\n")
    assert archive_package(repo, "demo.hello") != first


def test_inspect_reads_files_and_source(tmp_path: Path) -> None:
    source = {"repository": "https://example.org/demo.git", "commit": "c" * 40}
    data = archive_package(make_repo(tmp_path), "demo.hello", source=source)
    info = inspect_archive(data, "demo.hello")
    assert info.source == source
    assert SOURCE_FILE not in [item.path for item in info.files]
    assert "demo/demo/hello/pkg.py" in [item.path for item in info.files]
    with pytest.raises(Refusal) as raised:
        inspect_archive(data, "demo.other")
    assert raised.value.reason == RefusalReason.INVALID_SPEC


@pytest.mark.parametrize(
    ("member", "kind"),
    [
        ("../escape.py", tarfile.REGTYPE),
        ("/abs/pkg.py", tarfile.REGTYPE),
        ("demo/demo/hello/link", tarfile.SYMTYPE),
        ("demo/demo/hello/fifo", tarfile.FIFOTYPE),
    ],
)
def test_inspect_refuses_odd_members(member: str, kind: bytes) -> None:
    good = tarfile.TarInfo("demo/demo/hello/pkg.py")
    odd = tarfile.TarInfo(member)
    odd.type = kind
    if kind == tarfile.SYMTYPE:
        odd.linkname = "/etc/passwd"
    with pytest.raises(Refusal) as raised:
        inspect_archive(_tar([(good, b"x"), (odd, b"")]), "demo.hello")
    assert raised.value.reason == RefusalReason.INVALID_SPEC


def test_inspect_refuses_what_is_not_a_gzipped_tar() -> None:
    with pytest.raises(Refusal) as raised:
        inspect_archive(b"not an archive", "demo.hello")
    assert raised.value.reason == RefusalReason.INVALID_SPEC


def test_archive_refuses_a_name_the_repository_does_not_hold(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    with pytest.raises(Refusal):
        archive_package(repo, "demo.missing")
    with pytest.raises(Refusal):
        archive_package(repo, "other.hello")
    for bad in ("nodot", "demo.he-llo", "demo.hello.x", "1demo.hello", "dém.hello"):
        with pytest.raises(Refusal):
            archive_package(repo, bad)


def test_read_archive_explains_how_to_stage(tmp_path: Path) -> None:
    content = ContentStore(tmp_path / "content")
    with pytest.raises(Refusal) as raised:
        read_archive(content, "demo.hello", "a" * 64)
    assert raised.value.reason == RefusalReason.INPUT_MISSING
    data = archive_package(make_repo(tmp_path), "demo.hello")
    digest = content.put(data)
    assert read_archive(content, "demo.hello", digest).sha256 == digest


def test_direct_job_keeps_resources_and_inputs() -> None:
    spec = JobSpec.model_validate(
        {
            "package": {
                "name": "demo.hello",
                "version": "1",
                "sha256": "b" * 64,
                "parameters": {"flag": True, "n": 2},
            },
            "inputs": [{"name": "in.dat", "sha256": "c" * 64}],
            "resources": {"node_count": 2},
            "attributes": {"queue_name": "debug"},
        }
    )
    job = direct_job(spec, jarvis="~/jv/bin/jarvis")
    assert job.package is None
    assert job.executable == "/bin/bash"
    assert job.arguments[1] == SITE_SCRIPT
    assert job.arguments[2:8] == [
        "clio-relay-jarvis",
        "run",
        "b" * 64,
        "demo.hello",
        "~/jv/bin/jarvis",
        "",
    ]
    assert job.arguments[8].splitlines() == [
        f"name: relay_{'b' * 12}",
        "pkgs:",
        "- pkg_type: demo.hello",
        "  pkg_name: pkg",
        "  flag: true",
        "  n: 2",
    ]
    assert [item.name for item in job.inputs] == ["in.dat", f".relay-package/{'b' * 64}.tar.gz"]
    assert job.resources.node_count == 2
    assert job.attributes.queue_name == "debug"
    assert job.name == "demo-hello"
    with pytest.raises(Refusal) as raised:
        direct_job(spec.model_copy(update={"directory": "/scratch/run"}))
    assert raised.value.reason == RefusalReason.INVALID_SPEC


def test_a_parameter_naming_an_input_becomes_its_path_in_the_job() -> None:
    spec = JobSpec.model_validate(
        {
            "package": {
                "name": "demo.hello",
                "version": "1",
                "sha256": "b" * 64,
                "parameters": {"stl": "body.stl", "label": "body", "count": 2},
            },
            "inputs": [
                {"name": "body.stl", "sha256": "c" * 64},
                {"name": f"{ARCHIVE_DIR}/{'b' * 64}.tar.gz", "sha256": "b" * 64},
            ],
        }
    )
    archive = f"{ARCHIVE_DIR}/{'b' * 64}.tar.gz"
    assert input_paths(spec, "/scratch/job") == {"body.stl": "/scratch/job/body.stl"}
    entry = pipeline_document(spec, pipeline="p", job_dir="/scratch/job")["pkgs"]
    assert entry == [
        {
            "pkg_type": "demo.hello",
            "pkg_name": "pkg",
            "count": 2,
            "label": "body",
            "stl": "/scratch/job/body.stl",
        }
    ]
    kept = pipeline_document(spec, pipeline="p")["pkgs"]
    assert isinstance(kept, list)
    assert kept[0]["stl"] == "body.stl"
    assert archive not in str(entry)
    job = direct_job(spec, job_dir="/scratch/job")
    assert "stl: /scratch/job/body.stl" in job.arguments[8].splitlines()
