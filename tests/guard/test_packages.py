"""Relay's package catalog: staged archives an agent finds by name (RL24, RL42)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from clio_relay.jarvis_site import archive_package
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import PackageRef
from clio_relay.packages import PackageCatalog, describe_archive, main, stage

from .conftest import make_repo

CONTRACT = """\
version: 0.3.0
kind: simulation
summary: >-
  Says hello
  and writes a file.
parameters:
  required: [out]
  properties:
    out: {minLength: 1}
    steps: {type: integer, minimum: 1}
outputs:
  - {name: greeting, path: "{run_dir}/out.txt", format: text, reader: none, extra: 1}
progress: {unit: step, output: greeting, step_pattern: "step (\\\\d+)"}
"""


def _with_contract(tmp_path: Path) -> Path:
    repo = make_repo(tmp_path)
    (repo / "demo" / "hello" / "contract.yaml").write_text(CONTRACT)
    return repo


def test_an_archive_says_what_it_runs_without_running_it(tmp_path: Path) -> None:
    repo = _with_contract(tmp_path)
    data = archive_package(repo, "demo.hello")
    entry = describe_archive(data, "demo.hello")
    assert (entry.name, entry.version, entry.kind) == ("demo.hello", "0.3.0", "simulation")
    assert entry.summary == "Says hello and writes a file."
    assert entry.parameters == {
        "type": "object",
        "properties": {
            "out": {"description": "where", "type": "string", "default": "out.txt", "minLength": 1},
            "steps": {"type": "integer", "minimum": 1},
        },
        "required": ["out"],
    }
    assert entry.outputs == [
        {"name": "greeting", "path": "{run_dir}/out.txt", "format": "text", "reader": "none"}
    ]
    assert entry.progress == {"unit": "step", "output": "greeting"}
    assert describe_archive(data, "demo.hello", version="9").version == "9"


def test_a_version_comes_from_the_contract_the_tag_or_the_caller(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    with pytest.raises(Refusal) as raised:
        describe_archive(archive_package(repo, "demo.hello"), "demo.hello")
    assert raised.value.reason == RefusalReason.INVALID_ARGUMENTS
    tagged = archive_package(repo, "demo.hello", source={"tag": "v1.2.0", "commit": "c" * 40})
    assert describe_archive(tagged, "demo.hello").version == "v1.2.0"


def test_the_catalog_pins_a_package_named_alone(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    state = tmp_path / "state"
    first = stage(state, archive_package(repo, "demo.hello"), "demo.hello", version="0.1.0")
    (repo / "demo" / "hello" / "pkg.py").write_text("# edited\n")
    second = stage(state, archive_package(repo, "demo.hello"), "demo.hello", version="0.2.0")
    assert ContentStore(state / "content").has(first.sha256)
    catalog = PackageCatalog(state / "packages")
    assert [entry.version for entry in catalog.all()] == ["0.1.0", "0.2.0"]
    assert [entry.sha256 for entry in catalog.newest()] == [second.sha256]

    newest = catalog.resolve(PackageRef(name="demo.hello", parameters={"out": "x"}))
    assert newest.pinned() == ("0.2.0", second.sha256)
    assert newest.parameters == {"out": "x"}
    assert catalog.resolve(PackageRef(name="demo.hello", version="0.1.0")).sha256 == first.sha256
    assert catalog.resolve(PackageRef(name="demo.hello", sha256=first.sha256)).version == "0.1.0"
    given = PackageRef(name="elsewhere", version="1", sha256="a" * 64)
    assert catalog.resolve(given) is given

    with pytest.raises(Refusal) as raised:
        catalog.resolve(PackageRef(name="demo.nothing"))
    assert raised.value.reason == RefusalReason.INVALID_SPEC
    assert "It has: demo.hello" in raised.value.message
    assert 'packages(op="stage"' in raised.value.message
    with pytest.raises(Refusal):
        catalog.resolve(PackageRef(name="demo.hello", version="7"))
    with pytest.raises(ValueError, match="version and SHA-256"):
        PackageRef(name="demo.hello").pinned()


def test_the_command_line_stages_and_lists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _with_contract(tmp_path)
    state = tmp_path / "state"
    monkeypatch.setenv("CLIO_RELAY_STATE_DIR", str(state))
    output = tmp_path / "hello.tar.gz"
    assert main(["stage", str(repo), "demo.hello", "--output", str(output)]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert (printed["version"], printed["staged"]) == ("0.3.0", True)
    assert ContentStore(state / "content").has(printed["sha256"])
    assert output.read_bytes() == archive_package(repo, "demo.hello", source=printed["source"])
    assert main(["list"]) == 0
    [listed] = json.loads(capsys.readouterr().out)
    assert (listed["name"], listed["sha256"]) == ("demo.hello", printed["sha256"])
    assert main(["stage", str(repo), "demo.other", "--dry-run"]) == 2
    assert "--version" in capsys.readouterr().err
    assert main(["stage", str(repo), "demo.nothing", "--version", "1"]) == 2
