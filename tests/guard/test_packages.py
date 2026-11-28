"""Relay's package catalog: what each site's JARVIS root lists (RL24, RL42)."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from clio_relay.jarvis_site import parse_packages
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import PackageRef
from clio_relay.packages import ADD_COMMAND, PackageCatalog, describe_package

from .conftest import listed, make_repo

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


def test_a_listed_package_says_what_it_runs_without_running_it(tmp_path: Path) -> None:
    entry = describe_package(listed(_with_contract(tmp_path), "demo.hello"))
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


def test_a_version_comes_from_the_contract_the_commit_or_the_digest(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    plain = listed(repo, "demo.hello")
    assert describe_package(plain).version == plain["digest"][:12]
    cloned = describe_package(listed(repo, "demo.hello", {"commit": "ab" * 20, "url": "u"}))
    assert cloned.version == "ab" * 6
    assert cloned.source == {"repository": "u", "commit": "ab" * 20}
    assert describe_package(listed(_with_contract(tmp_path / "c"), "demo.hello")).version == "0.3.0"


def test_a_menu_entry_built_by_a_call_keeps_its_literal_keys(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    (repo / "demo" / "hello" / "pkg.py").write_text(
        "from jarvis_cd.core.pkg import Application\n\n\n"
        "class Hello(Application):\n"
        "    def _configure_menu(self):\n"
        "        return [\n"
        '            {"name": "inputs", "msg": "native input", "type": str, "default": "",\n'
        '             "input_binding": Binding(kind="local_file").to_dict()},\n'
        '            {"name": "steps", "msg": "how many", "type": int, "default": 10},\n'
        "        ]\n"
    )
    properties = describe_package(listed(repo, "demo.hello")).parameters["properties"]
    assert set(properties) == {"inputs", "steps"}
    assert properties["steps"]["type"] == "integer"


def test_the_catalog_pins_what_the_site_holds_now(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    catalog = PackageCatalog(tmp_path / "packages")
    assert catalog.all("hpc") == []
    first = listed(repo, "demo.hello")
    catalog.store("hpc", [first, listed(repo, "demo.other")])
    alone = catalog.resolve("hpc", PackageRef(name="demo.hello", parameters={"out": "x"}))
    assert alone.pinned() == (first["digest"][:12], first["digest"])
    assert alone.parameters == {"out": "x"}
    assert catalog.all("other") == []

    (repo / "demo" / "hello" / "pkg.py").write_text("# edited\n")
    second = listed(repo, "demo.hello")
    catalog.store("hpc", [second])
    assert [entry.digest for entry in catalog.all("hpc")] == [second["digest"]]
    with pytest.raises(Refusal) as moved:
        catalog.resolve("hpc", PackageRef(name="demo.hello", sha256=first["digest"]))
    assert moved.value.data["next_step"] == "pin_package"
    assert catalog.entry("hpc", PackageRef(name="demo.hello", sha256=first["digest"])) is None

    with pytest.raises(Refusal) as raised:
        catalog.resolve("hpc", PackageRef(name="demo.nothing"))
    assert raised.value.reason == RefusalReason.INVALID_SPEC
    assert "It has: demo.hello" in raised.value.message
    assert ADD_COMMAND in raised.value.message
    with pytest.raises(ValueError, match="version and SHA-256"):
        PackageRef(name="demo.hello").pinned()


def test_the_listing_reads_back_what_the_site_printed() -> None:
    pkg = base64.b64encode(b"class Hello: pass\n").decode()
    text = "\n".join(
        [
            "noise before",
            f"@@package demo.hello {'a' * 64}",
            "@@repo /home/u/.clio-relay/jarvis/repos/demo",
            f"@@git {'c' * 40} https://example.org/demo.git",
            f"@@file {'b' * 64} x 12 hello/run.sh",
            f"@@file {'d' * 64} - 5 _contract/spec.py",
            "@@file broken",
            f"@@pkg {pkg}",
            "@@refused demo.linked",
            "@@package demo.bad-name " + "a" * 64,
            "@@repo /ignored",
        ]
    )
    [item] = parse_packages(text) or []
    assert (item["name"], item["digest"]) == ("demo.hello", "a" * 64)
    assert item["repo_dir"] == "/home/u/.clio-relay/jarvis/repos/demo"
    assert item["git"] == {"commit": "c" * 40, "url": "https://example.org/demo.git"}
    assert item["files"] == [
        {"path": "hello/run.sh", "sha256": "b" * 64, "size": 12, "executable": True},
        {"path": "_contract/spec.py", "sha256": "d" * 64, "size": 5, "executable": False},
    ]
    assert (item["pkg"], item["contract"]) == ("class Hello: pass\n", "")
    assert parse_packages("@@unset\n") is None
    assert parse_packages("") == []


def test_jarvis_builtin_packages_are_never_run(tmp_path: Path) -> None:
    pinned = PackageRef(name="builtin.hello", version="1", sha256="a" * 64)
    with pytest.raises(Refusal) as run:
        PackageCatalog(tmp_path / "packages").resolve("hpc", pinned)
    assert run.value.reason is RefusalReason.POLICY_REFUSED
    assert run.value.data["next_step"] == "write_package"
