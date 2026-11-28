"""Unit checks for dropping a job's deleted inputs from the site's content store."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from clio_relay import jarvis_remote
from clio_relay.adapters.ssh import scripts, store
from clio_relay.jobs.spec import InputRef

pytestmark = pytest.mark.release_platform("posix")


def test_a_final_record_drops_the_inputs_the_job_deleted(tmp_path: Path) -> None:
    root = tmp_path / "R"
    job = root / "jobs" / "op_a"
    other = root / "jobs" / "op_b"
    names = {
        "deleted": "serve/secrets.env",
        "kept": "kept.txt",
        "replaced": "replaced.txt",
        "shared": "shared file.txt",
        "transient": "serve/never read.env",
    }
    digests = {case: f"{index:x}" * 64 for index, case in enumerate(names, start=10)}
    stored: dict[str, Path] = {}
    for case, name in names.items():
        blob = root / "content" / "cas" / "sha256" / digests[case][:2] / digests[case]
        blob.parent.mkdir(parents=True, exist_ok=True)
        blob.write_text(case)
        stored[case] = blob
        (job / name).parent.mkdir(parents=True, exist_ok=True)
        os.link(blob, job / name)
    other.mkdir(parents=True)
    os.link(stored["shared"], other / "shared file.txt")
    os.link(stored["transient"], other / "transient.env")
    (job / names["deleted"]).unlink()
    (job / names["shared"]).unlink()
    (job / names["replaced"]).unlink()
    (job / names["replaced"]).write_text("new")
    inputs_json = json.dumps(
        [{"name": names[c], "sha256": digests[c], "transient": c == "transient"} for c in names]
    )
    inputs = store.input_entries(inputs_json.splitlines())
    assert len(inputs) == 5
    assert [item.transient for item in inputs] == [False] * 4 + [True]
    script = scripts.record_script(
        str(job), "cancelled exit None", final=True, root=str(root), inputs=inputs
    )
    done = subprocess.run(
        ["sh", "-c", script], input=b"{}", capture_output=True, check=True, cwd=tmp_path
    )
    dropped = jarvis_remote.parse_sections(done.stdout.decode()).get("dropped", [])
    assert dropped == [digests["deleted"], digests["transient"]]
    assert {case for case, blob in stored.items() if blob.exists()} == {
        "kept",
        "replaced",
        "shared",
    }
    assert not (job / names["transient"]).exists()
    assert (other / "transient.env").read_text() == "transient"
    assert (job / "final").read_text() == "{}"


def test_inputs_json_that_is_not_a_list_gives_no_inputs() -> None:
    assert store.input_entries([]) == []
    assert store.input_entries(["{"]) == []
    assert store.input_entries(['{"a": 1}']) == []
    assert store.input_entries(['[{"name": "x", "sha256": "not hex"}]']) == []
    old = store.input_entries([json.dumps([{"name": "x", "sha256": "a" * 64}])])
    assert old == [store.StagedInput("x", "a" * 64, False)]


def test_inputs_json_carries_the_transient_mark() -> None:
    digest = "ab" * 32
    assert store.entry(InputRef(name="a", sha256=digest)) == {
        "name": "a",
        "sha256": digest,
        "transient": False,
    }
    assert store.entry(InputRef(name="s", sha256=digest, transient=True))["transient"] is True
