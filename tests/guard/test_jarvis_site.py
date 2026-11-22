from __future__ import annotations

import gzip
import io
import json
import os
import tarfile
from pathlib import Path

import pytest

from clio_relay.adapters.sched_info import SCHEMA as SCHED_SCHEMA
from clio_relay.adapters.sched_info import partitions as sched_partitions
from clio_relay.adapters.sched_info import report as sched_report
from clio_relay.adapters.sched_info import site_facts as sched_facts
from clio_relay.adapters.ssh.scripts import (
    exec_script,
    graph_collect_command,
    jarvis_command,
    relay_jarvis,
    sched_script,
)
from clio_relay.jarvis_report import (
    applied_decisions,
    blocking_issues,
    choice_issues,
    choice_message,
)
from clio_relay.jarvis_site import (
    SOURCE_FILE,
    archive_package,
    inspect_archive,
    package_files,
    read_archive,
)
from clio_relay.jarvis_submit import (
    ARCHIVE_DIR,
    JARVIS_PIN,
    PIN_ENV,
    SITE_SCRIPT,
    direct_job,
    explicit_jarvis,
    graph_pipeline,
    graph_report,
    input_paths,
    jarvis_refused,
    missing_jarvis,
    pipeline_document,
    queue_message,
    record_status,
    steps_digest,
)
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.login_load import light_argv

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
    assert job.arguments[2:7] == [
        "clio-relay-jarvis",
        "run",
        "~/jv/bin/jarvis",
        "",
        f"demo.hello@{'b' * 64}",
    ]
    assert job.arguments[7].splitlines() == [
        f"name: relay_{steps_digest(spec)[:12]}",
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


@pytest.mark.parametrize(
    "value", ["{job_dir}/body.stl", "$CLIO_RELAY_JOB_DIR/body.stl", "<job>/body.stl"]
)
def test_a_placeholder_around_an_input_name_is_refused(value: str) -> None:
    spec = JobSpec.model_validate(
        {
            "package": {
                "name": "demo.hello",
                "version": "1",
                "sha256": "b" * 64,
                "parameters": {"stl": value},
            },
            "inputs": [{"name": "body.stl", "sha256": "c" * 64}],
        }
    )
    with pytest.raises(Refusal) as raised:
        pipeline_document(spec, pipeline="p", job_dir="/scratch/job")
    assert raised.value.reason == RefusalReason.INVALID_SPEC
    assert "'body.stl'" in raised.value.message
    assert spec.package is not None
    dotted = spec.model_copy(
        update={"package": spec.package.model_copy(update={"parameters": {"stl": "./body.stl"}})}
    )
    assert pipeline_document(dotted, pipeline="p", job_dir="/scratch/job")["pkgs"] == [
        {"pkg_type": "demo.hello", "pkg_name": "pkg", "stl": "/scratch/job/body.stl"}
    ]


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
    assert "  stl: /scratch/job/body.stl" in job.arguments[7].splitlines()


def test_the_jarvis_setting_is_a_path_or_relays_pinned_one() -> None:
    assert explicit_jarvis("") == ""
    assert explicit_jarvis("jarvis") == ""
    assert explicit_jarvis("~/jv/bin/jarvis") == "~/jv/bin/jarvis"
    assert explicit_jarvis("/opt/jarvis/bin/jarvis") == "/opt/jarvis/bin/jarvis"
    for bare in ("jarvis-1.8", "bin/jarvis"):
        with pytest.raises(ValueError, match="PATH"):
            explicit_jarvis(bare)


def test_the_site_script_expects_relays_pin() -> None:
    assert PIN_ENV == "CLIO_RELAY_JARVIS_PIN"
    assert 'command -v "$jarvis"' not in SITE_SCRIPT
    assert '"${found%/*}/pin"' in SITE_SCRIPT


def test_a_missing_jarvis_says_whose_it_is() -> None:
    explicit = missing_jarvis('{"missing":"jarvis","tried":"/opt/j","explicit":true}', site="lab")
    assert explicit.data.get("explicit") is True
    assert "installable" not in explicit.data
    stale = missing_jarvis('{"missing":"jarvis","tried":"/h/j","pin":"abc"}', site="lab")
    assert stale.data["pin"] == "abc"
    assert stale.data["installable"] == JARVIS_PIN
    absent = missing_jarvis('{"missing":"jarvis","tried":"/h/j"}', site="lab")
    assert absent.reason == RefusalReason.ADAPTER_UNAVAILABLE
    assert absent.data["installable"] == JARVIS_PIN


def test_a_refusal_keeps_jarvis_typed_issues_even_pretty_printed() -> None:
    text = (
        "some log line\n"
        "{\n"
        '  "schema_version": "jarvis.refusal.v1",\n'
        '  "refused": true,\n'
        '  "issues": [\n'
        '    {"kind": "execution_move_refused", "field": "queue",\n'
        '     "message": "the job is already running"}\n'
        "  ]\n"
        "}\n"
    )
    refusal = jarvis_refused(text, site="lab", what="the move")
    assert refusal.reason == RefusalReason.INVALID_SPEC
    assert refusal.data["kinds"] == ["execution_move_refused"]
    assert "queue: the job is already running" in refusal.message


def test_the_graph_collection_runs_on_compute_nodes() -> None:
    assert graph_pipeline("slurm") == {
        "name": "site_net",
        "pkgs": [{"pkg_type": "builtin.resource_graph"}],
        "scheduler": {"nodes": 2, "time": "00:05:00"},
    }
    assert "scheduler" not in graph_pipeline("none")


def test_the_graph_report_is_jarvis_json_or_none() -> None:
    report = '{"schema_version": "jarvis.resource-graph.net-report.v1", "data": "x"}\ntrailing'
    found = graph_report("noise\n" + report)
    assert found is not None
    assert found["data"] == "x"
    assert graph_report("no graph yet") is None


def test_a_waiting_job_carries_jarvis_queue_facts() -> None:
    record = {
        "state": "submitted",
        "metadata": {
            "queue": {
                "state": "pending",
                "partition": "big",
                "reason": "Resources",
                "starts_in_s": 7200,
                "partitions": [
                    {"partition": "big", "fits": True},
                    {"partition": "small", "fits": True, "idle_nodes": 3},
                    {"partition": "tiny", "fits": False},
                ],
            }
        },
    }
    status = record_status(record)
    assert status.state == JobState.QUEUED
    assert status.queue is not None
    assert status.message == queue_message(status.queue)
    assert status.message == (
        "waiting in the queue in big (Resources); the scheduler expects it to start "
        "in about 2 h. It would fit now in small"
    )


def test_a_failed_job_says_jarvis_typed_issues() -> None:
    issue = {
        "kind": "mpi_network",
        "field": "resource_graph.net.override",
        "message": "ranks reached the wrong node",
    }
    status = record_status({"state": "failed", "return_code": 14, "metadata": {"issues": [issue]}})
    assert status.state == JobState.FAILED
    assert status.issues == [issue]
    assert status.message is not None
    assert "ranks reached the wrong node" in status.message


def test_only_decisions_jarvis_replayed_are_applied() -> None:
    report = {
        "applied_decisions": [
            {"issue_id": "a", "source": "decisions_file", "constraint": "^mpi/abc"},
            {"issue_id": "b", "source": "answers", "constraint": "%gcc"},
        ]
    }
    assert [item["issue_id"] for item in applied_decisions(report)] == ["a"]


def test_a_missing_spack_is_a_choice_of_the_site_spacks() -> None:
    root = "/opt/site/spack"
    issue = {
        "issue_id": "spack-1",
        "kind": "spack_missing",
        "subject": "spack",
        "scope": "site",
        "package": "*",
        "answerable": True,
        "question": "Which Spack should JARVIS use on this site?",
        "candidates": [{"id": root, "root": root, "constraint": root, "version": "0.23.1"}],
        "site_spacks": [{"id": root, "root": root, "constraint": root, "version": "0.23.1"}],
    }
    report = {"issues": [issue]}
    assert choice_issues(report) == [issue]
    assert blocking_issues(report) == []
    message = choice_message(issue)
    assert root in message
    assert "0.23.1" in message


def test_no_site_spack_blocks_the_install() -> None:
    issue: dict[str, object] = {
        "issue_id": "spack-2",
        "kind": "spack_missing",
        "answerable": True,
        "candidates": [],
    }
    report: dict[str, object] = {"issues": [issue]}
    assert choice_issues(report) == []
    assert blocking_issues(report) == [issue]


def test_describe_reads_jarvis_sched_info() -> None:
    document = {
        "schema_version": SCHED_SCHEMA,
        "provider": "slurm",
        "partitions": [
            {
                "partition": "small",
                "default": True,
                "total_nodes": 4,
                "idle_nodes": 3,
                "allocated_nodes": 1,
                "cpus_per_node": 40,
                "memory_mb_per_node": 1000,
                "max_time": "1:00:00",
                "max_time_s": 3600,
                "gres": "gpu:2",
            },
            {"name": "unnamed-is-skipped"},
            {"partition": ""},
        ],
        "mpi": {"choice_needed": True},
        "site_skills": [{"name": "lab", "description": "the lab's notes"}],
        "issues": [],
    }
    found = sched_report("[jarvis] noise\n" + json.dumps(document, indent=2) + "\n")
    assert found == document
    assert sched_report('{"schema_version": "other"}') is None
    listed = sched_partitions(found)
    assert [entry.name for entry in listed] == ["small", "unnamed-is-skipped"]
    small = listed[0]
    assert (small.default, small.nodes, small.idle, small.allocated) == (True, 4, 3, 1)
    assert (small.cpus_per_node, small.memory_mb, small.max_time_s) == (40, 1000, 3600)
    assert small.gres == ["gpu:2"]
    facts = sched_facts(found)
    assert facts["provider"] == "slurm"
    assert facts["site_skills"] == [{"name": "lab", "description": "the lab's notes"}]
    assert sched_partitions(None) == []


def test_the_jarvis_exec_runs_relays_jarvis_with_the_site_record() -> None:
    argv = jarvis_command("/home/me/.clio-relay")
    assert argv[:2] == ["sh", "-c"]
    assert argv[3] == "jarvis"
    script = argv[2]
    assert 'exec "$JB" "$@"' in script
    assert "JARVIS_RESOURCE_GRAPH" in script
    assert "JARVIS_SPACK_DECISIONS" in script
    assert 'JARVIS_SPACK_ROOT="$SR"' in script


def test_a_bare_jarvis_in_exec_is_relays_jarvis_with_its_arguments_unparsed() -> None:
    home = "/home/me/.clio-relay"
    read = ["jarvis", "rg", "facts", "show", "+json"]
    assert relay_jarvis(read, home) == [*jarvis_command(home), "rg", "facts", "show", "+json"]
    light = light_argv(["jarvis", "rg", "facts", "answer", "id; rm -rf ~", "+json"])
    wrapped = relay_jarvis(light, home)
    assert wrapped[:4] == light[:4]
    assert wrapped[4:8] == jarvis_command(home)
    assert wrapped[8:] == ["rg", "facts", "answer", "id; rm -rf ~", "+json"]
    for other in (["ls", "jarvis"], ["/opt/jarvis", "ppl", "list"], ["sh", "-c", "jarvis"]):
        assert relay_jarvis(other, home) == other
    script = exec_script(read, home)
    assert "set -- sh -c " in script and "jarvis rg facts show +json\n" in script
    assert exec_script(read).startswith('cd "$HOME" 2>/dev/null\nset -- jarvis rg')


def test_site_level_jarvis_calls_initialize_the_site_root_once() -> None:
    """sched info, the graph collection and the exec prefix run JARVIS with the
    site-level root, which JARVIS refuses until ``jarvis init`` has made it."""
    home = "/home/me/.clio-relay"
    scripts = {
        "exec": jarvis_command(home)[2],
        "sched": sched_script(home),
        "collect": graph_collect_command(home, "slurm")[2],
    }
    for name, script in scripts.items():
        guard = '[ -f "$R/jarvis_config.yaml" ] || JARVIS_ROOT="$R" "$JB" init'
        assert guard in script, name
        assert '"$R/config" "$R/private" "$R/shared"' in script, name
        assert script.index(guard) < script.rindex('"$JB"'), name
    assert 'ppl submit "$S/site_net.yaml" +json' in scripts["collect"]
    assert 'ppl run yaml "$S/site_net.yaml" +json' in graph_collect_command(home, "none")[2]
