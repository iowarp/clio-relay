from __future__ import annotations

import json

import pytest

from clio_relay import jarvis_graph
from clio_relay.adapters.sched_info import SCHEMA as SCHED_SCHEMA
from clio_relay.adapters.sched_info import partitions as sched_partitions
from clio_relay.adapters.sched_info import report as sched_report
from clio_relay.adapters.sched_info import site_facts as sched_facts
from clio_relay.adapters.ssh.scripts import (
    exec_script,
    graph_collect_command,
    graph_script,
    jarvis_command,
    relay_jarvis,
    sched_script,
)
from clio_relay.jarvis_report import (
    applied_decisions,
    blocking_issues,
    choice_details,
    choice_issues,
    choice_message,
)
from clio_relay.jarvis_submit import (
    JARVIS_PIN,
    PIN_ENV,
    SITE_SCRIPT,
    explicit_jarvis,
    graph_pipeline,
    graph_report,
    input_paths,
    jarvis_refused,
    missing_jarvis,
    pipeline_document,
    queue_message,
    record_status,
)
from clio_relay.jobs.handles import JobState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.login_load import light_argv


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
        {"pkg_type": "demo.hello", "pkg_name": "hello", "stl": "/scratch/job/body.stl"}
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
                {"name": "notes.txt", "sha256": "d" * 64},
            ],
        }
    )
    assert input_paths(spec, "/scratch/job") == {
        "body.stl": "/scratch/job/body.stl",
        "notes.txt": "/scratch/job/notes.txt",
    }
    entry = pipeline_document(spec, pipeline="p", job_dir="/scratch/job")["pkgs"]
    assert entry == [
        {
            "pkg_type": "demo.hello",
            "pkg_name": "hello",
            "count": 2,
            "label": "body",
            "stl": "/scratch/job/body.stl",
        }
    ]
    kept = pipeline_document(spec, pipeline="p")["pkgs"]
    assert isinstance(kept, list)
    assert kept[0]["stl"] == "body.stl"


def test_the_jarvis_setting_is_a_path_or_relays_pinned_one() -> None:
    assert explicit_jarvis("") == ""
    assert explicit_jarvis("jarvis") == ""
    assert explicit_jarvis("~/jv/bin/jarvis") == "~/jv/bin/jarvis"
    assert explicit_jarvis("/opt/jarvis/bin/jarvis") == "/opt/jarvis/bin/jarvis"
    for bare in ("jarvis-1.8", "bin/jarvis"):
        with pytest.raises(ValueError, match="PATH"):
            explicit_jarvis(bare)


def test_the_site_script_expects_relays_pin_in_the_sites_root() -> None:
    assert PIN_ENV == "CLIO_RELAY_JARVIS_PIN"
    assert 'command -v "$jarvis"' not in SITE_SCRIPT
    assert '"$root/relay.pin"' in SITE_SCRIPT


def test_a_missing_jarvis_says_whose_it_is() -> None:
    explicit = missing_jarvis('{"missing":"jarvis","tried":"/opt/j","explicit":true}', site="lab")
    assert explicit.data.get("explicit") is True
    assert "installable" not in explicit.data
    stale = missing_jarvis(
        '{"missing":"jarvis","tried":"/h/j","pin":"' + "a" * 40 + ' f38ecb74abcd",'
        '"want":"' + "b" * 40 + ' dae7f4d8abcd"}',
        site="lab",
    )
    assert stale.data["pin"] == "a" * 40 + " f38ecb74abcd"
    assert stale.data["installable"] == JARVIS_PIN
    assert stale.message.startswith(
        "JARVIS is on lab at jarvis-cd aaaaaaaa with clio-jarvis f38ecb74abcd; relay "
        "updates it to jarvis-cd bbbbbbbb with clio-jarvis dae7f4d8abcd. "
    )
    absent = missing_jarvis('{"missing":"jarvis","tried":"/h/j"}', site="lab")
    assert absent.reason == RefusalReason.ADAPTER_UNAVAILABLE
    assert absent.data["installable"] == JARVIS_PIN
    assert "pin" not in absent.data
    assert absent.message.startswith("lab has no JARVIS root that relay set up; ")


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
        "pkgs": [{"pkg_type": "clio_relay_site.net_graph"}],
        "scheduler": {"nodes": 2, "time": "00:05:00"},
    }
    assert graph_pipeline("none")["scheduler"] == {"name": "local"}


def test_the_graph_collection_is_relays_own_package_never_jarvis_builtin() -> None:
    """The user: builtins "should not even be installed for relay to use"."""
    script = graph_collect_command("/home/me/.clio-relay", "slurm")[2]
    assert "builtin." not in script
    write = script.index("/net_graph/pkg.py")
    register = script.index('"$JB" repo add "$P" +force')
    assert script.index('P="$S"/clio_relay_site') < write < register
    assert register < script.index('ppl submit "$S/site_net.yaml"')
    code = compile(jarvis_graph.PKG_SOURCE, "pkg.py", "exec")
    assert "NetGraph" in code.co_names
    # JARVIS loads <repo>.<package>.pkg and the PascalCase of the package's name.
    assert "".join(w.capitalize() for w in jarvis_graph.PACKAGE.split("_")) == "NetGraph"
    assert "net_only=True" in jarvis_graph.PKG_SOURCE


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


def test_a_failed_job_says_jarvis_error_once() -> None:
    said = "podman could not start the container on every node, so no package was started."
    issue = {"kind": "container_start_failed", "message": said}
    record = {"state": "failed", "return_code": 1, "error": said, "metadata": {"issues": [issue]}}
    assert record_status(record).message == said
    other = {"kind": "mpi_network", "message": "ranks reached the wrong node"}
    record["metadata"] = {"issues": [issue, other, other]}
    assert record_status(record).message == f"{said} ranks reached the wrong node"


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
    assert choice_message(issue) == "Which Spack should JARVIS use on this site?"
    (only,) = choice_details(issue)
    assert only.label == root
    assert "0.23.1" in (only.description or "")


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
                "limits": {"min_nodes": 0, "max_nodes": "UNLIMITED"},
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
    # Slurm's MinNodes=0 is no minimum, and UNLIMITED no per-job maximum.
    assert (small.min_nodes, small.max_nodes) == (1, None)
    assert (listed[1].min_nodes, listed[1].max_nodes) == (None, None)
    facts = sched_facts(found)
    assert facts["provider"] == "slurm"
    assert facts["site_skills"] == [{"name": "lab", "description": "the lab's notes"}]
    assert sched_partitions(None) == []


def test_describe_passes_jarvis_queue_limits_through_unchanged() -> None:
    from clio_relay.guard import limits

    queue_limits = {"min_nodes": 1, "max_nodes": None, "max_time": "24:00:00", "state": "UP"}
    source = {"provider": "pbs", "command": "qstat -Qf", "fields": {"max_time": "walltime"}}
    document = {
        "schema_version": SCHED_SCHEMA,
        "provider": "pbs",
        "providers_queues_readable": ["slurm", "pbs", "lsf", "flux"],
        "partitions": [
            {
                "partition": "prod",
                "total_nodes": 512,
                "limits": queue_limits,
                "limits_unknown": ["max_nodes"],
                "limits_source": source,
            }
        ],
    }
    prod = sched_partitions(document)[0]
    assert prod.limits == queue_limits and prod.limits_source == source
    assert prod.state == "UP"
    # Unknown is not unlimited: the guard reports it, and neither enforces nor waves it.
    assert prod.max_nodes is None and prod.limits_unknown == ["max_nodes"]
    assert limits.facts(prod)["limits_unknown"] == ["max_nodes"]
    assert sched_facts(document)["providers_queues_readable"] == ["slurm", "pbs", "lsf", "flux"]


def test_describe_reports_compute_cores_from_jarvis_never_the_login_host() -> None:
    from clio_relay.adapters import sched_info

    topology = {
        "sockets_per_node": 2,
        "cores_per_socket": 10,
        "threads_per_core": 2,
        "physical_cores_per_node": 20,
        "heterogeneous": False,
    }
    cpu = {
        "schema_version": sched_info.CPU_SCHEMA,
        "hosts": {"n1": {"cpus": 64, "physical_cores": 32, "threads_per_core": 2}},
        "smallest": {"cpus": 64, "sockets": 2, "physical_cores": 32, "threads_per_core": 2},
        "heterogeneous": False,
    }
    document = {
        "schema_version": SCHED_SCHEMA,
        "partitions": [
            {"partition": "compute", "default": True, "cpus_per_node": 40, **topology},
            {"partition": "gpu", "cpus_per_node": 64},
        ],
        "cpu": cpu,
    }
    listed = sched_partitions(document)
    compute = listed[0]
    assert (compute.sockets, compute.cores_per_socket) == (2, 10)
    assert (compute.threads_per_core, compute.physical_cores) == (2, 20)
    assert sched_info.graph_cpu(document) is cpu
    assert sched_info.graph_cpu({"cpu": {"schema_version": "other"}}) is None
    cores = sched_info.compute_cores(listed, cpu)
    assert cores["compute"]["source"] == "sinfo"
    assert cores["compute"]["physical_cores"] == 20
    # No sinfo topology: the graph's smallest collected node stands in, as JARVIS's plan does.
    assert cores["gpu"]["source"] == "resource_graph.cpu"
    assert cores["gpu"]["physical_cores"] == 32
    assert sched_info.compute_cores(listed[1:], None)["gpu"]["source"] is None
    assert sched_info.threads_per_core(cores, listed, None) == 2
    assert sched_info.threads_per_core(cores, listed, "gpu") == 2
    assert sched_info.threads_per_core(cores, listed, "nope") is None


def test_a_clean_site_reads_its_partitions_from_sinfo() -> None:
    from clio_relay.adapters import sched_info

    lines = (
        "compute*|up|17/5/0/22|2-00:00:00|40|47759|(null)|2|10|2|1-infinite\n"
        "debug|up|4/2/0/6|4-00:00:00|40|47661+|(null)|2|10|2|2-4\n"
        "gpu|up|1/0/1/2|12:00:00|64|256000|gpu:a100:4|2|16|2|1\n"
        "gpu|up|0/1/0/1|12:00:00|32|128000|gpu:a100:4|2|8|2|1\n"
        "noise line\n"
    )
    listed = {entry["partition"]: entry for entry in sched_info.sinfo_partitions(lines)}
    assert set(listed) == {"compute", "debug", "gpu"}
    compute = listed["compute"]
    assert compute["default"] is True and compute["heterogeneous"] is False
    assert (compute["idle_nodes"], compute["total_nodes"]) == (5, 22)
    assert compute["max_time_s"] == 2 * 86400
    assert (compute["physical_cores_per_node"], compute["memory_mb_per_node"]) == (20, 47759)
    # sinfo's "+" marks the smallest of differing nodes.
    assert listed["debug"]["heterogeneous"] is True
    gpu = listed["gpu"]
    assert (gpu["total_nodes"], gpu["idle_nodes"], gpu["other_nodes"]) == (3, 1, 1)
    assert gpu["heterogeneous"] is True and gpu["physical_cores_per_node"] == 16
    assert gpu["gres"] == "gpu:a100:4"
    parsed = {item.name: item for item in sched_partitions({"partitions": list(listed.values())})}
    compute_row = parsed["compute"]
    assert (compute_row.nodes, compute_row.idle, compute_row.physical_cores) == (22, 5, 20)
    assert (compute_row.memory_mb, compute_row.max_time_s) == (47759, 172800)
    # sinfo's %s is the nodes one job may ask for.
    assert (compute_row.min_nodes, compute_row.max_nodes) == (1, None)
    assert (parsed["debug"].min_nodes, parsed["debug"].max_nodes) == (2, 4)
    assert (parsed["gpu"].min_nodes, parsed["gpu"].max_nodes) == (1, 1)
    # Pre-placement limits have JARVIS's shape: what sinfo shows is known, the rest unknown.
    assert set(compute["limits"]) == set(sched_info.LIMIT_KEYS)
    assert compute["limits"]["max_nodes"] == "UNLIMITED"
    assert compute["limits"]["state"] == "UP" and compute["limits"]["total_nodes"] == 22
    assert "max_nodes" not in compute["limits_unknown"]
    assert "max_mem_per_node_mb" in compute["limits_unknown"]
    assert compute["limits_source"]["fields"]["max_nodes"] == "%s"
    assert compute_row.state == "UP" and compute_row.limits_unknown == compute["limits_unknown"]
    from clio_relay.adapters.base import SiteInfo
    from clio_relay.guard import limits

    site = SiteInfo(adapter="ssh", partitions=list(parsed.values()))

    def debug_job(nodes: int) -> JobSpec:
        return JobSpec.model_validate(
            {
                "executable": "a.out",
                "resources": {"node_count": nodes},
                "attributes": {"queue_name": "debug"},
            }
        )

    assert limits.check(debug_job(3), site) is None
    for nodes, text in ((5, "at most 4"), (1, "at least 2")):
        refused = limits.check(debug_job(nodes), site)
        assert refused is not None and text in refused[1]
        assert refused[2]["limits"]["max_nodes"] == 4
    from clio_relay.adapters.ssh.scripts import describe_script

    assert "sinfo --noheader --format=" in describe_script("slurm", "/home/me/.clio-relay")


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("2-00:00:00", 172800),
        ("1-12", 129600),
        ("30", 1800),
        ("30:15", 1815),
        ("1:00:00", 3600),
        ("infinite", None),
        ("UNLIMITED", None),
    ],
)
def test_sinfo_time_limits_are_seconds(text: str, seconds: int | None) -> None:
    from clio_relay.adapters import sched_info

    assert sched_info._seconds(text) == seconds  # pyright: ignore[reportPrivateUsage]


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


def test_site_level_jarvis_calls_use_the_set_up_site_root_without_builtin() -> None:
    """sched info, the graph collection and the exec prefix run JARVIS in the site's
    root, which relay's setup made; they never initialize it themselves. The user:
    builtins "should not even be installed for relay to use"."""
    home = "/home/me/.clio-relay"
    for jarvis in ("", "/opt/jarvis"):
        scripts = {
            "exec": jarvis_command(home, jarvis)[2],
            "sched": sched_script(home, jarvis),
            "collect": graph_collect_command(home, "slurm", jarvis)[2],
            "describe": graph_script(home, jarvis),
        }
        for name, script in scripts.items():
            assert "R=/home/me/.clio-relay/jarvis\n" in script, name
            assert '[ -x "$JB" ] && [ -f "$R/jarvis_config.yaml" ]' in script, name
            assert " init " not in script and "repo remove builtin" not in script, name
            assert "export JARVIS_NO_BUILTIN=1\n" in script, name
    for scheduler in ("slurm", "none"):
        assert 'ppl submit "$S/site_net.yaml" +json' in graph_collect_command(home, scheduler)[2]
