"""A clean names a run's outputs and keeps them by default; a plan that submits says so."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from clio_relay.jobs.handles import OutputRef
from clio_relay.site_clean import describe_plan, run_outputs
from clio_relay.surface.exec_plan import ExecPlan, _message  # pyright: ignore[reportPrivateUsage]

TARGETS = ["/home/u/clio/site", "~/.clio-relay", "~/.config/clio-relay"]


def _record(site: str, kind: str, *outputs: OutputRef) -> Any:
    return SimpleNamespace(site=site, kind=kind, operation_id=f"op_{kind}", outputs=list(outputs))


def _service(*records: Any) -> Any:
    return SimpleNamespace(operations=SimpleNamespace(all=lambda: list(records)))


def test_outputs_outside_their_job_directories_are_listed_once() -> None:
    service = _service(
        _record(
            "ares",
            "run",
            OutputRef(name="case", path="/home/u/clio-cases/tgv/", size=11_000_000_000),
            OutputRef(name="field", path="/home/u/clio-cases/tgv/data.bp"),
            OutputRef(name="log", path="/home/u/clio/site/jobs/op_run/out.log"),
            OutputRef(name="state", path="/home/u/.clio-relay/naca_production"),
            OutputRef(name="relative", path="out.txt"),
        ),
        _record("other", "run", OutputRef(name="o", path="/elsewhere/o")),
        _record("ares", "install", OutputRef(name="i", path="/home/u/spack-build")),
    )
    found = run_outputs(service, "ares", TARGETS)
    # A case a run wrote inside relay's state is still the person's: it is listed and kept.
    assert [item["path"] for item in found] == [
        "/home/u/.clio-relay/naca_production",
        "/home/u/clio-cases/tgv",
    ]
    assert found[1]["operation_id"] == "op_run" and found[1]["size"] == 11_000_000_000


def test_a_runs_outputs_directory_is_listed_once_for_all_it_holds() -> None:
    runs = "/home/u/clio-relay-runs/op_run/relay_op_run/executions/op_run"
    service = _service(
        _record(
            "ares",
            "run",
            OutputRef(name="environment-input", path=f"{runs}/input/environment.yaml", size=9),
            OutputRef(name="pipeline-input", path=f"{runs}/input/pipeline.yaml", size=9),
        ),
    )
    found = run_outputs(service, "ares", TARGETS)
    assert found == [
        {
            "path": "/home/u/clio-relay-runs/op_run",
            "name": "op_run",
            "operation_id": "op_run",
            "size": None,
        }
    ]


def _planned(remove: bool) -> dict[str, Any]:
    return {
        "site": "ares",
        "targets": TARGETS,
        "keep": ["/home/u/clio/site/spack", "/home/u/clio/site/jarvis-cd/site/decisions.yaml"],
        "cancels": [],
        "outputs": [{"path": "/home/u/clio-cases/tgv", "size": 11_000_000_000}],
        "remove_outputs": remove,
    }


def test_the_clean_ask_names_the_outputs_it_keeps_or_deletes() -> None:
    kept = describe_plan(_planned(False))
    assert "It keeps what relay's runs wrote" in kept
    assert "/home/u/clio-cases/tgv (11.0 GB)" in kept
    assert "remove_outputs" in kept
    removed = describe_plan(_planned(True))
    assert "It also deletes what relay's runs wrote" in removed


def test_a_plan_that_submits_a_job_does_not_say_it_runs_on_the_login_node() -> None:
    site: Any = SimpleNamespace(title="Ares")
    submits = ExecPlan(kind="resource_graph", summary="Collects the networks.", submits=True)
    text = _message(site, ["jarvis", "ppl", "submit"], submits)
    assert text.startswith("Submit a resource_graph job from Ares's login node?")
    assert "not on the login node" in text
    plain = ExecPlan(kind="probe", summary="Reads a file.")
    assert _message(site, ["cat", "x"], plain).startswith("Run a probe plan on Ares's login node?")
