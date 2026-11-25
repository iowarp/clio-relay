"""The guarded ask for a command that submits a job carries the job's planned card."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from clio_relay import jarvis_graph, jarvis_submit
from clio_relay.surface import exec_plan, job_card
from clio_relay.surface.exec_plan import ExecPlan

COMMAND = ["bash", "-lc", "collect", "compute"]
PLAN = ExecPlan(kind="resource_graph", summary="Collects the networks.", submits=True)


def _service() -> tuple[Any, Any]:
    site = SimpleNamespace(id="ares", title="Ares")

    def found(_id: str) -> Any:
        return site

    def titles() -> dict[str, str]:
        return {"repo.solver": "Solver"}

    packages = SimpleNamespace(titles=titles)
    return SimpleNamespace(offered_plans={}, site=found, packages=packages), site


def _described(pipeline: dict[str, Any]) -> dict[str, Any]:
    collect = {"exec": list(COMMAND), "pipeline": pipeline, "queue": "compute"}
    return {"scheduler": "slurm", "facts": {"resource_graph": {"collect": collect}}}


def test_the_offered_collect_shows_its_pipeline_as_the_planned_job() -> None:
    service, site = _service()
    exec_plan.offer(service, "ares", _described(jarvis_submit.graph_pipeline("slurm")))
    card = exec_plan.snapshot(service, site, COMMAND, PLAN)
    assert card is not None
    assert card["kind"] == "exec_plan" and card["phase"] == "planned"
    assert card["operation_id"] is None and card["resource"] is None
    assert card["argv_sha256"] == exec_plan.argv_digest(COMMAND)
    assert card["site"] == {"id": "ares", "label": "Ares"}
    assert card["scheduler"] == "slurm" and card["partition"] == "compute"
    assert card["time_limit_s"] == 300
    [component] = card["components"]
    assert component["package"] == jarvis_graph.PKG_TYPE
    assert component["label"] == jarvis_graph.LABEL
    assert component["role"] == "compute" and component["nodes"] == 2
    assert component["state"] == "planned" and component["launch_order"] == 1
    assert card["couplings"] == []


def test_the_card_lists_a_pipeline_producer_first() -> None:
    service, site = _service()
    pipeline = {
        "pkgs": [{"pkg_type": "repo.viewer"}, {"pkg_type": "repo.solver"}],
        "couplings": [{"from": "solver", "to": "viewer", "kind": "data"}],
        "scheduler": {"nodes": 2, "time": "1-00:00:10"},
    }
    exec_plan.offer(service, "ares", _described(pipeline))
    card = exec_plan.snapshot(service, site, COMMAND, PLAN)
    assert card is not None
    assert [(c["id"], c["launch_order"]) for c in card["components"]] == [
        ("solver", 2),
        ("viewer", 1),
    ]
    # A package is named by its contract's title, else by its step name.
    assert [c["label"] for c in card["components"]] == ["Solver", "viewer"]
    assert card["couplings"] == [
        {"from": "solver", "to": "viewer", "kind": "data", "label": "data", "source": "relay"}
    ]
    assert card["time_limit_s"] == 86410


def test_a_command_relay_did_not_offer_is_the_plan_as_one_component() -> None:
    service, site = _service()
    card = exec_plan.snapshot(service, site, ["sbatch", "x.sh"], PLAN)
    assert card is not None
    [component] = card["components"]
    assert component["id"] == "resource_graph" and component["package"] is None
    assert card["scheduler"] is None and card["time_limit_s"] is None


def test_a_plan_that_does_not_submit_carries_no_card() -> None:
    service, site = _service()
    plain = ExecPlan(kind="spack", summary="Clones Spack.")
    assert exec_plan.snapshot(service, site, ["git", "clone"], plain) is None
    assert job_card.JOB_META == "io.iowarp.clio-relay/job"
