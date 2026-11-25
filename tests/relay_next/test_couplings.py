"""A pipeline's couplings run producer to consumer, whatever order JARVIS starts it in."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from clio_relay import jarvis_submit
from clio_relay.jobs.spec import JobSpec, PackageRef, flow_order
from clio_relay.packages import (
    PackageEntry,
    contract_couplings,
    contract_data_flow,
    person_name,
    with_contract_couplings,
)
from clio_relay.surface import job_card
from clio_relay.surface.service import _steps_label  # pyright: ignore[reportPrivateUsage]

SHA = "a" * 64
PVSERVER = PackageRef(name="clio_jarvis.pvserver", version="1", sha256=SHA, service=True)
SOLVER = PackageRef(name="clio_jarvis.gray_scott", version="1", sha256="b" * 64)


def _entry(ref: PackageRef, **extra: Any) -> PackageEntry:
    return PackageEntry(name=ref.name, version="1", sha256=ref.sha256 or SHA, **extra)


VIEWER = _entry(
    PVSERVER, kind="service", data_flow={"direction": "in", "kind": "stream", "label": "views"}
)
FIELDS = _entry(SOLVER, kind="application", outputs=[{"name": "fields", "path": "gs.bp"}])


def test_a_service_that_reads_is_fed_by_the_steps_with_outputs() -> None:
    # JARVIS starts pvserver first, so it is listed first; the data still flows into it.
    spec = JobSpec(packages=[PVSERVER, SOLVER])
    filled = with_contract_couplings(spec, [VIEWER, FIELDS])
    assert [(e.producer, e.consumer, e.kind, e.label) for e in filled.couplings] == [
        ("gray_scott", "pvserver", "stream", "views")
    ]
    document = jarvis_submit.pipeline_document(filled, pipeline="relay_x")
    assert [p["pkg_name"] for p in document["pkgs"]] == ["pvserver", "gray_scott"]  # type: ignore[index]
    rendered = yaml.safe_load(jarvis_submit.render_pipeline(document))
    assert rendered["couplings"] == [
        {"from": "gray_scott", "to": "pvserver", "kind": "stream", "label": "views"}
    ]


def test_a_service_that_feeds_points_out_and_a_named_coupling_wins() -> None:
    feeder = _entry(PVSERVER, data_flow={"direction": "out", "kind": "service"})
    edges = contract_couplings([PVSERVER, SOLVER], [feeder, FIELDS])
    assert [(e.producer, e.consumer) for e in edges] == [("pvserver", "gray_scott")]
    named = JobSpec(
        packages=[PVSERVER, SOLVER],
        couplings=[{"from": "clio_jarvis.pvserver", "to": "gray_scott", "kind": "control"}],  # type: ignore[list-item]
    )
    assert with_contract_couplings(named, [VIEWER, FIELDS]) is named
    assert jarvis_submit.pipeline_couplings(named) == [
        {"from": "pvserver", "to": "gray_scott", "kind": "control"}
    ]


def test_no_data_flow_and_one_step_give_no_couplings() -> None:
    spec = JobSpec(packages=[PVSERVER, SOLVER])
    assert with_contract_couplings(spec, [_entry(PVSERVER), FIELDS]) is spec
    assert contract_couplings([PVSERVER], [VIEWER]) == []
    assert "couplings" not in spec.dump()
    assert "couplings" not in jarvis_submit.pipeline_document(spec, pipeline="relay_x")


def test_a_contract_data_flow_is_read_only_when_well_formed() -> None:
    assert contract_data_flow({"direction": "in", "kind": "stream", "label": " a  b "}) == {
        "direction": "in",
        "kind": "stream",
        "label": "a b",
    }
    assert contract_data_flow({"direction": "sideways", "kind": "stream"}) is None
    assert contract_data_flow({"direction": "in", "kind": "teleport"}) is None
    assert contract_data_flow(None) is None


@pytest.mark.parametrize(
    ("couplings", "message"),
    [
        ([{"from": "nothing", "to": "pvserver"}], "names no step"),
        ([{"from": "pvserver", "to": "clio_jarvis.pvserver"}], "feeds itself"),
    ],
)
def test_a_coupling_must_join_two_steps(couplings: list[dict[str, str]], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        JobSpec(packages=[PVSERVER, SOLVER], couplings=couplings)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="two or more"):
        JobSpec(package=SOLVER, couplings=[{"from": "a", "to": "b"}])  # type: ignore[list-item]


def test_the_label_names_the_data_direction_and_who_starts_first() -> None:
    spec = JobSpec(packages=[PVSERVER, SOLVER])
    assert _steps_label(spec) == "the pipeline pvserver then gray_scott"
    coupled = with_contract_couplings(spec, [VIEWER, FIELDS])
    assert _steps_label(coupled) == (
        "the pipeline gray_scott feeding pvserver (pvserver starts first)"
    )
    solver_first = with_contract_couplings(JobSpec(packages=[SOLVER, PVSERVER]), [FIELDS, VIEWER])
    assert _steps_label(solver_first) == "the pipeline gray_scott feeding pvserver"


def test_the_label_names_each_package_by_its_contract_title() -> None:
    titles = {PVSERVER.name: "ParaView server", SOLVER.name: "Gray-Scott"}
    coupled = with_contract_couplings(JobSpec(packages=[PVSERVER, SOLVER]), [VIEWER, FIELDS])
    assert _steps_label(coupled, titles) == (
        "the pipeline Gray-Scott feeding ParaView server (ParaView server starts first)"
    )
    assert _steps_label(JobSpec(package=SOLVER), titles) == "Gray-Scott"
    assert person_name("clio_jarvis.lammps", {"clio_jarvis.lammps": "LAMMPS"}) == "LAMMPS"
    assert person_name("clio_jarvis.lammps") == "lammps"
    assert person_name("plain") == "plain"


def test_the_card_names_each_component_by_its_contract_title() -> None:
    record: Any = SimpleNamespace(result={})
    plan = {
        "components": [
            {"id": "lammps", "package": "clio_jarvis.lammps", "nodes": 1, "role": "batch"},
            {"id": "pv", "package": "clio_jarvis.pvserver", "label": "Viewer", "role": "service"},
            {"id": "pvserver", "package": "clio_jarvis.pvserver", "role": "service"},
            {"id": "bare"},
        ]
    }
    titles = {"clio_jarvis.lammps": "LAMMPS"}
    found = job_card._planned(record, plan, "planned", titles)  # pyright: ignore[reportPrivateUsage]
    assert [c["label"] for c in found] == ["LAMMPS", "Viewer", "pvserver", "bare"]


def test_the_card_draws_producer_first_and_keeps_the_launch_order() -> None:
    components = [{"id": "pvserver"}, {"id": "gray_scott"}, {"id": "other"}]
    couplings = [{"from": "gray_scott", "to": "pvserver"}]
    ordered = job_card._in_flow_order(components, couplings)  # pyright: ignore[reportPrivateUsage]
    assert [(c["id"], c["launch_order"]) for c in ordered] == [
        ("gray_scott", 2),
        ("pvserver", 1),
        ("other", 3),
    ]
    alone = job_card._in_flow_order([{"id": "a"}, {"id": "b"}], [])  # pyright: ignore[reportPrivateUsage]
    assert [(c["id"], c["launch_order"]) for c in alone] == [("a", 1), ("b", 2)]


def test_flow_order_keeps_launch_order_for_ties_and_cycles() -> None:
    assert flow_order(["a", "b", "c"], [("c", "a")]) == ["b", "c", "a"]
    assert flow_order(["a", "b"], [("a", "b"), ("b", "a")]) == ["a", "b"]
