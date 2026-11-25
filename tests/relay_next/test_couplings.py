"""A pipeline's couplings run producer to consumer, whatever order JARVIS starts it in."""

from __future__ import annotations

from typing import Any

import pytest
import yaml

from clio_relay import jarvis_submit
from clio_relay.jobs.spec import JobSpec, PackageRef, flow_order
from clio_relay.packages import (
    PackageEntry,
    contract_couplings,
    contract_data_flow,
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


def test_nocontract_data_flow_and_one_step_give_no_couplings() -> None:
    spec = JobSpec(packages=[PVSERVER, SOLVER])
    assert with_contract_couplings(spec, [_entry(PVSERVER), FIELDS]) is spec
    assert contract_couplings([PVSERVER], [VIEWER]) == []
    assert "couplings" not in spec.dump()
    assert "couplings" not in jarvis_submit.pipeline_document(spec, pipeline="relay_x")


def test_a_contractcontract_data_flow_is_read_only_when_well_formed() -> None:
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
    assert _steps_label(spec) == "the pipeline clio_jarvis.pvserver then clio_jarvis.gray_scott"
    coupled = with_contract_couplings(spec, [VIEWER, FIELDS])
    assert _steps_label(coupled) == (
        "the pipeline clio_jarvis.gray_scott feeding clio_jarvis.pvserver "
        "(clio_jarvis.pvserver starts first)"
    )
    solver_first = with_contract_couplings(JobSpec(packages=[SOLVER, PVSERVER]), [FIELDS, VIEWER])
    assert _steps_label(solver_first) == (
        "the pipeline clio_jarvis.gray_scott feeding clio_jarvis.pvserver"
    )


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
