"""A plain approval is one boolean; an approval naming new facts is a choice."""

from __future__ import annotations

from typing import Any

import pytest

from clio_relay.adapters.hooks import (
    CHOICE_META,
    Ask,
    Charge,
    ChoiceOption,
    approve_schema,
    choice_meta,
    explain_approval,
    said_yes,
    with_choice_meta,
)
from clio_relay.surface.guarding import cost_text, node_hours_text
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths


def test_plain_is_one_boolean() -> None:
    schema = approve_schema()
    assert schema["properties"]["approve"]["type"] == "boolean"
    assert schema["required"] == ["approve"]


def test_typed_is_a_titled_choice() -> None:
    answer = approve_schema(plain=False)["properties"]["approve"]
    assert answer["type"] == "string"
    assert [item["const"] for item in answer["oneOf"]] == ["approve", "decline"]


@pytest.mark.parametrize(
    ("content", "yes"),
    [
        ({"approve": True}, True),
        ({"approve": "approve"}, True),
        ({"approve": False}, False),
        ({"approve": "decline"}, False),
        ({"approve": "yes"}, False),
        ({}, False),
        (None, False),
    ],
)
def test_said_yes(content: dict[str, object] | None, yes: bool) -> None:
    assert said_yes(content) is yes


@pytest.mark.parametrize(("value", "state"), [("approve", "accepted"), ("decline", "declined")])
def test_answer_reads_the_choice(relay_paths: RelayPaths, value: str, state: str) -> None:
    service = RelayService(relay_paths)
    ask = Ask(
        ask_id="ask_typed",
        kind="approve",
        site="fx",
        message="Spend 2 node-hours?",
        requested_schema=approve_schema(plain=False),
    )
    service.asks.put(ask.ask_id, ask)
    assert service.answer(ask.ask_id, "accept", {"approve": value}, by="test").state == state


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (1, "1 node-hour"),
        (0.5, "0.5 node-hours"),
        (0.1667, "about 0.17 node-hours"),
        (2.26, "about 2.3 node-hours"),
        (12.4, "about 12 node-hours"),
        (0.004, "under 0.01 node-hours"),
        (0, "0 node-hours"),
    ],
)
def test_node_hours_read_as_a_person_says_them(value: float, text: str) -> None:
    assert node_hours_text(value) == text


def test_the_cost_names_the_walltime_the_nodes_and_the_node_hours() -> None:
    lammps = Charge(site="ares", nodes=1, walltime_s=600, node_hours=0.1667)
    assert cost_text(lammps) == "up to 10 min on 1 node (about 0.17 node-hours)"
    wide = Charge(site="ares", nodes=2, walltime_s=5400, node_hours=3)
    assert cost_text(wide) == "up to 1 h 30 min on 2 nodes (3 node-hours)"
    assert cost_text(Charge(site="ares", nodes=1)) is None


def test_an_approval_says_what_each_answer_does() -> None:
    ask = Ask(
        ask_id="ask_spend",
        kind="approve",
        site="fx",
        message="Run LAMMPS on Ares?",
        requested_schema=approve_schema(plain=False),
        meta={"other": 1},
    )
    explain_approval(ask, title="Run LAMMPS on Ares", yes="Submits it.", no="Nothing runs.")
    schema: dict[str, Any] = ask.requested_schema or {}
    field = schema["properties"]["approve"]
    assert field["title"] == "Run LAMMPS on Ares"
    assert field["oneOf"] == [
        {"const": "approve", "title": "Approve", "description": "Submits it."},
        {"const": "decline", "title": "Decline", "description": "Nothing runs."},
    ]
    assert schema["required"] == ["approve"]
    meta = ask.meta or {}
    assert meta["other"] == 1
    typed = meta[CHOICE_META]["ask_spend"]["approve"]
    assert [item["label"] for item in typed["options"]] == ["Approve", "Decline"]
    assert typed["recommended"] is None
    assert said_yes({"approve": "approve"})
    plain = Ask(
        ask_id="ask_plain",
        kind="approve",
        site="fx",
        message="m",
        requested_schema=approve_schema(),
    )
    explain_approval(plain, title="t", yes="y", no="n")
    assert plain.requested_schema == approve_schema() and plain.meta is None


def test_two_choices_on_one_ask_keep_both() -> None:
    first = choice_meta(
        "ask_1", "approve", [ChoiceOption(value="approve", label="Approve")], title="Run it"
    )
    second = choice_meta(
        "ask_1", "held", [ChoiceOption(value="keep", label="Keep")], title="Services"
    )
    merged = with_choice_meta(with_choice_meta({"job": 1}, first), second)
    assert set(merged[CHOICE_META]["ask_1"]) == {"approve", "held"}
    assert merged["job"] == 1
