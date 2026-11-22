"""A plain approval is one boolean; an approval naming new facts is a choice."""

from __future__ import annotations

import pytest

from clio_relay.adapters.hooks import Ask, approve_schema, said_yes
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
