"""A client that reads relay's ask ``_meta`` gets the question alone; any other, the whole text."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from clio_relay.adapters.hooks import ASK_META, CHOICE_META, Ask, ask_text, short_ask
from clio_relay.surface.elicit import (
    elicit_document,
    input_required,
    reads_ask_meta,
    request_reads_ask_meta,
)

COMPILER = (
    "Compiler gcc@12.3.0 cannot run on the build host n1. Which compiler should builds use?"
    "\n\nspack -e x install\nlog: /op_1/log"
)


class _Ctx:
    def __init__(self, declared: tuple[str, ...] = (), capabilities: Any = None) -> None:
        self._declared = declared
        self.session = SimpleNamespace(client_capabilities=capabilities)

    def client_extension_settings(self, identifier: str) -> dict[str, Any] | None:
        return {} if identifier in self._declared else None


def _ask() -> Ask:
    return short_ask(Ask(ask_id="ask_c", kind="approve", site="fx", message=COMPILER))


def test_a_client_declares_it_reads_the_ask_meta() -> None:
    assert not reads_ask_meta(None)
    assert not reads_ask_meta(_Ctx())  # pyright: ignore[reportArgumentType]
    assert reads_ask_meta(_Ctx((ASK_META,)))  # pyright: ignore[reportArgumentType]
    assert reads_ask_meta(_Ctx((CHOICE_META,)))  # pyright: ignore[reportArgumentType]
    for group in ("extensions", "experimental"):
        legacy = SimpleNamespace(extensions=None, experimental=None)
        setattr(legacy, group, {ASK_META: {}})
        assert reads_ask_meta(_Ctx(capabilities=legacy))  # pyright: ignore[reportArgumentType]
    declared: dict[str, Any] = {
        "io.modelcontextprotocol/clientCapabilities": {"extensions": {ASK_META: {}}}
    }
    request = SimpleNamespace(params={"_meta": declared}, session=None)
    assert request_reads_ask_meta(request)
    assert not request_reads_ask_meta(SimpleNamespace(params={}, session=None))
    assert not request_reads_ask_meta(None)


def test_one_path_words_the_ask_for_either_client() -> None:
    ask = _ask()
    assert ask.message == "Which compiler should builds use?"
    assert short_ask(ask) is ask and ask.message == "Which compiler should builds use?"
    whole = short_ask(ask, reads_meta=False)
    assert whole is not ask and whole.message == ask_text(ask)
    assert whole.message.startswith("Which compiler should builds use?\n\nCompiler gcc@12.3.0")
    assert "log: /op_1/log" in whole.message
    assert ask.message == "Which compiler should builds use?"

    reader = elicit_document(ask, reads_meta=True)["params"]
    generic = elicit_document(ask, reads_meta=False)["params"]
    assert reader["message"] == "Which compiler should builds use?"
    assert generic["message"] == ask_text(ask)
    for params in (reader, generic):
        assert "log: /op_1/log" in params["_meta"][ASK_META]["ask_c"]["detail"]

    for reads_meta in (True, False):
        found = input_required([_ask()], "state", reads_meta=reads_meta)
        dumped = found.model_dump(by_alias=True, mode="json", exclude_none=True)
        message = dumped["inputRequests"]["ask_c"]["params"]["message"]
        assert (message == "Which compiler should builds use?") is reads_meta
        assert "ask_c" in (found.meta or {})[ASK_META]
