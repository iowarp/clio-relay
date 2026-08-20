"""Tests for the shared JSON/type primitives owner (clio-relay#231)."""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay import provenance_primitives
from clio_relay.provenance_primitives import MAX_JSON_DOCUMENT_BYTES, ProvenanceError


def test_json_loader_rejects_oversized_input_before_decoding(tmp_path: Path) -> None:
    document = tmp_path / "oversized.json"
    with document.open("wb") as stream:
        stream.truncate(MAX_JSON_DOCUMENT_BYTES + 1)

    with pytest.raises(ProvenanceError, match="exceeds"):
        provenance_primitives._load_json(document)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
