"""The vendored view descriptor schema and the neutral provenance block (RL43, RL45)."""

from __future__ import annotations

import hashlib

from clio_relay.jobs.handles import JobHandle, OutputRef
from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.serve import descriptor
from clio_relay.serve.provenance import PROVENANCE_KEY, SCHEMA, for_output

DIGEST = "b" * 64


def test_vendored_schema_is_the_published_bytes() -> None:
    data = descriptor.schema_bytes()
    assert hashlib.sha256(data).hexdigest() == descriptor.SOURCE["sha256"]
    assert descriptor.schema()["properties"]["schema"]["const"] == descriptor.SCHEMA_ID


def test_descriptor_problems_name_the_field() -> None:
    good = {
        "schema": "clio.view-descriptor.v1",
        "path": "/scratch/run/gs.bp",
        "path_kind": "directory",
        "format": "adios2-bp5",
        "reader": "ADIOS2VTXReader",
        "growing": True,
    }
    assert descriptor.problems(good) == []
    bad = {**good, "path": "relative/gs.bp"}
    del bad["reader"]
    found = descriptor.problems(bad)
    assert any(item.startswith("(root):") and "reader" in item for item in found)
    assert any(item.startswith("path:") for item in found)


def test_provenance_block_is_neutral() -> None:
    record = OperationRecord(
        operation_id="op_1",
        kind="run",
        site="ares",
        adapter="ssh",
        spec={"job": {"package": {"name": "gray_scott", "version": "0.1.0", "sha256": DIGEST}}},
        spec_digest="c" * 64,
        input_digests=["d" * 64],
        handle=JobHandle(adapter="ssh", native_id="27722"),
        state=OperationState.COMPLETED,
    )
    block = for_output(record, OutputRef(name="gs.bp", path="/run/gs.bp", size=10)).block()
    assert PROVENANCE_KEY == "provenance"
    assert block["schema"] == SCHEMA == "relay.provenance.v1"
    assert block["native_id"] == "27722"
    assert block["package"] == {"name": "gray_scott", "version": "0.1.0", "sha256": DIGEST}
    assert (block["uri"], block["size_bytes"]) == ("/run/gs.bp", 10)
    assert "clio" not in str(block).lower()
