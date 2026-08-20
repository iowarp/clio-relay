"""A .dynstr that spans two PT_LOAD segments must still resolve (#158).

Found upgrading the ares deployment. CPython 3.12.13 as shipped by uv lays out
its dynamic string table starting inside one PT_LOAD and continuing into the
next; the two are contiguous and share a vaddr-to-offset delta, so the file
offset is perfectly well defined. The provider verification required a SINGLE
segment to contain the ENTIRE table, so it found zero candidates and refused
the binary as "ambiguous" -- blocking every staged upgrade with a message that
described the wrong problem.

Measured on the real binary:

    LOAD  offset=0x0      vaddr=0x3ff000  filesz=0x1000
    LOAD  offset=0x1000   vaddr=0x400000  filesz=0x20a08
    DT_STRTAB=0x3ff400  DT_STRSZ=0xa415   ->  ends at 0x409815

The table starts in the first segment and ends inside the second.
"""

from __future__ import annotations

import re
from pathlib import Path

from clio_relay import bootstrap

# The exact layout measured on ares.
LOAD_SEGMENTS = [
    (0x0, 0x3FF000, 0x1000),
    (0x1000, 0x400000, 0x20A08),
    (0x22000, 0x421000, 0x9EA649),
]
STRTAB_ADDRESS = 0x3FF400
STRTAB_SIZE = 0xA415


def _resolve_old(address: int, size: int) -> list[int]:
    """The previous predicate: one segment had to contain the whole table."""
    return [
        file_offset + address - vaddr
        for file_offset, vaddr, filesz in LOAD_SEGMENTS
        if vaddr <= address and address + size <= vaddr + filesz
    ]


def _resolve_new(address: int, size: int) -> set[int]:
    """The shipped predicate: locate by the segment holding the START."""
    return {
        file_offset + address - vaddr
        for file_offset, vaddr, filesz in LOAD_SEGMENTS
        if vaddr <= address < vaddr + filesz
    }


def test_the_real_layout_was_refused_by_the_old_predicate() -> None:
    assert _resolve_old(STRTAB_ADDRESS, STRTAB_SIZE) == []


def test_a_spanning_string_table_now_resolves_to_one_offset() -> None:
    candidates = _resolve_new(STRTAB_ADDRESS, STRTAB_SIZE)
    assert len(candidates) == 1
    # vaddr-to-offset delta is 0x3ff000 for both segments, so the offset is exact.
    assert candidates == {0x400}


def test_segments_that_disagree_are_still_refused() -> None:
    """The anti-tamper property must survive the relaxation."""
    conflicting = [
        (0x0, 0x1000, 0x2000),
        (0x9000, 0x1000, 0x2000),  # same vaddr, different file offset
    ]
    candidates = {
        file_offset + 0x1400 - vaddr
        for file_offset, vaddr, filesz in conflicting
        if vaddr <= 0x1400 < vaddr + filesz
    }
    assert len(candidates) != 1


def test_both_provider_paths_ship_the_spanning_resolution() -> None:
    """Guard the rendered programs against a regression to whole-table containment."""
    source = Path(bootstrap.__file__).read_text(encoding="utf-8")
    assert source.count("virtual_address <= string_address < virtual_address + file_size") == 2
    assert not re.search(r"string_address \+ string_size <= virtual_address \+ file_size", source)
    # The relaxation is paired with an explicit file bound at both sites.
    assert source.count("ELF string table exceeds the file") == 2
