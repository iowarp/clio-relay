"""JARVIS-contract test-file cross-reference rows for the pin-site registry.

``release_pin_sites.py`` is at its 800-line no-accretion cap; this module
owns only the ``jarvis_contract_id`` rows whose site lives in a *test* file
(as opposed to the core source rows, which stay in the main module). The
public ``PINSITES`` tuple remains composed and exported by
``release_pin_sites`` -- see ``release_pin_sites_kit.py`` for the same split
applied to the clio-kit distribution rows.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clio_relay.release_pin_sites import PinFamily, PinSite, SelectorKind


PinRow = Callable[..., "PinSite"]

#: Same shape as release_pin_sites.py's own ``_CONTRACT`` -- duplicated here
#: rather than imported (a trivial one-liner, matching release_pin_sites_kit.py's
#: precedent of owning its own patterns rather than reaching back into the
#: module it was split out of).
_CONTRACT = re.compile(r"(?:clio-kit-)?jarvis-user-(v[0-9]+\.[0-9]+(?:\.[0-9]+)?)")


def jarvis_contract_test_pin_sites(
    row: PinRow,
    *,
    family: PinFamily,
    line: SelectorKind,
    filename: SelectorKind,
) -> tuple[PinSite, ...]:
    """Build the JARVIS-contract test-file rows for the public registry.

    clio-relay#288 recert (kit 2.10.5): these are the sites the newly-live
    ``jarvis_contract_v37`` completeness sweep found unregistered once
    ``_CURRENT_CONTRACT`` advanced from v3.7.1 to v3.7.2 -- test-file
    literals that were always present but never had a registry row.
    """
    return (
        row(
            "jc.test_release_workflows_digest_provenance_comment",
            "tests/test_release_workflows.py",
            family,
            line,
            "digest-provenance comment naming the vendored contract file",
            line=396,
            pattern=_CONTRACT,
            value_group="jarvis_contract_id",
        ),
        row(
            "jc.test_clio_kit_mcp_contracts_expected_key",
            "tests/test_clio_kit_mcp_contracts.py",
            family,
            line,
            "EXPECTED_CONTRACTS active-revision dict key",
            line=67,
            pattern=_CONTRACT,
            value_group="jarvis_contract_id",
        ),
        row(
            "jc.test_clio_kit_mcp_contracts_expected_artifact",
            "tests/test_clio_kit_mcp_contracts.py",
            family,
            filename,
            "EXPECTED_CONTRACTS active-revision artifact filename",
            line=69,
            pattern=_CONTRACT,
            value_group="jarvis_contract_id",
        ),
        row(
            "jc.test_clio_kit_mcp_contracts_jarvis_tools_lookup",
            "tests/test_clio_kit_mcp_contracts.py",
            family,
            line,
            "shipped_contracts[...] active-revision lookup (jarvis_tools projection)",
            line=359,
            pattern=_CONTRACT,
            value_group="jarvis_contract_id",
        ),
        row(
            "jc.test_clio_kit_mcp_contracts_wire_sha256_lookup",
            "tests/test_clio_kit_mcp_contracts.py",
            family,
            line,
            "shipped_contracts[...] active-revision lookup (wire_sha256 assertion)",
            line=519,
            pattern=_CONTRACT,
            value_group="jarvis_contract_id",
        ),
    )
