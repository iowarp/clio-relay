"""The clio-kit family rows composed into the release pin-site registry.

The registry has enough real sites that keeping every family in
``release_pin_sites.py`` would violate the repository's 800-line no-accretion
cap.  This module owns only the clio-kit distribution rows; the public
``PINSITES`` tuple remains composed and exported by ``release_pin_sites``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clio_relay.release_pin_sites import PinFamily, PinSite, SelectorKind


PinRow = Callable[..., "PinSite"]

_KIT_VERSION = re.compile(r"([0-9]+\.[0-9]+\.[0-9]+)")
_SHA256 = re.compile(r"([0-9a-f]{64})")


def kit_pin_sites(
    row: PinRow,
    *,
    family: PinFamily,
    line: SelectorKind,
    filename: SelectorKind,
    placeholder: SelectorKind,
    regex: SelectorKind,
) -> tuple[PinSite, ...]:
    """Build the ordered clio-kit pin family for the public registry."""
    return (
        # clio-relay#190/#199 deliberately decoupled the default bootstrap
        # install pin from the ares acceptance-policy pin. The acceptance
        # fixture records a past live run, not whatever the current default
        # is, so these remain independent value groups.
        row(
            "kv.jarvis_mcp_version",
            "src/clio_relay/jarvis_mcp.py",
            family,
            line,
            "CLIO_KIT_JARVIS_MCP_VERSION (sole canonical bootstrap definition)",
            line=32,
            pattern=_KIT_VERSION,
            value_group="bootstrap_kit_version_text",
        ),
        row(
            "kv.jarvis_mcp_wheel_sha256",
            "src/clio_relay/jarvis_mcp.py",
            family,
            line,
            "CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 (sole canonical bootstrap definition)",
            line=39,
            pattern=_SHA256,
            value_group="bootstrap_kit_wheel_sha256",
        ),
        row(
            "kv.bootstrap_placeholder",
            "src/clio_relay/bootstrap.py",
            family,
            placeholder,
            "JARVIS_MCP_VERSION script placeholder (f-string interpolation of "
            "CLIO_KIT_JARVIS_MCP_VERSION -- never a literal copy)",
            line=7365,
            placeholder="CLIO_KIT_JARVIS_MCP_VERSION",
            mutable=False,
        ),
        *(
            row(
                f"kv.ci_yml_{job}_filename",
                ".github/workflows/ci.yml",
                family,
                line,
                f"stage exact clio-kit release wheel ({job} build job): filename",
                line=fline,
                pattern=_KIT_VERSION,
                value_group="bootstrap_kit_version_text",
            )
            for job, fline in (("job1", 62), ("job2", 166))
        ),
        *(
            row(
                f"kv.ci_yml_{job}_sha256",
                ".github/workflows/ci.yml",
                family,
                line,
                f"stage exact clio-kit release wheel ({job} build job): SHA-256",
                line=sline,
                pattern=_SHA256,
                value_group="bootstrap_kit_wheel_sha256",
            )
            for job, sline in (("job1", 63), ("job2", 167))
        ),
        *(
            row(
                f"kv.ci_yml_{job}_url",
                ".github/workflows/ci.yml",
                family,
                filename,
                f"stage exact clio-kit release wheel ({job} build job): URL",
                line=uline,
                pattern=_KIT_VERSION,
                value_group="bootstrap_kit_version_text",
            )
            for job, uline in (("job1", 64), ("job2", 168))
        ),
        row(
            "kv.operations_doc",
            "docs/operations.md",
            family,
            filename,
            "Use Remote JARVIS MCP: uv tool install wheel URL",
            line=719,
            pattern=_KIT_VERSION,
            value_group="bootstrap_kit_version_text",
        ),
        row(
            "kv.remote_mcp_federation_filename",
            "docs/remote-mcp-federation.md",
            family,
            filename,
            "kit-pin digests paragraph: exact release wheel filename",
            line=472,
            pattern=_KIT_VERSION,
            value_group="bootstrap_kit_version_text",
        ),
        row(
            "kv.remote_mcp_federation_sha256",
            "docs/remote-mcp-federation.md",
            family,
            line,
            "kit-pin digests paragraph: exact release wheel SHA-256",
            line=473,
            pattern=_SHA256,
            value_group="bootstrap_kit_wheel_sha256",
        ),
        row(
            "kv.remote_mcp_federation_release_gate_prose",
            "docs/remote-mcp-federation.md",
            family,
            line,
            "'the release gate requires that exact ... artifact' prose -- "
            "describes the ACCEPTANCE-policy pin, not the bootstrap default",
            line=467,
            pattern=_KIT_VERSION,
            value_group="acceptance_kit_version_text",
        ),
        *(
            row(
                f"kv.release_gate_text_{gline}",
                "docs/release-gate-1.0.yaml",
                family,
                regex,
                "worker component-identity block: clio-kit version literal "
                "(ares acceptance-policy pin, independent of the bootstrap default)",
                line=gline,
                pattern=_KIT_VERSION,
                value_group="acceptance_kit_version_text",
                sweep="release_gate_kit_text",
            )
            for gline in (115, 121, 122, 226, 230, 231, 294, 299, 300, 302, 309, 374, 1187)
        ),
        *(
            row(
                f"kv.release_gate_digest_{dline}",
                "docs/release-gate-1.0.yaml",
                family,
                regex,
                "worker component-identity block: clio-kit wheel SHA-256 "
                "(ares acceptance-policy pin, independent of the bootstrap default)",
                line=dline,
                pattern=_SHA256,
                value_group="acceptance_kit_wheel_sha256",
                sweep="release_gate_kit_digest",
            )
            for dline in (124, 233, 303, 311, 371, 680, 782, 887)
        ),
    )
