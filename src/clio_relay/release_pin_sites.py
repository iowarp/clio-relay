"""The :data:`PINSITES` table -- every release-identity pin, named once.

Split from :mod:`clio_relay.release_pins` (iowarp/clio-relay#231 R7, ground
rule 6): the registry genuinely has ~70 real sites (doc
``docs/design/relay-architecture-2026-08.md`` §7's own audit plus this
module's completeness sweep), and a data table that size does not fit under
the 800-line cap in the same file as the logic that reads it -- the same
reasoning that split ``frp_link.py`` from ``frp_transport.py`` in R4/R5. This
is a private companion: callers import everything from
:mod:`clio_relay.release_pins`, which re-exports :data:`PINSITES` and the
types below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from clio_relay.errors import RelayError


class PinFamily(StrEnum):
    """Which release-identity axis a :class:`PinSite` belongs to."""

    RELAY_VERSION = "relay_version"
    MATRIX_DIGEST = "matrix_digest"
    JARVIS_CONTRACT = "jarvis_contract"
    KIT_VERSION = "kit_version"


class SelectorKind(StrEnum):
    """How a :class:`PinSite`'s current value is recognized (doc §7)."""

    LINE = "line"
    KEY = "key"
    FILENAME = "filename"
    PLACEHOLDER = "placeholder"
    REGEX = "regex"
    DERIVED_DIGEST = "derived_digest"


class PinSiteError(RelayError):
    """Base class for release-pin registry failures."""


class PinSiteDrifted(PinSiteError):
    """A registered site no longer matches its recorded shape.

    Raised when a site's line number, key path, or filename template no
    longer finds the value it pins -- the site moved or was hand-edited out
    from under the registry, exactly the silent drift this module exists to
    make loud.
    """

    def __init__(self, site: PinSite, reason: str) -> None:
        self.site = site
        self.reason = reason
        super().__init__(f"{site.id} ({site.path}:{site.line}): {reason}")


@dataclass(frozen=True)
class PinSite:
    """One place in the tree where a release-identity value is pinned.

    Attributes:
        id: Stable, unique identifier (used in reports and sabotage tests).
        path: POSIX path relative to the repository root.
        family: Which release-identity axis this site belongs to.
        kind: How the value is recognized (doc §7's selector taxonomy).
        summary: One-line human description for reports.
        line: 1-indexed line number. Used by ``LINE``/``REGEX``/
            ``PLACEHOLDER`` sites and by YAML ``KEY``/``DERIVED_DIGEST``
            sites, all matched by a single-line pattern rather than a
            structured parse.
        pattern: Regex with exactly one capture group isolating the current
            value. May match more than once per line (e.g. a URL that
            embeds the version twice); every match must agree.
        key_path: Key path into a JSON document (``KEY``/``DERIVED_DIGEST``
            sites whose file is JSON, read via a real ``json.loads``).
        placeholder: Name of the constant a ``PLACEHOLDER`` site references
            indirectly (never a literal copy -- nothing to rewrite).
        filename_template: For a ``FILENAME`` site whose own *path* embeds
            the value (e.g. ``"jarvis-user-{value}.json"``) -- a bump
            renames the file instead of editing its content.
        mutable: ``False`` for a site tracked for completeness but
            deliberately never asserted-to-agree or rewritten (a frozen,
            stable historical label -- doc §7's
            ``ares-secure-jarvis-runtime`` check-id/description pair, which
            permanently says "v3.6" by design, not by drift).
        value_group: Sites that must currently hold the identical value.
            ``None`` for frozen/placeholder sites, never agreement-checked.
        sweep: Name of the completeness sweep this site participates in
            (``None`` if its own line is the only source of truth for its
            existence -- see ``release_pins.sweep_incompleteness``).
    """

    id: str
    path: str
    family: PinFamily
    kind: SelectorKind
    summary: str
    line: int | None = None
    pattern: re.Pattern[str] | None = None
    key_path: tuple[str, ...] | None = None
    placeholder: str | None = None
    filename_template: str | None = None
    mutable: bool = True
    value_group: str | None = None
    sweep: str | None = None


def _row(
    id_: str,
    path: str,
    family: PinFamily,
    kind: SelectorKind,
    summary: str,
    **kwargs: object,
) -> PinSite:
    return PinSite(id=id_, path=path, family=family, kind=kind, summary=summary, **kwargs)  # type: ignore[arg-type]


# Shared patterns -- one compiled object per shape, reused across rows.
#: The JARVIS MCP user contract id/filename, in any of its surface forms:
#: `clio-kit-jarvis-user-vX.Y`, bare `jarvis-user-vX.Y.json`, and the
#: RST-backtick-quoted docstring form -- all share this one capture.
_CONTRACT = re.compile(r"(?:clio-kit-)?jarvis-user-(v[0-9]+\.[0-9]+)")
#: A bare `vX.Y` token with no surrounding contract-name context (doc §7's
#: two frozen "v3.6" sites: a description sentence and a stable check-id).
_BARE_V = re.compile(r"\b(v[0-9]+\.[0-9]+)\b")
#: A clio-kit-shaped `X.Y.Z` version literal, wherever it appears on a line.
_KV = re.compile(r"([0-9]+\.[0-9]+\.[0-9]+)")
#: A lowercase SHA-256 hex digest, wherever it appears on the line.
_HEX = re.compile(r"([0-9a-f]{64})")

_RV, _MD, _JC, _KV_FAM = (
    PinFamily.RELAY_VERSION,
    PinFamily.MATRIX_DIGEST,
    PinFamily.JARVIS_CONTRACT,
    PinFamily.KIT_VERSION,
)
_LN, _KY, _FN, _PH, _RX, _DD = (
    SelectorKind.LINE,
    SelectorKind.KEY,
    SelectorKind.FILENAME,
    SelectorKind.PLACEHOLDER,
    SelectorKind.REGEX,
    SelectorKind.DERIVED_DIGEST,
)
_CONTRACT_JSON = "src/clio_relay/_contracts/jarvis-user-v3.7.json"

PINSITES: tuple[PinSite, ...] = (
    # -- relay_version: clio-relay's own release version (4 sites) --------
    _row(
        "relay.pyproject",
        "pyproject.toml",
        _RV,
        _LN,
        "clio-relay package version",
        line=3,
        pattern=re.compile(r'^version = "([^"]+)"$'),
        value_group="relay_version",
    ),
    _row(
        "relay.package_init",
        "src/clio_relay/__init__.py",
        _RV,
        _LN,
        "clio-relay __version__",
        line=5,
        pattern=re.compile(r'^__version__ = "([^"]+)"$'),
        value_group="relay_version",
    ),
    _row(
        "relay.release_gate_version",
        "docs/release-gate-1.0.yaml",
        _RV,
        _KY,
        "release-gate policy release_version key",
        line=6,
        pattern=re.compile(r'^release_version: "([^"]+)"$'),
        value_group="relay_version",
    ),
    _row(
        "relay.matrix_version",
        "examples/release-gate/report-matrix-1.0.json",
        _RV,
        _KY,
        "acceptance matrix release_version key",
        key_path=("release_version",),
        value_group="relay_version",
    ),
    # -- matrix_digest: derived, recomputed strictly after every other -----
    # -- relay_version/matrix site is bumped (doc §7's ordering rule) ------
    _row(
        "matrix.canonical_digest",
        "examples/release-gate/report-matrix-1.0.json",
        _MD,
        _DD,
        "acceptance matrix self-digest (canonical)",
        key_path=("matrix_sha256",),
        value_group="matrix_digest",
    ),
    _row(
        "matrix.release_gate_mirror",
        "docs/release-gate-1.0.yaml",
        _MD,
        _DD,
        "release-gate policy acceptance_matrix_sha256 mirror",
        line=8,
        pattern=re.compile(r"^acceptance_matrix_sha256: ([0-9a-f]{64})$"),
        value_group="matrix_digest",
    ),
    # -- jarvis_contract: the user contract id/path literal (§1's ---------
    # -- "13-copy v3.7" story) -- mutable literal sites --------------------
    _row(
        "jc.cluster_config",
        "src/clio_relay/cluster_config.py",
        _JC,
        _LN,
        "RemoteMcpContract Literal member",
        line=280,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.installation",
        "src/clio_relay/installation.py",
        _JC,
        _LN,
        "CLIO_KIT_JARVIS_CONTRACT_ID",
        line=54,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.jarvis_input_plane_docstring",
        "src/clio_relay/jarvis_input_plane.py",
        _JC,
        _LN,
        "module docstring cross-reference",
        line=7,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.jarvis_mcp_contract_id",
        "src/clio_relay/jarvis_mcp.py",
        _JC,
        _LN,
        "CLIO_KIT_JARVIS_USER_CONTRACT_ID",
        line=41,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.jarvis_mcp_contract_path_ref",
        "src/clio_relay/jarvis_mcp.py",
        _JC,
        _FN,
        "_JARVIS_USER_CONTRACT_PATH file-name reference",
        line=80,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.models_registered",
        "src/clio_relay/models.py",
        _JC,
        _LN,
        "REGISTERED_JARVIS_USER_CONTRACT",
        line=43,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.models_package_route",
        "src/clio_relay/models.py",
        _JC,
        _LN,
        "JarvisPackageInputRoute.contract Literal + default",
        line=506,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.models_pipeline_route",
        "src/clio_relay/models.py",
        _JC,
        _LN,
        "JarvisPipelineInputRoute.contract Literal + default",
        line=618,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.remote_mcp_artifact_filename_ref",
        "src/clio_relay/remote_mcp.py",
        _JC,
        _FN,
        "CLIO_KIT_JARVIS_USER_CONTRACT_ARTIFACT_BY_ID current-contract entry",
        line=129,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.contract_file_rename",
        _CONTRACT_JSON,
        _JC,
        _FN,
        "vendored contract file -- the version IS the path",
        filename_template="jarvis-user-{value}.json",
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.contract_file_content",
        _CONTRACT_JSON,
        _JC,
        _KY,
        "vendored contract file's own contract_id field",
        line=3,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.runner_registered_contract",
        "jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py",
        _JC,
        _LN,
        "REGISTERED_JARVIS_EXECUTION_QUERY_CONTRACT",
        line=68,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_jarvis_mcp_validation",
        "tests/test_jarvis_mcp_validation.py",
        _JC,
        _LN,
        "docstring cross-reference",
        line=2100,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_remote_mcp_120",
        "tests/test_remote_mcp.py",
        _JC,
        _LN,
        "contract-id/filename tuple entry",
        line=120,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_remote_mcp_1391",
        "tests/test_remote_mcp.py",
        _JC,
        _LN,
        "vendored contract file path reference",
        line=1391,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_remote_mcp_1410",
        "tests/test_remote_mcp.py",
        _JC,
        _LN,
        "contract= keyword argument",
        line=1410,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_release_workflows_native_execution",
        "tests/test_release_workflows.py",
        _JC,
        _LN,
        "release-gate policy fixture assertion: native_execution.contract_id",
        line=368,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.release_gate_lammps_contract",
        "docs/release-gate-1.0.yaml",
        _JC,
        _RX,
        "ares-jarvis-lammps-package-progress worker component contract_id",
        line=131,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
        sweep="release_gate_contract_id",
    ),
    _row(
        "jc.release_gate_queue_contract",
        "docs/release-gate-1.0.yaml",
        _JC,
        _RX,
        "ares-queue-management worker component contract_id",
        line=320,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
        sweep="release_gate_contract_id",
    ),
    _row(
        "jc.remote_mcp_federation_contract_id",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "kit-pin digests paragraph: canonical contract",
        line=474,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.remote_mcp_federation_contract_filename",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "kit-pin digests paragraph: bundled contract file reference",
        line=476,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    # -- jarvis_contract: frozen historical labels (tracked, never rewritten)
    _row(
        "jc.release_gate_secure_runtime_description",
        "docs/release-gate-1.0.yaml",
        _JC,
        _LN,
        "ares-secure-jarvis-runtime requirement description (stable label)",
        line=1109,
        pattern=_BARE_V,
        mutable=False,
    ),
    _row(
        "jc.release_gate_secure_runtime_check_id",
        "docs/release-gate-1.0.yaml",
        _JC,
        _LN,
        "secure-runtime.jarvis-v3.6-query check id (stable label)",
        line=1115,
        pattern=_BARE_V,
        mutable=False,
    ),
    # -- jarvis_contract: content-identity digests (canonical + mirrors) --
    _row(
        "jc.contract_sha256_canonical",
        "src/clio_relay/remote_mcp.py",
        _JC,
        _LN,
        "CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID current-contract entry",
        line=106,
        pattern=_HEX,
        value_group="jarvis_contract_sha256",
    ),
    _row(
        "jc.release_gate_contract_sha256_lammps",
        "docs/release-gate-1.0.yaml",
        _JC,
        _RX,
        "ares-jarvis-lammps-package-progress contract_sha256",
        line=133,
        pattern=_HEX,
        value_group="jarvis_contract_sha256",
        sweep="release_gate_contract_sha256",
    ),
    _row(
        "jc.release_gate_contract_sha256_queue",
        "docs/release-gate-1.0.yaml",
        _JC,
        _RX,
        "ares-queue-management contract_sha256",
        line=322,
        pattern=_HEX,
        value_group="jarvis_contract_sha256",
        sweep="release_gate_contract_sha256",
    ),
    _row(
        "jc.remote_mcp_federation_contract_sha256",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "kit-pin digests paragraph: contract SHA-256",
        line=479,
        pattern=_HEX,
        value_group="jarvis_contract_sha256",
    ),
    _row(
        "jc.wire_sha256_canonical",
        "src/clio_relay/remote_mcp.py",
        _JC,
        _LN,
        "CLIO_KIT_JARVIS_USER_WIRE_SHA256_BY_ID current-contract entry",
        line=114,
        pattern=_HEX,
        value_group="jarvis_contract_wire_sha256",
    ),
    _row(
        "jc.remote_mcp_federation_wire_sha256",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "kit-pin digests paragraph: canonical tools-wire SHA-256",
        line=481,
        pattern=_HEX,
        value_group="jarvis_contract_wire_sha256",
    ),
    _row(
        "jc.artifact_sha256_canonical",
        "src/clio_relay/remote_mcp.py",
        _JC,
        _LN,
        "CLIO_KIT_JARVIS_USER_ARTIFACT_SHA256_BY_ID current-contract entry",
        line=122,
        pattern=_HEX,
        value_group="jarvis_contract_artifact_sha256",
    ),
    _row(
        "jc.remote_mcp_federation_artifact_sha256",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "kit-pin digests paragraph: bundled contract artifact SHA-256",
        line=483,
        pattern=_HEX,
        value_group="jarvis_contract_artifact_sha256",
    ),
    # -- kit_version: the clio-kit distribution pin (canonical + mirrors) -
    _row(
        "kv.jarvis_mcp_version",
        "src/clio_relay/jarvis_mcp.py",
        _KV_FAM,
        _LN,
        "CLIO_KIT_JARVIS_MCP_VERSION (sole canonical definition)",
        line=32,
        pattern=_KV,
        value_group="kit_version_text",
    ),
    _row(
        "kv.jarvis_mcp_wheel_sha256",
        "src/clio_relay/jarvis_mcp.py",
        _KV_FAM,
        _LN,
        "CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 (sole canonical definition)",
        line=39,
        pattern=_HEX,
        value_group="kit_wheel_sha256",
    ),
    _row(
        "kv.bootstrap_placeholder",
        "src/clio_relay/bootstrap.py",
        _KV_FAM,
        _PH,
        "JARVIS_MCP_VERSION script placeholder (f-string interpolation of "
        "CLIO_KIT_JARVIS_MCP_VERSION -- never a literal copy)",
        line=7351,
        placeholder="CLIO_KIT_JARVIS_MCP_VERSION",
        mutable=False,
    ),
    *(
        _row(
            f"kv.ci_yml_{job}_filename",
            ".github/workflows/ci.yml",
            _KV_FAM,
            _LN,
            f"stage exact clio-kit release wheel ({job} build job): filename",
            line=fline,
            pattern=_KV,
            value_group="kit_version_text",
        )
        for job, fline in (("job1", 62), ("job2", 166))
    ),
    *(
        _row(
            f"kv.ci_yml_{job}_sha256",
            ".github/workflows/ci.yml",
            _KV_FAM,
            _LN,
            f"stage exact clio-kit release wheel ({job} build job): SHA-256",
            line=sline,
            pattern=_HEX,
            value_group="kit_wheel_sha256",
        )
        for job, sline in (("job1", 63), ("job2", 167))
    ),
    *(
        _row(
            f"kv.ci_yml_{job}_url",
            ".github/workflows/ci.yml",
            _KV_FAM,
            _FN,
            f"stage exact clio-kit release wheel ({job} build job): URL",
            line=uline,
            pattern=_KV,
            value_group="kit_version_text",
        )
        for job, uline in (("job1", 64), ("job2", 168))
    ),
    _row(
        "kv.operations_doc",
        "docs/operations.md",
        _KV_FAM,
        _FN,
        "Use Remote JARVIS MCP: uv tool install wheel URL",
        line=719,
        pattern=_KV,
        value_group="kit_version_text",
    ),
    _row(
        "kv.remote_mcp_federation_filename",
        "docs/remote-mcp-federation.md",
        _KV_FAM,
        _FN,
        "kit-pin digests paragraph: exact release wheel filename",
        line=472,
        pattern=_KV,
        value_group="kit_version_text",
    ),
    _row(
        "kv.remote_mcp_federation_sha256",
        "docs/remote-mcp-federation.md",
        _KV_FAM,
        _LN,
        "kit-pin digests paragraph: exact release wheel SHA-256",
        line=473,
        pattern=_HEX,
        value_group="kit_wheel_sha256",
    ),
    _row(
        "kv.remote_mcp_federation_release_gate_prose",
        "docs/remote-mcp-federation.md",
        _KV_FAM,
        _LN,
        "'the release gate requires that exact ... artifact' prose",
        line=467,
        pattern=_KV,
        value_group="kit_version_text",
    ),
    *(
        _row(
            f"kv.release_gate_text_{gline}",
            "docs/release-gate-1.0.yaml",
            _KV_FAM,
            _RX,
            "worker component-identity block: clio-kit version literal",
            line=gline,
            pattern=_KV,
            value_group="kit_version_text",
            sweep="release_gate_kit_text",
        )
        for gline in (115, 121, 122, 226, 230, 231, 294, 299, 300, 302, 309, 374, 1187)
    ),
    *(
        _row(
            f"kv.release_gate_digest_{dline}",
            "docs/release-gate-1.0.yaml",
            _KV_FAM,
            _RX,
            "worker component-identity block: clio-kit wheel SHA-256",
            line=dline,
            pattern=_HEX,
            value_group="kit_wheel_sha256",
            sweep="release_gate_kit_digest",
        )
        for dline in (124, 233, 303, 311, 371, 680, 782, 887)
    ),
)
