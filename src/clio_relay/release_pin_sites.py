"""The :data:`PINSITES` table -- every release-identity pin, named once.

Split from :mod:`clio_relay.release_pins` (iowarp/clio-relay#231 R7, ground
rule 6): the registry genuinely has ~70 real sites (doc
``docs/design/relay-architecture-2026-08.md`` §7's own audit plus this
module's completeness sweep), and a data table that size does not fit under
the 800-line cap in the same file as the logic that reads it -- the same
reasoning that split ``frp_link.py`` from ``frp_transport.py`` in R4/R5. This
and the family-specific data module are private companions: callers import everything from
:mod:`clio_relay.release_pins`, which re-exports :data:`PINSITES` and the
types below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from clio_relay.errors import RelayError
from clio_relay.release_pin_sites_kit import kit_pin_sites


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
        dynamic_path: ``True`` when ``path`` is a ``{value}`` template, not
            a literal -- the vendored contract file's own path (and its
            internal content, once renamed) can only be found by first
            reading a reliable, never-renamed sibling site ("the version IS
            the path", doc §7): a static path baked in at import time goes
            permanently stale the moment a real bump renames the file.
        path_group: Which ``value_group`` to resolve a ``dynamic_path``
            site's anchor from. Defaults to this site's own ``value_group``
            when ``None`` -- correct for the rename/id-content sites, whose
            own value IS the path-determining contract revision. A digest
            site embedded in the same file (its own ``value_group`` is a
            *digest* family, e.g. ``jarvis_contract_sha256``) still needs
            the *id* group's anchor to find the file, so it sets this
            explicitly rather than resolving against its own digest value.
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
    dynamic_path: bool = False
    path_group: str | None = None


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
#: `clio-kit-jarvis-user-vX.Y` or the patch-level `vX.Y.Z` (owner doctrine:
#: contract versioning is patch-level -- v3.7.1 for a small additive change),
#: bare `jarvis-user-vX.Y[.Z].json`, and the RST-backtick-quoted docstring
#: form -- all share this one capture. The patch segment is OPTIONAL so
#: every existing vX.Y revision (v3.1-v3.7) still matches unchanged.
_CONTRACT = re.compile(r"(?:clio-kit-)?jarvis-user-(v[0-9]+\.[0-9]+(?:\.[0-9]+)?)")
#: A bare `vX.Y[.Z]` token with no surrounding contract-name context (doc
#: §7's two frozen "v3.6" sites: a description sentence and a stable
#: check-id). The optional patch segment mirrors ``_CONTRACT`` above --
#: without it, a bare `v3.7.1` token is truncated to `v3.7` on read (the
#: trailing `\b` is satisfied at the `7`/`.` boundary regardless of what
#: follows).
_BARE_V = re.compile(r"\b(v[0-9]+\.[0-9]+(?:\.[0-9]+)?)\b")
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
#: A ``{value}`` template, not a literal path -- see ``PinSite.dynamic_path``.
_CONTRACT_JSON = "src/clio_relay/_contracts/jarvis-user-{value}.json"

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
        # iowarp/clio-relay#231 split/cluster-config: RemoteMcpContract moved
        # from cluster_config.py (a thin facade now) to its real owner module,
        # cluster_config_models.py -- this site follows the definition, not
        # the facade's re-export line.
        "jc.cluster_config",
        "src/clio_relay/cluster_config_models.py",
        _JC,
        _LN,
        "RemoteMcpContract Literal member",
        line=139,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.installation",
        "src/clio_relay/native_jarvis_contract.py",
        _JC,
        _LN,
        "CLIO_KIT_JARVIS_CONTRACT_ID",
        # clio-relay#242: mechanical relocation only -- new contract_gate.py
        # imports pushed this unchanged constant from line 54 to line 60.
        # The requirement itself (v3.7) is untouched.
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new require_surface_contract import pushed this
        # unchanged constant from line 60 to line 61.
        # clio-relay#231 installation split: the constant's sole canonical
        # definition moved from installation.py:61 to the owner module
        # native_jarvis_contract.py (installation.py now only re-imports the
        # name, which carries no assignable value for this pin to read).
        line=37,
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
        "src/clio_relay/models_shared.py",
        _JC,
        _LN,
        "REGISTERED_JARVIS_USER_CONTRACT",
        # #231 decomposition: models.py's own definition was extracted into
        # models_shared.py (mechanical relocation, unchanged value) -- the
        # old models.py:43 site no longer exists there at all.
        line=46,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.models_package_route",
        "src/clio_relay/models_jarvis_package.py",
        _JC,
        _LN,
        "JarvisPackageInputRoute.contract Literal + default",
        # #231 decomposition: JarvisPackageInputRoute moved out of models.py
        # into its own module (mechanical relocation, unchanged value).
        line=34,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.models_pipeline_route",
        "src/clio_relay/models_jarvis_pipeline.py",
        _JC,
        _LN,
        "JarvisPipelineInputRoute.contract Literal + default",
        # #231 decomposition: JarvisPipelineInputRoute moved out of models.py
        # into its own module (mechanical relocation, unchanged value).
        line=42,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.remote_mcp_artifact_filename_ref",
        "src/clio_relay/remote_mcp.py",
        _JC,
        _FN,
        "CLIO_KIT_JARVIS_USER_CONTRACT_ARTIFACT_BY_ID current-contract entry",
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new `import logging` + module `logger` pushed this
        # unchanged entry from line 129 to line 132.
        # #231 decomposition: further growth above it (new constants/imports)
        # pushed this unchanged entry from line 132 to line 249.
        line=249,
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
        dynamic_path=True,
    ),
    _row(
        "jc.contract_file_content",
        _CONTRACT_JSON,
        _JC,
        _LN,
        "vendored contract file's own contract_id field",
        line=3,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
        dynamic_path=True,
    ),
    _row(
        "jc.contract_file_content_sha256",
        _CONTRACT_JSON,
        _JC,
        _LN,
        "vendored contract file's own import-validated content SHA-256",
        pattern=re.compile(r'"contract_sha256": "([0-9a-f]{64})"'),
        value_group="jarvis_contract_sha256",
        dynamic_path=True,
        path_group="jarvis_contract_id",
    ),
    _row(
        "jc.contract_file_wire_sha256",
        _CONTRACT_JSON,
        _JC,
        _LN,
        "vendored contract file's own import-validated wire SHA-256",
        pattern=re.compile(r'"wire_sha256": "([0-9a-f]{64})"'),
        value_group="jarvis_contract_wire_sha256",
        dynamic_path=True,
        path_group="jarvis_contract_id",
    ),
    _row(
        "jc.runner_registered_contract",
        "jarvis-packages/clio_relay/clio_relay/constants.py",
        _JC,
        _LN,
        "current member of the supported JARVIS execution query contracts",
        # #231 decomposition: _QUERY_CONTRACTS was extracted out of
        # mcp_call/runner.py (mechanical relocation, unchanged value) into
        # the vendored package's own constants.py.
        line=36,
        pattern=re.compile(
            r'^_QUERY_CONTRACTS = \("clio-kit-jarvis-user-v3\.6", '
            r'"clio-kit-jarvis-user-(v[0-9]+\.[0-9]+(?:\.[0-9]+)?)"\)$'
        ),
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.constants_registered_contract",
        "src/clio_relay/constants.py",
        _JC,
        _LN,
        "current member of the supported JARVIS execution query contracts "
        "(main-package sibling of the vendored jarvis-packages mirror)",
        # #231 decomposition: the mcp_call/runner.py monolith split into 20
        # owner modules that now live in BOTH src/clio_relay (the main
        # package) AND the vendored jarvis-packages/clio_relay mirror --
        # what was one _QUERY_CONTRACTS occurrence became two. This site
        # newly registers the main-package copy (never registered before;
        # jc.runner_registered_contract already covers the mirror copy).
        line=36,
        pattern=re.compile(
            r'^_QUERY_CONTRACTS = \("clio-kit-jarvis-user-v3\.6", '
            r'"clio-kit-jarvis-user-(v[0-9]+\.[0-9]+(?:\.[0-9]+)?)"\)$'
        ),
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.execution_watch_docstring",
        "src/clio_relay/execution_watch.py",
        _JC,
        _LN,
        "module-level comment cross-reference (cancel-refusal rationale)",
        # #231 decomposition: execution_watch.py is a new module born from
        # the mcp_call/runner.py split; its own comment naming the current
        # contract was never registered.
        line=102,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.relay_schema_equality_contract_const",
        "tests/fixtures/relay_schema_equality_v1.json",
        _JC,
        _LN,
        "RelayJob package route contract const in the schema-equality golden",
        line=354,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.relay_schema_equality_contract_default",
        "tests/fixtures/relay_schema_equality_v1.json",
        _JC,
        _LN,
        "RelayJob package route contract default in the schema-equality golden",
        line=355,
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
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new `import logging` pushed this unchanged tuple from
        # line 135 to line 136.
        # #231 decomposition: further test additions above it pushed this
        # unchanged tuple from line 136 to line 144.
        line=144,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_remote_mcp_1391",
        "tests/test_remote_mcp.py",
        _JC,
        _LN,
        "vendored contract file path reference",
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new `import logging` pushed this unchanged reference
        # from line 1406 to line 1407.
        # #231 decomposition: further test additions above it pushed this
        # unchanged reference from line 1407 to line 1421.
        line=1421,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_remote_mcp_1410",
        "tests/test_remote_mcp.py",
        _JC,
        _LN,
        "contract= keyword argument",
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new `import logging` pushed this unchanged argument
        # from line 1425 to line 1426.
        # #231 decomposition: further test additions above it pushed this
        # unchanged argument from line 1426 to line 1439.
        line=1439,
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
        "jc.test_endpoint_registered_contract_3139",
        "tests/test_endpoint.py",
        _JC,
        _LN,
        "registered-contract parametrized job spec (#231 A6; -1 from 3140, N15)",
        # #231 decomposition wave (post-A6): further test additions above it
        # pushed this unchanged entry from line 3139 to line 3172.
        line=3172,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.test_endpoint_registered_contract_5962",
        "tests/test_endpoint.py",
        _JC,
        _LN,
        "registered-contract parametrized job spec (#231 A6; -1 from 5963, N15)",
        # #231 decomposition wave (post-A6): further test additions above it
        # pushed this unchanged entry from line 5962 to line 6006.
        line=6006,
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
        "jc.remote_mcp_federation_wheel_carries_contract",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "'the released clio-kit artifact carries the pinned ... contract' prose",
        line=437,
        pattern=_BARE_V,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.remote_mcp_federation_staging_registered_route",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "staging-plane prose: 'A registered route reaches it through its ... registration'",
        line=290,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.remote_mcp_federation_staging_gate_condition",
        "docs/remote-mcp-federation.md",
        _JC,
        _LN,
        "staging-plane prose: 'a registration declares exactly contract: ...'",
        line=301,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.interface_context_staging_route",
        "docs/ai/interface-context.md",
        _JC,
        _LN,
        "'Exact ... routes additionally support package-described local-file staging'",
        line=286,
        pattern=_CONTRACT,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.system_context_staging_route",
        "docs/ai/system-context.md",
        _JC,
        _LN,
        "'Transparent local-file staging is enabled only for an immutable registered ... route'",
        line=111,
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
    # -- jarvis_contract: description prose moves with the contract; the --
    # -- check-id stays a frozen historical label (B5: a description is --
    # -- prose, not an identifier -- it must say what the requirement --
    # -- ACTUALLY exercises, so it moves like every other id-literal site; --
    # -- the check-id is a stable name other tooling/evidence references --
    # -- by string, so it is never rewritten). --
    _row(
        "jc.release_gate_secure_runtime_description",
        "docs/release-gate-1.0.yaml",
        _JC,
        _LN,
        "ares-secure-jarvis-runtime requirement description",
        line=1109,
        pattern=_BARE_V,
        value_group="jarvis_contract_id",
    ),
    _row(
        "jc.release_gate_secure_runtime_check_id",
        "docs/release-gate-1.0.yaml",
        _JC,
        _LN,
        "secure-runtime.jarvis-v3.6-query check id (stable label, tracked not rewritten)",
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
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new `import logging` + module `logger` pushed this
        # unchanged digest from line 106 to line 109.
        # #231 decomposition: further growth above it (new constants/imports)
        # pushed this unchanged digest from line 109 to line 226.
        line=226,
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
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new `import logging` + module `logger` pushed this
        # unchanged digest from line 114 to line 117.
        # #231 decomposition: further growth above it (new constants/imports)
        # pushed this unchanged digest from line 117 to line 234.
        line=234,
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
        # clio-relay#242 dev-mode course correction: mechanical relocation
        # only -- the new `import logging` + module `logger` pushed this
        # unchanged digest from line 122 to line 125.
        # #231 decomposition: further growth above it (new constants/imports)
        # pushed this unchanged digest from line 125 to line 242.
        line=242,
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
    *kit_pin_sites(
        _row,
        family=_KV_FAM,
        line=_LN,
        filename=_FN,
        placeholder=_PH,
        regex=_RX,
    ),
)
