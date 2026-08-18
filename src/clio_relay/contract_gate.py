"""Per-surface MCP contract capability gating (iowarp/clio-relay#242).

Owner doctrine: the bootstrap used to conflate two different concerns under
one name, "the clio-kit contract check":

* **Integrity pinning** -- is the artifact clio-relay is about to trust
  BYTE-FOR-BYTE the one this relay has on file (wheel sha256, and for each
  contract id it claims to speak, the exact digest of its declared tool
  surface)? This must stay EXACT. It never weakens.
* **Capability gating** -- does the artifact's shipped contract for one
  surface (e.g. JARVIS) happen to be at or above the relay's CURRENT pinned
  requirement for that surface? This is a per-surface, negotiable fact, not
  a cluster-wide precondition -- one surface (say, spack) can be fully ready
  while another (jarvis) is one release behind, with no relationship between
  the two.

Before this module, ``clio_relay.installation.probe_clio_kit_native_execution_contract``
did both at once, unconditionally, at BOOTSTRAP time: it asked clio-kit for
EXACTLY the relay's current pinned jarvis contract id and raised the moment
that ask came back "unknown MCP user contract: <id>" -- which killed the
ENTIRE cluster bootstrap even when every surface the operator actually
needed (e.g. spack) was fully present and correct.

This module is the INTEGRITY-only, per-surface identity probe used at
BOOTSTRAP time: :func:`probe_surface_contract_identity` negotiates down a
newest-first list of KNOWN contract ids for one surface, accepting whichever
one the artifact actually answers to as long as its declared tool surface's
SHA-256 matches that id's own registered digest (this module never accepts
an unrecognized or tampered response). It does NOT run the deep per-field
shape validation that only the CURRENT pin's schema is written against --
that stays exclusively in the strict, single-id probe
(``probe_clio_kit_native_execution_contract``) used at USE-time, when a
surface is actually about to be invoked. :func:`require_surface_contract`
is the USE-time gate: it turns a below-pin :class:`SurfaceContractStatus`
into the typed, catchable :class:`clio_relay.errors.ContractSurfaceUnavailableError`
naming the surface, what it has, and what it needs.

See the design/evidence comment on iowarp/clio-relay#242 for the bootstrap
failure this replaces and the release_pin_sites registry entries (untouched
by this change -- they define what the relay REQUIRES; this module only
relocates WHERE that requirement is enforced).
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict

from clio_relay.bounded_process import BoundedProcessError, run_bounded_process
from clio_relay.dev_mode import dev_mode_enabled
from clio_relay.errors import ConfigurationError, contract_surface_unavailable

logger = logging.getLogger(__name__)

SURFACE_CONTRACT_STATUS_SCHEMA = "clio-relay.surface-contract-status.v1"
SURFACE_CONTRACT_DEGRADATION_SCHEMA = "clio-relay.surface-contract-degradation.v1"
SURFACE_CONTRACT_DEGRADATION_REASON = "contract_surface_below_pin"
#: clio-relay#242 course correction: the tracking issue every USE-time
#: deferral cites -- the same one bootstrap-time degradations already cite
#: via their own explicit ``tracking_issue`` argument.
CONTRACT_GATE_TRACKING_ISSUE = "iowarp/clio-relay#242"

#: clio-kit's own wording (external to this repo) when its ``mcp-contract``
#: subcommand is asked to describe an id it does not recognize at all. Only
#: this exact signal moves the negotiation to the next, older candidate --
#: every other failure (timeout, crash, malformed JSON, a digest mismatch)
#: propagates immediately as a genuine integrity failure.
_UNKNOWN_CONTRACT_MARKER = "unknown mcp user contract"


class SurfaceContractStatus(BaseModel):
    """What one MCP surface's shipped contract identity actually is.

    Recorded on the install receipt regardless of whether ``meets_requirement``
    is True -- the whole point is to make a below-pin surface LOUD (in the
    receipt and the bootstrap report) rather than silently absent or, worse,
    a fatal bootstrap error for surfaces the operator never asked about.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = SURFACE_CONTRACT_STATUS_SCHEMA
    surface: str
    shipped_contract_id: str
    shipped_contract_sha256: str
    required_contract_id: str
    meets_requirement: bool


class SurfaceContractDegradation(BaseModel):
    """Typed, loud record of one surface shipping below the relay's pin.

    Never a warning that scrolls away: this is a structured value that lands
    in the install receipt and the bootstrap report (no-silent-fallback
    doctrine), naming exactly what is missing and where it is tracked.

    ``enforcement`` (clio-relay#242 dev-mode course correction) marks
    whether the USE-time gate this below-pin surface feeds
    (:func:`require_surface_contract`) will actually refuse ("enforced",
    the default -- byte-identical to before this field existed) or has been
    told to defer to dev mode ("deferred_dev_mode"). Bootstrap-time
    recording (:func:`evaluate_degradation`) leaves this at its default
    unless told otherwise; the USE-time gate stamps it explicitly on the
    record it logs when it defers, so a retest can tell "below pin" apart
    from "below pin AND let through" by this field alone.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = SURFACE_CONTRACT_DEGRADATION_SCHEMA
    surface: str
    have: str
    need: str
    reason: str = SURFACE_CONTRACT_DEGRADATION_REASON
    tracking_issue: str
    detected_at: datetime
    enforcement: Literal["enforced", "deferred_dev_mode"] = "enforced"


def run_json_probe(command: list[str], *, label: str) -> dict[str, object]:
    """Run one bounded probe process and require exactly one JSON object.

    Single owner for every contract-identity probe in this domain
    (moved from ``clio_relay.installation``, which imports it back --
    clio-relay#199-style consolidation: one algorithm, one definition).
    """
    try:
        completed = run_bounded_process(
            command,
            timeout_seconds=30,
            stdout_maximum_bytes=4 * 1024 * 1024,
            stderr_maximum_bytes=64 * 1024,
        )
    except (OSError, BoundedProcessError) as exc:
        raise ConfigurationError(f"{label} failed: {type(exc).__name__}: {exc}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no diagnostic"
        raise ConfigurationError(f"{label} failed: {detail[:2000]}")
    try:
        loaded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"{label} returned invalid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ConfigurationError(f"{label} did not return a JSON object")
    return {str(key): value for key, value in cast(dict[object, object], loaded).items()}


def mcp_contract_digest(tools: list[dict[str, object]]) -> str:
    """Recompute clio-kit's documented agent-facing contract projection.

    Single owner of this digest algorithm (moved from
    ``clio_relay.installation``, which imports it back).
    """
    projected = [
        {
            "annotations": tool.get("annotations"),
            "description": tool.get("description"),
            "input_schema": tool.get("inputSchema"),
            "name": tool.get("name"),
            "output_schema": tool.get("outputSchema"),
            "title": tool.get("title"),
        }
        for tool in sorted(tools, key=lambda item: str(item.get("name")))
    ]
    try:
        payload = json.dumps(
            {"tools": projected},
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"clio-kit native execution contract was not JSON: {exc}") from exc
    return hashlib.sha256(payload).hexdigest()


def probe_surface_contract_identity(
    command_prefix: list[str],
    *,
    surface: str,
    candidate_contract_ids: tuple[str, ...],
    contract_schema_version: str,
    sha256_by_id: dict[str, str],
) -> SurfaceContractStatus:
    """Enumerate the highest MCP contract a launcher actually ships for one surface.

    Tries ``candidate_contract_ids`` NEWEST-FIRST via the launcher's own
    ``mcp-contract <id>`` identity subcommand. The first id the launcher
    recognizes is verified for INTEGRITY ONLY: its response echoes the id it
    was asked for under the expected envelope schema, and the SHA-256 of its
    declared tool surface matches that EXACT id's own registered digest in
    ``sha256_by_id``. This is the full declared tool surface hashed as one
    unit, so it is exactly as strong a tamper check as validating every
    field individually -- it just does not additionally assert the
    CURRENT pin's specific field shape, which an older-but-genuine id would
    never satisfy.

    Args:
        command_prefix: The surface's CLI launcher, e.g. ``[clio_kit_executable]``.
        surface: Stable surface name (``"jarvis"``, ``"spack"``, ...) recorded
            on the returned status and used to build the typed refusal.
        candidate_contract_ids: Known ids for this surface, NEWEST FIRST.
            ``candidate_contract_ids[0]`` is treated as the relay's current
            requirement.
        contract_schema_version: The stable wrapper schema every
            ``mcp-contract`` response uses, regardless of which contract id
            it describes.
        sha256_by_id: Registered tool-surface digest for every known id.

    Returns:
        The status of whichever candidate id the launcher actually shipped.

    Raises:
        ConfigurationError: No candidate id was recognized at all (the
            launcher answered "unknown MCP user contract" for every one of
            them), or a recognized id's digest did not match its own
            registered value -- both genuine integrity failures, never
            downgraded to a degradation record.
    """
    if not candidate_contract_ids:
        raise ConfigurationError(f"{surface} surface has no candidate contract ids to probe")
    last_unknown: ConfigurationError | None = None
    for contract_id in candidate_contract_ids:
        probe_command = [*command_prefix, "mcp-contract", contract_id]
        try:
            document = run_json_probe(probe_command, label=f"{surface} surface contract")
        except ConfigurationError as exc:
            if _UNKNOWN_CONTRACT_MARKER in str(exc).casefold():
                last_unknown = exc
                continue
            raise
        if (
            document.get("schema_version") != contract_schema_version
            or document.get("contract_id") != contract_id
        ):
            raise ConfigurationError(f"{surface} surface contract identity did not match")
        raw_tools = document.get("tools")
        if not isinstance(raw_tools, list) or not all(
            isinstance(item, dict) for item in cast(list[object], raw_tools)
        ):
            raise ConfigurationError(f"{surface} surface contract tools were invalid")
        tools = [cast(dict[str, object], item) for item in cast(list[object], raw_tools)]
        observed_sha256 = mcp_contract_digest(tools)
        expected_sha256 = sha256_by_id.get(contract_id)
        if expected_sha256 is None or observed_sha256 != expected_sha256:
            raise ConfigurationError(
                f"{surface} surface contract {contract_id} digest did not match its "
                "registered identity"
            )
        return SurfaceContractStatus(
            surface=surface,
            shipped_contract_id=contract_id,
            shipped_contract_sha256=observed_sha256,
            required_contract_id=candidate_contract_ids[0],
            meets_requirement=contract_id == candidate_contract_ids[0],
        )
    raise ConfigurationError(
        f"{surface} surface shipped none of the known contracts {list(candidate_contract_ids)}"
    ) from last_unknown


def evaluate_degradation(
    status: SurfaceContractStatus,
    *,
    tracking_issue: str,
    detected_at: datetime | None = None,
) -> SurfaceContractDegradation | None:
    """Return the typed degradation record for a below-pin surface, else ``None``."""
    if status.meets_requirement:
        return None
    return SurfaceContractDegradation(
        surface=status.surface,
        have=status.shipped_contract_id,
        need=status.required_contract_id,
        tracking_issue=tracking_issue,
        detected_at=detected_at or datetime.now(UTC),
    )


def require_surface_contract(
    status: SurfaceContractStatus,
    *,
    dev_mode: bool | None = None,
) -> None:
    """Refuse at USE-time when one surface's shipped contract is below pin.

    Owner ruling (clio-relay#242 dev-mode course correction): dev mode means
    LOUD AND NON-BLOCKING. Production (dev mode off, the default) is
    byte-identical to before this gate learned about dev mode: it raises.
    Dev mode logs the SAME typed record this would-have-raised error carries
    (surface/have/need, the ``SurfaceContractDegradation`` shape) at WARNING
    with ``enforcement="deferred_dev_mode"`` stamped on it, then returns
    normally -- the surface serves, the tool executes. Never silent: the
    runtime log is the queryable trail a security-phase retest greps for
    every deferred enforcement.

    Args:
        status: The surface's bootstrap-recorded contract identity.
        dev_mode: Explicit override. Defaults to
            :func:`clio_relay.dev_mode.dev_mode_enabled` (the
            ``CLIO_RELAY_DEV_MODE`` environment switch, or a cluster's
            ``dev_mode`` registry flag when the caller threads it in) so
            every existing caller honors it for free.

    Raises:
        clio_relay.errors.ContractSurfaceUnavailableError: ``status`` does
            not meet its required contract id, and dev mode is off.
    """
    if status.meets_requirement:
        return
    resolved_dev_mode = dev_mode_enabled() if dev_mode is None else dev_mode
    if resolved_dev_mode:
        deferral = SurfaceContractDegradation(
            surface=status.surface,
            have=status.shipped_contract_id,
            need=status.required_contract_id,
            tracking_issue=CONTRACT_GATE_TRACKING_ISSUE,
            detected_at=datetime.now(UTC),
            enforcement="deferred_dev_mode",
        )
        logger.warning(
            "clio-relay: DEV MODE -- contract surface gate deferred (would refuse in "
            "enforcing mode): %s",
            deferral.model_dump(mode="json"),
        )
        return
    raise contract_surface_unavailable(
        surface=status.surface,
        have=status.shipped_contract_id,
        need=status.required_contract_id,
    )
