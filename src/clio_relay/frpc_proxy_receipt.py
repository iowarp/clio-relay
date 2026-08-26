"""Typed framed receipts and status for the cluster-side frpc proxy (clio-relay#279).

Two distinct wire shapes, matching two existing precedents in this codebase
rather than forcing one shape onto both:

- **Mutating operations** (install/teardown, ``frpc_proxy_scripts.py``'s
  install/teardown scripts) emit ``FrpcProxy<Field>=<value>`` lines, one
  fact per line -- the same "each line is one typed fact" framing
  ``bootstrap_one_pass_script.py`` uses for its own persistent receipt
  (there: one JSON blob behind a single marker; here: several small,
  independently-typed facts, none of which is secret, so there is no
  motivation to bundle them behind a single JSON marker line). This module
  parses those lines into :class:`FrpcProxyBringupReceipt` /
  :class:`FrpcProxyTeardownReceipt`.
- **Read-only status** (``frpc_proxy_scripts.py``'s status script) emits
  plain ``systemctl --user show --property=`` output exactly like
  ``endpoint_service_status.py``'s own readiness probe, plus one
  base64-encoded journal tail line. This module classifies that into the
  typed, diagnosable :class:`FrpcProxyStatusDocument` -- the classification
  itself is pure Python, not shell, so it is unit-testable without a real
  systemd.
"""

from __future__ import annotations

import base64
import binascii
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict

from clio_relay.errors import RelayError

BRINGUP_RECEIPT_SCHEMA: Final = "clio-relay.frpc-proxy-bringup-receipt.v1"
TEARDOWN_RECEIPT_SCHEMA: Final = "clio-relay.frpc-proxy-teardown-receipt.v1"

_ENABLED_UNIT_FILE_STATES: Final = frozenset(
    {"enabled", "enabled-runtime", "linked", "linked-runtime"}
)
_MAX_JOURNAL_TAIL_LINES: Final = 40


class FrpcProxyBringupReceipt(BaseModel):
    """The typed evidence one successful ``relay-host install-proxy`` pass produced."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.frpc-proxy-bringup-receipt.v1"] = BRINGUP_RECEIPT_SCHEMA
    cluster: str
    proxy_name: str
    unit_name: str
    toml_path: str
    env_path: str
    config_sha256: str
    enabled: bool
    active: bool
    installed_at: str


class FrpcProxyTeardownReceipt(BaseModel):
    """The typed evidence one ``relay-host teardown-proxy`` pass produced."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.frpc-proxy-teardown-receipt.v1"] = TEARDOWN_RECEIPT_SCHEMA
    cluster: str
    unit_name: str
    removed_unit: bool
    removed_toml: bool
    removed_env: bool
    torn_down_at: str


class FrpcProxyStatusDocument(BaseModel):
    """The typed, diagnosable status ``relay-host proxy-status`` reports."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.frpc-proxy-status.v1"] = "clio-relay.frpc-proxy-status.v1"
    cluster: str
    unit_name: str
    installed: bool
    enabled: bool
    active: bool
    load_state: str
    active_state: str
    sub_state: str
    journal_tail: list[str]
    diagnosis: str


def _parse_prefixed_key_value_lines(
    lines: list[str], *, prefix: str, required: set[str]
) -> dict[str, str]:
    """Parse ``<prefix><Field>=<value>`` lines into a dict, requiring an exact key set.

    Ignores any line that does not start with ``prefix`` -- ordinary
    stdout/diagnostic noise from the remote script is not framing. A
    duplicate key, or a required key that never appeared, is a typed
    refusal: malformed or missing framing must never be read as a
    partial-success guess.
    """
    properties: dict[str, str] = {}
    for raw_line in lines:
        if not raw_line.startswith(prefix):
            continue
        key, separator, value = raw_line.partition("=")
        if not separator or key in properties:
            raise RelayError(f"frpc proxy receipt line is malformed or duplicated: {raw_line!r}")
        properties[key] = value
    if properties.keys() != required:
        missing = sorted(required - properties.keys())
        raise RelayError(f"frpc proxy receipt output is incomplete; missing: {missing}")
    return properties


def _parse_bool(value: str, *, field: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise RelayError(f"frpc proxy receipt field {field!r} was not a boolean: {value!r}")


_BRINGUP_KEYS: Final = {
    "FrpcProxyReceiptSchema",
    "FrpcProxyCluster",
    "FrpcProxyName",
    "FrpcProxyUnitName",
    "FrpcProxyTomlPath",
    "FrpcProxyEnvPath",
    "FrpcProxyConfigSha256",
    "FrpcProxyEnabled",
    "FrpcProxyActive",
    "FrpcProxyInstalledAt",
}


def parse_frpc_proxy_bringup_receipt(lines: list[str]) -> FrpcProxyBringupReceipt:
    """Parse a bring-up script's stdout lines into the typed receipt contract."""
    properties = _parse_prefixed_key_value_lines(lines, prefix="FrpcProxy", required=_BRINGUP_KEYS)
    if properties["FrpcProxyReceiptSchema"] != BRINGUP_RECEIPT_SCHEMA:
        raise RelayError("frpc proxy bring-up receipt schema did not match")
    return FrpcProxyBringupReceipt(
        cluster=properties["FrpcProxyCluster"],
        proxy_name=properties["FrpcProxyName"],
        unit_name=properties["FrpcProxyUnitName"],
        toml_path=properties["FrpcProxyTomlPath"],
        env_path=properties["FrpcProxyEnvPath"],
        config_sha256=properties["FrpcProxyConfigSha256"],
        enabled=_parse_bool(properties["FrpcProxyEnabled"], field="FrpcProxyEnabled"),
        active=_parse_bool(properties["FrpcProxyActive"], field="FrpcProxyActive"),
        installed_at=properties["FrpcProxyInstalledAt"],
    )


_TEARDOWN_KEYS: Final = {
    "FrpcProxyTeardownSchema",
    "FrpcProxyCluster",
    "FrpcProxyUnitName",
    "FrpcProxyRemovedUnit",
    "FrpcProxyRemovedToml",
    "FrpcProxyRemovedEnv",
    "FrpcProxyTornDownAt",
}


def parse_frpc_proxy_teardown_receipt(lines: list[str]) -> FrpcProxyTeardownReceipt:
    """Parse a teardown script's stdout lines into the typed receipt contract."""
    properties = _parse_prefixed_key_value_lines(lines, prefix="FrpcProxy", required=_TEARDOWN_KEYS)
    if properties["FrpcProxyTeardownSchema"] != TEARDOWN_RECEIPT_SCHEMA:
        raise RelayError("frpc proxy teardown receipt schema did not match")
    return FrpcProxyTeardownReceipt(
        cluster=properties["FrpcProxyCluster"],
        unit_name=properties["FrpcProxyUnitName"],
        removed_unit=_parse_bool(properties["FrpcProxyRemovedUnit"], field="FrpcProxyRemovedUnit"),
        removed_toml=_parse_bool(properties["FrpcProxyRemovedToml"], field="FrpcProxyRemovedToml"),
        removed_env=_parse_bool(properties["FrpcProxyRemovedEnv"], field="FrpcProxyRemovedEnv"),
        torn_down_at=properties["FrpcProxyTornDownAt"],
    )


_STATUS_PROPERTY_KEYS: Final = {
    "LoadState",
    "ActiveState",
    "SubState",
    "UnitFileState",
    "JournalTailBase64",
}


def parse_frpc_proxy_status_properties(output: str) -> dict[str, str]:
    """Parse the status script's plain ``systemctl --user show`` style output."""
    properties: dict[str, str] = {}
    for raw_line in output.splitlines():
        key, separator, value = raw_line.partition("=")
        if not separator or not key or key in properties:
            raise RelayError("frpc proxy status output is invalid")
        properties[key] = value
    if not _STATUS_PROPERTY_KEYS.issubset(properties.keys()):
        missing = sorted(_STATUS_PROPERTY_KEYS - properties.keys())
        raise RelayError(f"frpc proxy status output is incomplete; missing: {missing}")
    return properties


def build_frpc_proxy_status_document(
    *,
    cluster: str,
    unit_name: str,
    properties: dict[str, str],
) -> FrpcProxyStatusDocument:
    """Classify raw systemd properties into the typed, diagnosable status document."""
    load_state = properties["LoadState"] or "unknown"
    active_state = properties["ActiveState"] or "unknown"
    sub_state = properties["SubState"] or "unknown"
    unit_file_state = properties["UnitFileState"] or "unknown"
    installed = load_state == "loaded"
    enabled = unit_file_state in _ENABLED_UNIT_FILE_STATES
    active = active_state == "active"
    journal_tail = _decode_journal_tail(properties["JournalTailBase64"])
    diagnosis = _diagnose_frpc_proxy(
        installed=installed,
        enabled=enabled,
        active=active,
        active_state=active_state,
        sub_state=sub_state,
        unit_name=unit_name,
    )
    return FrpcProxyStatusDocument(
        cluster=cluster,
        unit_name=unit_name,
        installed=installed,
        enabled=enabled,
        active=active,
        load_state=load_state,
        active_state=active_state,
        sub_state=sub_state,
        journal_tail=journal_tail,
        diagnosis=diagnosis,
    )


def _decode_journal_tail(encoded: str) -> list[str]:
    if not encoded:
        return []
    try:
        raw = base64.b64decode(encoded, validate=True)
    except binascii.Error as exc:
        raise RelayError("frpc proxy status journal tail was not valid base64") from exc
    return raw.decode("utf-8", errors="replace").splitlines()[-_MAX_JOURNAL_TAIL_LINES:]


def _diagnose_frpc_proxy(
    *,
    installed: bool,
    enabled: bool,
    active: bool,
    active_state: str,
    sub_state: str,
    unit_name: str,
) -> str:
    """Return the one-line, operator-facing reason for the observed state.

    Deliberately typed discrimination on the three booleans, not a message
    match -- frpc down / frps unreachable / token rejected all surface here
    as "installed and enabled but inactive", with the journal tail (fetched
    in the SAME ssh pass) carrying the actual frpc-reported cause.
    """
    if not installed:
        return "frpc proxy unit is not installed; run `clio-relay relay-host install-proxy`"
    if not enabled:
        return "frpc proxy unit is installed but not enabled"
    if active:
        return "frpc proxy unit is active"
    return (
        f"frpc proxy unit is inactive (state={active_state}/{sub_state}); see "
        f"journalctl --user --unit={unit_name} --lines=50 --no-pager"
    )
