"""Receipt/status parsing tests for the cluster-side frpc proxy (clio-relay#279).

Mirrors ``bootstrap_one_pass_script.py``'s own receipt-parsing test style:
well-formed evidence parses; malformed, missing, or duplicated framing is a
typed ``RelayError``, never a partial-success guess. Nothing here spawns a
process or touches the filesystem.
"""

from __future__ import annotations

import base64

import pytest

from clio_relay.errors import RelayError
from clio_relay.frpc_proxy_receipt import (
    BRINGUP_RECEIPT_SCHEMA,
    TEARDOWN_RECEIPT_SCHEMA,
    build_frpc_proxy_status_document,
    parse_frpc_proxy_bringup_receipt,
    parse_frpc_proxy_status_properties,
    parse_frpc_proxy_teardown_receipt,
)

_BRINGUP_LINES = [
    f"FrpcProxyReceiptSchema={BRINGUP_RECEIPT_SCHEMA}",
    "FrpcProxyCluster=ares",
    "FrpcProxyName=ares-owned-session",
    "FrpcProxyUnitName=clio-relay-frpc-proxy-ares.service",
    "FrpcProxyTomlPath=%h/.config/clio-relay/frpc-proxy-ares.toml",
    "FrpcProxyEnvPath=%h/.config/clio-relay/frpc-proxy-ares.env",
    "FrpcProxyConfigSha256=" + "a" * 64,
    "FrpcProxyEnabled=true",
    "FrpcProxyActive=true",
    "FrpcProxyLinger=yes",
    "FrpcProxyPersistence=systemd-user-linger",
    "FrpcProxyInstalledAt=2026-08-26T00:00:00Z",
]

_TEARDOWN_LINES = [
    f"FrpcProxyTeardownSchema={TEARDOWN_RECEIPT_SCHEMA}",
    "FrpcProxyCluster=ares",
    "FrpcProxyUnitName=clio-relay-frpc-proxy-ares.service",
    "FrpcProxyRemovedUnit=true",
    "FrpcProxyRemovedToml=true",
    "FrpcProxyRemovedEnv=false",
    "FrpcProxyTornDownAt=2026-08-26T00:05:00Z",
]


# --- bring-up receipt -------------------------------------------------------


def test_bringup_receipt_parses_well_formed_evidence() -> None:
    receipt = parse_frpc_proxy_bringup_receipt(["noise before", *_BRINGUP_LINES, "noise after"])

    assert receipt.cluster == "ares"
    assert receipt.proxy_name == "ares-owned-session"
    assert receipt.unit_name == "clio-relay-frpc-proxy-ares.service"
    assert receipt.enabled is True
    assert receipt.active is True
    assert receipt.config_sha256 == "a" * 64
    assert receipt.linger is True
    assert receipt.persistence == "systemd-user-linger"


@pytest.mark.parametrize(("raw", "expected"), [("yes", True), ("no", False), ("unknown", None)])
def test_bringup_receipt_parses_every_linger_value(raw: str, expected: bool | None) -> None:
    lines = [
        line if not line.startswith("FrpcProxyLinger=") else f"FrpcProxyLinger={raw}"
        for line in _BRINGUP_LINES
    ]

    receipt = parse_frpc_proxy_bringup_receipt(lines)

    assert receipt.linger is expected


def test_bringup_receipt_rejects_an_invalid_linger_value() -> None:
    lines = [
        line if not line.startswith("FrpcProxyLinger=") else "FrpcProxyLinger=maybe"
        for line in _BRINGUP_LINES
    ]

    with pytest.raises(RelayError, match="FrpcProxyLinger"):
        parse_frpc_proxy_bringup_receipt(lines)


def test_bringup_receipt_rejects_an_invalid_persistence_value() -> None:
    lines = [
        line
        if not line.startswith("FrpcProxyPersistence=")
        else "FrpcProxyPersistence=somewhere-else"
        for line in _BRINGUP_LINES
    ]

    with pytest.raises(RelayError, match="FrpcProxyPersistence"):
        parse_frpc_proxy_bringup_receipt(lines)


def test_bringup_receipt_rejects_a_missing_field() -> None:
    lines = [line for line in _BRINGUP_LINES if not line.startswith("FrpcProxyActive=")]

    with pytest.raises(RelayError, match="incomplete") as excinfo:
        parse_frpc_proxy_bringup_receipt(lines)
    assert "FrpcProxyActive" in str(excinfo.value)


def test_bringup_receipt_rejects_an_unexpected_extra_field_and_names_it() -> None:
    """The missing/extra fix: an extra key must be named, not silently reported

    as an empty missing-list (the original bug -- `properties.keys() !=
    required` can fail purely because of an EXTRA key, and the old message
    only ever computed `required - observed`, which is empty in that case).
    """
    lines = [*_BRINGUP_LINES, "FrpcProxyUnexpectedField=surprise"]

    with pytest.raises(RelayError, match="unexpected") as excinfo:
        parse_frpc_proxy_bringup_receipt(lines)
    assert "FrpcProxyUnexpectedField" in str(excinfo.value)


def test_bringup_receipt_rejects_no_framing_at_all() -> None:
    with pytest.raises(RelayError, match="incomplete"):
        parse_frpc_proxy_bringup_receipt(["nothing framed here"])


def test_bringup_receipt_rejects_a_duplicated_field() -> None:
    lines = [*_BRINGUP_LINES, "FrpcProxyCluster=a-second-cluster"]

    with pytest.raises(RelayError, match="malformed or duplicated"):
        parse_frpc_proxy_bringup_receipt(lines)


def test_bringup_receipt_rejects_a_non_boolean_flag() -> None:
    lines = [line for line in _BRINGUP_LINES if not line.startswith("FrpcProxyEnabled=")]
    lines.append("FrpcProxyEnabled=maybe")

    with pytest.raises(RelayError, match="was not a boolean"):
        parse_frpc_proxy_bringup_receipt(lines)


def test_bringup_receipt_rejects_a_mismatched_schema() -> None:
    lines = [
        "FrpcProxyReceiptSchema=clio-relay.frpc-proxy-bringup-receipt.v0-stale",
        *_BRINGUP_LINES[1:],
    ]

    with pytest.raises(RelayError, match="schema did not match"):
        parse_frpc_proxy_bringup_receipt(lines)


# --- teardown receipt -------------------------------------------------------


def test_teardown_receipt_parses_well_formed_evidence() -> None:
    receipt = parse_frpc_proxy_teardown_receipt(_TEARDOWN_LINES)

    assert receipt.cluster == "ares"
    assert receipt.removed_unit is True
    assert receipt.removed_env is False


def test_teardown_receipt_rejects_missing_framing() -> None:
    with pytest.raises(RelayError, match="incomplete"):
        parse_frpc_proxy_teardown_receipt(["stray output line"])


def test_teardown_receipt_rejects_a_mismatched_schema() -> None:
    lines = ["FrpcProxyTeardownSchema=stale-schema", *_TEARDOWN_LINES[1:]]

    with pytest.raises(RelayError, match="schema did not match"):
        parse_frpc_proxy_teardown_receipt(lines)


# --- status classification --------------------------------------------------


def _status_properties(
    *,
    load_state: str = "loaded",
    active_state: str = "active",
    sub_state: str = "running",
    unit_file_state: str = "enabled",
    journal_lines: tuple[str, ...] = ("frpc: login to server success",),
) -> dict[str, str]:
    encoded = base64.b64encode("\n".join(journal_lines).encode("utf-8")).decode("ascii")
    return {
        "LoadState": load_state,
        "ActiveState": active_state,
        "SubState": sub_state,
        "UnitFileState": unit_file_state,
        "JournalTailBase64": encoded,
    }


def test_parse_status_properties_requires_every_key() -> None:
    output = "LoadState=loaded\nActiveState=active\n"

    with pytest.raises(RelayError, match="incomplete"):
        parse_frpc_proxy_status_properties(output)


def test_parse_status_properties_rejects_a_duplicate_key() -> None:
    output = "LoadState=loaded\nLoadState=loaded\n"

    with pytest.raises(RelayError, match="invalid"):
        parse_frpc_proxy_status_properties(output)


def test_status_document_classifies_a_healthy_proxy() -> None:
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(),
    )

    assert document.installed is True
    assert document.enabled is True
    assert document.active is True
    assert document.diagnosis == "frpc proxy unit is active"
    assert document.journal_tail == ["frpc: login to server success"]


def test_status_document_classifies_a_not_installed_proxy() -> None:
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(
            load_state="not-found", active_state="inactive", sub_state="dead"
        ),
    )

    assert document.installed is False
    assert "not installed" in document.diagnosis
    assert "install-proxy" in document.diagnosis


def test_status_document_classifies_a_masked_proxy_distinctly_not_as_not_installed() -> None:
    """Minor fix: masked must NOT say "not installed; run install-proxy".

    That advice is actively wrong for a masked unit -- systemd refuses to
    start a masked unit no matter how many times the unit file is
    rewritten; the real fix is `systemctl --user unmask`.
    """
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(
            load_state="masked", active_state="inactive", sub_state="dead"
        ),
    )

    assert document.installed is False
    assert document.load_state_category == "masked"
    assert "not installed" not in document.diagnosis
    assert "run `clio-relay relay-host install-proxy`" not in document.diagnosis
    assert "unmask" in document.diagnosis


def test_status_document_classifies_a_malformed_unit_distinctly_not_as_not_installed() -> None:
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(
            load_state="error", active_state="inactive", sub_state="dead"
        ),
    )

    assert document.installed is False
    assert document.load_state_category == "error"
    assert "not installed; run" not in document.diagnosis
    assert "malformed" in document.diagnosis
    assert "install-proxy" in document.diagnosis  # reinstalling IS the fix here, unlike masked


def test_status_document_classifies_a_genuinely_not_found_unit() -> None:
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(
            load_state="not-found", active_state="inactive", sub_state="dead"
        ),
    )

    assert document.load_state_category == "not_found"
    assert document.installed is False
    assert "not installed" in document.diagnosis


def test_status_document_flags_restart_looping_as_a_typed_field() -> None:
    """Minor fix: restart-looping is a typed field, not only prose."""
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(active_state="activating", sub_state="auto-restart"),
    )

    assert document.restart_looping is True
    assert "restart-looping" in document.diagnosis
    assert document.active is False


def test_status_document_does_not_flag_restart_looping_for_a_healthy_proxy() -> None:
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(),
    )

    assert document.restart_looping is False


def test_status_document_classifies_an_installed_but_disabled_proxy() -> None:
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(unit_file_state="disabled", active_state="inactive"),
    )

    assert document.installed is True
    assert document.enabled is False
    assert "not enabled" in document.diagnosis


def test_status_document_classifies_frpc_down_as_inactive_with_journal_hint() -> None:
    """frpc down / frps unreachable / token rejected are diagnosable, not silent.

    All three surface identically at the systemd-property layer (installed,
    enabled, inactive) -- the journal tail (fetched in the same ssh pass) is
    what carries the actual cause; this test proves the status document
    always points the operator at it rather than guessing a reason.
    """
    document = build_frpc_proxy_status_document(
        cluster="ares",
        unit_name="clio-relay-frpc-proxy-ares.service",
        properties=_status_properties(
            active_state="failed",
            sub_state="failed",
            journal_lines=("frpc: login to server failed: EOF", "frpc: connect to server error"),
        ),
    )

    assert document.active is False
    assert "inactive" in document.diagnosis
    assert "journalctl" in document.diagnosis
    assert document.journal_tail == [
        "frpc: login to server failed: EOF",
        "frpc: connect to server error",
    ]


def test_status_document_handles_an_empty_journal_tail() -> None:
    properties = _status_properties()
    properties["JournalTailBase64"] = ""

    document = build_frpc_proxy_status_document(
        cluster="ares", unit_name="clio-relay-frpc-proxy-ares.service", properties=properties
    )

    assert document.journal_tail == []


def test_status_document_rejects_invalid_base64_journal_tail() -> None:
    properties = _status_properties()
    properties["JournalTailBase64"] = "not-valid-base64!!"

    with pytest.raises(RelayError, match="base64"):
        build_frpc_proxy_status_document(
            cluster="ares", unit_name="clio-relay-frpc-proxy-ares.service", properties=properties
        )
