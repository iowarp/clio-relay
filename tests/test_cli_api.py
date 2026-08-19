"""Tests for the ``api`` command group (iowarp/clio-relay#231).

This moved out of ``tests/test_cli.py`` alongside the ``api_app`` commands'
extraction into ``src/clio_relay/cli_api.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.

**Known-trap patch-target fix.** The original test patched
``cli.uvicorn.run`` -- valid only while ``cli.py`` itself did
``import uvicorn`` (removed in this same slice, since ``api_start`` was the
only caller). It now patches ``cli_api.uvicorn.run``, the module that
actually calls it post-extraction. Every other patch target
(``installation_module``, ``session_lifecycle``) already targeted the real
owner module directly and is unchanged.
"""

from __future__ import annotations

import importlib
import os
from typing import Any, cast

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_api as cli_api
import clio_relay.installation as installation_module
import clio_relay.session_lifecycle as session_lifecycle
from clio_relay import cli
from clio_relay.cli import app
from tests.test_cli import (
    _installation_identity,
    _session_api_release_identity,
)


def test_cli_api_start_verifies_process_bound_release_identity(
    monkeypatch: MonkeyPatch,
) -> None:
    installation = _installation_identity()
    identity = _session_api_release_identity()
    launches: list[tuple[str, int]] = []
    events: list[str] = []
    bound_app = object()
    late_app = object()
    http_api_module = importlib.import_module("clio_relay.http_api")
    monkeypatch.setattr(http_api_module, "app", bound_app)
    monkeypatch.setenv("CLIO_RELAY_SESSION_OWNER_TOKEN", "a" * 64)

    def publish() -> None:
        events.append("publish")
        os.environ.pop("CLIO_RELAY_SESSION_OWNER_TOKEN", None)
        cast(Any, http_api_module).app = late_app

    def launch(application: object, *, host: str, port: int) -> None:
        assert application is bound_app
        assert "CLIO_RELAY_SESSION_OWNER_TOKEN" not in os.environ
        events.append("serve")
        launches.append((host, port))

    receipt = cli.InstallReceipt.model_validate(installation["receipt"])
    monkeypatch.setattr(
        installation_module, "verified_session_api_install_receipt", lambda: receipt
    )

    def unexpected_full_installation_probe() -> dict[str, object]:
        raise AssertionError("API startup must not run full component installation probes")

    monkeypatch.setattr(
        installation_module, "installation_info", unexpected_full_installation_probe
    )
    monkeypatch.setattr(session_lifecycle, "publish_owned_session_api_startup_receipt", publish)
    monkeypatch.setattr(cli_api.uvicorn, "run", launch)
    monkeypatch.setenv("CLIO_RELAY_API_RELEASE_IDENTITY_SHA256", identity.sha256())

    accepted = CliRunner().invoke(app, ["api", "start", "--port", "9001"])

    assert accepted.exit_code == 0, accepted.output
    assert launches == [("127.0.0.1", 9001)]
    assert events == ["publish", "serve"]

    monkeypatch.setenv("CLIO_RELAY_API_RELEASE_IDENTITY_SHA256", "b" * 64)
    rejected = CliRunner().invoke(app, ["api", "start", "--port", "9002"])

    assert rejected.exit_code == 2
    assert "release identity does not match running package" in rejected.output
    assert launches == [("127.0.0.1", 9001)]
