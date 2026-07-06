from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.config import RelaySettings
from clio_relay.doctor import run_doctor
from clio_relay.errors import ConfigurationError
from clio_relay.relay_host import FrpsConfig, render_frps_config


def test_render_frps_config_has_no_application_state() -> None:
    rendered = render_frps_config(FrpsConfig(bind_port=7001, token="secret"))

    assert "bind_port = 7001" in rendered
    assert "token = secret" in rendered
    assert "job" not in rendered.lower()
    assert "queue" not in rendered.lower()


def test_live_doctor_requires_frps_address(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_FRPS_ADDR"):
        run_doctor(settings, live=True)
