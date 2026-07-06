from __future__ import annotations

from clio_relay.bootstrap import render_linux_user_bootstrap_script


def test_linux_user_bootstrap_script_installs_required_components() -> None:
    script = render_linux_user_bootstrap_script(frp_version="0.69.1")

    assert 'FRP_VERSION="0.69.1"' in script
    assert 'ARCHIVE="frp_${FRP_VERSION}_linux_amd64.tar.gz"' in script
    assert "uv python install 3.12" in script
    assert "CLIO_RELAY_AGENT_NPM_PACKAGE" in script
    assert "CLIO_RELAY_AGENT_NPM_BIN" in script
    assert 'npm install -g "$AGENT_NPM_PACKAGE"' in script
    assert "CLIO_RELAY_AGENT_BIN" in script
    assert "github.com/grc-iit/jarvis-cd.git" in script
    assert 'jarvis repo add "$DEST/jarvis-packages/clio_relay" --force true' in script
    assert "CLIO_RELAY_CORE_DIR" in script
    assert "clio-relay init" in script
    assert "\r" not in script
