from __future__ import annotations

from typing import Protocol

import pytest

from clio_relay.errors import ConfigurationError
from clio_relay.remote_values import (
    render_remote_shell_path,
    render_remote_shell_value,
    render_systemd_remote_path,
    render_systemd_remote_value,
)


class _RemoteRenderer(Protocol):
    def __call__(self, value: str, *, field: str) -> str:
        """Render one configured remote value."""
        ...


def test_remote_value_contract_expands_only_exact_leading_home() -> None:
    """Shell and systemd renderers preserve every non-leading expansion token literally."""
    leading = "$HOME/state dir/o'hare$(touch /tmp/x)`id`$VALUE_SUFFIX"
    absolute = "/srv/$HOME/state dir/o'hare$(touch /tmp/y)`id`$VALUE_SUFFIX"

    assert render_remote_shell_value(leading, field="value") == (
        '"$HOME/state dir/o\'hare\\$(touch /tmp/x)\\`id\\`\\$VALUE_SUFFIX"'
    )
    assert render_remote_shell_value(absolute, field="value") == (
        '"/srv/\\$HOME/state dir/o\'hare\\$(touch /tmp/y)\\`id\\`\\$VALUE_SUFFIX"'
    )
    assert render_systemd_remote_value(leading, field="value") == (
        "%h/state dir/o'hare$(touch /tmp/x)`id`$VALUE_SUFFIX"
    )
    assert render_systemd_remote_value(absolute, field="value") == absolute


@pytest.mark.parametrize("value", ["relative/path", "$USER/path", "$HOME-other/path", ""])
def test_remote_path_contract_requires_an_unambiguous_absolute_path(value: str) -> None:
    """State paths cannot depend on an unspecified remote working directory or variable."""
    with pytest.raises(ConfigurationError, match="absolute POSIX path|nonempty path"):
        render_remote_shell_path(value, field="core_dir")


@pytest.mark.parametrize(
    "renderer",
    [
        render_remote_shell_value,
        render_remote_shell_path,
        render_systemd_remote_value,
        render_systemd_remote_path,
    ],
)
def test_remote_value_contract_rejects_controls(renderer: _RemoteRenderer) -> None:
    """No remote rendering surface accepts control characters."""
    with pytest.raises(ConfigurationError, match="control characters"):
        renderer("$HOME/path\nforged", field="path")
