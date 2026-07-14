"""Safe rendering for operator-configured values used on remote Linux targets."""

from __future__ import annotations

from pathlib import PurePosixPath

from clio_relay.errors import ConfigurationError

REMOTE_HOME_TOKEN = "$HOME"
SYSTEMD_HOME_SPECIFIER = "%h"


def render_remote_shell_value(value: str, *, field: str) -> str:
    """Quote a remote shell value while expanding only a leading ``$HOME`` token."""
    _validate_remote_value(value, field=field)
    home_suffix = _leading_home_suffix(value)
    if home_suffix is None:
        return f'"{_escape_shell_double_quoted(value)}"'
    return f'"$HOME{_escape_shell_double_quoted(home_suffix)}"'


def render_remote_shell_path(value: str, *, field: str) -> str:
    """Render an absolute remote POSIX path with controlled leading HOME expansion."""
    validate_remote_path(value, field=field)
    return render_remote_shell_value(value, field=field)


def render_systemd_remote_path(value: str, *, field: str) -> str:
    """Render an absolute remote path for systemd with controlled HOME conversion."""
    validate_remote_path(value, field=field)
    return render_systemd_remote_value(value, field=field)


def render_systemd_remote_value(value: str, *, field: str) -> str:
    """Convert only a leading remote ``$HOME`` token into systemd's ``%h`` specifier."""
    _validate_remote_value(value, field=field)
    home_suffix = _leading_home_suffix(value)
    if home_suffix is None:
        return value
    return f"{SYSTEMD_HOME_SPECIFIER}{home_suffix}"


def remote_value_expands_home(value: str) -> bool:
    """Return whether an original configured value requests leading HOME expansion."""
    return _leading_home_suffix(value) is not None


def validate_remote_path(value: str, *, field: str) -> None:
    """Require an absolute POSIX path or one anchored by an exact leading HOME token."""
    _validate_remote_value(value, field=field)
    if not value:
        raise ConfigurationError(f"remote {field} must be a nonempty path")
    if _leading_home_suffix(value) is None and not PurePosixPath(value).is_absolute():
        raise ConfigurationError(
            f"remote {field} must be an absolute POSIX path or start with $HOME/"
        )


def _leading_home_suffix(value: str) -> str | None:
    """Return the suffix when HOME is one exact leading token, otherwise ``None``."""
    if value == REMOTE_HOME_TOKEN:
        return ""
    if value.startswith(f"{REMOTE_HOME_TOKEN}/"):
        return value.removeprefix(REMOTE_HOME_TOKEN)
    return None


def _validate_remote_value(value: str, *, field: str) -> None:
    """Reject control characters before a value enters shell or systemd syntax."""
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ConfigurationError(f"remote {field} must not contain control characters")


def _escape_shell_double_quoted(value: str) -> str:
    """Escape every character that remains active inside POSIX double quotes."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("`", "\\`")
