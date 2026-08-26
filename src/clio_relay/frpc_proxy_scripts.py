"""Compose the bash scripts one ssh pass sends to bring up/tear down/inspect the
cluster-side frpc proxy unit (clio-relay#279).

Each of the three public renderers here is a PURE function: given already-
rendered config text (from ``frpc_unit.py``) and paths, it returns the exact
script text a caller (``frpc_proxy_bringup.py``) sends over ``ssh <host> bash
-s`` -- never anything itself. Nothing in this module dials anywhere, so
every function here is exercised by giving it inputs and inspecting its
output text, exactly like ``bootstrap_one_pass_script.py``'s own composition
functions.

**Why this does not reuse #209's base64/heredoc payload framing.** #209's
``bootstrap_one_pass_script.py`` base64-encodes its payload because it is
BINARY (a tar archive) -- a shell string literal cannot safely carry
arbitrary binary bytes even single-quoted. This module's three payloads
(TOML, env file, unit file) are always plain UTF-8 text, so they follow the
simpler, already-proven pattern this exact codebase already uses for
installing a text config file over ssh: ``deployment_ssh.py``'s
``_remote_install_script`` embeds a full unit file's text via
``shlex.quote(text)`` then ``printf '%s' <literal> > path`` -- no base64,
no heredoc, no scratch staging directory. A POSIX single-quoted string
preserves embedded newlines literally, and the whole script (this literal
included) travels over ssh's STDIN, never argv, so the #158/#209 argv-
truncation hazard those techniques defend against does not apply here
either way. The one place this module DOES base64-encode a payload is the
status script's journal tail, which -- unlike the three rendered configs --
is arbitrary log content of unknown shape, exactly the class of payload
base64 exists for.

**Why no EXIT-trap/staging-directory cleanup.** #209 stages into a
caller-named directory specifically because it must extract a multi-file
archive there before moving anything into place, and a failed ``mkdir`` on
that caller-chosen path must not vanish a directory it did not create (the
"created-flag" guard). This module writes each of its three files directly
to its final location with ``umask 077`` set first (so every created file
is private by default; the env file is additionally ``chmod 600``'d for
defense in depth) -- there is no scratch directory to stage into or clean
up, so the trap discipline this guards would have nothing to protect.

**Secrets never touch this module's own memory longer than one render
call**: the env file's real secret values are interpolated into the script
text exactly once (via ``shlex.quote``) and the caller is responsible for
never logging the returned script text.
"""

from __future__ import annotations

import shlex

from clio_relay.deployment_activation import render_bounded_user_service_activation_helper
from clio_relay.errors import RelayError
from clio_relay.frpc_proxy_receipt import BRINGUP_RECEIPT_SCHEMA, TEARDOWN_RECEIPT_SCHEMA
from clio_relay.frpc_unit import (
    FrpcProxyPaths,
    frpc_proxy_config_digest,
    validate_frpc_proxy_service_name,
)


def render_frpc_proxy_install_script(
    *,
    cluster: str,
    proxy_name: str,
    paths: FrpcProxyPaths,
    toml_text: str,
    env_text: str,
    unit_text: str,
) -> str:
    """Render the ONE-pass bash script that installs, enables, and starts the proxy.

    Reuses ``deployment_activation.render_bounded_user_service_activation_helper``
    for the actual enable+start+poll-to-active step -- the SAME race-safe,
    already-tested observer the worker endpoint unit's own install script
    uses (``deployment_ssh._remote_install_script``) -- rather than a second,
    hand-rolled polling loop.
    """
    validate_frpc_proxy_service_name(paths.unit_name)
    _reject_unsafe_field_text(cluster, field="cluster")
    _reject_unsafe_field_text(proxy_name, field="proxy_name")
    quoted_unit = shlex.quote(paths.unit_name)
    digest = frpc_proxy_config_digest(toml_text)
    activation_helper = render_bounded_user_service_activation_helper()
    return f"""set -eu
umask 077
mkdir -p "{_shell_dirname(paths.toml_shell_path)}" \
"{_shell_dirname(paths.unit_shell_path)}" </dev/null
printf '%s' {shlex.quote(toml_text)} > "{paths.toml_shell_path}"
chmod 600 -- "{paths.toml_shell_path}"
printf '%s' {shlex.quote(env_text)} > "{paths.env_shell_path}"
chmod 600 -- "{paths.env_shell_path}"
printf '%s' {shlex.quote(unit_text)} > "{paths.unit_shell_path}"
systemctl --user daemon-reload </dev/null
systemctl --user enable {quoted_unit} </dev/null
CLIO_RELAY_ENDPOINT_SERVICE_NAME={quoted_unit}
CLIO_RELAY_ENDPOINT_ACTIVATION_ACTION=restart
{activation_helper}
if ! clio_relay_endpoint_activate_bounded; then
  echo "frpc proxy unit did not become active: {paths.unit_name} \
$CLIO_RELAY_ENDPOINT_ACTIVE_STATE/$CLIO_RELAY_ENDPOINT_SUB_STATE \
outcome=$CLIO_RELAY_ENDPOINT_ACTIVATION_OUTCOME" >&2
  exit 1
fi
service_enabled="$(systemctl --user is-enabled {quoted_unit} 2>/dev/null || true)"
service_active="$(systemctl --user is-active {quoted_unit} 2>/dev/null || true)"
service_enabled_bool=false
[ "$service_enabled" = "enabled" ] && service_enabled_bool=true
service_active_bool=false
[ "$service_active" = "active" ] && service_active_bool=true
printf 'FrpcProxyReceiptSchema=%s\\n' {shlex.quote(BRINGUP_RECEIPT_SCHEMA)}
printf 'FrpcProxyCluster=%s\\n' {shlex.quote(cluster)}
printf 'FrpcProxyName=%s\\n' {shlex.quote(proxy_name)}
printf 'FrpcProxyUnitName=%s\\n' {quoted_unit}
printf 'FrpcProxyTomlPath=%s\\n' {shlex.quote(paths.toml_unit_path)}
printf 'FrpcProxyEnvPath=%s\\n' {shlex.quote(paths.env_unit_path)}
printf 'FrpcProxyConfigSha256=%s\\n' {shlex.quote(digest)}
printf 'FrpcProxyEnabled=%s\\n' "$service_enabled_bool"
printf 'FrpcProxyActive=%s\\n' "$service_active_bool"
printf 'FrpcProxyInstalledAt=%s\\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
""".replace("\r\n", "\n")


def render_frpc_proxy_teardown_script(*, cluster: str, paths: FrpcProxyPaths) -> str:
    """Render the ONE-pass bash script that disables, stops, and removes the proxy."""
    validate_frpc_proxy_service_name(paths.unit_name)
    _reject_unsafe_field_text(cluster, field="cluster")
    quoted_unit = shlex.quote(paths.unit_name)
    return f"""set -eu
unit_existed=false
[ -e "{paths.unit_shell_path}" ] && unit_existed=true
toml_existed=false
[ -e "{paths.toml_shell_path}" ] && toml_existed=true
env_existed=false
[ -e "{paths.env_shell_path}" ] && env_existed=true
systemctl --user disable --now {quoted_unit} </dev/null 2>/dev/null || true
rm -f -- "{paths.unit_shell_path}" "{paths.toml_shell_path}" "{paths.env_shell_path}" \
"{paths.receipt_shell_path}"
systemctl --user daemon-reload </dev/null
printf 'FrpcProxyTeardownSchema=%s\\n' {shlex.quote(TEARDOWN_RECEIPT_SCHEMA)}
printf 'FrpcProxyCluster=%s\\n' {shlex.quote(cluster)}
printf 'FrpcProxyUnitName=%s\\n' {quoted_unit}
printf 'FrpcProxyRemovedUnit=%s\\n' "$unit_existed"
printf 'FrpcProxyRemovedToml=%s\\n' "$toml_existed"
printf 'FrpcProxyRemovedEnv=%s\\n' "$env_existed"
printf 'FrpcProxyTornDownAt=%s\\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
""".replace("\r\n", "\n")


def render_frpc_proxy_status_script(*, unit_name: str) -> str:
    """Render the ONE-pass, read-only bash script the ``proxy-status`` verb sends.

    Mirrors ``endpoint_service_status.render_endpoint_service_readiness_script``:
    plain ``systemctl --user show --property=`` output, no mutation. The one
    addition is the unit's own recent journal, base64-encoded on the one line
    ``JournalTailBase64=`` -- arbitrary log content is the one payload in this
    module that legitimately needs base64 (see module docstring).
    """
    validate_frpc_proxy_service_name(unit_name)
    quoted_unit = shlex.quote(unit_name)
    return f"""set -euo pipefail
export SYSTEMD_COLORS=0 LANG=C LC_ALL=C
if ! properties="$(
  systemctl --user show {quoted_unit} --no-pager \
    --property=LoadState --property=ActiveState --property=SubState \
    --property=UnitFileState 2>/dev/null
)"; then
  echo "bounded systemd frpc-proxy inspection failed: {unit_name}" >&2
  exit 74
fi
printf '%s\\n' "$properties"
journal_lines="$(journalctl --user --unit={quoted_unit} --no-pager --lines=20 2>/dev/null || true)"
printf 'JournalTailBase64=%s\\n' "$(printf '%s' "$journal_lines" | base64 | tr -d '\\n')"
""".replace("\r\n", "\n")


def _reject_unsafe_field_text(value: str, *, field: str) -> None:
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise RelayError(f"frpc proxy {field} must be non-empty with no newlines or NUL")


def _shell_dirname(path: str) -> str:
    """Return the parent directory of one of this module's own ``$HOME``-rooted paths."""
    parent = path.rsplit("/", 1)[0]
    if not parent:
        raise RelayError(f"could not resolve a parent directory for {path!r}")
    return parent
