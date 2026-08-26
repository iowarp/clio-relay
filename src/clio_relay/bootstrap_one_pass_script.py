"""The single combined stdin pass for a COLD cluster bootstrap (clio-relay#209).

A cold cluster bootstrap used to cost one lightweight preflight dial (which
correctly detects ``payload_required`` in one ``bash -s`` stdin pass, #158)
followed by SIX more separate ssh/scp dials -- mkdir staging, two scp
uploads, the remote install-script invocation, a receipt-``cat``
verification, and a staging cleanup -- plus a further, entirely separate
ssh dial elsewhere (``cli_remote_worker_probe._remote_target_identity``) to
observe the cluster's physical target identity. On a 2FA-gated cluster every
dial is a fresh authentication a human must approve, so a routine cold
install grew to roughly eight sessions (docs/connection-model.md:141-157
caps the whole bootstrap SETUP phase at one).

This module renders that entire six-step sequence -- and the physical
identity observation -- as ONE bash script, decoded and executed by a SINGLE
``ssh ... bash -s`` stdin pass (the same transport shape
``_bootstrap_preflight_over_ssh`` already established): it stages its own
scratch directory, decodes the archive and rendered install-script payloads
it carries inline as base64 blocks, runs the install script exactly as
before, re-reads the persistent receipt the install just wrote, observes the
freshly-installed host's physical identity, and self-cleans via an EXIT trap
on ANY outcome -- success, a failed install, or a dropped connection after
the remote process starts. ``bootstrap_ssh_deploy.py`` issues exactly one
dial with this script on stdin and parses its framed stdout; malformed or
missing framing is a typed ``RelayError``, never a partial-success guess.

Framing reuses the existing ``key=value`` stdout convention (the install
script's own ``bootstrap_receipt_json=`` line is untouched) rather than
inventing a second protocol: this module adds
``bootstrap_persistent_receipt_json=`` (folding in what used to be the
receipt-``cat`` dial) and ``bootstrap_target_identity_json=`` (folding in
the separate target-identity probe).

Scope note: the identity this module observes covers the REQUIRED
``ClusterTargetIdentity`` field this pass can prove from inside the freshly
installed host without any extra dependency -- ``hostnames`` -- plus the
optional ``site_marker_sha256``, using the exact same primitives
(``socket.gethostname``/``socket.getfqdn``/sha256 of ``/etc/machine-id``) as
the hidden ``clio-relay endpoint target-info`` command. ``ssh_host_key_sha256``
is deliberately NOT gathered here: it is observed by ``ssh-keyscan``/
``ssh-keygen`` against locally cached host keys, which never authenticates
against the target and so never costs a session -- callers compose it
locally. ``scheduler_cluster_name`` needs the full scheduler-provider
registry (``clio_relay.scheduler_providers``) and is left unset; a caller
that needs it still has the operator-pinned fallback.
"""

from __future__ import annotations

import base64
import json
import shlex
from typing import cast

from clio_relay.bootstrap_constants import BOOTSTRAP_PERSISTENT_RECEIPT_PATH
from clio_relay.bootstrap_staged_provider_source import (
    _POSIX_REMOTE_SHELL_STARTUP_ENVIRONMENT_NAMES,
    _STAGED_PROVIDER_ENVIRONMENT_SANITIZER,
)
from clio_relay.errors import RelayError

ONE_PASS_PERSISTENT_RECEIPT_MARKER = "bootstrap_persistent_receipt_json="
ONE_PASS_TARGET_IDENTITY_MARKER = "bootstrap_target_identity_json="
ONE_PASS_TARGET_IDENTITY_SCHEMA = "clio-relay.bootstrap-one-pass-target-identity.v1"

# Heredoc framing markers for the two inline payload blocks. Exported (rather
# than kept as literals inline in the renderer) so tests -- and the cleanup
# trap's own composition -- can locate each block precisely instead of
# fragile substring slicing.
ONE_PASS_ARCHIVE_HEREDOC_DELIMITER = "__CLIO_RELAY_ONE_PASS_ARCHIVE__"
ONE_PASS_SCRIPT_HEREDOC_DELIMITER = "__CLIO_RELAY_ONE_PASS_SCRIPT__"
ONE_PASS_STAGING_ROOT_ASSIGNMENT_PREFIX = "CLIO_RELAY_ONE_PASS_ROOT="
ONE_PASS_CLEANUP_TRAP_MARKER = "trap clio_relay_one_pass_cleanup EXIT"

_MAX_FRAMED_PAYLOAD_BYTES = 1024 * 1024

_ONE_PASS_POST_INSTALL_PYTHON = r"""
import hashlib
import json
import socket
import sys
from pathlib import Path

receipt_path = Path(sys.argv[1])
try:
    receipt_bytes = receipt_path.read_bytes()
except OSError as exc:
    print(f"could not read persistent bootstrap receipt: {exc}", file=sys.stderr)
    raise SystemExit(1) from None
if not receipt_bytes.strip():
    print("persistent bootstrap receipt is empty", file=sys.stderr)
    raise SystemExit(1)
print(
    "bootstrap_persistent_receipt_json="
    + receipt_bytes.decode("utf-8", errors="strict").strip()
)

hostnames = sorted({name for name in (socket.gethostname(), socket.getfqdn()) if name})
site_marker_sha256 = None
try:
    marker = Path("/etc/machine-id").read_bytes()
except OSError:
    marker = b""
if marker.strip():
    site_marker_sha256 = hashlib.sha256(marker).hexdigest()
identity = {
    "schema_version": "clio-relay.bootstrap-one-pass-target-identity.v1",
    "hostnames": hostnames,
    "site_marker_sha256": site_marker_sha256,
}
print(
    "bootstrap_target_identity_json="
    + json.dumps(identity, sort_keys=True, separators=(",", ":"))
)
""".strip("\n")


def render_one_pass_cold_bootstrap_script(
    *,
    remote_root: str,
    archive_bytes: bytes,
    install_script: str,
) -> str:
    """Render the single combined stdin pass for a cold cluster install.

    ``remote_root`` is the caller's already-unique staging directory (keyed
    off the bootstrap invocation id, exactly as before). The returned script
    is meant to travel over stdin to ``ssh ssh_host bash -s`` -- never in
    argv, for the same truncation reason the preflight script documents
    (#158).
    """
    encoded_archive = base64.b64encode(archive_bytes).decode("ascii")
    encoded_script = base64.b64encode(install_script.encode("utf-8")).decode("ascii")
    quoted_root = shlex.quote(remote_root)
    # BOOTSTRAP_PERSISTENT_RECEIPT_PATH is a fixed, trusted template
    # ("$HOME/...") that must expand $HOME in the remote shell -- double
    # quoting (not shlex.quote, which single-quotes and so would freeze
    # "$HOME" as a literal string instead of expanding it) is exactly what
    # the rest of this file's own ssh call sites rely on for the same path.
    quoted_receipt_path = f'"{BOOTSTRAP_PERSISTENT_RECEIPT_PATH}"'
    # The rest of the script is appended to the SAME bash -s stream rather
    # than re-exec'd through a nested `bash -c '<script>'` (the preflight
    # script's own pattern, safe there because its payload is small): the
    # archive's base64 block alone can run to megabytes, and a remote
    # `bash -c` argv is bounded by the host's ARG_MAX. Heredoc bodies are
    # ordinary stdin text with no such bound, so staying on this single
    # bash -s stream keeps the payload size unconstrained by argv.
    inner = "\n".join(
        [
            "set -eu",
            f"{ONE_PASS_STAGING_ROOT_ASSIGNMENT_PREFIX}{quoted_root}",
            "clio_relay_one_pass_cleanup() {",
            "  status=$?",
            '  rm -rf -- "$CLIO_RELAY_ONE_PASS_ROOT" 2>/dev/null || true',
            '  exit "$status"',
            "}",
            ONE_PASS_CLEANUP_TRAP_MARKER,
            "umask 077",
            'mkdir -- "$CLIO_RELAY_ONE_PASS_ROOT"',
            'chmod 700 -- "$CLIO_RELAY_ONE_PASS_ROOT"',
            (
                'base64 -d > "$CLIO_RELAY_ONE_PASS_ROOT/clio-relay-head.tar" '
                f"<<'{ONE_PASS_ARCHIVE_HEREDOC_DELIMITER}'"
            ),
            encoded_archive,
            ONE_PASS_ARCHIVE_HEREDOC_DELIMITER,
            (
                'base64 -d > "$CLIO_RELAY_ONE_PASS_ROOT/clio-relay-bootstrap.sh" '
                f"<<'{ONE_PASS_SCRIPT_HEREDOC_DELIMITER}'"
            ),
            encoded_script,
            ONE_PASS_SCRIPT_HEREDOC_DELIMITER,
            'bash "$CLIO_RELAY_ONE_PASS_ROOT/clio-relay-bootstrap.sh"',
            "if ! command -v python3 >/dev/null 2>&1; then",
            '  echo "python3 is required to observe the physical target identity" >&2',
            "  exit 1",
            "fi",
            f'if [ ! -e {quoted_receipt_path} ] && [ ! -L {quoted_receipt_path} ]; then',
            '  echo "bootstrap did not publish its persistent receipt" >&2',
            "  exit 1",
            "fi",
            f"python3 -I - {quoted_receipt_path} <<'__CLIO_RELAY_ONE_PASS_POST_INSTALL__'",
            *_ONE_PASS_POST_INSTALL_PYTHON.splitlines(),
            "__CLIO_RELAY_ONE_PASS_POST_INSTALL__",
        ]
    )
    return "\n".join(
        [
            'if [ -n "${BASH_VERSION-}" ]; then',
            f"  eval {shlex.quote(_STAGED_PROVIDER_ENVIRONMENT_SANITIZER)}",
            "else",
            "  unset " + " ".join(_POSIX_REMOTE_SHELL_STARTUP_ENVIRONMENT_NAMES),
            "fi",
            inner,
        ]
    )


def parse_one_pass_persistent_receipt(
    lines: list[str],
    *,
    receipt: dict[str, object],
) -> None:
    """Validate the folded-in persistent-receipt re-read against the stdout receipt.

    Replaces the standalone ``ssh ... cat bootstrap-receipt.json`` dial: the
    one-pass script re-reads the same file itself, in the same session, and
    frames the bytes on stdout for the desktop to compare -- same semantics
    as ``_verify_persistent_bootstrap_receipt``, zero extra dials.
    """
    payloads = [
        line.removeprefix(ONE_PASS_PERSISTENT_RECEIPT_MARKER)
        for line in lines
        if line.startswith(ONE_PASS_PERSISTENT_RECEIPT_MARKER)
    ]
    if len(payloads) != 1:
        raise RelayError(
            "bootstrap one-pass output must contain exactly one persistent receipt "
            f"observation, observed {len(payloads)}"
        )
    payload = payloads[0]
    if len(payload.encode("utf-8")) > _MAX_FRAMED_PAYLOAD_BYTES:
        raise RelayError("persistent bootstrap receipt exceeds the bounded size")
    try:
        persistent = cast(object, json.loads(payload))
    except json.JSONDecodeError as exc:
        raise RelayError(f"persistent bootstrap receipt was not valid JSON: {exc}") from exc
    if persistent != receipt:
        raise RelayError("persistent bootstrap receipt differs from current stdout evidence")


def parse_one_pass_target_identity(lines: list[str]) -> dict[str, object]:
    """Validate and return the framed target-identity observation.

    Replaces the standalone ``cli_remote_worker_probe._remote_target_identity``
    verification dial for a fresh cold install: the identity is LEARNED here,
    inside the same session that just installed the relay, instead of being
    re-observed over a later dial.
    """
    payloads = [
        line.removeprefix(ONE_PASS_TARGET_IDENTITY_MARKER)
        for line in lines
        if line.startswith(ONE_PASS_TARGET_IDENTITY_MARKER)
    ]
    if len(payloads) != 1:
        raise RelayError(
            "bootstrap one-pass output must contain exactly one target identity "
            f"observation, observed {len(payloads)}"
        )
    payload = payloads[0]
    if len(payload.encode("utf-8")) > _MAX_FRAMED_PAYLOAD_BYTES:
        raise RelayError("bootstrap target identity observation exceeds the bounded size")
    try:
        raw = cast(object, json.loads(payload))
    except json.JSONDecodeError as exc:
        raise RelayError(f"bootstrap target identity was not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise RelayError("bootstrap target identity did not return an object")
    identity = cast(dict[str, object], raw)
    if identity.get("schema_version") != ONE_PASS_TARGET_IDENTITY_SCHEMA:
        raise RelayError("bootstrap target identity schema did not match")
    raw_hostnames = identity.get("hostnames")
    if not isinstance(raw_hostnames, list):
        raise RelayError("bootstrap target identity omitted its observed hostnames")
    hostnames = cast("list[object]", raw_hostnames)
    if not hostnames or not all(isinstance(item, str) and item for item in hostnames):
        raise RelayError("bootstrap target identity omitted its observed hostnames")
    site_marker_sha256 = identity.get("site_marker_sha256")
    if site_marker_sha256 is not None and not isinstance(site_marker_sha256, str):
        raise RelayError("bootstrap target identity site marker was not a string")
    return identity


def extract_one_pass_payloads(script: str) -> tuple[bytes, str]:
    """Recover the embedded archive bytes and install-script text.

    Round-trips what :func:`render_one_pass_cold_bootstrap_script` embedded,
    for tests that must prove the payloads travel intact -- and for anyone
    debugging a captured script by hand.
    """

    def _heredoc_body(delimiter: str) -> str:
        opening = script.index(f"<<'{delimiter}'")
        start = script.index("\n", opening) + 1
        end = script.index(f"\n{delimiter}\n", start)
        return script[start:end]

    archive_bytes = base64.b64decode(_heredoc_body(ONE_PASS_ARCHIVE_HEREDOC_DELIMITER))
    install_script = base64.b64decode(_heredoc_body(ONE_PASS_SCRIPT_HEREDOC_DELIMITER)).decode(
        "utf-8"
    )
    return archive_bytes, install_script
