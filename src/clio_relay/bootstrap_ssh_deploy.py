"""SSH-based cluster bootstrap orchestration for the Linux cluster bootstrap.

Split from bootstrap.py (clio-relay#255). ``_bootstrap_preflight_over_ssh``
asks an already-installed relay to verify/repair its exact desired state
without a payload; ``bootstrap_cluster_over_ssh`` is the full
install-or-reconcile round trip. Both call back into ``clio_relay.bootstrap``
via a *qualified*, call-time module attribute lookup
(``bootstrap.<name>(...)``) for every collaborator bootstrap.py's own test
suite monkeypatches (``_run``, ``create_bootstrap_archive``,
``render_linux_user_bootstrap_script``, ``_verify_persistent_bootstrap_receipt``,
``_validate_relay_bootstrap_wheel``, ``uuid4``, ``BootstrapPreflightResult``)
or that simply still live there (``bootstrap_relay_identity``,
``_bootstrap_desired_state``, ``_is_clio_relay_git_checkout``,
``_sha256_regular_file``, ``_validate_ssh_destination``,
``_remaining_public_deadline``) -- never a bare/early-bound import -- so
``monkeypatch.setattr(bootstrap, "X", ...)`` in the existing test suite keeps
reaching the real call site. This is the same forwarder idiom cli.py's
R8(ii) decomposition established.

**One-pass cold install (clio-relay#209).** When preflight reports
``payload_required`` -- a cold target -- the archive build, install-script
render, and the actual remote install used to cost SIX further separate ssh/
scp dials (mkdir staging, two scp uploads, the remote script invocation, a
receipt-``cat`` verification, and a cleanup ``rm -rf``); on a 2FA-gated
cluster that is six more authentications for what the ssh-budget doctrine
(docs/connection-model.md:141-157) caps at one combined setup pass. Those six
steps are now composed as ONE ``bash -s`` stdin script by
``bootstrap_one_pass_script.render_one_pass_cold_bootstrap_script`` (payloads
travel inline as base64 blocks, never a second dial) and issued as a single
``bootstrap._run([...])`` call; its framed stdout is parsed by
``bootstrap_one_pass_script.parse_one_pass_persistent_receipt``/
``parse_one_pass_target_identity``, which also fold in what used to be the
receipt-``cat`` dial and a separate target-identity probe dial. A cold
bootstrap therefore costs exactly two ssh dials end to end: the preflight
discovery dial (unchanged, needed to tell warm from cold) and this one
combined install pass -- see ``tests/test_bootstrap_preflight_transport.py``'s
dial-count conformance test. The warm no-op fast path (below,
``preflight_receipt is not None``) is untouched and stays at its existing two
dials (preflight + the unchanged ``_verify_persistent_bootstrap_receipt``
re-verification).
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import shutil
import tempfile
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING, cast

from clio_relay import bootstrap_pin, bootstrap_receipt_validation
from clio_relay.bootstrap_constants import (
    BOOTSTRAP_PUBLIC_EXACT_DEADLINE_SECONDS,
    BOOTSTRAP_PUBLIC_REPAIR_DEADLINE_SECONDS,
    BOOTSTRAP_REMOTE_SCRIPT_TIMEOUT_SECONDS,
    DEFAULT_REMOTE_CORE_DIR,
    DEFAULT_REMOTE_SPOOL_DIR,
    FRP_VERSION,
)
from clio_relay.bootstrap_one_pass_script import (
    parse_one_pass_persistent_receipt,
    parse_one_pass_target_identity,
    render_one_pass_cold_bootstrap_script,
)
from clio_relay.bootstrap_receipt_classifier_source import (
    _BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE,
)
from clio_relay.bootstrap_staged_provider_source import (
    _POSIX_REMOTE_SHELL_STARTUP_ENVIRONMENT_NAMES,
    _STAGED_PROVIDER_ENVIRONMENT_SANITIZER,
)
from clio_relay.deployment import endpoint_user_service_name
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)
from clio_relay.remote_values import render_remote_shell_path

if TYPE_CHECKING:
    from clio_relay.bootstrap import BootstrapPreflightResult
    from clio_relay.bootstrap_reconcile import BootstrapDesiredState


def _bootstrap_preflight_over_ssh(
    *,
    ssh_host: str,
    invocation_id: str,
    desired: BootstrapDesiredState,
    core_dir: str,
    spool_dir: str,
    repair: bool,
    timeout_seconds: float,
) -> BootstrapPreflightResult:
    """Ask an installed relay to verify/repair exact state without a payload."""
    import clio_relay.bootstrap as bootstrap

    if timeout_seconds <= 2:
        raise RelayError("bootstrap preflight has no remaining public deadline")
    remote_timeout = max(1, min(55 if repair else 24, int(timeout_seconds - 1)))
    encoded = base64.b64encode(
        json.dumps(
            desired.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).decode("ascii")
    command = "\n".join(
        [
            "set -u",
            *_STAGED_PROVIDER_ENVIRONMENT_SANITIZER.splitlines(),
            f"export CLIO_RELAY_CORE_DIR={render_remote_shell_path(core_dir, field='core_dir')}",
            f"export CLIO_RELAY_SPOOL_DIR={render_remote_shell_path(spool_dir, field='spool_dir')}",
            ("export CLIO_RELAY_BOOTSTRAP_DESIRED_STATE_BASE64=" + shlex.quote(encoded)),
            f'if [ ! -x "{bootstrap_pin.BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE}" ]; then '
            "echo bootstrap_preflight_unsupported=not_installed; exit 0; fi",
            f'BOOTSTRAP_INSTALL_RECEIPT="{bootstrap_pin.BOOTSTRAP_PRODUCED_INSTALL_RECEIPT}"',
            'if [ -e "$BOOTSTRAP_INSTALL_RECEIPT" ] || [ -L "$BOOTSTRAP_INSTALL_RECEIPT" ]; then',
            '  if ! BOOTSTRAP_RELAY_RECEIPT_CLASS="$(python3 -I - '
            "\"$BOOTSTRAP_INSTALL_RECEIPT\" <<'__CLIO_RELAY_PREFLIGHT_RECEIPT__'",
            *_BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE.splitlines(),
            "__CLIO_RELAY_PREFLIGHT_RECEIPT__",
            '  )"; then',
            "    echo bootstrap_preflight_unsupported=legacy_relay_provider",
            "    exit 0",
            "  fi",
            '  if [ "$BOOTSTRAP_RELAY_RECEIPT_CLASS" != "current" ]; then',
            "    echo bootstrap_preflight_unsupported=legacy_relay_provider",
            "    exit 0",
            "  fi",
            "else",
            "  echo bootstrap_preflight_unsupported=legacy_relay_provider",
            "  exit 0",
            "fi",
            "if ! command -v timeout >/dev/null 2>&1; then",
            '  echo "timeout is required" >&2',
            "  exit 1",
            "fi",
            "set +e",
            (
                'BOOTSTRAP_PREFLIGHT_OUTPUT="$(timeout --signal=TERM --kill-after=2s '
                f"{remote_timeout}s "
                f'"{bootstrap_pin.BOOTSTRAP_PRODUCED_RELAY_EXECUTABLE}" '
                f"bootstrap-inspect --invocation-id {shlex.quote(invocation_id)} "
                + ("--repair " if repair else "--inspect-only ")
                + '2>&1)"'
            ),
            "BOOTSTRAP_PREFLIGHT_STATUS=$?",
            "set -e",
            'if [ "$BOOTSTRAP_PREFLIGHT_STATUS" -ne 0 ]; then',
            "  if printf '%s\\n' \"$BOOTSTRAP_PREFLIGHT_OUTPUT\" | "
            "grep -Eqi "
            "'no such command.*bootstrap-inspect|bootstrap-inspect.*no such command'; then",
            "    echo bootstrap_preflight_unsupported=missing_command",
            "    exit 0",
            "  fi",
            '  if BOOTSTRAP_PREFLIGHT_OUTPUT="$BOOTSTRAP_PREFLIGHT_OUTPUT" '
            "python3 -I - <<'__CLIO_RELAY_REPAIRABLE_QUEUE_ROOT__'",
            "import json",
            "import os",
            "import sys",
            "from pathlib import Path",
            "",
            'prefix = "error: "',
            "matches = [line.removeprefix(prefix) for line in "
            'os.environ["BOOTSTRAP_PREFLIGHT_OUTPUT"].splitlines() '
            "if line.startswith(prefix)]",
            "if len(matches) != 1:",
            "    raise SystemExit(1)",
            "try:",
            "    report = json.loads(matches[0])",
            "except json.JSONDecodeError:",
            "    raise SystemExit(1) from None",
            "expected = {",
            '    "schema_version": "clio-relay.legacy-state-audit.v1",',
            '    "family": "root",',
            '    "reason": "queue directory is readable or writable by another user",',
            '    "action": (',
            '        "move the unsafe state aside or export records with portable durable IDs "',
            '        "before retrying"',
            "    ),",
            "}",
            'if not isinstance(report, dict) or set(report) != {*expected, "path"}:',
            "    raise SystemExit(1)",
            "if any(report.get(name) != value for name, value in expected.items()):",
            "    raise SystemExit(1)",
            'path = report.get("path")',
            "if not isinstance(path, str):",
            "    raise SystemExit(1)",
            "try:",
            "    observed = Path(path).resolve(strict=True)",
            '    configured = Path(os.environ["CLIO_RELAY_CORE_DIR"]).resolve(strict=True)',
            "except OSError:",
            "    raise SystemExit(1) from None",
            "if observed != configured:",
            "    raise SystemExit(1)",
            "__CLIO_RELAY_REPAIRABLE_QUEUE_ROOT__",
            "  then",
            "    echo bootstrap_preflight_unsupported=repairable_queue_permissions",
            "    exit 0",
            "  fi",
            "  printf '%s\\n' \"$BOOTSTRAP_PREFLIGHT_OUTPUT\" >&2",
            '  exit "$BOOTSTRAP_PREFLIGHT_STATUS"',
            "fi",
            "printf '%s\\n' \"$BOOTSTRAP_PREFLIGHT_OUTPUT\"",
        ]
    )
    remote_command = "\n".join(
        [
            'if [ -n "${BASH_VERSION-}" ]; then',
            f"  eval {shlex.quote(_STAGED_PROVIDER_ENVIRONMENT_SANITIZER)}",
            "else",
            "  unset " + " ".join(_POSIX_REMOTE_SHELL_STARTUP_ENVIRONMENT_NAMES),
            "fi",
            f"exec bash -c {shlex.quote(command)}",
        ]
    )
    # The preflight script is ~11 KB. It travels on STDIN, never in argv: some
    # ssh clients silently truncate a long command-line argument (the MSYS2
    # OpenSSH in Git for Windows drops everything past roughly 8-10 KB), and
    # the remote shell then fails to parse a script that was cut mid-token --
    # reporting a syntax error that names neither the truncation nor the
    # transport (clio-relay#158).
    result = bootstrap._run(
        ["ssh", ssh_host, "bash", "-s"],
        input_bytes=remote_command.encode("utf-8"),
        timeout_seconds=timeout_seconds,
    )
    lines = result.stdout.splitlines()
    payload_lines = [
        line.removeprefix("bootstrap_preflight_json=")
        for line in lines
        if line.startswith("bootstrap_preflight_json=")
    ]
    if not payload_lines:
        unsupported = [
            line
            for line in lines
            if line
            in {
                "bootstrap_preflight_unsupported=not_installed",
                "bootstrap_preflight_unsupported=missing_command",
                "bootstrap_preflight_unsupported=legacy_relay_provider",
                "bootstrap_preflight_unsupported=repairable_queue_permissions",
            }
        ]
        if len(unsupported) != 1:
            raise RelayError("bootstrap preflight returned no supported inspector evidence")
        return bootstrap.BootstrapPreflightResult(
            action="payload_required", receipt=None, lines=lines
        )
    if len(payload_lines) != 1 or len(payload_lines[0].encode()) > 1024 * 1024:
        raise RelayError("bootstrap preflight returned invalid bounded evidence")
    try:
        raw = cast(object, json.loads(payload_lines[0]))
    except json.JSONDecodeError as exc:
        raise RelayError("bootstrap preflight returned invalid JSON") from exc
    if not isinstance(raw, dict):
        raise RelayError("bootstrap preflight did not return an object")
    payload = cast(dict[str, object], raw)
    if (
        payload.get("schema_version") != "clio-relay.bootstrap-preflight.v1"
        or payload.get("desired_fingerprint") != desired.fingerprint
        or not isinstance(payload.get("exact_match"), bool)
    ):
        raise RelayError("bootstrap preflight identity did not match the request")
    if payload.get("exact_match") is not True:
        action = payload.get("action")
        if (
            action not in {"payload_required", "repair_required"}
            or payload.get("receipt") is not None
        ):
            raise RelayError("bootstrap preflight returned ambiguous non-exact action evidence")
        if repair and action == "repair_required":
            raise RelayError("explicit bootstrap repair returned another repair request")
        return bootstrap.BootstrapPreflightResult(
            action=cast(str, action), receipt=None, lines=lines
        )
    raw_receipt = payload.get("receipt")
    if not isinstance(raw_receipt, dict):
        raise RelayError("successful bootstrap preflight omitted its receipt")
    receipt = cast(dict[str, object], raw_receipt)
    if receipt.get("invocation_id") != invocation_id:
        raise RelayError("bootstrap preflight receipt invocation changed")
    return bootstrap.BootstrapPreflightResult(action="exact", receipt=receipt, lines=lines)


def bootstrap_cluster_over_ssh(
    *,
    bootstrap_profile: str,
    ssh_host: str,
    source_root: Path,
    cluster: str | None = None,
    core_dir: str = DEFAULT_REMOTE_CORE_DIR,
    spool_dir: str = DEFAULT_REMOTE_SPOOL_DIR,
    relay_wheel: Path | None = None,
    relay_artifact_sha256: str | None = None,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    frp_version: str = FRP_VERSION,
    jarvis_resource_graph_profile: str | None = None,
    allow_jarvis_resource_graph_build: bool = False,
) -> list[str]:
    """Install relay dependencies and the current source tree on a cluster over SSH."""
    import clio_relay.bootstrap as bootstrap

    public_started = monotonic()
    if bootstrap_profile != "linux-user":
        raise ConfigurationError(f"unsupported bootstrap profile: {bootstrap_profile}")
    if cluster is not None:
        endpoint_user_service_name(cluster)
    render_remote_shell_path(core_dir, field="core_dir")
    render_remote_shell_path(spool_dir, field="spool_dir")
    bootstrap._validate_ssh_destination(ssh_host)
    expected_jarvis_mcp_spec = os.environ.get(
        "CLIO_RELAY_JARVIS_MCP_INSTALL_SPEC",
        CLIO_KIT_JARVIS_MCP_WHEEL_URL,
    )
    expected_jarvis_mcp_sha256 = os.environ.get(
        "CLIO_RELAY_JARVIS_MCP_ARTIFACT_SHA256",
        (
            CLIO_KIT_JARVIS_MCP_WHEEL_SHA256
            if expected_jarvis_mcp_spec == CLIO_KIT_JARVIS_MCP_WHEEL_URL
            else ""
        ),
    )
    if not bootstrap_receipt_validation.is_sha256_value(expected_jarvis_mcp_sha256):
        raise ConfigurationError("clio-kit bootstrap source requires its expected SHA-256")
    planned_identity = bootstrap.bootstrap_relay_identity(
        source_root=source_root,
        relay_wheel=relay_wheel,
        relay_artifact_sha256=relay_artifact_sha256,
    )
    if shutil.which("ssh") is None:
        raise ConfigurationError("ssh is required for remote bootstrap")
    expected_desired_state = bootstrap._bootstrap_desired_state(
        identity=planned_identity,
        cluster=cluster,
        core_dir=core_dir,
        spool_dir=spool_dir,
        frp_version=frp_version,
        clio_kit_install_spec=expected_jarvis_mcp_spec,
        clio_kit_artifact_sha256=expected_jarvis_mcp_sha256,
        agent_adapter=agent_adapter,
        agent_npm_package=agent_npm_package,
        agent_npm_bin=agent_npm_bin,
        agent_args=agent_args or [],
        jarvis_resource_graph_profile=jarvis_resource_graph_profile,
        allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
    )
    invocation_id = f"bootstrap_{bootstrap.uuid4().hex}"
    exact_deadline = public_started + BOOTSTRAP_PUBLIC_EXACT_DEADLINE_SECONDS
    repair_deadline = public_started + BOOTSTRAP_PUBLIC_REPAIR_DEADLINE_SECONDS
    preflight = bootstrap._bootstrap_preflight_over_ssh(
        ssh_host=ssh_host,
        invocation_id=invocation_id,
        desired=expected_desired_state,
        core_dir=core_dir,
        spool_dir=spool_dir,
        repair=False,
        timeout_seconds=bootstrap._remaining_public_deadline(exact_deadline, action="inspection"),
    )
    preflight_lines = list(preflight.lines)
    receipt_deadline = exact_deadline
    if preflight.action == "repair_required":
        repaired = bootstrap._bootstrap_preflight_over_ssh(
            ssh_host=ssh_host,
            invocation_id=invocation_id,
            desired=expected_desired_state,
            core_dir=core_dir,
            spool_dir=spool_dir,
            repair=True,
            timeout_seconds=bootstrap._remaining_public_deadline(repair_deadline, action="repair"),
        )
        preflight_lines.extend(repaired.lines)
        preflight = repaired
        receipt_deadline = repair_deadline
    preflight_receipt = preflight.receipt
    if preflight_receipt is not None:
        if (relay_wheel is not None or bootstrap._is_clio_relay_git_checkout(source_root)) and (
            bootstrap.bootstrap_relay_identity(
                source_root=source_root,
                relay_wheel=relay_wheel,
                relay_artifact_sha256=relay_artifact_sha256,
            )
            != planned_identity
        ):
            raise ConfigurationError("bootstrap source identity changed during preflight")
        bootstrap_receipt_validation.validate_bootstrap_receipt(
            preflight_receipt,
            bootstrap_profile=bootstrap_profile,
            relay_install_spec=planned_identity.install_spec,
            desired_fingerprint=expected_desired_state.fingerprint,
            expected_jarvis_resource_graph_profile=(
                expected_desired_state.jarvis_resource_graph_profile
            ),
            expected_allow_jarvis_resource_graph_build=(
                expected_desired_state.allow_jarvis_resource_graph_build
            ),
            expected_worker_service=(
                endpoint_user_service_name(cluster) if cluster is not None else None
            ),
        )
        bootstrap._verify_persistent_bootstrap_receipt(
            ssh_host=ssh_host,
            receipt=preflight_receipt,
            timeout_seconds=bootstrap._remaining_public_deadline(
                receipt_deadline,
                action="persistent receipt verification",
            ),
        )
        return [
            *preflight_lines,
            "bootstrap_receipt=$HOME/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_receipt_json="
            + json.dumps(preflight_receipt, sort_keys=True, separators=(",", ":")),
        ]

    if jarvis_resource_graph_profile is None:
        raise ConfigurationError(
            "cluster bootstrap requires an operator-selected "
            "jarvis_resource_graph_profile before payload reconciliation"
        )

    if relay_wheel is not None:
        observed_relay_sha256 = bootstrap._validate_relay_bootstrap_wheel(relay_wheel)
        if relay_artifact_sha256 != observed_relay_sha256:
            raise ConfigurationError("relay bootstrap wheel SHA-256 does not match its pin")
    remote_root = f"/tmp/clio-relay-{invocation_id}"
    remote_archive = f"{remote_root}/clio-relay-head.tar"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        archive = temp_path / "clio-relay-head.tar"
        deployment = bootstrap.create_bootstrap_archive(
            source_root=source_root,
            archive=archive,
            relay_wheel=relay_wheel,
        )
        rebound_identity = bootstrap.bootstrap_relay_identity(
            source_root=source_root,
            relay_wheel=relay_wheel,
            relay_artifact_sha256=relay_artifact_sha256,
        )
        if rebound_identity != planned_identity or (
            deployment.install_spec != planned_identity.transport_install_spec
        ):
            raise ConfigurationError(
                "bootstrap source identity changed between preflight and payload build"
            )
        source_archive_sha256 = bootstrap._sha256_regular_file(deployment.archive)
        install_script = bootstrap.render_linux_user_bootstrap_script(
            frp_version=frp_version,
            cluster=cluster,
            core_dir=core_dir,
            spool_dir=spool_dir,
            agent_adapter=agent_adapter,
            agent_npm_package=agent_npm_package,
            agent_npm_bin=agent_npm_bin,
            agent_args=agent_args or [],
            jarvis_resource_graph_profile=jarvis_resource_graph_profile,
            allow_jarvis_resource_graph_build=allow_jarvis_resource_graph_build,
            relay_install_spec=deployment.install_spec,
            relay_deployment_install_spec=planned_identity.install_spec,
            relay_artifact_sha256=planned_identity.deployment_artifact_sha256,
            relay_source_identity=planned_identity.source_identity,
            invocation_id=invocation_id,
            source_archive=remote_archive,
            source_archive_sha256=source_archive_sha256,
        )
        archive_bytes = deployment.archive.read_bytes()
    # clio-relay#209: mkdir staging, the two scp uploads, the remote install
    # invocation, the receipt-cat verification, and the staging cleanup are
    # now ONE combined ssh dial -- the payloads travel inline on this same
    # stdin pass, never a second connection. The remote script self-cleans
    # its own staging directory via an EXIT trap on any outcome.
    one_pass_script = render_one_pass_cold_bootstrap_script(
        remote_root=remote_root,
        archive_bytes=archive_bytes,
        install_script=install_script,
    )
    result = bootstrap._run(
        ["ssh", ssh_host, "bash", "-s"],
        input_bytes=one_pass_script.encode("utf-8"),
        timeout_seconds=BOOTSTRAP_REMOTE_SCRIPT_TIMEOUT_SECONDS,
    )
    output_lines = result.stdout.splitlines()
    receipt_lines = [
        line.removeprefix("bootstrap_receipt_json=")
        for line in output_lines
        if line.startswith("bootstrap_receipt_json=")
    ]
    if len(receipt_lines) != 1:
        raise RelayError(
            "bootstrap output must contain exactly one current invocation receipt, "
            f"observed {len(receipt_lines)}"
        )
    if len(receipt_lines[0].encode("utf-8")) > 1024 * 1024:
        raise RelayError("bootstrap stdout receipt exceeds the bounded size")
    try:
        raw_receipt = cast(object, json.loads(receipt_lines[0]))
    except json.JSONDecodeError as exc:
        raise RelayError(f"bootstrap receipt was not valid JSON: {exc}") from exc
    if not isinstance(raw_receipt, dict):
        raise RelayError("bootstrap receipt was not a JSON object")
    receipt = cast(dict[str, object], raw_receipt)
    if receipt.get("invocation_id") != invocation_id:
        raise RelayError("bootstrap receipt does not match the completed invocation")
    bootstrap_receipt_validation.validate_bootstrap_receipt(
        receipt,
        bootstrap_profile=bootstrap_profile,
        relay_install_spec=planned_identity.install_spec,
        desired_fingerprint=expected_desired_state.fingerprint,
        expected_jarvis_resource_graph_profile=(
            expected_desired_state.jarvis_resource_graph_profile
        ),
        expected_allow_jarvis_resource_graph_build=(
            expected_desired_state.allow_jarvis_resource_graph_build
        ),
        expected_worker_service=(
            endpoint_user_service_name(cluster) if cluster is not None else None
        ),
    )
    # Folds in what used to be a separate `ssh ... cat bootstrap-receipt.json`
    # dial: the one-pass script already re-read the persistent file itself,
    # in the same session, and framed it on stdout.
    parse_one_pass_persistent_receipt(output_lines, receipt=receipt)
    # Folds in what used to be a separate target-identity probe dial
    # (`cli_remote_worker_probe._remote_target_identity`): the physical
    # identity is observed here, inside the same session that just installed
    # the relay. Returned to the caller as an extra framed line so it can be
    # pinned into the cluster registry without a further dial.
    parse_one_pass_target_identity(output_lines)
    return output_lines
