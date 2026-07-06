"""Live frpc connectivity checks."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

from clio_relay.errors import ConfigurationError
from clio_relay.relay_host import FrpcConfig, render_frpc_config


def run_frpc_connection_check(
    *,
    frpc_bin: str,
    config: FrpcConfig,
    timeout_seconds: float = 10.0,
) -> list[str]:
    """Run frpc briefly and return status lines once login succeeds."""
    with tempfile.TemporaryDirectory(prefix="clio-relay-frpc-") as temp_dir:
        config_path = Path(temp_dir) / "frpc.toml"
        config_path.write_text(render_frpc_config(config), encoding="utf-8")
        try:
            result = subprocess.run(
                [frpc_bin, "-c", str(config_path)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            output = _decode_timeout_output(exc.stdout)
            return ["frpc stayed connected until timeout", *output.splitlines()]
        if result.returncode == 0:
            return ["frpc exited cleanly", *result.stdout.splitlines()]
        raise ConfigurationError(
            f"frpc exited before timeout with code {result.returncode}: "
            + "\n".join(result.stdout.splitlines()[-12:])
        )


def _decode_timeout_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value
