"""Write the worker conformance config for W2's fixture sites.

Usage: python scripts/ares/fixtures_worker_config.py FIXTURES_ENV RELAY_WHEEL OUT

The worker is placed on fx-slurm over the fixtures' ssh config and links back
through fx-frps: relay reaches frps on the published port, the site on the
fixture network. The site gets ``RELAY_WHEEL`` and the fixtures' own frpc.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip("'\"")
    return values


def worker_config(fx: dict[str, str], relay_wheel: str) -> dict[str, object]:
    return {
        "ssh_profile": "fx-slurm",
        "ssh_config_file": fx["FX_SSH_CONFIG"],
        "scheduler": "slurm",
        "remote_dir": ".clio-relay-ci-worker",
        "relay_wheel": relay_wheel,
        "frpc_binary": fx["FX_FRPC"],
        "rendezvous": {
            "address": fx["FX_CONNECT_ADDR"],
            "port": int(fx["FX_FRPS_PORT"]),
            "protocol": "tcp",
            "site_address": fx["FX_INNER_FRPS_ADDR"],
            "site_port": int(fx["FX_INNER_FRPS_PORT"]),
            "token_file": fx["FX_FRP_TOKEN_FILE"],
        },
    }


def main() -> None:
    env, wheel, out = sys.argv[1:4]
    config = worker_config(read_env(Path(env)), str(Path(wheel).resolve()))
    Path(out).write_text(json.dumps(config, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
