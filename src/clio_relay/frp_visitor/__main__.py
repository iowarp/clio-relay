"""``python -m clio_relay.frp_visitor -c visitor.toml``: run relay's frp visitor."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from clio_relay.frp_visitor.config import ConfigError, load
from clio_relay.frp_visitor.visitor import log, run


def main(argv: list[str] | None = None) -> int:
    """Run the visitor file named by ``-c`` until the process is stopped."""
    parser = argparse.ArgumentParser(
        prog="python -m clio_relay.frp_visitor",
        description="Relay's frp visitor: the desktop end of an frp path.",
    )
    parser.add_argument("-c", "--config", required=True, help="the frpc TOML file to run")
    args = parser.parse_args(argv)
    try:
        config = load(Path(args.config), os.environ)
    except ConfigError as exc:
        log("E", f"relay's frp visitor cannot use {args.config}: {exc}")
        return 2
    try:
        return asyncio.run(run(config, args.config))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
