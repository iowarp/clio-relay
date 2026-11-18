"""Run the Tapis stub on its own: ``python -m tests.fixtures.tapis_stub --port 8765``."""

from __future__ import annotations

import argparse

import uvicorn

from .app import StubState, create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the Tapis stub.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    uvicorn.run(create_app(StubState()), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
