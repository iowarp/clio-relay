"""The Tapis stub (W10). See :mod:`tests.fixtures.tapis_stub.app`.

``TapisStub`` serves the stub with uvicorn on a free loopback port in a thread::

    with TapisStub() as stub:
        stub.base_url          # http://127.0.0.1:<port>
        stub.state.approve(user_code, "alice")

It can also run on its own: ``python -m tests.fixtures.tapis_stub --port 0``.
"""

from __future__ import annotations

import socket
import threading
import time
from types import TracebackType
from typing import Any

import uvicorn

from .app import StubState, create_app


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class TapisStub:
    """Run the stub in a background thread."""

    def __init__(self, state: StubState | None = None, port: int | None = None) -> None:
        self.state = state or StubState()
        self.port = port or free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(
            create_app(self.state),
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
            access_log=False,
            lifespan="off",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self) -> TapisStub:
        self._thread.start()
        while not self._server.started:
            if not self._thread.is_alive():
                raise RuntimeError("the Tapis stub did not start")
            time.sleep(0.02)
        return self

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join()

    def seed_system(self, system_id: str, **fields: Any) -> dict[str, Any]:
        """Add a system as if it were registered already, with a working credential."""
        system: dict[str, Any] = {
            "id": system_id,
            "systemType": "LINUX",
            "host": "localhost",
            "port": 22,
            "rootDir": "/",
            "defaultAuthnMethod": "PKI_KEYS",
            "canExec": True,
            "canRunBatch": False,
            "batchScheduler": None,
            "jobRuntimes": [{"runtimeType": "ZIP"}],
        } | fields
        with self.state.lock:
            self.state.systems[system_id] = system
        return system

    def seed_credential(self, system_id: str, username: str) -> None:
        with self.state.lock:
            self.state.credentials[(system_id, username)] = {"loginUser": username, "tms": False}

    def __enter__(self) -> TapisStub:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()


__all__ = ["StubState", "TapisStub", "create_app", "free_port"]
