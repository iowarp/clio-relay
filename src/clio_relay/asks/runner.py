"""A private event loop on its own thread, for the page and the sign-ins behind it.

The page is served by a threaded loopback server over stdio and by the MCP
server's own loop over HTTP. Sign-in work (polling a device flow, running an
ssh sign-in) must outlive any one request, so it runs here, where both servers
can reach it.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Coroutine
from typing import Any


class Runner:
    """Runs coroutines on one background loop, started on first use."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._tasks: set[asyncio.Task[Any]] = set()

    def loop(self) -> asyncio.AbstractEventLoop:
        """Return the loop, starting it if needed."""
        with self._lock:
            if self._loop is None:
                loop = asyncio.new_event_loop()
                ready = threading.Event()

                def run() -> None:
                    asyncio.set_event_loop(loop)
                    loop.call_soon(ready.set)
                    loop.run_forever()

                self._thread = threading.Thread(target=run, name="relay-asks", daemon=True)
                self._thread.start()
                ready.wait()
                self._loop = loop
            return self._loop

    def on_loop(self) -> bool:
        """Return whether the caller is already on this loop."""
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def submit[T](self, coro: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        """Run ``coro`` on the loop and return its future."""
        return asyncio.run_coroutine_threadsafe(coro, self.loop())

    async def call[T](self, coro: Coroutine[Any, Any, T]) -> T:
        """Await ``coro`` on the loop from any other loop."""
        if self.on_loop():
            return await coro
        return await asyncio.wrap_future(self.submit(coro))

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Start a background task. Call this on the loop."""
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def close(self) -> None:
        """Cancel what is running and stop the loop."""
        with self._lock:
            loop, self._loop = self._loop, None
            thread, self._thread = self._thread, None
        if loop is None:
            return

        async def stop() -> None:
            for task in list(self._tasks):
                task.cancel()
            loop.stop()

        loop.call_soon_threadsafe(lambda: loop.create_task(stop()))
        if thread is not None:
            thread.join(timeout=5)
        if not loop.is_running():
            loop.close()
