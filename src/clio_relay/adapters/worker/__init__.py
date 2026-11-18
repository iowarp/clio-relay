"""The ``worker`` adapter and the relay worker it places on a site (RL11).

Everything the original relay was lives in this package: bootstrap, the queue,
leases, the one-link connection and the operator commands under
``clio-relay worker``. The adapter itself is :class:`WorkerAdapter`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clio_relay.adapters.worker.adapter import WorkerAdapter, WorkerConfig

__all__ = ["WorkerAdapter", "WorkerConfig"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from clio_relay.adapters.worker import adapter

        return getattr(adapter, name)
    raise AttributeError(name)
