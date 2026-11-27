"""One relay process holds each service's desktop forward.

Several relay processes can share one state directory: one a host prestarted and
the one its session started, or a CLI. Each keeps its own copy of a service's
plan. If each ran the forward, two watchers would bind the same service, and the
copy of one that never saw the other's port move would write the old port back,
so the next relay process binds a port the service's views no longer use.

So the process that runs a forward holds a lock beside it (``owner.lock`` in the
forward's directory) for as long as it watches it. Every other process leaves the
forward to it: it reads the desktop ports, the rebind count and the forward's pid
from the stored plan, and a plan it writes keeps them. The operating system drops
the lock when the owner exits, and the next process to look takes the forward
over, adopting the ports the forward still holds.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from clio_relay.serve.forward import Forward, forward_marker
from clio_relay.serve.plan import ServePlan
from clio_relay.serve.runtime_util import maybe

if TYPE_CHECKING:
    from collections.abc import Callable

    from clio_relay.jobs.refusals import Refusal
    from clio_relay.serve.store import PlanStore


def keep_forward_fields(plan: ServePlan, stored: ServePlan | None) -> None:
    """Take the forward owner's desktop ports, rebind count and pid from ``stored``."""
    if stored is None:
        return
    ports = {port.name: port.local_port for port in stored.ports}
    for port in plan.ports:
        port.local_port = ports.get(port.name, port.local_port)
    plan.rebinds = stored.rebinds
    plan.forward_pid = stored.forward_pid


class ForwardOwning:
    """The serve runtime's part that keeps one owner per forward and its plan current."""

    state_dir: Path
    store: PlanStore
    plans: dict[str, ServePlan]
    forwards: dict[str, Forward]

    if TYPE_CHECKING:

        def _failure_callback(self, operation_id: str) -> Callable[[Refusal], None]: ...

    def _owns_forward(self, operation_id: str) -> bool:
        forward = self.forwards.get(operation_id)
        return forward is not None and forward.lock.held()

    def _cached(self, operation_id: str) -> ServePlan | None:
        plan = self.plans.get(operation_id)
        if plan is None:
            plan = self.store.get(operation_id)
            if plan is not None:
                self.plans[operation_id] = plan
        elif not self._owns_forward(operation_id):
            keep_forward_fields(plan, self.store.get(operation_id))
        return plan

    def _save(self, plan: ServePlan) -> None:
        self.plans[plan.operation_id] = plan
        if not self._owns_forward(plan.operation_id):
            keep_forward_fields(plan, self.store.get(plan.operation_id))
        self.store.put(plan)

    async def _ensure_forward(self, plan: ServePlan, adapter: Any) -> Forward:
        forward = self.forwards.get(plan.operation_id)
        if forward is None:

            def restarted(count: int, pid: int | None) -> None:
                plan.rebinds = count
                plan.forward_pid = pid
                self._save(plan)

            directory = self.state_dir / "serve" / "forwards" / plan.operation_id
            failed = self._failure_callback(plan.operation_id)
            forward = Forward(plan, directory, restarted, on_failure=failed)
            self.forwards[plan.operation_id] = forward
        if forward.task is None:
            keep_forward_fields(plan, self.store.get(plan.operation_id))
            forward.marker = forward_marker(plan, forward.directory)
        if adapter is not None and plan.mode == "ssh":
            transport = getattr(adapter, "transport", None)
            environment = getattr(transport, "environment", None)
            if callable(environment):
                forward.ssh_env = dict(await maybe(environment()) or {})
        await forward.start()
        return forward
