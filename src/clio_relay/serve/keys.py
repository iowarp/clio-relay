"""Reading a compute node's host keys, and a desktop forward that stopped for good.

A service's forward reaches its compute node through the login host, so the
node's keys are read there first (:func:`read_host_keys`). A forward that ssh
refuses the same way on every try (a key that does not match, a node whose job
ended) is not started again: the service says why, as a typed ``service_error``,
and a serve task ends on it rather than waiting for the job to end.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from clio_relay.jobs.handles import ExecResult, JobStatus
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.serve.plan import ServePlan
from clio_relay.serve.runtime_util import HOST_KEYS_SH

if TYPE_CHECKING:
    from clio_relay.serve.forward import Forward

logger = logging.getLogger(__name__)

KEYS_TIMEOUT_S = 20.0
"""One look at the login host's known_hosts files; a hung look is tried again."""
KEYS_TRIES = 2
KEY_ROUNDS = 3
"""Status rounds whose looks all failed before the service reports it cannot be checked."""
JOB_END_TRIES = 4
JOB_END_WAIT_S = 5.0
"""A node refuses ssh as its job ends; the scheduler may say so a few seconds later."""
EXEC_OUTPUT = 65536


async def read_host_keys(adapter: Any, plan: ServePlan) -> list[str]:
    """Read the compute node's host keys from the login host's known_hosts files.

    The forward reaches the node through the login host, and the desktop has
    often never seen that node, so ``BatchMode`` would refuse its key. The site's
    own known_hosts files are read over the login link ssh has already checked.
    Each try is short, so a hung look does not hold the service's readiness;
    raises ``unreachable`` when no try got an answer.
    """
    if plan.mode != "ssh" or not plan.host or plan.host == plan.login_host:
        return []
    run: Any = getattr(adapter, "hop_exec", None) or adapter.exec
    argv = ["sh", "-c", HOST_KEYS_SH, "sh", plan.host]
    problem = "no answer"
    for attempt in range(KEYS_TRIES):
        try:
            raw: Any = await run(argv, timeout_s=KEYS_TIMEOUT_S, max_output_bytes=EXEC_OUTPUT)
            found = cast(ExecResult, raw)
        except Exception as exc:
            problem = f"{type(exc).__name__}: {exc}"
        else:
            if found.exit_code == 0 and not found.timed_out:
                return [
                    line.strip()
                    for line in found.stdout.splitlines()
                    if line.strip() and not line.lstrip().startswith("#") and len(line.split()) >= 3
                ]
            problem = (
                f"timed out after {KEYS_TIMEOUT_S:.0f} s"
                if found.timed_out
                else (found.stderr or found.stdout).strip()[-200:] or f"exit {found.exit_code}"
            )
        if attempt + 1 < KEYS_TRIES:
            await asyncio.sleep(1.0)
    raise Refusal(
        RefusalReason.UNREACHABLE,
        f"reading {plan.host}'s host keys on {plan.site}'s login host: {problem}",
        site=plan.site,
        host=plan.host,
    )


class ForwardCare:
    """The serve runtime's part that reads node keys and handles a forward that stopped."""

    forwards: dict[str, Forward]
    _adapters: dict[str, Any]

    if TYPE_CHECKING:

        def _save(self, plan: ServePlan) -> None: ...
        def _cached(self, operation_id: str) -> ServePlan | None: ...
        def _lock(self, operation_id: str) -> asyncio.Lock: ...
        async def _finish(self, plan: ServePlan, adapter: Any, status: JobStatus) -> JobStatus: ...

    async def _host_keys(self, adapter: Any, plan: ServePlan) -> list[str]:
        return await read_host_keys(adapter, plan)

    async def _keys_ready(self, plan: ServePlan, adapter: Any) -> str | None:
        """Read the keys for ``plan.host`` unless they are read; return why not, else None."""
        if plan.host_keys_for == plan.host:
            return None
        try:
            plan.host_keys = await self._host_keys(adapter, plan)
        except Refusal as refusal:
            plan.host_key_reads += 1
            if plan.host_key_reads >= KEY_ROUNDS:
                plan.forward_error = Refusal(
                    RefusalReason.HOST_KEY_UNVERIFIED,
                    f"relay could not read {plan.host}'s host key on the login host "
                    f"{plan.host_key_reads} times ({refusal.message}), so it cannot check "
                    "the node it would forward to. The job runs on; stop it, or serve again",
                    site=plan.site,
                    host=plan.host,
                ).to_dict()
            self._save(plan)
            logger.warning("service %s: %s", plan.operation_id, refusal.message)
            return refusal.message
        plan.host_keys_for = plan.host
        plan.host_key_reads = 0
        self._save(plan)
        if not plan.host_keys and plan.host and plan.host != plan.login_host:
            logger.info(
                "service %s: the login host lists no key for %s", plan.operation_id, plan.host
            )
        return None

    def _forward_failed(self, plan: ServePlan, status: JobStatus) -> JobStatus:
        error = plan.forward_error or {}
        message = (
            "the job runs on the site, but relay cannot bring the service to this desktop: "
            f"{error.get('message')}"
        )
        return status.model_copy(update={"message": message, "service_error": error})

    async def _keys_blocked(
        self, plan: ServePlan, adapter: Any, status: JobStatus
    ) -> JobStatus | None:
        """Return the status while the node's keys are unread or unreadable, else None."""
        problem = None if plan.forward_error else await self._keys_ready(plan, adapter)
        if plan.forward_error is not None:
            return self._forward_failed(plan, status)
        if problem is None:
            return None
        message = f"the service is up on {plan.host}; reading its host key, retrying: {problem}"
        return status.model_copy(update={"message": message})

    async def _forward_blocked(
        self, plan: ServePlan, adapter: Any, forward: Forward, status: JobStatus
    ) -> JobStatus | None:
        """Return the status when ``forward`` stopped for good, else None."""
        if forward.failure is None:
            return None
        await self._forward_stopped(plan, adapter, forward.failure)
        if plan.forward_error is not None:
            return self._forward_failed(plan, status)
        return status.model_copy(update={"message": "binding the desktop port again"})

    def _failure_callback(self, operation_id: str) -> Callable[[Refusal], None]:
        def failed(_refusal: Refusal) -> None:
            with contextlib.suppress(RuntimeError):
                asyncio.get_running_loop().create_task(self._forward_down(operation_id))

        return failed

    async def _forward_stopped(self, plan: ServePlan, adapter: Any, failure: Refusal) -> None:
        """The forward stopped for good. A refused key is read once more; else keep the reason."""
        forward = self.forwards.pop(plan.operation_id, None)
        if forward is not None:
            await forward.close()
        if failure.reason == RefusalReason.HOST_KEY_UNVERIFIED and not plan.host_keys_reread:
            plan.host_keys_reread = True
            before = list(plan.host_keys)
            with contextlib.suppress(Refusal):
                plan.host_keys = await self._host_keys(adapter, plan)
            if plan.host_keys and plan.host_keys != before:
                self._save(plan)
                return
        plan.forward_error = failure.to_dict()
        self._save(plan)
        logger.warning("service %s: forward stopped: %s", plan.operation_id, failure.message)

    async def _forward_down(self, operation_id: str) -> None:
        """A forward stopped by itself: finish the service if its job ended, else keep why.

        A node that refuses ssh for having no job (``pam_slurm_adopt``) is the job
        ending; the scheduler may say so a few seconds later than the node does.
        """
        plan = self._cached(operation_id)
        adapter = self._adapters.get(operation_id)
        forward = self.forwards.get(operation_id)
        if plan is None or adapter is None or forward is None or forward.failure is None:
            return
        failure = forward.failure
        try:
            if plan.handle is not None and failure.data.get("job_ended"):
                for attempt in range(JOB_END_TRIES):
                    status: JobStatus = await adapter.status(plan.handle)
                    if status.state.final:
                        async with self._lock(operation_id):
                            if plan.final is None:
                                plan.forward_error = None
                                await self._finish(plan, adapter, status)
                        return
                    if attempt + 1 < JOB_END_TRIES:
                        await asyncio.sleep(JOB_END_WAIT_S)
            async with self._lock(operation_id):
                if plan.final is None and self.forwards.get(operation_id) is forward:
                    await self._forward_stopped(plan, adapter, failure)
        except Exception:
            logger.warning("service %s: after its forward stopped", operation_id, exc_info=True)
