"""What the conformance kit runs against, and the helpers its tests share."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext, BaseAdapter
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import new_id
from clio_relay.jobs.spec import JobSpec

_ORDER = {
    JobState.QUEUED: 0,
    JobState.RUNNING: 1,
    JobState.COMPLETED: 2,
    JobState.FAILED: 2,
    JobState.CANCELLED: 2,
}


def _default_ok() -> JobSpec:
    return JobSpec(executable="echo", arguments=["relay-conformance"])


def _default_fail() -> JobSpec:
    return JobSpec(executable="false")


def _default_long() -> JobSpec:
    return JobSpec(executable="sleep", arguments=["300"])


def _default_cat(name: str) -> JobSpec:
    return JobSpec(executable="cat", arguments=[name])


@dataclass
class ConformanceTarget:
    """How to build the adapter under test, and the jobs and commands to try."""

    adapter: str
    config: dict[str, Any]
    state_dir: Path
    site_id: str = "conformance"
    ok_job: JobSpec = field(default_factory=_default_ok)
    ok_marker: str = "relay-conformance"
    fail_job: JobSpec = field(default_factory=_default_fail)
    long_job: JobSpec = field(default_factory=_default_long)
    cat_job: Any = _default_cat
    echo: list[str] = field(default_factory=lambda: ["echo", "relay-conformance"])
    flood: list[str] = field(default_factory=lambda: ["yes"])
    slow: list[str] = field(default_factory=lambda: ["sleep", "30"])
    job_timeout_s: float = 120.0
    poll_s: float = 0.5
    files_dir: str = "conformance-files"

    def context(self) -> AdapterContext:
        """Return the context every instance shares, as one relay host would."""
        return AdapterContext(
            site_id=self.site_id,
            state_dir=self.state_dir / "adapter",
            content=ContentStore(self.state_dir / "content"),
        )

    def build(self) -> BaseAdapter[Any]:
        """Build a fresh adapter instance."""
        return registry.build(self.adapter, self.config, self.context())


async def wait_final(
    adapter: BaseAdapter[Any], handle: JobHandle, timeout_s: float, poll_s: float
) -> tuple[JobStatus, list[JobState]]:
    """Poll until the job is final; return the last status and the states seen."""
    seen: list[JobState] = []
    deadline = time.monotonic() + timeout_s
    while True:
        status = await adapter.status(handle)
        if not seen or seen[-1] != status.state:
            seen.append(status.state)
        if status.state.final:
            return status, seen
        if time.monotonic() > deadline:
            raise AssertionError(f"job did not finish in {timeout_s}s; states seen {seen}")
        await asyncio.sleep(poll_s)


def assert_lifecycle(seen: list[JobState]) -> None:
    """States only move forward, and exactly one final state ends the list."""
    ranks = [_ORDER[state] for state in seen]
    assert ranks == sorted(ranks), f"states went backwards: {seen}"
    assert seen[-1].final, f"not final: {seen}"
    assert sum(1 for state in seen if state.final) == 1, f"more than one final state: {seen}"


async def call_verb(adapter: BaseAdapter[Any], target: ConformanceTarget, verb: str) -> Any:
    """Call ``verb`` once with the target's simplest arguments."""
    if verb == "run":
        return await adapter.run(target.ok_job, operation_id=new_id("op"))
    if verb == "exec":
        return await adapter.exec(target.echo, timeout_s=10, max_output_bytes=1024)
    if verb == "files":
        return await adapter.files()
    from clio_relay.jobs.spec import ServicePort, ServiceSpec

    spec = ServiceSpec(job=target.ok_job, ports=[ServicePort(name="main", port=8000)])
    return await adapter.serve(spec, operation_id=new_id("op"))
