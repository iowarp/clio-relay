"""The adapter conformance kit (RL7).

Every adapter runs this suite. Subclass :class:`AdapterConformance` in a test
module and provide a ``target`` fixture that returns a
:class:`ConformanceTarget`::

    class TestMyAdapter(AdapterConformance):
        @pytest.fixture
        def target(self, tmp_path: Path) -> ConformanceTarget:
            return ConformanceTarget(adapter="mine", config={...}, state_dir=tmp_path)

The suite checks the shape of ``describe``, typed refusals for the verbs the
adapter does not implement, bounded ``exec`` output, a file round trip, the job
lifecycle, a failing job, cancel, staged inputs, and observing a job from a
fresh adapter instance, which is what a relay restart looks like. Install the
``testing`` extra for pytest and pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters import registry
from clio_relay.adapters.base import VERBS, AdapterContext, BaseAdapter
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobSpec

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


def _implements(target: ConformanceTarget, verb: str) -> bool:
    return verb in registry.load(target.adapter).implements


class AdapterConformance:
    """The suite. Subclasses provide the ``target`` fixture."""

    @pytest.fixture
    def target(self) -> ConformanceTarget:
        raise NotImplementedError("provide a target fixture")

    def test_config_schema(self, target: ConformanceTarget) -> None:
        schema = registry.config_schema(target.adapter)
        assert schema.get("type") == "object"
        registry.validate_config(target.adapter, target.config)
        with pytest.raises(Refusal) as refused:
            registry.validate_config(target.adapter, {"no_such_setting_xyz": 1})
        assert refused.value.reason == RefusalReason.INVALID_CONFIG

    @pytest.mark.asyncio
    async def test_describe_shape(self, target: ConformanceTarget) -> None:
        adapter = target.build()
        info = await adapter.describe()
        assert info.adapter == target.adapter
        assert "describe" in info.verbs
        assert set(info.verbs) == set(type(adapter).implements)
        assert set(info.verbs) <= set(VERBS)
        info.model_dump_json()

    @pytest.mark.asyncio
    async def test_unimplemented_verbs_refuse(self, target: ConformanceTarget) -> None:
        adapter = target.build()
        missing = set(VERBS) - set(type(adapter).implements)
        calls = {
            "run": lambda: adapter.run(target.ok_job, operation_id=new_id("op")),
            "exec": lambda: adapter.exec(target.echo, timeout_s=10, max_output_bytes=1024),
            "files": lambda: adapter.files(),
        }
        for verb in sorted(missing & set(calls)):
            with pytest.raises(Refusal) as refused:
                await calls[verb]()
            assert refused.value.reason == RefusalReason.VERB_NOT_IMPLEMENTED, verb
        if "serve" in missing:
            from clio_relay.jobs.spec import ServicePort, ServiceSpec

            spec = ServiceSpec(job=target.ok_job, ports=[ServicePort(name="main", port=8000)])
            with pytest.raises(Refusal) as refused:
                await adapter.serve(spec, operation_id=new_id("op"))
            assert refused.value.reason == RefusalReason.VERB_NOT_IMPLEMENTED

    @pytest.mark.asyncio
    async def test_exec_bounded(self, target: ConformanceTarget) -> None:
        if not _implements(target, "exec"):
            pytest.skip("exec is not implemented; its refusal is checked separately")
        adapter = target.build()
        result = await adapter.exec(target.echo, timeout_s=30, max_output_bytes=4096)
        assert result.exit_code == 0, result
        assert target.ok_marker in result.stdout
        flood = await adapter.exec(target.flood, timeout_s=10, max_output_bytes=1000)
        assert flood.stdout_truncated
        assert len(flood.stdout.encode()) <= 1000
        slow = await adapter.exec(target.slow, timeout_s=1, max_output_bytes=1000)
        assert slow.timed_out
        assert slow.exit_code is None
        assert slow.duration_s < 20

    @pytest.mark.asyncio
    async def test_files_round_trip(self, target: ConformanceTarget) -> None:
        if not _implements(target, "files"):
            pytest.skip("files is not implemented; its refusal is checked separately")
        files = await target.build().files()
        payload = b"relay conformance \x00\x01 payload\n" * 64
        path = f"{target.files_dir}/{new_id('f')}.bin"
        written = await files.write(path, payload)
        assert written.size == len(payload)
        assert await files.read(path) == payload
        assert await files.read(path, offset=10, limit=5) == payload[10:15]
        assert (await files.stat(path)).size == len(payload)
        listing = await files.list(target.files_dir)
        assert any(entry.path.endswith(Path(path).name) for entry in listing)
        with pytest.raises(Refusal) as exists:
            await files.write(path, b"x")
        assert exists.value.reason == RefusalReason.PATH_EXISTS
        await files.write(path, b"replaced", overwrite=True)
        assert await files.read(path) == b"replaced"
        with pytest.raises(Refusal) as missing:
            await files.read(f"{target.files_dir}/does-not-exist-{new_id('f')}")
        assert missing.value.reason == RefusalReason.PATH_NOT_FOUND
        digest = await files.stage(payload)
        assert digest == hashlib.sha256(payload).hexdigest()
        assert await files.has(digest)
        assert await files.stage(payload) == digest

    @pytest.mark.asyncio
    async def test_job_lifecycle(self, target: ConformanceTarget) -> None:
        if not _implements(target, "run"):
            pytest.skip("run is not implemented; its refusal is checked separately")
        adapter = target.build()
        handle = await adapter.run(target.ok_job, operation_id=new_id("op"))
        assert handle.adapter == target.adapter
        JobHandle.model_validate_json(handle.model_dump_json())
        status, seen = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert_lifecycle(seen)
        assert status.state == JobState.COMPLETED, status
        assert status.exit_code == 0
        assert status.outputs, "a finished job lists its outputs"

    @pytest.mark.asyncio
    async def test_job_failure(self, target: ConformanceTarget) -> None:
        if not _implements(target, "run"):
            pytest.skip("run is not implemented")
        adapter = target.build()
        handle = await adapter.run(target.fail_job, operation_id=new_id("op"))
        status, seen = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert_lifecycle(seen)
        assert status.state == JobState.FAILED, status
        assert status.exit_code not in (None, 0)

    @pytest.mark.asyncio
    async def test_job_cancel(self, target: ConformanceTarget) -> None:
        if not _implements(target, "run"):
            pytest.skip("run is not implemented")
        adapter = target.build()
        handle = await adapter.run(target.long_job, operation_id=new_id("op"))
        first = await adapter.status(handle)
        assert not first.state.final, first
        cancelled = await adapter.cancel(handle)
        if not cancelled.state.final:
            cancelled, _ = await wait_final(adapter, handle, 60, target.poll_s)
        assert cancelled.state == JobState.CANCELLED, cancelled
        again = await target.build().status(handle)
        assert again.state == JobState.CANCELLED

    @pytest.mark.asyncio
    async def test_job_observed_after_restart(self, target: ConformanceTarget) -> None:
        if not _implements(target, "run"):
            pytest.skip("run is not implemented")
        operation_id = new_id("op")
        handle = await target.build().run(target.ok_job, operation_id=operation_id)
        stored = handle.model_dump_json()
        restarted = target.build()
        status, seen = await wait_final(
            restarted, JobHandle.model_validate_json(stored), target.job_timeout_s, target.poll_s
        )
        assert_lifecycle(seen)
        assert status.state == JobState.COMPLETED
        found = await target.build().find(operation_id)
        assert found is not None, "find() reconciles a job whose handle was not recorded"
        assert found.native_id == handle.native_id

    @pytest.mark.asyncio
    async def test_inputs_by_digest(self, target: ConformanceTarget) -> None:
        if not _implements(target, "run"):
            pytest.skip("run is not implemented")
        adapter = target.build()
        payload = f"input {new_id('x')}\n".encode()
        digest = target.context().content.put(payload)
        spec: JobSpec = target.cat_job("input.txt")
        spec = spec.model_copy(update={"inputs": [InputRef(name="input.txt", sha256=digest)]})
        handle = await adapter.run(spec, operation_id=new_id("op"))
        status, _ = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert status.state == JobState.COMPLETED, status
        missing = spec.model_copy(
            update={"inputs": [InputRef(name="input.txt", sha256="0" * 64)]}
        )
        with pytest.raises(Refusal) as refused:
            await adapter.run(missing, operation_id=new_id("op"))
        assert refused.value.reason == RefusalReason.INPUT_MISSING


__all__ = [
    "AdapterConformance",
    "ConformanceTarget",
    "assert_lifecycle",
    "wait_final",
]
