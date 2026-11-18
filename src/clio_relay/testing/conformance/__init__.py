"""The adapter conformance kit (RL7).

Every adapter runs this suite. Subclass :class:`AdapterConformance` in a test
module and provide a ``target`` fixture that returns a
:class:`ConformanceTarget`::

    class TestMyAdapter(AdapterConformance):
        @pytest.fixture
        def target(self, tmp_path: Path) -> ConformanceTarget:
            return ConformanceTarget(adapter="mine", config={...}, state_dir=tmp_path)

The suite checks the shape of ``describe``, typed refusals for the verbs the
adapter does not implement, bounded ``exec`` output, the files semantics of
docs/files.md, the job lifecycle, a failing job, cancel, staged inputs,
observing a job from a fresh adapter instance (which is what a relay restart
looks like), typed errors, cancelling a finished job and an ``exec`` call, and
open and guarded sites. Install the ``testing`` extra for pytest and
pytest-asyncio.
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
from clio_relay.jobs.file_rules import PART_SUFFIX, part_path
from clio_relay.jobs.handles import FileAccess, JobHandle, JobState, JobStatus
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


async def _call(adapter: BaseAdapter[Any], target: ConformanceTarget, verb: str) -> Any:
    if verb == "run":
        return await adapter.run(target.ok_job, operation_id=new_id("op"))
    if verb == "exec":
        return await adapter.exec(target.echo, timeout_s=10, max_output_bytes=1024)
    if verb == "files":
        return await adapter.files()
    from clio_relay.jobs.spec import ServicePort, ServiceSpec

    spec = ServiceSpec(job=target.ok_job, ports=[ServicePort(name="main", port=8000)])
    return await adapter.serve(spec, operation_id=new_id("op"))


async def _refuses(target: ConformanceTarget, verb: str) -> bool:
    """Check the typed refusal of a verb the adapter leaves out; True if it does."""
    if _implements(target, verb):
        return False
    with pytest.raises(Refusal) as refused:
        await _call(target.build(), target, verb)
    assert refused.value.reason == RefusalReason.VERB_NOT_IMPLEMENTED, verb
    return True


async def _deletes(files: FileAccess, path: str) -> bool:
    """Delete ``path``; False if the site cannot delete yet."""
    try:
        await files.delete(path)
    except Refusal as refusal:
        assert refusal.reason == RefusalReason.VERB_NOT_IMPLEMENTED, refusal
        return False
    return True


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
        for verb in ("run", "exec", "files", "serve"):
            await _refuses(target, verb)

    @pytest.mark.asyncio
    async def test_exec_bounded(self, target: ConformanceTarget) -> None:
        if await _refuses(target, "exec"):
            return
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
    async def test_exec_without_time_limit(self, target: ConformanceTarget) -> None:
        """No hard limit on agent actions: exec with no timeout runs to the end."""
        if await _refuses(target, "exec"):
            return
        result = await target.build().exec(target.echo, max_output_bytes=4096)
        assert result.exit_code == 0, result
        assert not result.timed_out
        assert target.ok_marker in result.stdout

    @pytest.mark.asyncio
    async def test_files_round_trip(self, target: ConformanceTarget) -> None:
        if await _refuses(target, "files"):
            return
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
    async def test_files_delete(self, target: ConformanceTarget) -> None:
        """docs/files.md: delete refuses a missing path, needs recursive for a
        directory that is not empty, and never removes relay's work directory."""
        if await _refuses(target, "files"):
            return
        files = await target.build().files()
        folder = f"{target.files_dir}/{new_id('d')}"
        await files.write(f"{folder}/inner/a.txt", b"a")
        if not await _deletes(files, f"{folder}/inner/a.txt"):
            return
        with pytest.raises(Refusal) as gone:
            await files.stat(f"{folder}/inner/a.txt")
        assert gone.value.reason == RefusalReason.PATH_NOT_FOUND
        with pytest.raises(Refusal) as missing:
            await files.delete(f"{folder}/inner/a.txt")
        assert missing.value.reason == RefusalReason.PATH_NOT_FOUND
        await files.delete(f"{folder}/inner")
        await files.write(f"{folder}/b.txt", b"b")
        with pytest.raises(Refusal) as full:
            await files.delete(folder)
        assert full.value.reason == RefusalReason.INVALID_ARGUMENTS
        await files.delete(folder, recursive=True)
        with pytest.raises(Refusal) as removed:
            await files.stat(folder)
        assert removed.value.reason == RefusalReason.PATH_NOT_FOUND
        for root in (".", "/"):
            with pytest.raises(Refusal) as kept:
                await files.delete(root, recursive=True)
            assert kept.value.reason == RefusalReason.INVALID_ARGUMENTS, root

    @pytest.mark.asyncio
    async def test_files_chunked_write(self, target: ConformanceTarget) -> None:
        """docs/files.md: chunks collect in <path>.relay-part, a wrong offset says
        where to resume, and the final chunk moves the file into place."""
        if await _refuses(target, "files"):
            return
        files = await target.build().files()
        path = f"{target.files_dir}/{new_id('c')}.bin"
        first, second, third = b"a" * 1000, b"b" * 700, b"c" * 300
        try:
            part = await files.write(path, first, final=False)
        except Refusal as refusal:
            assert refusal.reason == RefusalReason.VERB_NOT_IMPLEMENTED, refusal
            return
        assert part.path.endswith(PART_SUFFIX)
        assert part.size == len(first)
        with pytest.raises(Refusal) as not_yet:
            await files.stat(path)
        assert not_yet.value.reason == RefusalReason.PATH_NOT_FOUND
        # The link drops and the client resumes from the part's size.
        assert (await files.stat(part_path(path))).size == len(first)
        with pytest.raises(Refusal) as wrong:
            await files.write(path, second, offset=len(first) + 5, final=False)
        assert wrong.value.reason == RefusalReason.INVALID_ARGUMENTS
        assert wrong.value.data.get("resume_at") == len(first)
        await files.write(path, second, offset=len(first), final=False)
        done = await files.write(path, third, offset=len(first) + len(second))
        assert done.size == len(first) + len(second) + len(third)
        assert await files.read(path) == first + second + third
        with pytest.raises(Refusal) as part_gone:
            await files.stat(part_path(path))
        assert part_gone.value.reason == RefusalReason.PATH_NOT_FOUND
        with pytest.raises(Refusal) as exists:
            await files.write(path, first, final=False)
        assert exists.value.reason == RefusalReason.PATH_EXISTS
        await files.write(path, b"new ", overwrite=True, final=False)
        await files.write(path, b"bytes", overwrite=True, offset=4)
        assert await files.read(path) == b"new bytes"

    @pytest.mark.asyncio
    async def test_files_relative_paths_stay_inside(self, target: ConformanceTarget) -> None:
        """docs/files.md: a relative path may not leave relay's work directory."""
        if await _refuses(target, "files"):
            return
        files = await target.build().files()
        with pytest.raises(Refusal) as escaped:
            await files.write("../relay-escape.txt", b"x")
        assert escaped.value.reason == RefusalReason.INVALID_ARGUMENTS

    @pytest.mark.asyncio
    async def test_job_lifecycle(self, target: ConformanceTarget) -> None:
        if await _refuses(target, "run"):
            return
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
        if await _refuses(target, "run"):
            return
        adapter = target.build()
        handle = await adapter.run(target.fail_job, operation_id=new_id("op"))
        status, seen = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert_lifecycle(seen)
        assert status.state == JobState.FAILED, status
        assert status.exit_code not in (None, 0)

    @pytest.mark.asyncio
    async def test_job_cancel(self, target: ConformanceTarget) -> None:
        if await _refuses(target, "run"):
            return
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
        if await _refuses(target, "run"):
            return
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
        if await _refuses(target, "run"):
            return
        adapter = target.build()
        payload = f"input {new_id('x')}\n".encode()
        digest = target.context().content.put(payload)
        spec: JobSpec = target.cat_job("input.txt")
        spec = spec.model_copy(update={"inputs": [InputRef(name="input.txt", sha256=digest)]})
        handle = await adapter.run(spec, operation_id=new_id("op"))
        status, _ = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert status.state == JobState.COMPLETED, status
        missing = spec.model_copy(update={"inputs": [InputRef(name="input.txt", sha256="0" * 64)]})
        with pytest.raises(Refusal) as refused:
            await adapter.run(missing, operation_id=new_id("op"))
        assert refused.value.reason == RefusalReason.INPUT_MISSING

    @pytest.mark.asyncio
    async def test_errors_are_typed(self, target: ConformanceTarget) -> None:
        """A missing path is path_not_found; a missing program fails, it does not hang."""
        if not await _refuses(target, "files"):
            files = await target.build().files()
            with pytest.raises(Refusal) as missing:
                await files.list(f"{target.files_dir}/no-such-dir-{new_id('d')}")
            assert missing.value.reason == RefusalReason.PATH_NOT_FOUND
        if not await _refuses(target, "exec"):
            program = [f"relay-no-such-program-{new_id('p')}"]
            try:
                result = await target.build().exec(program, timeout_s=30, max_output_bytes=1000)
            except Refusal as refusal:
                assert refusal.reason in (
                    RefusalReason.INVALID_ARGUMENTS,
                    RefusalReason.INVALID_SPEC,
                ), refusal
            else:
                assert result.exit_code not in (None, 0), result

    @pytest.mark.asyncio
    async def test_cancel_after_finish(self, target: ConformanceTarget) -> None:
        """Cancelling a finished job changes nothing: its final state stands."""
        if await _refuses(target, "run"):
            return
        adapter = target.build()
        handle = await adapter.run(target.ok_job, operation_id=new_id("op"))
        status, _ = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert status.state == JobState.COMPLETED, status
        assert (await adapter.cancel(handle)).state == JobState.COMPLETED
        assert (await target.build().status(handle)).state == JobState.COMPLETED

    @pytest.mark.asyncio
    async def test_exec_cancelled(self, target: ConformanceTarget) -> None:
        """Cancelling an exec call with no time limit stops it promptly."""
        if await _refuses(target, "exec"):
            return
        adapter = target.build()
        call = asyncio.create_task(adapter.exec(target.slow, max_output_bytes=1000))
        await asyncio.sleep(1)
        assert not call.done(), "the slow command ended on its own"
        started = time.monotonic()
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(call, 15)
        assert time.monotonic() - started < 15

    @pytest.mark.asyncio
    async def test_open_and_guarded(self, target: ConformanceTarget) -> None:
        """The guard sits in front of the adapter: an open site runs what the agent
        asks; a guarded one refuses commands and anything but read-only exec."""
        from clio_relay.adapters.hooks import Ask, HookContext
        from clio_relay.guard import Guard
        from clio_relay.jobs.records import RecordStore
        from clio_relay.sites.models import SiteEntry

        state = target.state_dir / "guard-state"
        guard = Guard(HookContext(state_dir=state, asks=RecordStore(state / "asks", Ask)))
        adapter = target.build()

        def site(policy: str) -> SiteEntry:
            return SiteEntry.model_validate(
                {"id": target.site_id, "adapter": target.adapter, "policy": policy}
            )

        opened, guarded = site("open"), site("guarded")
        assert (await guard.check_run(opened, target.ok_job)).allowed
        assert (await guard.check_exec(opened, ["rm", "-rf", "x"])).allowed
        assert await guard.prepare(opened, target.ok_job, adapter=adapter) == target.ok_job
        refused = await guard.check_run(guarded, target.ok_job)
        assert not refused.allowed
        assert refused.reason == RefusalReason.POLICY_REFUSED
        assert not (await guard.check_exec(guarded, ["rm", "-rf", "x"])).allowed
        assert (await guard.check_exec(guarded, ["ls"])).allowed
        with pytest.raises(Refusal) as kept_out:
            await guard.prepare(guarded, target.ok_job, adapter=adapter)
        assert kept_out.value.reason == RefusalReason.POLICY_REFUSED


__all__ = [
    "AdapterConformance",
    "ConformanceTarget",
    "assert_lifecycle",
    "wait_final",
]
