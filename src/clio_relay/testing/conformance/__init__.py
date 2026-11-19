"""The adapter conformance kit.

Every adapter runs this suite. Subclass :class:`AdapterConformance` in a test
module, set ``adapter`` to your adapter class, and provide a ``target`` fixture
that returns a :class:`ConformanceTarget`::

    class TestMyAdapter(AdapterConformance):
        adapter = MyAdapter

        @pytest.fixture
        def target(self, tmp_path: Path) -> ConformanceTarget:
            return ConformanceTarget(adapter="mine", config={...}, state_dir=tmp_path)

The suite checks the shape of ``describe``, bounded ``exec`` output, the files
semantics of docs/files.md, the job lifecycle, a failing job, cancel, staged
inputs, observing a job from a fresh adapter instance (which is what a relay
restart looks like), typed errors, cancelling a finished job and an ``exec``
call, and open and guarded sites.

The kit collects tests only for the verbs in the adapter's ``implements``. For
each verb the adapter leaves out it collects one test instead,
``test_<verb>_is_left_out``, which checks that the verb refuses with
``verb_not_implemented``. So ``pytest -v`` names exactly what was tested.
Install the ``testing`` extra for pytest and pytest-asyncio;
:func:`use_memory_keyring` gives a test the secrets the adapter reads.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

import pytest

from clio_relay.adapters import registry
from clio_relay.adapters.base import VERBS, BaseAdapter
from clio_relay.jobs.file_rules import PART_SUFFIX, part_path
from clio_relay.jobs.handles import JobHandle, JobState
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobSpec
from clio_relay.testing.conformance.target import (
    ConformanceTarget,
    assert_lifecycle,
    call_verb,
    wait_final,
)

__all__ = [
    "AdapterConformance",
    "ConformanceTarget",
    "assert_lifecycle",
    "use_memory_keyring",
    "wait_final",
]

#: The verbs an adapter may leave out; ``describe`` is always there.
_OPTIONAL_VERBS = ("run", "exec", "files", "serve")

#: Kit test name to the verbs it exercises. A test is collected when the
#: adapter implements any of them.
_NEEDS: dict[str, frozenset[str]] = {}


def _needs[T: Callable[..., Any]](*verbs: str) -> Callable[[T], T]:
    def mark(test: T) -> T:
        _NEEDS[test.__name__] = frozenset(verbs)
        return test

    return mark


def _left_out(verb: str) -> Callable[..., Any]:
    """The one test collected for a verb the adapter leaves out."""

    @pytest.mark.asyncio
    async def test(self: Any, target: ConformanceTarget) -> None:
        with pytest.raises(Refusal) as refused:
            await call_verb(target.build(), target, verb)
        assert refused.value.reason == RefusalReason.VERB_NOT_IMPLEMENTED, verb

    test.__name__ = test.__qualname__ = f"test_{verb}_is_left_out"
    test.__doc__ = (
        f"The adapter leaves out {verb}: the kit checks only that it refuses with "
        f"verb_not_implemented, and collects none of the other {verb} tests."
    )
    return test


class AdapterConformance:
    """The suite. Subclasses set ``adapter`` and provide the ``target`` fixture."""

    #: The adapter class under test. Its ``implements`` decides which tests run.
    adapter: ClassVar[type[BaseAdapter[Any]]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        adapter: type[BaseAdapter[Any]] | None = getattr(cls, "adapter", None)
        if adapter is None:
            if cls.__name__.startswith("Test"):
                raise TypeError(
                    f"{cls.__name__} must set `adapter = <your adapter class>` so the "
                    "conformance kit knows which verbs to test"
                )
            return
        for name, verbs in _NEEDS.items():
            if not verbs & adapter.implements:
                setattr(cls, name, None)
        for verb in _OPTIONAL_VERBS:
            if verb not in adapter.implements:
                setattr(cls, f"test_{verb}_is_left_out", _left_out(verb))

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
        assert isinstance(adapter, self.adapter), (
            f"the target builds {type(adapter).__name__}, but the class names "
            f"{self.adapter.__name__} as the adapter under test"
        )
        info = await adapter.describe()
        assert info.adapter == target.adapter
        assert "describe" in info.verbs
        assert set(info.verbs) == set(type(adapter).implements)
        assert set(info.verbs) <= set(VERBS)
        info.model_dump_json()

    @_needs("exec")
    @pytest.mark.asyncio
    async def test_exec_bounded(self, target: ConformanceTarget) -> None:
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

    @_needs("exec")
    @pytest.mark.asyncio
    async def test_exec_without_time_limit(self, target: ConformanceTarget) -> None:
        """No hard limit on agent actions: exec with no timeout runs to the end."""
        result = await target.build().exec(target.echo, max_output_bytes=4096)
        assert result.exit_code == 0, result
        assert not result.timed_out
        assert target.ok_marker in result.stdout

    @_needs("files")
    @pytest.mark.asyncio
    async def test_files_round_trip(self, target: ConformanceTarget) -> None:
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

    @_needs("files")
    @pytest.mark.asyncio
    async def test_files_delete(self, target: ConformanceTarget) -> None:
        """docs/files.md: delete refuses a missing path, needs recursive for a
        directory that is not empty, and never removes relay's work directory."""
        files = await target.build().files()
        folder = f"{target.files_dir}/{new_id('d')}"
        await files.write(f"{folder}/inner/a.txt", b"a")
        await files.delete(f"{folder}/inner/a.txt")
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

    @_needs("files")
    @pytest.mark.asyncio
    async def test_files_chunked_write(self, target: ConformanceTarget) -> None:
        """docs/files.md: chunks collect in <path>.relay-part, a wrong offset says
        where to resume, and the final chunk moves the file into place."""
        files = await target.build().files()
        path = f"{target.files_dir}/{new_id('c')}.bin"
        first, second, third = b"a" * 1000, b"b" * 700, b"c" * 300
        part = await files.write(path, first, final=False)
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

    @_needs("files")
    @pytest.mark.asyncio
    async def test_files_relative_paths_stay_inside(self, target: ConformanceTarget) -> None:
        """docs/files.md: a relative path may not leave relay's work directory."""
        files = await target.build().files()
        with pytest.raises(Refusal) as escaped:
            await files.write("../relay-escape.txt", b"x")
        assert escaped.value.reason == RefusalReason.INVALID_ARGUMENTS

    @_needs("run")
    @pytest.mark.asyncio
    async def test_job_lifecycle(self, target: ConformanceTarget) -> None:
        adapter = target.build()
        handle = await adapter.run(target.ok_job, operation_id=new_id("op"))
        assert handle.adapter == target.adapter
        JobHandle.model_validate_json(handle.model_dump_json())
        status, seen = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert_lifecycle(seen)
        assert status.state == JobState.COMPLETED, status
        assert status.exit_code == 0
        assert status.outputs, "a finished job lists its outputs"

    @_needs("run")
    @pytest.mark.asyncio
    async def test_job_failure(self, target: ConformanceTarget) -> None:
        adapter = target.build()
        handle = await adapter.run(target.fail_job, operation_id=new_id("op"))
        status, seen = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert_lifecycle(seen)
        assert status.state == JobState.FAILED, status
        assert status.exit_code not in (None, 0)

    @_needs("run")
    @pytest.mark.asyncio
    async def test_job_cancel(self, target: ConformanceTarget) -> None:
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

    @_needs("run")
    @pytest.mark.asyncio
    async def test_job_observed_after_restart(self, target: ConformanceTarget) -> None:
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

    @_needs("run")
    @pytest.mark.asyncio
    async def test_inputs_by_digest(self, target: ConformanceTarget) -> None:
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

    @_needs("files", "exec")
    @pytest.mark.asyncio
    async def test_errors_are_typed(self, target: ConformanceTarget) -> None:
        """A missing path is path_not_found; a missing program fails, it does not hang."""
        if "files" in self.adapter.implements:
            files = await target.build().files()
            with pytest.raises(Refusal) as missing:
                await files.list(f"{target.files_dir}/no-such-dir-{new_id('d')}")
            assert missing.value.reason == RefusalReason.PATH_NOT_FOUND
        if "exec" in self.adapter.implements:
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

    @_needs("run")
    @pytest.mark.asyncio
    async def test_cancel_after_finish(self, target: ConformanceTarget) -> None:
        """Cancelling a finished job changes nothing: its final state stands."""
        adapter = target.build()
        handle = await adapter.run(target.ok_job, operation_id=new_id("op"))
        status, _ = await wait_final(adapter, handle, target.job_timeout_s, target.poll_s)
        assert status.state == JobState.COMPLETED, status
        assert (await adapter.cancel(handle)).state == JobState.COMPLETED
        assert (await target.build().status(handle)).state == JobState.COMPLETED

    @_needs("exec")
    @pytest.mark.asyncio
    async def test_exec_cancelled(self, target: ConformanceTarget) -> None:
        """Cancelling an exec call with no time limit stops it promptly."""
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

    @_needs("files")
    @pytest.mark.asyncio
    async def test_guarded_files_ask_first(self, target: ConformanceTarget) -> None:
        """docs/guarded-sites.md: on a guarded site, delete and a write that replaces
        a file need the person's yes; a new file and any open-site op do not."""
        from clio_relay.sites.models import SiteEntry
        from clio_relay.surface.files_approval import approval_action

        files = await target.build().files()

        def site(policy: str) -> SiteEntry:
            return SiteEntry.model_validate(
                {"id": target.site_id, "adapter": target.adapter, "policy": policy}
            )

        opened, guarded = site("open"), site("guarded")
        path = f"{target.files_dir}/{new_id('g')}.txt"
        assert await approval_action(guarded, files, "write", path, overwrite=True) is None
        await files.write(path, b"first")
        for policy in (opened, guarded):
            assert await approval_action(policy, files, "write", f"{path}.new") is None
        assert await approval_action(opened, files, "write", path, overwrite=True) is None
        assert await approval_action(opened, files, "delete", path) is None
        replace = await approval_action(guarded, files, "write", path, overwrite=True)
        assert replace is not None and path in replace
        middle = await approval_action(
            guarded, files, "write", path, overwrite=True, offset=5, final=False
        )
        assert middle is None, "only the first and the last chunk of an upload ask"
        assert await approval_action(guarded, files, "delete", path) is not None
        assert await approval_action(guarded, files, "delete", f"{path}.gone") is not None
        await files.delete(path)


def use_memory_keyring(monkeypatch: pytest.MonkeyPatch, secrets: dict[str, str]) -> None:
    """Keep relay's secrets in memory for this test and store ``secrets`` there.

    An adapter reads them with ``self.context.secret(name)``, as it reads a key
    the person typed for ``SignInNeed(provider="keyring:<name>")``.
    """
    from clio_relay.identity.store import TokenStore

    monkeypatch.setenv("CLIO_RELAY_KEYRING", "memory")
    store = TokenStore()
    for name, value in secrets.items():
        store.set_secret(name, value)


__all__ = [
    "AdapterConformance",
    "ConformanceTarget",
    "assert_lifecycle",
    "use_memory_keyring",
    "wait_final",
]
