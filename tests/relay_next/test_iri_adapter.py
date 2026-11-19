"""The ``iri`` adapter (RL13) against a stand-in IRI facility.

The stand-in answers the published IRI Facility API (versions 1 and 2) through
an ``httpx`` mock transport, keeps files on this machine and runs jobs as local
processes. These are mock results, never live passes.
"""

from __future__ import annotations

import asyncio
import stat
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

import httpx
import pytest

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.globus import tokens
from clio_relay.adapters.iri import IriAdapter, IriConfig, iri_job_spec
from clio_relay.adapters.iri.client import FILE_SIZE_LIMIT, IriClient
from clio_relay.adapters.iri.fakes import (
    ATTRIBUTE_FIELDS,
    JOB_SPEC_FIELDS,
    RESOURCE_FIELDS,
    FakeIriFacility,
)
from clio_relay.adapters.iri.files import decode_download
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobState
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobAttributes, JobResources, JobSpec, PackageRef
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget, wait_final

pytestmark = pytest.mark.release_platform("posix")

RESOURCE_SERVER = "ed3e577d-f7f3-4639-b96e-ff5a8445d699"


class SignedIn:
    """An identity provider that always has a token."""

    def __init__(self, access_token: str = "test-token") -> None:
        self.access_token = access_token

    async def token(self, *, resource_server: str) -> tokens.Token:
        return tokens.Token(resource_server=resource_server, access_token=self.access_token)

    def status(self) -> str:
        return "signed_in"


class SignedOut:
    """An identity provider with no token."""

    async def token(self, *, resource_server: str) -> tokens.Token | None:
        return None

    def status(self) -> str:
        return "signed_out"


class LocalIriAdapter(IriAdapter):
    """The adapter pointed at a stand-in facility whose files live under ``root``."""

    name: ClassVar[str] = "iri-local"

    def __init__(self, config: IriConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        self.identity = SignedIn()

    def _transport(self) -> httpx.AsyncBaseTransport | None:
        return self.facility().transport()

    def facility(self) -> FakeIriFacility:
        assert self.config.root is not None
        return FakeIriFacility(root=Path(self.config.root))


@pytest.fixture(autouse=True)
def registered_adapter() -> Any:
    registry.register(LocalIriAdapter, "iri-local")
    yield
    registry.unregister("iri-local")


def local_config(tmp_path: Path, version: str = "v2") -> dict[str, Any]:
    root = tmp_path / "facility"
    root.mkdir(exist_ok=True)
    return {
        "base_url": "https://iri.example.test",
        "api_version": version,
        "resource_id": "compute",
        "resource_server": RESOURCE_SERVER,
        "root": str(root),
        "poll_s": 0.05,
    }


class TestIriConformanceV2(AdapterConformance):
    @pytest.fixture
    def target(self, tmp_path: Path) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="iri-local",
            config=local_config(tmp_path, "v2"),
            state_dir=tmp_path,
            job_timeout_s=60,
            poll_s=0.2,
        )


class TestIriConformanceV1(AdapterConformance):
    @pytest.fixture
    def target(self, tmp_path: Path) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="iri-local",
            config=local_config(tmp_path, "v1"),
            state_dir=tmp_path,
            job_timeout_s=60,
            poll_s=0.2,
        )


def _context(tmp_path: Path) -> AdapterContext:
    return AdapterContext(
        site_id="perlmutter",
        state_dir=tmp_path / "adapter",
        content=ContentStore(tmp_path / "content"),
    )


def _build(tmp_path: Path, version: str = "v2", **overrides: Any) -> LocalIriAdapter:
    config = IriConfig.model_validate({**local_config(tmp_path, version), **overrides})
    return LocalIriAdapter(config, _context(tmp_path))


def test_job_spec_uses_only_published_fields(tmp_path: Path) -> None:
    spec = JobSpec(
        executable="/bin/hostname",
        arguments=["-f"],
        environment={"A": "1"},
        launcher="srun",
        resources=JobResources(node_count=2, processes_per_node=4, exclusive_node_use=True),
        attributes=JobAttributes(
            duration=timedelta(minutes=30),
            queue_name="debug",
            account="m1234",
            project_name="ignored by IRI",
            custom_attributes={"constraint": "gpu", "gpus": 4},
        ),
    )
    body = iri_job_spec(spec, job_dir="/scratch/jobs/op1")
    assert set(body) <= JOB_SPEC_FIELDS
    assert set(body["resources"]) <= RESOURCE_FIELDS
    assert set(body["attributes"]) <= ATTRIBUTE_FIELDS
    assert body["attributes"] == {
        "duration": 1800,
        "queue_name": "debug",
        "account": "m1234",
        "custom_attributes": {"constraint": "gpu", "gpus": "4"},
    }
    assert body["resources"] == {
        "node_count": 2,
        "processes_per_node": 4,
        "exclusive_node_use": True,
    }
    assert body["directory"] == "/scratch/jobs/op1"
    assert body["stdout_path"] == "/scratch/jobs/op1/stdout"
    assert body["launcher"] == "srun"


def test_job_spec_sends_no_duration_when_the_job_has_none() -> None:
    spec = JobSpec(executable="/bin/hostname", attributes=JobAttributes(account="m1234"))
    body = iri_job_spec(spec, job_dir=None)
    assert body["attributes"] == {"account": "m1234"}


def test_package_jobs_refuse() -> None:
    spec = JobSpec(package=PackageRef(name="demo", version="1", sha256="0" * 64))
    with pytest.raises(Refusal) as refused:
        iri_job_spec(spec, job_dir=None)
    assert refused.value.reason == RefusalReason.INVALID_SPEC


@pytest.mark.asyncio
async def test_v2_submit_carries_a_stable_idempotency_key(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    facility = adapter.facility()
    adapter.client.transport = facility.transport()
    operation = new_id("op")
    handle = await adapter.run(JobSpec(executable="true"), operation_id=operation)
    submits = [item for item in facility.requests if item["path"].startswith("/api/v2/compute/job")]
    assert len(submits) == 1
    key = submits[0]["headers"]["idempotency-key"]
    assert key == adapter._idempotency_key(operation)  # pyright: ignore[reportPrivateUsage]
    again = await adapter.client.submit("compute", facility.submitted()[0], idempotency_key=key)
    assert again["id"] == handle.native_id
    with pytest.raises(Refusal) as refused:
        await adapter.client.submit("compute", {"executable": "false"}, idempotency_key=key)
    assert refused.value.reason == RefusalReason.INVALID_SPEC
    await wait_final(adapter, handle, 60, 0.2)


@pytest.mark.asyncio
async def test_v1_has_no_idempotency_key_and_uses_query_parameters(tmp_path: Path) -> None:
    adapter = _build(tmp_path, "v1")
    facility = adapter.facility()
    adapter.client.transport = facility.transport()
    files = await adapter.files()
    await files.write("data/a.txt", b"hello")
    assert await files.read("data/a.txt") == b"hello"
    handle = await adapter.run(JobSpec(executable="true"), operation_id=new_id("op"))
    await wait_final(adapter, handle, 60, 0.2)
    submit = next(item for item in facility.requests if "/compute/job/" in item["path"])
    assert "idempotency-key" not in submit["headers"]
    stat_calls = [item for item in facility.requests if "/filesystem/stat/" in item["path"]]
    assert stat_calls and all(item["method"] == "GET" for item in stat_calls)
    assert all("path" in item["params"] for item in stat_calls)


@pytest.mark.asyncio
async def test_cancel_reaches_cancelled(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    handle = await adapter.run(JobSpec(executable="sleep", arguments=["300"]), operation_id="op1")
    status = await adapter.cancel(handle)
    assert status.state in (JobState.QUEUED, JobState.RUNNING, JobState.CANCELLED)
    final, _ = await wait_final(adapter, handle, 60, 0.2)
    assert final.state == JobState.CANCELLED
    again = await _build(tmp_path).status(handle)
    assert again.state == JobState.CANCELLED


@pytest.mark.asyncio
async def test_signed_out_refuses_with_needs_sign_in(tmp_path: Path) -> None:
    adapter = _build(tmp_path, scope="https://auth.globus.org/scopes/x/iri_api")
    adapter.identity = SignedOut()
    need = await adapter.sign_in_needed()
    assert need is not None
    assert need.provider == "globus"
    assert "iri_api" in need.message
    with pytest.raises(Refusal) as refused:
        await adapter.run(JobSpec(executable="true"), operation_id="op1")
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN
    info = await adapter.describe()
    assert info.facts["sign_in"] == "signed_out"
    assert "resources" not in info.facts


@pytest.mark.asyncio
async def test_a_rejected_token_refuses_with_needs_sign_in(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    adapter.identity = SignedIn("stale-token")
    with pytest.raises(Refusal) as refused:
        await (await adapter.files()).list(".")
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN


@pytest.mark.asyncio
async def test_token_command_supplies_the_token(tmp_path: Path) -> None:
    command = [sys.executable, "-c", "print('banner'); print('test-token')"]
    adapter = _build(tmp_path, token_command=command, resource_server=None)
    assert await adapter.sign_in_needed() is None
    info = await adapter.describe()
    assert info.facts["resources"] == ["compute"]
    assert info.facts["resource_status"] == "up"
    failing = _build(tmp_path, token_command=[sys.executable, "-c", "raise SystemExit(3)"])
    with pytest.raises(Refusal) as refused:
        await failing._token()  # pyright: ignore[reportPrivateUsage]
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN


def test_no_token_source_is_a_config_error(tmp_path: Path) -> None:
    adapter = _build(tmp_path, resource_server=None)
    with pytest.raises(Refusal) as refused:
        asyncio.run(adapter._token())  # pyright: ignore[reportPrivateUsage]
    assert refused.value.reason == RefusalReason.INVALID_CONFIG


@pytest.mark.asyncio
async def test_problem_documents_become_refusals() -> None:
    answers = {
        422: RefusalReason.INVALID_SPEC,
        400: RefusalReason.SUBMIT_FAILED,
        404: RefusalReason.INVALID_CONFIG,
        503: RefusalReason.UNREACHABLE,
    }
    for status, reason in answers.items():

        def handler(request: httpx.Request, status: int = status) -> httpx.Response:
            return httpx.Response(
                status,
                json={"type": "about:blank", "status": status, "title": "No", "detail": "because"},
            )

        async def token() -> str:
            return "t"

        client = IriClient("https://x.test", "v2", token, transport=httpx.MockTransport(handler))
        with pytest.raises(Refusal) as refused:
            await client.submit("compute", {"executable": "true"}, idempotency_key="k")
        assert refused.value.reason == reason, status
        assert "No: because" in refused.value.message


@pytest.mark.asyncio
async def test_connect_errors_are_unreachable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    async def token() -> str:
        return "t"

    client = IriClient("https://x.test", "v2", token, transport=httpx.MockTransport(handler))
    with pytest.raises(Refusal) as refused:
        await client.resources()
    assert refused.value.reason == RefusalReason.UNREACHABLE


@pytest.mark.asyncio
async def test_file_tasks_are_polled_until_they_complete(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    facility = FakeIriFacility(root=Path(adapter.config.root or ""), task_polls=3)
    adapter.client.transport = facility.transport()
    files = await adapter.files()
    await files.write("polled.txt", b"data")
    polls = [item for item in facility.requests if "/task/" in item["path"]]
    assert len(polls) >= 4


@pytest.mark.asyncio
async def test_a_task_that_never_changes_is_a_stall(tmp_path: Path) -> None:
    adapter = _build(tmp_path, stall_s=0.2)
    facility = FakeIriFacility(root=Path(adapter.config.root or ""), task_polls=10**6)
    adapter.client.transport = facility.transport()
    adapter.client.max_poll_s = 0.05
    with pytest.raises(Refusal) as refused:
        await (await adapter.files()).list(".")
    assert refused.value.reason == RefusalReason.UNREACHABLE
    assert "stayed active" in refused.value.message


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v1", "v2"])
async def test_mv_rm_and_lstat_reach_the_facility(tmp_path: Path, version: str) -> None:
    adapter = _build(tmp_path, version)
    root = Path(adapter.config.root or "")
    (root / "a.txt").write_bytes(b"a")
    (root / "d" / "e").mkdir(parents=True)
    (root / "d" / "e" / "f.txt").write_bytes(b"f")
    (root / "link").symlink_to(root / "d")
    task = adapter.client.file_task
    await task("mv", "compute", {"path": str(root / "a.txt"), "target_path": str(root / "b.txt")})
    assert (root / "b.txt").read_bytes() == b"a"
    assert not (root / "a.txt").exists()
    lstat = await task("stat", "compute", {"path": str(root / "link"), "dereference": False})
    assert stat.S_ISLNK(lstat["output"]["mode"])
    await task("rm", "compute", {"path": str(root / "link")})
    assert (root / "d" / "e" / "f.txt").is_file()
    await task("rm", "compute", {"path": str(root / "d")})
    assert not (root / "d").exists()


@pytest.mark.asyncio
async def test_large_files_refuse_with_a_pointer_to_globus(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    files = await adapter.files()
    with pytest.raises(Refusal) as refused:
        await files.write("big.bin", b"x" * (FILE_SIZE_LIMIT + 1))
    assert refused.value.reason == RefusalReason.INVALID_ARGUMENTS
    assert "globus" in refused.value.message


def test_download_results_in_either_form() -> None:
    assert decode_download({"output": "plain text"}) == b"plain text"
    assert decode_download({"output": {"content_base64": "AAEC"}}) == b"\x00\x01\x02"
    assert decode_download({"output": {"data": "AAEC"}}) == b"\x00\x01\x02"
    assert decode_download({"output": {"content": "not base64!"}}) == b"not base64!"
    with pytest.raises(Refusal):
        decode_download({"output": None})


def test_relative_paths_need_a_root(tmp_path: Path) -> None:
    adapter = IriAdapter(
        IriConfig.model_validate(
            {"base_url": "https://x.test", "resource_id": "c", "resource_server": "rs"}
        ),
        _context(tmp_path),
    )
    files = adapter._files()  # pyright: ignore[reportPrivateUsage]
    assert files.resolve("/abs/path") == "/abs/path"
    with pytest.raises(Refusal) as refused:
        files.resolve("relative")
    assert refused.value.reason == RefusalReason.INVALID_CONFIG
    with pytest.raises(Refusal) as no_home:
        files.resolve("~/a.txt")
    assert no_home.value.reason == RefusalReason.INVALID_CONFIG
    with pytest.raises(Refusal) as no_store:
        files.content_path("ab" * 32)
    assert no_store.value.reason == RefusalReason.INVALID_CONFIG


def test_home_paths_use_the_home_setting(tmp_path: Path) -> None:
    files = _build(tmp_path, home="/global/homes/u/user")._files()  # pyright: ignore[reportPrivateUsage]
    assert files.resolve("~/a.txt") == "/global/homes/u/user/a.txt"
    assert files.resolve("~") == "/global/homes/u/user"


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v1", "v2"])
async def test_a_recursive_delete_is_one_rm_and_links_are_not_followed(
    tmp_path: Path, version: str
) -> None:
    adapter = _build(tmp_path, version)
    root = Path(adapter.config.root or "")
    facility = FakeIriFacility(root=root)
    adapter.client.transport = facility.transport()
    (root / "kept" / "deep").mkdir(parents=True)
    (root / "kept" / "deep" / "k.txt").write_bytes(b"k")
    (root / "tree" / "a" / "b").mkdir(parents=True)
    (root / "tree" / "a" / "b" / "c.txt").write_bytes(b"c")
    (root / "tree" / "link").symlink_to(root / "kept")
    files = await adapter.files()
    await files.delete("tree/link")
    assert (root / "kept" / "deep" / "k.txt").is_file()
    facility.requests.clear()
    await files.delete("tree", recursive=True)
    assert not (root / "tree").exists()
    assert sum("/filesystem/rm" in item["path"] for item in facility.requests) == 1


@pytest.mark.asyncio
async def test_chunks_upload_once_and_refuse_past_the_limit_before_moving_bytes(
    tmp_path: Path,
) -> None:
    adapter = _build(tmp_path)
    root = Path(adapter.config.root or "")
    facility = FakeIriFacility(root=root)
    adapter.client.transport = facility.transport()
    files = await adapter.files()
    await files.write("c.bin", b"a" * 10, final=False)
    await files.write("c.bin", b"b" * 10, offset=10, final=False)
    await files.write("c.bin", b"c" * 10, offset=20)
    assert (root / "c.bin").read_bytes() == b"a" * 10 + b"b" * 10 + b"c" * 10
    assert sum("/filesystem/upload" in item["path"] for item in facility.requests) == 1
    facility.requests.clear()
    await files.write("big.bin", b"x" * 1024, final=False)
    with pytest.raises(Refusal) as refused:
        await files.write("big.bin", b"y", offset=FILE_SIZE_LIMIT, final=False)
    assert refused.value.reason == RefusalReason.INVALID_ARGUMENTS
    assert "globus" in refused.value.message
    assert not any("/filesystem/upload" in item["path"] for item in facility.requests)


@pytest.mark.asyncio
async def test_inputs_without_a_job_directory_refuse(tmp_path: Path) -> None:
    adapter = _build(tmp_path, job_root="")
    digest = adapter.context.content.put(b"data")
    spec = JobSpec(executable="cat", inputs=[InputRef(name="in.txt", sha256=digest)])
    with pytest.raises(Refusal) as refused:
        await adapter.run(spec, operation_id="op1")
    assert refused.value.reason == RefusalReason.INVALID_SPEC
