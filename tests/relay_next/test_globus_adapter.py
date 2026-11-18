"""The ``globus`` adapter (RL12) against local stand-ins for Compute and Transfer.

Nothing here talks to Globus. The stand-in endpoint runs the real task function
in a local process, so the job path the adapter drives is the one an endpoint
runs. These are mock results, never live passes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

import pytest
from pydantic import BaseModel, SecretStr

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.globus import GlobusAdapter, GlobusConfig, tokens
from clio_relay.adapters.globus.compute import ComputeService, function_digest
from clio_relay.adapters.globus.fakes import LocalComputeService, LocalTransferService
from clio_relay.adapters.globus.task import relay_task
from clio_relay.adapters.globus.transfer import TransferService
from clio_relay.adapters.local import LocalAdapter
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import ExecResult, JobState, StallReport
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobAttributes, JobResources, JobSpec
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget, wait_final

pytestmark = pytest.mark.release_platform("posix")

ENDPOINT = "9a947ba5-f537-4681-acf3-cc66485aadec"
SITE_COLLECTION = "05d2c76a-e867-4f67-aa57-76edeb0beda0"
LOCAL_COLLECTION = "11111111-2222-4333-8444-555555555555"


class SignedIn:
    """An identity provider that always has a token."""

    async def token(self, *, resource_server: str) -> tokens.Token:
        return tokens.Token(resource_server=resource_server, access_token="test-token")

    def status(self) -> str:
        return "signed_in"


class LocalGlobusAdapter(GlobusAdapter):
    """The adapter with its Globus services swapped for local stand-ins."""

    name: ClassVar[str] = "globus-local"

    def __init__(self, config: GlobusConfig, context: AdapterContext) -> None:
        super().__init__(config, context)
        self.identity = SignedIn()

    async def _compute_service(self) -> ComputeService:
        await tokens.bearer(self.identity, tokens.COMPUTE_RESOURCE_SERVER, what="compute")
        return LocalComputeService(self.context.state_dir / "endpoint")

    async def _transfer_service(self) -> TransferService:
        return LocalTransferService(
            {
                SITE_COLLECTION: self.context.state_dir / "site",
                LOCAL_COLLECTION: self.context.state_dir / "local",
            }
        )


@pytest.fixture(autouse=True)
def registered_adapter() -> Any:
    registry.register(LocalGlobusAdapter, "globus-local")
    yield
    registry.unregister("globus-local")


def local_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "endpoint": ENDPOINT,
        "scheduler": "pbs",
        "queues": ["debug", "prod"],
        "job_root": str(tmp_path / "site-jobs"),
        "cancel_poll_s": 0.2,
        "collection": SITE_COLLECTION,
        "local_collection": LOCAL_COLLECTION,
        "local_path": "/",
        "local_dir": str(tmp_path / "adapter" / "local"),
    }


class TestGlobusConformance(AdapterConformance):
    @pytest.fixture
    def target(self, tmp_path: Path) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="globus-local",
            config=local_config(tmp_path),
            state_dir=tmp_path,
            job_timeout_s=60,
            poll_s=0.2,
        )


def _context(tmp_path: Path) -> AdapterContext:
    return AdapterContext(
        site_id="polaris",
        state_dir=tmp_path / "adapter",
        content=ContentStore(tmp_path / "content"),
    )


def _build(tmp_path: Path, **overrides: Any) -> LocalGlobusAdapter:
    config = GlobusConfig.model_validate({**local_config(tmp_path), **overrides})
    return LocalGlobusAdapter(config, _context(tmp_path))


def test_user_config_maps_the_job_onto_the_template(tmp_path: Path) -> None:
    adapter = _build(tmp_path, user_config={"worker_init": "module load conda"})
    spec = JobSpec(
        executable="hostname",
        resources=JobResources(node_count=2),
        attributes=JobAttributes(
            duration=timedelta(minutes=30),
            queue_name="debug",
            account="myproject",
            custom_attributes={"globus.scheduler_options": "#PBS -l filesystems=home:eagle"},
        ),
    )
    assert adapter.user_config(spec) == {
        "worker_init": "module load conda",
        "account": "myproject",
        "queue": "debug",
        "walltime": "00:30:00",
        "nodes_per_block": 2,
        "scheduler_options": "#PBS -l filesystems=home:eagle",
    }


def test_user_config_keys_follow_the_site(tmp_path: Path) -> None:
    adapter = _build(
        tmp_path,
        scheduler="slurm",
        user_config_keys={"queue": "partition", "nodes": None},
        walltime_format="minutes",
    )
    spec = JobSpec(
        executable="hostname",
        attributes=JobAttributes(
            duration=timedelta(seconds=90), queue_name="ghx4", account="bbka-dtai-gh"
        ),
    )
    assert adapter.user_config(spec) == {
        "account": "bbka-dtai-gh",
        "partition": "ghx4",
        "walltime": 2,
    }


def test_user_config_sends_no_walltime_without_a_duration(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    spec = JobSpec(executable="hostname", attributes=JobAttributes(queue_name="debug"))
    assert adapter.user_config(spec) == {"queue": "debug", "nodes_per_block": 1}


@pytest.mark.asyncio
async def test_job_reaches_the_endpoint_with_its_user_config(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    launcher = bin_dir / "mpiexec"
    launcher.write_text('#!/bin/sh\nshift 2\nexec "$@"\n', encoding="utf-8")
    launcher.chmod(0o755)
    spec = JobSpec(
        executable="echo",
        arguments=["hello"],
        environment={"PATH": f"{bin_dir}:{os.environ.get('PATH', '')}"},
        resources=JobResources(node_count=1, process_count=4),
        launcher="mpiexec",
        attributes=JobAttributes(queue_name="debug", account="myproject"),
    )
    handle = await adapter.run(spec, operation_id="op-one")
    status, _ = await wait_final(adapter, handle, 60, 0.2)
    assert status.state == JobState.COMPLETED, status
    sent = LocalComputeService(tmp_path / "adapter" / "endpoint").submitted()
    assert sent[0]["user_config"]["queue"] == "debug"
    assert sent[0]["user_config"]["account"] == "myproject"
    assert sent[0]["payload"]["argv"][:3] == ["mpiexec", "-n", "4"]


@pytest.mark.asyncio
async def test_unknown_launchers_refuse(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    with pytest.raises(Refusal) as refused:
        await adapter.run(JobSpec(executable="x", launcher="jsrun"), operation_id=new_id("op"))
    assert refused.value.reason == RefusalReason.INVALID_SPEC


@pytest.mark.asyncio
async def test_inputs_over_the_inline_cap_refuse(tmp_path: Path) -> None:
    from clio_relay.jobs.spec import InputRef

    adapter = _build(tmp_path, inline_input_limit=10)
    digest = adapter.context.content.put(b"x" * 64)
    spec = JobSpec(executable="cat", inputs=[InputRef(name="in.txt", sha256=digest)])
    with pytest.raises(Refusal) as refused:
        await adapter.run(spec, operation_id=new_id("op"))
    assert refused.value.reason == RefusalReason.INVALID_SPEC


@pytest.mark.asyncio
async def test_files_without_a_collection_refuse(tmp_path: Path) -> None:
    adapter = _build(tmp_path, collection=None)
    with pytest.raises(Refusal) as refused:
        await adapter.files()
    assert refused.value.reason == RefusalReason.INVALID_CONFIG


@pytest.mark.asyncio
async def test_reading_without_a_local_collection_refuses(tmp_path: Path) -> None:
    adapter = _build(tmp_path, local_collection=None, local_path=None, local_dir=None)
    (tmp_path / "adapter" / "site" / "home").mkdir(parents=True)
    (tmp_path / "adapter" / "site" / "home" / "a.txt").write_text("a")
    files = await adapter.files()
    assert (await files.stat("a.txt")).size == 1
    with pytest.raises(Refusal) as refused:
        await files.read("a.txt")
    assert refused.value.reason == RefusalReason.INVALID_CONFIG


@pytest.mark.asyncio
async def test_signed_out_refuses_needs_sign_in(tmp_path: Path) -> None:
    config = GlobusConfig.model_validate(local_config(tmp_path))
    adapter = GlobusAdapter(config, _context(tmp_path))
    adapter.identity = tokens.GrantFileProvider("globus", tmp_path / "missing.json")
    need = await adapter.sign_in_needed()
    assert need is not None
    assert need.provider == "globus"
    with pytest.raises(Refusal) as refused:
        await adapter.run(JobSpec(executable="hostname"), operation_id=new_id("op"))
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN
    info = await adapter.describe()
    assert info.facts["sign_in"] == "signed_out"
    assert "endpoint_status" not in info.facts


class RecordingLocal(LocalAdapter):
    """The ``local`` adapter, keeping the arguments of each ``exec`` call."""

    name: ClassVar[str] = "start-recorder"
    calls: ClassVar[list[dict[str, Any]]] = []

    async def exec(
        self,
        command: list[str],
        *,
        timeout_s: float | None = None,
        max_output_bytes: int,
        on_stall: StallReport | None = None,
    ) -> ExecResult:
        RecordingLocal.calls.append(
            {"command": command, "timeout_s": timeout_s, "on_stall": on_stall}
        )
        return await super().exec(
            command, timeout_s=timeout_s, max_output_bytes=max_output_bytes, on_stall=on_stall
        )


class OfflineGlobusAdapter(LocalGlobusAdapter):
    """The local adapter with an endpoint that reports offline."""

    async def _compute_service(self) -> ComputeService:
        await tokens.bearer(self.identity, tokens.COMPUTE_RESOURCE_SERVER, what="compute")
        return LocalComputeService(self.context.state_dir / "endpoint", status="offline")


@pytest.fixture
def start_recorder() -> Any:
    RecordingLocal.calls.clear()
    registry.register(RecordingLocal, "start-recorder")
    yield RecordingLocal.calls
    registry.unregister("start-recorder")


def _offline(tmp_path: Path, command: list[str], **start: Any) -> OfflineGlobusAdapter:
    config = GlobusConfig.model_validate(
        {
            **local_config(tmp_path),
            "start": {"adapter": "start-recorder", "command": command, **start},
        }
    )
    return OfflineGlobusAdapter(config, _context(tmp_path))


def test_endpoint_start_has_no_default_limit() -> None:
    config = GlobusConfig.model_validate({"endpoint": ENDPOINT, "start": {"adapter": "worker"}})
    assert config.start is not None
    assert config.start.timeout_s is None


@pytest.mark.asyncio
async def test_offline_endpoint_is_started_with_no_limit(
    tmp_path: Path, start_recorder: list[dict[str, Any]]
) -> None:
    marker = tmp_path / "started"
    command = [sys.executable, "-c", f"open({str(marker)!r}, 'w').close()"]
    adapter = _offline(tmp_path, command)
    handle = await adapter.run(JobSpec(executable="true"), operation_id=new_id("op"))
    assert marker.exists()
    assert len(start_recorder) == 1
    assert start_recorder[0]["command"] == command
    assert start_recorder[0]["timeout_s"] is None
    assert start_recorder[0]["on_stall"] is not None
    await start_recorder[0]["on_stall"](30.0, 30.0)
    status, _ = await wait_final(adapter, handle, 60, 0.2)
    assert status.state == JobState.COMPLETED, status


@pytest.mark.asyncio
async def test_endpoint_start_passes_a_set_limit(
    tmp_path: Path, start_recorder: list[dict[str, Any]]
) -> None:
    adapter = _offline(tmp_path, [sys.executable, "-c", "pass"], timeout_s=120)
    await adapter.run(JobSpec(executable="true"), operation_id=new_id("op"))
    assert start_recorder[0]["timeout_s"] == 120


@pytest.mark.asyncio
async def test_failed_endpoint_start_refuses_unreachable(
    tmp_path: Path, start_recorder: list[dict[str, Any]]
) -> None:
    command = [sys.executable, "-c", "import sys; print('no config'); sys.exit(2)"]
    adapter = _offline(tmp_path, command)
    with pytest.raises(Refusal) as refused:
        await adapter.run(JobSpec(executable="true"), operation_id=new_id("op"))
    assert refused.value.reason == RefusalReason.UNREACHABLE
    assert "no config" in str(refused.value)
    assert LocalComputeService(tmp_path / "adapter" / "endpoint").submitted() == []


def _grant(path: Path, tokens_: list[dict[str, Any]]) -> Path:
    path.write_text(
        json.dumps({"version": 1, "provider": "globus", "written_at": 0, "tokens": tokens_}),
        encoding="utf-8",
    )
    return path


@pytest.mark.asyncio
async def test_grant_file_provider_reads_tokens(tmp_path: Path) -> None:
    later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    path = _grant(
        tmp_path / "grant.json",
        [
            {"resource_server": "funcx_service", "access_token": "c", "expires_at": later},
            {"resource_server": "transfer.api.globus.org", "access_token": "t", "expires_at": 1},
        ],
    )
    source = tokens.GrantFileProvider("globus", path)
    assert source.status() == "signed_in"
    assert await tokens.bearer(source, "funcx_service", what="x") == "c"
    with pytest.raises(Refusal) as refused:
        await tokens.bearer(source, "transfer.api.globus.org", what="Transfer")
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN


def test_grant_file_provider_reports_expired(tmp_path: Path) -> None:
    path = _grant(
        tmp_path / "grant.json", [{"resource_server": "x", "access_token": "a", "expires_at": 5}]
    )
    assert tokens.GrantFileProvider("globus", path).status() == "expired"
    assert tokens.GrantFileProvider("globus", tmp_path / "none.json").status() == "signed_out"


@pytest.mark.asyncio
async def test_status_of_accepts_enums_and_coroutines() -> None:
    from enum import StrEnum

    class Status(StrEnum):
        SIGNED_IN = "signed_in"

    class Async:
        async def token(self, *, resource_server: str) -> None:
            return None

        async def status(self) -> Status:
            return Status.SIGNED_IN

    assert await tokens.status_of(Async()) == "signed_in"


class W6Token(BaseModel):
    """The shape of W6's ``Token``: the access token is a ``SecretStr``."""

    resource_server: str | None = None
    access_token: SecretStr
    token_type: str = "Bearer"


class W6Status(BaseModel):
    """The shape of W6's ``IdentityStatus``."""

    provider: str
    state: str


class W6Provider:
    """A provider with W6's signatures: both calls async, ``resource_server`` optional."""

    async def token(self, resource_server: str | None = None) -> W6Token | None:
        if resource_server == tokens.COMPUTE_RESOURCE_SERVER:
            return W6Token(resource_server=resource_server, access_token=SecretStr("secret-1"))
        return None

    async def status(self) -> W6Status:
        return W6Status(provider="globus", state="expired")


@pytest.mark.asyncio
async def test_w6_token_and_status_shapes() -> None:
    source = W6Provider()
    assert await tokens.bearer(source, tokens.COMPUTE_RESOURCE_SERVER, what="x") == "secret-1"
    with pytest.raises(Refusal) as refused:
        await tokens.bearer(source, tokens.TRANSFER_RESOURCE_SERVER, what="files")
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN
    assert await tokens.status_of(source) == "expired"
    assert "secret-1" not in repr(await source.token(tokens.COMPUTE_RESOURCE_SERVER))


def test_provider_comes_from_the_shared_identities(monkeypatch: pytest.MonkeyPatch) -> None:
    built = W6Provider()
    asked: list[str] = []

    class Identities:
        def get(self, name: str) -> W6Provider | None:
            asked.append(name)
            return built if name == "globus" else None

    shared = Identities()
    fake = types.ModuleType("clio_relay.identity")
    fake.__dict__["shared"] = lambda: shared

    def factory_get(name: str) -> object:
        pytest.fail(f"get({name!r}) returns the factory, not a provider")

    fake.__dict__["get"] = factory_get
    monkeypatch.setitem(sys.modules, "clio_relay.identity", fake)
    assert tokens.provider("globus") is built
    assert isinstance(tokens.provider("nobody"), tokens.GrantFileProvider)
    assert asked == ["globus", "nobody"]


@pytest.mark.asyncio
async def test_transfer_stand_in_deletes_moves_and_makes_directories(tmp_path: Path) -> None:
    site = tmp_path / "site"
    service = LocalTransferService({SITE_COLLECTION: site})
    await service.mkdir(SITE_COLLECTION, "/d")
    await service.mkdir(SITE_COLLECTION, "/d")
    (site / "d" / "f.txt").write_bytes(b"f")
    await service.rename(SITE_COLLECTION, "/d/f.txt", "/d/g.txt")
    assert (site / "d" / "g.txt").read_bytes() == b"f"
    with pytest.raises(Refusal):
        await service.delete(SITE_COLLECTION, "/d", recursive=False)
    await service.delete(SITE_COLLECTION, "/d", recursive=True)
    assert not (site / "d").exists()


class RecordingTransferClient:
    """Stands in for ``globus_sdk.TransferClient`` and records the calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def submit_delete(self, data: Any) -> dict[str, str]:
        self.calls.append(("submit_delete", dict(data)))
        return {"task_id": "t-1"}

    def get_task(self, task_id: str) -> dict[str, str]:
        return {"status": "SUCCEEDED"}

    def operation_rename(self, collection: str, old: str, new: str) -> None:
        self.calls.append(("operation_rename", (collection, old, new)))

    def operation_mkdir(self, collection: str, path: str) -> None:
        self.calls.append(("operation_mkdir", (collection, path)))
        error = RuntimeError("exists")
        error.__dict__["code"] = "ExternalError.MkdirFailed.Exists"
        raise error


@pytest.mark.asyncio
async def test_sdk_transfer_deletes_moves_and_makes_directories() -> None:
    from clio_relay.adapters.globus.transfer import SdkTransferService

    service = SdkTransferService("token", poll_s=0.01)
    client = RecordingTransferClient()
    service.__dict__["_client"] = client
    await service.delete(SITE_COLLECTION, "/d", recursive=True)
    await service.rename(SITE_COLLECTION, "/a", "/b")
    await service.mkdir(SITE_COLLECTION, "/d")
    name, data = client.calls[0]
    assert name == "submit_delete"
    assert data["DATA_TYPE"] == "delete"
    assert data["endpoint"] == SITE_COLLECTION
    assert data["recursive"] is True
    assert [item["path"] for item in data["DATA"]] == ["/d"]
    assert client.calls[1] == ("operation_rename", (SITE_COLLECTION, "/a", "/b"))
    assert client.calls[2] == ("operation_mkdir", (SITE_COLLECTION, "/d"))


def test_relay_task_runs_as_plain_source(tmp_path: Path) -> None:
    """The endpoint runs the function from its source text, with no relay installed."""
    import inspect

    source = inspect.getsource(relay_task)
    script = tmp_path / "standalone.py"
    job_dir = tmp_path / "job"
    script.write_text(
        source
        + textwrap.dedent(
            f"""
            import json
            print(json.dumps(relay_task({{
                "mode": "run", "job_dir": {str(job_dir)!r},
                "argv": ["sh", "-c", "echo out; echo err >&2; exit 3"],
                "poll_s": 0.1,
            }})))
            """
        ),
        encoding="utf-8",
    )
    done = subprocess.run(
        [sys.executable, "-I", str(script)], capture_output=True, text=True, check=True
    )
    report = json.loads(done.stdout)
    assert report["state"] == "failed"
    assert report["exit_code"] == 3
    assert report["stdout_tail"] == "out\n"
    assert report["stderr_tail"] == "err\n"
    again = relay_task({"mode": "run", "job_dir": str(job_dir), "argv": ["false"]})
    assert again["exit_code"] == 3, "a retried task returns the recorded result"


def test_relay_task_serializes_for_globus_compute() -> None:
    serialize = pytest.importorskip("globus_compute_sdk.serialize")
    strategy = serialize.PureSourceTextInspect()
    packed = strategy.serialize(relay_task)
    assert "def relay_task" in packed
    restored = strategy.deserialize(packed)
    assert callable(restored)
    assert len(function_digest()) == 64
