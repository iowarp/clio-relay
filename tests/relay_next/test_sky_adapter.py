"""The ``sky`` adapter (RL14) through its real bridge, against a stand-in ``sky`` package.

The adapter runs ``bridge.py`` in a separate Python, as it runs SkyPilot's own
environment in use. Here that Python is this one, with the stand-in package on
``PYTHONPATH``; it runs managed jobs as local processes and prices from a fixed
table. These are mock results, never live passes.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.sky import FAKE_SKY_PATH, SkyAdapter, SkyConfig
from clio_relay.adapters.sky.adapter import run_command
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobAttributes, JobResources, JobSpec, PackageRef
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget, wait_final

pytestmark = pytest.mark.release_platform("posix")


class KeyringSky(SkyAdapter):
    """The adapter with an in-memory secret store."""

    name: ClassVar[str] = "sky-keyring"
    secrets: ClassVar[dict[str, str]] = {}

    def _secret(self, name: str) -> str | None:
        return self.secrets.get(name)


@pytest.fixture(autouse=True)
def registered_adapter() -> Any:
    registry.register(KeyringSky, "sky-keyring")
    yield
    registry.unregister("sky-keyring")
    KeyringSky.secrets.clear()


def local_config(tmp_path: Path) -> dict[str, Any]:
    return {
        "python": sys.executable,
        "infra": "aws",
        "cpus": "2+",
        "env": {"PYTHONPATH": str(FAKE_SKY_PATH), "FAKE_SKY_ROOT": str(tmp_path / "fake-sky")},
    }


class TestSkyConformance(AdapterConformance):
    @pytest.fixture
    def target(self, tmp_path: Path) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="sky",
            config=local_config(tmp_path),
            state_dir=tmp_path,
            job_timeout_s=60,
            poll_s=0.2,
        )


def _context(tmp_path: Path) -> AdapterContext:
    return AdapterContext(
        site_id="aws",
        state_dir=tmp_path / "adapter",
        content=ContentStore(tmp_path / "content"),
    )


def _build(tmp_path: Path, cls: type[SkyAdapter] = SkyAdapter, **overrides: Any) -> SkyAdapter:
    config = SkyConfig.model_validate({**local_config(tmp_path), **overrides})
    return cls(config, _context(tmp_path))


def _hour(**attributes: Any) -> JobSpec:
    return JobSpec(
        executable="true",
        resources=JobResources(node_count=2),
        attributes=JobAttributes(duration=timedelta(hours=1), **attributes),
    )


@pytest.mark.asyncio
async def test_price_comes_from_skypilot_in_dollars(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    charge = await adapter.price(_hour())
    assert charge.unit == "USD"
    assert charge.amount == pytest.approx(0.192)
    assert charge.nodes == 2
    assert charge.walltime_s == 3600
    assert charge.node_hours == pytest.approx(2.0)
    assert charge.site == "aws"


@pytest.mark.asyncio
async def test_price_follows_spot_and_job_overrides(tmp_path: Path) -> None:
    spot = await _build(tmp_path, use_spot=True).price(_hour())
    assert spot.amount == pytest.approx(0.192 * 0.125)
    bigger = await _build(tmp_path).price(_hour(custom_attributes={"sky.cpus": "8"}))
    assert bigger.amount == pytest.approx(0.768)
    gpus = await _build(tmp_path, accelerators="A100:8").price(_hour())
    assert gpus.amount == pytest.approx(2 * 21.95764)
    with pytest.raises(Refusal) as refused:
        await _build(tmp_path, accelerators="A100:1").price(_hour())
    assert "A100:8" in refused.value.message


def test_a_missing_skypilot_python_is_adapter_unavailable(tmp_path: Path) -> None:
    with pytest.raises(Refusal) as refused:
        registry.build("sky", {"python": str(tmp_path / "no-such-python")}, _context(tmp_path))
    assert refused.value.reason == RefusalReason.ADAPTER_UNAVAILABLE
    assert "uv venv" in refused.value.message


@pytest.mark.asyncio
async def test_a_python_without_skypilot_is_adapter_unavailable(tmp_path: Path) -> None:
    adapter = _build(tmp_path, env={})
    with pytest.raises(Refusal) as refused:
        await adapter.price(_hour())
    assert refused.value.reason == RefusalReason.ADAPTER_UNAVAILABLE
    assert "cannot import sky" in refused.value.message
    info = await adapter.describe()
    assert "cannot import sky" in info.facts["skypilot_error"]


@pytest.mark.asyncio
async def test_describe_reports_the_skypilot_version(tmp_path: Path) -> None:
    info = await _build(tmp_path).describe()
    assert info.facts["skypilot_version"] == "0.14.3-fake"
    assert info.facts["infra"] == "aws"
    assert info.verbs == ["describe", "run"]


@pytest.mark.asyncio
async def test_keys_from_the_keyring_reach_skypilot(tmp_path: Path) -> None:
    credentials = {"provider": "aws", "env": {"AWS_ACCESS_KEY_ID": "aws-access-key-id"}}
    adapter = _build(tmp_path, KeyringSky, credentials=credentials)
    need = await adapter.sign_in_needed()
    assert need is not None and need.provider == "keyring:aws-access-key-id"
    assert "aws" in need.message
    with pytest.raises(Refusal) as refused:
        await adapter.run(JobSpec(executable="true"), operation_id="op0")
    assert refused.value.reason == RefusalReason.NEEDS_SIGN_IN
    assert refused.value.data["provider"] == "keyring:aws-access-key-id"
    assert refused.value.data["cloud"] == "aws"
    KeyringSky.secrets["aws-access-key-id"] = "AKIAEXAMPLE"
    assert await adapter.sign_in_needed() is None
    spec = JobSpec(executable="sh", arguments=["-c", "echo key=$AWS_ACCESS_KEY_ID"])
    handle = await adapter.run(spec, operation_id="op1")
    status, _ = await wait_final(adapter, handle, 60, 0.2)
    assert status.state == JobState.COMPLETED
    log = next(Path(item.path) for item in status.outputs if item.name == "run.log")
    assert "key=AKIAEXAMPLE" in log.read_text()


def test_secrets_come_from_relays_token_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[str] = []

    class Store:
        def secret(self, name: str) -> str | None:
            asked.append(name)
            return "from-store" if name == "aws-access-key-id" else None

    fake = types.ModuleType("clio_relay.identity.store")
    fake.__dict__["TokenStore"] = Store
    monkeypatch.setitem(sys.modules, "clio_relay.identity.store", fake)
    credentials: dict[str, object] = {
        "provider": "aws",
        "env": {"AWS_ACCESS_KEY_ID": "aws-access-key-id"},
    }
    adapter = _build(tmp_path, credentials=credentials)
    assert adapter._credential_env() == {"AWS_ACCESS_KEY_ID": "from-store"}  # pyright: ignore[reportPrivateUsage]  # pyright: ignore[reportPrivateUsage]
    assert asked == ["aws-access-key-id"]


def test_the_old_keyring_service_setting_is_refused(tmp_path: Path) -> None:
    credentials: dict[str, object] = {"provider": "aws", "service": "clio-relay", "env": {}}
    with pytest.raises(ValidationError):
        SkyConfig.model_validate({**local_config(tmp_path), "credentials": credentials})


@pytest.mark.asyncio
async def test_launch_request_carries_the_job(tmp_path: Path) -> None:
    adapter = _build(tmp_path, setup="pip list")
    spec = JobSpec(
        executable="echo",
        arguments=["a b"],
        environment={"X": "1"},
        resources=JobResources(node_count=3),
        attributes=JobAttributes(custom_attributes={"sky.region": "us-west-2"}),
    )
    handle = await adapter.run(spec, operation_id="op1")
    task = json.loads(
        (tmp_path / "fake-sky" / "jobs" / str(handle.native_id) / "task.json").read_text()
    )
    assert task["name"] == "relay-op1"
    assert task["run"] == "echo 'a b'"
    assert task["setup"] == "pip list"
    assert task["envs"] == {"X": "1"}
    assert task["num_nodes"] == 3
    assert task["resources"]["region"] == "us-west-2"
    assert task["resources"]["cpus"] == "2+"
    await wait_final(adapter, handle, 60, 0.2)


def test_run_command_renders_launcher_paths_and_directory() -> None:
    spec = JobSpec(
        executable="./app",
        arguments=["--n", "4"],
        launcher="mpirun -np 4",
        stdin_path="in.txt",
        stdout_path="out.txt",
        stderr_path="err.txt",
        directory="run 1",
    )
    assert run_command(spec) == (
        "mkdir -p 'run 1' && cd 'run 1' && mpirun -np 4 ./app --n 4 < in.txt > out.txt 2> err.txt"
    )
    with pytest.raises(Refusal) as refused:
        run_command(JobSpec(package=PackageRef(name="p", version="1", sha256="0" * 64)))
    assert refused.value.reason == RefusalReason.INVALID_SPEC


@pytest.mark.asyncio
async def test_a_failed_job_without_a_code_records_one(tmp_path: Path) -> None:
    adapter = _build(tmp_path)
    handle = await adapter.run(JobSpec(executable="false"), operation_id="op1")
    status, _ = await wait_final(adapter, handle, 60, 0.2)
    assert status.state == JobState.FAILED
    assert status.exit_code == 1
    assert status.message is not None and "no exit code" in status.message


@pytest.mark.asyncio
async def test_inputs_and_a_site_workdir_refuse(tmp_path: Path) -> None:
    adapter = _build(tmp_path, workdir=str(tmp_path))
    digest = adapter.context.content.put(b"x")
    spec = JobSpec(executable="cat", inputs=[InputRef(name="in.txt", sha256=digest)])
    with pytest.raises(Refusal) as refused:
        await adapter.run(spec, operation_id="op1")
    assert refused.value.reason == RefusalReason.INVALID_SPEC
