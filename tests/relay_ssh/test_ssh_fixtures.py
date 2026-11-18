"""LQ5 to LQ9 for the ``ssh`` adapter, on W2's fixture sites.

Collected only where the fixtures' env file exists (see conftest.py). The waits
below are deadlines for a stuck job, not timings: a slow site only makes them
take longer.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import subprocess
import sys
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.ssh import SshAdapter
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobHandle, JobStatus
from clio_relay.jobs.records import new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import InputRef, JobAttributes, JobSpec

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

RUN_DIR = f".clio-relay-test/{new_id('w7lq')}"
DEADLINE_S = 600


def build(fx: dict[str, str], state: Path, profile: str, **config: Any) -> SshAdapter:
    context = AdapterContext(site_id=profile, state_dir=state, content=ContentStore(state / "cas"))
    settings: dict[str, Any] = {
        "ssh_profile": profile,
        "ssh_config": fx["FX_SSH_CONFIG"],
        "remote_dir": f"{RUN_DIR}/{profile}",
        **config,
    }
    adapter = registry.build("ssh", settings, context)
    assert isinstance(adapter, SshAdapter)
    return adapter


def ssh_children() -> list[str]:
    """Return the ``ssh`` and ``scp`` processes this test process still has."""
    me = str(os.getpid())
    found: list[str] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = dict(
                line.split(":\t", 1)
                for line in (entry / "status").read_text().splitlines()
                if ":\t" in line
            )
        except OSError:
            continue
        if fields.get("PPid", "").strip() == me and fields.get("Name", "").strip() in (
            "ssh",
            "scp",
        ):
            found.append(f"{entry.name} {fields['Name'].strip()}")
    return found


async def wait_final(adapter: SshAdapter, handle: JobHandle) -> JobStatus:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + DEADLINE_S
    while True:
        status = await adapter.status(handle)
        assert ssh_children() == []
        if status.state.final:
            return status
        assert loop.time() < deadline, f"job still {status.state} after {DEADLINE_S}s"
        await asyncio.sleep(2)


@pytest.fixture(scope="module", autouse=True)
def _clean_remote(fx: dict[str, str]) -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    yield
    if os.environ.get("RELAY_SSH_KEEP"):
        return
    for profile in ("fx-ssh", "fx-slurm", "fx-pbs"):
        subprocess.run(
            ["ssh", "-F", fx["FX_SSH_CONFIG"], "-o", "BatchMode=yes", profile, f"rm -rf {RUN_DIR}"],
            check=False,
            capture_output=True,
            timeout=120,
        )


async def test_lq5_a_slurm_job_completes_after_relay_restarts(
    fx: dict[str, str], tmp_path: Path
) -> None:
    operation_id = new_id("op")
    first = build(fx, tmp_path / "before", "fx-slurm")
    spec = JobSpec(executable="sh", arguments=["-c", "sleep 15; echo lq5-done"])
    handle = await first.run(spec, operation_id=operation_id)
    assert handle.data["scheduler"] == "slurm"
    status = await first.status(handle)
    assert not status.state.final
    assert ssh_children() == []
    del first  # relay closes; nothing of it is kept on this machine

    await asyncio.sleep(20)
    second = build(fx, tmp_path / "after", "fx-slurm")
    found = await second.find(operation_id)
    assert found is not None
    assert found.native_id == handle.native_id
    final = await wait_final(second, found)
    assert final.state.value == "completed", final
    assert final.exit_code == 0
    files = await second.files()
    stdout = await files.read(f"{found.data['job_dir']}/stdout")
    assert stdout.strip() == b"lq5-done"
    assert [ref.name for ref in final.outputs] == ["stderr", "stdout"]
    # A second run of the same operation submits nothing new.
    again = await second.run(spec, operation_id=operation_id)
    assert again.native_id == handle.native_id


def site_shell(fx: dict[str, str], profile: str, command: str) -> str:
    done = subprocess.run(
        ["ssh", "-F", fx["FX_SSH_CONFIG"], "-o", "BatchMode=yes", profile, command],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return done.stdout


def scheduler_limit(fx: dict[str, str], profile: str, native_id: str) -> str | None:
    """Return the walltime the scheduler holds for a job, as it reports it."""
    if profile == "fx-slurm":
        line = site_shell(fx, profile, f"scontrol show job -o {native_id}")
        return next(
            field.split("=", 1)[1] for field in line.split() if field.startswith("TimeLimit=")
        )
    report = json.loads(
        site_shell(fx, profile, f"PATH=/opt/pbs/bin:$PATH qstat -f -F json {native_id}")
    )
    (job,) = report["Jobs"].values()
    return job.get("Resource_List", {}).get("walltime")


@pytest.mark.parametrize("profile", ["fx-slurm", "fx-pbs"])
async def test_a_job_without_a_duration_gets_the_schedulers_limit(
    fx: dict[str, str], tmp_path: Path, profile: str
) -> None:
    """Relay sets no walltime of its own: no directive, and no 10 minute cap."""
    adapter = build(fx, tmp_path, profile)
    files = await adapter.files()
    # The fixture's default Slurm partition and PBS queue set no walltime.
    unset = {"fx-slurm": "UNLIMITED", "fx-pbs": None}[profile]
    timed = {"fx-slurm": "00:15:00", "fx-pbs": "00:15:00"}[profile]
    cases: list[tuple[JobSpec, bool, str | None]] = [
        (JobSpec(executable="sleep", arguments=["3600"]), False, unset),
        (
            JobSpec(
                executable="sleep",
                arguments=["3600"],
                attributes=JobAttributes(duration=timedelta(minutes=15)),
            ),
            True,
            timed,
        ),
    ]
    for spec, directive, limit in cases:
        handle = await adapter.run(spec, operation_id=new_id("op"))
        try:
            script = (await files.read(f"{handle.data['job_dir']}/submit.sh")).decode()
            has_directive = "#SBATCH --time=" in script or "#PBS -l walltime=" in script
            assert has_directive == directive, script
            assert handle.native_id is not None
            assert scheduler_limit(fx, profile, handle.native_id) == limit
        finally:
            await adapter.cancel(handle)


async def test_lq6_the_pbs_script_and_status_come_from_psij(
    fx: dict[str, str], tmp_path: Path
) -> None:
    adapter = build(fx, tmp_path, "fx-pbs")
    handle = await adapter.run(
        JobSpec(executable="sh", arguments=["-c", "echo lq6 $PBS_JOBID"]),
        operation_id=new_id("op"),
    )
    assert handle.data["scheduler"] == "pbs"
    final = await wait_final(adapter, handle)
    assert final.state.value == "completed", final
    files = await adapter.files()
    job_dir = handle.data["job_dir"]
    script = (await files.read(f"{job_dir}/submit.sh")).decode()
    assert "#PBS -N relay-" in script
    assert "/psij/" in script and "single_launch.sh" in script
    assert f"{handle.native_id}.ec" not in [ref.name for ref in final.outputs]
    stdout = (await files.read(f"{job_dir}/stdout")).decode()
    assert stdout.strip() == f"lq6 {handle.native_id}"


async def test_lq7_a_rerun_stages_nothing(fx: dict[str, str], tmp_path: Path) -> None:
    adapter = build(fx, tmp_path, "fx-slurm")
    chunk = hashlib.sha256(b"lq7").digest() * 2048
    data = os.urandom(1024) + chunk * (200 * 1024 * 1024 // len(chunk))
    digest = adapter.context.content.put(data)
    del data
    uploads: list[list[Path]] = []
    sizes: list[int] = []
    upload = adapter.transport.upload

    async def recording(sources: list[Path], destination: str) -> None:
        uploads.append(list(sources))
        sizes.extend(path.stat().st_size for path in sources)
        await upload(sources, destination)

    adapter.transport.upload = recording  # pyright: ignore[reportAttributeAccessIssue]
    blob = adapter.context.content.path_for(digest)
    spec = JobSpec(
        executable="sha256sum",
        arguments=["big.bin"],
        inputs=[InputRef(name="big.bin", sha256=digest)],
    )
    first_sizes = 0
    for attempt in range(2):
        handle = await adapter.run(spec, operation_id=new_id("op"))
        final = await wait_final(adapter, handle)
        assert final.state.value == "completed", final
        files = await adapter.files()
        stdout = (await files.read(f"{handle.data['job_dir']}/stdout")).decode()
        assert stdout.split()[0] == digest
        staged = [path for path in uploads[attempt] if path == blob]
        assert staged == ([blob] if attempt == 0 else []), uploads[attempt]
        if attempt == 0:
            first_sizes = len(sizes)
    assert all(size < 1024 * 1024 for size in sizes[first_sizes:]), sizes


async def test_lq8_a_detached_job_is_read_by_a_later_poll(
    fx: dict[str, str], tmp_path: Path
) -> None:
    operation_id = new_id("op")
    first = build(fx, tmp_path / "before", "fx-ssh")
    spec = JobSpec(
        executable="sh", arguments=["-c", "sleep 8; echo lq8-out; echo lq8-err >&2; exit 7"]
    )
    handle = await first.run(spec, operation_id=operation_id)
    assert handle.data["mode"] == "detached"
    assert handle.native_id is not None and handle.native_id.startswith("pid:")
    assert not (await first.status(handle)).state.final
    del first

    await asyncio.sleep(10)
    later = build(fx, tmp_path / "after", "fx-ssh")
    found = await later.find(operation_id)
    assert found is not None
    assert found == handle
    final = await wait_final(later, found)
    assert final.state.value == "failed"
    assert final.exit_code == 7
    files = await later.files()
    job_dir = handle.data["job_dir"]
    assert (await files.read(f"{job_dir}/stdout")).strip() == b"lq8-out"
    assert (await files.read(f"{job_dir}/stderr")).strip() == b"lq8-err"


@pytest.mark.parametrize("interactive", [False, True])
async def test_lq9_a_code_site_refuses_and_names_worker(
    fx: dict[str, str], tmp_path: Path, interactive: bool
) -> None:
    config: dict[str, Any] = {"interactive_sign_in": True} if interactive else {}
    adapter = build(fx, tmp_path, "fx-otp", **config)
    from clio_relay.adapters.ssh import transport

    if interactive and transport._askpass_module() is not None:  # pyright: ignore[reportPrivateUsage]
        pytest.skip("W6's askpass is installed, so this site can ask; see the sign-in test")
    assert await adapter.sign_in_needed() is None
    for call in (
        adapter.describe(),
        adapter.run(JobSpec(executable="true"), operation_id=new_id("op")),
    ):
        with pytest.raises(Refusal) as refused:
            await call
        assert refused.value.reason == RefusalReason.INTERACTIVE_AUTH_REQUIRED
        assert refused.value.data["adapter"] == "worker"
        assert "worker" in refused.value.message


def totp_helper(fx: dict[str, str], directory: Path) -> tuple[Path, Path]:
    """Write an askpass program that answers the code prompt, and log each prompt."""
    log = directory / "prompts.log"
    helper = directory / "totp-askpass"
    helper.write_text(
        f"""#!{sys.executable}
import base64, hashlib, hmac, struct, sys, time
prompt = sys.argv[1] if len(sys.argv) > 1 else ""
with open({str(log)!r}, "a") as log:
    log.write(prompt.strip() + "\\n")
if "code" not in prompt.lower():
    sys.exit(1)
secret = open({fx["FX_OTP_SECRET_FILE"]!r}).read().strip()
key = base64.b32decode(secret + "=" * (-len(secret) % 8))
digest = hmac.new(key, struct.pack(">Q", int(time.time()) // 30), hashlib.sha1).digest()
offset = digest[-1] & 15
code = (struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF) % 1000000
print(f"{{code:06d}}")
""",
        encoding="utf-8",
    )
    helper.chmod(helper.stat().st_mode | stat.S_IXUSR)
    return helper, log


async def test_lq9_the_code_site_gets_in_once_askpass_answers(
    fx: dict[str, str], tmp_path: Path
) -> None:
    helper, log = totp_helper(fx, tmp_path)
    adapter = build(fx, tmp_path / "state", "fx-otp", interactive_sign_in=True, askpass=str(helper))
    try:
        need = await adapter.sign_in_needed()
        if need is not None:  # W6's page would run this when the person presses start
            assert need.provider == "ssh"
            await adapter.transport.starter()(adapter.transport.environment() or {})
        assert await adapter.transport.master_up()
        await adapter.sign_in_done({})
        info = await adapter.describe()
        assert info.facts["host"]
        handle = await adapter.run(
            JobSpec(executable="sh", arguments=["-c", "echo otp-in"]), operation_id=new_id("op")
        )
        final = await wait_final(adapter, handle)
        assert final.state.value == "completed", final
        files = await adapter.files()
        assert (await files.read(f"{handle.data['job_dir']}/stdout")).strip() == b"otp-in"
        prompts = [line for line in log.read_text().splitlines() if line]
        assert len(prompts) == 1, prompts  # one sign-in served every call
    finally:
        subprocess.run(
            [
                "ssh",
                "-F",
                fx["FX_SSH_CONFIG"],
                "-o",
                "BatchMode=yes",
                "-o",
                f"ControlPath={adapter.transport.control_path}",
                "fx-otp",
                f"rm -rf {RUN_DIR}",
            ],
            check=False,
            capture_output=True,
            timeout=60,
        )
        await adapter.transport.stop_master()
        assert not await adapter.transport.master_up()
