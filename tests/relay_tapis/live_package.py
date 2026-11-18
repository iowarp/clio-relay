"""Run one reviewed package through relay's tapis adapter, the stub and the launcher.

    python -m tests.relay_tapis.live_package --root <clio-jarvis checkout> \
        --include clio_jarvis --include schemas \
        --package clio_jarvis.gray_scott --set L=64 --work <dir>

The stub runs the Tapis job on this machine, as Tapis would on the person's
system: it unpacks relay's launcher and runs ``tapisjob_app.sh``, which runs
the package through the ``jarvis`` on PATH. Run it where the package's
programs are, for example inside a Slurm allocation. It prints the Tapis job,
the jarvis output and the job directory's files.

``--service`` runs the package as a service (RL45): the driver waits for the
``--ready`` glob under the job directory, cancels the job through Tapis, and
waits for the launcher's clean stop. ``--batch <partition>`` makes the site a
Slurm system that allows a cmdPrefix and sends the job to the real Slurm with
sbatch, so the cancel is a real scancel.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.tapis import TapisAdapter, TapisConfig
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.spec import JobSpec, PackageRef
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import (
    package_archive,
    seed_host,
    sign_in,
    tapis_config,
    use_memory_keyring,
)


def guarded_adapter(stub: TapisStub, work: Path, site: str = "pkg", **system: Any) -> TapisAdapter:
    """A guarded tapis site on this machine, as the person's own system."""
    seed_host(stub, site, work / "site", **system)
    config = TapisConfig.model_validate(tapis_config(stub, launcher="guarded"))
    context = AdapterContext(
        site_id=site, state_dir=work / "relay", content=ContentStore(work / "content")
    )
    return TapisAdapter(config, context)


async def run_package(
    stub: TapisStub, adapter: TapisAdapter, spec: JobSpec, operation_id: str
) -> tuple[JobHandle, JobStatus, Path]:
    """Submit ``spec`` and follow it to the end, with no deadline."""
    handle = await adapter.run(spec, operation_id=operation_id)
    while True:
        status = await adapter.status(handle)
        if status.state.final:
            break
        await asyncio.sleep(0.2)
    system = str(handle.data["system_id"])
    return handle, status, stub.state.host_path(system, str(handle.data["job_dir"]))


def slurm_system(partition: str, minutes: int) -> dict[str, Any]:
    """The system fields of a Slurm site that lets a batch job exec the launcher."""
    return {
        "canRunBatch": True,
        "batchScheduler": "SLURM",
        "enableCmdPrefix": True,
        "batchDefaultLogicalQueue": partition,
        "batchLogicalQueues": [
            {"name": partition, "hpcQueueName": partition, "maxMinutes": minutes}
        ],
    }


async def cancel_when_ready(
    stub: TapisStub, adapter: TapisAdapter, spec: JobSpec, operation_id: str, ready: str
) -> tuple[JobHandle, JobStatus, Path, bool]:
    """Start a service, cancel it through Tapis once ``ready`` matches, wait for the stop.

    ``ready`` is a glob under the job directory.
    """
    handle = await adapter.run(spec, operation_id=operation_id)
    system = str(handle.data["system_id"])
    job_dir = stub.state.host_path(system, str(handle.data["job_dir"]))
    while not (found := sorted(job_dir.glob(ready))):
        status = await adapter.status(handle)
        if status.state.final:
            return handle, status, job_dir, False
        await asyncio.sleep(1)
    print(f"ready: {found[0]}")
    print(found[0].read_text(errors="replace"))
    status = await adapter.cancel(handle)
    print(f"cancelled through Tapis: {status.state.value}")
    while not (job_dir / "tapisjob.exitcode").exists():
        await asyncio.sleep(1)
    return handle, await adapter.status(handle), job_dir, True


def _tail(path: Path, lines: int = 25) -> str:
    if not path.is_file():
        return f"({path.name} missing)"
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])


async def _main(stub: TapisStub, args: argparse.Namespace) -> int:
    await sign_in(stub)
    system = slurm_system(args.batch, args.minutes) if args.batch else {}
    stub.state.slurm = bool(args.batch)
    adapter = guarded_adapter(stub, Path(args.work), **system)
    archive = package_archive(Path(args.root).resolve(), args.include)
    digest = adapter.context.content.put(archive)
    parameters = dict(item.split("=", 1) for item in args.set)
    spec = JobSpec(
        package=PackageRef(
            name=args.package,
            version=args.version,
            sha256=digest,
            parameters=parameters,
            service=args.service,
        )
    )
    print(f"package {args.package} {args.version} sha256 {digest} ({len(archive)} bytes)")
    if args.service:
        handle, status, job_dir, held = await cancel_when_ready(
            stub, adapter, spec, args.operation, args.ready
        )
    else:
        handle, status, job_dir = await run_package(stub, adapter, spec, args.operation)
        held = True
    job = stub.state.jobs[str(handle.native_id)]
    print(f"Tapis job {handle.native_id} history {job.history} remote {job.remote_job_id}")
    print(f"launcher exit {_tail(job_dir / 'relay.exitcode')}")
    print(f"status {status.model_dump(mode='json', exclude={'observed_at'})}")
    print(f"job directory {job_dir}")
    for item in sorted(job_dir.iterdir()):
        print(f"  {item.name}")
    print("--- stdout (jarvis)")
    print(_tail(job_dir / "stdout"))
    print("--- stderr (jarvis)")
    print(_tail(job_dir / "stderr"))
    root = job_dir / ".relay-jarvis"
    print(f"--- {root}")
    for item in sorted(root.glob("*/*")):
        print(f"  {item.relative_to(root)}")
    if args.service:
        clean = held and (job_dir / "relay.exitcode").read_text().strip() == "0"
        return 0 if clean and status.state == JobState.CANCELLED else 1
    return 0 if status.state == JobState.COMPLETED else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one reviewed package through relay")
    parser.add_argument("--root", required=True, help="the package's checkout")
    parser.add_argument(
        "--include", action="append", required=True, help="an entry of the checkout to pack"
    )
    parser.add_argument("--package", required=True, help="<repo>.<package>")
    parser.add_argument("--version", default="0.0.0")
    parser.add_argument("--set", action="append", default=[], help="key=value")
    parser.add_argument("--work", required=True)
    parser.add_argument("--operation", default="op-live-package")
    parser.add_argument("--service", action="store_true", help="run it as a service")
    parser.add_argument("--ready", help="with --service: a glob under the job directory")
    parser.add_argument("--batch", metavar="PARTITION", help="send the job to Slurm")
    parser.add_argument("--minutes", type=int, default=30, help="with --batch: the time limit")
    args = parser.parse_args()
    if args.service and not args.ready:
        parser.error("--service needs --ready")
    env = pytest.MonkeyPatch()
    use_memory_keyring(env)
    try:
        with TapisStub() as stub:
            return asyncio.run(_main(stub, args))
    finally:
        env.undo()


if __name__ == "__main__":
    sys.exit(main())
