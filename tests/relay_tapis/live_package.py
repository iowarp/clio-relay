"""Run one reviewed package through relay's tapis adapter, the stub and the launcher.

    python -m tests.relay_tapis.live_package --root <clio-jarvis checkout> \
        --include clio_jarvis --include schemas \
        --package clio_jarvis.gray_scott --set L=64 --work <dir>

The stub runs the Tapis job on this machine, as Tapis would on the person's
system: it unpacks relay's launcher and runs ``tapisjob_app.sh``, which runs
the package through the ``jarvis`` on PATH. Run it where the package's
programs are, for example inside a Slurm allocation. It prints the Tapis job,
the jarvis output and the job directory's files.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

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


def guarded_adapter(stub: TapisStub, work: Path, site: str = "pkg") -> TapisAdapter:
    """A guarded tapis site on this machine, as the person's own system."""
    seed_host(stub, site, work / "site")
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


def _tail(path: Path, lines: int = 25) -> str:
    if not path.is_file():
        return f"({path.name} missing)"
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])


async def _main(stub: TapisStub, args: argparse.Namespace) -> int:
    await sign_in(stub)
    adapter = guarded_adapter(stub, Path(args.work))
    archive = package_archive(Path(args.root).resolve(), args.include)
    digest = adapter.context.content.put(archive)
    parameters = dict(item.split("=", 1) for item in args.set)
    spec = JobSpec(
        package=PackageRef(
            name=args.package, version=args.version, sha256=digest, parameters=parameters
        )
    )
    print(f"package {args.package} {args.version} sha256 {digest} ({len(archive)} bytes)")
    handle, status, job_dir = await run_package(stub, adapter, spec, args.operation)
    job = stub.state.jobs[str(handle.native_id)]
    print(f"Tapis job {handle.native_id} history {job.history}")
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
    args = parser.parse_args()
    env = pytest.MonkeyPatch()
    use_memory_keyring(env)
    try:
        with TapisStub() as stub:
            return asyncio.run(_main(stub, args))
    finally:
        env.undo()


if __name__ == "__main__":
    sys.exit(main())
