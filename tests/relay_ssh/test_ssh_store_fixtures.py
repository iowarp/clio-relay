"""Live check on W2's SSH sites: an input the job deletes is dropped from the site's store."""

from __future__ import annotations

import asyncio
import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from clio_relay.jobs.handles import JobHandle
from clio_relay.jobs.records import new_id
from clio_relay.jobs.spec import InputRef, JobSpec
from tests.relay_ssh.test_ssh_fixtures import DEADLINE_S, RUN_DIR, build, site_shell, wait_final

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]
PROFILES = ("fx-slurm", "fx-pbs", "fx-ssh")


@pytest.fixture(scope="module", autouse=True)
def _clean_remote(fx: dict[str, str]) -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    yield
    if os.environ.get("RELAY_SSH_KEEP"):
        return
    for profile in PROFILES:
        subprocess.run(
            ["ssh", "-F", fx["FX_SSH_CONFIG"], "-o", "BatchMode=yes", profile, f"rm -rf {RUN_DIR}"],
            check=False,
            capture_output=True,
            timeout=120,
        )

@pytest.mark.parametrize("profile", PROFILES)
async def test_an_input_the_job_deletes_leaves_no_copy_in_the_store(
    fx: dict[str, str], tmp_path: Path, profile: str
) -> None:
    """Like serve's ``secrets.env``: read once and deleted by the job, then gone from the site."""
    adapter = build(fx, tmp_path, profile)
    files = await adapter.files()
    kept = adapter.context.content.put(f"kept {new_id('k')}\n".encode())

    def on_site(path: str) -> bool:
        parent = path.rsplit("/", 1)[0]
        answer = site_shell(
            fx, profile, f"ls -f {parent} >/dev/null 2>&1; test -e {path} && echo y"
        )
        return answer.strip() == "y"

    def store(digest: str) -> str:
        return f"{RUN_DIR}/{profile}/content/cas/sha256/{digest[:2]}/{digest}"

    async def start(body: str) -> tuple[JobHandle, str]:
        secret = adapter.context.content.put(f"TOKEN={new_id('s')}\n".encode())
        spec = JobSpec(
            executable="sh",
            arguments=["-c", f". serve/secrets.env; rm -f serve/secrets.env; {body}"],
            inputs=[
                InputRef(name="serve/secrets.env", sha256=secret),
                InputRef(name="kept.txt", sha256=kept),
            ],
        )
        handle = await adapter.run(spec, operation_id=new_id("op"))
        assert on_site(store(secret))
        return handle, secret

    finished, finished_secret = await start('echo "$TOKEN"')
    final = await wait_final(adapter, finished)
    assert final.state.value == "completed", final
    stdout = (await files.read(f"{finished.data['job_dir']}/stdout")).decode()
    assert stdout.startswith("s_")
    assert not on_site(store(finished_secret))
    assert on_site(store(kept))
    assert on_site(f"{finished.data['job_dir']}/kept.txt")

    running, running_secret = await start("sleep 3600")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + DEADLINE_S
    while on_site(f"{running.data['job_dir']}/serve/secrets.env"):
        assert loop.time() < deadline, "the job never deleted its copy"
        await asyncio.sleep(2)
    # The job is still running, so its input stays in the store until it ends.
    assert on_site(store(running_secret))
    await adapter.cancel(running)
    final = await wait_final(adapter, running)
    assert final.state.value == "cancelled", final
    assert not on_site(store(running_secret))
    assert on_site(store(kept))
