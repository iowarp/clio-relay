"""The warm holder: it keeps an allocation while steps run, then lets it go.

It runs under /bin/sh with stand-in ``squeue`` and ``sacct`` programs.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path

import pytest

from clio_relay.guard.warm import HOLDER, WARM_EXECUTABLE

pytestmark = pytest.mark.release_platform("posix")


def _program(directory: Path, name: str, body: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(body)
    path.chmod(0o755)
    return path


def _env(tmp_path: Path, bin_dir: Path, **extra: str) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key != "SLURM_JOB_ID"}
    env["PATH"] = f"{bin_dir}:/usr/bin:/bin"
    env.update(extra)
    return env


def _holder(
    env: dict[str, str], cwd: Path, limit: int, step: int
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/sh", "-c", HOLDER, WARM_EXECUTABLE, str(limit), str(step)],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_holder_waits_while_a_step_runs_then_lets_go(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    calls = tmp_path / "calls"
    _program(bin_dir, "sacct", "#!/bin/sh\nprintf '7|Unknown\\n7.batch|Unknown\\n'\n")
    # Busy (a step and a failing scheduler) for the first four looks, then idle.
    _program(
        bin_dir,
        "squeue",
        f"""#!/bin/sh
echo x >> {calls}
n=$(wc -l < {calls})
[ "$n" -eq 2 ] && exit 1
if [ "$n" -le 4 ]; then printf '7.batch\\n7.0\\n'; else printf '7.batch\\n7.extern\\n'; fi
""",
    )
    done = _holder(_env(tmp_path, bin_dir, SLURM_JOB_ID="7"), tmp_path, limit=2, step=1)
    assert done.returncode == 0, done.stderr
    assert "relay warm allocation 7: held" in done.stdout
    assert re.search(r"idle for [0-9]+ s; released", done.stdout), done.stdout
    looks = len(calls.read_text().splitlines())
    assert looks >= 7, "four busy looks, then idle looks for two seconds"


def test_a_short_step_between_looks_still_counts_as_use(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    start = int(time.time())
    ended = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(start + 3))
    # squeue never sees a step, but sacct says one ended three seconds after the start.
    _program(bin_dir, "squeue", "#!/bin/sh\nprintf '7.batch\\n'\n")
    steps = f"7|Unknown\\n7.batch|Unknown\\n7.0|{ended}\\n"
    _program(bin_dir, "sacct", f"#!/bin/sh\nprintf '{steps}'\n")
    began = time.monotonic()
    done = _holder(_env(tmp_path, bin_dir, SLURM_JOB_ID="7"), tmp_path, limit=2, step=1)
    assert done.returncode == 0, done.stderr
    assert "released" in done.stdout
    assert time.monotonic() - began >= 4, "released before two idle seconds after the step"


def test_a_holder_without_slurm_watches_its_marker(tmp_path: Path) -> None:
    done = _holder(_env(tmp_path, tmp_path / "bin"), tmp_path, limit=1, step=1)
    assert done.returncode == 0
    assert "released" in done.stdout
