"""The shell scripts relay sends to a site: the package runner, the warm holder, RL42.

They run under /bin/sh or bash with stand-in ``jarvis``, ``scontrol`` and
``squeue`` programs that log what they were asked.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.local import LocalAdapter, LocalConfig
from clio_relay.guard.warm import HOLDER, WARM_EXECUTABLE
from clio_relay.jarvis_site import RUNNER, add_clio_jarvis, archive_package
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import Refusal

from .conftest import make_repo

pytestmark = pytest.mark.release_platform("posix")

FAKE_JARVIS = """#!/bin/sh
echo "JARVIS_ROOT=${JARVIS_ROOT:-} $*" >> "$JARVIS_LOG"
case "$1 $2" in
  "repo list") cat "$JARVIS_REPOS" 2>/dev/null || true ;;
  "repo add") echo "9. $(basename "$3") ($3) ok" >> "$JARVIS_REPOS" ;;
  "ppl run") echo "ran in $(pwd)" ;;
esac
"""


def _program(directory: Path, name: str, body: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(body)
    path.chmod(0o755)
    return path


def _env(tmp_path: Path, bin_dir: Path, **extra: str) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in ("SLURM_JOB_ID", "SLURM_JOB_NODELIST", "PBS_NODEFILE", "PBS_JOBID", "JARVIS_ROOT")
    }
    env["PATH"] = f"{bin_dir}:/usr/bin:/bin"
    env["JARVIS_LOG"] = str(tmp_path / "jarvis.log")
    env["JARVIS_REPOS"] = str(tmp_path / "repos.txt")
    env.update(extra)
    return env


def _job_dir(tmp_path: Path) -> tuple[Path, str]:
    data = archive_package(make_repo(tmp_path / "src"), "demo.hello")
    job = tmp_path / "job"
    digest = ContentStore(tmp_path / "content").put(data)
    (job / ".relay-package").mkdir(parents=True)
    (job / ".relay-package" / f"{digest}.tar.gz").write_bytes(data)
    return job, digest


def _runner(job: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/bash", "-c", RUNNER, "clio-relay-package", *args],
        cwd=job,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_runner_builds_a_private_jarvis_pipeline_and_runs_it(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    bin_dir = tmp_path / "bin"
    _program(bin_dir, "jarvis", FAKE_JARVIS)
    _program(bin_dir, "scontrol", "#!/bin/sh\nprintf 'n1\\nn2\\n'\n")
    workdir = tmp_path / "work"
    workdir.mkdir()
    env = _env(tmp_path, bin_dir, SLURM_JOB_NODELIST="n[1-2]")
    done = _runner(job, env, digest, "demo.hello", "jarvis", str(workdir), "out=r.txt", "n=2")
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == f"ran in {workdir}"
    root = job / ".relay-jarvis"
    log = (tmp_path / "jarvis.log").read_text().splitlines()
    assert log == [
        f"JARVIS_ROOT={root} init {root}/config {root}/private {root}/shared",
        f"JARVIS_ROOT={root} repo add {root}/src/demo",
        f"JARVIS_ROOT={root} hostfile set {root}/hostfile",
        f"JARVIS_ROOT={root} ppl create relay_{digest[:12]}",
        f"JARVIS_ROOT={root} ppl append demo.hello pkg out=r.txt n=2",
        f"JARVIS_ROOT={root} ppl run",
    ]
    assert (root / "hostfile").read_text() == "n1\nn2\n"
    assert (root / "src" / "demo" / "demo" / "hello" / "pkg.py").is_file()
    assert not (root / "src" / "demo" / "demo" / "other").exists()


def test_the_runner_runs_only_the_reviewed_bytes(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    bin_dir = tmp_path / "bin"
    _program(bin_dir, "jarvis", FAKE_JARVIS)
    env = _env(tmp_path, bin_dir)
    archive = job / ".relay-package" / f"{digest}.tar.gz"
    archive.write_bytes(archive.read_bytes() + b"\0")
    done = _runner(job, env, digest, "demo.hello", "jarvis", "")
    assert done.returncode == 65
    assert "not the reviewed" in done.stderr
    assert not (tmp_path / "jarvis.log").exists()
    missing = _runner(job, env, "e" * 64, "demo.hello", "jarvis", "")
    assert missing.returncode == 66


def test_the_runner_says_when_jarvis_is_missing(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    done = _runner(
        job, _env(tmp_path, tmp_path / "bin"), digest, "demo.hello", "no-such-jarvis", ""
    )
    assert done.returncode == 69
    assert "no-such-jarvis is not on PATH" in done.stderr


def test_the_runner_expands_a_home_jarvis_path(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    home = tmp_path / "home"
    _program(home / "jv" / "bin", "jarvis", FAKE_JARVIS)
    env = _env(tmp_path, tmp_path / "bin", HOME=str(home))
    done = _runner(job, env, digest, "demo.hello", "~/jv/bin/jarvis", "")
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == f"ran in {job}"


def _holder(env: dict[str, str], cwd: Path, limit: int, step: int) -> subprocess.CompletedProcess[str]:
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
    assert "idle for 2 s; released" in done.stdout
    looks = len(calls.read_text().splitlines())
    assert looks >= 7, "four busy looks, two idle steps and the final check"


def test_a_holder_without_slurm_watches_its_marker(tmp_path: Path) -> None:
    done = _holder(_env(tmp_path, tmp_path / "bin"), tmp_path, limit=1, step=1)
    assert done.returncode == 0
    assert "released" in done.stdout


def _clio_jarvis_remote(tmp_path: Path) -> tuple[Path, str]:
    remote = tmp_path / "clio-jarvis"
    package = remote / "clio_jarvis" / "clio_jarvis" / "hello"
    package.mkdir(parents=True)
    (package / "pkg.py").write_text("# hello\n")

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(remote), *args], capture_output=True, text=True, check=True
        ).stdout.strip()

    git("init", "-q")
    git("add", ".")
    git("-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", "commit", "-qm", "x")
    git("tag", "v0.1.0")
    return remote, git("rev-parse", "HEAD")


@pytest.mark.asyncio
async def test_rl42_adds_a_tagged_clio_jarvis_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote, commit = _clio_jarvis_remote(tmp_path)
    jarvis = _program(tmp_path / "bin", "jarvis", FAKE_JARVIS)
    for key, value in _env(tmp_path, tmp_path / "bin").items():
        monkeypatch.setenv(key, value)
    adapter = LocalAdapter(
        LocalConfig(),
        AdapterContext(
            site_id="here", state_dir=tmp_path / "state", content=ContentStore(tmp_path / "c")
        ),
    )
    base = tmp_path / "site" / "clio-jarvis"
    first = await add_clio_jarvis(
        adapter, url=str(remote), tag="v0.1.0", commit=commit, base=str(base), jarvis=str(jarvis)
    )
    repo = base / "v0.1.0" / "clio_jarvis"
    assert first == {"tag": "v0.1.0", "commit": commit, "repo": str(repo), "added": True}
    again = await add_clio_jarvis(
        adapter, url=str(remote), tag="v0.1.0", base=str(base), jarvis=str(jarvis)
    )
    assert again["added"] is False
    adds = [line for line in (tmp_path / "jarvis.log").read_text().splitlines() if "repo add" in line]
    assert adds == [f"JARVIS_ROOT= repo add {repo}"]
    with pytest.raises(Refusal) as raised:
        await add_clio_jarvis(
            adapter, url=str(remote), tag="v0.1.0", commit="0" * 40, base=str(base), jarvis=str(jarvis)
        )
    assert "not " + "0" * 40 in raised.value.message
