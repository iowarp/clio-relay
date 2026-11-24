"""The shell scripts relay sends to a site: the JARVIS site script and the warm holder.

They run under /bin/sh or bash with stand-in ``jarvis``, ``scontrol`` and
``squeue`` programs that log what they were asked.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path

import pytest

from clio_relay.guard.warm import HOLDER, WARM_EXECUTABLE
from clio_relay.jarvis_site import archive_package
from clio_relay.jarvis_submit import (
    EXIT_DIGEST,
    EXIT_MISSING,
    EXIT_NO_JARVIS,
    EXIT_REFUSED,
    EXIT_USAGE,
    EXITCODE_FILE,
    JARVIS_FILE,
    PIPELINE_FILE,
    REFUSAL_FILE,
    SERVICE_NODE_FILE,
    SITE_SCRIPT,
    SUBMISSION_FILE,
    jarvis_refused,
    missing_jarvis,
    parse_handle,
)
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.refusals import RefusalReason

from .conftest import make_repo

pytestmark = pytest.mark.release_platform("posix")

FAKE_JARVIS = """#!/bin/sh
echo "JARVIS_ROOT=${JARVIS_ROOT:-} $*" >> "$JARVIS_LOG"
[ -z "${JARVIS_INSTALL_ANSWERS:-}" ] || echo "answers $JARVIS_INSTALL_ANSWERS" >> "$JARVIS_LOG"
case "$1 $2" in
  "init "*)
    [ "${JARVIS_NO_BUILTIN:-}" != 1 ] \\
      || echo "no_builtin: true" > "$JARVIS_ROOT/jarvis_config.yaml" ;;
  "repo list") cat "$JARVIS_REPOS" 2>/dev/null || true ;;
  "repo add") echo "9. $(basename "$3") ($3) ok" >> "$JARVIS_REPOS" ;;
  "ppl run") echo "ran in $(pwd)" ;;
  "ppl install")
    case " $* " in
      *" +submit "*)
        printf '{"execution_id":"install","schema_version":"jarvis.execution.handle.v1"}\\n'
        exit 0 ;;
      *" +check "*) ;;
      *)
        # JARVIS's own build on this host, under its limits.
        sleep "${JARVIS_BUILD_SLEEP:-0}"
        printf '{"schema_version":"jarvis.install-report.v1","satisfied":true}\\n'
        exit "${JARVIS_BUILD_RC:-0}" ;;
    esac
    printf '{"schema_version":"jarvis.install-report.v1","satisfied":%s,"issues":%s,' \\
      "${JARVIS_SATISFIED:-true}" "${JARVIS_ISSUES:-[]}"
    printf '"placement":%s}\\n' "${JARVIS_PLACEMENT:-null}"
    exit "${JARVIS_INSTALL_RC:-0}" ;;
  "ppl check")
    if [ -n "${JARVIS_CHECK_RC:-}" ]; then
      printf '{"schema_version":"jarvis.pipeline-check.v1","ok":false,"errors":'
      printf '[{"kind":"host_slice_overlap","field":"pkgs[1].host_slice",'
      printf '"message":"the slices overlap"}]}\\n'
      exit "$JARVIS_CHECK_RC"
    fi
    printf '{"schema_version":"jarvis.pipeline-check.v1","ok":true,'
    printf '"services":[{"nodes":{"first":2,"end":3}}]}\\n' ;;
  "ppl submit")
    case " $* " in
      *" +plan "*)
        if [ -n "${JARVIS_PLAN_RC:-}" ]; then echo "no +plan here" >&2; exit "$JARVIS_PLAN_RC"; fi
        printf '{"plan_sha256":"%s","schema_version":"jarvis.submit-plan.v1"}\\n' \\
          "${JARVIS_PLAN_SHA:-ab12}"
        exit 0 ;;
    esac
    if [ -n "${JARVIS_SUBMIT_RC:-}" ]; then
      echo "sbatch said no" >&2
      [ "$JARVIS_SUBMIT_RC" != 3 ] \\
        || printf '{"schema_version":"jarvis.install-report.v1","submitted":false}\\n'
      exit "$JARVIS_SUBMIT_RC"
    fi
    echo "submitting $3"
    printf '{"execution_id":"%s","schema_version":"jarvis.execution.handle.v1"}\\n' "$5" ;;
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
        if key
        not in ("SLURM_JOB_ID", "SLURM_JOB_NODELIST", "PBS_NODEFILE", "PBS_JOBID", "JARVIS_ROOT")
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


PIPELINE = "name: relay_op_x\npkgs:\n- pkg_type: demo.hello\n  pkg_name: pkg\n"


def _site(
    job: Path,
    env: dict[str, str],
    digest: str,
    package: str,
    jarvis: str,
    execution: str,
    *rest: str,
    mode: str = "run",
) -> subprocess.CompletedProcess[str]:
    steps = f"{package}@{digest}"
    if jarvis and "/" not in jarvis and not jarvis.startswith("~"):
        # Relay never looks JARVIS up on PATH, so a bare name is the stand-in's full path.
        jarvis = f"{env['PATH'].split(':')[0]}/{jarvis}"
    return subprocess.run(
        [
            "/bin/bash",
            "-c",
            SITE_SCRIPT,
            "clio-relay-jarvis",
            mode,
            jarvis,
            execution,
            steps,
            *rest,
        ],
        cwd=job,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _jarvis(tmp_path: Path) -> Path:
    return _program(tmp_path / "bin", "jarvis", FAKE_JARVIS)


def test_submit_hands_the_pipeline_to_jarvis_ppl_submit(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    jarvis = _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin")
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == 0, done.stderr
    root = job / ".relay-jarvis"
    log = (tmp_path / "jarvis.log").read_text().splitlines()
    assert log == [
        f"JARVIS_ROOT={root} init {root}/config {root}/private {root}/shared",
        f"JARVIS_ROOT={root} repo add {root}/src/demo +force",
        f"JARVIS_ROOT={root} ppl install yaml {job}/{PIPELINE_FILE} +check +json",
        f"JARVIS_ROOT={root} ppl check yaml {job}/{PIPELINE_FILE} +json",
        f"JARVIS_ROOT={root} ppl submit {job}/{PIPELINE_FILE} --execution-id op_x +json",
    ]
    assert (job / SERVICE_NODE_FILE).read_text() == "0\n"
    assert "sbatch" not in done.stdout
    handle = parse_handle((job / SUBMISSION_FILE).read_text())
    assert handle is not None and handle["execution_id"] == "op_x"
    assert (job / EXITCODE_FILE).read_text() == "0\n"
    assert (job / JARVIS_FILE).read_text() == f"{jarvis}\n"
    assert (root / "src" / "demo" / "demo" / "hello" / "pkg.py").is_file()
    assert (root / "src" / "schemas" / "package-contract.v1.json").is_file()
    assert not (root / "src" / "demo" / "demo" / "other").exists()


def test_the_job_root_leaves_jarvis_builtin_out(tmp_path: Path) -> None:
    """The user: builtins "should not even be installed for relay to use"."""
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin")
    env.pop("JARVIS_NO_BUILTIN", None)
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == 0, done.stderr
    root = job / ".relay-jarvis"
    assert (root / "jarvis_config.yaml").read_text() == "no_builtin: true\n"
    log = (tmp_path / "jarvis.log").read_text()
    assert "repo remove builtin" not in log


def test_a_refused_submit_keeps_jarvis_code_and_no_handle(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin", JARVIS_SUBMIT_RC="3")
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == 3
    assert "sbatch said no" in done.stderr
    assert not (job / SUBMISSION_FILE).exists()
    assert (job / EXITCODE_FILE).read_text() == "3\n"
    # JARVIS stopped before sbatch: its stdout is the install report, kept as such.
    report = json.loads((job / "relay.install-report.json").read_text())
    assert report["submitted"] is False


def test_submit_asks_the_light_check_first_and_stops_on_its_exit_3(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin", JARVIS_INSTALL_RC="3", JARVIS_SATISFIED="false")
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == 3, done.stderr
    log = (tmp_path / "jarvis.log").read_text()
    assert f"ppl install yaml {job}/{PIPELINE_FILE} +check +json" in log
    assert "ppl check" not in log and "ppl submit" not in log
    assert json.loads((job / "relay.install-report.json").read_text())["satisfied"] is False
    assert json.loads(done.stdout.strip().splitlines()[-1])["satisfied"] is False
    assert not (job / SUBMISSION_FILE).exists()


def test_the_connector_node_is_the_one_jarvis_names(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    _program(tmp_path / "bin", "python", '#!/bin/sh\nexec python3 "$@"\n')
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin")
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == 0, done.stderr
    assert (job / SERVICE_NODE_FILE).read_text() == "2\n"


def test_a_pipeline_jarvis_refuses_is_never_submitted(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin", JARVIS_CHECK_RC=str(EXIT_REFUSED))
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == EXIT_REFUSED
    assert "ppl submit" not in (tmp_path / "jarvis.log").read_text()
    assert not (job / SUBMISSION_FILE).exists()
    refusal = jarvis_refused(done.stdout, site="lab", what="the pipeline")
    assert refusal.reason == RefusalReason.INVALID_SPEC
    assert refusal.data["kinds"] == ["host_slice_overlap"]
    assert "the slices overlap" in refusal.message


@pytest.mark.parametrize("rc", [0, 3])
def test_check_asks_jarvis_what_is_installed_and_submits_nothing(tmp_path: Path, rc: int) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    satisfied = "true" if rc == 0 else "false"
    env = _env(tmp_path, tmp_path / "bin", JARVIS_INSTALL_RC=str(rc), JARVIS_SATISFIED=satisfied)
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="check")
    assert done.returncode == rc, done.stderr
    report = json.loads((job / "relay.install-report.json").read_text())
    assert report["satisfied"] is (rc == 0)
    last = (tmp_path / "jarvis.log").read_text().splitlines()[-1]
    assert last.endswith(f"ppl install yaml {job}/{PIPELINE_FILE} +check +json")
    assert "ppl submit" not in (tmp_path / "jarvis.log").read_text()
    assert not (job / SUBMISSION_FILE).exists()


UNCHECKED = '[{"kind":"install_unchecked","answerable":false,"reason":"no_record"}]'
PLACEMENT = '[{"kind":"install_placement","answerable":true,"scope":"site","issue_id":"sha256:p"}]'


def _python(tmp_path: Path) -> None:
    # The site script reads JARVIS's report with the python next to its jarvis.
    _program(tmp_path / "bin", "python", '#!/bin/sh\nexec python3 "$@"\n')


def test_recheck_is_a_check_job_with_the_answers(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    _python(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin", JARVIS_INSTALL_RC="3", JARVIS_ISSUES=UNCHECKED)
    answers = '{"sha256:1":{"constraint":"^mpich"}}'
    install = "name: relay_op_x_install\n"
    done = _site(
        job, env, digest, "demo.hello", "jarvis", "op_x-install", answers, install, mode="recheck"
    )
    assert done.returncode == 0, done.stderr
    assert (job / "relay.answers.json").read_text().strip() == answers
    assert (job / "relay.install-pipeline.yaml").read_text() == install
    log = (tmp_path / "jarvis.log").read_text()
    assert f"answers {job}/relay.answers.json" in log
    pipeline = (
        f"ppl install yaml {job}/relay.install-pipeline.yaml --answers {job}/relay.answers.json"
    )
    # JARVIS first records the answers and checks where the site chose; it deferred to a job.
    assert f"{pipeline} +check +json" in log
    assert f"{pipeline} +check +submit --execution-id op_x-install +json" in log
    assert "ppl submit" not in log
    handle = parse_handle((job / "relay.install.json").read_text())
    assert handle is not None and handle["execution_id"] == "install"


@pytest.mark.parametrize(("rc", "issues"), [(0, "[]"), (3, PLACEMENT)])
def test_a_recheck_jarvis_checks_here_submits_no_job(tmp_path: Path, rc: int, issues: str) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    _python(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    (job / "relay.install.json").write_text('{"stale": true}\n')
    env = _env(tmp_path, tmp_path / "bin", JARVIS_INSTALL_RC=str(rc), JARVIS_ISSUES=issues)
    answers, install = '{"sha256:p":"login_limited"}', "name: relay_op_x_install\n"
    done = _site(
        job, env, digest, "demo.hello", "jarvis", "op_x-install", answers, install, mode="recheck"
    )
    assert done.returncode == rc, done.stderr
    log = (tmp_path / "jarvis.log").read_text()
    assert "+submit" not in log and "ppl submit" not in log
    assert not (job / "relay.install.json").exists()
    report = json.loads(done.stdout.strip().splitlines()[-1])
    assert report["issues"] == json.loads(issues)
    assert json.loads((job / "relay.install-report.json").read_text()) == report


def test_relay_sets_no_placement_and_no_limits_of_its_own() -> None:
    assert "unset JARVIS_INSTALL_PLACEMENT_CHECK JARVIS_INSTALL_PLACEMENT_BUILD" in SITE_SCRIPT
    assert "export JARVIS_INSTALL_PLACEMENT" not in SITE_SCRIPT
    assert "+allow_local" not in SITE_SCRIPT and "nice " not in SITE_SCRIPT


def test_run_takes_the_pipeline_as_an_argument(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    env = _env(tmp_path, tmp_path / "bin")
    done = _site(job, env, digest, "demo.hello", "jarvis", "", PIPELINE)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == f"ran in {job}"
    assert (job / PIPELINE_FILE).read_text() == PIPELINE
    last = (tmp_path / "jarvis.log").read_text().splitlines()[-1]
    assert last.endswith(f"ppl run yaml {job}/{PIPELINE_FILE} +json")


def test_the_site_script_runs_only_the_reviewed_bytes(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    env = _env(tmp_path, tmp_path / "bin")
    archive = job / ".relay-package" / f"{digest}.tar.gz"
    archive.write_bytes(archive.read_bytes() + b"\0")
    done = _site(job, env, digest, "demo.hello", "jarvis", "", PIPELINE)
    assert done.returncode == EXIT_DIGEST
    assert "not the reviewed" in done.stderr
    assert not (tmp_path / "jarvis.log").exists()
    missing = _site(job, env, "e" * 64, "demo.hello", "jarvis", "", PIPELINE)
    assert missing.returncode == EXIT_MISSING


def test_the_site_script_checks_its_arguments_and_the_layout(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    env = _env(tmp_path, tmp_path / "bin")
    for bad in ("hello", "demo.hello.x", "demo.he-llo", "demo/x.hello"):
        done = _site(job, env, digest, bad, "jarvis", "", PIPELINE)
        assert done.returncode == EXIT_USAGE, bad
        assert "are not <repo>.<package>" in done.stderr
    mode = _site(job, env, digest, "demo.hello", "jarvis", "", PIPELINE, mode="up")
    assert mode.returncode == EXIT_USAGE
    assert "mode up is not submit, run, install, check or recheck" in mode.stderr
    unnamed = _site(job, env, digest, "demo.hello", "jarvis", "", PIPELINE, mode="submit")
    assert unnamed.returncode == EXIT_USAGE
    assert "needs an execution id" in unnamed.stderr
    wrong_repo = _site(job, env, digest, "other.hello", "jarvis", "", PIPELINE)
    assert wrong_repo.returncode == EXIT_MISSING
    assert "no other/other directory" in wrong_repo.stderr
    assert "repo add" not in (tmp_path / "jarvis.log").read_text()


def test_the_site_script_says_when_jarvis_is_missing(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    env = _env(tmp_path, tmp_path / "bin")
    done = _site(job, env, digest, "demo.hello", "no-such-jarvis", "op_x", PIPELINE, mode="submit")
    assert done.returncode == EXIT_NO_JARVIS
    tried = f"{tmp_path / 'bin'}/no-such-jarvis"
    assert f"the site's jarvis setting {tried} is not a program" in done.stderr
    written = json.loads((job / REFUSAL_FILE).read_text())
    assert written == {
        "missing": "jarvis",
        "reason": "adapter_unavailable",
        "tried": tried,
        "explicit": True,
    }
    refusal = missing_jarvis(done.stderr, site="lab")
    assert refusal.reason == RefusalReason.ADAPTER_UNAVAILABLE
    assert refusal.data == {"missing": "jarvis", "tried": tried, "explicit": True}


def test_the_site_script_expands_a_home_jarvis_path(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    home = tmp_path / "home"
    _program(home / "jv" / "bin", "jarvis", FAKE_JARVIS)
    env = _env(tmp_path, tmp_path / "bin", HOME=str(home))
    done = _site(job, env, digest, "demo.hello", "~/jv/bin/jarvis", "", PIPELINE)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == f"ran in {job}"


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
