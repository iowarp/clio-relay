"""The shell scripts relay sends to a site: the JARVIS site script and the package listing.

They run under /bin/sh or bash with stand-in ``jarvis``, ``scontrol`` and
``squeue`` programs that log what they were asked.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from clio_relay.jarvis_site import PACKAGES_SCRIPT, Unset, parse_packages, site_pin
from clio_relay.jarvis_submit import (
    EXIT_DIGEST,
    EXIT_MISSING,
    EXIT_NO_JARVIS,
    EXIT_REFUSED,
    EXIT_USAGE,
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
from clio_relay.jobs.refusals import RefusalReason

from .conftest import listed, make_repo

pytestmark = pytest.mark.release_platform("posix")

FAKE_JARVIS = """#!/bin/sh
echo "JARVIS_ROOT=${JARVIS_ROOT:-} $*" >> "$JARVIS_LOG"
[ "${JARVIS_NO_BUILTIN:-}" = 1 ] || echo "builtin left on" >> "$JARVIS_LOG"
[ -z "${JARVIS_INSTALL_ANSWERS:-}" ] || echo "answers $JARVIS_INSTALL_ANSWERS" >> "$JARVIS_LOG"
case "$1 $2" in
  "repo add") echo "- $3" >> "$JARVIS_ROOT/repos.yaml" ;;
  "repo list")
    [ "$3" = +json ] || exit 2
    printf '{"schema_version":"jarvis.repos.v1","builtin":false,"repos":['
    sed -n 's/^ *- *//p' "$JARVIS_ROOT/repos.yaml" | {
      sep=
      while IFS= read -r dir; do
        printf '%s{"name":"%s","path":"%s","git":null,"packages":[' "$sep" "${dir##*/}" "$dir"
        sep=, inner=
        for p in "$dir/${dir##*/}"/*/pkg.py; do
          [ -f "$p" ] || continue
          p=${p%/pkg.py}
          printf '%s{"name":"%s","menu":[{"name":"listed","match":"%s"}],"contract":null}' \\
            "$inner" "${p##*/}" '^Rank 0 - ET \\\\d+'
          inner=,
        done
        printf ']}'
      done
    }
    printf ']}\\n' ;;
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
    env["CLIO_RELAY_JARVIS_HOME"] = str(tmp_path / "site")
    env.update(extra)
    return env


def _root(tmp_path: Path) -> Path:
    """The site's JARVIS root, set up the way relay's setup leaves it, holding ``src/demo``."""
    root = tmp_path / "site" / "jarvis"
    if not root.is_dir():
        repo = make_repo(tmp_path / "src")
        root.mkdir(parents=True)
        (root / "shared").mkdir()
        shared = (root / "shared").resolve()
        (root / "jarvis_config.yaml").write_text(f"no_builtin: true\nshared_dir: {shared}\n")
        (root / "repos.yaml").write_text(f"repos:\n- {repo}\n")
        (root / "relay.pin").write_text(site_pin() + "\n")
    return root


def _job_dir(tmp_path: Path) -> tuple[Path, str]:
    _root(tmp_path)
    job = tmp_path / "job"
    job.mkdir(exist_ok=True)
    return job, listed(tmp_path / "src" / "demo", "demo.hello")["digest"]


PIPELINE = "name: op_x\npkgs:\n- pkg_type: demo.hello\n  pkg_name: pkg\n"


def _site(
    job: Path,
    env: dict[str, str],
    digest: str,
    package: str,
    jarvis: str,
    execution: str,
    *rest: str,
    mode: str = "check",
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
    root = tmp_path / "site" / "jarvis"
    log = (tmp_path / "jarvis.log").read_text().splitlines()
    assert log == [
        f"JARVIS_ROOT={root} ppl install yaml {job}/{PIPELINE_FILE} +check +json",
        f"JARVIS_ROOT={root} ppl check yaml {job}/{PIPELINE_FILE} +json",
        f"JARVIS_ROOT={root} ppl submit {job}/{PIPELINE_FILE} --execution-id op_x +json",
    ]
    # The fake check names a service whose first allocation index is 2.
    assert (job / SERVICE_NODE_FILE).read_text() == "2\n"
    assert "sbatch" not in done.stdout
    handle = parse_handle((job / SUBMISSION_FILE).read_text())
    assert handle is not None and handle["execution_id"] == "op_x"
    assert not (job / "relay.exitcode").exists()
    assert (job / JARVIS_FILE).read_text() == f"{jarvis}\n"
    # The pipeline writes its own directory in the root's JARVIS shared one, no link.
    pipeline_dir = (root / "shared").resolve() / "op_x"
    assert pipeline_dir.is_dir() and not pipeline_dir.is_symlink()
    assert (job / "relay.shared").read_text() == f"{pipeline_dir}\n"


def test_submit_refuses_a_root_whose_shared_directory_is_not_the_sites_outputs(
    tmp_path: Path,
) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin", CLIO_RELAY_OUTPUTS=str(tmp_path / "runs"))
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == 66
    assert "not in this site's outputs_dir" in done.stderr
    assert not (tmp_path / "jarvis.log").exists()


def test_a_refused_submit_keeps_jarvis_code_and_no_handle(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    (job / PIPELINE_FILE).write_text(PIPELINE)
    env = _env(tmp_path, tmp_path / "bin", JARVIS_SUBMIT_RC="3")
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", mode="submit")
    assert done.returncode == 3
    assert "sbatch said no" in done.stderr
    assert not (job / SUBMISSION_FILE).exists()
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


def test_the_site_script_runs_only_the_reviewed_content(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    env = _env(tmp_path, tmp_path / "bin")
    inner = tmp_path / "src" / "demo" / "demo"
    (inner / "hello" / "pkg.py").chmod(0o755)
    for change in ("mode", "content"):
        done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", PIPELINE)
        assert done.returncode == EXIT_DIGEST, change
        assert f"not the reviewed {digest}" in done.stderr
        (inner / "_contract" / "base.py").write_text("SHARED = 2\n")
    assert not (tmp_path / "jarvis.log").exists()
    (inner / "other" / "pkg.py").write_text("# another package's edit\n")
    (inner / "hello" / "__pycache__" / "more.pyc").write_bytes(b"\0")
    again = listed(tmp_path / "src" / "demo", "demo.hello")["digest"]
    assert _site(job, env, again, "demo.hello", "jarvis", "op_x", PIPELINE).returncode == 0
    (inner / "hello" / "link").symlink_to("/etc/passwd")
    linked = _site(job, env, again, "demo.hello", "jarvis", "op_x", PIPELINE)
    assert linked.returncode == EXIT_DIGEST
    assert "symbolic link" in linked.stderr
    missing = _site(job, env, again, "demo.nothing", "jarvis", "op_x", PIPELINE)
    assert missing.returncode == EXIT_MISSING
    assert "demo.nothing is not in the site's JARVIS root" in missing.stderr


def _packages(tmp_path: Path, mode: str, *rest: str) -> subprocess.CompletedProcess[str]:
    env = _env(tmp_path, tmp_path / "bin")
    jarvis = str(_jarvis(tmp_path))
    home = str(tmp_path / "site")
    return subprocess.run(
        ["/bin/sh", "-c", PACKAGES_SCRIPT, "clio-relay-packages", mode, home, site_pin()]
        + [jarvis, *rest],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _listing(text: str) -> list[dict[str, Any]]:
    found = parse_packages(text)
    assert not isinstance(found, Unset), found
    return found


def test_the_site_listing_reads_the_root_with_the_digest_relay_reviews(tmp_path: Path) -> None:
    root = _root(tmp_path)
    repo = tmp_path / "src" / "demo"
    (repo / "demo" / "hello" / "run.sh").write_text("#!/bin/sh\n")
    (repo / "demo" / "hello" / "run.sh").chmod(0o755)
    (repo / "demo" / "hello" / "contract.yaml").write_text("version: 0.3.0\n")
    done = _packages(tmp_path, "list")
    assert done.returncode == 0, done.stderr
    found = {item["name"]: item for item in _listing(done.stdout)}
    assert sorted(found) == ["demo.hello", "demo.other"]
    for name, item in found.items():
        expected = listed(repo, name)
        assert item["digest"] == expected["digest"], name
        assert sorted(item["files"], key=lambda f: f["path"]) == expected["files"]
        # A backslash in JARVIS's document reaches relay as JARVIS printed it.
        menu = [{"name": "listed", "match": r"^Rank 0 - ET \d+"}]
        assert (item["menu"], item["contract"]) == (menu, None)
        assert item["repo_dir"] == str(repo) and item["git"] is None
    [run] = [f for f in found["demo.hello"]["files"] if f["path"] == "hello/run.sh"]
    assert run["executable"] is True

    mine = make_repo(tmp_path / "user", repo="mine", packages=("solver",))
    added = _packages(tmp_path, "add", str(mine))
    assert added.returncode == 0, added.stderr
    assert "mine.solver" in {item["name"] for item in _listing(added.stdout)}
    assert f"JARVIS_ROOT={root} repo add {mine.resolve()}" in (tmp_path / "jarvis.log").read_text()
    assert f"JARVIS_ROOT={root} repo list +json" in (tmp_path / "jarvis.log").read_text()
    assert _packages(tmp_path, "add", str(tmp_path / "nowhere")).returncode == EXIT_MISSING

    (root / "relay.pin").write_text("an older setup\n")
    assert parse_packages(_packages(tmp_path, "list").stdout) == Unset(
        want=site_pin(), at="an older setup"
    )
    (root / "relay.pin").unlink()
    assert parse_packages(_packages(tmp_path, "list").stdout) == Unset(want=site_pin(), at="")


def test_the_site_script_checks_its_arguments_and_the_layout(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    _jarvis(tmp_path)
    env = _env(tmp_path, tmp_path / "bin")
    for bad in ("hello", "demo.hello.x", "demo.he-llo", "demo/x.hello"):
        done = _site(job, env, digest, bad, "jarvis", "op_x", PIPELINE)
        assert done.returncode == EXIT_USAGE, bad
        assert "are not <repo>.<package>" in done.stderr
    mode = _site(job, env, digest, "demo.hello", "jarvis", "", PIPELINE, mode="up")
    assert mode.returncode == EXIT_USAGE
    assert "mode up is not submit, plan, install, check or recheck" in mode.stderr
    unnamed = _site(job, env, digest, "demo.hello", "jarvis", "", PIPELINE, mode="submit")
    assert unnamed.returncode == EXIT_USAGE
    assert "needs an execution id" in unnamed.stderr
    wrong_repo = _site(job, env, digest, "other.hello", "jarvis", "op_x", PIPELINE)
    assert wrong_repo.returncode == EXIT_MISSING
    assert "other.hello is not in the site's JARVIS root" in wrong_repo.stderr
    assert not (tmp_path / "jarvis.log").exists()


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


def test_the_site_script_says_which_pin_an_outdated_root_holds(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    jarvis = _jarvis(tmp_path)
    (tmp_path / "site" / "jarvis" / "relay.pin").write_text('old "x"\n')
    env = _env(tmp_path, tmp_path / "bin", CLIO_RELAY_JARVIS_PIN="new y")
    done = _site(job, env, digest, "demo.hello", "jarvis", "op_x", PIPELINE)
    assert done.returncode == EXIT_NO_JARVIS
    assert 'the site\'s JARVIS root is at old "x", not new y' in done.stderr
    written = json.loads((job / REFUSAL_FILE).read_text())
    assert (written["pin"], written["want"], written["tried"]) == ('old "x"', "new y", str(jarvis))
    refusal = missing_jarvis(done.stderr, site="lab")
    assert (refusal.data["pin"], refusal.data["want"]) == ('old "x"', "new y")
    assert refusal.message.startswith(
        'JARVIS is on lab at the jarvis program old with clio-jarvis "x"'
    )


def test_the_site_script_expands_a_home_jarvis_path(tmp_path: Path) -> None:
    job, digest = _job_dir(tmp_path)
    home = tmp_path / "home"
    _program(home / "jv" / "bin", "jarvis", FAKE_JARVIS)
    env = _env(tmp_path, tmp_path / "bin", HOME=str(home))
    done = _site(job, env, digest, "demo.hello", "~/jv/bin/jarvis", "op_x", PIPELINE)
    assert done.returncode == 0, done.stderr
    assert (
        f"ppl install yaml {job}/{PIPELINE_FILE} +check +json"
        in (tmp_path / "jarvis.log").read_text()
    )
