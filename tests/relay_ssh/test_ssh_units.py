"""Unit checks for the ``ssh`` adapter: argv, refusals, parsing and PSI/J rendering.

These run without a site. The live checks are in the fixture modules.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import timedelta
from pathlib import Path, PurePosixPath

import pytest

from clio_relay import jarvis_submit
from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.ssh import SshAdapter, SshConfig, scripts
from clio_relay.adapters.ssh import adapter as ssh_adapter
from clio_relay.adapters.ssh.adapter import accounted_status, walltime
from clio_relay.adapters.ssh.transport import Completed, SshTransport, run_process
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobHandle
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import (
    JobAttributes,
    JobResources,
    JobSpec,
)
from clio_relay.sites.models import SpendPolicy

pytestmark = pytest.mark.release_platform("posix")


def _adapter(tmp_path: Path, **config: object) -> SshAdapter:
    settings = SshConfig.model_validate({"ssh_profile": "example", **config})
    context = AdapterContext(
        site_id="unit", state_dir=tmp_path / "state", content=ContentStore(tmp_path / "cas")
    )
    return SshAdapter(settings, context)


def test_entry_point_resolves() -> None:
    assert registry.load("ssh") is SshAdapter


def test_ssh_argv_uses_the_profile_and_batch_mode(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, ssh_config="/etc/fx_config", ssh_options=["Port=2222"])
    argv = adapter.ssh_argv()
    assert argv[0] == "ssh"
    assert argv[-1] == "example"
    assert argv[1:3] == ["-F", "/etc/fx_config"]
    assert "BatchMode=yes" in argv
    assert "Port=2222" in argv
    assert not any(item.startswith("ControlPath=") for item in argv)


def test_interactive_sign_in_keeps_the_plain_argv(tmp_path: Path) -> None:
    transport = SshTransport(
        SshConfig(ssh_profile="example", interactive_sign_in=True),
        site_id="unit",
        state_dir=tmp_path,
    )
    argv = transport.ssh_argv()
    assert not any(item.startswith("Control") for item in argv)
    assert argv[-1] == "example"


def _failed(stderr: str, code: int = 255) -> Completed:
    return Completed(argv=["ssh"], exit_code=code, stdout=b"", stderr=stderr.encode())


def test_a_code_prompt_refuses_with_interactive_auth_required(tmp_path: Path) -> None:
    transport = _adapter(tmp_path).transport
    with pytest.raises(Refusal) as refused:
        transport.raise_for_failure(
            _failed("relay@127.0.0.1: Permission denied (keyboard-interactive)."), "describe"
        )
    assert refused.value.reason == RefusalReason.INTERACTIVE_AUTH_REQUIRED
    assert refused.value.data["adapter"] == "worker"
    assert "worker" in refused.value.message


def test_network_failures_refuse_as_unreachable(tmp_path: Path) -> None:
    transport = _adapter(tmp_path).transport
    with pytest.raises(Refusal) as refused:
        transport.raise_for_failure(
            _failed("ssh: connect to host 10.0.0.1 port 22: Connection refused"), "describe"
        )
    assert refused.value.reason == RefusalReason.UNREACHABLE


def test_a_remote_command_exit_255_is_not_an_ssh_failure(tmp_path: Path) -> None:
    _adapter(tmp_path).transport.raise_for_failure(_failed("my tool failed"), "exec")


def test_a_missing_remote_program_is_not_a_missing_ssh(tmp_path: Path) -> None:
    _adapter(tmp_path).transport.raise_for_failure(
        _failed("bash: relay-no-such-program: command not found", code=127), "exec"
    )


@pytest.mark.asyncio
async def test_a_missing_ssh_program_is_adapter_unavailable(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, ssh_command="/nonexistent/ssh-relay-test")
    with pytest.raises(Refusal) as refused:
        await adapter.describe()
    assert refused.value.reason == RefusalReason.ADAPTER_UNAVAILABLE


@pytest.mark.asyncio
async def test_a_closed_port_is_unreachable(tmp_path: Path) -> None:
    adapter = _adapter(
        tmp_path,
        ssh_options=[
            "HostName=127.0.0.1",
            "Port=1",
            "StrictHostKeyChecking=no",
            "UserKnownHostsFile=/dev/null",
        ],
    )
    with pytest.raises(Refusal) as refused:
        await adapter.describe()
    assert refused.value.reason == RefusalReason.UNREACHABLE


def _package_spec(**update: object) -> JobSpec:
    return JobSpec.model_validate(
        {
            "package": {
                "name": "clio_jarvis.xcompact3d",
                "version": "0.2.2",
                "sha256": "a" * 64,
                "parameters": {"case": "tgv", "nx": 257},
            },
            "resources": {"process_count": 128, "processes_per_node": 32},
            "attributes": {
                "duration": 1800,
                "queue_name": "compute",
                "account": "lab",
            },
            **update,
        }
    )


def test_a_package_job_on_slurm_is_a_jarvis_pipeline(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, jarvis="~/jv/bin/jarvis")
    job_dir = PurePosixPath("/home/u/.clio-relay/jobs/op_x")
    rendered = adapter._render_jarvis(_package_spec(), "op_x", job_dir, "relay-op_x", "submit")  # pyright: ignore[reportPrivateUsage]
    pipeline = rendered.files["relay.pipeline.yaml"]
    assert pipeline.splitlines()[:10] == [
        "name: relay_op_x",
        "scheduler:",
        "  job_name: relay-op_x",
        "  nodes: 4",
        "  ntasks_per_node: 32",
        "  time: 00:30:00",
        "  partition: compute",
        "  account: lab",
        "pkgs:",
        "- pkg_type: clio_jarvis.xcompact3d",
    ]
    assert "sbatch" not in pipeline
    assert rendered.files["relay-jarvis.sh"].startswith("set -eu")
    command = rendered.submit_command
    assert command.startswith("env ")
    assert f"CLIO_RELAY_JARVIS_PIN={jarvis_submit.JARVIS_PIN}" in command
    assert command.endswith(
        f"bash relay-jarvis.sh submit '~/jv/bin/jarvis' op_x clio_jarvis.xcompact3d@{'a' * 64}"
    )


def test_cores_per_rank_reach_jarvis_as_hardware_threads_and_exclusive_as_is() -> None:
    def block(threads_per_core: int | None, **resources: object) -> dict[str, object]:
        spec = _package_spec(resources={"process_count": 32, "processes_per_node": 8, **resources})
        return jarvis_submit.scheduler_block(
            spec, job_name="relay-op_x", threads_per_core=threads_per_core
        )

    # 2 whole cores per rank on 2-thread cores: Slurm's cpus_per_task counts threads.
    assert block(2, cpu_cores_per_process=2)["cpus_per_task"] == 4
    assert block(None, cpu_cores_per_process=3)["cpus_per_task"] == 3
    # One core per rank is JARVIS's own default, so relay sends nothing.
    assert "cpus_per_task" not in block(2, cpu_cores_per_process=1)
    assert "cpus_per_task" not in block(2)
    assert block(2, exclusive_node_use=True)["exclusive"] is True
    assert "exclusive" not in block(2)


def test_a_jarvis_handle_comes_from_the_submit_line(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    line = (
        '{"cluster":null,"execution_id":"op_x","mode":"scheduler","pipeline_id":"relay_op_x",'
        '"schema_version":"jarvis.execution.handle.v1","scheduler_native_id":"27873",'
        '"scheduler_provider":"slurm"}'
    )
    root = PurePosixPath("/home/u/.clio-relay")
    handle = adapter._handle("jarvis", f"queued\n{line}", root, root / "jobs/op_x", "relay-op_x")  # pyright: ignore[reportPrivateUsage]
    assert handle.native_id == "27873"
    assert handle.data["mode"] == "jarvis"
    assert handle.data["execution_dir"] == (
        "/home/u/.clio-relay/jobs/op_x/.relay-jarvis/shared/relay_op_x/executions/op_x"
    )
    with pytest.raises(Refusal) as refused:
        adapter._handle("jarvis", "no json", root, root / "jobs/op_x", "relay-op_x")  # pyright: ignore[reportPrivateUsage]
    assert refused.value.reason == RefusalReason.SUBMIT_FAILED


@pytest.mark.asyncio
async def test_a_package_service_needs_jarvis_execution_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _package_spec()
    assert spec.package is not None
    service = spec.model_copy(update={"package": spec.package.model_copy(update={"service": True})})
    adapter = _adapter(tmp_path)

    async def site_without_scheduler(*_: object, **__: object) -> str:
        return "@@root /home/u/.clio-relay\n@@ready\n@@scheduler none\n"

    monkeypatch.setattr(adapter, "_ssh", site_without_scheduler)
    with pytest.raises(Refusal) as refused:
        await adapter.run(service, operation_id="op_x")
    assert refused.value.reason == RefusalReason.ADAPTER_UNAVAILABLE
    assert refused.value.data["needs"] == "jarvis ppl submit"
    assert "jarvis execution stop" in refused.value.message


@pytest.mark.asyncio
async def test_no_sign_in_without_interactive_sign_in(tmp_path: Path) -> None:
    assert await _adapter(tmp_path).sign_in_needed() is None


@pytest.mark.asyncio
async def test_a_code_site_relay_cannot_hold_refuses_before_asking(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path, interactive_sign_in=True, ssh_command="/opt/site/bin/gsissh")
    with pytest.raises(Refusal) as refused:
        await adapter.sign_in_needed()
    assert refused.value.reason == RefusalReason.INTERACTIVE_AUTH_REQUIRED
    assert refused.value.data["adapter"] == "worker"
    assert refused.value.data["cause"] == "openssh_only"
    assert "gsissh" in refused.value.message


@pytest.mark.parametrize(
    ("lines", "state", "exit_code"),
    [
        (["COMPLETED|0:0"], "completed", 0),
        (["COMPLETED|2:0"], "failed", 2),
        (["CANCELLED by 1000|0:15"], "cancelled", 0),
        (["TIMEOUT|0:0"], "failed", 0),
        (["NODE_FAIL|0:0"], "failed", 0),
    ],
)
def test_slurm_accounting_says_how_a_forgotten_job_ended(
    lines: list[str], state: str, exit_code: int
) -> None:
    status = accounted_status(lines)
    assert status is not None
    assert status.state.value == state
    assert status.exit_code == exit_code


@pytest.mark.parametrize("lines", [[], [""], ["RUNNING|0:0"]])
def test_no_accounting_is_no_evidence(lines: list[str]) -> None:
    assert accounted_status(lines) is None


@pytest.mark.parametrize("recorded", ["running", "queued", None])
def test_a_forgotten_job_with_no_evidence_is_unknown_not_failed(
    tmp_path: Path, recorded: str | None
) -> None:
    adapter = _adapter(tmp_path)
    handle = JobHandle(adapter="ssh", native_id="53", data={"job_dir": "/j", "scheduler": "slurm"})
    sections = {"schedrc": ["0"], "sched": ["JOBID ST REASON"], "ec": [""], "acct": [""]}
    executor = ssh_adapter._executor("slurm", tmp_path)  # pyright: ignore[reportPrivateUsage]
    status = adapter._scheduler_status(executor, handle, sections, recorded)  # pyright: ignore[reportPrivateUsage]
    assert not status.state.final
    assert status.state.value == ("queued" if recorded == "queued" else "running")
    assert status.native_state == "unknown"
    assert status.message is not None and "unknown" in status.message


def test_a_forgotten_job_with_an_exit_code_completes(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    handle = JobHandle(adapter="ssh", native_id="53", data={"job_dir": "/j", "scheduler": "slurm"})
    sections = {"schedrc": ["0"], "sched": ["JOBID ST REASON"], "ec": ["0"]}
    executor = ssh_adapter._executor("slurm", tmp_path)  # pyright: ignore[reportPrivateUsage]
    status = adapter._scheduler_status(executor, handle, sections, "running")  # pyright: ignore[reportPrivateUsage]
    assert status.state.value == "completed"
    assert status.exit_code == 0


@pytest.mark.parametrize(
    ("acct", "state", "native"),
    [
        (["CANCELLED by 1000|0:15"], "cancelled", "CANCELLED"),
        (["TIMEOUT|0:15"], "failed", "TIMEOUT"),
        ([""], "failed", "gone"),
    ],
)
def test_a_forgotten_job_the_scheduler_stopped_asks_accounting(
    tmp_path: Path, acct: list[str], state: str, native: str
) -> None:
    adapter = _adapter(tmp_path)
    handle = JobHandle(adapter="ssh", native_id="53", data={"job_dir": "/j", "scheduler": "slurm"})
    sections = {
        "schedrc": ["0"],
        "sched": ["JOBID ST REASON"],
        "ec": ["143"],
        "signalled": ["TERM"],
        "acct": acct,
    }
    executor = ssh_adapter._executor("slurm", tmp_path)  # pyright: ignore[reportPrivateUsage]
    status = adapter._scheduler_status(executor, handle, sections, "running")  # pyright: ignore[reportPrivateUsage]
    assert status.state.value == state
    assert status.native_state == native
    if native == "gone":
        assert status.exit_code == 143
        assert status.message is not None and "SIGTERM" in status.message
    script = scripts.status_script(
        "/j",
        mode="scheduler",
        status_command="squeue",
        native_id="53",
        accounting_command="sacct -j 53",
    )
    assert "grep -qxF 53" in script
    assert "sacct -j 53" in script
    plain = scripts.status_script("/j", mode="scheduler", status_command="qstat", native_id="5")
    assert "@@acct" not in plain


@pytest.mark.asyncio
async def test_a_quiet_process_gets_stall_reports(tmp_path: Path) -> None:
    flag = tmp_path / "go"
    reports: list[float] = []

    async def on_stall(idle_s: float, next_s: float) -> None:
        reports.append(idle_s)
        flag.touch()

    # The process ends only once a report has come, so the test does not race.
    wait = f"import os, time\nwhile not os.path.exists({str(flag)!r}): time.sleep(0.02)"
    result = await run_process(
        [sys.executable, "-c", wait], timeout_s=None, on_stall=on_stall, stall_s=0.1
    )
    assert result.exit_code == 0
    assert not result.timed_out
    assert reports and reports[0] >= 0.1


@pytest.mark.asyncio
async def test_the_exec_script_returns_the_exit_code_with_stdin_held(tmp_path: Path) -> None:
    script = scripts.exec_script(["sh", "-c", "echo out; exit 7"])
    result = await run_process(["sh", "-c", script], timeout_s=None, stdin=b"", hold_stdin=True)
    assert result.exit_code == 7
    assert result.text() == "out\n"


@pytest.mark.asyncio
async def test_the_exec_script_runs_a_login_shell_function_without_reparsing(
    tmp_path: Path,
) -> None:
    (tmp_path / ".bash_profile").write_text('relayfn() { printf "%s|" "$@"; }\n')
    script = scripts.exec_script(["relayfn", "a b; touch hit", "$HOME"])
    env = {**os.environ, "HOME": str(tmp_path)}
    result = await run_process(
        ["sh", "-c", script], timeout_s=None, stdin=b"", hold_stdin=True, env=env
    )
    assert result.exit_code == 0, result.text()
    assert result.text().endswith("a b; touch hit|$HOME|")
    assert not (tmp_path / "hit").exists()


def _alive(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return False
    return state != "Z"


@pytest.mark.asyncio
async def test_the_exec_watchdog_stops_the_command_when_stdin_closes(tmp_path: Path) -> None:
    pid_file = tmp_path / "pid"
    command = ["sh", "-c", f"echo $$ > {pid_file}; exec sleep 3600"]
    shell = await asyncio.create_subprocess_exec(
        "sh",
        "-c",
        scripts.exec_script(command),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    assert shell.stdin is not None
    while not pid_file.exists() or not pid_file.read_text().strip():
        assert shell.returncode is None
        await asyncio.sleep(0.05)
    pid = int(pid_file.read_text())
    assert _alive(pid)
    # This is what the site sees when relay's ssh ends, is cancelled or drops.
    shell.stdin.close()
    assert await shell.wait() != 0
    assert not _alive(pid)


def test_parse_sections() -> None:
    text = "noise\n@@root /home/u/.r\n@@have abc\n@@have def\n@@out\nline 1\nline 2\n@@ready\n"
    sections = scripts.parse_sections(text)
    assert sections["root"] == ["/home/u/.r"]
    assert sections["have"] == ["abc", "def"]
    assert sections["out"] == ["line 1", "line 2"]
    assert sections["ready"] == []


def test_listing_entries_skip_totals() -> None:
    lines = ["  12 ./stdout", "   0 ./sub dir/a b", " 12 total", "garbage"]
    assert scripts.listing_entries(lines) == [("stdout", 12), ("sub dir/a b", 0)]


def test_root_expression() -> None:
    assert scripts.root_expression(".clio-relay") == '"$HOME"/.clio-relay'
    assert scripts.root_expression("~/x y") == "\"$HOME\"/'x y'"
    assert scripts.root_expression("/scratch/me/relay/") == "/scratch/me/relay"


@pytest.mark.parametrize("scheduler", ["slurm", "pbs"])
def test_render_uses_the_sites_paths(tmp_path: Path, scheduler: str) -> None:
    adapter = _adapter(tmp_path)
    root = PurePosixPath("/home/someone/.clio-relay")
    job_dir = root / "jobs" / "op_abc"
    spec = JobSpec(
        executable="echo",
        arguments=["hello world"],
        stdout_path="out.txt",
        attributes=JobAttributes(queue_name="debug"),
    )
    rendered = adapter._render(spec, scheduler, root, job_dir, "relay-op_abc")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    script = rendered.files["submit.sh"]
    assert str(tmp_path) not in script
    assert f"{job_dir}/out.txt" in script
    assert f"{job_dir}/stderr" in script
    assert rendered.launcher_dir is not None
    assert f"{root}/psij/{rendered.launcher_dir}/single_launch.sh" in script
    assert f'"{job_dir}/$' in script
    assert "relay-op_abc" in script
    assert "'hello world'" in script
    assert rendered.submit_command.endswith(f"{job_dir}/submit.sh")
    assert {path.name for path in rendered.launcher_files} == {
        "launcher_lib.sh",
        "single_launch.sh",
    }
    assert all(path.is_file() for path in rendered.launcher_files)


_TIME_DIRECTIVES = {"slurm": "#SBATCH --time=", "pbs": "#PBS -l walltime="}


@pytest.mark.parametrize("scheduler", ["slurm", "pbs"])
def test_no_duration_renders_no_walltime(tmp_path: Path, scheduler: str) -> None:
    adapter = _adapter(tmp_path)
    root = PurePosixPath("/home/someone/.clio-relay")
    job_dir = root / "jobs" / "op_abc"
    directive = _TIME_DIRECTIVES[scheduler]
    bare = JobSpec(executable="true", attributes=JobAttributes(queue_name="debug"))
    assert walltime(bare) is None
    rendered = adapter._render(bare, scheduler, root, job_dir, "relay-op_abc")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    assert directive not in rendered.files["submit.sh"]
    # A duration that was given is rendered.
    timed = JobSpec(executable="true", attributes=JobAttributes(duration=timedelta(hours=3)))
    assert walltime(timed) == timedelta(hours=3)
    rendered = adapter._render(timed, scheduler, root, job_dir, "relay-op_abc")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    assert directive in rendered.files["submit.sh"]


def test_a_job_without_a_walltime_has_an_unknown_charge(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    bare = JobSpec(
        executable="true",
        resources=JobResources(node_count=2),
        attributes=JobAttributes(queue_name="debug", account="proj"),
    )
    charge = asyncio.run(adapter.price(bare))
    assert (charge.site, charge.queue, charge.account, charge.nodes) == ("unit", "debug", "proj", 2)
    assert (charge.walltime_s, charge.node_hours, charge.amount) == (None, None, None)
    assert SpendPolicy(ask=1000.0).needs_approval(charge.node_hours)
    assert not SpendPolicy(ask="never").needs_approval(charge.node_hours)
    timed = bare.model_copy(
        update={"attributes": JobAttributes(queue_name="debug", duration=timedelta(minutes=90))}
    )
    charge = asyncio.run(adapter.price(timed))
    assert (charge.walltime_s, charge.node_hours, charge.amount) == (5400.0, 3.0, 3.0)
    assert charge.unit == "node-hours"
    assert not SpendPolicy(ask=4.0).needs_approval(charge.node_hours)


def test_checks_of_remote_files_list_their_directory_first() -> None:
    status = scripts.status_script("/j", mode="scheduler", status_command="squeue", native_id="53")
    assert status.index("fresh .") < status.index("[ -f final ]")
    assert status.index("fresh .") < status.index("53.ec")
    check = scripts.check_script("~/.r", "op_1", "auto", ["ab" * 32])
    assert check.index('fresh "$J" "$I"') < check.index('[ -f "$J/submit.rc" ]')
    assert check.index('fresh "${c%/*}"') < check.index('[ -f "$c" ]')
    assert scripts.FRESH in status and scripts.FRESH in check


def test_render_detached(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    root = PurePosixPath("/home/someone/.clio-relay")
    job_dir = root / "jobs" / "op_abc"
    spec = JobSpec(
        executable="sh",
        arguments=["-c", "echo $X"],
        environment={"X": "a b"},
        inherit_environment=False,
    )
    rendered = adapter._render(spec, None, root, job_dir, "relay-op_abc")  # pyright: ignore[reportPrivateUsage]
    child = rendered.files["child.sh"]
    assert "exec env -i 'X=a b' sh -c 'echo $X'" in child
    assert f">{job_dir}/stdout" in child
    assert f"J={job_dir}" in rendered.files["supervise.sh"]


@pytest.mark.parametrize(
    ("verb", "extra"), [("get", ()), ("cancel", ()), ("stop", ()), ("move", ("--queue", "debug"))]
)
def test_jarvis_execution_shell_keeps_the_env_ahead_of_jarvis(
    verb: jarvis_submit.ExecutionVerb, extra: tuple[str, ...]
) -> None:
    shell = jarvis_submit.execution_shell(
        verb, "ex1", job_dir="/scratch/job", pipeline="pl1", extra=extra
    )
    tail = " ".join(["execution", verb, "ex1", *extra, "--pipeline-id", "pl1", "+json"])
    assert shell == (
        f'env JARVIS_ROOT=/scratch/job/.relay-jarvis JARVIS_NO_BUILTIN=1 "$jarvis" {tail}'
    )
    argv = jarvis_submit.execution_argv(
        verb, "ex1", job_dir="/scratch/job", jarvis="/opt/jarvis", pipeline="pl1", extra=extra
    )
    assert argv[: argv.index("/opt/jarvis")] == jarvis_submit.execution_env("/scratch/job")
    assert argv[argv.index("/opt/jarvis") + 1 :] == tail.split()
