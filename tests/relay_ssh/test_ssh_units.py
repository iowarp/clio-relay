"""Unit checks for the ``ssh`` adapter: argv, refusals, parsing and PSI/J rendering.

These run without a site. The live checks are in the fixture modules.
"""

from __future__ import annotations

import asyncio
import shlex
import signal
import sys
from datetime import timedelta
from pathlib import Path, PurePosixPath

import pytest

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.ssh import SshAdapter, SshConfig, scripts
from clio_relay.adapters.ssh import adapter as ssh_adapter
from clio_relay.adapters.ssh.adapter import accounted_status, walltime
from clio_relay.adapters.ssh.transport import Completed, SshTransport, run_process
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.handles import JobHandle
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobAttributes, JobSpec, PackageRef

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


def test_interactive_sign_in_uses_a_control_path(tmp_path: Path) -> None:
    transport = SshTransport(
        SshConfig(ssh_profile="example", interactive_sign_in=True),
        site_id="unit",
        state_dir=tmp_path,
    )
    argv = transport.ssh_argv()
    assert any(item.startswith("ControlPath=") for item in argv)
    assert "ControlMaster=no" in argv
    assert len(str(transport.control_path)) < 104


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


@pytest.mark.asyncio
async def test_a_package_job_is_an_invalid_spec(tmp_path: Path) -> None:
    spec = JobSpec(package=PackageRef(name="p", version="1", sha256="a" * 64))
    with pytest.raises(Refusal) as refused:
        await _adapter(tmp_path).run(spec, operation_id="op_x")
    assert refused.value.reason == RefusalReason.INVALID_SPEC


@pytest.mark.asyncio
async def test_no_sign_in_without_interactive_sign_in(tmp_path: Path) -> None:
    assert await _adapter(tmp_path).sign_in_needed() is None


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


def test_the_status_script_asks_accounting_only_for_a_forgotten_job() -> None:
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


def _batch(tmp_path: Path, command: str) -> Path:
    launcher = tmp_path / "single_launch.sh"
    launcher.write_text(f"sh -c {shlex.quote(command)}\n")
    rendered = f'#!/bin/bash\n\n/bin/bash {launcher} a b \n\necho "$?" > {tmp_path}/ec\n'
    batch = tmp_path / "submit.sh"
    batch.write_text(scripts.forward_signals(rendered, str(launcher)))
    return batch


@pytest.mark.asyncio
async def test_a_signal_to_the_batch_shell_reaches_the_command(tmp_path: Path) -> None:
    # Some Slurm sites signal only the batch shell on a cancel, as this test does.
    started = tmp_path / "started"
    command = (
        f"trap 'echo stopping > {tmp_path}/log; exit 143' TERM; touch {started}; sleep 3600 & wait"
    )
    shell = await asyncio.create_subprocess_exec("bash", str(_batch(tmp_path, command)))
    while not started.exists():
        assert shell.returncode is None
        await asyncio.sleep(0.05)
    shell.send_signal(signal.SIGTERM)
    await shell.wait()
    assert (tmp_path / "log").read_text() == "stopping\n"
    assert (tmp_path / "ec").read_text() == "143\n"


@pytest.mark.asyncio
async def test_the_batch_shell_keeps_the_commands_exit_code(tmp_path: Path) -> None:
    shell = await asyncio.create_subprocess_exec("bash", str(_batch(tmp_path, "exit 7")))
    assert await shell.wait() == 0
    assert (tmp_path / "ec").read_text() == "7\n"


@pytest.mark.parametrize(("scheduler", "wrapped"), [("slurm", True), ("pbs", False)])
def test_slurm_scripts_pass_signals_on(tmp_path: Path, scheduler: str, wrapped: bool) -> None:
    adapter = _adapter(tmp_path)
    root = PurePosixPath("/home/someone/.clio-relay")
    spec = JobSpec(executable="true")
    rendered = adapter._render(spec, scheduler, root, root / "jobs" / "op_abc", "relay-op_abc")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    script = rendered.files["submit.sh"]
    assert ("trap '_relay_pass TERM' TERM" in script) is wrapped
    launch = next(line for line in script.splitlines() if "single_launch.sh" in line)
    assert launch.endswith(" &") is wrapped
    assert script.index("single_launch.sh") < script.index(".ec")


def test_forward_signals_needs_a_launch_line() -> None:
    with pytest.raises(ValueError, match="no launch line"):
        scripts.forward_signals("#!/bin/bash\n# /x/single_launch.sh\n", "/x/single_launch.sh")


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
