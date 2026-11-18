"""Unit checks for passing a cancel's signal on to the command in Slurm scripts."""

from __future__ import annotations

import asyncio
import os
import shlex
import signal
from pathlib import Path, PurePosixPath

import pytest

from clio_relay.adapters.ssh.signals import forward_signals
from clio_relay.jobs.spec import JobSpec
from tests.relay_ssh.test_ssh_units import _adapter  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.release_platform("posix")


def _batch(tmp_path: Path, command: str) -> Path:
    launcher = tmp_path / "single_launch.sh"
    launcher.write_text(f"sh -c {shlex.quote(command)}\n")
    rendered = f'#!/bin/bash\n\n/bin/bash {launcher} a b \n\necho "$?" > {tmp_path}/ec\n'
    batch = tmp_path / "submit.sh"
    batch.write_text(forward_signals(rendered, str(launcher), str(tmp_path)))
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
    assert (tmp_path / "signalled").read_text() == "TERM\n"


@pytest.mark.asyncio
async def test_the_batch_shell_waits_for_a_trap_the_launcher_does_not(tmp_path: Path) -> None:
    # PSI/J's launcher dies on the TERM while the command's trap is still at work.
    started = tmp_path / "started"
    command = (
        f"trap 'sleep 1; echo stopped > {tmp_path}/log; exit 143' TERM; "
        f"touch {started}; sleep 3600 & wait"
    )
    launcher = tmp_path / "single_launch.sh"
    launcher.write_text(f"sh -c {shlex.quote(command)} &\nwait\n")
    rendered = f"#!/bin/bash\n\n/bin/bash {launcher}\n\necho done > {tmp_path}/batch\n"
    batch = tmp_path / "submit.sh"
    batch.write_text(forward_signals(rendered, str(launcher), str(tmp_path)))
    shell = await asyncio.create_subprocess_exec("bash", str(batch))
    while not started.exists():
        assert shell.returncode is None
        await asyncio.sleep(0.05)
    shell.send_signal(signal.SIGTERM)
    await shell.wait()
    assert (tmp_path / "log").read_text() == "stopped\n"
    assert (tmp_path / "batch").read_text() == "done\n"


@pytest.mark.asyncio
async def test_the_batch_shell_keeps_the_commands_exit_code(tmp_path: Path) -> None:
    shell = await asyncio.create_subprocess_exec("bash", str(_batch(tmp_path, "exit 7")))
    assert await shell.wait() == 0
    assert (tmp_path / "ec").read_text() == "7\n"
    assert not (tmp_path / "signalled").exists()


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
        forward_signals("#!/bin/bash\n# /x/single_launch.sh\n", "/x/single_launch.sh", "/j")


@pytest.mark.parametrize(
    "script", ["set -eu\necho one\n\necho 'two'", "echo a \\\n  b", "printf '%s\\n' \"x\ny\""]
)
def test_a_launch_over_several_lines_is_wrapped_whole(tmp_path: Path, script: str) -> None:
    # Agents send multi-line ``bash -c`` scripts; the ``&`` must come after the last line.
    adapter = _adapter(tmp_path)
    root = PurePosixPath("/home/someone/.clio-relay")
    spec = JobSpec(executable="bash", arguments=["-c", script])
    rendered = adapter._render(spec, "slurm", root, root / "jobs" / "op_abc", "relay-op_abc")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    submit = rendered.files["submit.sh"]
    launch = submit.split("\nset -m\n", 1)[1].split(" &\n_relay_job=$!\n", 1)[0]
    assert "single_launch.sh" in launch
    assert script in shlex.split(launch)
    assert submit.count("_relay_job=$!") == 1


def test_forward_signals_wraps_a_backslash_continuation() -> None:
    script = "#!/bin/bash\n/x/single_launch.sh a \\\n  b\necho after\n"
    wrapped = forward_signals(script, "/x/single_launch.sh", "/j")
    assert "/x/single_launch.sh a \\\n  b &\n_relay_job=$!" in wrapped
    assert wrapped.endswith('_relay_return "$_relay_rc"\necho after\n')


def test_forward_signals_needs_the_launch_to_end() -> None:
    with pytest.raises(ValueError, match="never ends"):
        forward_signals("#!/bin/bash\n/x/single_launch.sh 'open\n", "/x/single_launch.sh", "/j")


@pytest.mark.asyncio
async def test_a_multi_line_command_gets_the_signal(tmp_path: Path) -> None:
    started = tmp_path / "started"
    command = (
        "set -eu\n"
        f"trap 'echo stopping > {tmp_path}/log; exit 143' TERM\n"
        f"touch {started}\n"
        "sleep 3600 & wait\n"
    )
    launcher = tmp_path / "single_launch.sh"
    launcher.write_text('exec bash -c "$1"\n')
    rendered = (
        f'#!/bin/bash\n\n/bin/bash {launcher} {shlex.quote(command)}\n\necho "$?" > {tmp_path}/ec\n'
    )
    batch = tmp_path / "submit.sh"
    batch.write_text(forward_signals(rendered, str(launcher), str(tmp_path)))
    shell = await asyncio.create_subprocess_exec("bash", str(batch))
    while not started.exists():
        assert shell.returncode is None
        await asyncio.sleep(0.05)
    shell.send_signal(signal.SIGTERM)
    await shell.wait()
    assert (tmp_path / "log").read_text() == "stopping\n"
    assert (tmp_path / "ec").read_text() == "143\n"
    assert (tmp_path / "signalled").read_text() == "TERM\n"


@pytest.mark.asyncio
async def test_the_batch_shell_waits_when_the_scheduler_signals_every_process(
    tmp_path: Path,
) -> None:
    # Where Slurm signals every process itself, the launcher dies at once and there is
    # nothing to pass on, but the job still ends when the batch shell does.
    started = tmp_path / "started"
    command = (
        f"trap 'sleep 3; echo stopped > {tmp_path}/log; exit 143' TERM; "
        f"touch {started}; sleep 3600 & wait"
    )
    launcher = tmp_path / "single_launch.sh"
    launcher.write_text(f"echo $$ > {tmp_path}/launcher.pid\nsh -c {shlex.quote(command)} &\nwait\n")
    rendered = f"#!/bin/bash\n\n/bin/bash {launcher}\n\necho done > {tmp_path}/batch\n"
    batch = tmp_path / "submit.sh"
    batch.write_text(forward_signals(rendered, str(launcher), str(tmp_path)))
    shell = await asyncio.create_subprocess_exec("bash", str(batch))
    while not started.exists():
        assert shell.returncode is None
        await asyncio.sleep(0.05)
    group = os.getpgid(int((tmp_path / "launcher.pid").read_text()))
    assert group != os.getpgid(shell.pid)
    os.killpg(group, signal.SIGTERM)
    shell.send_signal(signal.SIGTERM)
    await shell.wait()
    # The trap had finished when the batch shell went on.
    assert (tmp_path / "log").read_text() == "stopped\n"
    assert (tmp_path / "batch").read_text() == "done\n"
    assert (tmp_path / "signalled").read_text() == "TERM\n"
