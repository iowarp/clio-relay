"""Unit checks for the ``ssh`` adapter: argv, refusals, parsing and PSI/J rendering.

These run without a site. The live checks are in the fixture modules.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath

import pytest

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.ssh import SshAdapter, SshConfig, scripts
from clio_relay.adapters.ssh.transport import Completed, SshTransport
from clio_relay.jobs.content import ContentStore
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
