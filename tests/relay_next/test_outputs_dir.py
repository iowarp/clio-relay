"""A run's outputs live outside relay's own directories, and site clean keeps them."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from clio_relay import jarvis_submit, relay_state, site_clean, site_outputs
from clio_relay.adapters.ssh.config import SshConfig
from clio_relay.adapters.worker.adapter import WorkerConfig
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, PackageRef


def test_the_outputs_directory_must_be_outside_relays_own() -> None:
    assert site_outputs.check_outputs_dir("clio-relay-runs", ".clio-relay") == "clio-relay-runs"
    assert site_outputs.check_outputs_dir("~/runs", "/srv/relay") == "~/runs"
    for outputs, home in (
        (".clio-relay/runs", ".clio-relay"),
        ("~/.clio-relay", ".clio-relay"),
        ("/srv/relay/out", "/srv/relay/"),
        ("~", ".clio-relay"),
        (".", ".clio-relay"),
    ):
        with pytest.raises(ValueError):
            site_outputs.check_outputs_dir(outputs, home)


def test_site_settings_refuse_outputs_inside_relays_directory() -> None:
    assert SshConfig(ssh_profile="ares").outputs_dir == site_outputs.OUTPUTS_DIR
    with pytest.raises(ValidationError):
        SshConfig(ssh_profile="ares", outputs_dir=".clio-relay/runs")
    assert WorkerConfig(ssh_profile="ares").outputs_dir == site_outputs.OUTPUTS_DIR


def _job(**parameters: Any) -> JobSpec:
    step = PackageRef(name="builtin.case", version="1", sha256="a" * 64, parameters=parameters)
    return JobSpec(package=step)


def test_the_site_script_gets_the_outputs_directory() -> None:
    job = site_outputs.with_outputs(_job(), "clio-relay-runs")
    assert job.environment[site_outputs.OUTPUTS_ENV] == "clio-relay-runs"
    assert site_outputs.with_outputs(_job(), None).environment == {}
    command = jarvis_submit.submit_command(job, operation_id="op_x", jarvis="", home=".clio-relay")
    assert f"{site_outputs.OUTPUTS_ENV}=clio-relay-runs" in command
    assert site_outputs.dir_facts(".clio-relay", "runs") == {
        "remote_dir": ".clio-relay",
        "outputs_dir": "runs",
    }


def test_an_outputs_directory_inside_relays_state_is_refused_for_every_adapter() -> None:
    cleaner = _Cleaner()
    cleaner.config = SimpleNamespace(outputs_dir=".clio-relay/runs")
    with pytest.raises(Refusal) as caught:
        relay_state.refuse_relay_state(cleaner, _job(), "ares")
    assert caught.value.reason is RefusalReason.INVALID_SPEC
    assert "outputs_dir" in caught.value.message


def test_the_site_script_links_the_shared_directory_to_the_outputs() -> None:
    script = jarvis_submit.SITE_SCRIPT
    assert "CLIO_RELAY_OUTPUTS" in script
    assert 'ln -sfn "$shared" "$root/shared"' in script
    assert '> "$job/relay.shared"' in script
    assert jarvis_submit.SHARED_SCHEMA in script
    assert jarvis_submit.SHARED_FILE in jarvis_submit.SITE_FILES


def test_a_handle_names_the_execution_where_the_outputs_are_not_through_the_link() -> None:
    printed = (
        "Loaded pipeline\n"
        '{"schema_version":"clio-relay.shared.v1","path":"/home/u/clio-relay-runs/op_x/"}\n'
        '{"schema_version":"jarvis.execution.handle.v1","execution_id":"op_x",'
        '"pipeline_id":"relay_op_x","scheduler_native_id":"7"}\n'
    )
    shared = jarvis_submit.shared_dir(printed)
    assert shared == "/home/u/clio-relay-runs/op_x"
    found = jarvis_submit.parse_handle(printed)
    assert found is not None
    _native, data = jarvis_submit.handle_data(
        found, job_dir="/home/u/.clio-relay/jobs/op_x", shared=shared
    )
    assert data["execution_dir"] == "/home/u/clio-relay-runs/op_x/relay_op_x/executions/op_x"
    _native, linked = jarvis_submit.handle_data(found, job_dir="/home/u/.clio-relay/jobs/op_x")
    assert linked["execution_dir"].startswith("/home/u/.clio-relay/jobs/op_x/.relay-jarvis/shared/")
    for text in ("", '{"schema_version":"clio-relay.shared.v1","path":"relative"}', '{"x":1}'):
        assert jarvis_submit.shared_dir(text) is None


class _Cleaner:
    name = "ssh"
    config = SimpleNamespace(outputs_dir="clio-relay-runs")

    def clean_targets(self) -> tuple[list[str], list[str]]:
        return [".clio-relay"], []

    async def clean(self, targets: list[str], keep: list[str]) -> dict[str, Any]:
        return {"removed": targets, "kept": keep}


def test_a_run_naming_relays_state_is_refused_with_the_outputs_directory() -> None:
    cleaner = _Cleaner()
    relay_state.refuse_relay_state(cleaner, _job(run_dir="~/clio-relay-runs/tgv"), "ares")
    relay_state.refuse_relay_state(cleaner, _job(run_dir="relative/inside/job"), "ares")
    for job in (
        _job(run_dir="/home/u/.clio-relay/naca_production"),
        _job(mesh="~/.clio-relay/body.stl"),
        JobSpec(executable="x", directory="~/.clio-relay/run"),
    ):
        with pytest.raises(Refusal) as caught:
            relay_state.refuse_relay_state(cleaner, job, "ares")
        assert caught.value.reason is RefusalReason.INVALID_SPEC
        assert "clio-relay-runs" in caught.value.message
    # Another relay home's directories are not this site's deployment.
    relay_state.refuse_relay_state(cleaner, _job(run_dir="~/.clio-relay-w2/x"), "ares")
    relay_state.refuse_relay_state(
        cleaner, JobSpec(executable="x", directory="~/.config/clio-relay/run"), "ares"
    )
    relay_state.refuse_relay_state(object(), _job(run_dir="/home/u/.clio-relay/x"), "ares")


@pytest.mark.asyncio
async def test_clean_keeps_listed_outputs_unless_the_person_says_remove(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleaner = _Cleaner()

    def _one_cleaner(*_args: Any) -> tuple[list[Any], list[str]]:
        return [cleaner], ["ssh"]

    monkeypatch.setattr(site_clean, "_cleaners", _one_cleaner)

    async def _no_shutdown(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"stopped": [], "failures": []}

    monkeypatch.setattr(site_clean, "shutdown", _no_shutdown)

    def _no_dir(*_args: Any) -> Any:
        return SimpleNamespace(exists=lambda: False)

    service: Any = SimpleNamespace(paths=SimpleNamespace(adapter_dir=_no_dir))
    planned = {
        "site": "ares",
        "adapter": "ssh",
        "adapters": ["ssh"],
        "targets": [".clio-relay"],
        "keep": ["spack"],
        "cancels": [],
        "outputs": [{"path": "/home/u/.clio-relay/naca"}],
        "remove_outputs": False,
    }
    kept = await site_clean.clean(service, planned)
    assert kept["kept"] == ["spack", "/home/u/.clio-relay/naca"]
    assert kept["removed"] == [".clio-relay"]
    assert kept["outputs_kept"] == ["/home/u/.clio-relay/naca"]
    removed = await site_clean.clean(service, {**planned, "remove_outputs": True})
    assert removed["kept"] == ["spack"]
    assert removed["removed"] == [".clio-relay", "/home/u/.clio-relay/naca"]
    assert removed["outputs_kept"] == []
