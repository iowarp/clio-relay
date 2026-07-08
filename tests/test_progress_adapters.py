from __future__ import annotations

from typing import cast

from clio_relay.package_adapters.lammps import LammpsThermoProgressAdapter
from clio_relay.progress_adapters import package_progress_adapter_from_pipeline


def test_lammps_parser_requires_thermo_header_and_named_step_column() -> None:
    adapter = LammpsThermoProgressAdapter(total_steps=150, warmup_samples=1)

    assert adapter.observe_line("25 1.40 -5.9") is None
    assert adapter.observe_line("Temp Step E_pair") is None
    first = adapter.observe_line("1.44 0 -6.0")
    adapter.observe_line("1.40 25 -5.9")
    third = adapter.observe_line("1.41 50 -5.8")

    assert first is not None
    assert first["label"] == "timestep"
    assert first["current"] == 0
    assert first["total"] == 150
    assert third is not None
    metadata = cast(dict[str, object], third["metadata"])
    assert metadata["source"] == "jarvis_package"
    assert metadata["adapter"] == "lammps"
    assert metadata["package_name"] == "builtin.lammps"
    assert metadata["step_column"] == "Step"
    assert metadata["confidence"] == "timing_unavailable"
    assert metadata["prediction_status"] == "no_lammps_timing_column"
    assert "eta_seconds" not in metadata


def test_lammps_parser_uses_thermo_cpu_for_eta_not_parse_time() -> None:
    adapter = LammpsThermoProgressAdapter(total_steps=150, warmup_samples=1)

    adapter.observe_line("Step Temp CPU")
    first = adapter.observe_line("0 1.44 0.0")
    second = adapter.observe_line("25 1.40 10.0")
    third = adapter.observe_line("50 1.41 20.0")

    assert first is not None
    assert second is not None
    assert third is not None
    metadata = cast(dict[str, object], third["metadata"])
    assert metadata["timing_column"] == "CPU"
    assert metadata["timing_source"] == "lammps_thermo_cpu"
    assert metadata["prediction_status"] == "observed_lammps_timing"
    assert metadata["prediction_method"] == "trimmed_mean_step_time_after_warmup"
    assert metadata["rate_samples"] == 2
    assert metadata["trimmed_rate_samples"] == 2
    assert metadata["remaining_steps"] == 100
    assert metadata["eta_seconds"] == 40
    assert metadata["elapsed_seconds"] == 20


def test_lammps_parser_resets_after_loop_footer() -> None:
    adapter = LammpsThermoProgressAdapter(total_steps=100)

    adapter.observe_line("Step Temp")
    assert adapter.observe_line("10 1.0") is not None
    adapter.observe_line("Loop time of 1.0 on 1 procs for 10 steps")

    assert adapter.observe_line("20 1.0") is None


def test_lammps_parser_tracks_repeated_runs_and_nonzero_start() -> None:
    adapter = LammpsThermoProgressAdapter(total_steps=300)

    adapter.observe_line("run 150")
    adapter.observe_line("Step Temp")
    first = adapter.observe_line("100 1.0")
    middle = adapter.observe_line("175 1.0")
    adapter.observe_line("250 1.0")
    adapter.observe_line("Loop time of 1.0 on 1 procs for 150 steps")
    adapter.observe_line("run 150")
    adapter.observe_line("Step Temp")
    repeated = adapter.observe_line("250 1.0")
    second_middle = adapter.observe_line("325 1.0")

    assert first is not None
    assert first["current"] == 0
    assert middle is not None
    assert middle["current"] == 75
    assert repeated is not None
    assert repeated["current"] == 150
    assert second_middle is not None
    assert second_middle["current"] == 225


def test_lammps_parser_handles_reset_timestep() -> None:
    adapter = LammpsThermoProgressAdapter(total_steps=100)

    adapter.observe_line("reset_timestep 0")
    adapter.observe_line("run 100")
    adapter.observe_line("Step Temp")
    first = adapter.observe_line("0 1.0")
    second = adapter.observe_line("50 1.0")

    assert first is not None
    assert first["current"] == 0
    assert second is not None
    assert second["current"] == 50


def test_lammps_parser_ignores_replayed_output_after_declared_total() -> None:
    adapter = LammpsThermoProgressAdapter(total_steps=100)

    adapter.observe_line("run 100")
    adapter.observe_line("Step Temp CPU")
    assert adapter.observe_line("0 1.0 0.0") is not None
    assert adapter.observe_line("100 1.0 10.0") is not None
    adapter.observe_line("Loop time of 10.0 on 1 procs for 100 steps")
    adapter.observe_line("Step Temp CPU")

    assert adapter.observe_line("0 1.0 0.0") is None
    assert adapter.observe_line("100 1.0 10.0") is None


def test_pipeline_adapter_requires_declared_lammps_package() -> None:
    generic = "name: generic\npkgs:\n- pkg_type: clio_relay.bounded_command\n"
    lammps = "name: lammps\npkgs:\n- pkg_type: builtin.lammps\n  progress:\n    total_steps: 250\n"
    alias = "name: lammps\npkgs:\n- pkg_type: lammps\n"
    mixed = (
        "name: mixed\npkgs:\n- pkg_type: builtin.lammps\n- pkg_type: clio_relay.bounded_command\n"
    )

    assert package_progress_adapter_from_pipeline(generic) is None
    assert package_progress_adapter_from_pipeline(alias) is None
    assert package_progress_adapter_from_pipeline(mixed) is None
    adapter = package_progress_adapter_from_pipeline(lammps)

    assert adapter is not None
    assert isinstance(adapter, LammpsThermoProgressAdapter)
    assert adapter.package_name == "builtin.lammps"
    assert adapter.total_steps == 250


def test_lammps_parser_only_observes_builtin_jarvis_scope() -> None:
    adapter = LammpsThermoProgressAdapter(total_steps=100)

    ignored = adapter.observe_jarvis_stdout(
        "[clio_relay.remote_agent] [START] BEGIN\n"
        "Step Temp\n0 1.0\n50 1.0\n"
        "[clio_relay.remote_agent] [START] END\n"
    )
    observed = adapter.observe_jarvis_stdout(
        "[builtin.lammps] [START] BEGIN\nStep Temp\n0 1.0\n50 1.0\n[builtin.lammps] [START] END\n"
    )

    assert ignored == []
    assert [record["current"] for record in observed] == [0, 50]
