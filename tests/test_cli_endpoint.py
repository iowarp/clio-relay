"""Tests for the ``endpoint`` command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond imports) alongside
the ``endpoint_app`` commands' extraction into
``src/clio_relay/cli_endpoint.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises.
``test_mint_receipt_then_pin_runtime_passes_verify_remote_worker_info_matrix``,
``test_remote_worker_info_binds_worker_to_operator_pinned_physical_target``,
and ``test_remote_worker_info_threads_cluster_pinned_receipt_path`` stay in
``tests/test_cli.py`` -- they exercise ``cluster pin-runtime`` together with
``endpoint worker-info`` as one cross-group flow, not this group alone.

Every ``monkeypatch.setattr`` target here is unchanged from ``test_cli.py``:
``endpoint.EndpointWorker`` and ``installation_module.worker_runtime_info``
already patched the real owner module directly (the R8(i) idiom, unaffected
by which file calls it), and ``cli._require_cluster`` still patches
``cli.py`` because that helper was never part of this extraction (shared
plumbing, ``cli_endpoint.py`` reaches it the same way ``cli_relay_host.py``
does).

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). It is reproduced here (the env-var half only,
the same precedent ``tests/test_cli_relay_host.py``'s own
``_default_cli_mode`` established) since several tests below reach
``cli._require_cluster`` through ``endpoint start``'s cluster-registry
resolution path -- the trap ``tests/test_cli_worker.py``'s docstring
documents hitting for real.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_endpoint as cli_endpoint
import clio_relay.endpoint as endpoint
import clio_relay.installation as installation_module
from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, WorkerCapacityPolicy
from clio_relay.errors import ConfigurationError
from clio_relay.models import JobKind
from clio_relay.scheduler_providers import SchedulerProvider


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only."""
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_endpoint_worker_with_explicit_provider_does_not_require_remote_registry(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeWorker:
        def register(self) -> None:
            captured["registered"] = True

        def run_once(self) -> None:
            captured["ran_once"] = True

        def close(self) -> None:
            captured["closed"] = True

    def make_worker(**kwargs: object) -> FakeWorker:
        captured.update(kwargs)
        return FakeWorker()

    def fail_registry_lookup(cluster: str) -> ClusterDefinition:
        raise AssertionError(f"unexpected registry lookup for {cluster}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(endpoint, "EndpointWorker", make_worker)
    monkeypatch.setattr(cli, "_require_cluster", fail_registry_lookup)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "homelab",
            "--scheduler-provider",
            "external",
            "--once",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["cluster"] == "homelab"
    assert captured["registered"] is True
    assert captured["ran_once"] is True
    assert captured["closed"] is True
    provider = cast(SchedulerProvider, captured["scheduler_provider"])
    assert provider.name == "external"


def test_endpoint_worker_without_explicit_provider_uses_cluster_registry(
    monkeypatch: MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    definition = ClusterDefinition(
        name="configured-cluster",
        ssh_host="configured-cluster",
        scheduler_provider="slurm",
    )

    class FakeWorker:
        def register(self) -> None:
            captured["registered"] = True

        def run_once(self) -> None:
            captured["ran_once"] = True

        def close(self) -> None:
            captured["closed"] = True

    def make_worker(**kwargs: object) -> FakeWorker:
        captured.update(kwargs)
        return FakeWorker()

    def load_cluster(cluster: str) -> ClusterDefinition:
        captured["registry_cluster"] = cluster
        return definition

    monkeypatch.setattr(endpoint, "EndpointWorker", make_worker)
    monkeypatch.setattr(cli, "_require_cluster", load_cluster)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "configured-cluster",
            "--once",
        ],
    )

    assert result.exit_code == 0
    assert captured["registry_cluster"] == "configured-cluster"
    assert captured["closed"] is True
    provider = cast(SchedulerProvider, captured["scheduler_provider"])
    assert provider.name == "slurm"


def test_endpoint_start_defaults_control_query_concurrency_from_cluster_registry(
    monkeypatch: MonkeyPatch,
) -> None:
    """#219: an unpinned worker must never silently start with zero control-query
    capacity. Historically ``endpoint start`` had its own disconnected CLI
    default of 0, independent of the cluster's registered WorkerCapacityPolicy
    (whose own default is 1) -- a fresh deployment that never pins
    --control-query-concurrency got a worker that accepts describe-class jobs
    and never runs them, with no typed reason.
    """
    captured: dict[str, object] = {}
    definition = ClusterDefinition(
        name="configured-cluster",
        ssh_host="configured-cluster",
        scheduler_provider="slurm",
    )

    class FakeWorker:
        def register(self) -> None:
            captured["registered"] = True

        def run_once(self) -> None:
            captured["ran_once"] = True

        def close(self) -> None:
            captured["closed"] = True

    def make_worker(**kwargs: object) -> FakeWorker:
        captured.update(kwargs)
        return FakeWorker()

    def load_cluster(cluster: str) -> ClusterDefinition:
        return definition

    monkeypatch.setattr(endpoint, "EndpointWorker", make_worker)
    monkeypatch.setattr(cli, "_require_cluster", load_cluster)

    result = CliRunner().invoke(
        app,
        ["endpoint", "start", "--role", "worker", "--cluster", "configured-cluster", "--once"],
    )

    assert result.exit_code == 0, result.output
    assert captured["control_query_concurrency"] == 1
    assert captured["concurrency"] == 3


def test_endpoint_start_rejects_zero_control_query_concurrency_against_a_registered_cluster(
    monkeypatch: MonkeyPatch,
) -> None:
    """#219: WorkerCapacityPolicy.control_query_concurrency requires >= 1, so an
    operator cannot even pin 0 against a cluster resolved from the registry --
    reusing that validation (the same one render-user-service relies on)
    rejects it outright instead of silently starting a worker that will
    never pick up a describe-class job.
    """
    definition = ClusterDefinition(
        name="configured-cluster",
        ssh_host="configured-cluster",
        scheduler_provider="slurm",
    )

    def _load_definition(_cluster: str) -> ClusterDefinition:
        return definition

    monkeypatch.setattr(cli, "_require_cluster", _load_definition)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "configured-cluster",
            "--control-query-concurrency",
            "0",
            "--once",
        ],
    )

    assert result.exit_code != 0
    assert "control_query_concurrency" in result.output


def test_endpoint_start_rejects_concurrency_one_against_a_registered_cluster(
    monkeypatch: MonkeyPatch,
) -> None:
    """#219 rework: this is a DELIBERATE side effect of reusing
    WorkerCapacityPolicy's own validation, pinned here rather than left as an
    unstated regression risk. --concurrency 1 -- the CLI's own PRE-#219
    default -- is now a typed refusal against a registered cluster, exactly
    like --control-query-concurrency 0 above, over-determined by TWO of
    WorkerCapacityPolicy's own invariants at once: its ``concurrency``
    field requires >= 2, and even were that alone relaxed, its
    ``_reserve_a_workload_slot`` model validator independently requires
    ``control_query_concurrency < concurrency`` -- with the registry's
    default control_query_concurrency of 1 inherited unpinned here,
    concurrency=1 collides with it regardless.

    LIVE FACT (resolved, verification round 2): the actual p5run2 worker
    running on ares (PID 3261405) was started with
    ``endpoint start --role worker --cluster ares-p5run2 --concurrency 4
    --control-query-concurrency 1 --kind-concurrency jarvis=2
    --kind-concurrency mcp_call=3 --scheduler-provider slurm`` -- no live
    worker uses --concurrency 1, so this refusal matches how the flag is
    actually operated in the accepted deployment, not merely how it once
    defaulted.
    """
    definition = ClusterDefinition(
        name="configured-cluster",
        ssh_host="configured-cluster",
        scheduler_provider="slurm",
    )

    def _load_definition(_cluster: str) -> ClusterDefinition:
        return definition

    monkeypatch.setattr(cli, "_require_cluster", _load_definition)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "configured-cluster",
            "--concurrency",
            "1",
            "--once",
        ],
    )

    assert result.exit_code != 0
    assert "concurrency" in result.output


def test_endpoint_start_inherits_registered_kind_concurrency_when_unpinned(
    monkeypatch: MonkeyPatch,
) -> None:
    """#219 rework: an unpinned --kind-concurrency now INHERITS the cluster's
    registered WorkerCapacityPolicy.kind_concurrency limits, not "no limits"
    -- a real behavior change #219's own commit message never stated. Before
    #219, omitting the flag always meant no per-kind limits regardless of any
    cluster registration (``_kind_concurrency_options(None)`` == ``{}``,
    applied unconditionally); now `_resolved_worker_capacity_policy` inherits
    ``current.kind_concurrency`` from the registry whenever the CLI flag is
    None and --clear-kind-concurrency was not passed. Pinned here so a future
    change to that inheritance is a deliberate decision, not a silent drift.
    """
    captured: dict[str, object] = {}
    definition = ClusterDefinition(
        name="configured-cluster",
        ssh_host="configured-cluster",
        scheduler_provider="slurm",
        worker_capacity=WorkerCapacityPolicy(
            concurrency=5,
            control_query_concurrency=1,
            kind_concurrency={JobKind.MCP_CALL: 2},
        ),
    )

    class FakeWorker:
        def register(self) -> None:
            pass

        def run_once(self) -> None:
            pass

        def close(self) -> None:
            pass

    def make_worker(**kwargs: object) -> FakeWorker:
        captured.update(kwargs)
        return FakeWorker()

    monkeypatch.setattr(endpoint, "EndpointWorker", make_worker)

    def _load_definition(_cluster: str) -> ClusterDefinition:
        return definition

    monkeypatch.setattr(cli, "_require_cluster", _load_definition)

    result = CliRunner().invoke(
        app,
        ["endpoint", "start", "--role", "worker", "--cluster", "configured-cluster", "--once"],
    )

    assert result.exit_code == 0, result.output
    assert captured["kind_concurrency"] == {JobKind.MCP_CALL: 2}


def test_endpoint_start_warns_loudly_when_control_query_concurrency_is_explicitly_zero(
    monkeypatch: MonkeyPatch,
) -> None:
    """#219: a worker started with an explicit --scheduler-provider bypasses
    cluster-registry resolution (test_endpoint_worker_with_explicit_provider_
    does_not_require_remote_registry) and so is not bound by
    WorkerCapacityPolicy's >= 1 floor; 0 remains reachable there. It must
    still surface a loud, typed warning rather than silently starting a
    worker that will never pick up a describe-class job.
    """

    class FakeWorker:
        def register(self) -> None:
            pass

        def run_once(self) -> None:
            pass

        def close(self) -> None:
            pass

    def make_worker(**kwargs: object) -> FakeWorker:
        return FakeWorker()

    def fail_registry_lookup(cluster: str) -> ClusterDefinition:
        raise AssertionError(f"unexpected registry lookup for {cluster}")

    monkeypatch.setattr(endpoint, "EndpointWorker", make_worker)
    monkeypatch.setattr(cli, "_require_cluster", fail_registry_lookup)

    zero = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "homelab",
            "--scheduler-provider",
            "external",
            "--control-query-concurrency",
            "0",
            "--once",
        ],
    )
    nonzero = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "homelab",
            "--scheduler-provider",
            "external",
            "--control-query-concurrency",
            "1",
            "--concurrency",
            "2",
            "--once",
        ],
    )

    assert zero.exit_code == 0, zero.output
    assert "control_query_concurrency=0" in zero.output
    assert "queue forever" in zero.output
    assert nonzero.exit_code == 0, nonzero.output
    assert "control_query_concurrency=0" not in nonzero.output


def test_endpoint_worker_info_exposes_bounded_readiness_mode(
    monkeypatch: MonkeyPatch,
) -> None:
    """Bootstrap can request readiness without exporting detailed installation records."""
    observed: dict[str, object] = {}

    def worker_info(**kwargs: object) -> dict[str, object]:
        observed.update(kwargs)
        return {
            "schema_version": "clio-relay.worker-readiness.v1",
            "cluster": kwargs["cluster"],
            "running": True,
        }

    monkeypatch.setattr(installation_module, "worker_runtime_info", worker_info)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "worker-info",
            "--cluster",
            "ares",
            "--freshness-seconds",
            "90",
            "--readiness-only",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["schema_version"] == "clio-relay.worker-readiness.v1"
    assert observed == {
        "cluster": "ares",
        "freshness_seconds": 90.0,
        "readiness_only": True,
        "pinned_install_receipt_path": None,
        "dev_mode": False,
    }


def test_endpoint_worker_info_forwards_pinned_install_receipt_path(
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#205: the CLI surface forwards the cluster's pinned receipt path."""
    observed: dict[str, object] = {}

    def worker_info(**kwargs: object) -> dict[str, object]:
        observed.update(kwargs)
        return {
            "schema_version": "clio-relay.worker-runtime-info.v1",
            "cluster": kwargs["cluster"],
            "running": True,
        }

    monkeypatch.setattr(installation_module, "worker_runtime_info", worker_info)

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "worker-info",
            "--cluster",
            "ares-p5run2",
            "--pinned-install-receipt-path",
            "$HOME/.local/share/clio-relay/generations/g1/install-receipt.json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (
        observed["pinned_install_receipt_path"]
        == "$HOME/.local/share/clio-relay/generations/g1/install-receipt.json"
    )


def test_endpoint_worker_info_dev_mode_flag_resolves_env_and_cluster_pin(
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#211: --dev-mode (cluster pin) combines with CLIO_RELAY_DEV_MODE either way."""
    observed: list[bool] = []

    def worker_info(**kwargs: object) -> dict[str, object]:
        observed.append(cast(bool, kwargs["dev_mode"]))
        return {
            "schema_version": "clio-relay.worker-runtime-info.v1",
            "cluster": kwargs["cluster"],
            "running": True,
        }

    monkeypatch.setattr(installation_module, "worker_runtime_info", worker_info)
    monkeypatch.delenv("CLIO_RELAY_DEV_MODE", raising=False)

    off = CliRunner().invoke(app, ["endpoint", "worker-info", "--cluster", "ares"])
    assert off.exit_code == 0, off.output

    on_via_flag = CliRunner().invoke(
        app, ["endpoint", "worker-info", "--cluster", "ares", "--dev-mode"]
    )
    assert on_via_flag.exit_code == 0, on_via_flag.output

    monkeypatch.setenv("CLIO_RELAY_DEV_MODE", "1")
    on_via_env = CliRunner().invoke(app, ["endpoint", "worker-info", "--cluster", "ares"])
    assert on_via_env.exit_code == 0, on_via_env.output

    assert observed == [False, True, True]


def test_endpoint_target_info_hashes_raw_machine_id_bytes(
    tmp_path: Path,
) -> None:
    machine_id = tmp_path / "machine-id"
    marker = b"production-site-id\n"
    machine_id.write_bytes(marker)

    observed = cli_endpoint._physical_site_marker_sha256(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        machine_id
    )

    assert observed == hashlib.sha256(marker).hexdigest()
    assert observed != hashlib.sha256(marker.strip()).hexdigest()

    machine_id.write_bytes(b"\n")
    with pytest.raises(ConfigurationError, match="physical site marker is empty"):
        cli_endpoint._physical_site_marker_sha256(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            machine_id
        )
