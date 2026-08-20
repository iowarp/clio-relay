"""Tests for the ``installation-write-receipt``/``installation-info``/
``bootstrap-inspect`` top-level command group (iowarp/clio-relay#231).

The one ``installation-write-receipt``-specific test moved out of
``tests/test_cli.py`` alongside its extraction into ``src/clio_relay/
cli_installation_receipt.py``, per ground rule 3 (SS2 of ``docs/design/
relay-architecture-2026-08.md``): a test reachable only through this command
group moves with the logic it exercises.

``installation-info`` has no dedicated ``CliRunner``-level test anywhere in
the suite (only a slow ``uv tool install`` + subprocess smoke test in
``tests/test_validation_report.py``, itself untouched -- it exercises the
real installed executable, not this module's internal wiring, so it needed
no changes). ``bootstrap-inspect`` already has extensive dedicated coverage
in ``tests/test_bootstrap_fast_path.py`` -- one of the four non-test_cli
monkeypatch cross-check files -- which patches every one of this group's six
reassigned collaborators (``installation.installation_info``, ``core_queue.
ClioCoreQueue``, ``bootstrap_reconcile.inspect_exact_bootstrap_noop``,
``bounded_process.run_bounded_process``, ``installation.worker_runtime_info``,
``bootstrap_reconcile.bootstrap_invocation_lock``) on their owner modules
directly, never through ``cli.py``, so it needed no patch-target changes and
stays exactly where it is.

**Patch-target parity.** The moved test patches ``installation_module.
write_self_install_receipt`` on the owner module directly (not through
``cli.py``), so the move needed no patch-target change at all.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.installation as installation_module
from clio_relay.cli import app
from clio_relay.installation import InstallReceipt
from clio_relay.validation_report import SoftwareIdentity


def test_installation_write_receipt_forwards_components_from(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The CLI surface forwards --components-from to write_self_install_receipt."""
    observed: dict[str, object] = {}
    output_path = tmp_path / "generations" / "mixed" / "install-receipt.json"
    source_path = tmp_path / "generation-install-receipt.json"

    def write_receipt(path: Path, **kwargs: object) -> InstallReceipt:
        observed["path"] = path
        observed.update(kwargs)
        return InstallReceipt(
            installed_at=datetime.now(UTC),
            install_spec="checkout",
            requested_source="vcs",
            distribution_version="0.0.0",
            software=SoftwareIdentity(version="0.0.0"),
        )

    monkeypatch.setattr(installation_module, "write_self_install_receipt", write_receipt)

    result = CliRunner().invoke(
        app,
        [
            "installation-write-receipt",
            "--self",
            "--output",
            str(output_path),
            "--components-from",
            str(source_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert observed["path"] == output_path
    assert observed["components_from"] == source_path
    assert observed["force"] is False
