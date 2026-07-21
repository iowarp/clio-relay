"""Read-only queue readiness contracts used by bootstrap reconciliation."""

from __future__ import annotations

import os
import stat
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import clio_relay.core_queue as core_queue_module
from clio_relay.core_queue import ClioCoreQueue, LegacyQueueStateError
from clio_relay.errors import QueueConflictError
from clio_relay.models import EndpointRegistration, EndpointRole


def _tree_identity(root: Path) -> dict[str, tuple[bytes, int]]:
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file()
    }


def test_readiness_info_does_not_initialize_a_missing_queue(tmp_path: Path) -> None:
    root = tmp_path / "core"

    result = ClioCoreQueue(root).readiness_info()

    assert result == {
        "schema_version": "clio-relay.queue-readiness.v1",
        "complete": False,
        "sealed": False,
        "repair_required": True,
        "inspection_mode": "fixed_layout_and_seal",
        "record_history_scanned": False,
        "records_examined": 0,
        "seal_trust_model": "owner_private_cooperative_same_uid_writers",
        "cryptographic_replay_protection": False,
        "record_integrity_verification": "on_access",
        "bounds": {
            "fixed_queue_family_count": 58,
            "fixed_global_order_family_count": 4,
        },
    }
    assert not root.exists()


def test_readiness_info_verifies_a_sealed_queue_without_writes(tmp_path: Path) -> None:
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.initialize()
    before = _tree_identity(root)

    result = ClioCoreQueue(root).readiness_info()

    assert result["schema_version"] == "clio-relay.queue-readiness.v1"
    assert result["complete"] is True
    assert result["sealed"] is True
    assert result["repair_required"] is False
    assert result["record_history_scanned"] is False
    assert _tree_identity(root) == before


def test_read_only_fresh_endpoint_lookup_never_scans_history_or_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    now = datetime.now(UTC)
    current = queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="endpoint_current",
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
            registered_at=now,
            last_seen_at=now,
        )
    )
    queue.register_endpoint(
        EndpointRegistration(
            endpoint_id="endpoint_historical",
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="old-worker",
            pid=456,
            registered_at=now - timedelta(days=30),
            last_seen_at=now - timedelta(days=30),
        )
    )
    before = _tree_identity(root)
    original_scan = ClioCoreQueue._scan_json_record_paths  # noqa: SLF001

    def reject_historical_scan(
        directory: Path,
        *,
        limit: int,
        label: str,
    ) -> tuple[list[Path], bool]:
        if directory == root / "endpoints":
            raise AssertionError("historical endpoints must not be scanned")
        return original_scan(directory, limit=limit, label=label)

    monkeypatch.setattr(
        core_queue_module.ClioCoreQueue,
        "_scan_json_record_paths",
        staticmethod(reject_historical_scan),
    )

    endpoints, truncated = ClioCoreQueue(root).scan_fresh_endpoints_read_only(
        limit=10,
        fresh_seconds=120,
        cluster="ares",
        now=now,
    )

    assert truncated is False
    assert [endpoint.endpoint_id for endpoint in endpoints] == [current.endpoint_id]
    assert _tree_identity(root) == before


def test_read_only_fresh_endpoint_lookup_refuses_unsealed_queue_without_writes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "core"

    with pytest.raises(QueueConflictError, match="sealed indexed queue"):
        ClioCoreQueue(root).scan_fresh_endpoints_read_only(
            limit=10,
            fresh_seconds=120,
            cluster="ares",
        )

    assert not root.exists()


def test_posix_queue_initialization_creates_owner_private_fixed_directories(
    tmp_path: Path,
) -> None:
    """The seal trust premise is established by actual directory protections."""
    if os.name == "nt":
        return
    root = tmp_path / "core"

    ClioCoreQueue(root).initialize()

    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "jobs").stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "global_order" / "jobs" / "entries").stat().st_mode) == 0o700


def test_posix_readiness_refuses_nonprivate_sealed_root_without_repair(
    tmp_path: Path,
) -> None:
    """Read-only readiness never repairs a violated owner-private premise."""
    if os.name == "nt":
        return
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    os.chmod(root, 0o755)
    before = _tree_identity(root)

    with pytest.raises(LegacyQueueStateError, match="readable or writable"):
        ClioCoreQueue(root).readiness_info()

    assert stat.S_IMODE(root.stat().st_mode) == 0o755
    assert _tree_identity(root) == before


@pytest.mark.parametrize("relative", [Path("global_order"), Path("global_order/jobs")])
def test_readiness_refuses_redirected_global_order_intermediate(
    tmp_path: Path,
    relative: Path,
) -> None:
    """Every intermediate fixed-layout directory remains inside the pinned root."""
    root = tmp_path / "core"
    ClioCoreQueue(root).initialize()
    redirected = root / relative
    outside = tmp_path / ("outside-" + "-".join(relative.parts))
    redirected.rename(outside)
    if os.name == "nt":
        junction = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(redirected), str(outside)],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert junction.returncode == 0, junction.stderr
    else:
        redirected.symlink_to(outside, target_is_directory=True)

    with pytest.raises(LegacyQueueStateError, match="not an owned directory"):
        ClioCoreQueue(root).readiness_info()
