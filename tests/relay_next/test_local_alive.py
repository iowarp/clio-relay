"""The local adapter tells a dead supervisor from a live one on Windows too.

Before, every pid counted as alive on Windows, so a job whose supervisor died
with its relay stayed running and could never be cancelled.
"""

from __future__ import annotations

import pytest

from clio_relay import shutdown
from clio_relay.adapters import local
from clio_relay.adapters.local import pid_alive


@pytest.mark.parametrize(
    ("probe", "alive"),
    [("gone", False), ("relay", True), ("other", True), ("unknown", True)],
)
def test_windows_liveness_follows_the_process_probe(
    monkeypatch: pytest.MonkeyPatch, probe: str, alive: bool
) -> None:
    def probed(pid: int) -> tuple[str, str]:
        return probe, ""

    monkeypatch.setattr(local, "POSIX", False)
    monkeypatch.setattr(shutdown, "probe_windows", probed)
    assert pid_alive(4242) is alive


def test_no_pid_is_never_alive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(local, "POSIX", False)
    assert pid_alive(None) is False
    assert pid_alive(0) is False
