from __future__ import annotations

import platform
from pathlib import Path

import pytest

from clio_relay import frp_check as frp_check_module
from clio_relay.errors import ConfigurationError
from clio_relay.frp_check import run_frpc_connection_check
from clio_relay.relay_host import FrpcConfig


def test_frpc_connection_succeeds_when_process_stays_connected(tmp_path: Path) -> None:
    fake = _write_fake_frpc(tmp_path)

    lines = run_frpc_connection_check(
        frpc_bin=str(fake),
        config=FrpcConfig(
            server_addr="example.test",
            server_port=443,
            token="secret",
            local_port=8848,
            secret_key="stcp-secret",
        ),
        timeout_seconds=0.5,
    )

    assert lines[0] == "frpc stayed connected until timeout"


def test_frpc_failure_detail_is_bounded_by_bytes_not_line_count(tmp_path: Path) -> None:
    """The failure detail embedded in ``ConfigurationError`` used to be
    ``"\\n".join(stdout.splitlines()[-12:])`` -- a line-count heuristic with
    no byte guarantee at all. Three lines, each far larger than the T1
    budget, would previously all survive intact (well under the 12-line
    cap) and blow the detail out to roughly 9,000 bytes. The byte-bounded
    tail (doc §6.4) keeps the detail near the 2,000-byte T1 budget
    regardless of how many lines that spans.
    """
    payload_path = tmp_path / "frpc-output.txt"
    lines = ["x" * 3_000, "y" * 3_000, "z" * 3_000]
    payload_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    fake = _write_fake_failing_frpc(tmp_path, payload_path)

    with pytest.raises(ConfigurationError) as excinfo:
        run_frpc_connection_check(
            frpc_bin=str(fake),
            config=FrpcConfig(
                server_addr="example.test",
                server_port=443,
                token="secret",
                local_port=8848,
                secret_key="stcp-secret",
            ),
            timeout_seconds=5.0,
        )

    detail = str(excinfo.value)
    assert "[clio-relay: elided" in detail
    assert "x" * 3_000 not in detail  # the earliest line is well outside the tail window
    assert len(detail.encode("utf-8")) < 2_500  # far under the ~9,000-byte unbounded total


def test_frpc_failure_detail_logs_the_discarded_truncation_record(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F8 (#231 R6 review): the structured truncation record used to be
    built then discarded outright once the bounded string was returned.
    ``ConfigurationError`` has no typed data channel of its own (unlike
    ``door_errors.classify()``'s exception dispatch), so the record is
    logged here instead of silently dropped.
    """
    payload_path = tmp_path / "frpc-output.txt"
    payload_path.write_text("z" * 5_000 + "\n", encoding="utf-8")
    fake = _write_fake_failing_frpc(tmp_path, payload_path)

    with (
        caplog.at_level("WARNING", logger="clio_relay.frp_check"),
        pytest.raises(ConfigurationError),
    ):
        run_frpc_connection_check(
            frpc_bin=str(fake),
            config=FrpcConfig(
                server_addr="example.test",
                server_port=443,
                token="secret",
                local_port=8848,
                secret_key="stcp-secret",
            ),
            timeout_seconds=5.0,
        )

    assert "frpc connection-check failure detail was elided" in caplog.text


def test_no_truncation_logs_nothing(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Sabotage twin: an under-budget failure must not log a fabricated
    elision record.
    """
    payload_path = tmp_path / "frpc-output.txt"
    payload_path.write_text("short\n", encoding="utf-8")
    fake = _write_fake_failing_frpc(tmp_path, payload_path)

    with (
        caplog.at_level("WARNING", logger="clio_relay.frp_check"),
        pytest.raises(ConfigurationError),
    ):
        run_frpc_connection_check(
            frpc_bin=str(fake),
            config=FrpcConfig(
                server_addr="example.test",
                server_port=443,
                token="secret",
                local_port=8848,
                secret_key="stcp-secret",
            ),
            timeout_seconds=5.0,
        )

    assert "elided" not in caplog.text


def test_bounded_capture_applies_the_generous_t3_record_cap() -> None:
    """F8 (#231 R6 review): subprocess.run() has no native output-byte cap
    of its own -- Popen.communicate() has already drained the pipe to EOF
    (holding the full unbounded string) by the time this function ever
    runs, so a generous T3-shaped RECORD-time bound (doc §6.4) is applied
    right after that unavoidable read completes, distinct from (and much
    larger than) ``_bounded_failure_detail``'s later, much smaller T1 tail
    budget. A3 (#231 R6-fix review): the constants were renamed off
    "READ_*" -- this was never actually bounding the read itself.
    """
    huge = "Q" * (
        frp_check_module.FRPC_CHECK_CAPTURE_HEAD_MAX_BYTES
        + frp_check_module.FRPC_CHECK_CAPTURE_TAIL_MAX_BYTES
        + 1_000_000
    )

    bounded = frp_check_module._bounded_capture(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        huge
    )

    assert len(bounded.encode("utf-8")) < len(huge.encode("utf-8"))
    assert "[clio-relay: elided" in bounded


def test_bounded_capture_logs_the_discarded_truncation_record(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A3 (#231 R6-fix review): ``_bounded_capture`` used to build the
    structured truncation record then discard it outright -- the same gap
    its sibling ``_bounded_failure_detail`` already closed (see
    ``test_frpc_failure_detail_logs_the_discarded_truncation_record``
    above) for the T1 tail budget. Exercised directly against the private
    helper since triggering this from ``run_frpc_connection_check`` would
    require an 8 MiB+8 MiB fixture payload.
    """
    huge = "Q" * (
        frp_check_module.FRPC_CHECK_CAPTURE_HEAD_MAX_BYTES
        + frp_check_module.FRPC_CHECK_CAPTURE_TAIL_MAX_BYTES
        + 1_000_000
    )

    with caplog.at_level("WARNING", logger="clio_relay.frp_check"):
        frp_check_module._bounded_capture(huge)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert "frpc raw output capture was elided" in caplog.text


def test_bounded_capture_logs_nothing_when_nothing_is_elided(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Sabotage twin: an under-budget capture must not log a fabricated
    elision record.
    """
    with caplog.at_level("WARNING", logger="clio_relay.frp_check"):
        bounded = frp_check_module._bounded_capture(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "short"
        )

    assert bounded == "short"
    assert "elided" not in caplog.text


def test_timeout_path_output_is_bounded_before_splitlines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timeout branch's ``splitlines()`` return (frpc staying connected
    IS this probe's success signal) must not be able to hold an unbounded
    capture -- the record-time cap applies before this return, not only the
    failure detail. The module-level cap constants are shrunk here so the
    fixture payload can stay small and fast rather than needing a genuinely
    multi-megabyte subprocess dump.
    """
    monkeypatch.setattr(frp_check_module, "FRPC_CHECK_CAPTURE_HEAD_MAX_BYTES", 10)
    monkeypatch.setattr(frp_check_module, "FRPC_CHECK_CAPTURE_TAIL_MAX_BYTES", 10)
    fake = _write_fake_frpc_with_output(
        tmp_path,
        "login to server success\n" + "Q" * 500,
        exits_cleanly=False,
    )

    lines = run_frpc_connection_check(
        frpc_bin=str(fake),
        config=FrpcConfig(
            server_addr="example.test",
            server_port=443,
            token="secret",
            local_port=8848,
            secret_key="stcp-secret",
        ),
        timeout_seconds=0.5,
    )

    assert lines[0] == "frpc stayed connected until timeout"
    joined = "\n".join(lines)
    assert "[clio-relay: elided" in joined
    assert "Q" * 500 not in joined


def test_clean_exit_output_is_bounded_before_splitlines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``returncode == 0`` branch's ``splitlines()`` return must not be
    able to hold an unbounded capture either.
    """
    monkeypatch.setattr(frp_check_module, "FRPC_CHECK_CAPTURE_HEAD_MAX_BYTES", 10)
    monkeypatch.setattr(frp_check_module, "FRPC_CHECK_CAPTURE_TAIL_MAX_BYTES", 10)
    fake = _write_fake_frpc_with_output(tmp_path, "Q" * 500, exits_cleanly=True)

    lines = run_frpc_connection_check(
        frpc_bin=str(fake),
        config=FrpcConfig(
            server_addr="example.test",
            server_port=443,
            token="secret",
            local_port=8848,
            secret_key="stcp-secret",
        ),
        timeout_seconds=5.0,
    )

    assert lines[0] == "frpc exited cleanly"
    joined = "\n".join(lines)
    assert "[clio-relay: elided" in joined
    assert "Q" * 500 not in joined


def _write_fake_frpc_with_output(tmp_path: Path, output: str, *, exits_cleanly: bool) -> Path:
    """A fake frpc that emits ``output`` then either exits 0 or stays connected."""
    payload_path = tmp_path / f"frpc-output-{exits_cleanly}.txt"
    payload_path.write_text(output, encoding="utf-8")
    tail = "exit /b 0\n" if exits_cleanly else "ping -n 3 127.0.0.1 > nul\n"
    posix_tail = "" if exits_cleanly else "sleep 2\n"
    if platform.system().lower() == "windows":
        path = tmp_path / f"fake-frpc-output-{exits_cleanly}.cmd"
        path.write_text(
            f'@echo off\ntype "{payload_path}"\n{tail}',
            encoding="utf-8",
        )
        return path
    path = tmp_path / f"fake-frpc-output-{exits_cleanly}"
    path.write_text(
        f'#!/usr/bin/env sh\ncat "{payload_path}"\n{posix_tail}',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | 0o111)
    return path


def _write_fake_failing_frpc(tmp_path: Path, payload_path: Path) -> Path:
    """A fake frpc that dumps ``payload_path`` to stdout and exits nonzero."""
    if platform.system().lower() == "windows":
        path = tmp_path / "fake-failing-frpc.cmd"
        path.write_text(
            f'@echo off\ntype "{payload_path}"\nexit /b 1\n',
            encoding="utf-8",
        )
        return path
    path = tmp_path / "fake-failing-frpc"
    path.write_text(
        f'#!/usr/bin/env sh\ncat "{payload_path}"\nexit 1\n',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | 0o111)
    return path


def _write_fake_frpc(tmp_path: Path) -> Path:
    if platform.system().lower() == "windows":
        path = tmp_path / "fake-frpc.cmd"
        path.write_text(
            "@echo off\necho login to server success\nping -n 3 127.0.0.1 > nul\n",
            encoding="utf-8",
        )
        return path
    path = tmp_path / "fake-frpc"
    path.write_text(
        "#!/usr/bin/env sh\necho login to server success\nsleep 2\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | 0o111)
    return path
