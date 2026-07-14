from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any, Protocol, cast

import pytest


class ProgressModule(Protocol):
    def adapter_from_config(self, config: object) -> object:
        """Build a progress adapter."""
        ...

    def append_progress_record(self, record: dict[str, object]) -> None:
        """Append a progress record."""
        ...


class Adapter(Protocol):
    def observe_stdout(self, line: str) -> list[dict[str, object]]:
        """Observe stdout."""
        ...


def test_bounded_command_rejects_external_progress_adapter() -> None:
    module = _load_progress_module()
    try:
        module.adapter_from_config({"adapter": "site-progress", "total_steps": 150})
    except ValueError as exc:
        assert "unsupported progress adapter: site-progress" in str(exc)
    else:
        raise AssertionError("bounded_command must not own external application semantics")


def test_regex_progress_adapter_writes_side_channel(tmp_path: Path) -> None:
    module = _load_progress_module()
    adapter = cast(
        Adapter,
        module.adapter_from_config(
            {
                "adapter": "regex",
                "pattern": r"iter=(?P<current>\d+) of (?P<total>\d+)",
                "label": "iteration",
                "current_group": "current",
                "total_group": "total",
            }
        ),
    )

    record = adapter.observe_stdout("iter=4 of 10\n")[0]
    sidecar = tmp_path / "progress.jsonl"
    _precreate_private_file(sidecar)
    previous = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    previous_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    os.environ["CLIO_RELAY_PROGRESS_FILE"] = str(sidecar)
    os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = "test-token"
    try:
        module.append_progress_record(record)
    finally:
        if previous is None:
            os.environ.pop("CLIO_RELAY_PROGRESS_FILE", None)
        else:
            os.environ["CLIO_RELAY_PROGRESS_FILE"] = previous
        if previous_token is None:
            os.environ.pop("CLIO_RELAY_PROGRESS_TOKEN", None)
        else:
            os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = previous_token

    decoded = json.loads(sidecar.read_text(encoding="utf-8"))
    assert decoded["schema_version"] == "clio-relay.progress-sidecar-record.v1"
    assert decoded["sequence"] == 1
    progress = decoded["progress"]
    assert progress["label"] == "iteration"
    assert progress["current"] == 4
    assert progress["total"] == 10
    assert progress["metadata"]["source"] == "jarvis_package"
    assert progress["metadata"]["package_name"] == "clio_relay.bounded_command"
    assert "test-token" not in sidecar.read_text(encoding="utf-8")
    signed = {key: decoded[key] for key in ("schema_version", "sequence", "progress")}
    canonical = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    assert hmac.compare_digest(
        decoded["progress_hmac"],
        hmac.new(b"test-token", canonical, hashlib.sha256).hexdigest(),
    )


def test_regex_progress_adapter_cannot_spoof_package_identity() -> None:
    module = _load_progress_module()
    adapter = cast(
        Adapter,
        module.adapter_from_config(
            {
                "adapter": "regex",
                "pattern": r"step=(?P<current>\d+)",
                "metadata": {
                    "source": "jarvis_package",
                    "adapter": "site-progress",
                    "package_name": "site.simulation",
                    "package_version": "2.1",
                    "run_id": "job_spoofed",
                    "user_field": "kept",
                },
            }
        ),
    )

    record = adapter.observe_stdout("step=4\n")[0]
    metadata = cast(dict[str, object], record["metadata"])

    assert metadata["source"] == "jarvis_package"
    assert metadata["adapter"] == "regex"
    assert metadata["package_name"] == "clio_relay.bounded_command"
    assert metadata["package_version"] == "builtin"
    assert "run_id" not in metadata
    assert metadata["user_field"] == "kept"


def test_progress_sidecar_rejects_oversized_records(tmp_path: Path) -> None:
    module = _load_progress_module()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_private_file(sidecar)
    previous_file = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    previous_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    os.environ["CLIO_RELAY_PROGRESS_FILE"] = str(sidecar)
    os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = "test-token"
    try:
        with pytest.raises(ValueError, match="record exceeded its byte limit"):
            module.append_progress_record(
                {"label": "oversized", "metadata": {"payload": "x" * 70_000}}
            )
    finally:
        _restore_environment("CLIO_RELAY_PROGRESS_FILE", previous_file)
        _restore_environment("CLIO_RELAY_PROGRESS_TOKEN", previous_token)

    assert sidecar.read_bytes() == b""


def test_progress_sidecar_rejects_non_regular_target(tmp_path: Path) -> None:
    module = _load_progress_module()
    sidecar = tmp_path / "progress.jsonl"
    sidecar.mkdir()
    previous_file = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    previous_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    os.environ["CLIO_RELAY_PROGRESS_FILE"] = str(sidecar)
    os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = "test-token"
    try:
        with pytest.raises(OSError):
            module.append_progress_record({"label": "invalid"})
    finally:
        _restore_environment("CLIO_RELAY_PROGRESS_FILE", previous_file)
        _restore_environment("CLIO_RELAY_PROGRESS_TOKEN", previous_token)


def test_progress_sidecar_enforces_bounded_total_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_progress_module()
    sidecar = tmp_path / "progress.jsonl"
    _precreate_private_file(sidecar)
    sidecar.write_text("x" * 100, encoding="utf-8")
    monkeypatch.setattr(cast(Any, module), "PROGRESS_SIDECAR_MAX_BYTES", 100)
    previous_file = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    previous_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    os.environ["CLIO_RELAY_PROGRESS_FILE"] = str(sidecar)
    os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = "test-token"
    try:
        with pytest.raises(ValueError, match="sidecar exceeded its byte limit"):
            module.append_progress_record({"label": "bounded"})
    finally:
        _restore_environment("CLIO_RELAY_PROGRESS_FILE", previous_file)
        _restore_environment("CLIO_RELAY_PROGRESS_TOKEN", previous_token)


def _restore_environment(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def _precreate_private_file(path: Path) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
    finally:
        os.close(descriptor)


def _load_progress_module() -> ProgressModule:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "bounded_command"
        / "progress.py"
    )
    spec = importlib.util.spec_from_file_location("bounded_command_progress_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load progress module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return cast(ProgressModule, module)
