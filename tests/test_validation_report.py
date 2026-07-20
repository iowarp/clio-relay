"""Tests for stable validation evidence and the release gate."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import zipfile
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from clio_relay import validation_report as validation_report_module
from clio_relay.errors import ConfigurationError
from clio_relay.models import GatewaySession, GatewaySessionState
from clio_relay.remote_mcp import (
    RemoteMcpAcceptanceCheck,
    RemoteMcpAcceptanceReport,
    RemoteMcpStructuredResultExpectation,
    build_remote_mcp_structured_result_check,
)
from clio_relay.service_runtime import ServiceRuntimeStartResult, ServiceRuntimeStopResult
from clio_relay.session_lifecycle import (
    CleanupResource,
    RemoteSessionStateEvidence,
    SessionLifecycleReport,
)
from clio_relay.validation_report import (
    EvidenceOrigin,
    EvidenceReference,
    EvidenceTrust,
    InstallSource,
    InstallSourceKind,
    LiveValidationReport,
    ReleaseGatePolicy,
    ReleaseGateRequirement,
    ReleaseGateResult,
    ReleaseResourceRequirement,
    ReleaseTargetIdentity,
    SoftwareIdentity,
    TransportCleanupResourceEvidence,
    TransportProbeEvidence,
    ValidationCheck,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    default_report_path,
    evaluate_release_gate,
    load_release_gate_policy,
    load_validation_report,
    new_live_validation_report,
    render_validation_markdown,
    transport_probe_evidence_line,
    write_validation_report,
)

_verify_running_artifact_identity = cast(
    Callable[..., bool],
    validation_report_module._verify_running_artifact_identity,  # pyright: ignore[reportPrivateUsage]
)
_uv_executable_identity = cast(
    Callable[[str | None], tuple[bool, str | None, str | None]],
    validation_report_module._uv_executable_identity,  # pyright: ignore[reportPrivateUsage]
)


def _timestamp() -> datetime:
    return datetime(2026, 7, 10, 18, 0, tzinfo=UTC)


def test_install_source_records_unverified_uvx_without_crashing_report_creation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    class FakeDistribution:
        version = "1.0.0"

        def read_text(self, _filename: str) -> str | None:
            return None

    def fake_distribution(_name: str) -> object:
        return FakeDistribution()

    def fake_files(_name: str) -> Path:
        return tmp_path

    def verified_artifact(*_args: object, **_kwargs: object) -> bool:
        return True

    def unverified_launcher(
        _launcher: str,
        **_kwargs: object,
    ) -> tuple[bool, dict[str, object]]:
        return False, {"verified": False}

    monkeypatch.setattr(validation_report_module.metadata, "distribution", fake_distribution)
    monkeypatch.setattr(validation_report_module.resources, "files", fake_files)
    monkeypatch.setattr(
        validation_report_module,
        "_verify_running_artifact_identity",
        verified_artifact,
    )
    monkeypatch.setattr(
        validation_report_module,
        "_detect_launcher_receipt",
        unverified_launcher,
    )

    source = validation_report_module.detect_install_source(
        launcher="uvx",
        artifact_sha256="a" * 64,
    )

    assert source.artifact_identity_verified is True
    assert source.launcher_verified is False
    assert source.released_artifact is False


class _FakeWheelDistribution:
    def __init__(self, root: Path) -> None:
        self.root = root

    def locate_file(self, path: str) -> Path:
        return self.root / path


def _test_wheel(tmp_path: Path) -> tuple[Path, Path, str, metadata.Distribution]:
    installed_root = tmp_path / "installed"
    installed_file = installed_root / "clio_relay" / "probe.py"
    installed_file.parent.mkdir(parents=True)
    content = b"PROBE = 'wheel-record-bound'\n"
    installed_file.write_bytes(content)
    encoded = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).decode().rstrip("=")
    record_path = "clio_relay-1.0.0.dist-info/RECORD"
    record = f"clio_relay/probe.py,sha256={encoded},{len(content)}\n{record_path},,\n"
    wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("clio_relay/probe.py", content)
        archive.writestr(record_path, record)
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    distribution = cast(metadata.Distribution, _FakeWheelDistribution(installed_root))
    return wheel, installed_file, digest, distribution


def test_local_wheel_identity_hashes_archive_and_verifies_installed_record(
    tmp_path: Path,
) -> None:
    wheel, installed_file, digest, distribution = _test_wheel(tmp_path)
    direct_url = {
        "url": wheel.as_uri(),
        "archive_info": {"hash": f"sha256={digest}"},
    }

    assert _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url=direct_url,
        artifact_sha256=digest,
        launcher="uvx",
    )

    installed_file.write_text("PROBE = 'tampered'\n", encoding="utf-8")
    assert not _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url=direct_url,
        artifact_sha256=digest,
        launcher="uvx",
    )


def test_local_wheel_identity_rejects_direct_url_hash_without_archive(tmp_path: Path) -> None:
    wheel, _, digest, distribution = _test_wheel(tmp_path)
    wheel.unlink()

    assert not _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url={
            "url": wheel.as_uri(),
            "archive_info": {"hash": f"sha256={digest}"},
        },
        artifact_sha256=digest,
        launcher="uvx",
    )


def test_local_wheel_identity_rejects_forged_direct_url_hash(tmp_path: Path) -> None:
    wheel, _, _, distribution = _test_wheel(tmp_path)
    forged_digest = "f" * 64

    assert not _verify_running_artifact_identity(
        distribution,
        detected_kind=InstallSourceKind.WHEEL,
        direct_url={
            "url": wheel.as_uri(),
            "archive_info": {"hash": f"sha256={forged_digest}"},
        },
        artifact_sha256=forged_digest,
        launcher="uvx",
    )


def test_released_https_wheel_binds_url_sha_record_and_uv_tool(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    wheel, _, digest, installed = _test_wheel(tmp_path)
    wheel_bytes = wheel.read_bytes()
    source_url = (
        "https://github.com/iowarp/clio-relay/releases/download/"
        "v1.0.0/clio_relay-1.0.0-py3-none-any.whl"
    )

    class RemoteDistribution(_FakeWheelDistribution):
        version = "1.0.0"

        def read_text(self, filename: str) -> str | None:
            if filename == "direct_url.json":
                return json.dumps({"url": source_url, "archive_info": {}})
            return None

    class WheelResponse(io.BytesIO):
        def __enter__(self) -> WheelResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            self.close()

        def geturl(self) -> str:
            return (
                "https://release-assets.githubusercontent.com/"
                "github-production-release-asset/exact-wheel?token=test"
            )

    class WheelOpener:
        def open(self, *_args: object, **_kwargs: object) -> WheelResponse:
            return WheelResponse(wheel_bytes)

    installed_root = cast(_FakeWheelDistribution, installed).root
    distribution = cast(metadata.Distribution, RemoteDistribution(installed_root))

    def find_distribution(_name: str) -> metadata.Distribution:
        return distribution

    def package_files(_name: str) -> Path:
        return tmp_path

    def verified_launcher(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[bool, dict[str, object]]:
        return True, {"verified": True}

    def build_opener(*_args: object, **_kwargs: object) -> WheelOpener:
        return WheelOpener()

    def publicly_resolved(_url: str) -> bool:
        return True

    monkeypatch.setattr(
        validation_report_module.metadata,
        "distribution",
        find_distribution,
    )
    monkeypatch.setattr(validation_report_module.resources, "files", package_files)
    monkeypatch.setattr(
        validation_report_module,
        "_detect_launcher_receipt",
        verified_launcher,
    )
    monkeypatch.setattr(
        validation_report_module.urllib.request,
        "build_opener",
        build_opener,
    )
    monkeypatch.setattr(
        validation_report_module,
        "_url_host_resolves_publicly",
        publicly_resolved,
    )

    source = validation_report_module.detect_install_source(
        launcher="uv-tool",
        artifact_sha256=digest,
    )

    assert source.kind is InstallSourceKind.WHEEL
    assert source.reference == source_url
    assert source.artifact_identity_verified is True
    assert source.launcher_verified is True
    assert source.released_artifact is True

    rejected = validation_report_module.detect_install_source(
        launcher="uv-tool",
        artifact_sha256="f" * 64,
    )
    assert rejected.artifact_identity_verified is False
    assert rejected.released_artifact is False


@pytest.mark.parametrize(
    "url",
    [
        "https://user@github.com/iowarp/clio-relay/releases/download/v1.0.0/"
        "clio_relay-1.0.0-py3-none-any.whl",
        "https://github.com/iowarp/clio-relay/releases/download/v1.0.0/"
        "clio_relay-1.0.0-py3-none-any.whl?redirect=https://127.0.0.1",
        "https://github.com/other/clio-relay/releases/download/v1.0.0/"
        "clio_relay-1.0.0-py3-none-any.whl",
        "https://github.com/iowarp/clio-relay/releases/download/v1.0.0/"
        "clio_relay-1.0.1-py3-none-any.whl",
        "https://127.0.0.1/clio_relay-1.0.0-py3-none-any.whl",
        "https://github.com:invalid/iowarp/clio-relay/releases/download/v1.0.0/"
        "clio_relay-1.0.0-py3-none-any.whl",
    ],
)
def test_remote_wheel_fetch_rejects_noncanonical_sources(url: str) -> None:
    assert validation_report_module._is_official_release_wheel_url(url) is False  # pyright: ignore[reportPrivateUsage]
    assert (
        validation_report_module._direct_wheel_bytes(  # pyright: ignore[reportPrivateUsage]
            {"url": url, "archive_info": {}}
        )
        is None
    )


def test_release_wheel_fetch_rejects_private_dns_and_unsafe_redirects(
    monkeypatch: MonkeyPatch,
) -> None:
    source_url = (
        "https://github.com/iowarp/clio-relay/releases/download/"
        "v1.0.0/clio_relay-1.0.0-py3-none-any.whl"
    )

    def private_dns(
        _host: str,
        _port: int,
        *,
        type: socket.SocketKind,
    ) -> list[tuple[socket.AddressFamily, socket.SocketKind, int, str, tuple[str, int]]]:
        assert type is socket.SOCK_STREAM
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(validation_report_module.socket, "getaddrinfo", private_dns)

    assert validation_report_module._is_official_release_wheel_url(source_url) is True  # pyright: ignore[reportPrivateUsage]
    assert validation_report_module._url_host_resolves_publicly(source_url) is False  # pyright: ignore[reportPrivateUsage]
    handler = validation_report_module._ReleaseWheelRedirectHandler()  # pyright: ignore[reportPrivateUsage]
    request = validation_report_module.urllib.request.Request(source_url)
    with pytest.raises(validation_report_module.urllib.error.HTTPError):
        handler.redirect_request(
            request,
            None,
            302,
            "Found",
            {},
            "https://127.0.0.1/private-wheel.whl",
        )


def test_uv_tool_receipt_binds_exact_remote_url_and_launcher(tmp_path: Path) -> None:
    environment = tmp_path / "tools" / "clio-relay"
    environment.mkdir(parents=True)
    launcher = tmp_path / "bin" / "clio-relay.exe"
    launcher.parent.mkdir()
    launcher.write_bytes(b"launcher")
    source_url = "https://example.invalid/releases/clio_relay-1.0.0-py3-none-any.whl"
    (environment / "uv-receipt.toml").write_text(
        "\n".join(
            [
                "[tool]",
                f'requirements = [{{ name = "clio-relay", url = "{source_url}" }}]',
                "entrypoints = [",
                (
                    '  { name = "clio-relay", '
                    f'install-path = "{launcher.as_posix()}", from = "clio-relay" }},'
                ),
                "]",
            ]
        ),
        encoding="utf-8",
    )

    class Distribution:
        version = "1.0.0"

        def read_text(self, filename: str) -> str | None:
            if filename == "direct_url.json":
                return json.dumps({"url": source_url, "archive_info": {}})
            return None

    identity = validation_report_module._persistent_uv_tool_receipt_identity(  # pyright: ignore[reportPrivateUsage]
        environment_prefix=environment,
        tool_executable=launcher.absolute(),
        distribution=cast(metadata.Distribution, Distribution()),
    )

    assert identity["launcher_bound"] is True
    assert identity["source_bound"] is True
    assert identity["requirement_url"] == source_url
    assert identity["verified"] is True

    changed = (
        (environment / "uv-receipt.toml")
        .read_text(encoding="utf-8")
        .replace(
            source_url,
            f"{source_url}.changed",
        )
    )
    (environment / "uv-receipt.toml").write_text(changed, encoding="utf-8")
    rejected = validation_report_module._persistent_uv_tool_receipt_identity(  # pyright: ignore[reportPrivateUsage]
        environment_prefix=environment,
        tool_executable=launcher.absolute(),
        distribution=cast(metadata.Distribution, Distribution()),
    )
    assert rejected["source_bound"] is False
    assert rejected["verified"] is False


@pytest.mark.parametrize(
    ("launcher_case", "expected_verified"),
    [
        ("scoped", True),
        ("missing", False),
        ("invalid-override", False),
        *([("non-executable", False)] if os.name != "nt" else []),
    ],
)
def test_persistent_uv_tool_scopes_launcher_discovery_and_fails_closed(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    launcher_case: str,
    expected_verified: bool,
) -> None:
    tool_directory = tmp_path / "uv" / "tools"
    environment = tool_directory / "clio-relay"
    tool_bin = tmp_path / "uv" / "bin"
    package = environment / "Lib" / "site-packages" / "clio_relay"
    interpreter = environment / "Scripts" / "python.exe"
    launcher_name = "clio-relay.exe" if os.name == "nt" else "clio-relay"
    launcher = tool_bin / launcher_name
    stale_launcher = tmp_path / "path-bin" / launcher_name
    uv = tmp_path / "bin" / "uv.exe"
    base_prefix = tmp_path / "python"
    for directory in (
        package,
        interpreter.parent,
        tool_bin,
        stale_launcher.parent,
        uv.parent,
        base_prefix,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    interpreter.write_bytes(b"python")
    launcher.write_bytes(b"launcher")
    launcher.chmod(0o755)
    stale_launcher.write_bytes(b"stale launcher from PATH")
    stale_launcher.chmod(0o755)
    uv.write_bytes(b"uv")
    source_url = (
        "https://github.com/iowarp/clio-relay/releases/download/"
        "v1.0.0/clio_relay-1.0.0-py3-none-any.whl"
    )
    (environment / "uv-receipt.toml").write_text(
        "\n".join(
            [
                "[tool]",
                f'requirements = [{{ name = "clio-relay", url = "{source_url}" }}]',
                "entrypoints = [",
                (
                    '  { name = "clio-relay", '
                    f'install-path = "{launcher.as_posix()}", from = "clio-relay" }},'
                ),
                "]",
            ]
        ),
        encoding="utf-8",
    )

    class Distribution:
        version = "1.0.0"

        def read_text(self, filename: str) -> str | None:
            if filename == "direct_url.json":
                return json.dumps({"url": source_url, "archive_info": {}})
            return None

    launcher_digest = hashlib.sha256(launcher.read_bytes()).hexdigest()
    if launcher_case == "missing":
        launcher.unlink()
    elif launcher_case == "non-executable":
        launcher.chmod(0o644)
    real_which = shutil.which

    def find_executable(
        name: str,
        mode: int = os.F_OK | os.X_OK,
        path: str | None = None,
    ) -> str | None:
        if name == "uv":
            assert path is None
            return str(uv)
        assert Path(name) == launcher
        return real_which(name, mode=mode, path=path)

    def uv_identity(_path: str | None) -> tuple[bool, str | None, str | None]:
        return True, "0.11.28", hashlib.sha256(uv.read_bytes()).hexdigest()

    def uv_tool_directory(_path: Path | None, *, bin_directory: bool) -> Path:
        return tool_bin if bin_directory else tool_directory

    def pyvenv_version(_prefix: Path) -> str:
        return "0.11.28"

    def record_identity(_distribution: metadata.Distribution) -> dict[str, object]:
        return {
            "verified": True,
            "console_script_sha256": [launcher_digest],
            "record_sha256": "a" * 64,
        }

    monkeypatch.delenv("UV", raising=False)
    if launcher_case == "invalid-override":
        monkeypatch.setenv(
            "CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE",
            str(stale_launcher),
        )
    else:
        monkeypatch.delenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", raising=False)
    monkeypatch.chdir(stale_launcher.parent)
    monkeypatch.setattr(
        validation_report_module.shutil,
        "which",
        find_executable,
    )
    monkeypatch.setattr(validation_report_module.sys, "prefix", str(environment))
    monkeypatch.setattr(validation_report_module.sys, "base_prefix", str(base_prefix))
    monkeypatch.setattr(validation_report_module.sys, "executable", str(interpreter))
    monkeypatch.setattr(
        validation_report_module,
        "_uv_executable_identity",
        uv_identity,
    )
    monkeypatch.setattr(
        validation_report_module,
        "_uv_tool_dir",
        uv_tool_directory,
    )
    monkeypatch.setattr(
        validation_report_module,
        "_pyvenv_uv_version",
        pyvenv_version,
    )
    monkeypatch.setattr(
        validation_report_module,
        "_installed_record_identity",
        record_identity,
    )

    verified, receipt = validation_report_module._detect_persistent_uv_tool_receipt(  # pyright: ignore[reportPrivateUsage]
        detected_kind=InstallSourceKind.WHEEL,
        package_path=str(package),
        distribution=cast(metadata.Distribution, Distribution()),
    )

    assert verified is expected_verified
    assert receipt["uv_executable"] == str(uv)
    assert receipt["verified"] is expected_verified
    assert receipt["uv_tool_receipt"]["verified"] is expected_verified
    if launcher_case == "scoped":
        assert receipt["tool_executable"] == str(launcher)
        assert receipt["tool_bin_bound"] is True
    elif launcher_case in {"missing", "non-executable"}:
        assert receipt["tool_executable"] is None
        assert receipt["tool_bin_bound"] is False
    else:
        assert receipt["tool_executable"] == str(stale_launcher)
        assert receipt["tool_bin_bound"] is False


def test_uv_launcher_identity_hashes_exact_regular_executable(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    executable = tmp_path / "uv.exe"
    executable.write_bytes(b"trusted uv executable")

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([str(executable), "--version"], 0, "uv 0.11.28\n", "")

    monkeypatch.setattr(validation_report_module.subprocess, "run", fake_run)

    verified, version, digest = _uv_executable_identity(str(executable))

    assert verified is True
    assert version == "0.11.28"
    assert digest == hashlib.sha256(executable.read_bytes()).hexdigest()


def test_uv_launcher_identity_rejects_executable_replaced_during_probe(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    executable = tmp_path / "uv.exe"
    executable.write_bytes(b"trusted uv executable")

    def replacing_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        executable.write_bytes(b"substituted executable")
        return subprocess.CompletedProcess([str(executable), "--version"], 0, "uv 0.11.28\n", "")

    monkeypatch.setattr(validation_report_module.subprocess, "run", replacing_run)

    assert _uv_executable_identity(str(executable)) == (
        False,
        None,
        None,
    )


@pytest.fixture(scope="module")
def real_uv_relay_wheel(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str, Path]:
    """Build one real wheel for persistent-tool and uvx receipt probes."""
    uv = shutil.which("uv")
    uvx = shutil.which("uvx")
    assert uv is not None and uvx is not None, "the production test suite requires uv and uvx"
    uv_version = subprocess.run(
        [uv, "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    uvx_version = subprocess.run(
        [uvx, "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    assert uv_version[:2] == ["uv", "0.11.28"]
    assert uvx_version[:2] == ["uvx", "0.11.28"]
    tmp_path = tmp_path_factory.mktemp("real-uvx-receipt")
    artifact_dir = tmp_path / "dist"
    project_root = Path(__file__).parents[1]
    subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(artifact_dir), str(project_root)],
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(artifact_dir.glob("clio_relay-*.whl"))
    assert len(wheels) == 1
    return uv, uvx, wheels[0].resolve()


def test_real_uv_tool_scopes_launcher_to_isolated_bin(
    tmp_path: Path,
    real_uv_relay_wheel: tuple[str, str, Path],
) -> None:
    uv, _uvx, wheel = real_uv_relay_wheel
    tool_directory = tmp_path / "tools"
    tool_bin_directory = tmp_path / "tool-bin"
    isolated_home = tmp_path / "home"
    uv_cache = tmp_path / "uv-cache"
    collision_directory = tmp_path / "cwd-collision"
    for directory in (
        tool_directory,
        tool_bin_directory,
        isolated_home,
        uv_cache,
        collision_directory,
    ):
        directory.mkdir(parents=True)
    collision_executable = collision_directory / (
        "clio-relay.exe" if os.name == "nt" else "clio-relay"
    )
    collision_executable.write_bytes(b"unrelated current-directory launcher")
    environment = os.environ.copy()
    environment.update(
        {
            "HOME": str(isolated_home),
            "USERPROFILE": str(isolated_home),
            "UV": uv,
            "UV_CACHE_DIR": str(uv_cache),
            "UV_TOOL_BIN_DIR": str(tool_bin_directory),
            "UV_TOOL_DIR": str(tool_directory),
        }
    )
    environment.pop("CLIO_RELAY_INSTALL_RECEIPT", None)
    environment.pop("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", None)
    environment["PATH"] = os.pathsep.join(
        entry
        for entry in environment.get("PATH", "").split(os.pathsep)
        if entry and Path(entry).resolve() != tool_bin_directory.resolve()
    )
    subprocess.run(
        [
            uv,
            "tool",
            "install",
            "--force",
            "--no-config",
            "--python",
            f"{sys.version_info.major}.{sys.version_info.minor}",
            str(wheel),
        ],
        capture_output=True,
        check=True,
        env=environment,
        text=True,
        timeout=180,
    )
    executable = tool_bin_directory / ("clio-relay.exe" if os.name == "nt" else "clio-relay")

    completed = subprocess.run(
        [str(executable), "installation-info"],
        capture_output=True,
        check=False,
        cwd=collision_directory,
        env=environment,
        text=True,
        timeout=60,
    )

    assert completed.returncode == 0, completed.stderr
    installation = json.loads(completed.stdout)
    source = installation["install_source"]
    assert installation["receipt_origin"] == "uv-tool"
    assert installation["receipt_matches_install"] is True
    assert source["artifact_identity_verified"] is True
    assert source["launcher_verified"] is True
    assert Path(source["launcher_receipt"]["tool_executable"]).resolve() == executable.resolve()
    assert source["launcher_receipt"]["tool_bin_bound"] is True
    assert source["launcher_receipt"]["uv_tool_receipt"]["launcher_bound"] is True


def test_real_uvx_0_11_28_produces_verified_os_bound_launcher_receipt(
    tmp_path: Path,
    real_uv_relay_wheel: tuple[str, str, Path],
) -> None:
    _uv, uvx, wheel = real_uv_relay_wheel
    report = tmp_path / "uvx-receipt.json"
    environment = os.environ.copy()
    environment["CLIO_RELAY_VALIDATION_INVOCATION_ID"] = "real-uvx-receipt-test"
    completed = subprocess.run(
        [
            uvx,
            "--refresh",
            "--no-config",
            "--python",
            f"{sys.version_info.major}.{sys.version_info.minor}",
            "--from",
            str(wheel),
            "clio-relay",
            "live-test",
            "--cluster",
            "__missing_real_uvx_receipt_cluster__",
            "--validation-launcher",
            "uvx",
            "--validation-install-source",
            f"wheel:{wheel}",
            "--validation-artifact",
            str(wheel),
            "--report",
            str(report),
        ],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=180,
    )

    assert completed.returncode != 0
    assert report.is_file(), completed.stderr
    source = json.loads(report.read_text(encoding="utf-8"))["install_source"]
    receipt = source["launcher_receipt"]
    assert receipt["schema_version"] == "clio-relay.launcher-receipt.v2"
    assert receipt["verified"] is True
    assert receipt["uv_executable_verified"] is True
    assert receipt["uv_executable_stable"] is True
    assert receipt["uv_version"] == "0.11.28"
    assert (
        receipt["uv_executable_sha256"]
        == hashlib.sha256(Path(receipt["uv_executable"]).read_bytes()).hexdigest()
    )
    assert receipt["uv_cache_contains_environment"] is True
    assert Path(receipt["process_prefix"]).is_relative_to(Path(receipt["uv_cache_directory"]))
    assert receipt["pyvenv_matches_uv"] is True
    assert receipt["package_in_process_environment"] is True
    assert receipt["executable_in_process_environment"] is True
    assert receipt["executable_target_bound"] is True
    assert receipt["uv_process_ancestor_verified"] is True
    assert receipt["uv_process_ancestor"]["depth"] <= (
        validation_report_module.MAX_LAUNCHER_PROCESS_ANCESTORS
    )
    assert receipt["invocation_id"] == "real-uvx-receipt-test"
    assert "uv_run_recursion_depth" not in receipt
    assert "virtual_environment" not in receipt


def test_real_uvx_no_cache_temporary_environment_is_not_a_verified_receipt(
    tmp_path: Path,
    real_uv_relay_wheel: tuple[str, str, Path],
) -> None:
    _uv, uvx, wheel = real_uv_relay_wheel
    report = tmp_path / "uvx-no-cache-receipt.json"
    environment = os.environ.copy()
    environment["CLIO_RELAY_VALIDATION_INVOCATION_ID"] = "real-uvx-no-cache-test"
    completed = subprocess.run(
        [
            uvx,
            "--refresh",
            "--no-cache",
            "--no-config",
            "--python",
            f"{sys.version_info.major}.{sys.version_info.minor}",
            "--from",
            str(wheel),
            "clio-relay",
            "live-test",
            "--cluster",
            "__missing_real_uvx_no_cache_cluster__",
            "--validation-launcher",
            "uvx",
            "--validation-install-source",
            f"wheel:{wheel}",
            "--validation-artifact",
            str(wheel),
            "--report",
            str(report),
        ],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=180,
    )

    assert completed.returncode != 0
    assert report.is_file(), completed.stderr
    source = json.loads(report.read_text(encoding="utf-8"))["install_source"]
    receipt = source["launcher_receipt"]
    assert receipt["schema_version"] == "clio-relay.launcher-receipt.v2"
    assert receipt["verified"] is False
    assert receipt["uv_executable_verified"] is True
    assert receipt["uv_cache_contains_environment"] is False
    assert not Path(receipt["process_prefix"]).is_relative_to(Path(receipt["uv_cache_directory"]))
    assert source["launcher_verified"] is False
    assert source["released_artifact"] is False
    assert receipt["invocation_id"] == "real-uvx-no-cache-test"


def _report(
    *,
    kind: InstallSourceKind = InstallSourceKind.PYPI,
    launcher: str = "uv-tool",
    released_artifact: bool = True,
    artifact_identity_verified: bool = True,
    version: str = "1.0.0",
) -> LiveValidationReport:
    now = _timestamp()
    absolute_root = Path(Path.cwd().anchor) / "clio-relay-test"
    return LiveValidationReport(
        report_id="validation_test",
        scenario="remote-mcp",
        cluster="primary",
        started_at=now,
        completed_at=now,
        status=ValidationStatus.PASSED,
        evidence_trust=EvidenceTrust(
            producer_github_login="release-operator",
            producer_github_id=123456,
            invocation_id="run-20260710-0001",
        ),
        software=SoftwareIdentity(
            version=version,
            commit="a" * 40,
            tag=f"v{version}",
            dirty=False,
        ),
        install_source=InstallSource(
            kind=kind,
            detected_kind=kind,
            reference=f"clio-relay=={version}",
            launcher=launcher,
            package_path="/tmp/uv/archive/clio_relay",
            distribution_version=version,
            artifact_sha256="b" * 64,
            artifact_identity_verified=artifact_identity_verified,
            released_artifact=released_artifact,
            launcher_verified=launcher == "uv-tool",
            launcher_receipt={
                "verified": launcher == "uv-tool",
                "claimed_launcher": launcher,
                "uv_executable_verified": launcher == "uv-tool",
                "uv_version": "0.11.28",
                "uv_executable_sha256": "e" * 64,
                "invocation_id": "run-20260710-0001",
                "uv_tool_directory": str(absolute_root / "uv" / "tools"),
                "uv_tool_bin_directory": str(absolute_root / "uv" / "bin"),
                "process_prefix": str(absolute_root / "uv" / "tools" / "clio-relay"),
                "tool_environment_verified": True,
                "tool_bin_bound": True,
                "tool_target_bound": True,
                "pyvenv_matches_uv": True,
                "package_in_process_environment": True,
                "executable_in_process_environment": True,
                "isolated_environment": True,
                "distribution_record": {
                    "verified": True,
                    "record_sha256": "c" * 64,
                    "runtime_closure_sha256": "d" * 64,
                },
            },
        ),
        checks=[
            ValidationCheck(
                check_id="remote-mcp.call",
                summary="call a virtualized remote MCP tool",
                status=ValidationStatus.PASSED,
                started_at=now,
                completed_at=now,
                evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
            )
        ],
        resources=[
            ValidationResource(
                kind="relay_job",
                resource_id="job_123",
                cluster="primary",
                state="succeeded",
            )
        ],
    )


def _policy() -> ReleaseGatePolicy:
    return ReleaseGatePolicy(
        release_version="1.0.0",
        required_uv_version="0.11.28",
        requirements=[
            ReleaseGateRequirement(
                requirement_id="remote-mcp-primary",
                description="non-JARVIS virtual MCP on the primary cluster",
                cluster="primary",
                scenarios=["remote-mcp"],
                required_checks=["remote-mcp.call"],
                required_resource_kinds=["relay_job"],
            )
        ],
    )


def _without_acceptance_matrix(policy: ReleaseGatePolicy) -> ReleaseGatePolicy:
    """Clone a production policy for isolated requirement unit tests."""
    document = policy.model_dump(mode="python")
    document["acceptance_matrix_path"] = None
    document["acceptance_matrix_sha256"] = None
    return ReleaseGatePolicy.model_validate(document)


def _evaluate(
    policy: ReleaseGatePolicy,
    reports: list[LiveValidationReport],
) -> ReleaseGateResult:
    return evaluate_release_gate(
        policy,
        reports,
        expected_artifact_sha256="b" * 64,
    )


def _target_policy(
    *,
    hostnames: list[str] | None = None,
    fingerprints: list[str] | None = None,
    site_marker_sha256: str = "d" * 64,
    scheduler_provider: str = "slurm",
    scheduler_cluster_name: str | None = "primary-scheduler",
) -> ReleaseTargetIdentity:
    normalized_hostnames = sorted(
        item.strip().rstrip(".").lower() for item in (hostnames or ["login.primary.example"])
    )
    normalized_fingerprints = sorted(fingerprints or ["SHA256:primary-host-key"])
    canonical = {
        "schema_version": "clio-relay.cluster-target-identity.v1",
        "observed_hostnames": normalized_hostnames,
        "observed_ssh_host_key_sha256": normalized_fingerprints,
        "scheduler_cluster_name": scheduler_cluster_name,
        "site_marker_sha256": site_marker_sha256,
        "scheduler_provider": scheduler_provider,
    }
    digest = hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return ReleaseTargetIdentity(
        hostnames=normalized_hostnames,
        ssh_host_key_sha256=normalized_fingerprints,
        scheduler_provider=scheduler_provider,
        scheduler_cluster_name=scheduler_cluster_name,
        site_marker_sha256=site_marker_sha256,
        identity_sha256=digest,
    )


def _attach_verified_target_identity(
    report: LiveValidationReport,
    *,
    hostname: str = "login.primary.example",
    fingerprint: str = "SHA256:primary-host-key",
    expected_hostnames: list[str] | None = None,
    expected_fingerprints: list[str] | None = None,
    site_marker_sha256: str = "d" * 64,
    expected_site_marker_sha256: str | None = "d" * 64,
) -> None:
    now = _timestamp()
    report.checks.append(
        ValidationCheck(
            check_id="worker.target-identity",
            summary="worker physical target identity",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="target identity verified")],
        )
    )
    report.resources.append(
        ValidationResource(
            kind="cluster_target",
            resource_id=f"target:{report.cluster}",
            role="physical_cluster_target",
            cluster=report.cluster,
            state="verified",
            provider="slurm",
            metadata={
                "schema_version": "clio-relay.cluster-target-info.v1",
                "hostname": hostname,
                "fqdn": hostname,
                "scheduler_provider": "slurm",
                "scheduler_cluster_name": "primary-scheduler",
                "site_marker_sha256": site_marker_sha256,
                "ssh_host_key_sha256": [fingerprint],
                "expected_hostnames": (
                    expected_hostnames if expected_hostnames is not None else [hostname]
                ),
                "expected_ssh_host_key_sha256": (
                    expected_fingerprints if expected_fingerprints is not None else [fingerprint]
                ),
                "expected_scheduler_cluster_name": "primary-scheduler",
                "expected_site_marker_sha256": expected_site_marker_sha256,
                "verified": True,
            },
        )
    )


def test_report_json_round_trip_is_stable(tmp_path: Path) -> None:
    path = tmp_path / "report.json"

    write_validation_report(_report(), path)
    loaded = load_validation_report(path)

    assert loaded.report_id == "validation_test"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "1.0"
    assert payload["evidence_trust"]["producer_github_id"] == 123456
    assert payload["evidence_trust"]["invocation_id"] == "run-20260710-0001"
    assert path.read_text(encoding="utf-8").endswith("\n")
    assert not list(tmp_path.glob("*.tmp"))


def test_report_writer_redacts_nested_capability_values(tmp_path: Path) -> None:
    report = _report()
    owner_token = "owned-connector-capability-123"
    report.resources[0].metadata = {
        "owner_token": owner_token,
        "command": f"connector-wrapper {owner_token} --serve",
        "token_env": "CLIO_RELAY_FRP_TOKEN",
        "nested": {"API-TOKEN": "remote-api-capability-456"},
    }
    report.checks[0].evidence[0].metadata = {
        "diagnostic": f"process environment contained {owner_token}"
    }
    path = tmp_path / "redacted-report.json"

    write_validation_report(report, path)

    rendered = path.read_text(encoding="utf-8")
    payload = json.loads(rendered)
    metadata = payload["resources"][0]["metadata"]
    assert owner_token not in rendered
    assert "remote-api-capability-456" not in rendered
    assert metadata["owner_token"] == "<redacted>"
    assert metadata["command"] == "connector-wrapper <redacted> --serve"
    assert metadata["token_env"] == "CLIO_RELAY_FRP_TOKEN"
    assert metadata["nested"]["API-TOKEN"] == "<redacted>"


def test_report_rejects_unknown_schema_or_unevidenced_success() -> None:
    payload = _report().model_dump(mode="python")
    payload["schema_version"] = "2.0"
    with pytest.raises(ValidationError, match="schema_version"):
        LiveValidationReport.model_validate(payload)

    payload = _report().model_dump(mode="python")
    payload["checks"][0]["evidence"] = []
    with pytest.raises(ValidationError, match="require evidence"):
        LiveValidationReport.model_validate(payload)


def test_report_cannot_self_claim_independent_producer_execution_proof() -> None:
    report = _report()
    payload = report.model_dump(mode="python")
    payload["evidence_trust"]["producer_execution_verified"] = True

    with pytest.raises(ValidationError, match="producer_execution_verified"):
        LiveValidationReport.model_validate(payload)

    local = new_live_validation_report(scenario="local-release", cluster="local")
    remote = new_live_validation_report(scenario="live-test", cluster="site-a")
    assert local.evidence_trust.origin is EvidenceOrigin.LOCAL_PROCESS
    assert remote.evidence_trust.origin is EvidenceOrigin.OPERATOR_GENERATED
    assert remote.evidence_trust.producer_execution_verified is False


def test_new_report_records_only_explicit_complete_producer_identity(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN", "release-operator")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID", "123456")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_INVOCATION_ID", "run-20260710-0001")

    report = new_live_validation_report(scenario="live-test", cluster="site-a")

    assert report.evidence_trust.producer_github_login == "release-operator"
    assert report.evidence_trust.producer_github_id == 123456
    assert report.evidence_trust.invocation_id == "run-20260710-0001"


def test_new_report_allows_partial_but_rejects_invalid_producer_identity(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN", "release-operator")
    partial = new_live_validation_report(scenario="live-test", cluster="site-a")
    assert partial.evidence_trust.producer_github_login == "release-operator"
    assert partial.evidence_trust.producer_github_id is None

    monkeypatch.setenv("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID", "not-numeric")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_INVOCATION_ID", "run-20260710-0001")
    with pytest.raises(ConfigurationError, match="positive integer"):
        new_live_validation_report(scenario="live-test", cluster="site-a")


def test_release_gate_rejects_missing_or_mismatched_producer_invocation() -> None:
    missing = _report()
    missing.evidence_trust = EvidenceTrust()
    missing_result = _evaluate(_policy(), [missing])

    mismatched = _report()
    mismatched.install_source.launcher_receipt["invocation_id"] = "run-20260710-other"
    mismatched_result = _evaluate(_policy(), [mismatched])

    assert any(
        "omits authenticated producer" in reason
        for reason in missing_result.unsatisfied_requirements["remote-mcp-primary"]
    )
    assert any(
        "invocation id does not match" in reason
        for reason in mismatched_result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_rejects_unpinned_launcher_binary_identity() -> None:
    report = _report()
    report.install_source.launcher_receipt["uv_version"] = "0.11.27"
    report.install_source.launcher_receipt["uv_executable_sha256"] = "E" * 64

    result = _evaluate(_policy(), [report])

    reasons = result.unsatisfied_requirements["remote-mcp-primary"]
    assert "launcher receipt uv version must be 0.11.28, got 0.11.27" in reasons
    assert "launcher receipt omits a lowercase uv executable SHA-256" in reasons


def test_recorder_converts_lines_to_resources_and_artifacts() -> None:
    report = _report()
    report.checks.clear()
    report.resources.clear()
    recorder = ValidationRecorder(report)

    recorder.observe_line("acceptance.job_id=job_abc")
    recorder.observe_line("acceptance.job_state=succeeded")
    recorder.observe_line("acceptance.stdout_bytes=42")
    recorder.observe_line("acceptance.artifacts=stdout,provenance")
    recorder.observe_line("scheduler.pending=observed")
    recorder.observe_line("cluster.bootstrap=verified")
    recorder.observe_line("worker.running=passed")
    recorder.observe_line("worker.service-enabled=verified")
    recorder.observe_line("worker.service-persistence=verified")
    recorder.observe_line("worker.artifact-version=1.0.0")
    recorder.observe_line('worker.components={"clio-kit":"2.2.6"}')
    recorder.observe_line("worker.execute=passed")
    recorder.observe_line("live acceptance passed")
    recorder.finish()

    job = next(resource for resource in report.resources if resource.kind == "relay_job")
    assert job.resource_id == "job_abc"
    assert job.state == "succeeded"
    assert {item.kind for item in report.artifacts} == {"log", "stdout", "provenance"}
    assert any(check.check_id == "scheduler.pending" for check in report.checks)
    assert {
        "cluster.bootstrap",
        "worker.running",
        "worker.service-enabled",
        "worker.service-persistence",
        "worker.artifact-version",
        "worker.execute",
    }.issubset({check.check_id for check in report.checks})
    assert report.status is ValidationStatus.PASSED
    worker = next(resource for resource in report.resources if resource.kind == "relay_worker")
    assert worker.metadata["components"] == {"clio-kit": "2.2.6"}


def test_recorder_does_not_turn_failure_or_informational_lines_into_passes() -> None:
    report = _report()
    report.checks.clear()
    recorder = ValidationRecorder(report)

    recorder.observe_line("transport.remote_cleanup=not_started")
    recorder.observe_line("transport.cleanup=detached")
    recorder.observe_line("direct_transport.result=frp_stcp")
    recorder.observe_line("scheduler.pending=unknown")
    recorder.observe_line("worker.artifact-sha256=none")
    recorder.observe_line("transport.server=relay.example:7000")
    recorder.observe_line("acceptance.unrecognized=looks-good")

    check_ids = {check.check_id for check in report.checks}
    assert "transport.remote_cleanup" not in check_ids
    assert "transport.cleanup" not in check_ids
    assert "direct_transport.result" not in check_ids
    assert "transport.direct" not in check_ids
    assert "scheduler.pending" not in check_ids
    assert "worker.artifact-sha256" not in check_ids
    assert "transport.server" not in check_ids
    assert "acceptance.unrecognized" not in check_ids


def test_recorder_preserves_exact_structured_transport_cleanup_resources() -> None:
    report = _report()
    report.checks.clear()
    report.resources.clear()
    report.cleanup.actions.clear()
    recorder = ValidationRecorder(report)
    recorder.observe_line(
        transport_probe_evidence_line(
            TransportProbeEvidence(
                probe_id="ssh-probe:session-a:generation-a",
                cluster=report.cluster,
                cleanup_mode="transport_probe_teardown",
                resources=[
                    TransportCleanupResourceEvidence(
                        kind="relay_session",
                        resource_id="session-a:generation-a",
                        role="remote_transport_session",
                        location="cluster-host",
                        action="stop",
                        ownership_verified=True,
                        outcome="stopped",
                        verified_after_operation=True,
                        observed_state="stopped",
                        residual=False,
                        detail="owned generation stopped",
                        metadata={"session_generation_id": "generation-a"},
                    ),
                    TransportCleanupResourceEvidence(
                        kind="connector",
                        resource_id="connector-4271",
                        role="remote_frpc_connector",
                        location="cluster-host",
                        action="stop",
                        ownership_verified=True,
                        outcome="stopped",
                        verified_after_operation=True,
                        observed_state="stopped",
                        residual=False,
                        detail=None,
                    ),
                    TransportCleanupResourceEvidence(
                        kind="gateway_session",
                        resource_id="gateway-91",
                        role="gateway_record:close",
                        location="cluster-host",
                        action="close",
                        ownership_verified=True,
                        outcome="closed",
                        verified_after_operation=True,
                        observed_state="closed",
                        residual=False,
                        detail=None,
                    ),
                ],
            )
        )
    )

    assert recorder.transport_probe_count == 1
    assert {(item.kind, item.resource_id) for item in report.resources} == {
        ("relay_session", "session-a:generation-a"),
        ("connector", "connector-4271"),
        ("gateway_session", "gateway-91"),
    }
    gateway = next(item for item in report.resources if item.kind == "gateway_session")
    assert gateway.metadata["ownership_verified"] is True
    assert gateway.metadata["observed_state"] == "closed"
    assert gateway.metadata["detail"] is None
    assert report.cleanup.remaining_resources == []
    assert all(
        action["probe_id"] == "ssh-probe:session-a:generation-a"
        for action in report.cleanup.actions
    )
    assert not any(action.get("kind") == "transport_probe" for action in report.cleanup.actions)


def test_recorder_preserves_partial_transport_cleanup_as_remaining_resource() -> None:
    report = _report()
    report.checks.clear()
    report.resources.clear()
    report.cleanup.actions.clear()
    recorder = ValidationRecorder(report)
    recorder.observe_line(
        transport_probe_evidence_line(
            TransportProbeEvidence(
                probe_id="frp-probe-partial",
                cluster=report.cluster,
                cleanup_mode="transport_probe_teardown",
                resources=[
                    TransportCleanupResourceEvidence(
                        kind="connector",
                        resource_id="remote-frpc-pid-811",
                        role="remote_frpc_connector",
                        location="cluster-host",
                        action="stop",
                        ownership_verified=True,
                        outcome="failed",
                        verified_after_operation=False,
                        observed_state="running",
                        residual=True,
                        detail="process remained after TERM and KILL",
                        metadata={"pid": 811},
                    )
                ],
            )
        )
    )

    remaining = report.cleanup.remaining_resources
    assert [(item.kind, item.resource_id) for item in remaining] == [
        ("connector", "remote-frpc-pid-811")
    ]
    assert remaining[0].metadata["ownership_verified"] is True
    assert remaining[0].metadata["observed_state"] == "running"
    assert remaining[0].metadata["residual"] is True
    assert remaining[0].metadata["detail"] == "process remained after TERM and KILL"
    assert report.cleanup.actions[0]["outcome"] == "failed"


@pytest.mark.parametrize(
    "payload",
    [
        '{"schema_version":"clio-relay.transport-probe-evidence.v1","extra":true}',
        '{"schema_version":"clio-relay.transport-probe-evidence.v1","probe_id":"p",'
        '"cluster":"test-cluster","cleanup_mode":"teardown","resources":[],"n":NaN}',
        "x" * (256 * 1024 + 1),
    ],
    ids=["extra-field", "non-finite", "oversized"],
)
def test_recorder_rejects_unbounded_or_non_strict_transport_evidence(payload: str) -> None:
    recorder = ValidationRecorder(_report())

    with pytest.raises(ConfigurationError, match="transport probe evidence"):
        recorder.observe_line(f"transport.probe_evidence={payload}")


def test_recorder_records_failed_check_and_report() -> None:
    report = _report()
    report.checks.clear()
    recorder = ValidationRecorder(report)
    error = RuntimeError("live failure")

    recorder.record_failure("transport.cleanup", "clean owned connector", error)
    recorder.finish(error)

    assert report.status is ValidationStatus.FAILED
    assert report.error == "RuntimeError: live failure"
    assert report.checks[0].status is ValidationStatus.FAILED


def test_release_gate_accepts_only_complete_released_uv_tool_evidence() -> None:
    result = _evaluate(_policy(), [_report()])

    assert result.passed is True
    assert result.satisfied_requirements == ["remote-mcp-primary"]
    assert result.report_ids == ["validation_test"]


def test_release_gate_requires_verified_identity_for_every_nonlocal_report() -> None:
    policy = _policy()
    policy.require_target_identity = True

    result = _evaluate(policy, [_report()])

    assert result.passed is False
    assert any(
        "must identify exactly one cluster_target resource" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_rejects_same_cluster_label_for_different_physical_targets() -> None:
    policy = _policy()
    policy.require_target_identity = True
    first = _report()
    _attach_verified_target_identity(first)
    second = _report()
    second.report_id = "validation_same_label_different_target"
    _attach_verified_target_identity(
        second,
        hostname="other-login.example",
        fingerprint="SHA256:other-host-key",
    )

    policy.targets = {"primary": _target_policy()}
    result = _evaluate(policy, [first, second])

    assert result.passed is False
    assert any(
        "reports identify different physical target identities" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_does_not_confuse_broad_pins_with_observed_identity() -> None:
    policy = _policy()
    policy.require_target_identity = True
    expected_hostnames = ["login-a.example", "login-b.example"]
    expected_fingerprints = ["SHA256:host-a", "SHA256:host-b"]
    first = _report()
    _attach_verified_target_identity(
        first,
        hostname="login-a.example",
        fingerprint="SHA256:host-a",
        expected_hostnames=expected_hostnames,
        expected_fingerprints=expected_fingerprints,
        site_marker_sha256="a" * 64,
        expected_site_marker_sha256=None,
    )
    second = _report()
    second.report_id = "validation_broad_pins_other_target"
    _attach_verified_target_identity(
        second,
        hostname="login-b.example",
        fingerprint="SHA256:host-b",
        expected_hostnames=expected_hostnames,
        expected_fingerprints=expected_fingerprints,
        site_marker_sha256="b" * 64,
        expected_site_marker_sha256=None,
    )

    policy.targets = {
        "primary": _target_policy(
            hostnames=["login-a.example"],
            fingerprints=["SHA256:host-a"],
            site_marker_sha256="a" * 64,
        )
    }
    result = _evaluate(policy, [first, second])

    assert result.passed is False
    assert any(
        "reports identify different physical target identities" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_records_one_stable_target_digest_per_cluster() -> None:
    policy = _policy()
    policy.require_target_identity = True
    first = _report()
    _attach_verified_target_identity(first)
    second = _report()
    second.report_id = "validation_same_physical_target"
    _attach_verified_target_identity(second)

    policy.targets = {"primary": _target_policy()}
    result = _evaluate(policy, [first, second])

    assert result.passed is True
    assert set(result.target_identity_sha256) == {"primary"}
    assert len(result.target_identity_sha256["primary"]) == 64
    assert result.policy_target_identity_sha256 == result.target_identity_sha256


def test_target_digest_normalizes_hostname_and_host_key_set_order() -> None:
    policy = _policy()
    policy.require_target_identity = True
    first = _report()
    _attach_verified_target_identity(first)
    first_target = next(
        resource for resource in first.resources if resource.kind == "cluster_target"
    )
    first_target.metadata.update(
        {
            "hostname": "LOGIN.PRIMARY.EXAMPLE.",
            "fqdn": "worker.primary.example",
            "ssh_host_key_sha256": ["SHA256:key-b", "SHA256:primary-host-key"],
            "expected_hostnames": ["login.primary.example", "worker.primary.example"],
            "expected_ssh_host_key_sha256": [
                "SHA256:primary-host-key",
                "SHA256:key-b",
            ],
        }
    )
    second = _report()
    second.report_id = "validation_same_target_reordered_observations"
    _attach_verified_target_identity(second)
    second_target = next(
        resource for resource in second.resources if resource.kind == "cluster_target"
    )
    second_target.metadata.update(
        {
            "hostname": "worker.primary.example",
            "fqdn": "login.primary.example",
            "ssh_host_key_sha256": ["SHA256:primary-host-key", "SHA256:key-b"],
            "expected_hostnames": ["worker.primary.example", "login.primary.example"],
            "expected_ssh_host_key_sha256": [
                "SHA256:key-b",
                "SHA256:primary-host-key",
            ],
        }
    )

    policy.targets = {
        "primary": _target_policy(
            hostnames=["login.primary.example", "worker.primary.example"],
            fingerprints=["SHA256:primary-host-key", "SHA256:key-b"],
        )
    }
    result = _evaluate(policy, [first, second])

    assert result.passed is True
    assert set(result.target_identity_sha256) == {"primary"}


def test_release_gate_requires_exact_policy_target_coverage() -> None:
    policy = _policy()
    policy.require_target_identity = True
    policy.targets = {
        "primary": _target_policy(),
        "unreported-site": _target_policy(hostnames=["unreported.example"]),
    }
    report = _report()
    _attach_verified_target_identity(report)

    result = _evaluate(policy, [report])

    assert (
        "policy targets lack report coverage: ['unreported-site']"
        in (result.unsatisfied_requirements["target-identity"])
    )


def test_release_gate_ignores_self_reported_expected_target_fields() -> None:
    policy = _policy()
    policy.require_target_identity = True
    policy.targets = {"primary": _target_policy()}
    report = _report()
    _attach_verified_target_identity(
        report,
        hostname="attacker.example",
        fingerprint="SHA256:attacker-key",
        expected_hostnames=["attacker.example"],
        expected_fingerprints=["SHA256:attacker-key"],
    )

    result = _evaluate(policy, [report])

    assert result.passed is False
    assert any(
        "does not match policy-pinned fields" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_rejects_policy_digest_not_bound_to_pinned_fields() -> None:
    policy = _policy()
    policy.require_target_identity = True
    target = _target_policy()
    target.identity_sha256 = "e" * 64
    policy.targets = {"primary": target}
    report = _report()
    _attach_verified_target_identity(report)

    result = _evaluate(policy, [report])

    assert any(
        "identity_sha256 does not match its pinned fields" in reason
        for reason in result.unsatisfied_requirements["target-identity"]
    )


def test_release_gate_fails_closed_for_declared_external_blockers() -> None:
    policy = _policy()
    policy.release_blockers = ["JARVIS-CD structured scheduler submission has no released artifact"]

    result = _evaluate(policy, [_report()])

    assert result.passed is False
    assert result.satisfied_requirements == ["remote-mcp-primary"]
    assert result.unsatisfied_requirements["declared-release-blockers"] == (policy.release_blockers)


def test_release_gate_rejects_checkout_or_local_wheel_claim() -> None:
    report = _report(
        kind=InstallSourceKind.WHEEL,
        launcher="uv-tool",
        released_artifact=False,
    )

    result = _evaluate(_policy(), [report])

    assert result.passed is False
    reasons = result.unsatisfied_requirements["remote-mcp-primary"]
    assert "install source wheel is not release-approved" in reasons
    assert "report does not prove a released artifact" in reasons


def test_release_gate_rejects_dirty_or_untagged_build() -> None:
    report = _report()
    report.software.dirty = True
    report.software.tag = None

    result = _evaluate(_policy(), [report])

    reasons = result.unsatisfied_requirements["remote-mcp-primary"]
    assert "report does not prove a clean build" in reasons
    assert "report source tag must be v1.0.0, got None" in reasons


def test_release_gate_binds_candidate_wheel_reports_to_independent_digest() -> None:
    policy = _policy()
    policy.artifact_stage = "immutable_candidate"
    policy.require_released_artifact = False
    policy.allowed_install_sources = [InstallSourceKind.WHEEL]
    report = _report(
        kind=InstallSourceKind.WHEEL,
        launcher="uv-tool",
        released_artifact=False,
    )

    accepted = evaluate_release_gate(
        policy,
        [report],
        expected_artifact_sha256="B" * 64,
    )
    rejected = evaluate_release_gate(
        policy,
        [report],
        expected_artifact_sha256="c" * 64,
    )

    assert accepted.passed is True
    assert accepted.artifact_sha256 == "b" * 64
    assert rejected.passed is False
    assert (
        "tested artifact SHA-256 does not match the immutable candidate: " + "b" * 64
        in rejected.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_candidate_gate_rejects_unbound_running_distribution() -> None:
    policy = _policy()
    policy.artifact_stage = "immutable_candidate"
    policy.require_released_artifact = False
    policy.allowed_install_sources = [InstallSourceKind.WHEEL]
    report = _report(
        kind=InstallSourceKind.WHEEL,
        released_artifact=False,
        artifact_identity_verified=False,
    )

    result = evaluate_release_gate(
        policy,
        [report],
        expected_artifact_sha256="b" * 64,
    )

    assert result.passed is False
    assert (
        "running distribution is not bound to the expected wheel bytes"
        in (result.unsatisfied_requirements["remote-mcp-primary"])
    )


def test_release_gate_rejects_invalid_expected_artifact_digest() -> None:
    with pytest.raises(ConfigurationError, match="64 hexadecimal"):
        evaluate_release_gate(_policy(), [_report()], expected_artifact_sha256="not-a-digest")


def test_candidate_gate_requires_independently_computed_digest() -> None:
    policy = _policy()
    policy.artifact_stage = "immutable_candidate"
    policy.require_released_artifact = False
    policy.allowed_install_sources = [InstallSourceKind.WHEEL]

    with pytest.raises(ConfigurationError, match="independently computed"):
        evaluate_release_gate(policy, [])


def test_published_gate_requires_independently_computed_digest() -> None:
    with pytest.raises(ConfigurationError, match="published gates.*independently computed"):
        evaluate_release_gate(_policy(), [_report()])


def test_release_gate_enforces_resource_state_role_and_metadata() -> None:
    policy = _policy()
    policy.requirements[0].required_resources = [
        ReleaseResourceRequirement(
            kind="relay_job",
            roles=["virtual_remote_mcp_call"],
            states=["succeeded"],
            metadata_equals={"ownership_verified": True},
        )
    ]
    report = _report()
    report.resources[0].role = "virtual_remote_mcp_call"
    report.resources[0].metadata["ownership_verified"] = True

    accepted = _evaluate(policy, [report])
    report.resources[0].state = "failed"
    rejected = _evaluate(policy, [report])

    assert accepted.passed is True
    assert rejected.passed is False
    assert any(
        "requires 1 matching relay_job" in reason
        for reason in rejected.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_rejects_required_resources_from_another_target() -> None:
    policy = _policy()
    report = _report()
    report.resources.append(
        ValidationResource(
            kind="relay_job",
            resource_id="job_cross_target",
            cluster="different-target",
            state="succeeded",
        )
    )

    result = _evaluate(policy, [report])

    assert result.passed is False
    assert any(
        "required evidence resources must belong to cluster primary" in reason
        and "relay_job:job_cross_target:different-target" in reason
        for reason in result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_can_combine_checks_from_multiple_live_reports() -> None:
    now = _timestamp()
    policy = _policy()
    policy.requirements[0].required_checks = ["cleanup.detach", "cleanup.teardown"]
    policy.requirements[0].required_resource_kinds = [
        "relay_job",
        "connector",
        "relay_session",
    ]
    policy.requirements[0].evidence_group_resource_kind = "relay_session"
    detached = _report()
    detached.report_id = "validation_detach"
    detached.checks = [
        ValidationCheck(
            check_id="cleanup.detach",
            summary="detach",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    detached.resources.append(
        ValidationResource(kind="relay_session", resource_id="session-1", cluster="primary")
    )
    teardown = _report()
    teardown.report_id = "validation_teardown"
    teardown.checks = [
        ValidationCheck(
            check_id="cleanup.teardown",
            summary="teardown",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    teardown.resources = [
        ValidationResource(kind="connector", resource_id="connector_1", cluster="primary"),
        ValidationResource(kind="relay_session", resource_id="session-1", cluster="primary"),
    ]

    result = _evaluate(policy, [detached, teardown])

    assert result.passed is True
    assert result.report_ids == ["validation_detach", "validation_teardown"]


def test_actual_release_policy_combines_bootstrap_with_worker_by_physical_target() -> None:
    now = _timestamp()
    policy = load_release_gate_policy(Path(__file__).parents[1] / "docs" / "release-gate-1.0.yaml")
    requirement = next(
        item for item in policy.requirements if item.requirement_id == "ares-released-bootstrap"
    )
    policy.requirements = [requirement]
    target = policy.targets["ares"]
    policy.targets = {"ares": target}
    policy = _without_acceptance_matrix(policy)

    def attach_target(report: LiveValidationReport) -> None:
        report.checks.append(
            ValidationCheck(
                check_id="worker.target-identity",
                summary="verify the physical cluster target",
                status=ValidationStatus.PASSED,
                started_at=now,
                completed_at=now,
                evidence=[EvidenceReference(kind="cluster_target", excerpt="verified")],
            )
        )
        report.resources.append(
            ValidationResource(
                kind="cluster_target",
                resource_id="target:ares",
                role="physical_cluster_target",
                cluster="ares",
                state="verified",
                provider=target.scheduler_provider,
                metadata={
                    "schema_version": "clio-relay.cluster-target-info.v1",
                    "hostname": target.hostnames[0],
                    "fqdn": target.hostnames[0],
                    "scheduler_provider": target.scheduler_provider,
                    "scheduler_cluster_name": target.scheduler_cluster_name,
                    "site_marker_sha256": target.site_marker_sha256,
                    "ssh_host_key_sha256": target.ssh_host_key_sha256,
                    "expected_hostnames": target.hostnames,
                    "expected_ssh_host_key_sha256": target.ssh_host_key_sha256,
                    "expected_scheduler_cluster_name": target.scheduler_cluster_name,
                    "expected_site_marker_sha256": target.site_marker_sha256,
                    "verified": True,
                },
            )
        )

    bootstrap = _report(version=policy.release_version)
    bootstrap.report_id = "validation_bootstrap"
    bootstrap.scenario = "cluster-bootstrap"
    bootstrap.cluster = "ares"
    bootstrap.evidence_trust.invocation_id = "bootstrap-invocation"
    bootstrap.install_source.launcher_receipt["invocation_id"] = "bootstrap-invocation"
    bootstrap.checks = [
        ValidationCheck(
            check_id="cluster.bootstrap",
            summary="bootstrap the exact artifact",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="bootstrap_receipt", excerpt="verified")],
        )
    ]
    bootstrap.resources = [
        ValidationResource(
            kind="bootstrap_invocation",
            resource_id="bootstrap_unique",
            role="cluster_bootstrap",
            cluster="ares",
            state="succeeded",
        )
    ]
    attach_target(bootstrap)

    worker = _report(version=policy.release_version)
    worker.report_id = "validation_worker"
    worker.scenario = "cluster-bootstrap"
    worker.cluster = "ares"
    worker.evidence_trust.invocation_id = "worker-invocation"
    worker.install_source.launcher_receipt["invocation_id"] = "worker-invocation"
    worker.checks = [
        ValidationCheck(
            check_id=check_id,
            summary=check_id,
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="verified")],
        )
        for check_id in requirement.required_checks
        if check_id not in {"cluster.bootstrap", "worker.target-identity"}
    ]
    worker_requirement = next(
        item for item in requirement.required_resources if item.kind == "relay_worker"
    )
    worker.resources = [
        ValidationResource(
            kind="relay_worker",
            resource_id="worker:ares",
            role="cluster_worker",
            cluster="ares",
            state="running",
            metadata=dict(worker_requirement.metadata_equals),
        ),
        ValidationResource(
            kind="relay_job",
            resource_id="job_bootstrap_execution",
            cluster="ares",
            state="succeeded",
        ),
    ]
    attach_target(worker)

    result = evaluate_release_gate(
        policy,
        [bootstrap, worker],
        expected_artifact_sha256="b" * 64,
    )

    assert result.passed is True, result.unsatisfied_requirements
    assert result.report_ids == ["validation_bootstrap", "validation_worker"]


@pytest.mark.parametrize(
    "missing_check",
    ["local.packaged-mcp-boundary", "local.secure-runtime-acceptance"],
)
def test_local_release_policy_rejects_missing_critical_boundary_checks(
    missing_check: str,
) -> None:
    policy = load_release_gate_policy(Path(__file__).parents[1] / "docs" / "release-gate-1.0.yaml")
    requirement = next(
        item for item in policy.requirements if item.requirement_id == "local-release-gate"
    )
    policy = _without_acceptance_matrix(policy)
    policy.requirements = [requirement]
    policy.targets = {}

    report = _report(
        kind=InstallSourceKind.CHECKOUT,
        launcher="uv",
        released_artifact=False,
        artifact_identity_verified=False,
        version=policy.release_version,
    )
    report.scenario = "local-release"
    report.cluster = "local"
    now = _timestamp()
    report.checks = [
        ValidationCheck(
            check_id=check_id,
            summary=check_id,
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="verified")],
        )
        for check_id in requirement.required_checks
        if check_id != missing_check
    ]
    report.resources = [
        ValidationResource(
            kind="wheel",
            resource_id="clio-relay-1.4.13-py3-none-any.whl",
            cluster="local",
            state="built",
        ),
        ValidationResource(
            kind="source_distribution",
            resource_id="clio_relay-1.4.13.tar.gz",
            cluster="local",
            state="built",
        ),
    ]

    result = _evaluate(policy, [report])

    assert result.passed is False
    assert any(
        missing_check in reason for reason in result.unsatisfied_requirements["local-release-gate"]
    )


def test_release_gate_does_not_combine_reports_outside_expected_artifact_hash() -> None:
    now = _timestamp()
    policy = _policy()
    policy.requirements[0].required_checks = ["cleanup.detach", "cleanup.teardown"]
    detached = _report()
    detached.checks = [
        ValidationCheck(
            check_id="cleanup.detach",
            summary="detach",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    teardown = _report()
    teardown.report_id = "validation_other_artifact"
    teardown.install_source.artifact_sha256 = "c" * 64
    teardown.checks = [
        ValidationCheck(
            check_id="cleanup.teardown",
            summary="teardown",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]

    result = _evaluate(policy, [detached, teardown])

    assert result.passed is False
    assert any(
        "tested artifact SHA-256 does not match the immutable candidate" in reason
        for reason in result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_does_not_combine_unrelated_session_evidence() -> None:
    now = _timestamp()
    policy = _policy()
    requirement = policy.requirements[0]
    requirement.required_checks = ["cleanup.detach", "cleanup.teardown"]
    requirement.required_resource_kinds = ["relay_session"]
    requirement.evidence_group_resource_kind = "relay_session"
    detached = _report()
    detached.checks = [
        ValidationCheck(
            check_id="cleanup.detach",
            summary="detach",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    detached.resources = [
        ValidationResource(kind="relay_session", resource_id="session-a", cluster="primary")
    ]
    teardown = _report()
    teardown.report_id = "validation_other_session"
    teardown.checks = [
        ValidationCheck(
            check_id="cleanup.teardown",
            summary="teardown",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="test evidence")],
        )
    ]
    teardown.resources = [
        ValidationResource(kind="relay_session", resource_id="session-b", cluster="primary")
    ]

    result = _evaluate(policy, [detached, teardown])

    assert result.passed is False
    assert any(
        "missing passed checks across reports" in reason
        for reason in result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_release_gate_rejects_one_report_with_multiple_group_resource_ids() -> None:
    policy = _policy()
    requirement = policy.requirements[0]
    requirement.evidence_group_resource_kind = "relay_session"
    requirement.required_resource_kinds = ["relay_session"]
    report = _report()
    report.resources = [
        ValidationResource(kind="relay_session", resource_id="session-a", cluster="primary"),
        ValidationResource(kind="relay_session", resource_id="session-b", cluster="primary"),
    ]

    result = _evaluate(policy, [report])

    assert result.passed is False
    assert any(
        "exactly one evidence-group resource" in reason
        or "no reports share required evidence group" in reason
        for reason in result.unsatisfied_requirements["remote-mcp-primary"]
    )


def test_markdown_is_a_view_of_canonical_report() -> None:
    rendered = render_validation_markdown(_report())

    assert "# live validation validation_test" in rendered
    assert "`passed` `remote-mcp.call`" in rendered
    assert "`relay_job` `job_123`" in rendered


def test_default_report_path_sanitizes_cluster_name(tmp_path: Path) -> None:
    path = default_report_path("site/cluster one", root=tmp_path)

    assert path.parent == tmp_path
    assert path.name.startswith("validation-")
    assert "site-cluster-one" in path.name
    assert path.suffix == ".json"


def test_repository_release_policy_is_machine_readable() -> None:
    policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))

    assert policy.release_version == "1.4.13"
    assert policy.acceptance_matrix is not None
    assert policy.acceptance_matrix["report_count_per_stage"] == 19
    assert policy.acceptance_matrix["matrix_sha256"] == policy.acceptance_matrix_sha256
    matrix_stages = cast(list[dict[str, object]], policy.acceptance_matrix["stages"])
    assert [stage["name"] for stage in matrix_stages] == [
        "candidate",
        "released",
    ]
    assert set(policy.targets) == {"ares", "homelab"}
    assert policy.targets["ares"].identity_sha256 == (
        "f114f70f952dad7eccb5c5cfb340feb653e8879080238e5a8465205ff84afa6b"
    )
    requirement_ids = {item.requirement_id for item in policy.requirements}
    assert "ares-non-jarvis-virtual-mcp" in requirement_ids
    assert "ares-jarvis-native-application-progress" in requirement_ids
    assert "ares-jarvis-lammps-package-progress" in requirement_ids
    assert "homelab-owned-cleanup" in requirement_ids
    catalog = next(
        item for item in policy.requirements if item.requirement_id == "ares-non-jarvis-virtual-mcp"
    )
    assert "remote-mcp.scientific-catalog-user-contract" in catalog.required_checks
    assert "remote-mcp.scientific-catalog-result" in catalog.required_checks
    assert catalog.evidence_group_resource_kind == "mcp_server"
    ares_bootstrap = next(
        item for item in policy.requirements if item.requirement_id == "ares-released-bootstrap"
    )
    assert "worker.service-enabled" in ares_bootstrap.required_checks
    assert "worker.service-persistence" in ares_bootstrap.required_checks
    homelab_cleanup = next(
        item for item in policy.requirements if item.requirement_id == "homelab-owned-cleanup"
    )
    assert "cleanup.gateway-record" in homelab_cleanup.required_checks
    assert "gateway_session" in homelab_cleanup.required_resource_kinds


def test_release_gate_rejects_exact_matrix_when_any_report_is_only_a_subset_match(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    policy_dir = repository / "docs"
    matrix_dir = repository / "examples"
    reports_dir = repository / "reports"
    policy_dir.mkdir(parents=True)
    matrix_dir.mkdir()
    reports_dir.mkdir()
    (repository / "pyproject.toml").write_text("[project]\nname = 'gate-test'\n", encoding="utf-8")
    matrix: dict[str, object] = {
        "schema_version": "clio-relay.release-acceptance-matrix.v1",
        "release_version": "1.0.0",
        "report_count_per_stage": 2,
        "target_labels_are_policy_evidence_instances": True,
        "stages": [
            {
                "name": "candidate",
                "artifact_stage": "immutable_candidate",
                "filename_prefix": "validation",
            },
            {
                "name": "released",
                "artifact_stage": "published",
                "filename_prefix": "released-validation",
            },
        ],
        "reports": [
            {
                "ordinal": 1,
                "id": "first",
                "cluster": "primary",
                "scenario": "remote-mcp",
                "command": ["remote-mcp", "validate"],
                "report_option": "--report",
            },
            {
                "ordinal": 2,
                "id": "second",
                "cluster": "primary",
                "scenario": "remote-mcp",
                "command": ["remote-mcp", "validate"],
                "report_option": "--report",
            },
        ],
    }
    matrix_digest = hashlib.sha256(
        json.dumps(matrix, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    matrix["matrix_sha256"] = matrix_digest
    matrix_path = matrix_dir / "matrix.json"
    matrix_path.write_text(json.dumps(matrix), encoding="utf-8")
    policy_document = _policy().model_dump(mode="json")
    policy_document["acceptance_matrix_path"] = "examples/matrix.json"
    policy_document["acceptance_matrix_sha256"] = matrix_digest
    policy_path = policy_dir / "release-gate.yaml"
    policy_path.write_text(json.dumps(policy_document), encoding="utf-8")
    policy = load_release_gate_policy(policy_path)

    reports: list[LiveValidationReport] = []
    for logical_id in ("first", "second"):
        report = _report()
        report.report_id = f"validation_document_{logical_id}"
        report_path = reports_dir / f"released-validation-{logical_id}.json"
        write_validation_report(report, report_path)
        reports.append(load_validation_report(report_path))

    result = _evaluate(policy, list(reversed(reports)))

    assert result.passed is False
    assert result.satisfied_requirements == ["remote-mcp-primary"]
    assert result.acceptance_matrix_sha256 == matrix_digest
    assert result.acceptance_matrix_stage == "released"
    assert result.acceptance_report_ids == ["first", "second"]
    assert result.acceptance_report_document_ids == [
        "validation_document_first",
        "validation_document_second",
    ]
    assert result.unsatisfied_requirements["acceptance-matrix"] == [
        "acceptance matrix reports were not used by any policy requirement: ['first']"
    ]


def test_explicit_cancel_release_gate_requires_preserved_unowned_scheduler_sentinel() -> None:
    repository_policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))
    requirement = next(
        item
        for item in repository_policy.requirements
        if item.requirement_id == "ares-explicit-job-cancel"
    )
    policy = _policy().model_copy(update={"requirements": [requirement]})
    report = _report()
    report.scenario = "cleanup"
    report.cluster = "ares"
    now = _timestamp()
    report.checks = [
        ValidationCheck(
            check_id=check_id,
            summary=check_id,
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt=f"{check_id}=passed")],
        )
        for check_id in requirement.required_checks
    ]
    report.resources = [
        ValidationResource(
            kind="relay_session",
            resource_id="session-1:generation-1",
            role="remote_relay_api:stop",
            cluster="ares",
            state="stopped",
        ),
        ValidationResource(
            kind="relay_job",
            resource_id="relay-owned",
            cluster="ares",
            state="canceled",
        ),
        ValidationResource(
            kind="scheduler_job",
            resource_id="scheduler-owned",
            cluster="ares",
            state="canceled",
            provider="slurm",
            metadata={
                "ownership_verified": True,
                "verified_after_operation": True,
            },
        ),
        ValidationResource(
            kind="scheduler_job",
            resource_id="scheduler-unrelated",
            role="scheduler_sentinel:retain",
            cluster="ares",
            state="retained",
            provider="slurm",
            metadata={
                "ownership_verified": False,
                "verified_after_operation": True,
                "unowned_sentinel": True,
                "active_before_operation": True,
                "preservation_verified": True,
                "pre_phase": "running",
                "post_phase": "running",
            },
        ),
        ValidationResource(
            kind="relay_worker",
            resource_id="worker-1",
            role="cluster_worker",
            cluster="ares",
            state="running",
        ),
    ]

    accepted = _evaluate(policy, [report])
    assert accepted.passed is True

    rejected_cases: dict[str, list[LiveValidationReport]] = {}
    without_sentinel = report.model_copy(deep=True)
    without_sentinel.resources = [
        resource
        for resource in without_sentinel.resources
        if resource.role != "scheduler_sentinel:retain"
    ]
    rejected_cases["missing"] = [without_sentinel]

    target_not_canceled = report.model_copy(deep=True)
    next(
        resource
        for resource in target_not_canceled.resources
        if resource.resource_id == "scheduler-owned"
    ).state = "failed"
    rejected_cases["target-not-canceled"] = [target_not_canceled]

    sentinel_canceled = report.model_copy(deep=True)
    next(
        resource
        for resource in sentinel_canceled.resources
        if resource.resource_id == "scheduler-unrelated"
    ).state = "canceled"
    rejected_cases["sentinel-canceled"] = [sentinel_canceled]

    sentinel_unverified = report.model_copy(deep=True)
    next(
        resource
        for resource in sentinel_unverified.resources
        if resource.resource_id == "scheduler-unrelated"
    ).metadata["preservation_verified"] = False
    rejected_cases["sentinel-unverified"] = [sentinel_unverified]

    split_owned = without_sentinel.model_copy(deep=True)
    split_sentinel = report.model_copy(deep=True)
    split_sentinel.report_id = "validation_other_session"
    split_sentinel.resources = [
        ValidationResource(
            kind="relay_session",
            resource_id="session-2:generation-2",
            role="remote_relay_api:stop",
            cluster="ares",
            state="stopped",
        ),
        next(
            resource
            for resource in split_sentinel.resources
            if resource.resource_id == "scheduler-unrelated"
        ),
    ]
    rejected_cases["different-session-group"] = [split_owned, split_sentinel]

    for case, reports in rejected_cases.items():
        result = _evaluate(policy, reports)
        assert result.passed is False, case
        assert requirement.requirement_id in result.unsatisfied_requirements, case


def test_repository_policy_target_digests_match_canonical_live_pins() -> None:
    policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))
    ares = _report(version=policy.release_version)
    ares.cluster = "ares"
    _attach_verified_target_identity(
        ares,
        hostname="ares.ares.local",
        fingerprint="SHA256:bRLzWxtUOvr7HWapionskS2S74r21SlOArTukSkKrcw",
        site_marker_sha256=("2162bf2c726235b2d856e1adeaded38c626766245b6e36ef933a0f83cb9d54ea"),
    )
    ares_target = next(resource for resource in ares.resources if resource.kind == "cluster_target")
    ares_target.metadata.update(
        {
            "scheduler_cluster_name": "linux",
            "ssh_host_key_sha256": [
                "SHA256:bRLzWxtUOvr7HWapionskS2S74r21SlOArTukSkKrcw",
                "SHA256:U5VnrrdDfBluAMQpDQ+PSHU9XyaVc5JeJ+u1oSW3zk4",
                "SHA256:rlGpdsBvZOaqcY5d6fKgVCUrAwZhLRGK70v3MNQ/LKA",
            ],
        }
    )

    homelab = _report(version=policy.release_version)
    homelab.report_id = "validation_homelab"
    homelab.cluster = "homelab"
    _attach_verified_target_identity(
        homelab,
        hostname="Server",
        fingerprint="SHA256:Icn/LmcEIhEg//DW9ClFNxUrP3lawXtiQM7PzuCHUpY",
        site_marker_sha256=("4a966c6fb25b0aef845a064fdb5243269d749c81370ba015172cefde5fd1f32f"),
    )
    homelab_target = next(
        resource for resource in homelab.resources if resource.kind == "cluster_target"
    )
    homelab_target.provider = "external"
    homelab_target.metadata.update(
        {
            "scheduler_provider": "external",
            "scheduler_cluster_name": None,
            "ssh_host_key_sha256": [
                "SHA256:Icn/LmcEIhEg//DW9ClFNxUrP3lawXtiQM7PzuCHUpY",
                "SHA256:wq6LnyUAsrEEe/mK+jHaOlpCF/CLoWa9xxgefvBR1JY",
            ],
        }
    )

    result = _evaluate(policy, [ares, homelab])

    assert "target-identity" not in result.unsatisfied_requirements
    assert result.policy_target_identity_sha256 == result.target_identity_sha256


def test_native_application_progress_gate_rejects_legacy_adapter_only_evidence() -> None:
    repository_policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))
    requirement = next(
        item
        for item in repository_policy.requirements
        if item.requirement_id == "ares-jarvis-native-application-progress"
    )
    lammps_requirement = next(
        item
        for item in repository_policy.requirements
        if item.requirement_id == "ares-jarvis-lammps-package-progress"
    )
    worker_requirement = next(
        item for item in requirement.required_resources if item.kind == "relay_worker"
    )
    worker_metadata = deepcopy(worker_requirement.metadata_equals)
    component_runtime = worker_metadata.get("component_runtime")
    assert isinstance(component_runtime, dict)
    typed_component_runtime = cast(dict[str, object], component_runtime)
    jarvis_runtime = typed_component_runtime.get("jarvis-cd")
    assert isinstance(jarvis_runtime, dict)
    typed_jarvis_runtime = cast(dict[str, object], jarvis_runtime)
    typed_jarvis_runtime.update(
        {
            "provider_interpreter_verified": True,
            "provider_native_execution_capability_verified": True,
            "extra_runtime_evidence": "preserved",
        }
    )
    policy = _without_acceptance_matrix(
        repository_policy.model_copy(
            update={
                "release_blockers": [],
                "requirements": [requirement],
                "targets": {
                    "ares": _target_policy(hostnames=["ares-login.example"]),
                },
            }
        )
    )
    report = _report(version=policy.release_version)
    report.scenario = "remote-mcp"
    report.cluster = "ares"
    now = _timestamp()
    gray_execution_id = "execution-gray-scott"
    report.checks = [
        ValidationCheck(
            check_id=check_id,
            summary=check_id,
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="test",
                    excerpt=f"{check_id}=passed",
                    metadata={"execution_id": gray_execution_id},
                )
            ],
        )
        for check_id in requirement.required_checks
    ]
    legacy_provider_metadata = {
        "adapter": "lammps",
        "package_name": "builtin.lammps",
        "package_version": "builtin",
        "provider_entry_point": "lammps",
        "provider_entry_point_value": "jarvis_cd.progress.lammps:adapter_from_package",
        "provider_distribution": "jarvis_cd",
        "provider_distribution_version": "1.3.12",
        "provider_source_authority": "package_log",
        "provider_validated": True,
        "acceptance_validated": True,
    }
    report.resources = [
        ValidationResource(
            kind="relay_job",
            resource_id="job_gray_scott",
            role="virtual_jarvis_mcp_call",
            cluster="ares",
            state="succeeded",
            metadata={"execution_id": gray_execution_id},
        ),
        ValidationResource(
            kind="relay_worker",
            resource_id="worker:ares",
            role="cluster_worker",
            cluster="ares",
            state="running",
            metadata=worker_metadata,
        ),
        ValidationResource(
            kind="package_progress_provider",
            resource_id="jarvis_cd:1.3.12:lammps:lammps",
            role="jarvis_package_progress",
            cluster="ares",
            state="verified",
            provider="jarvis_cd",
            metadata=legacy_provider_metadata,
        ),
    ]
    _attach_verified_target_identity(report, hostname="ares-login.example")

    legacy_provider = next(
        resource for resource in report.resources if resource.kind == "package_progress_provider"
    )
    legacy_provider.metadata = legacy_provider_metadata
    rejected = _evaluate(policy, [report])
    report.resources.append(
        ValidationResource(
            kind="relay_job",
            resource_id="job_gray_scott_query",
            role="jarvis_mcp_execution_query",
            cluster="ares",
            state="succeeded",
            metadata={"execution_id": gray_execution_id},
        )
    )
    report.resources.append(
        ValidationResource(
            kind="jarvis_execution_progress",
            resource_id="native-execution:gray_scott_bp5",
            role="jarvis_mcp_native_progress",
            cluster="ares",
            state="verified",
            provider="jarvis_cd",
            metadata={
                "source": "jarvis_get_execution",
                "execution_id": gray_execution_id,
                "package_name": "builtin.gray_scott",
                "package_id": "gray_scott_bp5",
                "progress_schema_version": "jarvis.progress.v1",
                "provider_source_authority": "jarvis_get_execution",
                "native_documents_validated": True,
                "query_identity_validated": True,
                "live_observed_while_running": True,
                "lifecycle_query_validated": True,
                "terminal_query_bound": True,
            },
        )
    )
    report.resources.append(
        ValidationResource(
            kind="jarvis_generated_artifact",
            resource_id="art_gray_scott_output",
            role="output",
            cluster="ares",
            state="finalized",
            provider="jarvis-cd",
            metadata={
                "execution_id": gray_execution_id,
                "package_id": "gray_scott_bp5",
                "logical_name": "gray-scott-timesteps",
                "kind": "scientific_dataset",
                "role": "output",
                "structure": "collection",
                "ownership": "shared",
                "state": "finalized",
                "media_type": "application/x-adios2-bp",
                "format": "adios2-bp5",
                "location": {
                    "kind": "cluster_path",
                    "value": "/scratch/ares/gray-scott/gs.bp",
                },
                "metadata": {
                    "application": "gray_scott",
                    "io_backend": "adios2",
                    "latest_timestep": 10000,
                    "member_pattern": "adios2-steps",
                    "members_observed": 10,
                    "completion_signal": "process_exit_zero_after_final_output",
                },
            },
        )
    )
    accepted = _evaluate(policy, [report])

    lammps_policy = policy.model_copy(update={"requirements": [lammps_requirement]})
    lammps_rejected = _evaluate(lammps_policy, [report])
    lammps_execution_id = "execution-lammps"
    mixed_execution_report = report.model_copy(deep=True)
    mixed_execution_report.resources.append(
        ValidationResource(
            kind="jarvis_execution_progress",
            resource_id="native-execution:lammps",
            role="jarvis_mcp_native_progress",
            cluster="ares",
            state="verified",
            provider="jarvis_cd",
            metadata={
                "source": "jarvis_get_execution",
                "execution_id": lammps_execution_id,
                "package_name": "builtin.lammps",
                "package_id": "lammps",
                "progress_schema_version": "jarvis.progress.v1",
                "progress_determinate": True,
                "provider_source_authority": "jarvis_get_execution",
                "native_documents_validated": True,
                "query_identity_validated": True,
                "live_observed_while_running": True,
                "lifecycle_query_validated": True,
                "terminal_query_bound": True,
            },
        )
    )
    mixed_execution = _evaluate(lammps_policy, [mixed_execution_report])

    lammps_report = mixed_execution_report.model_copy(deep=True)
    lammps_report.report_id = "validation_lammps_execution"
    lammps_report.evidence_trust.invocation_id = "run-20260710-lammps"
    lammps_report.install_source.launcher_receipt["invocation_id"] = "run-20260710-lammps"
    lammps_report.resources = [
        resource
        for resource in lammps_report.resources
        if resource.kind != "jarvis_generated_artifact"
        and not (
            resource.kind == "jarvis_execution_progress"
            and resource.metadata.get("package_id") == "gray_scott_bp5"
        )
    ]
    for check in lammps_report.checks:
        for evidence in check.evidence:
            evidence.metadata["execution_id"] = lammps_execution_id
    for resource in lammps_report.resources:
        if resource.role in {"virtual_jarvis_mcp_call", "jarvis_mcp_execution_query"}:
            resource.metadata["execution_id"] = lammps_execution_id
            resource.resource_id = resource.resource_id.replace("gray_scott", "lammps")

    lammps_accepted = _evaluate(lammps_policy, [lammps_report])
    dual_policy = policy.model_copy(update={"requirements": [requirement, lammps_requirement]})
    dual_accepted = _evaluate(dual_policy, [report, lammps_report])

    lammps_progress_only = lammps_report.model_copy(deep=True)
    lammps_progress_only.report_id = "validation_lammps_progress_only"
    lammps_progress_only.evidence_trust.invocation_id = "run-20260710-lammps-progress"
    lammps_progress_only.install_source.launcher_receipt["invocation_id"] = (
        "run-20260710-lammps-progress"
    )
    lammps_progress_only.checks = [
        ValidationCheck(
            check_id="worker.artifact-version",
            summary="worker identity only",
            status=ValidationStatus.PASSED,
            started_at=now,
            completed_at=now,
            evidence=[EvidenceReference(kind="test", excerpt="worker identity")],
        )
    ]
    split_reports = _evaluate(lammps_policy, [report, lammps_progress_only])

    cross_execution_artifact = report.model_copy(deep=True)
    next(
        resource
        for resource in cross_execution_artifact.resources
        if resource.kind == "jarvis_generated_artifact"
    ).metadata["execution_id"] = "execution-other"
    cross_execution = _evaluate(policy, [cross_execution_artifact])

    assert rejected.passed is False
    assert accepted.passed is True
    assert lammps_rejected.passed is False
    assert lammps_requirement.requirement_id in lammps_rejected.unsatisfied_requirements
    assert mixed_execution.passed is False
    assert lammps_accepted.passed is True
    assert dual_accepted.passed is True
    assert split_reports.passed is False
    assert any(
        "one coherent report" in reason
        for reason in split_reports.unsatisfied_requirements[lammps_requirement.requirement_id]
    )
    assert cross_execution.passed is False
    assert any(
        "do not identify exactly one execution" in reason
        for reason in cross_execution.unsatisfied_requirements[requirement.requirement_id]
    )

    artifact = next(
        resource for resource in report.resources if resource.kind == "jarvis_generated_artifact"
    )
    invalid_values: list[tuple[str, str, object]] = [
        ("location", "kind", "local_path"),
        ("metadata", "member_pattern", "empty"),
        ("metadata", "members_observed", 0),
        ("metadata", "completion_signal", "compute_step_completed"),
    ]
    for parent, key, invalid_value in invalid_values:
        invalid = report.model_copy(deep=True)
        invalid_artifact = next(
            resource
            for resource in invalid.resources
            if resource.resource_id == artifact.resource_id
        )
        nested = cast(dict[str, object], invalid_artifact.metadata[parent])
        nested[key] = invalid_value
        result = _evaluate(policy, [invalid])
        assert result.passed is False, f"{parent}.{key}"
        assert requirement.requirement_id in result.unsatisfied_requirements


def test_report_invocation_redacts_cli_secrets(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.orig_argv",
        ["clio-relay", "live-test", "--transport-token", "secret-value"],
    )

    report = new_live_validation_report(
        scenario="live-test",
        cluster="primary",
        install_source="checkout:file:///checkout",
    )

    assert "secret-value" not in report.invocation
    assert report.invocation[-1] == "<redacted>"


def test_remote_mcp_acceptance_converts_to_canonical_report() -> None:
    domain = RemoteMcpAcceptanceReport(
        generated_at=_timestamp(),
        cluster="primary",
        server_name="science",
        remote_tool_name="inspect",
        virtual_alias="remote_science_inspect",
        profile="user",
        passed=True,
        checks=[
            RemoteMcpAcceptanceCheck(
                name="remote-mcp.call",
                passed=True,
                message="durable virtual call succeeded",
                evidence={"job_id": "job_call"},
            )
        ],
        discovery={
            "provenance": {
                "discovery_job_id": "job_discovery",
                "artifact_id": "artifact_schema",
            }
        },
        call_job={"job_id": "job_call", "state": "succeeded"},
        artifacts=[
            {
                "artifact_id": "artifact_result",
                "kind": "mcp_result",
                "uri": "file:///result.json",
                "sha256": "a" * 64,
            }
        ],
    )

    canonical = domain.to_live_validation_report(
        launcher="uv-tool",
        install_source="pypi:clio-relay==1.0.0",
        artifact_sha256="b" * 64,
    )

    assert canonical.scenario == "remote-mcp"
    assert canonical.status is ValidationStatus.PASSED
    assert canonical.checks[0].check_id == "remote-mcp.call"
    assert {resource.resource_id for resource in canonical.resources} == {
        "job_call",
        "job_discovery",
        "artifact_schema",
        "artifact_result",
    }


def test_spack_producer_evidence_satisfies_real_policy_and_rejects_mutations() -> None:
    full_policy = load_release_gate_policy(
        Path(__file__).parents[1] / "docs" / "release-gate-1.0.yaml"
    )
    requirement = next(
        item for item in full_policy.requirements if item.requirement_id == "ares-spack-virtual-mcp"
    )
    policy = _without_acceptance_matrix(full_policy).model_copy(
        update={
            "requirements": [requirement],
            "require_target_identity": False,
            "targets": {},
        }
    )
    expected_hash = "p5gjmq4rseitqanua7mdd2zdnag4v3u2"
    expected_prefix = (
        "/mnt/common/jcernudagarcia/spack/opt/spack/"
        "linux-ubuntu22.04-skylake_avx512/gcc-11.4.0/"
        f"lammps-20240829.1-{expected_hash}"
    )
    cases: list[
        tuple[
            str,
            dict[str, object],
            dict[str, object],
            dict[str, object],
        ]
    ] = [
        (
            "spack_find",
            {"query": "lammps"},
            {},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "find",
                "query": "lammps",
                "count": 1,
                "packages": [{"name": "lammps", "dag_hash": expected_hash}],
            },
        ),
        (
            "spack_locate",
            {"spec": "lammps"},
            {"requested_spec": "lammps", "prefix": expected_prefix},
            {
                "schema_version": "spack.mcp.result.v1",
                "operation": "locate",
                "requested_spec": "lammps",
                "load_spec": f"/{expected_hash}",
                "package": {"name": "lammps", "dag_hash": expected_hash},
                "prefix": expected_prefix,
            },
        ),
    ]
    server_requirement = next(
        item for item in requirement.required_resources if item.kind == "mcp_server"
    )
    worker_requirement = next(
        item for item in requirement.required_resources if item.kind == "relay_worker"
    )
    reports: list[LiveValidationReport] = []
    trusted = _report(version=policy.release_version)
    for index, (tool, arguments, expectation_fields, structured) in enumerate(cases, start=1):
        expectation = RemoteMcpStructuredResultExpectation.model_validate(
            {
                "contract": "clio-kit-spack-user-v2.1",
                "tool": tool,
                "package_name": "lammps",
                "dag_hash": expected_hash,
                **expectation_fields,
            }
        )
        semantic = build_remote_mcp_structured_result_check(
            expectation=expectation,
            remote_tool_name=tool,
            arguments=arguments,
            protocol_result={"structuredContent": structured},
            output_schema={
                "type": "object",
                "properties": {key: {} for key in structured},
                "required": sorted(structured),
                "additionalProperties": False,
            },
        )
        checks = [semantic]
        checks.extend(
            RemoteMcpAcceptanceCheck(
                name=check_id,
                passed=True,
                message=f"{check_id} passed",
                evidence={"production_policy_fixture": True},
            )
            for check_id in requirement.required_checks
            if check_id != semantic.name
        )
        domain = RemoteMcpAcceptanceReport(
            generated_at=_timestamp(),
            cluster="ares",
            server_name="spack",
            remote_tool_name=tool,
            virtual_alias=f"remote_spack_{tool}",
            profile="user",
            passed=True,
            checks=checks,
            call_job={
                "job_id": f"job_{tool}",
                "cluster": "ares",
                "kind": "mcp_call",
                "state": "succeeded",
                "spec": {"arguments": arguments},
            },
            artifacts=[
                {
                    "artifact_id": f"artifact_{tool}",
                    "kind": "mcp_result",
                    "sha256": str(index) * 64,
                }
            ],
        )
        report = domain.to_live_validation_report()
        invocation_id = f"run-20260710-spack-{index}"
        report.report_id = f"validation_ares_{tool}"
        report.software = trusted.software.model_copy(deep=True)
        report.install_source = trusted.install_source.model_copy(deep=True)
        report.install_source.launcher_receipt["invocation_id"] = invocation_id
        report.evidence_trust = trusted.evidence_trust.model_copy(
            update={"invocation_id": invocation_id}
        )
        report.resources.extend(
            [
                ValidationResource(
                    kind="mcp_server",
                    resource_id="ares-spack-server",
                    role=cast(list[str], server_requirement.roles)[0],
                    cluster="ares",
                    state=cast(list[str], server_requirement.states)[0],
                    metadata=server_requirement.metadata_equals,
                ),
                ValidationResource(
                    kind="relay_worker",
                    resource_id="ares-relay-worker",
                    role=cast(list[str], worker_requirement.roles)[0],
                    cluster="ares",
                    state=cast(list[str], worker_requirement.states)[0],
                    metadata=worker_requirement.metadata_equals,
                ),
            ]
        )
        reports.append(report)

    accepted = _evaluate(policy, reports)
    assert accepted.passed is True
    assert requirement.requirement_id in accepted.satisfied_requirements

    mutations: list[tuple[str, str, object]] = [
        ("spack_find", "package_hashes_for_expected_name", ["f" * 32]),
        ("spack_locate", "prefix", "/wrong/prefix"),
    ]
    for tool, key, value in mutations:
        changed = [report.model_copy(deep=True) for report in reports]
        target = next(
            resource
            for report in changed
            for resource in report.resources
            if resource.role == "virtual_remote_mcp_call"
            and resource.metadata.get("remote_mcp_tool_name") == tool
        )
        assertion = cast(dict[str, object], target.metadata["structured_result_assertion"])
        observed = cast(dict[str, object], assertion["observed"])
        observed[key] = value

        rejected = _evaluate(policy, changed)
        assert requirement.requirement_id in rejected.unsatisfied_requirements


def _fresh_spack_transition_report(
    requirement: ReleaseGateRequirement,
    *,
    release_version: str,
) -> LiveValidationReport:
    """Build representative canonical evidence for the typed fresh-install gate."""
    requested_spec = "libsigsegv@2.14"
    package_name = "libsigsegv"
    dag_hash = "a" * 32
    exact_hash_spec = f"/{dag_hash}"
    root = "/scratch/clio-relay-acceptance/run-42/spack-fresh"
    store_root = f"{root}/store"
    prefix = f"{store_root}/linux-x86_64/gcc-12.3.0/libsigsegv-2.14-a"
    configuration_path = f"{root}/acceptance-manifest.sha256"
    configuration_sha256 = "6" * 64
    components = [
        {
            "relative_path": "bin/spack",
            "sha256": "7" * 64,
            "size_bytes": 256,
            "regular_file": True,
        },
        {
            "relative_path": "overrides/config.yaml",
            "sha256": "8" * 64,
            "size_bytes": 128,
            "regular_file": True,
        },
    ]
    pre_configuration = {
        "schema_version": "clio-relay.spack-configuration-observation.v1",
        "phase": "preinstall",
        "manifest_path": configuration_path,
        "manifest_sha256": configuration_sha256,
        "manifest_size_bytes": 512,
        "manifest_regular_file": True,
        "components": components,
    }
    post_configuration = {
        **pre_configuration,
        "phase": "postinstall",
        "components": [dict(item) for item in components],
    }
    preinstall_result: dict[str, object] = {
        "schema_version": "spack.mcp.result.v1",
        "operation": "find",
        "query": requested_spec,
        "count": 0,
        "packages": [],
    }
    package: dict[str, object] = {
        "name": package_name,
        "version": "2.14",
        "dag_hash": dag_hash,
        "compiler": "gcc@12.3.0",
        "architecture": "linux-x86_64",
    }
    install_result: dict[str, object] = {
        "schema_version": "spack.mcp.result.v1",
        "operation": "install",
        "requested_spec": requested_spec,
        "reuse": False,
        "status": "installed",
        "duration_seconds": 1.25,
        "package": package,
        "package_count": 1,
    }
    postinstall_result: dict[str, object] = {
        "schema_version": "spack.mcp.result.v1",
        "operation": "locate",
        "requested_spec": exact_hash_spec,
        "load_spec": exact_hash_spec,
        "prefix": prefix,
        "package": package,
    }
    phase_jobs = {
        "preinstall": "job-spack-preinstall",
        "install": "job-spack-install",
        "postinstall": "job-spack-postinstall",
    }
    check_evidence: dict[str, dict[str, object]] = {
        "remote-mcp.spack-preinstall-absent": {
            "expected_requested_spec": requested_spec,
            "submitted_arguments": {"query": requested_spec},
            "observed": preinstall_result,
            "output_schema": {"structured_content_valid": True},
            "failures": [],
        },
        "remote-mcp.spack-fresh-install": {
            "expected": {
                "requested_spec": requested_spec,
                "package_name": package_name,
                "dag_hash": dag_hash,
                "reuse": False,
                "status": "installed",
            },
            "submitted_arguments": {"spec": requested_spec, "reuse": False},
            "observed": install_result,
            "output_schema": {"structured_content_valid": True},
            "failures": [],
        },
        "remote-mcp.spack-postinstall-locate": {
            "expected": {
                "requested_spec": exact_hash_spec,
                "package_name": package_name,
                "dag_hash": dag_hash,
            },
            "submitted_arguments": {"spec": exact_hash_spec},
            "observed": postinstall_result,
            "output_schema": {"structured_content_valid": True},
            "failures": [],
        },
        "remote-mcp.spack-disposable-store": {
            "fresh_install_store_root": store_root,
            "observed_prefix": prefix,
            "root_is_canonical_absolute": True,
            "prefix_is_strict_descendant": True,
        },
        "remote-mcp.spack-transition-identity": {
            "underlying_reports_passed": True,
            "scopes": [["ares", "spack-fresh", "user"]],
            "tool_names": ["spack_find", "spack_install", "spack_locate"],
            "expected_tool_names": ["spack_find", "spack_install", "spack_locate"],
            "registration_revisions": ["3" * 64] * 3,
            "cluster_route_revisions": ["4" * 64] * 3,
            "catalog_revisions": ["5" * 64] * 3,
            "revision_matches": {
                "registration": True,
                "cluster_route": True,
                "catalog": True,
            },
            "same_server_artifact": True,
            "server_artifact_sha256": "9" * 64,
        },
        "remote-mcp.spack-transition-durable-evidence": {
            "required_artifact_kinds": ["mcp_result", "provenance", "stderr", "stdout"],
            "job_ids": list(phase_jobs.values()),
            "distinct_job_ids": True,
            "distinct_artifact_ids": True,
            "phases": {
                phase: {
                    "job_id": job_id,
                    "state": "succeeded",
                    "artifact_kinds": ["mcp_result", "provenance", "stderr", "stdout"],
                    "artifact_count": 4,
                    "artifacts_valid": True,
                    "stdio_valid": True,
                    "passed": True,
                }
                for phase, job_id in phase_jobs.items()
            },
        },
        "remote-mcp.spack-fresh-configuration": {
            "expected": {
                "manifest_path": configuration_path,
                "configuration_sha256": configuration_sha256,
            },
            "preinstall": pre_configuration,
            "postinstall": post_configuration,
            "digest_matches": True,
            "path_matches": True,
            "components_match": True,
            "manifest_metadata_matches": True,
            "phases_match": True,
        },
    }
    report = _report(version=release_version)
    report.report_id = "validation_ares_spack_fresh_install"
    report.cluster = "ares"
    report.checks = [
        ValidationCheck(
            check_id=check_id,
            summary=f"{check_id} passed",
            status=ValidationStatus.PASSED,
            started_at=_timestamp(),
            completed_at=_timestamp(),
            evidence=[
                EvidenceReference(
                    kind="remote_mcp_acceptance",
                    excerpt=f"{check_id} passed",
                    metadata=evidence,
                )
            ],
        )
        for check_id, evidence in check_evidence.items()
    ]
    phase_definitions: tuple[tuple[str, str, str, dict[str, object], dict[str, object]], ...] = (
        (
            "preinstall",
            "spack_preinstall_find",
            "spack_find",
            {"query": requested_spec},
            preinstall_result,
        ),
        (
            "install",
            "spack_fresh_install",
            "spack_install",
            {"spec": requested_spec, "reuse": False},
            install_result,
        ),
        (
            "postinstall",
            "spack_postinstall_locate",
            "spack_locate",
            {"spec": exact_hash_spec},
            postinstall_result,
        ),
    )
    report.resources = []
    for phase, role, tool, arguments, structured_result in phase_definitions:
        report.resources.append(
            ValidationResource(
                kind="relay_job",
                resource_id=phase_jobs[phase],
                role=role,
                cluster="ares",
                state="succeeded",
                metadata={
                    "remote_mcp_server_name": "spack-fresh",
                    "remote_mcp_tool_name": tool,
                    "virtual_alias": f"remote_spack_fresh_{tool}",
                    "profile": "user",
                    "arguments": arguments,
                    "stdio": {"initialize_passed": True},
                    "structured_result": structured_result,
                },
            )
        )
    report.artifacts = [
        EvidenceReference(
            kind="spack_fresh_install_configuration",
            reference=configuration_path,
            sha256=configuration_sha256,
        )
    ]
    for phase, role, _, _, _ in phase_definitions:
        for kind in ("stdout", "stderr", "mcp_result", "provenance"):
            artifact_id = f"artifact-{phase}-{kind}"
            uri = f"relay-artifact://ares/{artifact_id}"
            artifact_sha256 = hashlib.sha256(artifact_id.encode()).hexdigest()
            report.resources.append(
                ValidationResource(
                    kind="artifact",
                    resource_id=artifact_id,
                    role=f"{role}_{kind}",
                    cluster="ares",
                    references=[uri],
                    metadata={
                        "artifact_id": artifact_id,
                        "job_id": phase_jobs[phase],
                        "kind": kind,
                        "sha256": artifact_sha256,
                        "uri": uri,
                        "transition_phase": phase,
                    },
                )
            )
            report.artifacts.append(
                EvidenceReference(kind=f"{role}_{kind}", reference=uri, sha256=artifact_sha256)
            )
    report.resources.append(
        ValidationResource(
            kind="configuration_manifest",
            resource_id=configuration_sha256,
            role="spack_fresh_install_configuration",
            cluster="ares",
            state="verified",
            references=[configuration_path],
            metadata={
                "expected_sha256": configuration_sha256,
                "preinstall": pre_configuration,
                "postinstall": post_configuration,
            },
        )
    )
    server_requirement = next(
        item for item in requirement.required_resources if item.kind == "mcp_server"
    )
    worker_requirement = next(
        item for item in requirement.required_resources if item.kind == "relay_worker"
    )
    report.resources.extend(
        [
            ValidationResource(
                kind="relay_worker",
                resource_id="ares-relay-worker",
                role="cluster_worker",
                cluster="ares",
                state="running",
                metadata=worker_requirement.metadata_equals,
            ),
            ValidationResource(
                kind="mcp_server",
                resource_id="spack-fresh:clio-kit-2.5.0",
                role="remote_mcp_server",
                cluster="ares",
                state="verified",
                metadata=server_requirement.metadata_equals,
            ),
        ]
    )
    return LiveValidationReport.model_validate(report.model_dump(mode="python"))


def _mutate_fresh_spack_transition(
    report: LiveValidationReport,
    case: str,
) -> None:
    """Apply one adversarial mutation to typed fresh-install evidence."""
    phase_resources = {
        resource.role: resource
        for resource in report.resources
        if resource.kind == "relay_job" and resource.role is not None
    }
    checks = {check.check_id: check.evidence[0].metadata for check in report.checks}
    if case == "profile":
        phase_resources["spack_fresh_install"].metadata["profile"] = "admin"
    elif case == "package":
        structured = cast(
            dict[str, object],
            phase_resources["spack_fresh_install"].metadata["structured_result"],
        )
        cast(dict[str, object], structured["package"])["name"] = "wrong"
    elif case == "reuse":
        phase_resources["spack_fresh_install"].metadata["arguments"] = {
            "spec": "libsigsegv@2.14",
            "reuse": True,
        }
    elif case == "duplicate-job":
        phase_resources["spack_postinstall_locate"].resource_id = phase_resources[
            "spack_preinstall_find"
        ].resource_id
    elif case == "phase-order":
        pre_index = report.resources.index(phase_resources["spack_preinstall_find"])
        post_index = report.resources.index(phase_resources["spack_postinstall_locate"])
        report.resources[pre_index], report.resources[post_index] = (
            report.resources[post_index],
            report.resources[pre_index],
        )
    elif case == "dag-shape":
        structured = cast(
            dict[str, object],
            phase_resources["spack_fresh_install"].metadata["structured_result"],
        )
        cast(dict[str, object], structured["package"])["dag_hash"] = "not-a-dag-hash"
    elif case == "store-path":
        checks["remote-mcp.spack-disposable-store"]["fresh_install_store_root"] = "relative/store"
    elif case == "prefix-path":
        structured = cast(
            dict[str, object],
            phase_resources["spack_postinstall_locate"].metadata["structured_result"],
        )
        structured["prefix"] = "relative/prefix"
        postinstall = cast(
            dict[str, object],
            checks["remote-mcp.spack-postinstall-locate"]["observed"],
        )
        postinstall["prefix"] = "relative/prefix"
        checks["remote-mcp.spack-disposable-store"]["observed_prefix"] = "relative/prefix"
    elif case == "configuration-resource":
        configuration = next(
            resource for resource in report.resources if resource.kind == "configuration_manifest"
        )
        configuration.resource_id = "f" * 64
    elif case == "configuration-artifact":
        configuration = next(
            artifact
            for artifact in report.artifacts
            if artifact.kind == "spack_fresh_install_configuration"
        )
        configuration.reference = "/wrong/configuration"
    elif case == "configuration-path":
        configuration_check = checks["remote-mcp.spack-fresh-configuration"]
        cast(dict[str, object], configuration_check["expected"])["manifest_path"] = (
            "relative/configuration"
        )
        for phase in ("preinstall", "postinstall"):
            cast(dict[str, object], configuration_check[phase])["manifest_path"] = (
                "relative/configuration"
            )
        configuration = next(
            resource for resource in report.resources if resource.kind == "configuration_manifest"
        )
        configuration.references = ["relative/configuration"]
        for phase in ("preinstall", "postinstall"):
            cast(dict[str, object], configuration.metadata[phase])["manifest_path"] = (
                "relative/configuration"
            )
        artifact = next(
            item for item in report.artifacts if item.kind == "spack_fresh_install_configuration"
        )
        artifact.reference = "relative/configuration"
    elif case == "configuration-sha":
        configuration_check = checks["remote-mcp.spack-fresh-configuration"]
        cast(dict[str, object], configuration_check["expected"])["configuration_sha256"] = "bad-sha"
        for phase in ("preinstall", "postinstall"):
            cast(dict[str, object], configuration_check[phase])["manifest_sha256"] = "bad-sha"
        configuration = next(
            resource for resource in report.resources if resource.kind == "configuration_manifest"
        )
        configuration.resource_id = "bad-sha"
        configuration.metadata["expected_sha256"] = "bad-sha"
        for phase in ("preinstall", "postinstall"):
            cast(dict[str, object], configuration.metadata[phase])["manifest_sha256"] = "bad-sha"
        artifact = next(
            item for item in report.artifacts if item.kind == "spack_fresh_install_configuration"
        )
        artifact.sha256 = "bad-sha"
    else:  # pragma: no cover - parametrization is closed below.
        raise AssertionError(f"unknown mutation: {case}")


@pytest.mark.parametrize(
    "case",
    [
        "profile",
        "package",
        "reuse",
        "duplicate-job",
        "phase-order",
        "dag-shape",
        "store-path",
        "prefix-path",
        "configuration-resource",
        "configuration-artifact",
        "configuration-path",
        "configuration-sha",
    ],
)
def test_fresh_spack_transition_policy_cross_binds_dynamic_evidence(case: str) -> None:
    full_policy = load_release_gate_policy(
        Path(__file__).parents[1] / "docs" / "release-gate-1.0.yaml"
    )
    requirement = next(
        item
        for item in full_policy.requirements
        if item.requirement_id == "ares-spack-fresh-install"
    )
    policy = _without_acceptance_matrix(full_policy).model_copy(
        update={
            "requirements": [requirement],
            "require_target_identity": False,
            "targets": {},
        }
    )
    report = _fresh_spack_transition_report(
        requirement,
        release_version=policy.release_version,
    )

    accepted = _evaluate(policy, [report])
    assert accepted.passed is True
    assert accepted.satisfied_requirements == [requirement.requirement_id]

    changed = report.model_copy(deep=True)
    _mutate_fresh_spack_transition(changed, case)
    rejected = _evaluate(policy, [changed])

    assert rejected.passed is False, case
    assert requirement.requirement_id in rejected.unsatisfied_requirements, case


def test_session_cleanup_converts_to_canonical_report_with_safe_job_default() -> None:
    lifecycle = SessionLifecycleReport(
        cluster="primary",
        session_id="desktop-session",
        session_generation_id="generation-123",
        mode="teardown",
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-123",
            process_start_marker="start-123",
            running=True,
            ownership_verified=True,
            observed_at=datetime.now(UTC),
            started_at=datetime.now(UTC),
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=123,
            session_generation_id="generation-123",
            process_start_marker="start-123",
            running=False,
            ownership_verified=True,
            observed_at=datetime.now(UTC),
            started_at=datetime.now(UTC),
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="123",
                location="primary-login",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
            ),
            CleanupResource(
                kind="relay_job",
                resource_id="relay-123",
                location="primary-login",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                metadata={"scheduler_job_ids": ["slurm-123"]},
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="slurm-123",
                location="primary-login",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                provider="slurm",
                verified_after_operation=True,
                observed_state="running",
                metadata={"relay_job_id": "relay-123"},
            ),
        ],
    )

    canonical = lifecycle.to_live_validation_report(cancel_jobs=False)

    assert canonical.scenario == "cleanup"
    assert canonical.status is ValidationStatus.PASSED
    assert {check.check_id for check in canonical.checks} == {
        "cleanup.relay-session",
        "cleanup.jobs-preserved-default",
        "cleanup.no-owned-resources",
    }
    assert canonical.cleanup.cancel_scheduler_jobs is False
    assert canonical.resources[0].kind == "relay_session"


def test_nonexistent_session_teardown_is_not_passing_acceptance_evidence() -> None:
    lifecycle = SessionLifecycleReport(
        cluster="primary",
        session_id="never-started",
        mode="teardown",
        prior_session_status=RemoteSessionStateEvidence(
            running=False,
            ownership_verified=False,
            observed_at=datetime.now(UTC),
        ),
        post_session_status=RemoteSessionStateEvidence(
            running=False,
            ownership_verified=False,
            observed_at=datetime.now(UTC),
        ),
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id="never-started",
                location="primary-login",
                action="stop",
                ownership_verified=False,
                outcome="missing",
            )
        ],
    )

    canonical = lifecycle.to_live_validation_report(cancel_jobs=False)

    checks = {check.check_id: check.status for check in canonical.checks}
    assert canonical.status is ValidationStatus.FAILED
    assert checks["cleanup.relay-session"] is ValidationStatus.FAILED
    assert checks["cleanup.no-owned-resources"] is ValidationStatus.FAILED


def test_gateway_start_and_stop_reports_combine_for_release_gate() -> None:
    ready = GatewaySession(
        session_id="gateway_1",
        cluster="ares",
        name="service",
        state=GatewaySessionState.READY,
        scheduler="slurm",
        scheduler_job_id="123",
        queue_state="running",
        node="compute-1",
        gateway={
            "transport": {
                "remote_connector": {"pid": 11, "config_path": "/tmp/remote.toml"},
                "desktop_connector": {"pid": 12, "config_path": "/tmp/local.toml"},
            }
        },
    )
    started = ServiceRuntimeStartResult(
        session=ready,
        connect_url="http://127.0.0.1:18080",
        health_url="http://127.0.0.1:18080/healthz",
        stream_url="http://127.0.0.1:18080/stream",
        compatibility_urls={},
        events_url=None,
    ).to_live_validation_report()
    stopped_session = ready.model_copy(
        update={
            "state": GatewaySessionState.CLOSED,
            "gateway": {
                **ready.gateway,
                "teardown_intent": {
                    "operation_id": "gateway_cleanup_00000000000000000000000000000000",
                    "gateway_session_id": ready.session_id,
                    "cancel_scheduler_job": False,
                },
            },
        }
    )
    stopped = ServiceRuntimeStopResult(
        session=stopped_session,
        mode="teardown",
        stopped_local_pid=12,
        stopped_remote_pid=11,
        canceled_scheduler_job=None,
        resources=[
            CleanupResource(
                kind="desktop_connector",
                resource_id="12",
                location="desktop",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway_1"},
            ),
            CleanupResource(
                kind="remote_connector",
                resource_id="11",
                location="primary",
                action="stop",
                ownership_verified=True,
                outcome="stopped",
                verified_after_operation=True,
                metadata={"gateway_session_id": "gateway_1"},
            ),
            CleanupResource(
                kind="scheduler_job",
                resource_id="123",
                location="primary",
                provider="slurm",
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state="running",
            ),
            CleanupResource(
                kind="gateway_record",
                resource_id="gateway_1",
                location="core",
                action="close",
                ownership_verified=True,
                outcome="closed",
                verified_after_operation=True,
            ),
        ],
        errors=[],
    ).to_live_validation_report()

    assert started.status is ValidationStatus.PASSED
    assert stopped.status is ValidationStatus.PASSED
    assert {check.check_id for check in started.checks} == {
        "gateway.submit",
        "gateway.allocated",
        "gateway.ready",
        "gateway.connect",
    }
    assert {check.check_id for check in stopped.checks} == {
        "gateway.stop-connectors",
        "gateway.jobs-preserved-default",
        "gateway.closed-record",
    }

    policy_path = Path(__file__).parents[1] / "docs" / "release-gate-1.0.yaml"
    full_policy = load_release_gate_policy(policy_path)
    gateway_requirement = next(
        requirement
        for requirement in full_policy.requirements
        if requirement.requirement_id == "ares-gateway-runtime"
    )
    policy = _without_acceptance_matrix(
        full_policy.model_copy(
            update={
                "requirements": [gateway_requirement],
                "require_target_identity": False,
                "targets": {},
            }
        )
    )
    worker_checks = [
        ValidationCheck(
            check_id=check_id,
            summary=check_id,
            status=ValidationStatus.PASSED,
            started_at=_timestamp(),
            completed_at=_timestamp(),
            evidence=[EvidenceReference(kind="test", excerpt="verified")],
        )
        for check_id in gateway_requirement.required_checks
        if check_id.startswith("worker.")
    ]

    def bind_production_evidence(
        canonical: LiveValidationReport,
        *,
        report_id: str,
        invocation_id: str,
        include_worker: bool,
    ) -> LiveValidationReport:
        evidence = _report(version=policy.release_version)
        evidence.report_id = report_id
        evidence.scenario = canonical.scenario
        evidence.cluster = canonical.cluster
        evidence.status = canonical.status
        evidence.checks = [*canonical.checks, *(worker_checks if include_worker else [])]
        evidence.resources = [*canonical.resources]
        if include_worker:
            evidence.resources.append(
                ValidationResource(
                    kind="relay_worker",
                    resource_id="worker:ares",
                    role="cluster_worker",
                    cluster="ares",
                    state="running",
                )
            )
        evidence.evidence_trust.invocation_id = invocation_id
        evidence.install_source.launcher_receipt["invocation_id"] = invocation_id
        return evidence

    result = evaluate_release_gate(
        policy,
        [
            bind_production_evidence(
                started,
                report_id="validation_gateway_start",
                invocation_id="gateway-start-invocation",
                include_worker=False,
            ),
            bind_production_evidence(
                stopped,
                report_id="validation_gateway_stop",
                invocation_id="gateway-stop-invocation",
                include_worker=True,
            ),
        ],
        expected_artifact_sha256="b" * 64,
    )

    assert result.passed is True, result.unsatisfied_requirements


def test_secure_runtime_release_requirement_accepts_only_verified_preservation() -> None:
    """Secure evidence distinguishes immutable binding from final cleanup state."""
    full_policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))
    requirement = next(
        item
        for item in full_policy.requirements
        if item.requirement_id == "ares-secure-jarvis-runtime"
    )
    policy = _without_acceptance_matrix(
        full_policy.model_copy(
            update={
                "release_blockers": [],
                "requirements": [requirement],
                "require_target_identity": False,
                "targets": {},
            }
        )
    )
    now = _timestamp()

    def metadata_for(kind: str) -> dict[str, object]:
        predicate = next(item for item in requirement.required_resources if item.kind == kind)
        return deepcopy(predicate.metadata_equals)

    def report_with_scheduler_state(state: str) -> LiveValidationReport:
        report = _report(version=policy.release_version)
        report.report_id = "validation_ares_secure_runtime"
        report.scenario = "secure-runtime"
        report.cluster = "ares"
        report.checks = [
            ValidationCheck(
                check_id=check_id,
                summary=check_id,
                status=ValidationStatus.PASSED,
                started_at=now,
                completed_at=now,
                evidence=[EvidenceReference(kind="test", excerpt="verified")],
            )
            for check_id in requirement.required_checks
        ]
        report.resources = [
            ValidationResource(
                kind="relay_job",
                resource_id="job_secure_query",
                role="secure_runtime_query",
                cluster="ares",
                state="succeeded",
            ),
            ValidationResource(
                kind="artifact",
                resource_id="artifact_private_result",
                role="private_mcp_result",
                cluster="ares",
                metadata=metadata_for("artifact"),
            ),
            ValidationResource(
                kind="secure_runtime_binding",
                resource_id="gateway_secure:revision:7",
                role="private_authority_bind",
                cluster="ares",
                state="ready",
                metadata={
                    **metadata_for("secure_runtime_binding"),
                    "source_job_id": "job_secure_query",
                    "source_artifact_id": "artifact_private_result",
                    "service_instance_id": "srv_secure",
                    "service_revision": 7,
                },
            ),
            ValidationResource(
                kind="gateway_session",
                resource_id="gateway_secure",
                role="secure_runtime_teardown",
                cluster="ares",
                state="closed",
                metadata=metadata_for("gateway_session"),
            ),
            *[
                ValidationResource(
                    kind="connector",
                    resource_id=f"connector_{index}",
                    role="secure_runtime_teardown",
                    cluster="ares",
                    state="stopped" if index == 1 else "missing",
                    metadata=metadata_for("connector"),
                )
                for index in (1, 2)
            ],
            ValidationResource(
                kind="scheduler_job",
                resource_id="21996",
                role="secure_runtime_teardown",
                cluster="ares",
                state=state,
                provider="slurm",
                metadata={
                    **metadata_for("scheduler_job"),
                    "observed_state": (
                        "running"
                        if state == "retained"
                        else "completed"
                        if state == "terminal"
                        else "missing"
                    ),
                },
            ),
            ValidationResource(
                kind="relay_worker",
                resource_id="worker:ares",
                role="cluster_worker",
                cluster="ares",
                state="running",
                metadata=metadata_for("relay_worker"),
            ),
            ValidationResource(
                kind="cluster_target",
                resource_id="target:ares",
                role="physical_cluster_target",
                cluster="ares",
                state="verified",
                provider="slurm",
                metadata=metadata_for("cluster_target"),
            ),
        ]
        return report

    for accepted_state in ("retained", "terminal", "missing"):
        accepted = _evaluate(policy, [report_with_scheduler_state(accepted_state)])
        assert accepted.passed is True, accepted.unsatisfied_requirements

    rejected_cases: dict[str, LiveValidationReport] = {}
    missing_binding = report_with_scheduler_state("retained")
    missing_binding.resources = [
        resource
        for resource in missing_binding.resources
        if resource.kind != "secure_runtime_binding"
    ]
    rejected_cases["missing-binding"] = missing_binding

    canceled = report_with_scheduler_state("retained")
    canceled_scheduler = next(
        resource for resource in canceled.resources if resource.kind == "scheduler_job"
    )
    canceled_scheduler.state = "canceled"
    canceled_scheduler.metadata["cancel_scheduler_job"] = True
    rejected_cases["scheduler-canceled"] = canceled

    unverified = report_with_scheduler_state("retained")
    next(resource for resource in unverified.resources if resource.kind == "connector").metadata[
        "verified_after_operation"
    ] = False
    rejected_cases["connector-unverified"] = unverified

    wrong_provider = report_with_scheduler_state("retained")
    next(
        resource for resource in wrong_provider.resources if resource.kind == "scheduler_job"
    ).provider = "external"
    rejected_cases["wrong-provider"] = wrong_provider

    uncontained_query = report_with_scheduler_state("retained")
    next(
        resource
        for resource in uncontained_query.resources
        if resource.kind == "secure_runtime_binding"
    ).metadata["query_mcp_containment_enforceable"] = False
    rejected_cases["uncontained-query-mcp"] = uncontained_query

    uncontained_bind = report_with_scheduler_state("retained")
    next(
        resource
        for resource in uncontained_bind.resources
        if resource.kind == "secure_runtime_binding"
    ).metadata["bind_mcp_containment_enforceable"] = False
    rejected_cases["uncontained-bind-mcp"] = uncontained_bind

    for case, report in rejected_cases.items():
        result = _evaluate(policy, [report])
        assert result.passed is False, case
        assert requirement.requirement_id in result.unsatisfied_requirements, case
