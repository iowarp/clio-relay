"""Tests for run reports: schema, recorder, durable writes and install identity."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from threading import Event, Thread
from typing import cast

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from clio_relay import artifact_identity_verification as artifact_identity_verification_module
from clio_relay import durable_validation_write as durable_validation_write_module
from clio_relay import install_identity as install_identity_module
from clio_relay import process_ancestry as process_ancestry_module
from clio_relay import uv_tool_receipt as uv_tool_receipt_module
from clio_relay import validation_limits as validation_limits_module
from clio_relay import validation_writer_lock as validation_writer_lock_module
from clio_relay.adapters.worker.session_lifecycle import (
    CleanupResource,
    RemoteSessionStateEvidence,
    SessionLifecycleReport,
)
from clio_relay.errors import ConfigurationError
from clio_relay.models import GatewaySession, GatewaySessionState
from clio_relay.run_report import (
    default_report_path,
    load_validation_report,
    render_validation_markdown,
    write_validation_report,
)
from clio_relay.run_report_models import (
    EvidenceOrigin,
    EvidenceReference,
    EvidenceTrust,
    InstallSource,
    InstallSourceKind,
    LiveValidationReport,
    SoftwareIdentity,
    TransportCleanupResourceEvidence,
    TransportProbeEvidence,
    ValidationCheck,
    ValidationResource,
    ValidationStatus,
    transport_probe_evidence_line,
)
from clio_relay.run_report_recorder import ValidationRecorder, new_live_validation_report
from clio_relay.service_runtime import ServiceRuntimeStartResult, ServiceRuntimeStopResult

_verify_running_artifact_identity = cast(
    Callable[..., bool],
    install_identity_module._verify_running_artifact_identity,  # pyright: ignore[reportPrivateUsage]
)
_uv_executable_identity = cast(
    Callable[[str | None], tuple[bool, str | None, str | None]],
    install_identity_module._uv_executable_identity,  # pyright: ignore[reportPrivateUsage]
)


def _timestamp() -> datetime:
    return datetime(2026, 7, 10, 18, 0, tzinfo=UTC)


def test_validation_writer_recovers_current_pending_and_prunes_stale_pending(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "reports"
    parent.mkdir(mode=0o700)
    target = parent / "current.json"

    def pending_path(name: str) -> Path:
        identity = hashlib.sha256(name.encode("utf-8")).hexdigest()[:32]
        return parent / f".clio-validation-{identity}.pending"

    stale = pending_path("stale.json")
    current = pending_path(target.name)
    stale.write_bytes(b"stale")
    current.write_bytes(b"partial")
    if os.name == "posix":
        stale.chmod(0o600)
        current.chmod(0o600)

    durable_validation_write_module.atomic_write_text(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        target,
        "exact",
    )

    assert target.read_text(encoding="utf-8") == "exact"
    assert not stale.exists()
    assert not current.exists()


def test_validation_writer_refuses_a_concurrent_writer_without_corruption(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    target = tmp_path / "report.json"
    entered = Event()
    release = Event()
    thread_errors: list[BaseException] = []
    original = durable_validation_write_module.atomic_write_text_locked

    def blocking_writer(
        path: Path,
        text: str,
        *,
        writer_lock: object,
    ) -> None:
        entered.set()
        if not release.wait(timeout=5):
            raise AssertionError("concurrent validation writer test timed out")
        original(path, text, writer_lock=writer_lock)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(
        durable_validation_write_module, "atomic_write_text_locked", blocking_writer
    )

    def first_writer() -> None:
        try:
            durable_validation_write_module.atomic_write_text(
                target,
                "first",
            )
        except BaseException as exc:  # pragma: no cover - asserted through the parent thread
            thread_errors.append(exc)

    thread = Thread(target=first_writer)
    thread.start()
    assert entered.wait(timeout=2)
    try:
        with pytest.raises(
            (OSError, ConfigurationError),
            match=(
                "another validation writer|could not open private Windows file|"
                "validation writer lock could not be acquired"
            ),
        ):
            durable_validation_write_module.atomic_write_text(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                target,
                "second",
            )
    finally:
        release.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert thread_errors == []
    assert target.read_text(encoding="utf-8") == "first"


@pytest.mark.parametrize("create_replacement", [False, True])
def test_posix_validation_writer_rejects_parent_swap_after_lock(
    tmp_path: Path,
    create_replacement: bool,
) -> None:
    if os.name != "posix":
        return
    parent = tmp_path / "reports"
    parent.mkdir(mode=0o700)
    displaced = tmp_path / "displaced-reports"
    writer_lock = validation_writer_lock_module.acquire_validation_writer_lock(parent)
    try:
        parent.rename(displaced)
        if create_replacement:
            parent.mkdir(mode=0o700)
        with pytest.raises(OSError, match="differs from its writer lock|disappeared"):
            durable_validation_write_module.atomic_write_text_locked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                parent / "report.json",
                "outside",
                writer_lock=writer_lock,
            )
    finally:
        validation_writer_lock_module.release_validation_writer_lock(writer_lock)

    assert parent.exists() is create_replacement
    if create_replacement:
        assert list(parent.iterdir()) == []
    assert not (displaced / "report.json").exists()
    assert {path.name for path in displaced.iterdir()} == {writer_lock.path.name}


def test_windows_validation_lock_guard_blocks_parent_swap_before_lock_open(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    if os.name != "nt":
        return
    parent = tmp_path / "reports"
    parent.mkdir(mode=0o700)
    displaced = tmp_path / "displaced-reports"
    rename_errors: list[OSError] = []
    original = validation_writer_lock_module.open_windows_validation_directory

    def try_swap_after_pin(*args: object, **kwargs: object) -> object:
        anchor = original(*args, **kwargs)  # pyright: ignore[reportArgumentType]
        try:
            parent.rename(displaced)
            parent.mkdir(mode=0o700)
        except OSError as exc:
            rename_errors.append(exc)
        return anchor

    monkeypatch.setattr(
        validation_writer_lock_module,
        "open_windows_validation_directory",
        try_swap_after_pin,
    )
    writer_lock = validation_writer_lock_module.acquire_validation_writer_lock(parent)
    try:
        assert rename_errors
        assert not displaced.exists()
        assert {path.name for path in parent.iterdir()} == {writer_lock.path.name}
    finally:
        validation_writer_lock_module.release_validation_writer_lock(writer_lock)


def test_windows_validation_lock_swap_before_guard_creation_leaves_replacement_empty(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    if os.name != "nt":
        return
    parent = tmp_path / "reports"
    parent.mkdir(mode=0o700)
    displaced = tmp_path / "displaced-before-guard"
    original = validation_writer_lock_module.acquire_private_configuration_windows_parent_guard

    def swap_then_guard(path: Path) -> tuple[Path, object]:
        path.rename(displaced)
        path.mkdir(mode=0o700)
        guard_path, handle = original(path)
        return guard_path, handle

    monkeypatch.setattr(
        validation_writer_lock_module,
        "acquire_private_configuration_windows_parent_guard",
        swap_then_guard,
    )
    with pytest.raises((OSError, ConfigurationError), match="changed|path"):
        validation_writer_lock_module.acquire_validation_writer_lock(parent)

    assert list(parent.iterdir()) == []
    assert list(displaced.iterdir()) == []


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

    monkeypatch.setattr(install_identity_module.metadata, "distribution", fake_distribution)
    monkeypatch.setattr(install_identity_module.resources, "files", fake_files)
    monkeypatch.setattr(
        install_identity_module,
        "_verify_running_artifact_identity",
        verified_artifact,
    )
    monkeypatch.setattr(
        install_identity_module,
        "_detect_launcher_receipt",
        unverified_launcher,
    )

    source = install_identity_module.detect_install_source(
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
        install_identity_module.metadata,
        "distribution",
        find_distribution,
    )
    monkeypatch.setattr(install_identity_module.resources, "files", package_files)
    monkeypatch.setattr(
        install_identity_module,
        "_detect_launcher_receipt",
        verified_launcher,
    )
    monkeypatch.setattr(
        artifact_identity_verification_module.urllib.request,
        "build_opener",
        build_opener,
    )
    monkeypatch.setattr(
        artifact_identity_verification_module,
        "url_host_resolves_publicly",
        publicly_resolved,
    )

    source = install_identity_module.detect_install_source(
        launcher="uv-tool",
        artifact_sha256=digest,
    )

    assert source.kind is InstallSourceKind.WHEEL
    assert source.reference == source_url
    assert source.artifact_identity_verified is True
    assert source.launcher_verified is True
    assert source.released_artifact is True

    rejected = install_identity_module.detect_install_source(
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
    assert artifact_identity_verification_module.is_official_release_wheel_url(url) is False  # pyright: ignore[reportPrivateUsage]
    assert (
        artifact_identity_verification_module._direct_wheel_bytes(  # pyright: ignore[reportPrivateUsage]
            {"url": url, "archive_info": {}}
        )
        is None
    )


def test_official_release_wheel_url_recognizes_github_unchanged() -> None:
    """clio-relay#206: the pre-existing GitHub release-asset channel is untouched."""
    url = (
        "https://github.com/iowarp/clio-relay/releases/download/v1.6.6/"
        "clio_relay-1.6.6-py3-none-any.whl"
    )
    assert artifact_identity_verification_module.is_official_release_wheel_url(url) is True  # pyright: ignore[reportPrivateUsage]


def test_official_release_wheel_url_recognizes_pypi_channel() -> None:
    """clio-relay#206: PyPI (files.pythonhosted.org) is an official wheel channel too.

    Observed live: a desktop ``uv tool install clio-relay==1.6.6`` resolved
    from PyPI produced a wheel with the identical sha256 digest as the GitHub
    release asset, but ``_is_official_release_wheel_url`` hardcoded
    ``github.com`` and rejected it outright.
    """
    url = (
        "https://files.pythonhosted.org/packages/4a/1e/"
        "cadc4d66aa11223344556677889900aabbccddeeff00112233445566778899/"
        "clio_relay-1.6.6-py3-none-any.whl"
    )
    assert artifact_identity_verification_module.is_official_release_wheel_url(url) is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    "url",
    [
        # wrong project name on an otherwise well-formed PyPI wheel path
        "https://files.pythonhosted.org/packages/4a/1e/"
        "cadc4d66aa11223344556677889900aabbccddeeff00112233445566778899/"
        "other-project-1.6.6-py3-none-any.whl",
        # credentials embedded in the host
        "https://user@files.pythonhosted.org/packages/4a/1e/"
        "cadc4d66aa11223344556677889900aabbccddeeff00112233445566778899/"
        "clio_relay-1.6.6-py3-none-any.whl",
        # query string smuggled onto an otherwise canonical URL
        "https://files.pythonhosted.org/packages/4a/1e/"
        "cadc4d66aa11223344556677889900aabbccddeeff00112233445566778899/"
        "clio_relay-1.6.6-py3-none-any.whl?redirect=https://127.0.0.1",
        # an arbitrary other host is never an official channel
        "https://pypi.example.org/packages/4a/1e/"
        "cadc4d66aa11223344556677889900aabbccddeeff00112233445566778899/"
        "clio_relay-1.6.6-py3-none-any.whl",
        # not the packages/ hash-addressed path shape at all
        "https://files.pythonhosted.org/clio_relay-1.6.6-py3-none-any.whl",
        # legacy simple-index style path is not the hash-addressed download path
        "https://files.pythonhosted.org/packages/source/c/clio-relay/"
        "clio_relay-1.6.6-py3-none-any.whl",
    ],
)
def test_official_release_wheel_url_rejects_noncanonical_pypi_sources(url: str) -> None:
    """clio-relay#206: recognition is narrow -- host and filename pattern both bind."""
    assert artifact_identity_verification_module.is_official_release_wheel_url(url) is False  # pyright: ignore[reportPrivateUsage]
    assert (
        artifact_identity_verification_module._direct_wheel_bytes(  # pyright: ignore[reportPrivateUsage]
            {"url": url, "archive_info": {}}
        )
        is None
    )


_FULL_COMMIT_SHA = "a" * 40


def test_vcs_commit_identity_accepts_exact_full_sha_pin() -> None:
    """clio-relay#206: a VCS install pinned to a full 40-hex commit sha is exact."""
    direct_url = {
        "url": "https://github.com/iowarp/clio-relay",
        "vcs_info": {
            "vcs": "git",
            "requested_revision": _FULL_COMMIT_SHA,
            "commit_id": _FULL_COMMIT_SHA.upper(),
        },
    }
    assert (
        artifact_identity_verification_module._vcs_commit_identity_verified(  # pyright: ignore[reportPrivateUsage]
            direct_url
        )
        == _FULL_COMMIT_SHA
    )


@pytest.mark.parametrize(
    "vcs_info",
    [
        # a branch reference is a moving target, not a fixed identity
        {"vcs": "git", "requested_revision": "main", "commit_id": _FULL_COMMIT_SHA},
        # a tag reference is also a moving target relative to a commit sha
        {"vcs": "git", "requested_revision": "v1.6.6", "commit_id": _FULL_COMMIT_SHA},
        # a short sha is not the full 40-hex identity anchor
        {"vcs": "git", "requested_revision": _FULL_COMMIT_SHA[:12], "commit_id": _FULL_COMMIT_SHA},
        # no requested_revision at all (unpinned VCS reference)
        {"vcs": "git", "commit_id": _FULL_COMMIT_SHA},
        # a non-git VCS is out of scope for this recognition
        {"vcs": "hg", "requested_revision": _FULL_COMMIT_SHA, "commit_id": _FULL_COMMIT_SHA},
        # requested_revision and the resolved commit_id disagree
        {"vcs": "git", "requested_revision": _FULL_COMMIT_SHA, "commit_id": "b" * 40},
    ],
)
def test_vcs_commit_identity_rejects_moving_or_inexact_references(
    vcs_info: dict[str, object],
) -> None:
    """clio-relay#206: branch/tag refs and short shas never count as pinned."""
    direct_url = {"url": "https://github.com/iowarp/clio-relay", "vcs_info": vcs_info}
    assert (
        artifact_identity_verification_module._vcs_commit_identity_verified(  # pyright: ignore[reportPrivateUsage]
            direct_url
        )
        is None
    )
    assert (
        artifact_identity_verification_module._vcs_commit_identity_verified(None)  # pyright: ignore[reportPrivateUsage]
        is None
    )


def test_infer_running_artifact_identity_records_exact_vcs_sha() -> None:
    """clio-relay#206: an exact-sha VCS pin is captured the same way a wheel digest is."""
    distribution = metadata.distribution("clio-relay")
    pinned_direct_url = {
        "url": "https://github.com/iowarp/clio-relay",
        "vcs_info": {
            "vcs": "git",
            "requested_revision": _FULL_COMMIT_SHA,
            "commit_id": _FULL_COMMIT_SHA,
        },
    }
    digest, verified = install_identity_module._infer_running_artifact_identity(  # pyright: ignore[reportPrivateUsage]
        distribution,
        detected_kind=InstallSourceKind.VCS,
        direct_url=pinned_direct_url,
        launcher="uv-tool",
    )
    assert digest == _FULL_COMMIT_SHA
    assert verified is True

    branch_direct_url = {
        "url": "https://github.com/iowarp/clio-relay",
        "vcs_info": {"vcs": "git", "requested_revision": "main", "commit_id": _FULL_COMMIT_SHA},
    }
    digest, verified = install_identity_module._infer_running_artifact_identity(  # pyright: ignore[reportPrivateUsage]
        distribution,
        detected_kind=InstallSourceKind.VCS,
        direct_url=branch_direct_url,
        launcher="uv-tool",
    )
    assert digest is None
    assert verified is False

    # a non uv-tool launcher never counts, matching the wheel path's own gate
    digest, verified = install_identity_module._infer_running_artifact_identity(  # pyright: ignore[reportPrivateUsage]
        distribution,
        detected_kind=InstallSourceKind.VCS,
        direct_url=pinned_direct_url,
        launcher="uvx",
    )
    assert digest is None
    assert verified is False


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

    monkeypatch.setattr(artifact_identity_verification_module.socket, "getaddrinfo", private_dns)

    assert artifact_identity_verification_module.is_official_release_wheel_url(source_url) is True  # pyright: ignore[reportPrivateUsage]
    assert artifact_identity_verification_module.url_host_resolves_publicly(source_url) is False  # pyright: ignore[reportPrivateUsage]
    handler = artifact_identity_verification_module._ReleaseWheelRedirectHandler()  # pyright: ignore[reportPrivateUsage]
    request = artifact_identity_verification_module.urllib.request.Request(source_url)
    with pytest.raises(artifact_identity_verification_module.urllib.error.HTTPError):
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

    identity = uv_tool_receipt_module.persistent_uv_tool_receipt_identity(  # pyright: ignore[reportPrivateUsage]
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
    rejected = uv_tool_receipt_module.persistent_uv_tool_receipt_identity(  # pyright: ignore[reportPrivateUsage]
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
        uv_tool_receipt_module.shutil,
        "which",
        find_executable,
    )
    monkeypatch.setattr(uv_tool_receipt_module.sys, "prefix", str(environment))
    monkeypatch.setattr(uv_tool_receipt_module.sys, "base_prefix", str(base_prefix))
    monkeypatch.setattr(uv_tool_receipt_module.sys, "executable", str(interpreter))
    monkeypatch.setattr(
        uv_tool_receipt_module,
        "uv_executable_identity",
        uv_identity,
    )
    monkeypatch.setattr(
        uv_tool_receipt_module,
        "uv_tool_dir",
        uv_tool_directory,
    )
    monkeypatch.setattr(
        uv_tool_receipt_module,
        "pyvenv_uv_version_marker",
        pyvenv_version,
    )
    monkeypatch.setattr(
        uv_tool_receipt_module,
        "installed_record_identity",
        record_identity,
    )

    verified, receipt = uv_tool_receipt_module.detect_persistent_uv_tool_receipt(
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

    monkeypatch.setattr(process_ancestry_module.subprocess, "run", fake_run)

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

    monkeypatch.setattr(process_ancestry_module.subprocess, "run", replacing_run)

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
    assert source["launcher_receipt"]["executable_in_process_environment"] is True
    assert source["launcher_receipt"]["executable_target_bound"] is True
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
        validation_limits_module.MAX_LAUNCHER_PROCESS_ANCESTORS
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
                "executable_target_bound": True,
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
    assert not list(tmp_path.glob(".clio-validation-*.pending"))


def test_atomic_report_writer_replaces_exactly_and_supports_new_nested_parent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nested" / "reports" / "report.json"
    durable_validation_write_module.atomic_write_text(path, "old\n")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    outside = tmp_path / "old-hardlink.json"
    os.link(path, outside)

    durable_validation_write_module.atomic_write_text(path, "new\n")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert path.read_text(encoding="utf-8") == "new\n"
    assert outside.read_text(encoding="utf-8") == "old\n"
    assert path.stat().st_nlink == 1
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700


def test_atomic_report_writer_recovers_one_deterministic_partial_pending(
    tmp_path: Path,
) -> None:
    path = tmp_path / "report.json"
    pending_digest = hashlib.sha256(path.name.encode("utf-8")).hexdigest()[:32]
    pending = tmp_path / f".clio-validation-{pending_digest}.pending"
    pending.write_bytes(b"partial")
    if os.name == "posix":
        pending.chmod(0o600)

    durable_validation_write_module.atomic_write_text(path, "complete\n")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert path.read_text(encoding="utf-8") == "complete\n"
    assert not pending.exists()
    assert not list(tmp_path.glob(".clio-validation-*.pending"))


def test_atomic_report_writer_refuses_concurrent_same_parent_writer(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    entered = Event()
    release = Event()
    failures: list[BaseException] = []
    original = durable_validation_write_module.atomic_write_text_locked

    def blocked_writer(*args: object, **kwargs: object) -> None:
        entered.set()
        if not release.wait(10):
            raise TimeoutError("test writer was not released")
        original(*args, **kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(durable_validation_write_module, "atomic_write_text_locked", blocked_writer)

    def first_writer() -> None:
        try:
            durable_validation_write_module.atomic_write_text(tmp_path / "first.json", "first\n")  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        except BaseException as exc:
            failures.append(exc)

    thread = Thread(target=first_writer)
    thread.start()
    assert entered.wait(10)
    try:
        with pytest.raises((OSError, ConfigurationError), match="writer|lock"):
            durable_validation_write_module.atomic_write_text(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                tmp_path / "second.json",
                "second\n",
            )
    finally:
        release.set()
        thread.join(timeout=10)
    assert not thread.is_alive()
    assert failures == []
    assert (tmp_path / "first.json").read_text(encoding="utf-8") == "first\n"
    assert not (tmp_path / "second.json").exists()


def test_validation_directory_creation_is_bound_to_pinned_ancestor(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    trusted = tmp_path / "trusted"
    outside = tmp_path / "outside"
    displaced = tmp_path / "displaced"
    trusted.mkdir(mode=0o700)
    outside.mkdir(mode=0o700)
    requested = trusted / "a" / "b"

    if os.name == "posix":
        original = durable_validation_write_module.create_posix_validation_directory_child
        swapped = False

        def swap_then_create(parent_fd: int, child_name: str) -> int:
            nonlocal swapped
            if not swapped:
                swapped = True
                trusted.rename(displaced)
                trusted.symlink_to(outside, target_is_directory=True)
            return original(parent_fd, child_name)

        monkeypatch.setattr(
            durable_validation_write_module,
            "create_posix_validation_directory_child",
            swap_then_create,
        )
        with pytest.raises(OSError, match="changed"):
            durable_validation_write_module.durably_ensure_validation_directory(requested)
        assert not (outside / "a").exists()
        assert (displaced / "a").is_dir()
        return

    original_windows = durable_validation_write_module.create_windows_validation_directory_child
    rename_errors: list[OSError] = []

    def try_swap_then_create(parent: object, child_name: str) -> object:
        try:
            trusted.rename(displaced)
        except OSError as exc:
            rename_errors.append(exc)
        return original_windows(parent, child_name)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(
        durable_validation_write_module,
        "create_windows_validation_directory_child",
        try_swap_then_create,
    )
    with pytest.raises(OSError, match="changed|find the file"):
        durable_validation_write_module.durably_ensure_validation_directory(requested)
    assert not rename_errors
    assert displaced.is_dir()
    assert not (outside / "a").exists()


def test_report_writer_rejects_symlink_or_junction_parent(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    linked_parent = tmp_path / "linked"
    real_parent.mkdir()
    if os.name == "nt":
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(linked_parent), str(real_parent)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
    else:
        linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(OSError, match="symlink|reparse|not a real directory"):
        durable_validation_write_module.atomic_write_text(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            linked_parent / "report.json",
            "blocked\n",
        )
    assert not (real_parent / "report.json").exists()


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


def test_gateway_start_and_stop_reports_record_their_checks() -> None:
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
