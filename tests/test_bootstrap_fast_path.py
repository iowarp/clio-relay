"""Focused acceptance contracts for payload-free repeated cluster bootstrap."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn, cast

import pytest
from typer.testing import CliRunner

import clio_relay.bootstrap as bootstrap
import clio_relay.cli as cli
from clio_relay import __version__
from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    BootstrapInspection,
    BootstrapReadinessEvidence,
    BootstrapTransactionJournal,
    BootstrapTransactionState,
    JarvisStateEvidence,
    make_bootstrap_receipt,
)
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.errors import ConfigurationError, RelayError


def _verify_persistent_receipt(**_kwargs: object) -> None:
    """Model a successfully re-read persistent receipt."""


def _which(executable: str) -> str:
    """Return a deterministic executable resolution for bootstrap tests."""

    return executable


def _run_posix_embedded_driver(
    driver: str,
    *sources: str,
    timeout_seconds: float = 60,
) -> subprocess.CompletedProcess[str]:
    """Run one embedded bootstrap security probe on Linux or the local WSL runtime."""
    executable = ["wsl.exe", "-e", "python3"] if os.name == "nt" else [sys.executable]
    encoded = [base64.b64encode(source.encode()).decode("ascii") for source in sources]
    launcher = (
        "import json,sys; payload=json.load(sys.stdin); "
        "sys.argv=['embedded-driver',*payload['arguments']]; "
        "exec(compile(payload['driver'],'<embedded-driver>','exec'))"
    )
    return subprocess.run(
        [*executable, "-I", "-c", launcher],
        input=json.dumps({"driver": driver, "arguments": encoded}),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def test_receipt_classifier_accepts_stable_generation_symlink() -> None:
    """Warm bootstraps classify the supported stable receipt link without following others."""
    driver = r"""
import base64
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

source = base64.b64decode(sys.argv[1]).decode()
with tempfile.TemporaryDirectory() as value:
    home = Path(value)
    relay = home / ".local/share/clio-relay"
    generation = relay / "generations" / ("a" * 64)
    generation.mkdir(parents=True)
    receipt = generation / "install-receipt.json"
    receipt.write_text(
        json.dumps({"component_artifacts": {"clio-relay": {"persistent_tool": {}}}}),
        encoding="utf-8",
    )
    (relay / "current").symlink_to(generation, target_is_directory=True)
    stable = relay / "install-receipt.json"
    stable.symlink_to(relay / "current/install-receipt.json")
    environment = {**os.environ, "HOME": str(home)}
    result = subprocess.run(
        [sys.executable, "-I", "-", str(stable)],
        input=source,
        text=True,
        capture_output=True,
        env=environment,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stdout + result.stderr)
    if result.stdout.strip() != "current":
        raise SystemExit("stable generation receipt was not classified as current")
print("stable-receipt-ok")
"""
    result = _run_posix_embedded_driver(
        driver,
        bootstrap._BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "stable-receipt-ok"


def test_preparing_root_and_uv_copy_recover_without_following_links() -> None:
    """Power-loss scratch is reclaimed fd-relatively and uv executes from a private copy."""
    driver = r"""
import base64
import hashlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path

preparing = base64.b64decode(sys.argv[1]).decode()
copy_uv = base64.b64decode(sys.argv[2]).decode()
with tempfile.TemporaryDirectory() as value:
    workspace = Path(value)
    parent = workspace / "preparing"
    parent.mkdir(mode=0o700)
    active = parent / "active"
    quarantine = parent / ".active.quarantine"
    sentinel = workspace / "outside-sentinel"
    sentinel.write_text("preserve", encoding="utf-8")
    for stale in (active, quarantine):
        (stale / "nested").mkdir(parents=True, mode=0o700)
        stale.chmod(0o700)
        (stale / "nested/outbound").symlink_to(sentinel)
    subprocess.run(
        [sys.executable, "-I", "-c", preparing, str(parent), str(active), "prepare"],
        check=True,
    )
    if not active.is_dir() or list(active.iterdir()) or not sentinel.is_file():
        raise SystemExit("fixed scratch preparation did not safely reclaim stale state")
    if quarantine.exists() or quarantine.is_symlink():
        raise SystemExit("scratch quarantine leaked after reclamation")

    source = workspace / "uv-source"
    payload = b"#!/bin/sh\nexit 0\n"
    source.write_bytes(payload)
    source.chmod(0o500)
    digest = hashlib.sha256(payload).hexdigest()
    copied = subprocess.run(
        [sys.executable, "-I", "-c", copy_uv, str(source), str(active), digest],
        check=True,
        capture_output=True,
        text=True,
    )
    private_uv = Path(copied.stdout.strip())
    if private_uv != active / "pinned-uv":
        raise SystemExit("candidate uv copy returned the wrong private path")
    if private_uv.stat().st_ino == source.stat().st_ino:
        raise SystemExit("candidate uv copy reused the mutable source inode")
    if hashlib.sha256(private_uv.read_bytes()).hexdigest() != digest:
        raise SystemExit("candidate uv private copy digest changed")
    if private_uv.stat().st_mode & 0o777 != 0o500:
        raise SystemExit("candidate uv private copy mode is not sealed")

    subprocess.run(
        [sys.executable, "-I", "-c", preparing, str(parent), str(active), "cleanup"],
        check=True,
    )
    if active.exists() or quarantine.exists() or not sentinel.is_file():
        raise SystemExit("scratch cleanup did not preserve its outbound target")

    for stale in (active, quarantine):
        (stale / "nested").mkdir(parents=True, mode=0o700)
        stale.chmod(0o700)
    subprocess.run(
        [sys.executable, "-I", "-c", preparing, str(parent), str(active), "prepare"],
        check=True,
    )
    rejected = subprocess.run(
        [sys.executable, "-I", "-c", copy_uv, str(source), str(active), "0" * 64],
        check=False,
        capture_output=True,
        text=True,
    )
    if rejected.returncode == 0 or (active / "pinned-uv").exists():
        raise SystemExit("a digest-mismatched uv copy was retained")
print("scratch-and-uv-ok")
"""
    result = _run_posix_embedded_driver(
        driver,
        bootstrap._BOOTSTRAP_PREPARING_ROOT_SOURCE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        bootstrap._BOOTSTRAP_PINNED_UV_COPY_SOURCE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "scratch-and-uv-ok"


def test_fd_bound_candidate_verifier_rejects_swapped_wheel_install() -> None:
    """Installed relay bytes must match the wheel fd held across uv installation."""
    driver = r"""
import base64
import csv
import hashlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

verifier = base64.b64decode(sys.argv[1]).decode()
uv = shutil.which("uv") or str(Path.home() / ".local/bin/uv")
with tempfile.TemporaryDirectory() as value:
    workspace = Path(value)
    built = workspace / "built"
    subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(built)],
        check=True,
        capture_output=True,
    )
    original = next(built.glob("clio_relay-*.whl"))
    tampered = workspace / original.name
    with zipfile.ZipFile(original) as archive:
        entries = [(item, archive.read(item.filename)) for item in archive.infolist()]
    record_name = next(
        item.filename
        for item, _payload in entries
        if item.filename.endswith(".dist-info/RECORD")
    )
    target_name = "clio_relay/__init__.py"
    payloads = {item.filename: payload for item, payload in entries}
    payloads[target_name] += b"\nSWAPPED_WHEEL_SENTINEL = True\n"
    rows = list(csv.reader(io.StringIO(payloads[record_name].decode()), strict=True))
    for row in rows:
        if row[0] == target_name:
            digest = hashlib.sha256(payloads[target_name]).digest()
            row[1] = "sha256=" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
            row[2] = str(len(payloads[target_name]))
    record_stream = io.StringIO(newline="")
    csv.writer(record_stream, lineterminator="\n").writerows(rows)
    payloads[record_name] = record_stream.getvalue().encode()
    with zipfile.ZipFile(tampered, "w") as archive:
        for item, _payload in entries:
            archive.writestr(item, payloads[item.filename])

    tool_directory = workspace / "tools"
    tool_bin_directory = workspace / "bin"
    cache_directory = workspace / "cache"
    python_directory = Path.home() / ".local/share/clio-relay/uv-python"
    environment = {
        **os.environ,
        "UV_TOOL_DIR": str(tool_directory),
        "UV_TOOL_BIN_DIR": str(tool_bin_directory),
        "UV_CACHE_DIR": str(cache_directory),
        "UV_PYTHON_INSTALL_DIR": str(python_directory),
        "UV_PYTHON_DOWNLOADS": "never",
    }
    subprocess.run(
        [
            uv,
            "tool",
            "install",
            "--force",
            "--python",
            "3.12",
            "--no-config",
            "--default-index",
            "https://pypi.org/simple",
            str(tampered),
        ],
        check=True,
        capture_output=True,
        env=environment,
    )
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            verifier,
            "verify-installed",
            uv,
            hashlib.sha256(Path(uv).read_bytes()).hexdigest(),
            str(original),
            hashlib.sha256(original.read_bytes()).hexdigest(),
            str(tool_directory),
            str(tool_bin_directory),
            str(cache_directory),
            str(python_directory),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        raise SystemExit("swapped wheel installation passed pinned-fd verification")
    if "installed candidate differs from the pinned wheel fd" not in result.stderr:
        raise SystemExit(result.stdout + result.stderr)
print("swapped-wheel-rejected")
"""
    result = _run_posix_embedded_driver(
        driver,
        bootstrap._BOOTSTRAP_CANDIDATE_UV_INSTALL_SOURCE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "swapped-wheel-rejected"


def test_candidate_verifier_accepts_pinned_uv_metadata_subset_only() -> None:
    """Pinned uv metadata may vary by build path, while unknown members fail closed."""
    driver = r"""
import base64
import csv
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

verifier = base64.b64decode(sys.argv[1]).decode()
uv = shutil.which("uv") or str(Path.home() / ".local/bin/uv")
with tempfile.TemporaryDirectory() as value:
    workspace = Path(value)
    built = workspace / "built"
    subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(built)],
        check=True,
        capture_output=True,
    )
    wheel = next(built.glob("clio_relay-*.whl"))
    tool_directory = workspace / "tools"
    tool_bin_directory = workspace / "bin"
    cache_directory = workspace / "cache"
    python_directory = Path.home() / ".local/share/clio-relay/uv-python"
    arguments = [
        sys.executable,
        "-I",
        "-c",
        verifier,
        "install-and-verify",
        uv,
        hashlib.sha256(Path(uv).read_bytes()).hexdigest(),
        str(wheel),
        hashlib.sha256(wheel.read_bytes()).hexdigest(),
        str(tool_directory),
        str(tool_bin_directory),
        str(cache_directory),
        str(python_directory),
    ]
    accepted = subprocess.run(arguments, check=False, capture_output=True, text=True)
    if accepted.returncode != 0:
        raise SystemExit(accepted.stdout + accepted.stderr)

    site_packages = next((tool_directory / "clio-relay").glob("lib/python*/site-packages"))
    record = next(site_packages.glob("clio_relay-*.dist-info/RECORD"))
    rows = list(csv.reader(record.read_text(encoding="utf-8").splitlines(), strict=True))
    with zipfile.ZipFile(wheel) as archive:
        wheel_names = {item.filename for item in archive.infolist() if not item.is_dir()}
    launcher = os.path.relpath(
        tool_directory / "clio-relay/bin/clio-relay", site_packages
    ).replace(os.sep, "/")
    installed_names = {row[0] for row in rows if len(row) == 3}
    dist_info = record.parent.relative_to(site_packages).as_posix()
    generated = {
        launcher,
        *(f"{dist_info}/{name}" for name in (
            "INSTALLER",
            "REQUESTED",
            "direct_url.json",
            "uv_build.json",
            "uv_cache.json",
        )),
    }
    if (
        len(installed_names) != len(rows)
        or not wheel_names.issubset(installed_names)
        or not {
            launcher,
            f"{dist_info}/INSTALLER",
            f"{dist_info}/REQUESTED",
            f"{dist_info}/direct_url.json",
        }.issubset(installed_names)
        or not installed_names.issubset(wheel_names | generated)
    ):
        raise SystemExit("candidate install exposed an invalid uv metadata set")

    uv_build = record.parent / "uv_build.json"
    if not uv_build.exists():
        payload = b'{"source":"focused-test"}'
        uv_build.write_bytes(payload)
        digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()
        rows.append([
            uv_build.relative_to(site_packages).as_posix(),
            "sha256=" + digest,
            str(len(payload)),
        ])
        with record.open("w", encoding="utf-8", newline="") as stream:
            csv.writer(stream, lineterminator="\n").writerows(rows)
    arguments[4] = "verify-installed"
    accepted_with_build_metadata = subprocess.run(
        arguments, check=False, capture_output=True, text=True
    )
    if accepted_with_build_metadata.returncode != 0:
        raise SystemExit(
            accepted_with_build_metadata.stdout + accepted_with_build_metadata.stderr
        )

    uv_build_payload = uv_build.read_bytes()
    uv_build.write_bytes(uv_build_payload + b"tampered")
    rejected_tamper = subprocess.run(arguments, check=False, capture_output=True, text=True)
    if (
        rejected_tamper.returncode == 0
        or "generated member differs from its RECORD identity" not in rejected_tamper.stderr
    ):
        raise SystemExit(rejected_tamper.stdout + rejected_tamper.stderr)
    uv_build.write_bytes(uv_build_payload)

    unexpected = record.parent / "unexpected.json"
    unexpected.write_text("{}", encoding="utf-8")
    rows.append([unexpected.relative_to(site_packages).as_posix(), "", ""])
    with record.open("w", encoding="utf-8", newline="") as stream:
        csv.writer(stream, lineterminator="\n").writerows(rows)
    rejected = subprocess.run(arguments, check=False, capture_output=True, text=True)
    if rejected.returncode == 0 or "contains unpinned members" not in rejected.stderr:
        raise SystemExit(rejected.stdout + rejected.stderr)
print("bounded-uv-metadata-ok")
"""
    result = _run_posix_embedded_driver(
        driver,
        bootstrap._BOOTSTRAP_CANDIDATE_UV_INSTALL_SOURCE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "bounded-uv-metadata-ok"


def test_candidate_coordinator_rejects_provider_path_swap_after_open() -> None:
    """A provider pathname replacement cannot execute after its coordinator opens it."""
    driver = r"""
import base64
import ctypes
import hashlib
import os
import select
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

coordinator = base64.b64decode(sys.argv[1]).decode()
uv = shutil.which("uv") or str(Path.home() / ".local/bin/uv")
with tempfile.TemporaryDirectory() as value:
    workspace = Path(value)
    built = workspace / "built"
    subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(built)],
        check=True,
        capture_output=True,
    )
    wheel = next(built.glob("clio_relay-*.whl"))
    tool_directory = workspace / "tools"
    tool_bin_directory = workspace / "bin"
    cache_directory = workspace / "cache"
    python_directory = Path.home() / ".local/share/clio-relay/uv-python"
    environment = {
        **os.environ,
        "UV_TOOL_DIR": str(tool_directory),
        "UV_TOOL_BIN_DIR": str(tool_bin_directory),
        "UV_CACHE_DIR": str(cache_directory),
        "UV_PYTHON_INSTALL_DIR": str(python_directory),
        "UV_PYTHON_DOWNLOADS": "never",
    }
    subprocess.run(
        [
            uv,
            "tool",
            "install",
            "--force",
            "--python",
            "3.12",
            "--no-config",
            "--default-index",
            "https://pypi.org/simple",
            str(wheel),
        ],
        check=True,
        capture_output=True,
        env=environment,
    )
    provider = tool_directory / "clio-relay/bin/python"
    provider_target = provider.resolve(strict=True)
    provider_sha256 = hashlib.sha256(provider_target.read_bytes()).hexdigest()
    uv_sha256 = hashlib.sha256(Path(uv).read_bytes()).hexdigest()
    wheel_sha256 = hashlib.sha256(wheel.read_bytes()).hexdigest()
    probe_source = f'''
import os
from importlib.metadata import version
from pathlib import Path
from clio_relay.installation import probe_persistent_uv_tool_identity
identity = probe_persistent_uv_tool_identity(
    uv_executable={uv!r},
    tool_executable={str(tool_bin_directory / 'clio-relay')!r},
    provider_interpreter=os.environ['BOOTSTRAP_PLAN_PROVIDER'],
    source_artifact=Path({str(wheel)!r}),
    distribution='clio-relay',
    distribution_version=version('clio-relay'),
    entry_point='clio-relay',
    tool_directory={str(tool_directory)!r},
    tool_bin_directory={str(tool_bin_directory)!r},
    expected_uv_executable_sha256={uv_sha256!r},
    expected_provider_interpreter_sha256={provider_sha256!r},
)
print('in-process-provider=' + identity.provider_interpreter_sha256)
'''
    positive = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            coordinator,
            "verify-installed-and-exec",
            uv,
            uv_sha256,
            str(wheel),
            wheel_sha256,
            str(tool_directory),
            str(tool_bin_directory),
            str(cache_directory),
            str(python_directory),
            provider_sha256,
            "-I",
            "-",
        ],
        input=probe_source,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if positive.returncode != 0 or positive.stdout.strip() != (
        "in-process-provider=" + provider_sha256
    ):
        raise SystemExit(positive.stdout + positive.stderr)
    executed_a = workspace / "provider-a-executed"
    executed_b = workspace / "provider-b-executed"
    libc = ctypes.CDLL(None, use_errno=True)
    inotify_descriptor = libc.inotify_init1(os.O_CLOEXEC)
    if inotify_descriptor < 0:
        raise SystemExit("could not initialize the provider-open observer")
    watch = libc.inotify_add_watch(
        inotify_descriptor,
        os.fsencode(provider_target),
        0x00000020,
    )
    if watch < 0:
        os.close(inotify_descriptor)
        raise SystemExit("could not observe the provider target opening")
    process = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            coordinator,
            "verify-installed-and-exec",
            uv,
            uv_sha256,
            str(wheel),
            wheel_sha256,
            str(tool_directory),
            str(tool_bin_directory),
            str(cache_directory),
            str(python_directory),
            provider_sha256,
            "-I",
            "-",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    readable, _writable, _exceptional = select.select(
        [inotify_descriptor],
        [],
        [],
        10,
    )
    if not readable:
        process.kill()
        process.wait(timeout=5)
        os.close(inotify_descriptor)
        raise SystemExit("provider swap test did not observe the coordinator opening it")
    os.read(inotify_descriptor, 4096)
    os.close(inotify_descriptor)
    provider.unlink()
    provider.write_text(
        "#!/bin/sh\nprintf hostile > " + str(executed_b) + "\nexit 91\n",
        encoding="utf-8",
    )
    provider.chmod(0o700)
    stdout, stderr = process.communicate(
        input=(
            "from pathlib import Path\n"
            f"Path({str(executed_a)!r}).write_text('trusted', encoding='utf-8')\n"
        ),
        timeout=30,
    )
    if process.returncode == 0:
        if not executed_a.exists():
            raise SystemExit("the sealed provider did not execute the trusted payload")
    elif not any(
        message in stderr
        for message in (
            "candidate provider path changed while it was pinned",
            "candidate provider changed after its planning pin",
        )
    ):
        raise SystemExit(stdout + stderr)
    if executed_b.exists():
        raise SystemExit("the replacement provider pathname was executed")
print("provider-swap-rejected")
"""
    result = _run_posix_embedded_driver(
        driver,
        bootstrap._BOOTSTRAP_CANDIDATE_UV_INSTALL_SOURCE,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        timeout_seconds=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "provider-swap-rejected"


def test_staged_provider_exec_is_hash_bound_sealed_and_venv_aware(tmp_path: Path) -> None:
    """The staged provider is executed from sealed bytes with lexical venv semantics."""
    if sys.platform != "linux":
        result = subprocess.run(
            [
                "wsl.exe",
                "-e",
                "bash",
                "-lc",
                (
                    "export UV_PROJECT_ENVIRONMENT="
                    "/tmp/clio-relay-staged-provider-test-venv; "
                    "uv run pytest -q "
                    "tests/test_bootstrap_fast_path.py::"
                    "test_staged_provider_exec_is_hash_bound_sealed_and_venv_aware"
                ),
            ],
            cwd=Path(__file__).parents[1],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return
    generation = tmp_path / "generation"
    provider_root = generation / "tools/clio-relay"
    subprocess.run(
        [sys.executable, "-m", "venv", str(provider_root)],
        check=True,
        capture_output=True,
        text=True,
    )
    provider = provider_root / "bin/python"
    relay = generation / "bin/clio-relay"
    relay.parent.mkdir(parents=True)
    relay_payload = f"#!{provider}\n# staged relay launcher\n".encode()
    relay.write_bytes(relay_payload)
    relay.chmod(0o755)
    provider_sha256 = hashlib.sha256(provider.resolve(strict=True).read_bytes()).hexdigest()
    receipt = {
        "component_artifacts": {
            "clio-relay": {
                "runtime_interpreters": {"provider": str(provider)},
                "runtime_executables": {"clio-relay": str(relay)},
                "persistent_tool": {
                    "provider_interpreter": str(provider),
                    "provider_interpreter_sha256": provider_sha256,
                    "tool_executable": str(relay),
                    "tool_executable_sha256": hashlib.sha256(relay_payload).hexdigest(),
                },
            }
        }
    }
    receipt_path = generation / "install-receipt.json"
    receipt_payload = json.dumps(receipt, sort_keys=True).encode()
    receipt_path.write_bytes(receipt_payload)
    manifest = {
        "install_receipt": str(receipt_path),
        "install_receipt_sha256": hashlib.sha256(receipt_payload).hexdigest(),
    }
    manifest_payload = json.dumps(manifest, sort_keys=True).encode()
    (generation / "manifest.json").write_bytes(manifest_payload)
    expected_manifest_sha256 = hashlib.sha256(manifest_payload).hexdigest()
    poisoned_python_path = tmp_path / "poisoned-python-path"
    poisoned_python_path.mkdir()
    (poisoned_python_path / "sitecustomize.py").write_text(
        "raise SystemExit('poisoned PYTHONPATH was imported')\n",
        encoding="utf-8",
    )
    compiler = shutil.which("cc")
    assert compiler is not None, "the sealed-provider test requires a C compiler"
    preload_library = tmp_path / "preload-sentinel.so"
    subprocess.run(
        [compiler, "-shared", "-fPIC", "-x", "c", "-o", str(preload_library), "-"],
        input=(
            "#include <fcntl.h>\n"
            "#include <stdlib.h>\n"
            "#include <unistd.h>\n"
            "__attribute__((constructor)) static void mark_loaded(void) {\n"
            '  const char *marker = getenv("CLIO_PRELOAD_MARKER");\n'
            "  if (marker == NULL) return;\n"
            "  int descriptor = open(marker, O_WRONLY | O_CREAT | O_APPEND, 0600);\n"
            "  if (descriptor < 0) return;\n"
            '  (void)write(descriptor, "loaded\\n", 7);\n'
            "  (void)close(descriptor);\n"
            "}\n"
        ),
        check=True,
        capture_output=True,
        text=True,
    )
    preload_marker = tmp_path / "preload-sentinel.marker"
    provider_environment = os.environ.copy()
    provider_environment.update(
        {
            "CLIO_PRELOAD_MARKER": str(preload_marker),
            "LD_PRELOAD": str(preload_library),
            "LD_LIBRARY_PATH": "/clio-relay/poisoned-library-path",
            "PYTHONHOME": "/clio-relay/poisoned-python-home",
            "PYTHONPATH": str(poisoned_python_path),
        }
    )
    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                "set -euo pipefail\n"
                ': > "$CLIO_PRELOAD_MARKER"\n'
                f"{bootstrap._STAGED_PROVIDER_ENVIRONMENT_SANITIZER}\n"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                'exec "$@"\n'
            ),
            "bootstrap-provider-test",
            sys.executable,
            "-I",
            "-c",
            bootstrap._STAGED_PROVIDER_EXEC_PROGRAM,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            str(generation),
            expected_manifest_sha256,
            "-c",
            (
                "import os, sys; "
                f"assert sys.prefix == {str(provider_root)!r}; "
                f"assert sys.executable == {str(provider)!r}; "
                "assert sys.flags.isolated == 1; "
                "assert not any(name.startswith('LD_') for name in os.environ); "
                "assert not any(name.startswith('PYTHON') for name in os.environ); "
                "print('sealed-provider-ok')"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=provider_environment,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "sealed-provider-ok"
    assert preload_marker.read_bytes() == b""


def test_exact_remote_bootstrap_never_reads_or_builds_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A released-wheel no-op ends after remote evidence and receipt verification."""
    digest = "a" * 64
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"
    source_root = tmp_path / "poison-source"
    identity = bootstrap.bootstrap_relay_identity(
        source_root=source_root,
        relay_wheel=wheel,
        relay_artifact_sha256=digest,
    )
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=identity,
        cluster="ares",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    jarvis_state = JarvisStateEvidence(
        initialized=True,
        root="/home/operator/.ppi-jarvis",
        roots={
            "config_dir": "/operator/jarvis/config",
            "private_dir": "/operator/jarvis/private",
            "shared_dir": "/operator/jarvis/shared",
        },
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=True,
    )
    inspection = BootstrapInspection(
        exact_match=True,
        desired_fingerprint=desired.fingerprint,
        install_receipt_sha256="e" * 64,
        active_generation=desired.fingerprint,
        current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
        jarvis_state=jarvis_state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=True,
            service_was_enabled=True,
            queue_ready=True,
            queue={
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            },
            worker_ready=True,
            worker={"running": True},
        ),
    )
    receipt = make_bootstrap_receipt(
        invocation_id="bootstrap_test",
        desired=desired,
        outcome="noop_verified",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=None,
        previous_generation=desired.fingerprint,
        active_generation=desired.fingerprint,
    )
    observed_desired: list[str] = []

    def preflight(**kwargs: object) -> bootstrap.BootstrapPreflightResult:
        requested = kwargs["desired"]
        assert hasattr(requested, "fingerprint")
        observed_desired.append(requested.fingerprint)  # type: ignore[attr-defined]
        return bootstrap.BootstrapPreflightResult(
            action="exact",
            receipt=receipt,
            lines=["bootstrap_preflight_json={}"],
        )

    def poison(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("the exact no-op touched bootstrap payload code")

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(
        bootstrap,
        "_verify_persistent_bootstrap_receipt",
        _verify_persistent_receipt,
    )
    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)
    monkeypatch.setattr(bootstrap, "_validate_relay_bootstrap_wheel", poison)
    monkeypatch.setattr(bootstrap.shutil, "which", _which)
    monkeypatch.setattr(bootstrap, "uuid4", lambda: type("Uuid", (), {"hex": "test"})())

    lines = bootstrap.bootstrap_cluster_over_ssh(
        bootstrap_profile="linux-user",
        ssh_host="ares",
        source_root=source_root,
        cluster="ares",
        relay_wheel=wheel,
        relay_artifact_sha256=digest,
        jarvis_resource_graph_profile="ares",
    )

    assert observed_desired == [desired.fingerprint]
    assert any(line.startswith("bootstrap_receipt_json=") for line in lines)
    assert receipt["jarvis_commands"] == {"count": 0, "argv": []}
    operations = receipt["operations"]
    assert isinstance(operations, dict)
    assert operations["payload_transfer_count"] == 0
    assert operations["payload_transfer_bytes"] == 0


def test_legacy_preflight_classifies_receipt_before_invoking_old_relay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A receipt without persistent-tool proof forces the candidate payload path."""
    identity = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "release",
        relay_wheel=None,
        relay_artifact_sha256="a" * 64,
    )
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=identity,
        cluster="ares",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    observed: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            "bootstrap_preflight_unsupported=legacy_relay_provider\n",
            "",
        )

    monkeypatch.setattr(bootstrap, "_run", run)
    result = bootstrap._bootstrap_preflight_over_ssh(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ssh_host="ares",
        invocation_id="bootstrap_test",
        desired=desired,
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        repair=False,
        timeout_seconds=30,
    )

    assert result.action == "payload_required"
    assert len(observed) == 1
    remote_script = observed[0][-1]
    classifier = remote_script.index('relay.get("persistent_tool")')
    old_relay = remote_script.index('"$HOME/.local/bin/clio-relay" bootstrap-inspect')
    assert classifier < old_relay
    assert "bootstrap_preflight_unsupported=legacy_relay_provider" in remote_script
    assert 'if ! BOOTSTRAP_RELAY_RECEIPT_CLASS="$(' in remote_script
    assert "python3 -I -" in remote_script
    sanitizer = remote_script.index("while IFS= read -r bootstrap_environment_name")
    assert 'LD_*|PYTHON*|BASH_ENV|ENV) unset "$bootstrap_environment_name"' in remote_script
    assert sanitizer < remote_script.index("python3 -I -")
    assert sanitizer < remote_script.index("timeout --signal=TERM")
    assert "env -u PYTHONPATH" not in remote_script
    shell = (
        ["wsl.exe", "-e", "bash", "-lc", "tr -d '\r' | bash -n"]
        if os.name == "nt"
        else ["bash", "-n"]
    )
    syntax = subprocess.run(
        shell,
        input=remote_script,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert syntax.returncode == 0, syntax.stderr
    execution_driver = r"""
import base64
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

script = base64.b64decode(sys.argv[1]).decode()
script = (
    "export LD_AUDIT=/definitely/missing/LD_AUDIT_SENTINEL.so\n"
    "export PYTHONWARNINGS=error::DefinitelyMissingWarning\n"
    + script
)
with tempfile.TemporaryDirectory() as value:
    home = Path(value)
    relay = home / ".local/bin/clio-relay"
    relay.parent.mkdir(parents=True)
    marker = home / "legacy-relay-executed"
    relay.write_text(
        "#!/bin/sh\nprintf invoked > " + str(marker) + "\nexit 99\n",
        encoding="utf-8",
    )
    relay.chmod(0o700)
    receipt = home / ".local/share/clio-relay/install-receipt.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(
        json.dumps({"component_artifacts": {"clio-relay": {}}}),
        encoding="utf-8",
    )
    environment = {**os.environ, "HOME": str(home)}
    result = subprocess.run(
        ["bash"],
        input=script,
        text=True,
        capture_output=True,
        env=environment,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stdout + result.stderr)
    if marker.exists():
        raise SystemExit("legacy relay provider executed before candidate staging")
    if "LD_AUDIT_SENTINEL" in result.stderr or "Invalid -W option" in result.stderr:
        raise SystemExit("preflight leaked hostile loader or Python environment")
    if result.stdout.strip() != "bootstrap_preflight_unsupported=legacy_relay_provider":
        raise SystemExit("legacy relay receipt did not force the candidate payload path")
print("legacy-preflight-ok")
"""
    execution = _run_posix_embedded_driver(execution_driver, remote_script)
    assert execution.returncode == 0, execution.stdout + execution.stderr
    assert execution.stdout.strip() == "legacy-preflight-ok"


def test_public_cluster_bootstrap_noop_never_touches_nonexistent_wheel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real public command reaches exact evidence before touching payload bytes."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "cluster-a": ClusterDefinition(
                name="cluster-a",
                ssh_host="cluster-a.example.test",
            )
        }
    ).save(tmp_path / ".clio-relay/clusters.json")
    digest = "a" * 64
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"

    def preflight(**kwargs: object) -> bootstrap.BootstrapPreflightResult:
        desired = kwargs["desired"]
        invocation_id = kwargs["invocation_id"]
        assert isinstance(desired, BootstrapDesiredState)
        assert isinstance(invocation_id, str)
        jarvis_state = JarvisStateEvidence(
            initialized=True,
            root="/home/operator/.ppi-jarvis",
            roots={
                "config_dir": "/operator/jarvis/config",
                "private_dir": "/operator/jarvis/private",
                "shared_dir": "/operator/jarvis/shared",
            },
            config_sha256="b" * 64,
            repos_sha256="c" * 64,
            resource_graph_sha256="d" * 64,
            managed_repo_registered=True,
        )
        inspection = BootstrapInspection(
            exact_match=True,
            desired_fingerprint=desired.fingerprint,
            install_receipt_sha256="e" * 64,
            active_generation=desired.fingerprint,
            current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
            jarvis_state=jarvis_state,
            readiness=BootstrapReadinessEvidence(
                service_name=desired.worker_service,
                service_was_active=True,
                service_was_enabled=True,
                queue_ready=True,
                queue={
                    "schema_version": "clio-relay.queue-readiness.v1",
                    "complete": True,
                    "sealed": True,
                    "repair_required": False,
                },
                worker_ready=True,
                worker={"running": True},
            ),
        )
        receipt = make_bootstrap_receipt(
            invocation_id=invocation_id,
            desired=desired,
            outcome="noop_verified",
            inspection=inspection,
            started_at=datetime.now(UTC),
            transaction=None,
            previous_generation=desired.fingerprint,
            active_generation=desired.fingerprint,
        )
        return bootstrap.BootstrapPreflightResult(
            action="exact",
            receipt=receipt,
            lines=["bootstrap_preflight_json={}"],
        )

    def poison(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("the public exact no-op touched bootstrap payload code")

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(
        bootstrap,
        "_verify_persistent_bootstrap_receipt",
        _verify_persistent_receipt,
    )
    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)
    monkeypatch.setattr(bootstrap, "_validate_relay_bootstrap_wheel", poison)
    monkeypatch.setattr(bootstrap.shutil, "which", _which)
    monkeypatch.setattr(bootstrap, "uuid4", lambda: type("Uuid", (), {"hex": "cli_test"})())
    monkeypatch.setattr(cli, "package_source_root", lambda: tmp_path / "missing-source")

    def remote_target_identity(_definition: ClusterDefinition) -> dict[str, object]:
        return {"verified": True}

    monkeypatch.setattr(cli, "_remote_target_identity", remote_target_identity)

    result = CliRunner().invoke(
        cli.app,
        [
            "cluster",
            "bootstrap",
            "--cluster",
            "cluster-a",
            "--relay-wheel",
            str(wheel),
            "--relay-artifact-sha256",
            digest,
        ],
    )

    assert result.exit_code == 0, result.output
    assert not wheel.exists()


def test_payload_reconcile_requires_profile_before_building_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only exact reuse may omit a profile; fresh state needs operator graph policy."""

    def preflight(**_kwargs: object) -> bootstrap.BootstrapPreflightResult:
        return bootstrap.BootstrapPreflightResult(
            action="payload_required",
            receipt=None,
            lines=["bootstrap_preflight_json={}"],
        )

    monkeypatch.setattr(bootstrap, "_bootstrap_preflight_over_ssh", preflight)
    monkeypatch.setattr(bootstrap.shutil, "which", _which)

    def poison(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("missing profile reached payload construction")

    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", poison)

    with pytest.raises(ConfigurationError, match="operator-selected"):
        bootstrap.bootstrap_cluster_over_ssh(
            bootstrap_profile="linux-user",
            ssh_host="cluster-a",
            source_root=tmp_path / "not-a-checkout",
            cluster="cluster-a",
            relay_artifact_sha256="a" * 64,
        )


def test_public_release_bootstrap_requires_artifact_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A release invocation cannot collapse rebuilt wheels into one identity."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "cluster-a": ClusterDefinition(
                name="cluster-a",
                ssh_host="cluster-a.example.test",
            )
        }
    ).save(tmp_path / ".clio-relay/clusters.json")
    monkeypatch.setattr(cli, "package_source_root", lambda: tmp_path / "installed-package")
    monkeypatch.setattr(bootstrap.shutil, "which", _which)

    result = CliRunner().invoke(
        cli.app,
        ["cluster", "bootstrap", "--cluster", "cluster-a"],
    )

    assert result.exit_code != 0
    assert "--relay-artifact-sha256" in result.output


def test_payload_free_inspector_fails_closed_after_repair_does_not_converge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A supported inspector never asks the desktop for payload after mutation."""
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=bootstrap.BootstrapRelayIdentity(
            install_spec=f"clio-relay=={__version__}",
            transport_install_spec=f"clio-relay=={__version__}",
            source_identity=f"release:clio-relay=={__version__}:sha256:{'a' * 64}",
            deployment_artifact_sha256="a" * 64,
        ),
        cluster="cluster-a",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    monkeypatch.setenv(
        "CLIO_RELAY_BOOTSTRAP_DESIRED_STATE_BASE64",
        base64.b64encode(desired.model_dump_json().encode()).decode(),
    )
    state = JarvisStateEvidence(
        initialized=True,
        root=str(tmp_path / ".ppi-jarvis"),
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=True,
    )
    initial = BootstrapInspection(
        exact_match=False,
        desired_fingerprint=desired.fingerprint,
        reasons=["managed endpoint service is inactive"],
        jarvis_state=state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=False,
            service_was_enabled=True,
            queue_ready=True,
            worker_ready=False,
        ),
    )
    failed_repair = initial.model_copy(
        update={
            "reasons": ["active endpoint worker readiness did not verify"],
            "readiness": initial.readiness.model_copy(
                update={"service_was_active": True, "worker_ready": False}
            ),
        }
    )
    inspections = iter((initial, failed_repair))

    class ReadyQueue:
        def __init__(self, _root: Path) -> None:
            pass

        def readiness_info(self) -> dict[str, object]:
            return {
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            }

    def systemctl(
        command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        if "is-active" in command:
            return subprocess.CompletedProcess(command, 3, "", "")
        stdout = "loaded\n" if "show" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout, "")

    def installation_info() -> dict[str, object]:
        return {}

    def inspect(*_args: object, **_kwargs: object) -> BootstrapInspection:
        return next(inspections)

    def worker_info(**_kwargs: object) -> dict[str, object]:
        return {"running": True}

    def invocation_lock(**_kwargs: object) -> nullcontext[Path]:
        return nullcontext(tmp_path / "bootstrap.lock")

    monkeypatch.setattr(cli, "installation_info", installation_info)
    monkeypatch.setattr(cli, "ClioCoreQueue", ReadyQueue)
    monkeypatch.setattr(cli, "inspect_exact_bootstrap_noop", inspect)
    monkeypatch.setattr(cli, "run_bounded_process", systemctl)
    monkeypatch.setattr(cli, "worker_runtime_info", worker_info)
    monkeypatch.setattr(cli, "bootstrap_invocation_lock", invocation_lock)

    result = CliRunner().invoke(
        cli.app,
        ["bootstrap-inspect", "--invocation-id", "bootstrap_fail_closed", "--repair"],
    )

    assert result.exit_code != 0
    assert "repair did not converge" in result.output
    assert "bootstrap_preflight_json=" not in result.output


def test_bootstrap_inspection_deadlines_match_acceptance_contract() -> None:
    assert 0 < cli.BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS < 30
    assert (
        cli.BOOTSTRAP_EXACT_INSPECTION_DEADLINE_SECONDS < cli.BOOTSTRAP_REPAIR_DEADLINE_SECONDS < 60
    )


def test_component_upgrade_receipt_accepts_only_bound_managed_repo_registration() -> None:
    """A fenced upgrade may register relay's exact repository without changing other state."""
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=bootstrap.BootstrapRelayIdentity(
            install_spec=f"clio-relay=={__version__}",
            transport_install_spec=f"clio-relay=={__version__}",
            source_identity=f"release:clio-relay=={__version__}:sha256:{'a' * 64}",
            deployment_artifact_sha256="a" * 64,
        ),
        cluster="ares",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    before = JarvisStateEvidence(
        initialized=True,
        root="/home/operator/.ppi-jarvis",
        roots={
            "config_dir": "/operator/jarvis/config",
            "private_dir": "/operator/jarvis/private",
            "shared_dir": "/operator/jarvis/shared",
        },
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=False,
    )
    after = before.model_copy(update={"repos_sha256": "e" * 64, "managed_repo_registered": True})
    inspection = BootstrapInspection(
        exact_match=True,
        desired_fingerprint=desired.fingerprint,
        install_receipt_sha256="f" * 64,
        active_generation=desired.fingerprint,
        current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
        jarvis_state=after,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=True,
            service_was_enabled=True,
            queue_ready=True,
            queue={
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            },
            worker_ready=True,
        ),
    )
    transaction = BootstrapTransactionJournal(
        invocation_id="bootstrap_component_upgrade",
        desired_fingerprint=desired.fingerprint,
        mode="component-upgrade",
        state=BootstrapTransactionState.COMMITTED,
        previous_generation="legacy",
        prepared_generation=desired.fingerprint,
        service_name=desired.worker_service,
        service_was_active=True,
        service_was_enabled=True,
        irreversible_boundary=True,
    )
    actions = {
        "clio-relay": "replaced",
        "clio-kit": "replaced",
        "jarvis-cd": "replaced",
        "jarvis-util": "reused",
        "frp": "reused",
        "uv": "reused",
    }
    components: dict[str, dict[str, object]] = {
        name: {
            "action": action,
            "observed_identity": {},
            "duration_seconds": 1.0,
        }
        for name, action in actions.items()
    }
    managed_repo = "/home/operator/.local/share/clio-relay/managed-jarvis-repo"
    repository_update: dict[str, object] = {
        "link_action": "created",
        "link": managed_repo,
        "target": (
            "/home/operator/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        ),
        "repositories": {
            "action": "updated",
            "managed_repo": managed_repo,
            "added_managed_repos": [managed_repo],
            "removed_previous_managed_repos": [
                "/home/operator/.local/src/clio-relay/jarvis-packages/clio_relay"
            ],
            "before_sha256": before.repos_sha256,
            "after_sha256": after.repos_sha256,
        },
    }
    receipt = make_bootstrap_receipt(
        invocation_id=transaction.invocation_id,
        desired=desired,
        outcome="reconciled",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=transaction,
        previous_generation="legacy",
        active_generation=desired.fingerprint,
        components=components,
        duration_seconds=1.0,
        jarvis_state_before=before,
        jarvis_repo_reconciliation=repository_update,
        payload_transfer_count=2,
        payload_transfer_bytes=1,
    )

    bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        bootstrap_profile="linux-user",
        relay_install_spec=desired.relay_install_spec,
        desired_fingerprint=desired.fingerprint,
        expected_jarvis_resource_graph_profile="ares",
        expected_allow_jarvis_resource_graph_build=False,
        expected_worker_service=desired.worker_service,
    )

    relay_only = copy.deepcopy(receipt)
    relay_transaction = cast(dict[str, object], relay_only["transaction"])
    relay_transaction["mode"] = "relay-only"
    relay_components = cast(dict[str, object], relay_only["components"])
    for name, action in {
        "clio-relay": "prepared",
        "clio-kit": "reused",
        "jarvis-cd": "reused",
    }.items():
        evidence = cast(dict[str, object], relay_components[name])
        evidence["action"] = action
    bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        relay_only,
        bootstrap_profile="linux-user",
        relay_install_spec=desired.relay_install_spec,
        desired_fingerprint=desired.fingerprint,
        expected_jarvis_resource_graph_profile="ares",
        expected_allow_jarvis_resource_graph_build=False,
        expected_worker_service=desired.worker_service,
    )
    invalid_relay_binding = copy.deepcopy(relay_only)
    relay_preservation = cast(dict[str, object], invalid_relay_binding["jarvis_preservation"])
    relay_binding = cast(dict[str, object], relay_preservation["repositories"])
    relay_binding["target"] = "/operator/unrelated-repository"
    with pytest.raises(RelayError, match="repository binding is invalid"):
        bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            invalid_relay_binding,
            bootstrap_profile="linux-user",
            relay_install_spec=desired.relay_install_spec,
            desired_fingerprint=desired.fingerprint,
            expected_jarvis_resource_graph_profile="ares",
            expected_allow_jarvis_resource_graph_build=False,
            expected_worker_service=desired.worker_service,
        )

    tampered = copy.deepcopy(receipt)
    preservation = cast(dict[str, object], tampered["jarvis_preservation"])
    binding = cast(dict[str, object], preservation["repositories"])
    update = cast(dict[str, object], binding["repositories"])
    update["after_sha256"] = "0" * 64
    with pytest.raises(RelayError, match="hashes do not bind"):
        bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            tampered,
            bootstrap_profile="linux-user",
            relay_install_spec=desired.relay_install_spec,
            desired_fingerprint=desired.fingerprint,
            expected_jarvis_resource_graph_profile="ares",
            expected_allow_jarvis_resource_graph_build=False,
            expected_worker_service=desired.worker_service,
        )

    unauthorized_removal = copy.deepcopy(receipt)
    preservation = cast(dict[str, object], unauthorized_removal["jarvis_preservation"])
    binding = cast(dict[str, object], preservation["repositories"])
    update = cast(dict[str, object], binding["repositories"])
    update["removed_previous_managed_repos"] = ["/operator/unrelated-repository"]
    with pytest.raises(RelayError, match="repository migration is invalid"):
        bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            unauthorized_removal,
            bootstrap_profile="linux-user",
            relay_install_spec=desired.relay_install_spec,
            desired_fingerprint=desired.fingerprint,
            expected_jarvis_resource_graph_profile="ares",
            expected_allow_jarvis_resource_graph_build=False,
            expected_worker_service=desired.worker_service,
        )

    replaced_link = copy.deepcopy(receipt)
    preservation = cast(dict[str, object], replaced_link["jarvis_preservation"])
    binding = cast(dict[str, object], preservation["repositories"])
    binding["link_action"] = "retargeted"
    bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        replaced_link,
        bootstrap_profile="linux-user",
        relay_install_spec=desired.relay_install_spec,
        desired_fingerprint=desired.fingerprint,
        expected_jarvis_resource_graph_profile="ares",
        expected_allow_jarvis_resource_graph_build=False,
        expected_worker_service=desired.worker_service,
    )

    unproven_retarget = copy.deepcopy(replaced_link)
    generation = cast(dict[str, object], unproven_retarget["generation"])
    generation["previous"] = "unproven"
    with pytest.raises(RelayError, match="did not preserve existing JARVIS state"):
        bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            unproven_retarget,
            bootstrap_profile="linux-user",
            relay_install_spec=desired.relay_install_spec,
            desired_fingerprint=desired.fingerprint,
            expected_jarvis_resource_graph_profile="ares",
            expected_allow_jarvis_resource_graph_build=False,
            expected_worker_service=desired.worker_service,
        )


def test_fresh_bootstrap_receipt_allows_explicit_pending_service_install(
    tmp_path: Path,
) -> None:
    """Unit creation remains the documented explicit command after fresh bootstrap."""
    desired = bootstrap._bootstrap_desired_state(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        identity=bootstrap.BootstrapRelayIdentity(
            install_spec=f"clio-relay=={__version__}",
            transport_install_spec=f"clio-relay=={__version__}",
            source_identity=f"release:clio-relay=={__version__}:sha256:{'a' * 64}",
            deployment_artifact_sha256="a" * 64,
        ),
        cluster="fresh-cluster",
        core_dir=bootstrap.DEFAULT_REMOTE_CORE_DIR,
        spool_dir=bootstrap.DEFAULT_REMOTE_SPOOL_DIR,
        frp_version=bootstrap.FRP_VERSION,
        clio_kit_install_spec=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_URL,
        clio_kit_artifact_sha256=bootstrap.CLIO_KIT_JARVIS_MCP_WHEEL_SHA256,
        agent_adapter="exec",
        agent_npm_package=None,
        agent_npm_bin=None,
        agent_args=[],
        jarvis_resource_graph_profile="ares",
    )
    jarvis_state = JarvisStateEvidence(
        initialized=True,
        root=str(tmp_path / ".ppi-jarvis"),
        config_sha256="b" * 64,
        repos_sha256="c" * 64,
        resource_graph_sha256="d" * 64,
        managed_repo_registered=True,
    )
    inspection = BootstrapInspection(
        exact_match=False,
        desired_fingerprint=desired.fingerprint,
        reasons=[
            "managed endpoint service is inactive",
            "managed endpoint service is disabled",
        ],
        install_receipt_sha256="e" * 64,
        active_generation=desired.fingerprint,
        current_generation_target=f"/home/operator/generations/{desired.fingerprint}",
        jarvis_state=jarvis_state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=False,
            service_was_enabled=False,
            queue_ready=True,
            queue={
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": True,
                "sealed": True,
                "repair_required": False,
            },
            worker_ready=False,
        ),
    )
    components: dict[str, dict[str, object]] = {
        name: {
            "action": "prepared",
            "observed_identity": {},
            "duration_seconds": 1.0,
        }
        for name in ("clio-relay", "clio-kit", "jarvis-cd", "jarvis-util", "frp", "uv")
    }
    loaded_builtin_result: dict[str, object] = {
        "schema_version": "jarvis.resource-graph-builtin.v1",
        "profile": "ares",
        "action": "loaded",
        "available": True,
        "source": "/opt/jarvis/resource_graphs/ares.yaml",
        "source_sha256": "d" * 64,
        "catalog": ["ares"],
    }
    receipt = make_bootstrap_receipt(
        invocation_id="bootstrap_fresh",
        desired=desired,
        outcome="full",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=None,
        previous_generation=None,
        active_generation=desired.fingerprint,
        components=components,
        jarvis_init_action="initialized",
        jarvis_init_duration_seconds=1.0,
        jarvis_graph_action="loaded",
        jarvis_graph_duration_seconds=1.0,
        jarvis_builtin_result=loaded_builtin_result,
        jarvis_commands=[
            ["jarvis", "init", "/config", "/private", "/shared"],
            ["jarvis", "rg", "load-builtin", "ares", "+json"],
        ],
        jarvis_state_before=JarvisStateEvidence(
            initialized=False,
            root=jarvis_state.root,
        ),
        service_pending_install=True,
        service_active_after=False,
        service_enabled_after=False,
        payload_transfer_count=2,
        payload_transfer_bytes=1,
    )

    bootstrap._validate_bootstrap_receipt(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        receipt,
        bootstrap_profile="linux-user",
        relay_install_spec=desired.relay_install_spec,
        desired_fingerprint=desired.fingerprint,
        expected_jarvis_resource_graph_profile="ares",
        expected_allow_jarvis_resource_graph_build=False,
        expected_worker_service=desired.worker_service,
    )
    service = receipt["service"]
    assert isinstance(service, dict)
    assert service["pending_install"] is True

    with pytest.raises(ValueError, match="packaged source digest"):
        make_bootstrap_receipt(
            invocation_id="bootstrap_mismatched_builtin",
            desired=desired,
            outcome="full",
            inspection=inspection,
            started_at=datetime.now(UTC),
            transaction=None,
            previous_generation=None,
            active_generation=desired.fingerprint,
            components=components,
            jarvis_init_action="initialized",
            jarvis_init_duration_seconds=1.0,
            jarvis_graph_action="loaded",
            jarvis_graph_duration_seconds=1.0,
            jarvis_builtin_result={
                **loaded_builtin_result,
                "source_sha256": "e" * 64,
            },
        )

    unavailable: dict[str, object] = {
        "schema_version": "jarvis.resource-graph-builtin.v1",
        "profile": "ares",
        "action": "unavailable",
        "available": False,
        "source": None,
        "source_sha256": None,
        "catalog": [],
    }
    with pytest.raises(ValueError, match="build was not enabled"):
        make_bootstrap_receipt(
            invocation_id="bootstrap_unauthorized_build",
            desired=desired,
            outcome="full",
            inspection=inspection,
            started_at=datetime.now(UTC),
            transaction=None,
            previous_generation=None,
            active_generation=desired.fingerprint,
            components=components,
            jarvis_init_action="initialized",
            jarvis_init_duration_seconds=1.0,
            jarvis_graph_action="built",
            jarvis_graph_duration_seconds=1.0,
            jarvis_builtin_result=unavailable,
        )

    allowed = desired.model_copy(update={"allow_jarvis_resource_graph_build": True})
    built = make_bootstrap_receipt(
        invocation_id="bootstrap_allowed_build",
        desired=allowed,
        outcome="full",
        inspection=inspection,
        started_at=datetime.now(UTC),
        transaction=None,
        previous_generation=None,
        active_generation=allowed.fingerprint,
        components=components,
        jarvis_init_action="initialized",
        jarvis_init_duration_seconds=1.0,
        jarvis_graph_action="built",
        jarvis_graph_duration_seconds=1.0,
        jarvis_builtin_result=unavailable,
        jarvis_commands=[
            ["jarvis", "init", "/config", "/private", "/shared"],
            ["jarvis", "rg", "load-builtin", "ares", "+json"],
            ["jarvis", "rg", "build", "+no_benchmark"],
        ],
    )
    graph = built["jarvis_resource_graph"]
    assert isinstance(graph, dict)
    assert graph["action"] == "built"
    assert graph["builtin_result"] == unavailable


def test_release_identity_is_canonical_across_wheel_and_pypi_transport(
    tmp_path: Path,
) -> None:
    digest = "f" * 64
    wheel = tmp_path / f"clio_relay-{__version__}-py3-none-any.whl"

    wheel_identity = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=wheel,
        relay_artifact_sha256=digest,
    )
    pypi_identity = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256=digest,
    )
    assert wheel_identity.install_spec == pypi_identity.install_spec
    assert wheel_identity.source_identity == pypi_identity.source_identity
    assert wheel_identity.transport_install_spec.endswith(wheel.name)
    assert pypi_identity.transport_install_spec == f"clio-relay=={__version__}"


def test_future_release_identity_does_not_require_network_or_a_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bootstrap, "__version__", "1.4.18")

    identity = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256="1" * 64,
    )

    assert identity.install_spec == "clio-relay==1.4.18"
    assert identity.source_identity == f"release:clio-relay==1.4.18:sha256:{'1' * 64}"
    assert identity.deployment_artifact_sha256 == "1" * 64


def test_release_identity_requires_exact_artifact_digest(tmp_path: Path) -> None:
    with pytest.raises(bootstrap.ConfigurationError, match="--relay-artifact-sha256"):
        bootstrap.bootstrap_relay_identity(
            source_root=tmp_path / "not-a-checkout",
            relay_wheel=None,
            relay_artifact_sha256=None,
        )


def test_same_release_version_with_different_wheels_has_different_identity(
    tmp_path: Path,
) -> None:
    first = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256="1" * 64,
    )
    second = bootstrap.bootstrap_relay_identity(
        source_root=tmp_path / "not-a-checkout",
        relay_wheel=None,
        relay_artifact_sha256="2" * 64,
    )

    assert first.source_identity != second.source_identity
    assert first.deployment_artifact_sha256 != second.deployment_artifact_sha256


def test_payload_script_uses_digest_bound_safe_extractor() -> None:
    """Payload bootstrap never dispatches the source archive through raw tar."""
    script = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a")

    assert 'tar -xf "$SOURCE_ARCHIVE"' not in script
    assert "stream.extractall(destination" not in script
    assert "from clio_relay.safe_archive import safe_extract_tar" in script
    assert "candidate_safe_archive" not in script
    assert "BOOTSTRAP_CANDIDATE_PACKAGE/safe_archive.py" in script


def test_legacy_planning_attests_private_candidate_before_transaction_or_fence() -> None:
    """A legacy relay cannot authorize its own replacement or touch stable state."""
    script = bootstrap.render_linux_user_bootstrap_script(
        cluster="cluster-a",
        relay_install_spec="$DEST/wheels/clio_relay-1.4.18-py3-none-any.whl",
        relay_deployment_install_spec="clio-relay==1.4.18",
        relay_artifact_sha256="a" * 64,
        source_archive_sha256="b" * 64,
    )
    planning = script.index('BOOTSTRAP_PLAN_MODE="full"')
    archive = script.index("  SOURCE_ARCHIVE=/tmp/clio-relay-head.tar", planning)
    archive_digest = script.index(
        'echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -',
        archive,
    )
    extraction = script.index('bootstrap_safe_extract python3 "$SOURCE_ARCHIVE"', archive_digest)
    wheel_digest = script.index(
        'echo "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256 *$BOOTSTRAP_CANDIDATE_ARTIFACT"',
        extraction,
    )
    uv_copy = script.index('BOOTSTRAP_PINNED_UV="$(', wheel_digest)
    uv_digest = script.index(
        "candidate uv source changed or did not match its release pin",
        uv_copy,
    )
    install = script.index("bootstrap_candidate_install=fd-bound-wheel-verified:", uv_digest)
    chain_exec = script.index(" install-verify-and-exec ", install)
    attestation = script.index("prove_bootstrap_replacement_provider(", install)
    plan = script.index(
        "plan_bootstrap_reconcile(desired, replacement_provider=evidence)",
        attestation,
    )
    dispatch = script.index("  bootstrap_relay_only_reconcile", plan)
    planning_heredoc = script[chain_exec:dispatch]
    retained_planning = script[planning:dispatch]

    assert archive < archive_digest < extraction < wheel_digest < uv_copy < uv_digest
    assert uv_digest < install < chain_exec < attestation < plan < dispatch
    assert "spec_from_file_location" not in planning_heredoc
    assert "from clio_relay.bootstrap_reconcile import (" in planning_heredoc
    assert '"UV_TOOL_DIR": str(tool_directory)' in script
    assert '"UV_CACHE_DIR": str(cache_directory)' in script
    assert 'BOOTSTRAP_PREPARING_ROOT="$BOOTSTRAP_PREPARING_PARENT/active"' in script
    assert "BOOTSTRAP_CANDIDATE_INSTALL_SPEC='$DEST/wheels/" in script
    assert 'UV_PYTHON_INSTALL_DIR="$HOME/.local/share/clio-relay/uv-python"' in script
    assert 'UV_PYTHON_INSTALL_DIR="$BOOTSTRAP_PREPARING_ROOT/' not in script
    assert '"$BOOTSTRAP_PINNED_UV" tool install' not in script
    assert 'executable=f"/proc/self/fd/{uv_descriptor}"' in script
    assert "installed candidate differs from the pinned wheel fd" in script
    assert "install-and-verify" in script
    assert "install-verify-and-exec" in planning_heredoc
    assert 'BOOTSTRAP_PLAN_PROVIDER="$(sed' not in retained_planning
    assert '"$BOOTSTRAP_PLAN_PROVIDER" -I' not in retained_planning
    assert 'bootstrap_provider_exec "$@"' in script
    assert '"$provider" -I - "$BOOTSTRAP_CANDIDATE_PYTHON_ROOT"' not in script
    assert "expected_provider_interpreter_sha256" in planning_heredoc
    assert planning_heredoc.index("from clio_relay.bootstrap_reconcile import (") < (
        planning_heredoc.index("prove_bootstrap_replacement_provider(")
    )
    assert planning_heredoc.index("prove_bootstrap_replacement_provider(") < (
        planning_heredoc.index("plan_bootstrap_reconcile(desired, replacement_provider=evidence)")
    )
    assert "shutil.rmtree" not in script
    no_downloads = script.index('"UV_PYTHON_DOWNLOADS": "never"', uv_digest)
    assert uv_digest < no_downloads < install
    assert '"uv_build.json"' in script
    assert "bootstrap_cleanup_preparing_root" in script
    assert script.index("BOOTSTRAP_LEGACY_RELAY_PROVIDER=1") < script.index(
        '"$BOOTSTRAP_CURRENT_PROVIDER" -c'
    )


def test_staged_upgrade_uses_journal_bound_idempotent_forward_activation() -> None:
    """Legacy adoption and recovery share one staged-provider activation path."""
    script = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a")
    provider_start = script.index("bootstrap_provider_exec() (")
    provider_end = script.index("\n)\n\nbootstrap_candidate_action", provider_start)
    provider_function = script[provider_start:provider_end]
    activation = script.index("  bootstrap_candidate_action journal-advance activating")
    finish = script.index("bootstrap_candidate_action finish-activation", activation)
    verify = script.index("  bootstrap_verify_stable_activation_links", finish)
    activated = script.index(
        "  bootstrap_candidate_action journal-advance activated",
        verify,
    )
    migration = script.index(
        "  bootstrap_candidate_action journal-advance migration_started",
        activated,
    )
    recovery = script[script.index("bootstrap_recover_previous_transaction()") : activation]

    assert "bootstrap_use_staged_provider()" in script
    assert "journal-phase prepared_manifest" in script
    assert provider_function.index("compgen -e") < provider_function.index("exec python3 -I -c")
    assert 'LD_*|PYTHON*|BASH_ENV|ENV) unset "$bootstrap_environment_name"' in provider_function
    assert activation < finish < verify < activated < migration
    assert "bootstrap_candidate_action finish-activation" in recovery
    assert "phase_identities" in recovery
    assert "sha256sum --check --strict -" in recovery
    assert "printf '%s\\n' \"bootstrap_reconcile_plan=$BOOTSTRAP_PLAN_JSON\" >&2" in script
    assert 'mv -Tf "$HOME/.local/share/clio-relay/.current.' not in script
    assert 'readlink "$HOME/.local/share/clio-relay/current"' not in script
    assert (
        'MANAGED_JARVIS_REPO_TARGET="$HOME/.local/share/clio-relay/current/'
        'source/jarvis-packages/clio_relay"'
    ) in script
    assert "scancel" not in script


def test_fresh_jarvis_hardware_graph_commands_are_exact_and_ordered() -> None:
    lines = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a").splitlines()
    init_argv = [
        '"$JARVIS_VENV/bin/jarvis"',
        "init",
        '"$HOME/.local/share/clio-relay/jarvis-config"',
        '"$HOME/.local/share/clio-relay/jarvis-private"',
        '"$HOME/.local/share/clio-relay/jarvis-shared"',
    ]
    graph_argv = ['"$JARVIS_VENV/bin/jarvis"', "rg", "build", "+no_benchmark"]
    init_indexes = [index for index, line in enumerate(lines) if line.split() == init_argv]
    graph_indexes = [index for index, line in enumerate(lines) if line.split() == graph_argv]

    assert len(init_indexes) == 1
    assert len(graph_indexes) == 1
    assert init_indexes[0] < graph_indexes[0]


def test_fresh_journal_precedes_every_mutation_boundary() -> None:
    """First-install state is owned durably before components or JARVIS can mutate."""
    script = bootstrap.render_linux_user_bootstrap_script(cluster="cluster-a")
    journal = script.index("bootstrap_journal_action create")
    boundaries = (
        'curl -L --fail --retry 3 -o "$ARCHIVE"',
        "uv python install 3.12",
        'uv venv --python 3.12 --seed "$JARVIS_VENV"',
        '"$JARVIS_VENV/bin/jarvis" init',
        '"$JARVIS_VENV/bin/jarvis" rg build +no_benchmark',
        'managed_repo "$MANAGED_JARVIS_REPO_TARGET"',
        'mkdir -m 0700 "$BOOTSTRAP_GENERATION/bin"',
        'current "$BOOTSTRAP_GENERATION"',
        'bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" migration_started',
    )

    assert all(journal < script.index(boundary, journal) for boundary in boundaries)
    assert 'rm -rf "$DEST"' not in script
    assert '--clear "$JARVIS_VENV"' not in script
    assert 'bootstrap_journal_action discard-full "$BOOTSTRAP_TRANSACTION_JOURNAL"' in script
    assert 'mkdir -m 0700 -p "$BOOTSTRAP_TRANSACTION_ROOT/downloads"' not in script
    assert 'bootstrap_journal_action own "$BOOTSTRAP_TRANSACTION_JOURNAL"' not in script
    for owned_action in ("mkdir-owned", "copy-owned", "symlink-owned"):
        assert f"bootstrap_journal_action {owned_action}" in script[journal:]
    for phase in (
        "ownership_manifest",
        "components_prepared",
        "jarvis_initialized",
        "resource_graph_$JARVIS_GRAPH_ACTION",
        "managed_repository_reconciled",
        "generation_prepared",
        "generation_activated",
        "queue_migrated",
        "service_verified",
        "final_inspection",
    ):
        assert phase in script[journal:]


def test_packaged_payload_archive_has_only_safe_canonical_modes(tmp_path: Path) -> None:
    """Windows host modes cannot leak group/world write bits into the wire tar."""
    deployment = bootstrap.create_bootstrap_archive(
        source_root=tmp_path / "not-a-checkout",
        archive=tmp_path / "bootstrap.tar",
    )

    with tarfile.open(deployment.archive, "r:") as archive:
        members = archive.getmembers()
    assert members
    assert all(member.isdir() or member.isreg() for member in members)
    assert all(member.mode == (0o755 if member.isdir() else 0o644) for member in members)
