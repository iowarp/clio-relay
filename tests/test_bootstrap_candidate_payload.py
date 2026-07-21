"""Execution coverage for the shipped bootstrap reconciliation overlay."""

from __future__ import annotations

import base64
import json
import shutil
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay import bootstrap
from clio_relay import bootstrap_provider_build_info as provider_build_info


def _extract_candidate_sources(script: str) -> dict[str, bytes]:
    """Extract the exact source manifest embedded in one rendered bootstrap."""
    prefix = "encoded_sources = json.loads(r'''"
    start = script.index(prefix) + len(prefix)
    end = script.index("''')", start)
    raw = cast(object, json.loads(script[start:end]))
    assert isinstance(raw, dict)
    encoded = cast(dict[str, object], raw)
    sources: dict[str, bytes] = {}
    for name, value in encoded.items():
        assert isinstance(value, str)
        sources[name] = base64.b64decode(value, validate=True)
    return sources


def test_extracted_candidate_reconciler_runs_without_provider_bounded_process(
    tmp_path: Path,
) -> None:
    """The candidate overlay supplies its complete bounded-process subsystem."""
    sources = _extract_candidate_sources(
        bootstrap.render_linux_user_bootstrap_script(cluster="candidate-test")
    )
    assert set(sources) == {
        "__init__.py",
        "bootstrap_provider_build_info.py",
        "bootstrap_reconcile.py",
        "bounded_process.py",
        "errors.py",
        "process_containment.py",
        "safe_archive.py",
    }

    candidate_root = tmp_path / "candidate-python"
    candidate_package = candidate_root / "clio_relay"
    candidate_package.mkdir(parents=True)
    for name, payload in sources.items():
        (candidate_package / name).write_bytes(payload)

    installed_package = Path(bootstrap.__file__).resolve().parent
    legacy_root = tmp_path / "legacy-provider"
    shutil.copytree(
        installed_package,
        legacy_root / "clio_relay",
        ignore=shutil.ignore_patterns(
            "__pycache__",
            "bootstrap_reconcile.py",
            "bounded_process.py",
            "errors.py",
            "process_containment.py",
            "safe_archive.py",
        ),
    )
    probe = r"""
import importlib.util
import json
import sys
from pathlib import Path

candidate_root = Path(sys.argv[1]).resolve()
legacy_root = Path(sys.argv[2]).resolve()
sys.path.insert(0, str(legacy_root))
sys.path.insert(0, str(candidate_root))
reconciler_path = candidate_root / "clio_relay/bootstrap_reconcile.py"
name = "clio_relay.bootstrap_reconcile_candidate"
spec = importlib.util.spec_from_file_location(name, reconciler_path)
if spec is None or spec.loader is None:
    raise SystemExit("candidate reconciler spec was unavailable")
module = importlib.util.module_from_spec(spec)
sys.modules[name] = module
spec.loader.exec_module(module)

from clio_relay import bounded_process, process_containment

completed = bounded_process.run_bounded_process(
    [sys.executable, "-c", "print('candidate-ok')"],
    timeout_seconds=10,
    stdout_maximum_bytes=4096,
    stderr_maximum_bytes=4096,
)
if completed.returncode != 0 or completed.stdout.strip() != "candidate-ok":
    raise SystemExit("candidate bounded-process probe failed")
print(json.dumps({
    "bounded_process": str(Path(bounded_process.__file__).resolve()),
    "process_containment": str(Path(process_containment.__file__).resolve()),
    "reconcile_digest": module.canonical_json_sha256({"candidate": True}),
}, sort_keys=True))
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            probe,
            str(candidate_root),
            str(legacy_root),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    evidence = json.loads(completed.stdout.splitlines()[-1])
    assert Path(evidence["bounded_process"]) == (candidate_package / "bounded_process.py").resolve()
    assert (
        Path(evidence["process_containment"])
        == (candidate_package / "process_containment.py").resolve()
    )
    assert len(evidence["reconcile_digest"]) == 64


def test_candidate_overlay_preserves_legacy_provider_install_identity(
    tmp_path: Path,
) -> None:
    """Candidate reconciliation must inspect the installed provider as itself."""
    sources = _extract_candidate_sources(
        bootstrap.render_linux_user_bootstrap_script(cluster="candidate-test")
    )
    candidate_root = tmp_path / "candidate-python"
    candidate_package = candidate_root / "clio_relay"
    candidate_package.mkdir(parents=True)
    for name, payload in sources.items():
        (candidate_package / name).write_bytes(payload)

    legacy_version = "1.4.12"
    legacy_commit = "dc6d36682598d466f8512f8082edfaf5c92af02b"
    legacy_root = tmp_path / "legacy-provider"
    installed_package = Path(bootstrap.__file__).resolve().parent
    shutil.copytree(
        installed_package,
        legacy_root / "clio_relay",
        ignore=shutil.ignore_patterns("__pycache__", *sources),
    )
    distribution_metadata = legacy_root / f"clio_relay-{legacy_version}.dist-info"
    distribution_metadata.mkdir()
    (distribution_metadata / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: clio-relay\nVersion: {legacy_version}\n",
        encoding="utf-8",
    )
    build_info = {
        "version": legacy_version,
        "commit": legacy_commit,
        "tag": f"v{legacy_version}",
        "dirty": False,
    }
    (legacy_root / "clio_relay" / "_build_info.json").write_text(
        json.dumps(build_info),
        encoding="utf-8",
    )
    (distribution_metadata / "RECORD").write_text(
        "clio_relay/_build_info.json,,\n",
        encoding="utf-8",
    )
    receipt = tmp_path / "install-receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": "clio-relay.install-receipt.v1",
                "installed_at": datetime.now(UTC).isoformat(),
                "install_spec": f"clio-relay=={legacy_version}",
                "requested_source": "wheel",
                "artifact_filename": None,
                "artifact_sha256": None,
                "distribution_version": legacy_version,
                "software": {
                    "version": legacy_version,
                    "commit": legacy_commit,
                    "tag": f"v{legacy_version}",
                    "dirty": False,
                },
                "components": {},
                "component_artifacts": {},
                "deployment_fingerprint": None,
                "deployment_manifest": None,
                "generation": None,
            }
        ),
        encoding="utf-8",
    )

    probe = r"""
import json
import runpy
import sys
from pathlib import Path

stager = Path(sys.argv[1]).resolve()
candidate_root = Path(sys.argv[2]).resolve()
legacy_root = Path(sys.argv[3]).resolve()
receipt = Path(sys.argv[4]).resolve()
sys.path.insert(0, str(legacy_root))
sys.argv = [str(stager), str(candidate_root / "clio_relay")]
try:
    runpy.run_path(str(stager), run_name="__main__")
except SystemExit as exc:
    if exc.code not in (None, 0):
        raise
sys.path.insert(0, str(candidate_root))

import clio_relay
from clio_relay.installation import installation_info

info = installation_info(receipt)
print(json.dumps({
    "package_version": clio_relay.__version__,
    "distribution_version": info["distribution_version"],
    "software": info["software"],
    "receipt_matches_install": info["receipt_matches_install"],
}, sort_keys=True))
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            probe,
            str(candidate_package / "bootstrap_provider_build_info.py"),
            str(candidate_root),
            str(legacy_root),
            str(receipt),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    evidence = json.loads(completed.stdout.splitlines()[-1])
    assert evidence == {
        "distribution_version": legacy_version,
        "package_version": legacy_version,
        "receipt_matches_install": True,
        "software": build_info,
    }


def test_provider_build_info_stager_rejects_hostile_provider_records(tmp_path: Path) -> None:
    """Missing, malformed, and oversized recorded provider files are fatal."""
    sources = _extract_candidate_sources(
        bootstrap.render_linux_user_bootstrap_script(cluster="candidate-test")
    )
    stager = tmp_path / "bootstrap_provider_build_info.py"
    stager.write_bytes(sources["bootstrap_provider_build_info.py"])
    candidate_package = tmp_path / "candidate-python" / "clio_relay"
    candidate_package.mkdir(parents=True)

    probe = r"""
import runpy
import sys
from pathlib import Path

stager = Path(sys.argv[1]).resolve()
provider_root = Path(sys.argv[2]).resolve()
candidate_package = Path(sys.argv[3]).resolve()
sys.path.insert(0, str(provider_root))
sys.argv = [str(stager), str(candidate_package)]
runpy.run_path(str(stager), run_name="__main__")
"""
    for case, payload in (
        ("missing", None),
        ("malformed", b"not-json"),
        ("oversized", b"{" + b"x" * (64 * 1024)),
    ):
        provider_root = tmp_path / f"provider-{case}"
        provider_package = provider_root / "clio_relay"
        provider_package.mkdir(parents=True)
        metadata = provider_root / "clio_relay-1.4.12.dist-info"
        metadata.mkdir()
        (metadata / "METADATA").write_text(
            "Metadata-Version: 2.1\nName: clio-relay\nVersion: 1.4.12\n",
            encoding="utf-8",
        )
        (metadata / "RECORD").write_text(
            "clio_relay/_build_info.json,,\n",
            encoding="utf-8",
        )
        if payload is not None:
            (provider_package / "_build_info.json").write_bytes(payload)
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                probe,
                str(stager),
                str(provider_root),
                str(candidate_package),
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )
        assert completed.returncode != 0, case
        assert "provider build info" in completed.stderr, (case, completed.stderr)


def test_provider_build_info_stager_keeps_fresh_bootstrap_free_of_candidate_identity(
    tmp_path: Path,
) -> None:
    """No-provider bootstrap is safe and cannot consume a candidate identity."""
    sources = _extract_candidate_sources(
        bootstrap.render_linux_user_bootstrap_script(cluster="candidate-test")
    )
    stager = tmp_path / "bootstrap_provider_build_info.py"
    stager.write_bytes(sources["bootstrap_provider_build_info.py"])
    candidate_package = tmp_path / "candidate-python" / "clio_relay"
    candidate_package.mkdir(parents=True)

    clean = subprocess.run(
        [sys.executable, "-I", "-S", str(stager), str(candidate_package)],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert clean.returncode == 0, clean.stderr
    assert clean.stdout.strip() == "bootstrap_provider_build_info=unavailable"

    (candidate_package / "_build_info.json").write_text(
        '{"version":"candidate"}',
        encoding="utf-8",
    )
    substituted = subprocess.run(
        [sys.executable, "-I", "-S", str(stager), str(candidate_package)],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert substituted.returncode != 0
    assert "candidate build info exists without an installed provider" in substituted.stderr


def test_provider_build_info_stager_rejects_existing_candidate_links_and_mismatches(
    tmp_path: Path,
) -> None:
    """A preexisting candidate must be an exact independent regular copy."""
    source = tmp_path / "provider-build-info.json"
    source.write_text("provider", encoding="utf-8")
    candidate = tmp_path / "candidate-build-info.json"
    candidate.write_text("candidate", encoding="utf-8")
    write_candidate_copy = cast(Any, provider_build_info)._write_candidate_copy
    with pytest.raises(RuntimeError, match="does not match the installed provider"):
        write_candidate_copy(
            candidate,
            b"provider",
            source_details=source.lstat(),
        )

    class SyntheticSymlink:
        """Path-shaped hostile candidate used where Windows cannot create symlinks."""

        def lstat(self) -> object:
            return type(
                "SymlinkDetails",
                (),
                {"st_mode": stat.S_IFLNK, "st_size": 8, "st_nlink": 1},
            )()

    with pytest.raises(RuntimeError, match="bounded regular file"):
        write_candidate_copy(
            cast(Path, SyntheticSymlink()),
            b"provider",
            source_details=source.lstat(),
        )
