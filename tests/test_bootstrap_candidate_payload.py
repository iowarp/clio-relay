"""Execution coverage for the shipped bootstrap reconciliation overlay."""

from __future__ import annotations

import base64
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from clio_relay import bootstrap


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
    evidence = json.loads(completed.stdout)
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
                    "commit": None,
                    "tag": None,
                    "dirty": None,
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
import sys
from pathlib import Path

candidate_root = Path(sys.argv[1]).resolve()
legacy_root = Path(sys.argv[2]).resolve()
receipt = Path(sys.argv[3]).resolve()
sys.path.insert(0, str(legacy_root))
sys.path.insert(0, str(candidate_root))

import clio_relay
from clio_relay.installation import installation_info

info = installation_info(receipt)
print(json.dumps({
    "package_version": clio_relay.__version__,
    "distribution_version": info["distribution_version"],
    "software_version": info["software"]["version"],
    "receipt_matches_install": info["receipt_matches_install"],
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
            str(receipt),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    evidence = json.loads(completed.stdout)
    assert evidence == {
        "distribution_version": legacy_version,
        "package_version": legacy_version,
        "receipt_matches_install": True,
        "software_version": legacy_version,
    }
