from __future__ import annotations

import hashlib
import json
import os
import subprocess
import venv
import zipfile
from base64 import urlsafe_b64encode
from pathlib import Path
from typing import cast

import pytest

import clio_relay.wheel_record_closure as wheel_record_closure_module


def _record_row(relative: str, payload: bytes) -> str:
    digest = urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()
    return f"{relative},sha256={digest},{len(payload)}"


def _bound_python_script_header(executable: str, *, posix_launcher: str = "uv") -> bytes:
    if os.name == "nt":
        return f"#!{executable}\n".encode()
    if posix_launcher == "uv":
        provider = "'" + executable.replace("'", "'\"'\"'") + "'"
    elif posix_launcher == "pip":
        provider = executable
    else:
        raise AssertionError(f"unsupported fixture launcher: {posix_launcher}")
    return f"#!/bin/sh\n'''exec' {provider} \"$0\" \"$@\"\n' '''\n".encode()


def _create_wheel_scripts_fixture(
    tmp_path: Path,
    *,
    posix_launcher: str = "uv",
) -> tuple[Path, str, Path, dict[str, Path], dict[str, str]]:
    environment = tmp_path / "environment"
    venv.EnvBuilder(with_pip=False, symlinks=os.name != "nt").create(environment)
    invoked_python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    completed = subprocess.run(
        [
            str(invoked_python),
            "-I",
            "-c",
            (
                "import json, sys, sysconfig; "
                "print(json.dumps({'executable': sys.executable, "
                "'paths': sysconfig.get_paths()}))"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    runtime = cast(dict[str, object], json.loads(completed.stdout))
    executable = cast(str, runtime["executable"])
    paths = cast(dict[str, str], runtime["paths"])
    site_packages = Path(paths["purelib"])
    scripts_root = Path(paths["scripts"])
    scripts_root.mkdir(parents=True, exist_ok=True)

    metadata_root = "fixture_jarvis-1.0.dist-info"
    record_name = f"{metadata_root}/RECORD"
    entry_points = b"[console_scripts]\nfixture-jarvis = fixture_jarvis.cli:main\n"
    installed_members = {
        "fixture_jarvis/__init__.py": b'"""Fixture package."""\n',
        "fixture_jarvis/cli.py": b"def main() -> None:\n    return None\n",
        f"{metadata_root}/METADATA": (
            b"Metadata-Version: 2.1\nName: fixture-jarvis\nVersion: 1.0\n\n"
        ),
        f"{metadata_root}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n"
        ),
        f"{metadata_root}/entry_points.txt": entry_points,
    }
    for relative, payload in installed_members.items():
        destination = site_packages / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)

    source_scripts = {
        "fixture-jarvis": b"#!python\nprint('wheel console body')\n",
        "fixture-resource": b"#!python\nprint('wheel resource body')\n",
    }
    header = _bound_python_script_header(executable, posix_launcher=posix_launcher)
    installed_scripts = {
        "fixture-jarvis": scripts_root / "fixture-jarvis",
        "fixture-resource": scripts_root / "fixture-resource",
    }
    installed_scripts["fixture-jarvis"].write_bytes(
        header
        + b"import sys\n"
        + b"from fixture_jarvis.cli import main\n"
        + b"if __name__ == '__main__':\n"
        + b"    sys.argv[0] = sys.argv[0].removesuffix('.exe')\n"
        + b"    sys.exit(main())\n"
    )
    installed_scripts["fixture-resource"].write_bytes(header + b"print('wheel resource body')\n")
    if os.name != "nt":
        for script in installed_scripts.values():
            script.chmod(0o755)

    installed_record_rows = [
        _record_row(relative, payload) for relative, payload in sorted(installed_members.items())
    ]
    record_members: dict[str, str] = {}
    for script_name, script in sorted(installed_scripts.items()):
        relative = os.path.relpath(script, site_packages).replace("\\", "/")
        record_members[script_name] = relative
        installed_record_rows.append(_record_row(relative, script.read_bytes()))
    installed_record_rows.append(f"{record_name},,")
    installed_record = site_packages / record_name
    installed_record.write_text("\n".join(installed_record_rows) + "\n", encoding="utf-8")

    wheel_members = dict(installed_members)
    for script_name, payload in source_scripts.items():
        wheel_members[f"fixture_jarvis-1.0.data/scripts/{script_name}"] = payload
    wheel_record_rows = [
        _record_row(relative, payload) for relative, payload in sorted(wheel_members.items())
    ]
    wheel_record_rows.append(f"{record_name},,")
    wheel_record = ("\n".join(wheel_record_rows) + "\n").encode()
    wheel = tmp_path / "fixture_jarvis-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative, payload in wheel_members.items():
            archive.writestr(relative, payload)
        archive.writestr(record_name, wheel_record)
    return invoked_python, executable, wheel, installed_scripts, record_members


def _create_wheel_data_fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    environment = tmp_path / "environment"
    venv.EnvBuilder(with_pip=False, symlinks=os.name != "nt").create(environment)
    invoked_python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    completed = subprocess.run(
        [
            str(invoked_python),
            "-I",
            "-c",
            (
                "import json, sysconfig; "
                "print(json.dumps({'purelib': sysconfig.get_path('purelib'), "
                "'data': sysconfig.get_path('data')}))"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    paths = cast(dict[str, str], json.loads(completed.stdout))
    site_packages = Path(paths["purelib"])
    data_root = Path(paths["data"])
    metadata_root = "fixture_jarvis-1.0.dist-info"
    record_name = f"{metadata_root}/RECORD"
    wheel_data_name = "fixture_jarvis-1.0.data/data/wfcommons-schema.json"
    data_payload = b'{"schema": "fixture"}\n'
    installed_data = data_root / "wfcommons-schema.json"
    installed_data.parent.mkdir(parents=True, exist_ok=True)
    installed_data.write_bytes(data_payload)
    members = {
        "fixture_jarvis/__init__.py": b'"""Fixture package."""\n',
        f"{metadata_root}/METADATA": (
            b"Metadata-Version: 2.1\nName: fixture-jarvis\nVersion: 1.0\n\n"
        ),
        f"{metadata_root}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n"
        ),
    }
    installed_record_rows: list[str] = []
    for relative, payload in sorted(members.items()):
        destination = site_packages / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        installed_record_rows.append(_record_row(relative, payload))
    installed_data_relative = os.path.relpath(installed_data, site_packages).replace("\\", "/")
    installed_record_rows.append(_record_row(installed_data_relative, data_payload))
    installed_record_rows.append(f"{record_name},,")
    installed_record = site_packages / record_name
    installed_record.write_text("\n".join(installed_record_rows) + "\n", encoding="utf-8")

    wheel_members = {**members, wheel_data_name: data_payload}
    wheel_record_rows = [
        _record_row(relative, payload) for relative, payload in sorted(wheel_members.items())
    ]
    wheel_record_rows.append(f"{record_name},,")
    wheel = tmp_path / "fixture_jarvis-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative, payload in wheel_members.items():
            archive.writestr(relative, payload)
        archive.writestr(record_name, ("\n".join(wheel_record_rows) + "\n").encode())
    return invoked_python, wheel, installed_record, installed_data


def _refresh_installed_record_member(
    python: Path,
    distribution_name: str,
    relative: str,
    payload: bytes,
) -> None:
    completed = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            (
                "from importlib import metadata; "
                "print(metadata.distribution('"
                + distribution_name
                + "').locate_file('fixture_jarvis-1.0.dist-info/RECORD'))"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    record = Path(completed.stdout.strip())
    replacement = _record_row(relative, payload)
    rows = record.read_text(encoding="utf-8").splitlines()
    matching = [index for index, row in enumerate(rows) if row.startswith(relative + ",")]
    assert len(matching) == 1
    rows[matching[0]] = replacement
    record.write_text("\n".join(rows) + "\n", encoding="utf-8")


@pytest.mark.parametrize("posix_launcher", ["uv", "pip"])
def test_native_jarvis_record_closure_verifies_standard_wheel_scripts(
    tmp_path: Path,
    posix_launcher: str,
) -> None:
    python, _, wheel, _, _ = _create_wheel_scripts_fixture(
        tmp_path,
        posix_launcher=posix_launcher,
    )
    probe = wheel_record_closure_module._probe_python_distribution_record_closure  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    verified = probe(str(python), "fixture-jarvis", wheel)

    assert verified["verified"] is True
    assert verified["wheel_script_transform_count"] == 2
    transforms = {
        cast(str, item["member"]): item
        for item in cast(list[dict[str, object]], verified["wheel_script_transforms"])
    }
    console = transforms["fixture_jarvis-1.0.data/scripts/fixture-jarvis"]
    resource = transforms["fixture_jarvis-1.0.data/scripts/fixture-resource"]
    assert console["transform"] == "declared-console-wrapper"
    assert console["entry_point"] == "fixture_jarvis.cli:main"
    assert resource["transform"] == "interpreter-shebang"
    assert resource["entry_point"] is None
    expected_launcher = (
        "direct-interpreter" if os.name == "nt" else f"{posix_launcher}-posix-trampoline"
    )
    assert console["launcher"] == expected_launcher
    assert resource["launcher"] == expected_launcher


def test_native_jarvis_record_closure_verifies_standard_wheel_data(
    tmp_path: Path,
) -> None:
    python, wheel, installed_record, installed_data = _create_wheel_data_fixture(tmp_path)
    probe = wheel_record_closure_module._probe_python_distribution_record_closure  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    verified = probe(str(python), "fixture-jarvis", wheel)

    assert verified["verified"] is True
    assert verified["wheel_payload_file_count"] == 4

    relative = os.path.relpath(installed_data, installed_record.parent.parent).replace("\\", "/")
    rows = installed_record.read_text(encoding="utf-8").splitlines()
    installed_record.write_text(
        "\n".join(row for row in rows if not row.startswith(relative + ",")) + "\n",
        encoding="utf-8",
    )
    unowned = probe(str(python), "fixture-jarvis", wheel)

    assert unowned["verified"] is False
    assert unowned["error_code"] == "installed-wheel-data-file-is-not-owned-by-distribution-RECORD"


@pytest.mark.parametrize("tamper", ["resource-body", "console-wrapper", "trampoline"])
def test_native_jarvis_record_closure_rejects_arbitrary_script_transforms(
    tmp_path: Path,
    tamper: str,
) -> None:
    python, executable, wheel, scripts, record_members = _create_wheel_scripts_fixture(tmp_path)
    script_name = "fixture-resource" if tamper != "console-wrapper" else "fixture-jarvis"
    script = scripts[script_name]
    payload = script.read_bytes()
    if tamper == "resource-body":
        payload = _bound_python_script_header(executable) + b"print('substituted body')\n"
        expected_error = "body does not match"
    elif tamper == "console-wrapper":
        payload += b"print('injected statement')\n"
        expected_error = "not a canonical declared wrapper"
    else:
        if os.name == "nt":
            payload = payload.replace(
                f"#!{executable}\n".encode(),
                f"#!{executable} -I\n".encode(),
                1,
            )
        else:
            payload = payload.replace(b' "$0" "$@"\n', b' "$0" "$@"; echo injected\n', 1)
        expected_error = "not bound" if os.name == "nt" else "invalid POSIX shell trampoline"
    script.write_bytes(payload)
    if os.name != "nt":
        script.chmod(0o755)
    _refresh_installed_record_member(
        python,
        "fixture-jarvis",
        record_members[script_name],
        payload,
    )
    probe = wheel_record_closure_module._probe_python_distribution_record_closure  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    rejected = probe(str(python), "fixture-jarvis", wheel)

    assert rejected["verified"] is False
    assert expected_error in str(rejected["error"])
    assert rejected["error_code"] != "unclassified-record-closure-error"


def test_native_jarvis_record_closure_rejects_a_tampered_installed_member(
    tmp_path: Path,
) -> None:
    environment = tmp_path / "environment"
    venv.EnvBuilder(with_pip=False, symlinks=os.name != "nt").create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    completed = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import sysconfig; print(sysconfig.get_paths()['purelib'])",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    site_packages = Path(completed.stdout.strip())
    package = site_packages / "fixture_jarvis"
    metadata_root = site_packages / "fixture_jarvis-1.0.dist-info"
    package.mkdir(parents=True)
    metadata_root.mkdir()
    members = {
        "fixture_jarvis/__init__.py": b'"""Fixture package."""\n',
        "fixture_jarvis/runtime.py": b"VALUE = 1\n",
        "fixture_jarvis-1.0.dist-info/METADATA": (
            b"Metadata-Version: 2.1\nName: fixture-jarvis\nVersion: 1.0\n\n"
        ),
        "fixture_jarvis-1.0.dist-info/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n"
        ),
    }
    record_name = "fixture_jarvis-1.0.dist-info/RECORD"
    record_lines: list[str] = []
    for relative, payload in sorted(members.items()):
        encoded = urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()
        record_lines.append(f"{relative},sha256={encoded},{len(payload)}")
        destination = site_packages / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
    record_lines.append(f"{record_name},,")
    record = ("\n".join(record_lines) + "\n").encode()
    (site_packages / record_name).write_bytes(record)
    wheel = tmp_path / "fixture_jarvis-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative, payload in members.items():
            archive.writestr(relative, payload)
        archive.writestr(record_name, record)
    probe = wheel_record_closure_module._probe_python_distribution_record_closure  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    verified = probe(str(python), "fixture-jarvis", wheel)

    assert verified["verified"] is True
    assert verified["wheel_payload_file_count"] == len(members)
    assert verified["tree_scanned"] is False
    assert verified["tree_copied"] is False

    (package / "runtime.py").write_bytes(b"VALUE = 2\n")
    tampered = probe(str(python), "fixture-jarvis", wheel)

    assert tampered["verified"] is False
    assert "digest mismatch" in str(tampered["error"])
