from __future__ import annotations

import json
import re
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import cast

import yaml

from clio_relay.models import ServiceRuntimeSpec

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "examples" / "release-gate"
RUNBOOK = ROOT / "docs" / "release-acceptance-1.0.md"
POLICY = ROOT / "docs" / "release-gate-1.0.yaml"
RELEASE_PROCESS = ROOT / "docs" / "release.md"


def _render(path: Path, replacements: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8")
    for name, value in replacements.items():
        text = text.replace(f"__{name}__", value)
    assert "__" not in text
    return text


def _matrix_reports() -> list[dict[str, object]]:
    document = cast(
        dict[str, object],
        json.loads((FIXTURES / "report-matrix-1.0.json").read_text(encoding="utf-8")),
    )
    assert document["report_count_per_stage"] == 17
    return cast(list[dict[str, object]], document["reports"])


def test_release_identity_is_consistent_across_package_policy_matrix_and_runbook() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    policy = cast(dict[str, object], yaml.safe_load(POLICY.read_text(encoding="utf-8")))
    matrix = cast(
        dict[str, object],
        json.loads((FIXTURES / "report-matrix-1.0.json").read_text(encoding="utf-8")),
    )
    version = cast(dict[str, object], project["project"])["version"]
    relay_lock = next(
        package
        for package in cast(list[dict[str, object]], lock["package"])
        if package["name"] == "clio-relay"
    )
    init_source = (ROOT / "src" / "clio_relay" / "__init__.py").read_text(encoding="utf-8")
    release_process = RELEASE_PROCESS.read_text(encoding="utf-8")

    assert version == "1.3.0"
    assert relay_lock["version"] == version
    assert f'__version__ = "{version}"' in init_source
    assert policy["release_version"] == version
    assert matrix["release_version"] == version
    assert f'$Version = "{version}"' in RUNBOOK.read_text(encoding="utf-8")
    assert f'$Tag = "v{version}"' in release_process
    assert f'--title "clio-relay {version}"' in release_process
    assert f"> Version `{version}` uses a release-first patch process." in (
        ROOT / "README.md"
    ).read_text(encoding="utf-8")


def test_candidate_manifest_parser_accepts_the_exact_gnu_checksum_separator() -> None:
    digest = "a" * 64
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    version = cast(dict[str, object], project["project"])["version"]
    wheel_name = f"clio_relay-{version}-py3-none-any.whl"
    selector = re.compile(rf"^[0-9A-Fa-f]{{64}} [ *]{re.escape(wheel_name)}$")
    parser = re.compile(r"^([0-9A-Fa-f]{64}) [ *](.+)$")

    for mode in ("*", " "):
        line = f"{digest} {mode}{wheel_name}"
        assert selector.fullmatch(line) is not None
        match = parser.fullmatch(line)
        assert match is not None
        assert match.groups() == (digest, wheel_name)

    assert selector.fullmatch(f"{digest}*{wheel_name}") is None


def test_spack_json_array_fallback_is_safe_under_powershell_strict_mode() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    assert "$AdiosDecoded.specs" not in runbook
    assert '$AdiosSpecs = $AdiosDecoded.PSObject.Properties["specs"]' in runbook
    assert "$AdiosRecords = @(" in runbook
    assert "if ($null -ne $AdiosSpecs) { $AdiosSpecs.Value } else { $AdiosDecoded }" in runbook
    assert "$Record.$Property" not in runbook
    assert "$Candidate = $Record.PSObject.Properties[$Property]" in runbook
    assert "$Value = [string]$Candidate.Value" in runbook


def test_remote_release_text_fixtures_are_lf_attributed_and_normalized_before_scp() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    attributes = (ROOT / ".gitattributes").read_text(encoding="utf-8").splitlines()
    remote_text_fixtures = {
        "examples/release-gate/gray-scott-direct-build.sh",
        "examples/release-gate/spack-fresh-store.sh",
        "examples/release-gate/gateway/slurm_submit.sh",
        "examples/release-gate/gateway/http_service.py",
        "examples/release-gate/gateway/external_runtime.py",
        "examples/release-gate/gateway/slurm_status.py",
        "examples/release-gate/gateway/slurm_cancel.py",
        "examples/release-gate/lammps-bounded.in",
    }
    staged_sources = set(
        re.findall(
            r'^\s*-Source "(examples/release-gate/[^"]+)"',
            runbook,
            flags=re.MULTILINE,
        )
    )

    assert "*.sh text eol=lf" in attributes
    assert "examples/release-gate/**/*.py text eol=lf" in attributes
    assert "examples/release-gate/**/*.in text eol=lf" in attributes
    assert "examples/release-gate/**/*.tmpl text eol=lf" in attributes
    assert staged_sources == remote_text_fixtures
    assert runbook.count("& $OpenScp") == 1
    assert "function New-LfTextStagingCopy" in runbook
    assert "function Copy-RemoteTextFile" in runbook
    assert '& $OpenScp $StagingPath "${SshHost}:$RemotePath" | Out-Host' in runbook
    assert '$Text.Replace("`r`n", "`n").Replace("`r", "`n")' in runbook
    assert "$StagedBytes -contains [byte]0x0D" in runbook
    assert "$StagedBytes -contains [byte]0x00" in runbook
    assert "$Text = Read-LfUtf8TextFile $Source" in runbook
    assert '$Text = ConvertTo-LfText $Text "rendered template $Destination"' in runbook


def test_lf_text_staging_and_rendering_normalize_windows_bytes_under_powershell(
    tmp_path: Path,
) -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    function_sources: list[str] = []
    for name in (
        "ConvertTo-LfText",
        "Read-LfUtf8TextFile",
        "Render-Template",
        "New-LfTextStagingCopy",
    ):
        function_match = re.search(
            rf"^function {re.escape(name)} \{{.*?^\}}",
            runbook,
            flags=re.DOTALL | re.MULTILINE,
        )
        assert function_match is not None
        function_sources.append(function_match.group(0))
    template = tmp_path / "windows-template.yaml.tmpl"
    template.write_bytes(b"\xef\xbb\xbfvalue: __VALUE__\r\n")
    rendered_root = tmp_path / "rendered"
    powershell = next(
        (
            executable
            for name in ("powershell.exe", "pwsh", "powershell")
            if (executable := shutil.which(name)) is not None
        ),
        None,
    )
    assert powershell is not None, "PowerShell is required to validate the release runbook"
    script_path = tmp_path / "normalize-shell-script.ps1"
    script_path.write_text(
        "\n".join(
            (
                (
                    "param([string] $Source, [string] $OutputRoot, "
                    "[string] $Template, [string] $StagingName)"
                ),
                "Set-StrictMode -Version Latest",
                "$ErrorActionPreference = 'Stop'",
                "$RenderedRoot = $OutputRoot",
                *function_sources,
                "$Staged = New-LfTextStagingCopy $Source $StagingName",
                "$Rendered = Join-Path $RenderedRoot 'rendered.yaml'",
                "Render-Template $Template $Rendered @{ VALUE = 'ready' }",
                "$StagedBytes = [IO.File]::ReadAllBytes($Staged)",
                "$RenderedBytes = [IO.File]::ReadAllBytes($Rendered)",
                "$StrictUtf8 = [Text.UTF8Encoding]::new($false, $true)",
                "[pscustomobject]@{",
                "  staged_hex = [BitConverter]::ToString($StagedBytes)",
                "  staged_text = $StrictUtf8.GetString($StagedBytes)",
                "  rendered_hex = [BitConverter]::ToString($RenderedBytes)",
                "  rendered_text = $StrictUtf8.GetString($RenderedBytes)",
                "} | ConvertTo-Json -Compress",
            )
        ),
        encoding="utf-8",
    )

    def invoke(source: Path, staging_name: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                powershell,
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script_path),
                str(source),
                str(rendered_root),
                str(template),
                staging_name,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )

    cases = (
        (
            "fixture.sh",
            b"\xef\xbb\xbf#!/usr/bin/env bash\r\nprintf '%s\\n' ready\r\n",
            "#!/usr/bin/env bash\nprintf '%s\\n' ready\n",
        ),
        ("fixture.py", b"print('ready')\r", "print('ready')\n"),
        ("fixture.in", b"\xef\xbb\xbfunits lj\r\nrun 1", "units lj\nrun 1\n"),
    )
    for name, original, expected in cases:
        source = tmp_path / name
        source.write_bytes(original)
        result = invoke(source, f"normalized-{name}")

        assert result.returncode == 0, result.stderr
        document = json.loads(result.stdout)
        assert "EF-BB-BF" not in document["staged_hex"]
        assert "-0D-" not in f"-{document['staged_hex']}-"
        assert "-00-" not in f"-{document['staged_hex']}-"
        assert document["staged_text"] == expected
        assert document["rendered_hex"].startswith("76-61-")
        assert "EF-BB-BF" not in document["rendered_hex"]
        assert "-0D-" not in f"-{document['rendered_hex']}-"
        assert document["rendered_text"] == "value: ready\n"
        assert source.read_bytes() == original
        if name.endswith(".py"):
            compile(document["staged_text"], name, "exec")

    bash = shutil.which("bash")
    if bash is not None:
        syntax = subprocess.run(
            [bash, "-n"],
            check=False,
            capture_output=True,
            input=(rendered_root / "remote-text" / "normalized-fixture.sh").read_bytes(),
            timeout=30,
        )
        assert syntax.returncode == 0, syntax.stderr.decode(errors="replace")
    assert template.read_bytes() == b"\xef\xbb\xbfvalue: __VALUE__\r\n"

    collision = invoke(tmp_path / "fixture.sh", "normalized-fixture.sh")
    assert collision.returncode != 0
    assert "refusing to replace normalized text staging copy" in collision.stderr

    nul_source = tmp_path / "contains-nul.py"
    nul_source.write_bytes(b"print('before')\r\n\x00print('after')\r\n")
    nul_rejected = invoke(nul_source, "contains-nul.py")
    assert nul_rejected.returncode != 0
    assert "contains a NUL byte" in nul_rejected.stderr

    invalid_source = tmp_path / "invalid-utf8.in"
    invalid_source.write_bytes(b"valid\r\n\xffinvalid")
    utf8_rejected = invoke(invalid_source, "invalid-utf8.in")
    assert utf8_rejected.returncode != 0
    assert "is not valid UTF-8" in utf8_rejected.stderr


def test_remote_release_programs_parse_after_lf_normalization() -> None:
    python_programs = sorted((FIXTURES / "gateway").glob("*.py"))
    assert [path.name for path in python_programs] == [
        "external_runtime.py",
        "http_service.py",
        "slurm_cancel.py",
        "slurm_status.py",
    ]
    for path in python_programs:
        normalized = path.read_bytes().decode("utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
        compile(normalized, str(path), "exec")

    bash = shutil.which("bash")
    if bash is None:
        return
    shell_programs = sorted(FIXTURES.rglob("*.sh"))
    assert [path.relative_to(FIXTURES).as_posix() for path in shell_programs] == [
        "gateway/slurm_submit.sh",
        "gray-scott-direct-build.sh",
        "spack-fresh-store.sh",
    ]
    for path in shell_programs:
        normalized = path.read_bytes().decode("utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
        result = subprocess.run(
            [bash, "-n"],
            input=normalized.encode("utf-8"),
            check=False,
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 0, f"{path}: {result.stderr.decode(errors='replace')}"


def test_owned_session_helper_isolates_command_output_from_its_return_value(
    tmp_path: Path,
) -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    function_match = re.search(
        r"^function Start-OwnedSession \{.*?^\}",
        runbook,
        flags=re.DOTALL | re.MULTILINE,
    )
    assert function_match is not None
    function_source = function_match.group(0)
    assert "--remote-api-port $RemotePort --require-token | Out-Host" in function_source

    powershell = next(
        (
            executable
            for name in ("pwsh", "powershell.exe", "powershell")
            if (executable := shutil.which(name)) is not None
        ),
        None,
    )
    assert powershell is not None, "PowerShell is required to validate the release runbook"
    script_path = tmp_path / "owned-session-output.ps1"
    script_path.write_text(
        "\n".join(
            (
                "Set-StrictMode -Version Latest",
                "$global:LASTEXITCODE = 0",
                "$Relay = {",
                "  param([Parameter(ValueFromRemainingArguments=$true)] [object[]] $Command)",
                "  $global:LASTEXITCODE = 0",
                "  if (($Command -join ' ') -like 'session start *') {",
                "    'session_started=owned-session'",
                "    'api_pid=123'",
                "    'remote_api_port=9001'",
                "    return",
                "  }",
                "  if (($Command -join ' ') -like 'session status *') {",
                '    \'{"session_generation_id":"generation-123"}\'',
                "    return",
                "  }",
                '  throw "unexpected relay command: $Command"',
                "}",
                function_source,
                "$Generation = @(Start-OwnedSession 'ares' 'owned-session' 9001)",
                "[pscustomobject]@{",
                "  count = $Generation.Count",
                "  value = [string]$Generation[0]",
                "} | ConvertTo-Json -Compress",
            )
        ),
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            powershell,
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    output_lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert output_lines[:3] == [
        "session_started=owned-session",
        "api_pid=123",
        "remote_api_port=9001",
    ]
    assert json.loads(output_lines[-1]) == {"count": 1, "value": "generation-123"}


def test_release_helper_scripts_bind_direct_gray_scott_and_private_spack_state() -> None:
    gray = (FIXTURES / "gray-scott-direct-build.sh").read_text(encoding="utf-8")
    fresh_spack = (FIXTURES / "spack-fresh-store.sh").read_text(encoding="utf-8")

    assert "https://github.com/iowarp/clio-core.git" in gray
    assert "rev-parse HEAD:external/iowarp-gray-scott" in gray
    assert 'build-env "/$adios_hash"' in gray
    assert 'location -i "/$adios_hash"' in gray
    assert 'if ! adios_prefix="$("$spack" location -i "/$adios_hash")"; then' in gray
    assert 'if ! wait "$find_pid"; then' in gray
    assert "selected ADIOS2 prefix must contain exactly one CMake package config" in gray
    assert '-DADIOS2_DIR="$adios2_dir"' in gray
    assert '[[ "$value" == *//* ]]' in gray
    assert '[[ "$value" == *"/./"* ]]' in gray
    assert "coeus" not in gray.lower()
    assert "hermes" not in gray.lower()

    assert "concretizer:\n  reuse: false" in fresh_spack
    assert "mirrors:: {}" in fresh_spack
    assert "upstreams:: {}" in fresh_spack
    assert "SPACK_USER_CONFIG_PATH" in fresh_spack
    assert "acceptance-manifest.sha256" in fresh_spack
    assert "sha256sum --check --strict" in fresh_spack
    assert '[[ "$value" == *//* ]]' in fresh_spack
    assert '[[ "$value" == *"/./"* ]]' in fresh_spack


def test_gray_scott_helper_fails_closed_on_spack_and_config_discovery_errors(
    tmp_path: Path,
) -> None:
    bash = shutil.which("bash")
    assert bash is not None, "Bash is required to validate the remote release helper"
    helper = (FIXTURES / "gray-scott-direct-build.sh").read_bytes()
    helper = helper.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    commit = "e2fedd8847f8deb71f041f692e405023a712ca44"
    tree = "072d6eab3df3bde92e48ae2f4823305af831535e"
    adios_hash = "wqc5dwbj7vx4i3oekrembw6irpo5h7g6"

    def invoke(
        name: str,
        *,
        location_exit: int = 0,
        find_program: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        root = tmp_path / name
        config = root / "adios" / "lib" / "cmake" / "adios2" / "adios2-config.cmake"
        config.parent.mkdir(parents=True)
        config.write_bytes(b"# test config\n")
        (root / "helper.sh").write_bytes(helper)
        (root / "spack").write_bytes(
            (
                "#!/usr/bin/env bash\n"
                'if [ "$1" = location ]; then\n'
                "  printf '%s\\n' \"$PWD/adios\"\n"
                f"  exit {location_exit}\n"
                "fi\n"
                "exit 64\n"
            ).encode()
        )
        fake_bin = root / "fake-bin"
        fake_bin.mkdir()
        if find_program is not None:
            (fake_bin / "find").write_bytes(find_program.encode("utf-8"))
        command = (
            'chmod 700 "$PWD/helper.sh" "$PWD/spack"; '
            'if [ -f "$PWD/fake-bin/find" ]; then '
            'chmod 700 "$PWD/fake-bin/find"; fi; '
            'export PATH="$PWD/fake-bin:$PATH"; '
            '"$PWD/helper.sh" "$PWD/spack" "$PWD/source" "$PWD/build" '
            f'"$PWD/install" "{commit}" "{tree}" "{adios_hash}"'
        )
        return subprocess.run(
            [bash, "-c", command],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )

    location_failure = invoke("location-failure", location_exit=23)
    assert location_failure.returncode == 66
    assert "selected ADIOS2 prefix lookup failed" in location_failure.stderr

    partial_find_failure = invoke(
        "find-failure",
        find_program=(
            "#!/usr/bin/env bash\n"
            "printf '%s\\0' \"$PWD/adios/lib/cmake/adios2/adios2-config.cmake\"\n"
            "exit 19\n"
        ),
    )
    assert partial_find_failure.returncode == 74
    assert "ADIOS2 CMake package discovery failed" in partial_find_failure.stderr

    empty_find = invoke(
        "find-empty",
        find_program="#!/usr/bin/env bash\nexit 0\n",
    )
    assert empty_find.returncode == 65
    assert "must contain exactly one CMake package config" in empty_find.stderr

    ambiguous_find = invoke(
        "find-ambiguous",
        find_program=(
            "#!/usr/bin/env bash\n"
            "printf '%s\\0%s\\0' "
            '"$PWD/adios/lib/cmake/adios2/adios2-config.cmake" '
            '"$PWD/adios/lib/cmake/adios2/ADIOS2Config.cmake"\n'
        ),
    )
    assert ambiguous_find.returncode == 65
    assert "must contain exactly one CMake package config" in ambiguous_find.stderr


def test_release_acceptance_matrix_is_complete_ordered_and_unique() -> None:
    reports = _matrix_reports()

    assert [report["ordinal"] for report in reports] == list(range(1, 18))
    assert len({cast(str, report["id"]) for report in reports}) == 17
    assert reports[2]["id"] == "ares-queue-management"
    assert all(
        cast(int, report["ordinal"]) > 3
        for report in reports
        if report["scenario"] in {"cleanup", "transport", "gateway-runtime"}
    )
    jarvis_reports = [
        report for report in reports if cast(str, report["id"]).startswith("ares-jarvis-")
    ]
    assert [report.get("package") for report in jarvis_reports] == [
        "builtin.gray_scott",
        "builtin.lammps",
    ]
    assert [report.get("remote_tool") for report in reports if "remote_tool" in report] == [
        "spack_find",
        "spack_locate",
        "spack_install",
    ]


def test_release_acceptance_upload_checks_derive_matrix_cardinality() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")

    assert "-ne 17" not in runbook
    assert runbook.count("-ne $Matrix.report_count_per_stage") >= 3


def test_release_acceptance_matrix_preserves_evidence_groups() -> None:
    reports = _matrix_reports()
    grouped: dict[str, list[str]] = {}
    for report in reports:
        group = report.get("evidence_group")
        if isinstance(group, str):
            grouped.setdefault(group, []).append(cast(str, report["id"]))

    assert grouped["ares-default-cleanup-session"] == [
        "ares-cleanup-detach",
        "ares-cleanup-teardown",
    ]
    assert grouped["homelab-default-cleanup-session"] == [
        "homelab-cleanup-detach",
        "homelab-cleanup-teardown",
    ]
    assert grouped["ares-dedicated-gateway"] == [
        "ares-gateway-start",
        "ares-gateway-stop",
    ]


def test_release_policy_groups_bootstrap_and_worker_proof_by_physical_target() -> None:
    document = cast(dict[str, object], yaml.safe_load(POLICY.read_text(encoding="utf-8")))
    requirements = cast(list[dict[str, object]], document["requirements"])
    bootstrap = next(
        requirement
        for requirement in requirements
        if requirement["requirement_id"] == "ares-released-bootstrap"
    )

    assert bootstrap["evidence_group_resource_kind"] == "cluster_target"


def test_release_policy_requires_existing_lammps_and_fresh_install_transition() -> None:
    document = cast(dict[str, object], yaml.safe_load(POLICY.read_text(encoding="utf-8")))
    requirements = cast(list[dict[str, object]], document["requirements"])
    requirement = next(
        item for item in requirements if item["requirement_id"] == "ares-spack-virtual-mcp"
    )
    checks = cast(list[str], requirement["required_checks"])
    assert "remote-mcp.structured-result" in checks

    resources = cast(list[dict[str, object]], requirement["required_resources"])
    calls = {
        cast(
            str, cast(dict[str, object], resource["metadata_equals"])["remote_mcp_tool_name"]
        ): cast(dict[str, object], resource["metadata_equals"])
        for resource in resources
        if resource["kind"] == "relay_job"
    }
    expected_hash = "p5gjmq4rseitqanua7mdd2zdnag4v3u2"
    assert set(calls) == {"spack_find", "spack_locate"}
    for tool, metadata in calls.items():
        assertion = cast(dict[str, object], metadata["structured_result_assertion"])
        expected = cast(dict[str, object], assertion["expected"])
        observed = cast(dict[str, object], assertion["observed"])
        assert expected["tool"] == tool
        assert expected["dag_hash"] == expected_hash
        assert observed["structured_content_present"] is True
        assert assertion["failures"] == []
    locate = cast(
        dict[str, object],
        calls["spack_locate"]["structured_result_assertion"],
    )
    locate_observed = cast(dict[str, object], locate["observed"])
    assert locate_observed["load_spec"] == f"/{expected_hash}"
    expected_prefix = (
        "/mnt/common/jcernudagarcia/spack/opt/spack/"
        "linux-ubuntu22.04-skylake_avx512/gcc-11.4.0/"
        f"lammps-20240829.1-{expected_hash}"
    )
    assert cast(dict[str, object], locate["expected"])["prefix"] == expected_prefix
    assert locate_observed["prefix"] == expected_prefix
    assert locate_observed["prefix_is_canonical_absolute"] is True
    assert locate_observed["prefix_matches_expected"] is True
    fresh = next(
        item for item in requirements if item["requirement_id"] == "ares-spack-fresh-install"
    )
    assert cast(list[str], fresh["required_checks"]) == [
        "remote-mcp.spack-preinstall-absent",
        "remote-mcp.spack-fresh-install",
        "remote-mcp.spack-postinstall-locate",
        "remote-mcp.spack-disposable-store",
        "remote-mcp.spack-transition-identity",
        "remote-mcp.spack-transition-durable-evidence",
        "remote-mcp.spack-fresh-configuration",
    ]
    transition = cast(dict[str, object], fresh["spack_fresh_install_transition"])
    assert transition == {
        "schema_version": "clio-relay.release-spack-fresh-install.v1",
        "server_name": "spack-fresh",
        "profile": "user",
        "package_name": "libsigsegv",
        "requested_spec": "libsigsegv@2.14",
        "reuse": False,
    }
    fresh_resources = cast(list[dict[str, object]], fresh["required_resources"])
    phase_roles = {
        cast(list[str], resource["roles"])[0]
        for resource in fresh_resources
        if resource["kind"] == "relay_job"
    }
    assert phase_roles == {
        "spack_preinstall_find",
        "spack_fresh_install",
        "spack_postinstall_locate",
    }
    assert any(resource["kind"] == "configuration_manifest" for resource in fresh_resources)


def test_release_acceptance_yaml_templates_are_real_bounded_pipelines() -> None:
    replacements = {
        "RUN_ID": "released-1-0-acceptance",
        "REMOTE_ROOT": "/tmp/clio-relay-release-acceptance",
    }
    for name in (
        "ares-bootstrap-echo.yaml.tmpl",
        "homelab-transport-echo.yaml.tmpl",
        "owned-cleanup-ares.yaml.tmpl",
        "owned-cleanup-homelab.yaml.tmpl",
        "owned-cancel-ares.yaml.tmpl",
    ):
        document = cast(dict[str, object], yaml.safe_load(_render(FIXTURES / name, replacements)))
        packages = cast(list[dict[str, object]], document["pkgs"])
        assert packages
        assert all(isinstance(package.get("pkg_type"), str) for package in packages)


def test_release_acceptance_gateway_templates_match_runtime_contract() -> None:
    replacements = {
        "RUN_ID": "released-1-0-gateway",
        "REMOTE_FIXTURE_ROOT": "/tmp/clio-relay-release-fixtures",
        "REMOTE_STATE_ROOT": "/tmp/clio-relay-release-state",
        "SERVICE_PORT": "19080",
        "DESKTOP_PORT": "29080",
        "HEALTH_NONCE": "a" * 64,
    }
    ares = ServiceRuntimeSpec.model_validate_json(
        _render(FIXTURES / "gateway" / "ares-runtime.json.tmpl", replacements)
    )
    homelab = ServiceRuntimeSpec.model_validate_json(
        _render(FIXTURES / "gateway" / "homelab-runtime.json.tmpl", replacements)
    )

    assert ares.scheduler == "slurm"
    assert ares.deployment_driver == "scheduler"
    assert ares.status_command is not None and "{scheduler_job_id}" in ares.status_command
    assert ares.health_expected_body == "a" * 64
    assert homelab.scheduler == "external"
    assert homelab.deployment_driver == "custom"
    assert homelab.cancel_command is not None and "{scheduler_job_id}" in homelab.cancel_command
    assert homelab.health_expected_body == "a" * 64


def test_release_acceptance_fixture_lifetimes_dominate_operation_budgets() -> None:
    ares_submit = (FIXTURES / "gateway" / "slurm_submit.sh").read_text(encoding="utf-8")
    homelab_runtime = (FIXTURES / "gateway" / "homelab-runtime.json.tmpl").read_text(
        encoding="utf-8"
    )
    ares_cleanup = (FIXTURES / "owned-cleanup-ares.yaml.tmpl").read_text(encoding="utf-8")
    ares_cancel = (FIXTURES / "owned-cancel-ares.yaml.tmpl").read_text(encoding="utf-8")
    homelab_cleanup = (FIXTURES / "owned-cleanup-homelab.yaml.tmpl").read_text(encoding="utf-8")

    assert "--lifetime-seconds 900" in ares_submit
    assert "--time 00:20:00" in ares_submit
    assert '"600"' in homelab_runtime
    assert 'time: "00:20:00"' in ares_cleanup and "sleep 900" in ares_cleanup
    assert 'time: "00:30:00"' in ares_cancel and "sleep 1200" in ares_cancel
    assert "sleep 900" in homelab_cleanup and "timeout_seconds: 960" in homelab_cleanup


def test_release_acceptance_runbook_binds_production_specifics_without_secrets() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "Windows PowerShell 5.1 or newer" in text
    assert "uv tool install --force --python 3.12 --no-config" in text
    assert "uv tool install --force --refresh --no-config" in text
    assert "CLIO_RELAY_VALIDATION_INVOCATION_ID" in text
    assert '--validation-scenario", "cluster-bootstrap' in text
    assert "--verify-cluster-deployment" in text
    assert "--require-structured-runtime-metadata" in text
    assert '$GrayExecutable = "$GrayInstallRoot/bin/gray-scott"' in text
    assert "examples/release-gate/gray-scott-direct-build.sh" in text
    assert '$ExpectedCoreCommit = "e2fedd8847f8deb71f041f692e405023a712ca44"' in text
    assert '$ExpectedGrayTree = "072d6eab3df3bde92e48ae2f4823305af831535e"' in text
    assert "$ExpectedAdiosHash = [string]$env:CLIO_RELAY_ACCEPTANCE_ADIOS_HASH" in text
    assert "find --json '/$ExpectedAdiosHash'" in text
    assert "find --json adios2" not in text
    assert "ADIOS2_DIR" in text
    assert 'spack_specs = @("/$ExpectedAdiosHash")' in text
    assert 'spack_specs = @("/$ExpectedLammpsHash")' in text
    assert 'spack_specs = @("adios2-coeus")' not in text
    assert "find --format '{hash}' adios2-coeus" not in text
    slurm_submit = (FIXTURES / "gateway" / "slurm_submit.sh").read_text(encoding="utf-8")
    assert '[[ ! "$job_id" =~ ^[0-9]+$ ]]' in slurm_submit
    assert "examples/release-gate/spack-fresh-store.sh" in text
    assert "spec --format '{hash}' '$FreshSpackSpec'" in text
    assert "fresh_install_store_root = $FreshSpackStore" in text
    assert "fresh_install_configuration_manifest_path = $FreshSpackConfigurationManifest" in text
    assert "fresh_install_configuration_sha256 = $ExpectedFreshSpackConfigurationSha256" in text
    assert '"--name", "spack-fresh"' in text
    assert '$ExpectedLammpsHash = "p5gjmq4rseitqanua7mdd2zdnag4v3u2"' in text
    assert "$LammpsHashes.Count -ne 1" in text
    assert "$ExpectedLammpsPrefix" in text
    assert "expected exact LAMMPS prefix is unavailable or non-canonical" in text
    assert "--contract clio-kit-spack-user-v2" in text
    assert "--allow-tool spack_find --allow-tool spack_locate --allow-tool spack_install" in text
    assert '"--result-expectation-json-file", $Expectation' in text
    assert '--keep-jobs", "--keep-scheduler-jobs' in text
    assert '--cancel-jobs", "--cancel-scheduler-jobs' in text
    assert "--preserve-scheduler-job-id" in text
    assert '{ "validation" } else { "released-validation" }' in text
    assert '"--transport-local-bind-port"' in text
    assert '"--transport-remote-api-port"' in text
    assert '"--ssh-transport-local-bind-port"' in text
    assert '"--ssh-transport-remote-api-port"' in text
    assert '"--no-verify-direct-transport"' in text
    assert '"--no-allow-direct-transport-fallback"' in text
    assert "cluster registry contains a blank secret environment-variable name" in text
    assert '"--transport-local-port"' not in text
    assert '"--transport-remote-port"' not in text
    assert '"--ssh-transport-local-port"' not in text
    assert '"--ssh-transport-remote-port"' not in text
    assert "not product allowlists" in text
    assert "REPLACE_WITH_VERIFIED_CANDIDATE_MANIFEST_DIGEST" not in text
    assert '$GitHubRepo = "github.com/iowarp/clio-relay"' in text
    assert "gh release download $Tag --repo $GitHubRepo" in text
    assert "gh api --hostname github.com user" in text
    assert "$ManifestLines.Count -ne 1" in text
    assert "^[0-9A-Fa-f]{64} [ *]$([Regex]::Escape($WheelName))$" in text
    assert "^([0-9A-Fa-f]{64}) [ *](.+)$" in text
    assert "$Observed -ne $ExpectedWheelSha256" in text
    assert "gh attestation verify $Wheel --hostname github.com --repo iowarp/clio-relay" in text
    assert "$CheckoutCommit -ne $TagCommit" in text
    assert "acceptance tag checkout is not clean" in text
    assert "$AresSshHost = [string]$AresDefinition.ssh_host" in text
    assert "$AresHome = Get-RemoteHome $AresSshHost" in text
    assert "$AresSpack = [string]$AresDefinition.spack_executable" in text
    assert '$AresFreshBaseSpack = "$AresHome/spack/bin/spack"' in text
    assert "'$FreshSpackRoot' '$AresFreshBaseSpack'" in text
    assert "$Promotion.version -cne $Version" in text
    assert "promotion wheel digest is missing or malformed" in text
    index_cleanup = text.index("$AllowedUvLocationEnvironment = @(")
    candidate_install = text.index("uv tool install --force --python 3.12 --no-config")
    assert index_cleanup < candidate_install
    assert '$_.Name -like "UV_*"' in text
    assert '@("UV_CACHE_DIR", "UV_TOOL_DIR", "UV_TOOL_BIN_DIR")' in text
    assert "--default-index https://pypi.org/simple $Wheel" in text
    assert "$AdiosLibraries.Count -eq 0" in text
    assert "$MpiLibraries.Count -ne 1" in text
    assert "health_expected_body" not in text
    assert "HEALTH_NONCE = $AresDefaultHealthNonce" in text
    assert "desktop acceptance port is already occupied" in text
    assert "[string] $SessionId, [string] $Generation," in text
    assert "$Relay session submit-jarvis --cluster $Cluster" in text
    assert "--session-generation-id $Generation" in text
    assert "/jobs/jarvis" not in text
    assert 'Authorization = "Bearer' not in text
    assert "$Forward.HasExited" not in text
    assert "$AresDefaultLocalPort, $CancelLocalPort, $HomelabDefaultLocalPort" in text
    assert "$HomelabTransportLocalPort, $HomelabSshTransportLocalPort" in text
    assert "$SentinelStatus.phase" in text
    assert "CLIO_RELAY_FRP_TOKEN=" not in text
    assert "CLIO_RELAY_STCP_SECRET=" not in text
    assert "$_.evidence_trust.invocation_id" in text
    assert "$_.producer.invocation_id" not in text
    assert "$ExpectedNames" in text
    assert "$ActualNames" in text
    assert "policy report file set or order does not match the release matrix" in text
    assert "$Observed.cluster -ne $Expected.cluster" in text
    assert "$Observed.scenario -ne $Expected.scenario" in text
    assert "Invoke-EmergencySessionCleanup" in text
    assert "Invoke-EmergencyGatewayCleanup" in text
    assert "\"emergency-session-$([guid]::NewGuid().ToString('N'))\"" in text
    assert "\"emergency-gateway-$([guid]::NewGuid().ToString('N'))\"" in text
    assert "RELEASED-VALIDATION-BINDING.json" in text
    assert "refusing to modify a sealed evidence stage" in text
    assert "gh release delete-asset $Tag $AssetName --repo $GitHubRepo --yes" in text
    assert "-ConfirmDiscardEntireIncompleteStage" in text
    assert "Do not run this section during a normal acceptance pass" in text
    assert "--clobber" in text


def test_release_acceptance_creates_remote_output_roots_before_first_pipeline() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    root_creation = text.index("remote acceptance root creation failed")
    first_pipeline = text.index('Invoke-RelayReport -Id "ares-cluster-bootstrap-live-test"')
    assert root_creation < first_pipeline


def test_release_process_continues_after_publication_without_a_main_freeze() -> None:
    release = (ROOT / "docs" / "release.md").read_text(encoding="utf-8")
    acceptance = RUNBOOK.read_text(encoding="utf-8")

    assert "Do not wait for pull-request, `main`, tag, or release workflows." in release
    assert "continue live testing while they run" in release
    assert "keep `main` frozen" not in release
    assert "confirm that maintainers have frozen" in acceptance
    assert "candidate tag commit through finalization" in acceptance
    assert "moving the protected tag" in acceptance
