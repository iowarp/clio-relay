"""The launcher's package mode, through the stub, with a ``jarvis`` that records its calls.

The real jarvis-cd run is ``python -m tests.relay_tapis.live_package`` on Ares
(ledger, W10 evidence).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from clio_relay.jobs.handles import JobState
from clio_relay.jobs.spec import JobSpec, PackageRef
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import package_archive, sign_in, use_memory_keyring

from .live_package import guarded_adapter, run_package

pytestmark = [pytest.mark.release_platform("posix"), pytest.mark.asyncio]

FAKE_JARVIS = """#!/bin/bash
{ printf '%s' "$JARVIS_ROOT"; printf '\\x1f%s' "$@"; printf '\\n'; } >> "$FAKE_JARVIS_LOG"
case "$1 ${2:-}" in
"init "*) mkdir -p "$2" "$3" "$4" ;;
"repo add") [ -d "$3/$(basename "$3")" ] || { echo "no package named for $3" >&2; exit 1; } ;;
"ppl run") echo ran > "$JARVIS_ROOT/shared/ran" ;;
esac
echo "fake jarvis $1 ${2:-}"
"""


@pytest.fixture
def jarvis_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put a recording ``jarvis`` first on PATH, and give the job a home of its own."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "jarvis"
    script.write_text(FAKE_JARVIS, encoding="utf-8")
    script.chmod(0o755)
    log = tmp_path / "jarvis.log"
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_JARVIS_LOG", str(log))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    return log


def demo_checkout(root: Path) -> Path:
    """A checkout holding ``w10demo``, a jarvis repo with the package ``w10demo.hello``."""
    checkout = root / "src"
    package = checkout / "w10demo" / "w10demo" / "hello"
    package.mkdir(parents=True)
    (package.parent / "__init__.py").write_text("", encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "pkg.py").write_text("# the fake jarvis never imports this\n", encoding="utf-8")
    return checkout


async def test_a_package_runs_through_a_jarvis_of_its_own(
    stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, jarvis_log: Path
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = guarded_adapter(stub, tmp_path)
    digest = adapter.context.content.put(package_archive(demo_checkout(tmp_path), ["w10demo"]))
    spec = JobSpec(
        package=PackageRef(
            name="w10demo.hello",
            version="0.1.0",
            sha256=digest,
            parameters={"greeting": "hi there", "count": 3},
        )
    )
    handle, status, job_dir = await run_package(stub, adapter, spec, "op-package")
    calls = [line.split("\x1f") for line in jarvis_log.read_text().splitlines()]
    print(f"package calls {calls}")
    assert status.state == JobState.COMPLETED, (status, (job_dir / "stderr").read_text())
    assert status.exit_code == 0
    root = str(job_dir / ".relay-jarvis")
    assert {call[0] for call in calls} == {root}
    pipeline = "relay_" + str(handle.native_id).replace("-", "_")
    assert [call[1:] for call in calls if call[1:3] != ["repo", "add"]] == [
        ["init", f"{root}/config", f"{root}/private", f"{root}/shared"],
        ["ppl", "create", pipeline],
        ["ppl", "append", "w10demo.hello", "pkg", "greeting=hi there", "count=3"],
        ["ppl", "run"],
    ]
    (added,) = [call[3] for call in calls if call[1:3] == ["repo", "add"]]
    assert added.endswith(f"/packages/{digest}/w10demo")
    assert (job_dir / ".relay-jarvis" / "shared" / "ran").is_file()
    assert not (tmp_path / "home" / ".ppi-jarvis").exists()


@pytest.mark.parametrize(
    ("name", "flat", "code", "said"),
    [
        ("hello", False, 64, "package name hello is not <repo>.<package>"),
        ("w10demo.hello", True, 66, "package archive has no w10demo/w10demo directory"),
    ],
)
async def test_a_package_the_launcher_cannot_place_fails_before_jarvis(
    stub: TapisStub,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    jarvis_log: Path,
    name: str,
    flat: bool,
    code: int,
    said: str,
) -> None:
    use_memory_keyring(monkeypatch)
    await sign_in(stub)
    adapter = guarded_adapter(stub, tmp_path, site=f"pkg-refused-{code}")
    checkout = demo_checkout(tmp_path)
    root = checkout / "w10demo" if flat else checkout
    digest = adapter.context.content.put(package_archive(root, ["w10demo"]))
    spec = JobSpec(package=PackageRef(name=name, version="0.1.0", sha256=digest))
    _, status, job_dir = await run_package(stub, adapter, spec, "op-bad-package")
    print(f"refused package {name} flat={flat}: {status.exit_code} {status.message}")
    assert status.state == JobState.FAILED
    assert status.exit_code == code
    assert said in (job_dir / "relay.launcher.log").read_text()
    assert not jarvis_log.exists()
