"""Operations that find a site without JARVIS at the same time share one ask.

Two ``install`` operations started together on a clean site both get
``missing_jarvis``. Relay asks once for the site: the second holds the first's
ask, one answer moves both on, and cancelling one leaves the ask to the other.
The setup script on the site takes turns (``.relay.lock``) and the later one
reuses the first setup; the POSIX test runs the real script twice at once.
"""

from __future__ import annotations

import asyncio
import os
import stat
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from clio_relay import jarvis_submit
from clio_relay.adapters.hooks import Ask
from clio_relay.asks.core import stale_approval
from clio_relay.jarvis_site import CLIO_JARVIS_REF, CLIO_JARVIS_URL, site_pin
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.surface import install_op, installing
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

XCOMPACT = JobSpec(package=PackageRef(name="demo.xcompact3d", version="1", sha256="a" * 64))
PVSERVER = JobSpec(package=PackageRef(name="demo.pvserver", version="1", sha256="b" * 64))


def _report() -> dict[str, Any]:
    return {
        "schema_version": jarvis_submit.INSTALL_REPORT_SCHEMA,
        "satisfied": True,
        "installers": {"spack": {"packages": {"pkg": {"status": "satisfied"}}}},
        "issues": [],
    }


@dataclass
class _Site:
    """A stand-in site whose jarvis-cd install takes turns, like the real script."""

    jarvis: bool = False
    builds: int = 0
    reuses: int = 0
    calls: list[str] = field(default_factory=list[str])
    lock: asyncio.Lock | None = None


@pytest.fixture
def site(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Site]:
    state = _Site()

    async def install_check(self: FixtureAdapter, spec: JobSpec, *, operation_id: str) -> Any:
        state.calls.append("check")
        await asyncio.sleep(0)
        if not state.jarvis:
            raise jarvis_submit.missing_jarvis('{"missing":"jarvis","tried":"/h/j"}', site="fx")
        return _report()

    async def install_jarvis(self: FixtureAdapter) -> str:
        state.calls.append("install_jarvis")
        if state.lock is None:
            state.lock = asyncio.Lock()
        async with state.lock:
            if state.jarvis:
                state.reuses += 1
            else:
                await asyncio.sleep(0.01)
                state.builds += 1
                state.jarvis = True
        return "/h/.clio-relay/bin/jarvis"

    async def install(self: FixtureAdapter, *args: Any, **kwargs: Any) -> JobHandle:
        raise AssertionError("the software is all there")

    async def status(self: FixtureAdapter, handle: JobHandle) -> JobStatus:
        return JobStatus(state=JobState.COMPLETED, exit_code=0)

    async def site_packages(self: FixtureAdapter, add: str = "", *, force: bool = False) -> str:
        return '@@repos {"schema_version": "jarvis.repos.v1", "repos": []}'

    for name, value in {
        "install_check": install_check,
        "install_jarvis": install_jarvis,
        "install": install,
        "site_packages": site_packages,
        "status": status,
    }.items():
        monkeypatch.setattr(FixtureAdapter, name, value)
    yield state


async def _both(service: RelayService) -> tuple[str, str]:
    first, second = await asyncio.gather(
        install_op.start(service, "fx", XCOMPACT), install_op.start(service, "fx", PVSERVER)
    )
    return first.operation_id, second.operation_id


def _jarvis_asks(service: RelayService) -> list[Ask]:
    return [ask for ask in service.asks.all() if ask.binding.get("kind") == installing.JARVIS_KIND]


def test_two_installs_on_a_clean_site_ask_once_and_install_jarvis_once(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first, second = await _both(service)
        (ask,) = _jarvis_asks(service)
        assert ask.binding["scope"] == "site"
        assert "spec_digest" not in ask.binding
        for operation_id in (first, second):
            record = service.operation(operation_id)
            assert record.state == OperationState.AWAITING_INPUT
            assert [held.ask_id for held in service.outstanding(record)] == [ask.ask_id]
        assert sorted(installing.holders(service, ask)) == sorted([first, second])
        assert stale_approval(ask, service.operations) is None
        service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
        done = await asyncio.gather(service.advance(first), service.advance(second))
        assert [record.state for record in done] == [OperationState.COMPLETED] * 2, done
        assert site.builds == 1
        assert site.reuses == 1
        assert len(_jarvis_asks(service)) == 1
        await service.close()

    asyncio.run(scenario())


def test_an_approved_ask_covers_an_operation_that_comes_while_jarvis_installs(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        (ask,) = _jarvis_asks(service)
        service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
        installing._update(service, first, stage="starting")  # pyright: ignore[reportPrivateUsage]
        second = (await install_op.start(service, "fx", PVSERVER)).operation_id
        assert len(_jarvis_asks(service)) == 1
        assert service.outstanding(service.operation(second)) == []
        record = await service.advance(second)
        assert record.state == OperationState.COMPLETED, record
        assert site.builds == 1
        await service.close()

    asyncio.run(scenario())


def test_cancelling_one_operation_leaves_the_shared_ask_to_the_other(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first, second = await _both(service)
        (ask,) = _jarvis_asks(service)
        cancelled = await service.cancel(first)
        assert cancelled.state == OperationState.CANCELLED
        assert service.asks.get(ask.ask_id).state == "pending"  # pyright: ignore[reportOptionalMemberAccess]
        assert installing.holders(service, ask) == [second]
        assert stale_approval(ask, service.operations) is None
        service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
        record = await service.advance(second)
        assert record.state == OperationState.COMPLETED, record
        await service.close()

    asyncio.run(scenario())


def test_a_later_operation_asks_again_once_the_install_is_done(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first = (await install_op.start(service, "fx", XCOMPACT)).operation_id
        (ask,) = _jarvis_asks(service)
        service.answer(ask.ask_id, "accept", {"approve": True}, by="test")
        assert (await service.advance(first)).state == OperationState.COMPLETED
        site.jarvis = False
        later = (await install_op.start(service, "fx", PVSERVER)).operation_id
        asks = _jarvis_asks(service)
        assert len(asks) == 2
        (fresh,) = service.outstanding(service.operation(later))
        assert fresh.ask_id != ask.ask_id
        await service.close()

    asyncio.run(scenario())


def test_a_declined_shared_ask_cancels_every_operation_holding_it(
    relay_paths: RelayPaths, site: _Site
) -> None:
    async def scenario() -> None:
        add_site(relay_paths)
        service = RelayService(relay_paths)
        first, second = await _both(service)
        (ask,) = _jarvis_asks(service)
        service.answer(ask.ask_id, "decline", by="test")
        done = await asyncio.gather(service.advance(first), service.advance(second))
        assert [record.state for record in done] == [OperationState.CANCELLED] * 2
        assert site.calls.count("install_jarvis") == 0
        await service.close()

    asyncio.run(scenario())


FAKE_UV = r"""#!/bin/bash
# Stands in for uv: a slow jarvis-cd tool install that counts builds and leaves a jarvis.
[ "$1 $2" = "tool install" ] || exit 1
echo build >> "$FAKE_BUILDS"
sleep 1
cat > "$UV_TOOL_BIN_DIR/jarvis" <<'JARVIS'
#!/bin/bash
echo "$*" >> "$FAKE_BUILDS.jarvis"
case "$1 $2" in
  "init "*)
    mkdir -p "$JARVIS_ROOT"
    printf 'no_builtin: true\nshared_dir: %s\n' "$4" > "$JARVIS_ROOT/jarvis_config.yaml" ;;
  "repo add")
    mkdir -p "$JARVIS_ROOT/repos/clio_jarvis/.git" "$JARVIS_ROOT/repos/clio_jarvis/clio_jarvis"
    printf 'repos:\n- %s\n' "$JARVIS_ROOT/repos/clio_jarvis" > "$JARVIS_ROOT/repos.yaml" ;;
esac
JARVIS
chmod +x "$UV_TOOL_BIN_DIR/jarvis"
"""


@pytest.mark.release_platform("posix")
def test_two_setups_at_once_set_the_site_up_once(tmp_path: Path) -> None:
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    fake = bin_dir / "uv"
    fake.write_text(FAKE_UV, encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    # The fake clone has no commits, so git answers the ref setup checks it against.
    git = bin_dir / "git"
    git.write_text(f"#!/bin/bash\necho {CLIO_JARVIS_REF}\n", encoding="utf-8")
    git.chmod(git.stat().st_mode | stat.S_IXUSR)
    builds = tmp_path / "builds"
    env = {
        **os.environ,
        "HOME": str(tmp_path),
        "FAKE_BUILDS": str(builds),
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '/usr/bin:/bin')}",
    }
    home = str(tmp_path / ".clio-relay")
    argv = jarvis_submit.setup_command(home)
    runs = [
        subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for _ in range(2)
    ]
    outputs = [run.communicate(timeout=60) for run in runs]
    assert [run.returncode for run in runs] == [0, 0], outputs
    assert builds.read_text(encoding="utf-8").splitlines() == ["build"]
    lines = [out.strip().splitlines()[-1] for out, _ in outputs]
    assert sum('"from":"reused"' in line for line in lines) == 1, lines
    root = Path(home) / "jarvis"
    assert jarvis_submit.setup_result(0, outputs[0][0], site="lab") == f"{home}/bin/jarvis"
    assert (root / "relay.pin").read_text().strip() == site_pin()
    calls = Path(f"{builds}.jarvis").read_text(encoding="utf-8").splitlines()
    assert calls == [
        f"init {root}/config {root}/private {(root / 'shared').resolve()}",
        "repo rm builtin",
        f"repo add {CLIO_JARVIS_URL} --ref {CLIO_JARVIS_REF}",
    ]

    # A root at another setup moves clio-jarvis's clone to the ref, without a new build.
    (root / "relay.pin").write_text("older\n")
    again = subprocess.run(argv, env=env, capture_output=True, text=True, check=False)
    assert again.returncode == 0, again.stderr
    assert builds.read_text(encoding="utf-8").splitlines() == ["build"] * 2
    assert Path(f"{builds}.jarvis").read_text(encoding="utf-8").splitlines()[3:] == [
        "repo rm builtin",
        f"repo update clio_jarvis --ref {CLIO_JARVIS_REF}",
    ]
