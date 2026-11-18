"""LQ35: the conformance kit on ``tapis`` against the Tapis stub.

The stub runs each job on this machine the way Tapis's ``ZIP`` runtime does:
it unpacks relay's launcher archive and runs ``tapisjob_app.sh`` with the
app arguments relay wrote. ``exec`` and ``serve`` are not Tapis verbs, so the
kit's typed-refusal check covers them, and ``test_exec_bounded`` is replaced
by the refusal it gets instead.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget
from tests.fixtures.tapis_stub import TapisStub
from tests.fixtures.tapis_stub.support import seed_host, sign_in, tapis_config, use_memory_keyring

pytestmark = pytest.mark.release_platform("posix")


class TestTapisAdapter(AdapterConformance):
    @pytest.fixture
    def target(
        self, stub: TapisStub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> ConformanceTarget:
        use_memory_keyring(monkeypatch)
        asyncio.run(sign_in(stub))
        seed_host(stub, "conformance", tmp_path / "site")
        return ConformanceTarget(
            adapter="tapis",
            config=tapis_config(stub),
            state_dir=tmp_path / "relay",
            site_id="conformance",
            job_timeout_s=60,
            poll_s=0.1,
        )

    @pytest.mark.asyncio
    async def test_exec_bounded(self, target: ConformanceTarget) -> None:
        adapter = target.build()
        with pytest.raises(Refusal) as refused:
            await adapter.exec(target.echo, timeout_s=10, max_output_bytes=1024)
        assert refused.value.reason == RefusalReason.VERB_NOT_IMPLEMENTED
