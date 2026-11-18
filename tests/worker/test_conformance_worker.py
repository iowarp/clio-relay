"""The conformance kit against a placed worker (RL7, RL11, LQ4).

RELAY_TEST_WORKER_CONFIG names a JSON file holding the worker config (an
ssh_profile that signs in without a prompt, a rendezvous, and usually a
relay_wheel). RELAY_TEST_WORKER_DIR is where relay's side keeps its state.
The worker is placed once, over one ssh pass; every test then reaches it over
the rendezvous only, from its own state directory.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.worker.adapter import WORKER_LINK_FILE, WorkerAdapter, WorkerConfig
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.records import new_id
from clio_relay.testing.conformance import AdapterConformance, ConformanceTarget

pytestmark = pytest.mark.release_platform("posix")


def _config() -> dict[str, Any]:
    raw = json.loads(Path(os.environ["RELAY_TEST_WORKER_CONFIG"]).read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    return raw  # pyright: ignore[reportUnknownVariableType]


@pytest.fixture(scope="module")
def placed_link() -> Path:
    root = Path(os.environ["RELAY_TEST_WORKER_DIR"]) / new_id("placed")
    context = AdapterContext(
        site_id="conformance",
        state_dir=root / "adapter",
        content=ContentStore(root / "content"),
    )
    adapter = WorkerAdapter(WorkerConfig.model_validate(_config()), context)
    asyncio.run(adapter.connect())
    return adapter.link_path


class TestWorker(AdapterConformance):
    @pytest.fixture
    def target(self, placed_link: Path) -> ConformanceTarget:
        state = Path(os.environ["RELAY_TEST_WORKER_DIR"]) / new_id("conformance")
        (state / "adapter").mkdir(parents=True)
        shutil.copy2(placed_link, state / "adapter" / WORKER_LINK_FILE)
        return ConformanceTarget(
            adapter="worker",
            config=_config(),
            state_dir=state,
            job_timeout_s=float(os.environ.get("RELAY_TEST_WORKER_TIMEOUT_S", "300")),
            poll_s=1.0,
        )
