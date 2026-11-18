from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

import pytest

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.hooks import Charge, GuardHook
from clio_relay.guard import WARM_EXECUTABLE, Guard
from clio_relay.jarvis_site import ARCHIVE_DIR
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.testing.fixture_adapter import FixtureAdapter, FixtureConfig

from .conftest import answer, make_repo, site, stage


def package_spec(digest: str, name: str = "demo.hello", **extra: Any) -> JobSpec:
    return JobSpec.model_validate(
        {
            "package": {
                "name": name,
                "version": "0.1.0",
                "sha256": digest,
                "parameters": {"out": "result.txt", "steps": 3},
            },
            **extra,
        }
    )


def test_the_guard_implements_the_hook(guard: Guard) -> None:
    assert isinstance(guard, GuardHook)


@pytest.mark.asyncio
async def test_open_sites_run_anything(guard: Guard) -> None:
    decision = await guard.check_run(site("open"), JobSpec(executable="/bin/rm", arguments=["-rf"]))
    assert decision.allowed
    assert (await guard.check_exec(site("open"), ["rm", "-rf", "/tmp/x"])).allowed


@pytest.mark.asyncio
async def test_a_free_form_run_on_a_guarded_site_is_refused_with_the_way_on(guard: Guard) -> None:
    decision = await guard.check_run(site(), JobSpec(executable="bash", arguments=["job.sh"]))
    assert not decision.allowed
    assert decision.reason == RefusalReason.POLICY_REFUSED
    assert decision.data["next_step"] == "package_review"
    assert "reviewed JARVIS packages" in (decision.message or "")
    assert "jarvis_site archive" in decision.data["how"]
    with pytest.raises(Refusal) as raised:
        decision.raise_if_refused()
    assert raised.value.reason == RefusalReason.POLICY_REFUSED


@pytest.mark.asyncio
async def test_a_package_relay_has_not_got_cannot_be_reviewed(guard: Guard) -> None:
    with pytest.raises(Refusal) as raised:
        await guard.check_run(site(), package_spec("a" * 64))
    assert raised.value.reason == RefusalReason.INPUT_MISSING
    assert "--stage" in raised.value.message


@pytest.mark.asyncio
async def test_a_new_package_comes_back_with_a_review_question(
    guard: Guard, tmp_path: Path
) -> None:
    repo = make_repo(tmp_path)
    source = {"repository": "https://example.org/demo.git", "tag": "v0.1.0", "commit": "c" * 40}
    digest = stage(guard, repo, "demo.hello", source)
    decision = await guard.check_run(site(), package_spec(digest))
    assert decision.allowed
    [review] = decision.data["asks"]
    binding = review["binding"]
    assert binding["kind"] == "package_review"
    assert binding["package"] == "demo.hello"
    assert binding["version"] == "0.1.0"
    assert binding["package_hash"] == digest
    assert binding["source"] == source
    message = review["message"]
    assert digest in message
    assert "https://example.org/demo.git, tag v0.1.0, commit " + "c" * 40 in message
    assert "demo/demo/hello/pkg.py" in message
    assert "out=result.txt" in message


@pytest.mark.asyncio
async def test_a_yes_reviews_that_hash_and_an_edit_needs_a_new_review(
    guard: Guard, tmp_path: Path
) -> None:
    repo = make_repo(tmp_path)
    digest = stage(guard, repo, "demo.hello")
    hpc = site()
    first = await guard.check_run(hpc, package_spec(digest))
    review = first.data["asks"][0]
    answer(guard, "ask-1", hpc.id, review["message"], {**review["binding"], "site": hpc.id})

    again = await guard.check_run(hpc, package_spec(digest))
    assert again.allowed
    assert "asks" not in again.data
    assert again.data["review"]["sha256"] == digest
    assert again.data["review"]["ask_id"] == "ask-1"
    assert [item.sha256 for item in guard.reviews.all(hpc.id)] == [digest]

    (repo / "demo" / "hello" / "pkg.py").write_text("# edited\n")
    edited = stage(guard, repo, "demo.hello")
    assert edited != digest
    after_edit = await guard.check_run(hpc, package_spec(edited))
    assert after_edit.data["asks"][0]["binding"]["package_hash"] == edited

    other_site = await guard.check_run(site(site_id="other"), package_spec(digest))
    assert "asks" in other_site.data, "a review is per site"
    other_version = package_spec(digest).model_copy(deep=True)
    assert other_version.package is not None
    other_version.package.version = "0.2.0"
    assert "asks" in (await guard.check_run(hpc, other_version)).data


@pytest.mark.asyncio
async def test_declined_or_tampered_review_asks_review_nothing(
    guard: Guard, tmp_path: Path
) -> None:
    digest = stage(guard, make_repo(tmp_path), "demo.hello")
    hpc = site()
    review = (await guard.check_run(hpc, package_spec(digest))).data["asks"][0]
    binding = {**review["binding"], "site": hpc.id}
    answer(guard, "ask-no", hpc.id, review["message"], binding, state="declined")
    tampered = answer(guard, "ask-t", hpc.id, review["message"], binding)
    tampered.binding["package_hash"] = "f" * 64
    guard.context.asks.put("ask-t", tampered)
    wrong_site = answer(guard, "ask-w", "other", review["message"], binding)
    assert wrong_site.site == "other"
    assert "asks" in (await guard.check_run(hpc, package_spec(digest))).data
    assert guard.reviews.all() == []


@pytest.mark.asyncio
async def test_package_jobs_take_settings_only_as_parameters(guard: Guard, tmp_path: Path) -> None:
    digest = stage(guard, make_repo(tmp_path), "demo.hello")
    decision = await guard.check_run(site(), package_spec(digest, environment={"LD_PRELOAD": "x"}))
    assert not decision.allowed
    assert decision.reason == RefusalReason.POLICY_REFUSED


@pytest.mark.asyncio
async def test_prepare_lowers_only_a_reviewed_package(guard: Guard, tmp_path: Path) -> None:
    digest = stage(guard, make_repo(tmp_path), "demo.hello")
    hpc = site()
    adapter = fixture(guard)
    with pytest.raises(Refusal) as raised:
        await guard.prepare(hpc, package_spec(digest), adapter=adapter)
    assert raised.value.reason == RefusalReason.POLICY_REFUSED
    review = (await guard.check_run(hpc, package_spec(digest))).data["asks"][0]
    answer(guard, "ask-1", hpc.id, review["message"], {**review["binding"], "site": hpc.id})

    lowered = await guard.prepare(hpc, package_spec(digest), adapter=adapter)
    assert lowered.package is None
    assert lowered.executable == "/bin/bash"
    assert lowered.arguments[2:8] == [
        "clio-relay-package",
        digest,
        "demo.hello",
        "jarvis",
        "",
        "run",
    ]
    assert lowered.arguments[8:] == ["out=result.txt", "steps=3"]
    assert [(item.name, item.sha256) for item in lowered.inputs] == [
        (f"{ARCHIVE_DIR}/{digest}.tar.gz", digest)
    ]
    served = await guard.prepare(hpc, package_spec(digest), adapter=adapter, service=True)
    assert served.arguments[7] == "service"

    class Runs(FixtureAdapter):
        runs_packages: ClassVar[bool] = True

    kept = await guard.prepare(
        hpc,
        package_spec(digest),
        adapter=Runs(FixtureConfig(), _context(guard)),
    )
    assert kept.package is not None


@pytest.mark.asyncio
async def test_prepare_refuses_a_free_form_job_on_a_guarded_site(guard: Guard) -> None:
    adapter = fixture(guard)
    with pytest.raises(Refusal):
        await guard.prepare(site(), JobSpec(executable="bash"), adapter=adapter)
    open_job = JobSpec(executable="bash")
    assert await guard.prepare(site("open"), open_job, adapter=adapter) == open_job


@pytest.mark.asyncio
async def test_a_warm_allocation_is_allowed_everywhere_and_becomes_its_holder(
    guard: Guard,
) -> None:
    warm = JobSpec.model_validate({"executable": WARM_EXECUTABLE, "resources": {"node_count": 2}})
    assert (await guard.check_run(site(), warm)).allowed
    with_args = warm.model_copy(update={"arguments": ["sleep", "1"]})
    assert not (await guard.check_run(site(), with_args)).allowed
    adapter = fixture(guard)
    held = await guard.prepare(site(), warm, adapter=adapter)
    assert held.executable == "/bin/sh"
    assert held.arguments[2:] == [WARM_EXECUTABLE, "600", "30"]
    assert held.resources.node_count == 2
    assert held.name == "relay-warm"
    (Path(guard.config_dir or ".") / "guard.yaml").write_text(
        "sites:\n  hpc:\n    warm_idle_s: 120\n"
    )
    assert (await guard.prepare(site(), warm, adapter=adapter)).arguments[3] == "120"


@pytest.mark.asyncio
async def test_price_is_unknown_without_a_walltime(guard: Guard) -> None:
    spec = JobSpec.model_validate(
        {
            "executable": "x",
            "resources": {"node_count": 4},
            "attributes": {"queue_name": "debug", "account": "proj"},
        }
    )
    charge = await guard.price(site("open"), spec)
    assert charge.node_hours is None
    assert charge.walltime_s is None
    assert (charge.nodes, charge.queue, charge.account) == (4, "debug", "proj")
    timed = spec.model_copy(
        update={"attributes": spec.attributes.model_copy(update={"duration": timedelta(hours=2)})}
    )
    charge = await guard.price(site("open"), timed)
    assert charge.node_hours == 8.0
    assert charge.walltime_s == 7200.0
    assert charge.unit == "node-hours"


class _Priced(FixtureAdapter):
    name: ClassVar[str] = "priced-fixture"

    async def price(self, spec: JobSpec) -> Charge:
        return Charge(site=self.context.site_id, nodes=1, node_hours=1.0, amount=2.5, unit="USD")


@pytest.mark.asyncio
async def test_price_comes_from_an_adapter_that_prices(guard: Guard) -> None:
    registry.register(_Priced)
    try:
        spec = JobSpec.model_validate(
            {"executable": "x", "attributes": {"queue_name": "q", "account": "a"}}
        )
        charge = await guard.price(site("open", adapter="priced-fixture"), spec)
    finally:
        registry.unregister("priced-fixture")
    assert (charge.amount, charge.unit, charge.node_hours) == (2.5, "USD", 1.0)
    assert (charge.queue, charge.account) == ("q", "a")


@pytest.mark.asyncio
async def test_a_broken_guard_file_is_a_typed_refusal(guard: Guard) -> None:
    (Path(guard.config_dir or ".") / "guard.yaml").write_text("sites: [nope\n")
    with pytest.raises(Refusal) as raised:
        await guard.check_exec(site(), ["ls"])
    assert raised.value.reason == RefusalReason.INVALID_CONFIG


def _context(guard: Guard) -> AdapterContext:
    return AdapterContext(
        site_id="hpc", state_dir=guard.context.state_dir / "sites" / "hpc", content=guard.content
    )


def fixture(guard: Guard) -> FixtureAdapter:
    return FixtureAdapter(FixtureConfig(), _context(guard))
