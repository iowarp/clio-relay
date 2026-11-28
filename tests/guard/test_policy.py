from __future__ import annotations

import copy
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext
from clio_relay.adapters.hooks import Charge, GuardHook
from clio_relay.guard import WARM_EXECUTABLE, Guard
from clio_relay.jobs.records import OperationState
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.packages import ADD_COMMAND
from clio_relay.surface.stubs import DefaultGuard
from clio_relay.testing.fixture_adapter import FixtureAdapter, FixtureConfig

from .conftest import answer, context_for, make_repo, site, stage


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
@pytest.mark.parametrize("policy", ["open", "guarded"])
async def test_a_read_only_exec_runs_only_reads_on_every_site(guard: Guard, policy: str) -> None:
    where = site(policy)
    for command in (["ls", "-l"], ["jarvis", "rg", "facts", "show", "+json"], ["squeue"]):
        assert (await guard.check_exec(where, command, read_only=True)).allowed, command
    refused = await guard.check_exec(where, ["rm", "-rf", "/tmp/x"], read_only=True)
    assert not refused.allowed
    assert refused.reason == RefusalReason.POLICY_REFUSED
    assert refused.data["next_step"] == "exec"


@pytest.mark.asyncio
async def test_without_a_guard_a_read_only_exec_runs_nothing(tmp_path: Path) -> None:
    stub = DefaultGuard(context_for(tmp_path))
    assert (await stub.check_exec(site("open"), ["ls"])).allowed
    decision = await stub.check_exec(site("open"), ["ls"], read_only=True)
    assert not decision.allowed
    assert decision.reason == RefusalReason.GUARD_UNAVAILABLE


@pytest.mark.asyncio
async def test_a_free_form_run_on_a_guarded_site_is_refused_with_the_way_on(guard: Guard) -> None:
    decision = await guard.check_run(site(), JobSpec(executable="bash", arguments=["job.sh"]))
    assert not decision.allowed
    assert decision.reason == RefusalReason.POLICY_REFUSED
    assert decision.data["next_step"] == "package_review"
    assert "reviewed JARVIS packages" in (decision.message or "")
    assert "describe(site=...)" in decision.data["how"]
    assert ADD_COMMAND in decision.data["how"]
    with pytest.raises(Refusal) as raised:
        decision.raise_if_refused()
    assert raised.value.reason == RefusalReason.POLICY_REFUSED


@pytest.mark.asyncio
async def test_a_package_relay_has_not_got_cannot_be_reviewed(guard: Guard) -> None:
    with pytest.raises(Refusal) as raised:
        await guard.check_run(site(), package_spec("a" * 64))
    assert raised.value.reason == RefusalReason.INPUT_MISSING
    assert ADD_COMMAND in raised.value.message


@pytest.mark.asyncio
async def test_a_new_package_comes_back_with_a_review_question(
    guard: Guard, tmp_path: Path
) -> None:
    repo = make_repo(tmp_path)
    git = {"url": "https://example.org/demo.git", "commit": "c" * 40}
    digest = stage(guard, repo, "demo.hello", git)
    decision = await guard.check_run(site(), package_spec(digest))
    assert decision.allowed
    [review] = decision.data["asks"]
    binding = review["binding"]
    assert binding["kind"] == "package_review"
    assert binding["package"] == "demo.hello"
    assert binding["version"] == "0.1.0"
    assert binding["package_hash"] == digest
    assert binding["source"] == {"repository": git["url"], "commit": git["commit"]}
    message = review["message"]
    assert digest in message
    assert "https://example.org/demo.git, commit " + "c" * 40 in message
    assert "hello/pkg.py" in message and "_contract/base.py" in message
    assert "other/pkg.py" not in message and "__pycache__" not in message
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

    stage(guard, repo, "demo.hello", site_id="other")
    other_site = await guard.check_run(site(site_id="other"), package_spec(digest))
    assert "asks" in other_site.data, "a review is per site"
    other_version = package_spec(digest).model_copy(deep=True)
    assert other_version.package is not None
    other_version.package.version = "0.2.0"
    assert "asks" in (await guard.check_run(hpc, other_version)).data

    (repo / "demo" / "_contract" / "base.py").write_text("SHARED = 2\n")
    shared_edit = stage(guard, repo, "demo.hello")
    (repo / "demo" / "hello" / "pkg.py").write_text("# edited\n")
    edited = stage(guard, repo, "demo.hello")
    assert len({digest, shared_edit, edited}) == 3
    after_edit = await guard.check_run(hpc, package_spec(edited))
    assert after_edit.data["asks"][0]["binding"]["package_hash"] == edited


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
async def test_a_deleted_review_stays_withdrawn_once_its_operation_ended(
    guard: Guard, tmp_path: Path
) -> None:
    digest = stage(guard, make_repo(tmp_path), "demo.hello")
    hpc = site()
    review = (await guard.check_run(hpc, package_spec(digest))).data["asks"][0]
    answer(guard, "ask-1", hpc.id, review["message"], {**review["binding"], "site": hpc.id})
    assert "asks" not in (await guard.check_run(hpc, package_spec(digest))).data

    record = guard.operations.get("op-1")
    assert record is not None
    record.move(OperationState.COMPLETED)
    guard.operations.put("op-1", record)
    for path in (guard.reviews.root / hpc.id).glob("*.json"):
        path.unlink()
    again = await guard.check_run(hpc, package_spec(digest))
    assert again.data["review"] is None
    assert again.data["asks"][0]["binding"]["package_hash"] == digest
    assert guard.reviews.all() == []

    rebound = {**review["binding"], "site": hpc.id}
    answer(guard, "ask-2", hpc.id, review["message"], rebound, operation_id="op-2")
    assert (await guard.check_run(hpc, package_spec(digest))).data["review"]["ask_id"] == "ask-2"


@pytest.mark.asyncio
async def test_a_pipelines_unreviewed_packages_are_one_review_question(
    guard: Guard, tmp_path: Path
) -> None:
    repo = make_repo(tmp_path)
    hello = stage(guard, repo, "demo.hello")
    other = stage(guard, repo, "demo.other")
    hpc = site()
    pipeline = JobSpec.model_validate(
        {
            "packages": [
                {"name": "demo.hello", "version": "0.1.0", "sha256": hello},
                {"name": "demo.other", "version": "0.1.0", "sha256": other},
            ]
        }
    )
    decision = await guard.check_run(hpc, pipeline)
    [review] = decision.data["asks"]
    binding = review["binding"]
    assert binding["kind"] == "package_review"
    assert [item["package_hash"] for item in binding["packages"]] == [hello, other]
    assert "Approve 2 JARVIS packages" in review["message"]
    assert hello in review["message"] and other in review["message"]
    answer(guard, "ask-1", hpc.id, review["message"], {**binding, "site": hpc.id})

    again = await guard.check_run(hpc, pipeline)
    assert "asks" not in again.data
    assert [item["ask_id"] for item in again.data["reviews"]] == ["ask-1", "ask-1"]
    assert sorted(item.sha256 for item in guard.reviews.all(hpc.id)) == sorted([hello, other])

    stage(guard, repo, "demo.hello", site_id="other")
    stage(guard, repo, "demo.other", site_id="other")
    tampered = answer(
        guard, "ask-t", "other", review["message"], copy.deepcopy({**binding, "site": "other"})
    )
    tampered.binding["packages"][1]["package_hash"] = "f" * 64
    guard.context.asks.put("ask-t", tampered)
    assert "asks" in (await guard.check_run(site(site_id="other"), pipeline)).data
    assert guard.reviews.all("other") == []


@pytest.mark.asyncio
async def test_package_jobs_take_settings_only_as_parameters(guard: Guard, tmp_path: Path) -> None:
    digest = stage(guard, make_repo(tmp_path), "demo.hello")
    decision = await guard.check_run(site(), package_spec(digest, environment={"LD_PRELOAD": "x"}))
    assert not decision.allowed
    assert decision.reason == RefusalReason.POLICY_REFUSED


def test_an_environment_name_is_never_an_env_option() -> None:
    with pytest.raises(ValidationError, match="shell variable names"):
        JobSpec(executable="x", environment={"-S": "rm -rf ~"})
    assert JobSpec(executable="x", environment={"_A1": "a b"}).environment == {"_A1": "a b"}


@pytest.mark.asyncio
async def test_prepare_hands_only_a_reviewed_package_to_an_adapter_that_runs_it(
    guard: Guard, tmp_path: Path
) -> None:
    digest = stage(guard, make_repo(tmp_path), "demo.hello")
    hpc = site()
    adapter = fixture(guard)
    with pytest.raises(Refusal) as raised:
        await guard.prepare(hpc, package_spec(digest), adapter=adapter)
    assert raised.value.reason == RefusalReason.POLICY_REFUSED
    review = (await guard.check_run(hpc, package_spec(digest))).data["asks"][0]
    answer(guard, "ask-1", hpc.id, review["message"], {**review["binding"], "site": hpc.id})

    class CommandsOnly(FixtureAdapter):
        runs_packages: ClassVar[bool] = False

    commands_only = CommandsOnly(FixtureConfig(), _context(guard))
    for service in (False, True):
        with pytest.raises(Refusal) as raised:
            await guard.prepare(hpc, package_spec(digest), adapter=commands_only, service=service)
        assert raised.value.reason == RefusalReason.ADAPTER_UNAVAILABLE
        assert raised.value.data["needs"] == "jarvis ppl submit through the fixture adapter"
        assert "inside an allocation" in raised.value.message

    kept = await guard.prepare(hpc, package_spec(digest), adapter=adapter)
    assert kept.package is not None
    assert kept.package.service is False
    served_copy = await guard.prepare(hpc, package_spec(digest), adapter=adapter, service=True)
    assert served_copy.package is not None
    assert served_copy.package.service is True
    assert served_copy.package.sha256 == digest


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
