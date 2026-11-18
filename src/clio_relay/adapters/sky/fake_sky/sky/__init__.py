"""A stand-in for SkyPilot 0.14.3, for tests and offline demos. It is not SkyPilot.

Put the directory that holds this package on ``PYTHONPATH`` of the bridge
process. It keeps the calls the bridge makes with the same shapes, runs each
managed job as a local shell process under ``$FAKE_SKY_ROOT``, and prices from
a small fixed table (AWS on-demand prices that SkyPilot's catalog gave for
these instance types).
"""

from __future__ import annotations

from typing import Any

from . import jobs
from .jobs import Request

__version__ = "0.14.3-fake"

PRICES = {"m6i.large": 0.096, "m6i.2xlarge": 0.384, "p4d.24xlarge": 21.95764}
SPOT_FACTOR = 0.125


def get(request_id: Request) -> Any:
    return request_id.value


def stream_and_get(request_id: Request | None = None, **_: Any) -> Any:
    return None if request_id is None else request_id.value


class _Feasible:
    def __init__(self, resources_list: list[Resources], fuzzy: list[str]) -> None:
        self.resources_list = resources_list
        self.fuzzy_candidate_list = fuzzy


class _Cloud:
    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return self.name

    def get_default_instance_type(self, cpus: str | None = None, **_: Any) -> str | None:
        wanted = float(str(cpus or "2").rstrip("+"))
        return "m6i.large" if wanted <= 2 else "m6i.2xlarge" if wanted <= 8 else None

    def _get_feasible_launchable_resources(self, resources: Resources) -> _Feasible:
        if str(resources.accelerators) == "A100:8":
            return _Feasible([Resources(infra=self.name, instance_type="p4d.24xlarge")], [])
        return _Feasible([], ["A100:8"])


class Resources:
    def __init__(self, infra: str | None = None, **fields: Any) -> None:
        self.infra = infra
        self.instance_type: str | None = fields.get("instance_type")
        self.accelerators = fields.get("accelerators")
        self.use_spot = bool(fields.get("use_spot"))
        self.region = fields.get("region") or ("us-east-1" if infra else None)
        self.fields = {"infra": infra, **fields}
        self.cloud = _Cloud(infra.split("/")[0].upper()) if infra else None

    def get_cost(self, seconds: float) -> float:
        assert self.instance_type is not None, "Instance type must be specified"
        hourly = PRICES[self.instance_type] * (SPOT_FACTOR if self.use_spot else 1.0)
        return hourly * seconds / 3600


class Task:
    def __init__(self, name: str | None = None, **fields: Any) -> None:
        self.name = name
        self.fields = fields
        self.resources: Resources | None = None

    def set_resources(self, resources: Resources) -> Task:
        self.resources = resources
        return self


__all__ = ["Resources", "Task", "get", "jobs", "stream_and_get"]
