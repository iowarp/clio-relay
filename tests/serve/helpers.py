"""Shared builders for the serve tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from clio_relay.jobs.spec import ServiceSpec
from clio_relay.serve.config import Rendezvous, SiteServe
from clio_relay.serve.plan import ServePlan, plan_ports


def service(**overrides: Any) -> ServiceSpec:
    raw: dict[str, Any] = {
        "job": {
            "name": "Web App",
            "executable": "python3",
            "arguments": ["app.py", "--port", "${CLIO_RELAY_PORT_WEB}"],
        },
        "ports": [
            {
                "name": "web",
                "protocol": "http",
                "health_path": "/health",
                "protection": "secret_env:SVC_SECRET",
            }
        ],
    }
    raw.update(overrides)
    return ServiceSpec.model_validate(raw)


def rendezvous(tmp_path: Path, **overrides: Any) -> Rendezvous:
    token = tmp_path / "token"
    token.write_text("frp-token-value\n", encoding="utf-8")
    raw: dict[str, Any] = {"address": "relay.example.org", "port": 7000, "token_file": str(token)}
    raw.update(overrides)
    return Rendezvous.model_validate(raw)


def frp_plan(
    tmp_path: Path, spec: ServiceSpec | None = None, *, site: SiteServe | None = None, **rv: Any
) -> ServePlan:
    spec = spec or service()
    ports = plan_ports(spec)
    return ServePlan(
        operation_id="op_test",
        site="fx",
        adapter="fixture",
        service="web_app",
        mode="frp",
        ports=ports,
        secret_key="shared-key-value",
        secrets={port.secret_env: f"secret-of-{port.name}" for port in ports if port.secret_env},
        rendezvous=rendezvous(tmp_path, **rv),
        site_serve=site or SiteServe(frpc="/opt/frp/frpc"),
    )
