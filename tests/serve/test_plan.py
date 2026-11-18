"""Turning a service spec into the wrapped job and the files it needs (RL19, RL22)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from clio_relay.jobs.refusals import Refusal
from clio_relay.serve.config import SiteServe
from clio_relay.serve.plan import (
    HopState,
    check_wrappable,
    env_suffix,
    plan_ports,
    secrets_file,
    service_name,
    site_files,
    site_secrets,
    substitute,
    wrapped_job,
)

from .helpers import frp_plan, service

DIGEST = "a" * 64


def test_service_name_comes_from_the_job_or_the_first_port() -> None:
    assert service_name(service()) == "web_app"
    unnamed = service(job={"executable": "x"}, ports=[{"name": "paraview"}])
    assert service_name(unnamed) == "paraview"


def test_env_suffix_is_a_variable_name() -> None:
    assert env_suffix("my-port") == "MY_PORT"


def test_ports_keep_a_named_port_and_read_the_secret_variable() -> None:
    spec = service(
        ports=[
            {"name": "web", "port": 8888, "protocol": "http", "protection": "secret_env:TOKEN"},
            {"name": "raw", "protection": "loopback on the node"},
        ]
    )
    web, raw = plan_ports(spec)
    assert (web.remote_port, web.secret_env) == (8888, "TOKEN")
    assert raw.secret_env is None
    assert web.local_port != raw.local_port


def test_ports_refuse_a_repeated_name_and_a_bad_variable() -> None:
    twice = service(ports=[{"name": "a"}, {"name": "a"}])
    with pytest.raises(Refusal, match="named twice"):
        plan_ports(twice)
    bad = service(ports=[{"name": "web", "protocol": "http", "protection": "secret_env:1x"}])
    with pytest.raises(Refusal, match="names no environment variable"):
        plan_ports(bad)


@pytest.mark.parametrize(
    ("job", "words"),
    [
        (
            {"package": {"name": "pvserver", "version": "1", "sha256": DIGEST}},
            "package service",
        ),
        ({"executable": "x", "launcher": "mpirun"}, "one wrapper process"),
        ({"executable": "x", "inputs": [{"name": "serve/x", "sha256": DIGEST}]}, "relay uses"),
    ],
)
def test_wrapper_refuses_what_it_cannot_wrap(job: dict[str, object], words: str) -> None:
    with pytest.raises(Refusal, match=words):
        check_wrappable(service(job=job, ports=[{"name": "a"}]))


def test_substitute_fills_only_known_relay_variables() -> None:
    values = ["--port", "${CLIO_RELAY_PORT_WEB}", "${CLIO_RELAY_OTHER}", "${HOME}"]
    assert substitute(values, {"CLIO_RELAY_PORT_WEB": "9000"}) == [
        "--port",
        "9000",
        "${CLIO_RELAY_OTHER}",
        "${HOME}",
    ]


def test_secrets_file_refuses_a_value_it_cannot_quote() -> None:
    assert secrets_file({"B": "two", "A": "one"}) == b"export A='one'\nexport B='two'\n"
    with pytest.raises(Refusal):
        secrets_file({"A": "it's"})


def test_wrapped_job_keeps_secrets_out_of_arguments_and_environment(tmp_path: Path) -> None:
    spec = service()
    plan = frp_plan(tmp_path, spec)
    job = wrapped_job(spec, plan, {"launch.sh": DIGEST})
    web = plan.ports[0]
    assert job.executable == "/bin/sh"
    assert job.arguments[:4] == ["serve/launch.sh", "--", "python3", "app.py"]
    assert job.arguments[-1] == str(web.remote_port)
    assert job.environment["CLIO_RELAY_PORT_WEB"] == str(web.remote_port)
    assert job.environment["CLIO_RELAY_FRPC"] == "/opt/frp/frpc"
    assert job.attributes.duration is None
    assert [item.name for item in job.inputs] == ["serve/launch.sh"]
    staged = wrapped_job(spec, plan, {"launch.sh": DIGEST, "secrets.env": DIGEST})
    assert {item.name: item.transient for item in staged.inputs} == {
        "serve/launch.sh": False,
        "serve/secrets.env": True,
    }
    dumped = json.dumps(job.model_dump(mode="json"))
    for secret in [*plan.secrets.values(), plan.secret_key, "frp-token-value"]:
        assert secret not in dumped


def test_endpoint_and_service_env_carry_no_secret(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    port = plan.ports[0]
    endpoint = plan.endpoint(note="serving", connect_command=None)
    assert endpoint.ports[0].url == f"http://127.0.0.1:{port.local_port}/health"
    dumped = json.dumps([endpoint.model_dump(mode="json"), plan.service_env()])
    assert "secret-of-web" not in dumped
    assert "shared-key-value" not in dumped


def test_site_secrets_carry_the_frp_token_key_and_hop_token(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path)
    plan.hop = HopState(pid=1, host="login1", port=3128, token="hop-token")
    values = site_secrets(plan)
    assert values["SVC_SECRET"] == "secret-of-web"
    assert values["CLIO_RELAY_FRP_TOKEN"] == "frp-token-value"
    assert values["CLIO_RELAY_FRP_SK"] == "shared-key-value"
    assert values["CLIO_RELAY_HOP_TOKEN"] == "hop-token"


def test_site_files_stage_frpc_and_the_wss_ca(tmp_path: Path) -> None:
    binary = tmp_path / "frpc-linux"
    binary.write_bytes(b"\x7fELF")
    ca = tmp_path / "ca.pem"
    ca.write_text("CA", encoding="utf-8")
    plan = frp_plan(
        tmp_path,
        site=SiteServe(frpc_binary=str(binary)),
        protocol="wss",
        ca_file=str(ca),
        server_name="frps.example.org",
    )
    files = site_files(plan)
    assert set(files) == {"launch.sh", "ports", "secrets.env", "frpc.toml", "frpc", "ca.pem"}
    assert files["ports"] == f"web {plan.ports[0].remote_port} http /health\n".encode()
    toml = files["frpc.toml"].decode()
    assert 'transport.tls.trustedCaFile = "{{ .Envs.CLIO_RELAY_JOB_DIR }}/serve/ca.pem"' in toml
    assert 'transport.tls.serverName = "frps.example.org"' in toml
    assert "frp-token-value" not in toml
    assert "shared-key-value" not in toml


def test_site_files_refuse_a_site_with_no_frp_client(tmp_path: Path) -> None:
    with pytest.raises(Refusal, match="no frp client"):
        site_files(frp_plan(tmp_path, site=SiteServe()))


def test_login_hop_and_http_proxy_set_frpc_proxy_url(tmp_path: Path) -> None:
    plan = frp_plan(tmp_path, site=SiteServe(frpc="frpc", hop="login"))
    plan.hop = HopState(pid=1, host="login1", port=3128, token="hop-token")
    toml = site_files(plan)["frpc.toml"].decode()
    assert (
        'transport.proxyURL = "http://relay:{{ .Envs.CLIO_RELAY_HOP_TOKEN }}@login1:3128"' in toml
    )
    assert "hop-token" not in toml
    proxied = frp_plan(
        tmp_path, site=SiteServe(frpc="frpc", hop="http_proxy", proxy_url="http://proxy:3128")
    )
    assert 'transport.proxyURL = "http://proxy:3128"' in site_files(proxied)["frpc.toml"].decode()
