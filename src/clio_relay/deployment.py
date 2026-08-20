"""Sudo-less endpoint deployment helpers.

Thin facade over the deployment-helper owner modules (clio-relay#231):

- :mod:`clio_relay.deployment_activation` -- the bounded systemd
  activation-observer bash template and its timing constants.
- :mod:`clio_relay.deployment_unit` -- the systemd unit-file template,
  escaping helpers, and the deterministic cluster -> unit-name mapping.
- :mod:`clio_relay.deployment_ssh` -- the SSH-borne install/restart
  operations that consume the two templates above.

Every public name this module used to define is re-exported here verbatim so
existing imports (``from clio_relay.deployment import ...``, or
``import clio_relay.deployment as deployment`` followed by a qualified
``deployment.<name>`` call/monkeypatch) keep working unchanged. ``subprocess``
is imported directly (not merely re-exported) so
``deployment.subprocess.run`` stays patchable exactly as before: it is the
same interpreter-wide module object :mod:`clio_relay.deployment_ssh` reads,
so a test patch reaches the real call site regardless of which module holds
the reference.
"""

from __future__ import annotations

import subprocess  # noqa: F401 -- re-exported for `deployment.subprocess.run` patch sites

from clio_relay.deployment_activation import (
    ENDPOINT_SERVICE_CONTROL_TIMEOUT_SECONDS,
    ENDPOINT_SERVICE_SSH_SETUP_MARGIN_SECONDS,
    ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS,
    ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS,
    ENDPOINT_SERVICE_START_POLL_SECONDS,
    ENDPOINT_SERVICE_START_PROGRESS_SECONDS,
    ENDPOINT_SERVICE_SYSTEMD_START_TIMEOUT_SECONDS,
    render_bounded_user_service_activation_helper,
)
from clio_relay.deployment_ssh import (
    _remote_install_script,  # pyright: ignore[reportPrivateUsage]
    _remote_restart_script,  # pyright: ignore[reportPrivateUsage]
    install_endpoint_user_service_over_ssh,
    restart_endpoint_user_service_over_ssh,
)
from clio_relay.deployment_unit import (
    endpoint_user_service_name,
    render_endpoint_user_service,
    write_endpoint_user_service,
)

__all__ = [
    # Private-named, but re-exported deliberately: existing tests call these
    # two remote-script builders directly as `deployment._remote_*_script`
    # (see tests/test_doctor_and_relay_host.py, tests/test_worker_service_
    # policy.py). Listing them here is what tells ruff's F401 check this
    # import is an intentional re-export, not dead code.
    "_remote_install_script",
    "_remote_restart_script",
    "ENDPOINT_SERVICE_CONTROL_TIMEOUT_SECONDS",
    "ENDPOINT_SERVICE_SSH_SETUP_MARGIN_SECONDS",
    "ENDPOINT_SERVICE_SSH_TIMEOUT_SECONDS",
    "ENDPOINT_SERVICE_START_OBSERVATION_TIMEOUT_SECONDS",
    "ENDPOINT_SERVICE_START_POLL_SECONDS",
    "ENDPOINT_SERVICE_START_PROGRESS_SECONDS",
    "ENDPOINT_SERVICE_SYSTEMD_START_TIMEOUT_SECONDS",
    "endpoint_user_service_name",
    "install_endpoint_user_service_over_ssh",
    "render_bounded_user_service_activation_helper",
    "render_endpoint_user_service",
    "restart_endpoint_user_service_over_ssh",
    "write_endpoint_user_service",
]
