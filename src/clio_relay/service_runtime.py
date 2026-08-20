"""Generic supervisor for scheduler-backed streaming service sessions."""

from __future__ import annotations

from clio_relay import service_runtime_attach as _attach
from clio_relay import service_runtime_browser as _browser
from clio_relay import service_runtime_core as _core
from clio_relay import service_runtime_detach as _detach
from clio_relay import service_runtime_jarvis_bind as _jarvis_bind
from clio_relay import service_runtime_local_connector as _local_connector
from clio_relay import service_runtime_local_start as _local_start
from clio_relay import service_runtime_observation as _observation
from clio_relay import service_runtime_reconciliation as _reconciliation
from clio_relay import service_runtime_remote_connector as _remote_connector
from clio_relay import service_runtime_start as _start
from clio_relay import service_runtime_stop as _stop
from clio_relay.service_runtime_results import (
    ServiceRuntimePendingResult,  # noqa: F401 -- cli.py/mcp_server.py/live_acceptance.py bare-import this
    ServiceRuntimeStartResult,  # noqa: F401 -- cli.py bare-imports this; see rationale above
    ServiceRuntimeStopResult,  # noqa: F401 -- cli.py/mcp_server.py/live_acceptance.py bare-import this
)


class ServiceRuntimeSupervisor(
    _core._ServiceRuntimeCoreMixin,
    _start._ServiceRuntimeStartMixin,
    _jarvis_bind._ServiceRuntimeJarvisBindMixin,
    _browser._ServiceRuntimeBrowserMixin,
    _stop._ServiceRuntimeStopMixin,
    _detach._ServiceRuntimeDetachMixin,
    _attach._ServiceRuntimeAttachMixin,
    _reconciliation._ServiceRuntimeReconciliationMixin,
    _local_connector._ServiceRuntimeLocalConnectorMixin,
    _observation._ServiceRuntimeObservationMixin,
    _remote_connector._ServiceRuntimeRemoteConnectorMixin,
    _local_start._ServiceRuntimeLocalStartMixin,
):
    """Start, bind, probe, and tear down scheduler-backed remote service sessions.

    Composed entirely from owner-module mixins (#231 class-mixin split):
    each mixin owns one coherent slice of the state machine's methods, and
    this class is assembly only -- imports, the mixin composition, and this
    docstring. See each mixin's own module docstring for its exact method
    set and cross-mixin dependencies. Mixins call each other freely through
    ``self`` -- Python's MRO resolves ``self.other_method(...)`` to
    whichever mixin defines it regardless of where the call originates, so
    no cross-mixin qualification is needed or used.

    Composition, in base-list order:
      - ``_ServiceRuntimeCoreMixin`` (service_runtime_core.py): construction,
        the per-gateway transition lock, the shared SSH transport, JARVIS
        authorization resolution, durable-failure recorders. Must stay
        first: every other mixin depends on ``__init__`` here.
      - ``_ServiceRuntimeStartMixin`` (service_runtime_start.py): the
        start/resume-start state machine.
      - ``_ServiceRuntimeJarvisBindMixin`` (service_runtime_jarvis_bind.py):
        JARVIS-bound runtime binding.
      - ``_ServiceRuntimeBrowserMixin`` (service_runtime_browser.py):
        browser sandbox attach/detach.
      - ``_ServiceRuntimeStopMixin`` (service_runtime_stop.py): teardown.
      - ``_ServiceRuntimeDetachMixin`` (service_runtime_detach.py):
        desktop-connector-only detach and resumability predicates.
      - ``_ServiceRuntimeAttachMixin`` (service_runtime_attach.py):
        desktop-connector reattachment.
      - ``_ServiceRuntimeReconciliationMixin``
        (service_runtime_reconciliation.py): crash-recovery ownership-intent
        reconciliation.
      - ``_ServiceRuntimeLocalConnectorMixin``
        (service_runtime_local_connector.py): desktop-connector process
        stop, shared across five other mixins.
      - ``_ServiceRuntimeObservationMixin`` (service_runtime_observation.py):
        scheduler/runtime observation and verification.
      - ``_ServiceRuntimeRemoteConnectorMixin``
        (service_runtime_remote_connector.py): remote/allocation connector
        lifecycle.
      - ``_ServiceRuntimeLocalStartMixin`` (service_runtime_local_start.py):
        local process start and HTTP health waits.
    """
