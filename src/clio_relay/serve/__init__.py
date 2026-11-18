"""Relay's service runtime: a service is a job with ports (RL19 to RL23).

Adapters call :func:`start_for`; relay builds :class:`ServeRuntime` through
the ``serve`` entry point of ``clio_relay.hooks``.
"""

from clio_relay.serve.runtime import ServeRuntime, current, factory, start_for

__all__ = ["ServeRuntime", "current", "factory", "start_for"]
