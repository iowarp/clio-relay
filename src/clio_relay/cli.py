"""The `clio-relay` entry point. It re-exports the worker command tree until the cutover."""

from clio_relay.adapters.worker.cli import app as app
