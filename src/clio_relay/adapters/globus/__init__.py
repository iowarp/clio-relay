"""The ``globus`` adapter: Globus Compute for jobs, Globus Transfer for files."""

from clio_relay.adapters.globus.adapter import GlobusAdapter, GlobusConfig, UserConfigKeys

__all__ = ["GlobusAdapter", "GlobusConfig", "UserConfigKeys"]
