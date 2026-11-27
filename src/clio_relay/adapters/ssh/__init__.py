"""The ``ssh`` adapter: a site reached with the person's own ``ssh`` (RL10)."""

from clio_relay.adapters.ssh.adapter import SshAdapter
from clio_relay.adapters.ssh.config import SshConfig

__all__ = ["SshAdapter", "SshConfig"]
