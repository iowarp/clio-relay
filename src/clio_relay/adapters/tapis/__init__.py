"""The ``tapis`` adapter: jobs and files on a Tapis system as the signed-in person."""

from clio_relay.adapters.tapis.adapter import TapisAdapter
from clio_relay.adapters.tapis.config import TapisConfig

__all__ = ["TapisAdapter", "TapisConfig"]
