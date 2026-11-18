"""The ``sky`` adapter: SkyPilot managed jobs, run from SkyPilot's own environment (RL14)."""

from pathlib import Path

from clio_relay.adapters.sky.adapter import SkyAdapter, SkyConfig, SkyCredentials, find_python

FAKE_SKY_PATH = Path(__file__).with_name("fake_sky")

__all__ = ["FAKE_SKY_PATH", "SkyAdapter", "SkyConfig", "SkyCredentials", "find_python"]
