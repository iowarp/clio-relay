"""Release dependency constraints that protect clean-wheel installs."""

import tomllib
from pathlib import Path

from packaging.requirements import Requirement


def test_httpx_stays_on_the_fastmcp_compatible_major_version() -> None:
    """Prevent prerelease resolution from selecting incompatible httpx 1.x."""
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    requirements = {
        requirement.name: requirement
        for requirement in (Requirement(value) for value in project["dependencies"])
    }

    assert str(requirements["httpx"].specifier) == "<1,>=0.27"
