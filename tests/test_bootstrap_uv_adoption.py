"""A byte-identical pinned uv must be adoptable (clio-relay#158).

Found driving the ares self-install. The host carried uv at exactly the pinned
version, and the bootstrap's own sha256 check on the binary PASSED -- proving
it byte-identical to the pinned artifact -- yet bootstrap still refused with

    bootstrap cannot adopt an existing uv version

because the adoption check compared ``uv --version`` output by exact string
equality against "uv <version>", while uv prints

    uv 0.11.28 (x86_64-unknown-linux-gnu)

The suffix is cosmetic. Any host with the correct uv already installed was
rejected, and the reason named the version rather than the formatting.
"""

from __future__ import annotations

import re

from clio_relay import bootstrap

# The exact shapes uv has emitted across releases.
SUFFIXED = f"uv {bootstrap.UV_VERSION} (x86_64-unknown-linux-gnu)"
BARE = f"uv {bootstrap.UV_VERSION}"
WRONG_VERSION = "uv 0.10.0 (x86_64-unknown-linux-gnu)"
NOT_UV = "python 3.12.0"


def _adoption_predicate(stdout: str) -> bool:
    """Mirror the rendered check: compare the version TOKEN, not the line."""
    return stdout.strip().split()[:2] == ["uv", bootstrap.UV_VERSION]


def test_the_pinned_uv_is_adopted_whatever_suffix_it_prints() -> None:
    assert _adoption_predicate(SUFFIXED)
    assert _adoption_predicate(BARE)


def test_a_different_uv_version_is_still_refused() -> None:
    """Loosening the comparison must not loosen the version pin itself."""
    assert not _adoption_predicate(WRONG_VERSION)
    assert not _adoption_predicate(NOT_UV)
    assert not _adoption_predicate("")


def test_rendered_script_compares_the_uv_version_token_not_the_whole_line() -> None:
    """Guard the rendered program against a regression to exact equality."""
    script = bootstrap.render_linux_user_bootstrap_script(
        cluster="ares-p5run2",
        core_dir="/mnt/common/u/relay-core",
        spool_dir="/mnt/common/u/relay-spool",
    )
    assert 'version_fields[:2] != ["uv", "' in script
    # The old exact-equality form must not come back.
    assert not re.search(r'completed\.stdout\.strip\(\) != "uv \d', script)
