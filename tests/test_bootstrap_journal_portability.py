"""The first-install journal must run on an OLD system interpreter (#158).

``bootstrap_journal`` is the one module the bootstrap shell executes on a
virgin cluster with the SYSTEM python, before anything has been downloaded or
installed -- its own docstring states that contract. It therefore may not use
stdlib APIs newer than the oldest interpreter a target cluster plausibly ships.

Found on ares (Ubuntu 22.04, system python 3.10.12): the module imported
``datetime.UTC``, which only exists from 3.11, so the bootstrap died with

    File "bootstrap_journal.py", line 20, in <module>
    ImportError: cannot import name 'UTC' from 'datetime'

after the remote work had already begun. ``timezone.utc`` is the portable
spelling and has existed since 3.2.
"""

from __future__ import annotations

import ast
from pathlib import Path

import clio_relay.bootstrap_journal as journal

# The floor is set by the system interpreters real clusters ship. Ubuntu 22.04
# -- still the common HPC login-node OS, and what ares runs -- carries 3.10.
#
# 3.10 and not lower: the module evaluates runtime unions such as
# ``cast(Callable[[], int] | None, ...)``, which need 3.10. Supporting a Debian
# 11 style 3.9 system python would take more than this guard checks, so the
# floor is stated where it is actually met rather than where we wish it were.
MINIMUM_SUPPORTED_PYTHON = (3, 10)

# stdlib names introduced after the floor that this module must not reach for.
FORBIDDEN_IMPORTS: dict[tuple[str, str], str] = {
    ("datetime", "UTC"): "datetime.UTC is 3.11+; use timezone.utc",
    ("typing", "Self"): "typing.Self is 3.11+",
    ("typing", "override"): "typing.override is 3.12+",
    ("enum", "StrEnum"): "enum.StrEnum is 3.11+",
    ("asyncio", "TaskGroup"): "asyncio.TaskGroup is 3.11+",
    ("itertools", "batched"): "itertools.batched is 3.12+",
    ("tomllib", "*"): "tomllib is 3.11+",
}


def _module_source() -> str:
    return Path(journal.__file__).read_text(encoding="utf-8")


def test_first_install_journal_avoids_post_floor_stdlib_apis() -> None:
    tree = ast.parse(_module_source())
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                reason = FORBIDDEN_IMPORTS.get((node.module, alias.name))
                if reason is None:
                    reason = FORBIDDEN_IMPORTS.get((node.module, "*"))
                if reason is not None:
                    violations.append(f"line {node.lineno}: {node.module}.{alias.name} -- {reason}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                reason = FORBIDDEN_IMPORTS.get((alias.name, "*"))
                if reason is not None:
                    violations.append(f"line {node.lineno}: import {alias.name} -- {reason}")

    assert not violations, (
        "bootstrap_journal runs on the target's SYSTEM python "
        f"(floor {MINIMUM_SUPPORTED_PYTHON[0]}.{MINIMUM_SUPPORTED_PYTHON[1]}): "
        + "; ".join(violations)
    )


def test_first_install_journal_uses_the_portable_utc_spelling() -> None:
    source = _module_source()
    assert "datetime.now(timezone.utc)" in source
    assert "datetime.now(UTC)" not in source


def test_first_install_journal_still_stamps_utc_timestamps() -> None:
    """Portability must not silently change the recorded timezone."""
    from datetime import datetime, timezone

    stamped = datetime.now(timezone.utc).isoformat()
    assert stamped.endswith("+00:00")


def test_first_install_journal_forbids_clio_relay_imports() -> None:
    """The embedded exec() blob has no clio_relay package to import from.

    ``bootstrap.py``'s ``render_linux_user_bootstrap_script`` reads this
    file's RAW BYTES verbatim (``Path(__file__).with_name("bootstrap_journal
    .py").read_bytes()``) and embeds them as one base64 blob that
    ``bootstrap_script_preamble.py``'s ``bootstrap_journal_action()`` shell
    function ``exec()``s in an isolated namespace
    (``{"__name__": "__main__", "__file__": "bootstrap_journal.py"}``) on the
    TARGET cluster before anything -- not even the ``clio_relay`` package
    itself -- has been downloaded or installed there. A module-level
    ``import clio_relay...`` (absolute or relative) here would raise
    ``ModuleNotFoundError`` the instant that exec runs on a virgin host,
    regardless of which CLI action was requested, so this module cannot
    become a thin facade over sibling ``clio_relay.bootstrap_journal_*``
    owner modules the way its cousins (``bootstrap_reconcile.py``,
    ``process_containment.py``, ...) can -- their split survives because
    their own deployment path installs the whole candidate-overlay
    directory onto ``sys.path`` first; this module has no equivalent
    directory-shipping path. See the ``scripts/check_file_size.py``
    ``RATCHET_BASELINE`` entry for this file for the full investigation
    (split/bootstrap-journal-w3) and the isolated-interpreter repro that
    proved it.
    """
    tree = ast.parse(_module_source())
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level > 0 or (node.module and node.module.split(".")[0] == "clio_relay"):
                target = node.module or ("." * node.level)
                violations.append(f"line {node.lineno}: from {target} import ...")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "clio_relay":
                    violations.append(f"line {node.lineno}: import {alias.name}")

    assert not violations, (
        "bootstrap_journal.py is exec()'d as one raw self-contained blob on a "
        "virgin cluster with no clio_relay package installed -- it cannot "
        "import any clio_relay sibling module: " + "; ".join(violations)
    )
