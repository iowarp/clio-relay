"""Pinned artifact identity constants for the Linux cluster bootstrap.

Split from bootstrap.py (clio-relay#255). Pure data -- versions, expected
SHA-256 pins, default paths, and script-deadline budgets -- with zero
dependency on bootstrap.py itself, so both bootstrap.py and every
bootstrap_*-owner render module can import these directly without risking
a circular import. None of these names are monkeypatched by the test
suite; a plain re-import is sufficient in bootstrap.py.
"""

from __future__ import annotations

FRP_VERSION = "0.69.1"
FRP_WINDOWS_AMD64_SHA256 = "829ac915f8655d4d4e021b8db61b46c3445205ed80d32b04cda7fa89d87c46e0"
FRP_LINUX_AMD64_SHA256 = "7be257b72dbbc60bcb3e0e25a5afd1dfac7b63f897084864d3c956dd3d5674e1"
FRPC_LINUX_AMD64_SHA256 = "142f447f43fef286acc8da8a6852dda80631db631d604b2e63634b2db4d6848c"
FRPS_LINUX_AMD64_SHA256 = "68d2908bb73fe7a03c29d9227d2acc2104bff3fea6b1cece0b8388c1a0660442"
FRPC_WINDOWS_AMD64_SHA256 = "1d1c4f988b1808bb458a4ba38f00359052d14636023a504520e0afed127d636d"
FRPS_WINDOWS_AMD64_SHA256 = "bd463ef89370abc6973c86258256fa65776baa5f515ef91ebeabd6070b92e229"
UV_VERSION = "0.11.28"
UV_LINUX_AMD64_ARCHIVE_SHA256 = "e490a6464492183c5d4534a5527fb4440f7f2bb2f228162ad7e4afe076dc0224"
UV_LINUX_AMD64_EXECUTABLE_SHA256 = (
    "1cb9cd0a1749debf6049d7d2bb933882cc52d81016326ee6d99a786d6c988b03"
)
JARVIS_UTIL_COMMIT = "c91bfdc9bba802e4b03bfb1babe614ffa3e09644"
JARVIS_CD_VERSION = "1.8.1"
JARVIS_CD_WHEEL_FILENAME = f"jarvis_cd-{JARVIS_CD_VERSION}-py3-none-any.whl"
JARVIS_CD_WHEEL_URL = (
    "https://github.com/grc-iit/jarvis-cd/releases/download/"
    f"v{JARVIS_CD_VERSION}/{JARVIS_CD_WHEEL_FILENAME}"
)
JARVIS_CD_WHEEL_SHA256 = "ed891233e4b3767e949c6b5217bb03d4175b7d39334969c17ec83beb3c0c02d0"
DEFAULT_REMOTE_CORE_DIR = "$HOME/.local/share/clio-relay/core"
DEFAULT_REMOTE_SPOOL_DIR = "$HOME/.local/share/clio-relay/spool"
MAX_RELAY_WHEEL_METADATA_BYTES = 1024 * 1024
BOOTSTRAP_REMOTE_SCRIPT_TIMEOUT_SECONDS = 1800.0
BOOTSTRAP_PUBLIC_EXACT_DEADLINE_SECONDS = 29.0
BOOTSTRAP_PUBLIC_REPAIR_DEADLINE_SECONDS = 58.0
BOOTSTRAP_PERSISTENT_RECEIPT_PATH = "$HOME/.local/share/clio-relay/bootstrap-receipt.json"
"""Stable receipt path bootstrap publishes after every successful install.

Shared by the warm re-verification dial (``bootstrap._verify_persistent_
bootstrap_receipt``) and the cold one-pass script's own in-session re-read
(``bootstrap_one_pass_script.py``, clio-relay#209 -- which folds what used to
be a standalone ``ssh ... cat`` verification dial into the same combined
pass), so both name the exact same file from one source of truth.
"""
