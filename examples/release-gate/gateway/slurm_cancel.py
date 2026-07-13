"""Cancel one exact live-validation SLURM job."""

from __future__ import annotations

import re
import subprocess
import sys


def main() -> None:
    """Cancel a validated numeric scheduler identifier without a shell."""
    if len(sys.argv) != 2 or re.fullmatch(r"[0-9]+", sys.argv[1]) is None:
        raise SystemExit("expected one numeric scheduler job id")
    result = subprocess.run(
        ["scancel", sys.argv[1]],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or "scancel failed")


if __name__ == "__main__":
    main()
