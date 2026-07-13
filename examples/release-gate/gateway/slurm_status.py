"""Emit the generic service-runtime status contract for one SLURM job."""

from __future__ import annotations

import json
import re
import subprocess
import sys


def _record(line: str) -> dict[str, str]:
    """Parse one oneline ``scontrol`` record without accounting."""
    normalized = line.strip()
    matches = list(re.finditer(r"(?<!\S)([A-Za-z][A-Za-z0-9_:]*)=", normalized))
    result: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        result[match.group(1)] = normalized[match.end() : end].strip()
    return result


def main() -> None:
    """Print one structured runtime status JSON object."""
    if len(sys.argv) != 2 or re.fullmatch(r"[0-9]+", sys.argv[1]) is None:
        raise SystemExit("expected one numeric scheduler job id")
    job_id = sys.argv[1]
    queued = subprocess.run(
        ["squeue", "-h", "-j", job_id, "-o", "%T|%N|%R"],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if queued.returncode != 0:
        raise SystemExit(queued.stderr.strip() or "squeue failed")
    row = queued.stdout.strip().split("|", 2)
    if len(row) == 3 and row[0]:
        state, node, reason = row
        print(
            json.dumps(
                {
                    "state": state.lower(),
                    "service_host": None if node in {"", "(null)", "N/A"} else node,
                    "reason": reason,
                },
                sort_keys=True,
            )
        )
        return
    historical = subprocess.run(
        ["scontrol", "show", "job", job_id, "-o"],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if historical.returncode != 0:
        raise SystemExit(historical.stderr.strip() or "scontrol failed")
    record = _record(historical.stdout)
    node = record.get("NodeList") or record.get("BatchHost")
    print(
        json.dumps(
            {
                "state": record.get("JobState", "unknown").lower(),
                "service_host": None if node in {None, "", "(null)", "N/A"} else node,
                "reason": record.get("Reason"),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
